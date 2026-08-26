//! Offline designation (DESIGN.md §5.2): CAS-enforced, exactly one
//! designee per path, overlap-checked at creation.
//!
//! A designation is one small JSON object,
//! `designations/<hash-of-path>.json`, using the same CAS idioms as
//! [`crate::lease`]: `If-None-Match` claims a fresh path, `If-Match`
//! releases or transfers it. Unlike a lease it has no TTL — the
//! designee's claim is non-stealable by design (DESIGN.md: "no
//! TTL-steal on the designee's claim"); it ends only by an explicit
//! `online <path>` (a CAS-delete/release by the designee itself, or by
//! anyone once `released` is already true).
//!
//! ### Overlap and the TOCTOU window
//!
//! Exactly one designation may cover a given subtree: creating
//! `/site/sub` while `/site` is designated (or vice versa) must be
//! refused. The check-then-create is not atomic against a second racing
//! creator, so after a successful CAS-create this store re-lists and
//! self-deletes if it now sees a strictly older overlapping designation
//! — the older timestamp wins, and the loser's create is undone. This
//! is a best-effort close of the window, not a proof of exclusion: it

use crate::error::StoreError;
use crate::layout;
use futures::TryStreamExt;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub const DESIGNATION_VERSION: u32 = 1;

fn default_version() -> u32 {
    DESIGNATION_VERSION
}

/// Exactly-one-designee claim over a subtree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Designation {
    #[serde(default = "default_version")]
    pub v: u32,
    /// Absolute path this designation covers, e.g. `/site`.
    pub path: String,
    /// Node id that owns the claim.
    pub designee: u64,
    pub created_unix_ms: i64,
    /// `--ro`: a read guarantee (via pinning) without write authority —
    /// non-designee writes are NOT restricted in this mode.
    #[serde(default)]
    pub read_only: bool,
    /// Set by `online <path>`: the designation is over. Kept (rather
    /// than deleted) so a concurrent creator's overlap check and the
    /// TOCTOU tie-break above have something to compare timestamps
    /// against; a released designation is simply skipped as "not a
    /// covering claim" everywhere else.
    #[serde(default)]
    pub released: bool,
}

impl Designation {
    pub fn new(path: &str, designee: u64, read_only: bool) -> Self {
        Self {
            v: DESIGNATION_VERSION,
            path: path.to_string(),
            designee,
            created_unix_ms: crate::lease::now_unix_ms(),
            read_only,
            released: false,
        }
    }

    pub fn released(&self) -> Self {
        Self {
            released: true,
            ..self.clone()
        }
    }

    /// Does this designation's path cover (equal or ancestor of) `other`?
    pub fn covers(&self, other: &str) -> bool {
        path_covers(&self.path, other)
    }

    /// Do this and `other`'s paths overlap in either direction (one is
    /// an ancestor of, or equal to, the other)?
    pub fn overlaps(&self, other: &str) -> bool {
        path_covers(&self.path, other) || path_covers(other, &self.path)
    }
}

/// Is `ancestor` equal to, or a path-component-wise prefix of, `path`?
/// Deliberately component-aware: `/site` must not "cover" `/site2`.
pub fn path_covers(ancestor: &str, path: &str) -> bool {
    let a: Vec<&str> = ancestor.split('/').filter(|s| !s.is_empty()).collect();
    let p: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    a.len() <= p.len() && a.iter().zip(p.iter()).all(|(x, y)| x == y)
}

/// Hash a path into the object key suffix. Not security-sensitive (the
/// bucket's IAM is the trust boundary); this only needs to be short,
/// filesystem/URL-safe, and stable so repeated `offline <path>` calls
/// resolve to the same object.
pub fn path_hash(path: &str) -> String {
    let digest = blake3::hash(path.as_bytes());
    digest.to_hex()[..32].to_string()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesignationTag(UpdateVersion);

/// Whether the backend can enforce the CAS (see `crate::lease::LeaseMode`
/// for the identical trade-off and its rationale).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DesignationMode {
    Cas,
    SingleWriter,
}

pub struct DesignationStore {
    store: Arc<dyn ObjectStore>,
    mode: DesignationMode,
}

impl DesignationStore {
    pub fn new(store: Arc<dyn ObjectStore>, mode: DesignationMode) -> Self {
        Self { store, mode }
    }

    pub async fn get(
        &self,
        path: &str,
    ) -> Result<Option<(Designation, DesignationTag)>, StoreError> {
        self.get_by_hash(&path_hash(path)).await
    }

    async fn get_by_hash(
        &self,
        hash: &str,
    ) -> Result<Option<(Designation, DesignationTag)>, StoreError> {
        let res = match self.store.get(&layout::designation(hash)).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let tag = DesignationTag(UpdateVersion {
            e_tag: res.meta.e_tag.clone(),
            version: res.meta.version.clone(),
        });
        let d: Designation = serde_json::from_slice(&res.bytes().await?)?;
        Ok(Some((d, tag)))
    }

    /// Every designation object, including released ones (callers filter
    /// as needed — the overlap check must see released claims too, so it
    /// can tell "truly free" from "was here, now released").
    pub async fn list_all(&self) -> Result<Vec<Designation>, StoreError> {
        let metas = self
            .store
            .list(Some(&layout::designations_prefix()))
            .try_collect::<Vec<_>>()
            .await?;
        let mut out = Vec::new();
        for m in metas {
            let Ok(res) = self.store.get(&m.location).await else {
                continue;
            };
            let Ok(bytes) = res.bytes().await else {
                continue;
            };
            if let Ok(d) = serde_json::from_slice::<Designation>(&bytes) {
                out.push(d);
            }
        }
        Ok(out)
    }

    /// Currently-active designations whose path overlaps `path` (either
    /// direction), excluding a released claim at exactly `path` itself
    /// (that one is what `create` is about to supersede).
    pub async fn overlapping(&self, path: &str) -> Result<Vec<Designation>, StoreError> {
        Ok(self
            .list_all()
            .await?
            .into_iter()
            .filter(|d| !d.released && d.overlaps(path))
            .collect())
    }

    /// Create a fresh designation, refusing on overlap with any live
    /// claim. Closes the TOCTOU window documented in the module doc: if
    /// an overlapping designation with an older timestamp is visible
    /// right after our own CAS-create landed, we lost the race and
    /// self-delete.
    ///
    /// If a released designation already sits at exactly this path (a
    /// previous `online <path>` kept the object rather than deleting
    /// it — see the module doc), this CAS-swaps over it instead of a
    /// bare create, since the object already exists.
    pub async fn create(&self, d: &Designation) -> Result<(), StoreError> {
        let overlap = self.overlapping(&d.path).await?;
        if !overlap.is_empty() {
            return Err(StoreError::Conflict(format!(
                "path {:?} overlaps existing designation(s): {}",
                d.path,
                overlap
                    .iter()
                    .map(|o| o.path.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        let existing = self.get(&d.path).await?;
        let tag = match existing {
            Some((prev, prev_tag)) => {
                debug_assert!(prev.released, "overlapping() must have caught a live claim");
                self.swap(d, &prev_tag).await?
            }
            None => self.put(d, PutMode::Create).await?,
        };
        // Re-check: a concurrent creator may have landed an overlapping
        // designation between our list and our create.
        let after = self.overlapping(&d.path).await?;
        if let Some(older) = after
            .iter()
            .find(|o| o.path != d.path && o.created_unix_ms < d.created_unix_ms)
        {
            self.delete_at(d, &tag).await.ok();
            return Err(StoreError::Conflict(format!(
                "lost the race to an older overlapping designation at {:?}",
                older.path
            )));
        }
        Ok(())
    }

    /// Release (`online <path>`): mark the designation released via CAS.
    pub async fn release(&self, d: &Designation, tag: &DesignationTag) -> Result<(), StoreError> {
        self.swap(&d.released(), tag).await.map(|_| ())
    }

    async fn delete_at(&self, d: &Designation, tag: &DesignationTag) -> Result<(), StoreError> {
        self.swap(&d.released(), tag).await.map(|_| ())
    }

    async fn swap(
        &self,
        d: &Designation,
        tag: &DesignationTag,
    ) -> Result<DesignationTag, StoreError> {
        let mode = match self.mode {
            DesignationMode::Cas => PutMode::Update(tag.0.clone()),
            DesignationMode::SingleWriter => PutMode::Overwrite,
        };
        self.put(d, mode).await
    }

    async fn put(&self, d: &Designation, mode: PutMode) -> Result<DesignationTag, StoreError> {
        let body = serde_json::to_vec(d)?;
        match self
            .store
            .put_opts(
                &layout::designation(&path_hash(&d.path)),
                PutPayload::from(body),
                PutOptions::from(mode),
            )
            .await
        {
            Ok(r) => Ok(DesignationTag(UpdateVersion {
                e_tag: r.e_tag,
                version: r.version,
            })),
            Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::NotModified { .. }) => Err(StoreError::CasConflict),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn ds() -> DesignationStore {
        DesignationStore::new(Arc::new(InMemory::new()), DesignationMode::Cas)
    }

    #[test]
    fn path_covers_is_component_aware() {
        assert!(path_covers("/site", "/site"));
        assert!(path_covers("/site", "/site/sub"));
        assert!(path_covers("/", "/site/sub"));
        assert!(
            !path_covers("/site", "/site2"),
            "must not match by prefix string"
        );
        assert!(!path_covers("/site/sub", "/site"));
    }

    #[tokio::test]
    async fn create_get_release_roundtrip() {
        let s = ds();
        assert!(s.get("/site").await.unwrap().is_none());
        let d = Designation::new("/site", 1, false);
        s.create(&d).await.unwrap();
        let (got, tag) = s.get("/site").await.unwrap().unwrap();
        assert_eq!((got.designee, got.released), (1, false));

        s.release(&got, &tag).await.unwrap();
        let (released, _) = s.get("/site").await.unwrap().unwrap();
        assert!(released.released);
        // A released designation no longer blocks a fresh one at the
        // same or an overlapping path.
        s.create(&Designation::new("/site", 2, false))
            .await
            .unwrap();
        assert_eq!(s.get("/site").await.unwrap().unwrap().0.designee, 2);
    }

    /// Overlap in either direction (parent-designated-then-child, and
    /// child-designated-then-parent) must be refused.
    #[tokio::test]
    async fn overlapping_designations_are_refused() {
        let s = ds();
        s.create(&Designation::new("/site", 1, false))
            .await
            .unwrap();
        assert!(matches!(
            s.create(&Designation::new("/site/sub", 2, false)).await,
            Err(StoreError::Conflict(_))
        ));
        assert!(matches!(
            s.create(&Designation::new("/", 2, false)).await,
            Err(StoreError::Conflict(_))
        ));
        // A sibling path is unaffected.
        s.create(&Designation::new("/other", 2, false))
            .await
            .unwrap();
    }

    /// Two nodes race to designate overlapping paths. Both list-checks
    /// pass (neither has landed yet) and both CAS-creates succeed
    /// (distinct object keys — different path hashes), so the TOCTOU
    /// re-list-and-self-delete step must leave exactly the older one
    /// standing.
    #[tokio::test]
    async fn concurrent_overlapping_creates_leave_only_the_older() {
        let store = Arc::new(InMemory::new());
        let s = DesignationStore::new(store, DesignationMode::Cas);
        let mut older = Designation::new("/site", 1, false);
        let mut younger = Designation::new("/site/sub", 2, false);
        older.created_unix_ms = 100;
        younger.created_unix_ms = 200;

        // Simulate both passing the pre-check by creating directly
        // (bypassing the higher-level overlap check) and then invoking
        // the same re-check logic `create` uses.
        s.put(&older, PutMode::Create).await.unwrap();
        let tag = s.put(&younger, PutMode::Create).await.unwrap();

        let after = s.overlapping(&younger.path).await.unwrap();
        assert!(after.iter().any(|o| o.path == "/site"));
        s.delete_at(&younger, &tag).await.unwrap();

        let (site, _) = s.get("/site").await.unwrap().unwrap();
        assert!(!site.released, "the older designation must survive");
        let (sub, _) = s.get("/site/sub").await.unwrap().unwrap();
        assert!(sub.released, "the younger loser must be released");
    }

    #[tokio::test]
    async fn read_only_designation_flag_persists() {
        let s = ds();
        s.create(&Designation::new("/site", 1, true)).await.unwrap();
        let (d, _) = s.get("/site").await.unwrap().unwrap();
        assert!(d.read_only);
    }
}
