//! Partition leases (DESIGN.md §4 "Leases (the authority mechanism)",
//! §5 "Write Authority: the One Rule").
//!
//! A lease is one small JSON object per partition, `leases/<part>.json`,
//! whose *only* commit primitive is a conditional write:
//!
//! - **create** (`If-None-Match: *`) claims a partition nobody has ever
//!   held,
//! - **swap** (`If-Match: <etag>`) renews it (same holder, same epoch),
//!   releases it, or takes it over from an expired/released holder.
//!
//! Safety comes entirely from the store rejecting a stale precondition:
//! two nodes racing for the same expired lease read the same etag, both
//! swap, and exactly one wins ([`StoreError::CasConflict`] for the
//! loser). No clock agreement is needed for *mutual exclusion* — only
//! for liveness (when may a lease be considered expired), and there the
//! TTL is deliberately coarse (~60 s) relative to any plausible skew.
//!
//! Every holder change bumps [`Lease::epoch`], which log segments carry
//! so a deposed holder's late flush is recognizable (fencing).
//!
//! ### Backends without `If-Match`
//!
//! Some backends (notably `object_store`'s `LocalFileSystem`, which the
//! host smoke lane uses) implement create-if-absent but not etag CAS.
//! Renew and takeover are impossible there, so [`LeaseStore`] can be
//! built in [`LeaseMode::SingleWriter`], where swaps degrade to
//! unconditional PUTs: the state machine above still runs and still
//! records holder/epoch, but mutual exclusion is *assumed*, not
//! enforced. Callers must refuse to steal a live foreign lease in that
//! mode and say so loudly — see `cli::lease`.

use crate::error::StoreError;
use crate::layout;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Lease TTL default; `CONSTELLATION_LEASE_TTL_MS` overrides it (tests
/// use short TTLs to exercise expiry and takeover).
pub const DEFAULT_LEASE_TTL_MS: u64 = 60_000;

/// Current lease encoding version. Readers accept anything they can
/// deserialize (all fields default) so a future field is not a fault.
pub const LEASE_VERSION: u32 = 1;

pub fn lease_ttl_ms() -> u64 {
    std::env::var("CONSTELLATION_LEASE_TTL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&v| v > 0)
        .unwrap_or(DEFAULT_LEASE_TTL_MS)
}

pub fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Write authority over one partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    #[serde(default = "default_version")]
    pub v: u32,
    #[serde(default)]
    pub partition: String,
    /// Node id of the holder (`nodes/<id>`); 0 means "never held".
    #[serde(default)]
    pub holder: u64,
    /// Monotonic per-partition counter, bumped on every holder change.
    /// Stamped into every log segment the holder ships (fencing).
    #[serde(default)]
    pub epoch: u64,
    #[serde(default)]
    pub expires_unix_ms: i64,
    /// Set by a clean unmount: the partition is immediately claimable,
    /// no TTL to wait out.
    #[serde(default)]
    pub released: bool,
}

fn default_version() -> u32 {
    LEASE_VERSION
}

impl Lease {
    /// A fresh grant to `holder` at `epoch`, valid for `ttl_ms`.
    pub fn granted(partition: &str, holder: u64, epoch: u64, ttl_ms: u64) -> Self {
        Self {
            v: LEASE_VERSION,
            partition: partition.to_string(),
            holder,
            epoch,
            expires_unix_ms: now_unix_ms() + ttl_ms as i64,
            released: false,
        }
    }

    /// Same holder and epoch, pushed-out expiry.
    pub fn renewed(&self, ttl_ms: u64) -> Self {
        Self {
            expires_unix_ms: now_unix_ms() + ttl_ms as i64,
            released: false,
            ..self.clone()
        }
    }

    /// Voluntary hand-back: holder and epoch are preserved as history,
    /// `released` makes the partition claimable without waiting.
    pub fn released(&self) -> Self {
        Self {
            released: true,
            ..self.clone()
        }
    }

    pub fn is_expired(&self, now_ms: i64) -> bool {
        now_ms >= self.expires_unix_ms
    }

    /// Claimable by anyone: never held, cleanly released, or expired.
    pub fn is_claimable(&self, now_ms: i64) -> bool {
        self.holder == 0 || self.released || self.is_expired(now_ms)
    }

    pub fn expires_in_ms(&self, now_ms: i64) -> i64 {
        self.expires_unix_ms - now_ms
    }
}

/// The version token a swap must match. Thin wrapper so callers never
/// have to name `object_store` types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseTag(UpdateVersion);

/// How [`LeaseStore`] commits a swap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseMode {
    /// `If-Match` etag CAS: mutual exclusion enforced by the backend.
    Cas,
    /// Backend has no `If-Match`: swaps are unconditional PUTs and
    /// exclusion is assumed (single writer). Loud warning territory.
    SingleWriter,
}

/// Read/modify/write access to one partition's lease object.
pub struct LeaseStore {
    store: Arc<dyn ObjectStore>,
    partition: String,
    mode: LeaseMode,
}

impl LeaseStore {
    pub fn new(store: Arc<dyn ObjectStore>, partition: &str, mode: LeaseMode) -> Self {
        Self {
            store,
            partition: partition.to_string(),
            mode,
        }
    }

    pub fn mode(&self) -> LeaseMode {
        self.mode
    }

    pub fn partition(&self) -> &str {
        &self.partition
    }

    /// Current lease plus the token needed to swap it; `None` when no
    /// node has ever claimed the partition.
    pub async fn get(&self) -> Result<Option<(Lease, LeaseTag)>, StoreError> {
        let res = match self.store.get(&layout::lease(&self.partition)).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let tag = LeaseTag(UpdateVersion {
            e_tag: res.meta.e_tag.clone(),
            version: res.meta.version.clone(),
        });
        let lease: Lease = serde_json::from_slice(&res.bytes().await?)?;
        Ok(Some((lease, tag)))
    }

    /// Claim a partition that has no lease object at all.
    /// [`StoreError::CasConflict`] means somebody created it first.
    pub async fn try_create(&self, lease: &Lease) -> Result<LeaseTag, StoreError> {
        self.put(lease, PutMode::Create).await
    }

    /// Replace the lease, but only if it still has version `tag`.
    /// [`StoreError::CasConflict`] means another node changed it since
    /// the read — in [`LeaseMode::SingleWriter`] this cannot be
    /// detected and the write always lands.
    pub async fn try_swap(&self, lease: &Lease, tag: &LeaseTag) -> Result<LeaseTag, StoreError> {
        let mode = match self.mode {
            LeaseMode::Cas => PutMode::Update(tag.0.clone()),
            LeaseMode::SingleWriter => PutMode::Overwrite,
        };
        self.put(lease, mode).await
    }

    async fn put(&self, lease: &Lease, mode: PutMode) -> Result<LeaseTag, StoreError> {
        let body = serde_json::to_vec(lease)?;
        match self
            .store
            .put_opts(
                &layout::lease(&self.partition),
                PutPayload::from(body),
                PutOptions::from(mode),
            )
            .await
        {
            Ok(r) => Ok(LeaseTag(UpdateVersion {
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

/// Partition ids whose lease is currently held by `node_id` (not
/// released, not expired). Used by admin `leave --node-id` to refuse
/// retiring a node that still appears to own write authority.
pub async fn live_leases_held_by(
    store: Arc<dyn ObjectStore>,
    node_id: u64,
) -> Result<Vec<String>, StoreError> {
    use futures::TryStreamExt;
    let prefix = object_store::path::Path::from("leases");
    let metas = store.list(Some(&prefix)).try_collect::<Vec<_>>().await?;
    let now = now_unix_ms();
    let mut out = Vec::new();
    for m in metas {
        let Ok(res) = store.get(&m.location).await else {
            continue;
        };
        let Ok(bytes) = res.bytes().await else {
            continue;
        };
        let Ok(lease) = serde_json::from_slice::<Lease>(&bytes) else {
            continue;
        };
        if lease.holder == node_id && !lease.is_claimable(now) {
            out.push(lease.partition);
        }
    }
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    const P: &str = "p0";
    const TTL: u64 = 60_000;

    fn ls(mode: LeaseMode) -> LeaseStore {
        LeaseStore::new(Arc::new(InMemory::new()), P, mode)
    }

    #[tokio::test]
    async fn create_then_renew_keeps_holder_and_epoch() {
        let s = ls(LeaseMode::Cas);
        assert!(s.get().await.unwrap().is_none());
        let tag = s.try_create(&Lease::granted(P, 7, 1, TTL)).await.unwrap();
        let (lease, read_tag) = s.get().await.unwrap().unwrap();
        assert_eq!((lease.holder, lease.epoch, lease.released), (7, 1, false));
        assert_eq!(read_tag, tag);
        assert!(!lease.is_claimable(now_unix_ms()));

        s.try_swap(&lease.renewed(TTL), &read_tag).await.unwrap();
        let (renewed, _) = s.get().await.unwrap().unwrap();
        assert_eq!((renewed.holder, renewed.epoch), (7, 1));
        assert!(renewed.expires_unix_ms >= lease.expires_unix_ms);
    }

    #[tokio::test]
    async fn second_create_conflicts() {
        let s = ls(LeaseMode::Cas);
        s.try_create(&Lease::granted(P, 1, 1, TTL)).await.unwrap();
        assert!(matches!(
            s.try_create(&Lease::granted(P, 2, 1, TTL)).await,
            Err(StoreError::CasConflict)
        ));
        assert_eq!(s.get().await.unwrap().unwrap().0.holder, 1);
    }

    #[tokio::test]
    async fn expired_lease_is_taken_over_with_epoch_bump() {
        let s = ls(LeaseMode::Cas);
        // TTL of 0 ms: expired the instant it is written.
        s.try_create(&Lease::granted(P, 1, 4, 0)).await.unwrap();
        let (old, tag) = s.get().await.unwrap().unwrap();
        assert!(old.is_expired(now_unix_ms()));
        assert!(old.is_claimable(now_unix_ms()));
        s.try_swap(&Lease::granted(P, 2, old.epoch + 1, TTL), &tag)
            .await
            .unwrap();
        let (new, _) = s.get().await.unwrap().unwrap();
        assert_eq!((new.holder, new.epoch), (2, 5));
        assert!(!new.is_claimable(now_unix_ms()));
    }

    #[tokio::test]
    async fn release_makes_it_claimable_before_expiry() {
        let s = ls(LeaseMode::Cas);
        s.try_create(&Lease::granted(P, 1, 1, TTL)).await.unwrap();
        let (held, tag) = s.get().await.unwrap().unwrap();
        s.try_swap(&held.released(), &tag).await.unwrap();
        let (rel, _) = s.get().await.unwrap().unwrap();
        assert!(rel.released && !rel.is_expired(now_unix_ms()));
        assert!(rel.is_claimable(now_unix_ms()));
        // History is preserved so the next holder can bump the epoch.
        assert_eq!((rel.holder, rel.epoch), (1, 1));
    }

    /// Two nodes read the same expired lease and both try to take it:
    /// exactly one swap lands, the loser sees `CasConflict`.
    #[tokio::test]
    async fn concurrent_takeover_has_exactly_one_winner() {
        let s = ls(LeaseMode::Cas);
        s.try_create(&Lease::granted(P, 1, 1, 0)).await.unwrap();
        let (_, tag_a) = s.get().await.unwrap().unwrap();
        let (_, tag_b) = s.get().await.unwrap().unwrap();
        s.try_swap(&Lease::granted(P, 2, 2, TTL), &tag_a)
            .await
            .unwrap();
        assert!(matches!(
            s.try_swap(&Lease::granted(P, 3, 2, TTL), &tag_b).await,
            Err(StoreError::CasConflict)
        ));
        assert_eq!(s.get().await.unwrap().unwrap().0.holder, 2);
    }

    /// A deposed holder's renew must fail: its tag is stale.
    #[tokio::test]
    async fn stale_tag_renew_is_refused() {
        let s = ls(LeaseMode::Cas);
        s.try_create(&Lease::granted(P, 1, 1, 0)).await.unwrap();
        let (mine, my_tag) = s.get().await.unwrap().unwrap();
        let (_, tag) = s.get().await.unwrap().unwrap();
        s.try_swap(&Lease::granted(P, 2, 2, TTL), &tag)
            .await
            .unwrap();
        assert!(matches!(
            s.try_swap(&mine.renewed(TTL), &my_tag).await,
            Err(StoreError::CasConflict)
        ));
    }

    /// Without `If-Match` a swap cannot be refused; the fallback mode
    /// documents that by overwriting unconditionally.
    #[tokio::test]
    async fn single_writer_mode_swaps_unconditionally() {
        let s = ls(LeaseMode::SingleWriter);
        s.try_create(&Lease::granted(P, 1, 1, TTL)).await.unwrap();
        let (_, tag) = s.get().await.unwrap().unwrap();
        s.try_swap(&Lease::granted(P, 1, 2, TTL), &tag)
            .await
            .unwrap();
        // Same (now stale) tag still succeeds: no enforcement.
        s.try_swap(&Lease::granted(P, 1, 3, TTL), &tag)
            .await
            .unwrap();
        assert_eq!(s.get().await.unwrap().unwrap().0.epoch, 3);
        // Create is still conditional even in this mode.
        assert!(matches!(
            s.try_create(&Lease::granted(P, 2, 1, TTL)).await,
            Err(StoreError::CasConflict)
        ));
    }

    #[tokio::test]
    async fn forward_compatible_decode() {
        let s = ls(LeaseMode::Cas);
        s.store
            .put(
                &layout::lease(P),
                PutPayload::from(
                    br#"{"v":9,"partition":"p0","holder":5,"epoch":3,
                         "expires_unix_ms":1,"released":false,"future":42}"#
                        .to_vec(),
                ),
            )
            .await
            .unwrap();
        let (lease, _) = s.get().await.unwrap().unwrap();
        assert_eq!((lease.holder, lease.epoch), (5, 3));
        // Missing fields default rather than failing the mount.
        let s2 = ls(LeaseMode::Cas);
        s2.store
            .put(&layout::lease(P), PutPayload::from(br#"{}"#.to_vec()))
            .await
            .unwrap();
        let (empty, _) = s2.get().await.unwrap().unwrap();
        assert_eq!(empty.holder, 0);
        assert!(empty.is_claimable(now_unix_ms()));
    }
}
