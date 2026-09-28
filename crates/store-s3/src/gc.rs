//! Bucket-GC coordination objects (DESIGN.md §14).
//!
//! The condemned pointer closes delete-vs-dedup: it is CAS-published before
//! the grace wait, and every dedup decision (an upload finding its object
//! already there) checks it before relying on the existing object — see
//! [`CondemnedView`] for when, and why reading it *after* the existence
//! answer is as safe as reading it before. The journal is write-new, never
//! overwritten, so fsck can audit every destructive action independently
//! of daemon logs.

use crate::{layout, StoreError};
use constellation_fs_core::ChunkHash;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CondemnedList {
    pub epoch: u64,
    pub hashes: Vec<String>,
    pub published_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcJournalEntry {
    pub key: String,
    pub rule: String,
    pub evidence: serde_json::Value,
    pub ts: i64,
}

pub async fn read_condemned(
    store: &Arc<dyn ObjectStore>,
) -> Result<Option<CondemnedList>, StoreError> {
    match store.get(&layout::gc_condemned()).await {
        Ok(result) => Ok(Some(serde_json::from_slice(&result.bytes().await?)?)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Which state of the condemned pointer an observation saw. Every
/// publication writes a new `(epoch, published_ms)` (the epoch is CAS-
/// advanced; nothing ever deletes the pointer), so two observations with
/// the same identity saw the same publication and no other one landed
/// between them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointerId {
    /// No round has ever published a pointer (it is never deleted).
    Absent,
    Published {
        epoch: u64,
        published_ms: i64,
    },
}

/// One completed read of the pointer.
#[derive(Debug, Clone)]
struct Observation {
    id: PointerId,
    /// For the next read's `If-None-Match` (a `304` costs no body).
    e_tag: Option<String>,
    hashes: Arc<std::collections::HashSet<String>>,
}

/// What a dedup hit may conclude from the pointer (see [`CondemnedView`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupVerdict {
    /// The existing object may stand in for an upload.
    Sound,
    /// A GC round intends to delete it: upload the bytes (an unconditional
    /// PUT, which resurrects it if the delete already happened).
    Condemned,
    /// Undecided: the pointer changed between the last observation made
    /// before the existence answer and the read made after it (or nothing
    /// was observed before). Ask the existence question again now that
    /// the read precedes it (or upload the bytes).
    Unordered,
}

/// The condemned pointer as one upload path sees it: the last completed
/// read, kept so a dedup decision can tell whether the pointer moved
/// around its existence check.
///
/// # When the pointer is read, and why that is safe
///
/// DESIGN.md §14's handshake: a GC round `R` lists candidates, CAS-
/// publishes them as the pointer, waits one lease TTL, tails the log,
/// re-checks liveness, and deletes (each candidate at most once, never
/// one that is not on its own list). The pointer only matters to an
/// upload that finds its object *already there* and wants to rely on it
/// (a dedup hit): that is the one decision a deletion could invalidate.
///
/// Before plan 30's small-file fix every chunk upload GET the pointer
/// first, then asked S3 (a conditional create or a `HEAD`) — two serialized
/// round trips per unique file. Now:
///
/// 1. **The object turned out absent** (the create created it, or the
///    `HEAD` found nothing and the bytes were PUT): the pointer is not
///    read at all. With the object absent, a create and an unconditional
///    PUT have the same effect and the same result, and the pointer's
///    answer only ever chose between those two — so whatever it would
///    have said, the old order would have left the bucket in exactly this
///    state. (And no round deletes the new object: a round deletes each
///    of its candidates once, and an absent candidate was already
///    deleted; a new round lists it with a fresh modification time, far
///    inside `gc.horizon`.)
/// 2. **The object was there** (at time `t_e`): the pointer is read
///    *after* that answer (at `t_r > t_e`), and
///    - it lists the hash → upload the bytes unconditionally, exactly the
///      old path for a condemned hash;
///    - it is absent → no round ever published, so none deleted anything:
///      the object still exists at `t_r`, and "read at `t_r`, then find
///      the object" is an execution of the old order;
///    - it does not list the hash and has the same identity as an
///      observation *completed before the existence request was sent*
///      (`t_s < t_e`) → no publication landed in `(t_s, t_r)`. The round
///      whose list was current throughout does not list the hash, so it
///      deletes nothing of it; earlier rounds finished before that list
///      was published, i.e. before `t_e`, when the object existed. So the
///      object still exists at `t_r`: again an execution of the old
///      order (read at `t_r`, object found right after);
///    - otherwise (the pointer moved, or nothing was observed before) →
///      [`DedupVerdict::Unordered`]: a deletion by a round that a newer
///      round already superseded could have fallen between `t_e` and
///      `t_r`, so the hit is not relied on. The caller asks again — a
///      `HEAD` sent after this read (at `t_h > t_r`): found, that is the
///      old order exactly (read at `t_r`, not condemned, object found
///      after it); absent, it uploads the bytes. (Uploading is never less
///      safe than a hit either: it only makes the object present with a
///      fresher modification time, and GC's choices depend on nothing
///      else.)
///
/// Every execution of the new order is therefore one the old order could
/// produce, so the old order's argument (the horizon, the TTL wait, the
/// post-wait tail and re-check) carries over unchanged. Same premise as
/// before: the deletes of a round happen while its own list is current
/// (the `_gc` singleton lease; the pointer's CAS stops two holders from
/// both advancing the epoch).
///
/// Reads use `If-None-Match` with the last observation's ETag, so an
/// unchanged pointer (the common case: rounds are daily) answers `304`
/// with no body; a store that ignores the header answers `200` and the
/// identity is compared from the body instead.
#[derive(Debug, Default)]
pub struct CondemnedView {
    last: std::sync::Mutex<Option<Observation>>,
    /// Set once a store refused a conditional GET outright: plain GETs.
    unconditional: std::sync::atomic::AtomicBool,
}

impl CondemnedView {
    /// The identity of the last completed read, taken *before* sending
    /// the existence request a later [`Self::verdict`] judges.
    pub fn snapshot(&self) -> Option<PointerId> {
        self.last.lock().unwrap().as_ref().map(|o| o.id)
    }

    /// Read the pointer now (conditionally on the last observation's
    /// ETag) and remember it.
    async fn read(&self, store: &Arc<dyn ObjectStore>) -> Result<Observation, StoreError> {
        let cached = self.last.lock().unwrap().clone();
        let conditional = !self
            .unconditional
            .load(std::sync::atomic::Ordering::Relaxed);
        let if_none_match = cached
            .as_ref()
            .filter(|_| conditional)
            .and_then(|o| o.e_tag.clone());
        let options = object_store::GetOptions {
            if_none_match: if_none_match.clone(),
            ..Default::default()
        };
        let result = match store.get_opts(&layout::gc_condemned(), options).await {
            Err(object_store::Error::NotModified { .. }) => {
                if let Some(cached) = cached {
                    return Ok(cached);
                }
                // A 304 with nothing cached cannot happen (no ETag was
                // sent); read plainly.
                store.get(&layout::gc_condemned()).await
            }
            Err(object_store::Error::NotFound { .. }) => {
                let observed = Observation {
                    id: PointerId::Absent,
                    e_tag: None,
                    hashes: Arc::default(),
                };
                *self.last.lock().unwrap() = Some(observed.clone());
                return Ok(observed);
            }
            Err(_) if if_none_match.is_some() => {
                // Maybe a store that rejects the conditional header: read
                // plainly, and if that works, stop sending it (comparing
                // the body's identity is enough). A plain read failing too
                // is the store's transient error, reported as such.
                let plain = store.get(&layout::gc_condemned()).await;
                if matches!(&plain, Ok(_) | Err(object_store::Error::NotFound { .. })) {
                    self.unconditional
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }
                if let Err(object_store::Error::NotFound { .. }) = plain {
                    let observed = Observation {
                        id: PointerId::Absent,
                        e_tag: None,
                        hashes: Arc::default(),
                    };
                    *self.last.lock().unwrap() = Some(observed.clone());
                    return Ok(observed);
                }
                plain
            }
            other => other,
        }?;
        let e_tag = result.meta.e_tag.clone();
        let list: CondemnedList = serde_json::from_slice(&result.bytes().await?)?;
        let observed = Observation {
            id: PointerId::Published {
                epoch: list.epoch,
                published_ms: list.published_ms,
            },
            e_tag,
            hashes: Arc::new(list.hashes.into_iter().collect()),
        };
        *self.last.lock().unwrap() = Some(observed.clone());
        Ok(observed)
    }

    /// Judge a dedup hit on `hash`: read the pointer now — after the
    /// existence answer — and compare it with `before`, the
    /// [`Self::snapshot`] taken before that request was sent. See the
    /// type's doc for the argument.
    pub async fn verdict(
        &self,
        store: &Arc<dyn ObjectStore>,
        hash: &ChunkHash,
        before: Option<PointerId>,
    ) -> Result<DedupVerdict, StoreError> {
        let now = self.read(store).await?;
        if now.hashes.contains(&hash.to_hex()) {
            return Ok(DedupVerdict::Condemned);
        }
        if now.id == PointerId::Absent || before == Some(now.id) {
            return Ok(DedupVerdict::Sound);
        }
        Ok(DedupVerdict::Unordered)
    }
}

/// CAS-publish a replacement pointer. Concurrent GC holders cannot both
/// advance the epoch even if lease fencing is accidentally bypassed.
pub async fn publish_condemned(
    store: &Arc<dyn ObjectStore>,
    hashes: Vec<String>,
    published_ms: i64,
) -> Result<CondemnedList, StoreError> {
    let (epoch, mode) = match store.get(&layout::gc_condemned()).await {
        Ok(result) => {
            let version = UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            };
            let current: CondemnedList = serde_json::from_slice(&result.bytes().await?)?;
            // The pointer is plain JSON in the bucket: a corrupt or hostile
            // epoch at `u64::MAX` must not wrap to 0 (the epoch is what
            // orders condemned lists) — refuse it instead.
            let epoch = current.epoch.checked_add(1).ok_or_else(|| {
                StoreError::Meta("gc/condemned.json: epoch exhausted; the pointer is corrupt".into())
            })?;
            (epoch, PutMode::Update(version))
        }
        Err(object_store::Error::NotFound { .. }) => (1, PutMode::Create),
        Err(error) => return Err(error.into()),
    };
    let list = CondemnedList {
        epoch,
        hashes,
        published_ms,
    };
    // Plan 30 §M4 item 1 (`crate::cas`): a 409 retries the same attempt,
    // and a 412/404 is a lost race unless the pointer is this very list
    // (epoch, hashes and a millisecond timestamp: our own earlier attempt
    // landed behind a retried 5xx or a lost reply).
    match crate::cas::put_conditional(
        store.as_ref(),
        &layout::gc_condemned(),
        serde_json::to_vec(&list)?.into(),
        mode,
        crate::cas::Verify::Body,
    )
    .await?
    {
        crate::cas::CasPut::Won(_) => Ok(list),
        crate::cas::CasPut::Lost | crate::cas::CasPut::Missing => Err(StoreError::CasConflict),
    }
}

/// Plan 28 S7b: the metadata packs a GC round intends to delete or
/// rewrite, as hex pack hashes (`hashes`). Same handshake as chunks: the
/// list is published before the grace wait, and a tree publisher never
/// deduplicates a node against a pack on it (and re-checks right before
/// its commit CAS). Written only by the `_gc` singleton-lease holder.
pub async fn read_condemned_packs(
    store: &Arc<dyn ObjectStore>,
) -> Result<std::collections::HashSet<crate::packs::PackHash>, StoreError> {
    match store.get(&layout::gc_condemned_packs()).await {
        Ok(result) => {
            let list: CondemnedList = serde_json::from_slice(&result.bytes().await?)?;
            Ok(list
                .hashes
                .iter()
                .filter_map(|hex| crate::packs::PackHash::from_hex(hex))
                .collect())
        }
        Err(object_store::Error::NotFound { .. }) => Ok(Default::default()),
        Err(error) => Err(error.into()),
    }
}

/// Replace the condemned-pack list (an empty set clears it).
pub async fn publish_condemned_packs(
    store: &Arc<dyn ObjectStore>,
    packs: &std::collections::HashSet<crate::packs::PackHash>,
    epoch: u64,
    published_ms: i64,
) -> Result<(), StoreError> {
    let mut hashes: Vec<String> = packs.iter().map(|pack| pack.to_hex()).collect();
    hashes.sort();
    let list = CondemnedList {
        epoch,
        hashes,
        published_ms,
    };
    store
        .put(
            &layout::gc_condemned_packs(),
            PutPayload::from(serde_json::to_vec(&list)?),
        )
        .await?;
    Ok(())
}

/// Plan 29 M3a: the `blobs/*` hashes a GC round intends to delete, as
/// hex blob hashes. Same handshake shape as
/// [`read_condemned_packs`]/[`publish_condemned_packs`]: published
/// before the grace wait, and a tree publisher never references a blob
/// on this list without re-checking right before its commit CAS.
/// Written only by the `_gc` singleton-lease holder.
pub async fn read_condemned_blobs(
    store: &Arc<dyn ObjectStore>,
) -> Result<std::collections::HashSet<constellation_mtree::BlobHash>, StoreError> {
    match store.get(&layout::gc_condemned_blobs()).await {
        Ok(result) => {
            let list: CondemnedList = serde_json::from_slice(&result.bytes().await?)?;
            Ok(list
                .hashes
                .iter()
                .filter_map(|hex| {
                    constellation_mtree::NodeHash::from_hex(hex)
                        .map(|h| constellation_mtree::BlobHash(h.0))
                })
                .collect())
        }
        Err(object_store::Error::NotFound { .. }) => Ok(Default::default()),
        Err(error) => Err(error.into()),
    }
}

/// Replace the condemned-blob list (an empty set clears it).
pub async fn publish_condemned_blobs(
    store: &Arc<dyn ObjectStore>,
    blobs: &std::collections::HashSet<constellation_mtree::BlobHash>,
    epoch: u64,
    published_ms: i64,
) -> Result<(), StoreError> {
    let mut hashes: Vec<String> = blobs
        .iter()
        .map(|hash| constellation_mtree::NodeHash(hash.0).to_hex())
        .collect();
    hashes.sort();
    let list = CondemnedList {
        epoch,
        hashes,
        published_ms,
    };
    store
        .put(
            &layout::gc_condemned_blobs(),
            PutPayload::from(serde_json::to_vec(&list)?),
        )
        .await?;
    Ok(())
}

pub async fn append_journal(
    store: &Arc<dyn ObjectStore>,
    entry: &GcJournalEntry,
) -> Result<(), StoreError> {
    let key = layout::gc_journal(entry.ts, &uuid::Uuid::new_v4().to_string());
    // The key is fresh (a uuid), so an object already there can only be
    // this very write landed behind a lost reply; a 409 (the OVH run's
    // finding 5: some stores answer a conditional write with 409 before
    // settling) retries rather than failing the GC round.
    crate::cas::create_content_addressed(store.as_ref(), &key, serde_json::to_vec(entry)?.into())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    /// The OVH run's finding 5: a 409 on the GC journal's create-if-
    /// absent is retried, not a failed round.
    #[tokio::test]
    async fn a_409_on_the_gc_journal_append_retries() {
        use crate::faulty::{Calls, Fault, FaultyStore, OpKind};
        let faulty = FaultyStore::new();
        let store: Arc<dyn ObjectStore> = faulty.clone();
        faulty.script(OpKind::Put, "gc", Calls::First(2), Fault::Status(409));
        let entry = GcJournalEntry {
            key: "chunks/x".into(),
            rule: "test".into(),
            evidence: serde_json::Value::Null,
            ts: 1,
        };
        append_journal(&store, &entry).await.unwrap();
        assert_eq!(faulty.calls(OpKind::Put, "gc"), 3);
    }

    /// Plan 30 §M4 item 1: the condemned pointer's CAS under each error
    /// code. A 409 is retried; a publish that landed behind a 412 is ours;
    /// a genuine 412 (another GC round's pointer) is a conflict; a 500 is
    /// the store's error.
    #[tokio::test]
    async fn condemned_pointer_cas_error_codes() {
        use crate::faulty::{Calls, Fault, FaultyStore, OpKind};
        let faulty = FaultyStore::new();
        let store: Arc<dyn ObjectStore> = faulty.clone();
        faulty.script(OpKind::Put, "condemned", Calls::Nth(1), Fault::Status(409));
        assert_eq!(publish_condemned(&store, vec![], 1).await.unwrap().epoch, 1);
        faulty.clear();
        faulty.script(
            OpKind::Put,
            "condemned",
            Calls::Nth(1),
            Fault::AppliedThen(412),
        );
        assert_eq!(publish_condemned(&store, vec![], 2).await.unwrap().epoch, 2);
        faulty.clear();
        faulty.script(OpKind::Put, "condemned", Calls::Nth(1), Fault::Status(412));
        assert!(matches!(
            publish_condemned(&store, vec![], 3).await,
            Err(StoreError::CasConflict)
        ));
        faulty.clear();
        faulty.script(OpKind::Put, "condemned", Calls::Nth(1), Fault::Status(500));
        assert!(matches!(
            publish_condemned(&store, vec![], 4).await,
            Err(StoreError::ObjectStore(_))
        ));
        assert_eq!(read_condemned(&store).await.unwrap().unwrap().epoch, 2);
    }

    #[tokio::test]
    async fn condemned_pointer_advances_and_is_queryable() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let hash = ChunkHash::of(b"race");
        let first = publish_condemned(&store, vec![hash.to_hex()], 10)
            .await
            .unwrap();
        let second = publish_condemned(&store, Vec::new(), 20).await.unwrap();
        assert_eq!(first.epoch, 1);
        assert_eq!(second.epoch, 2);
        let view = CondemnedView::default();
        assert_eq!(
            view.verdict(&store, &hash, None).await.unwrap(),
            DedupVerdict::Unordered,
            "nothing observed before the existence answer"
        );
        let before = view.snapshot();
        assert_eq!(
            view.verdict(&store, &hash, before).await.unwrap(),
            DedupVerdict::Sound
        );
    }
}
