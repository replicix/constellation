//! Bucket-GC coordination objects (DESIGN.md §14).
//!
//! The condemned pointer closes delete-vs-dedup: it is CAS-published before
//! the grace wait, and every upload checks it before deciding an existing
//! object is a usable dedup hit. The journal is write-new, never overwritten,
//! so fsck can audit every destructive action independently of daemon logs.

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

pub async fn is_condemned(
    store: &Arc<dyn ObjectStore>,
    hash: &ChunkHash,
) -> Result<bool, StoreError> {
    let Some(list) = read_condemned(store).await? else {
        return Ok(false);
    };
    let needle = hash.to_hex();
    Ok(list.hashes.iter().any(|candidate| candidate == &needle))
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
            (current.epoch + 1, PutMode::Update(version))
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
        assert!(!is_condemned(&store, &hash).await.unwrap());
    }
}
