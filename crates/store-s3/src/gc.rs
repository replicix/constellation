//! Bucket-GC coordination objects (DESIGN.md §14).
//!
//! The condemned pointer closes delete-vs-dedup: it is CAS-published before
//! the grace wait, and every upload checks it before deciding an existing
//! object is a usable dedup hit. The journal is write-new, never overwritten,
//! so fsck can audit every destructive action independently of daemon logs.

use crate::{layout, StoreError};
use constellation_fs_core::ChunkHash;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
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
    match store
        .put_opts(
            &layout::gc_condemned(),
            PutPayload::from(serde_json::to_vec(&list)?),
            PutOptions::from(mode),
        )
        .await
    {
        Ok(_) => Ok(list),
        Err(object_store::Error::AlreadyExists { .. })
        | Err(object_store::Error::Precondition { .. })
        | Err(object_store::Error::NotModified { .. }) => Err(StoreError::CasConflict),
        Err(error) => Err(error.into()),
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

pub async fn append_journal(
    store: &Arc<dyn ObjectStore>,
    entry: &GcJournalEntry,
) -> Result<(), StoreError> {
    let key = layout::gc_journal(entry.ts, &uuid::Uuid::new_v4().to_string());
    store
        .put_opts(
            &key,
            PutPayload::from(serde_json::to_vec(entry)?),
            PutOptions::from(PutMode::Create),
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

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
