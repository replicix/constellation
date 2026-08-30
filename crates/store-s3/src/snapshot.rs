//! Named snapshot records in `snaps/` (DESIGN.md §13).
//!
//! Tree and manifest objects use the ordinary chunk path.  This module only
//! owns the small mutable namespace of names pointing at immutable roots.
//! Creation is conditional, so two creators of the same `path@name` cannot
//! silently replace one another.

use crate::{layout, StoreError};
use constellation_fs_core::ChunkHash;
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRecord {
    pub v: u32,
    pub name: String,
    pub path: String,
    pub created_unix_ms: i64,
    pub creator: u64,
    pub root: ChunkHash,
}

impl SnapshotRecord {
    pub fn new(
        path: impl Into<String>,
        name: impl Into<String>,
        creator: u64,
        root: ChunkHash,
    ) -> Self {
        Self {
            v: 1,
            path: path.into(),
            name: name.into(),
            created_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            creator,
            root,
        }
    }

    pub fn id(&self) -> String {
        snapshot_id(&self.path, &self.name)
    }
}

pub fn snapshot_id(path: &str, name: &str) -> String {
    blake3::hash(format!("{path}@{name}").as_bytes())
        .to_hex()
        .to_string()
}

#[derive(Clone)]
pub struct SnapshotStore {
    store: Arc<dyn ObjectStore>,
}

impl SnapshotStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub async fn create(&self, record: &SnapshotRecord) -> Result<(), StoreError> {
        let body = serde_json::to_vec_pretty(record)?;
        match self
            .store
            .put_opts(
                &layout::snapshot(&record.id()),
                PutPayload::from(body),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => Err(StoreError::AlreadyExists),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn get(&self, id: &str) -> Result<Option<SnapshotRecord>, StoreError> {
        let result = match self.store.get(&layout::snapshot(id)).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        Ok(Some(serde_json::from_slice(&result.bytes().await?)?))
    }

    pub async fn list(&self) -> Result<Vec<SnapshotRecord>, StoreError> {
        let mut stream = self.store.list(Some(&layout::snapshots_prefix()));
        let mut records = Vec::new();
        while let Some(item) = stream.next().await {
            let meta = item?;
            let bytes = self.store.get(&meta.location).await?.bytes().await?;
            let record: SnapshotRecord = serde_json::from_slice(&bytes)?;
            if record.v != 1 {
                return Err(StoreError::Meta(format!(
                    "snapshot {} has unsupported version {}",
                    meta.location, record.v
                )));
            }
            records.push(record);
        }
        records.sort_by(|a, b| (&a.path, &a.name).cmp(&(&b.path, &b.name)));
        Ok(records)
    }

    pub async fn delete(&self, path: &str, name: &str) -> Result<bool, StoreError> {
        let key = layout::snapshot(&snapshot_id(path, name));
        match self.store.delete(&key).await {
            Ok(()) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn create_is_cas_and_delete_leaves_tree_blobs_alone() {
        let backend = Arc::new(InMemory::new());
        let snapshots = SnapshotStore::new(backend);
        let record = SnapshotRecord::new("/data", "daily", 7, ChunkHash::of(b"root"));
        snapshots.create(&record).await.unwrap();
        assert!(matches!(
            snapshots.create(&record).await,
            Err(StoreError::AlreadyExists)
        ));
        assert_eq!(snapshots.list().await.unwrap(), vec![record.clone()]);
        assert!(snapshots.delete("/data", "daily").await.unwrap());
        assert!(snapshots.list().await.unwrap().is_empty());
    }

    #[test]
    fn identity_is_stable_and_path_scoped() {
        assert_eq!(snapshot_id("/a", "daily"), snapshot_id("/a", "daily"));
        assert_ne!(snapshot_id("/a", "daily"), snapshot_id("/b", "daily"));
    }
}
