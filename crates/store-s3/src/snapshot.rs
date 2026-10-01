//! Named snapshot records in `snaps/` (DESIGN.md §13).
//!
//! A snapshot is a name pointing at an immutable root. Since plan 28 the
//! root is a directory inside a published metadata tree — `(commit seq,
//! mtree root, dir ino)` — so taking one costs a forced publish and
//! nothing else: the tree is already on the bucket and a snapshot merely
//! keeps its root alive. This module only owns the small mutable
//! namespace of names.
//! Creation is conditional, so two creators of the same `path@name` cannot
//! silently replace one another.

use crate::{layout, StoreError};
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Plan 32 §0.4 added everything after `tree`, all `#[serde(default)]`:
/// the change is additive, so [`SNAPSHOT_RECORD_VERSION`] stays 2 and a
/// record written before it still reads.
///
/// Every field here is immutable once written, which is why `held`/`held_by`
/// are *not* among them (plan 32 §0.4: "they live only in the row"). A hold
/// is taken and released through the metadata log, which no bucket object
/// participates in; a copy here would be a write-only field that still said
/// `held: true` long after the hold was released, and §0.3's orphan
/// reconciliation would read it as truth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRecord {
    pub v: u32,
    pub name: String,
    pub path: String,
    pub created_unix_ms: i64,
    pub creator: u64,
    /// The directory inside a published metadata tree.
    pub tree: SnapshotTreeRoot,
    /// 0 manual, 1 policy-created.
    #[serde(default)]
    pub origin: u8,
    /// The directory inode carrying the owning policy; 0 for none.
    #[serde(default)]
    pub policy_ino: u64,
    /// The subtree's logical size at creation (REFER), when it was
    /// available.
    #[serde(default)]
    pub refer_bytes: Option<u64>,
}

/// Where a snapshot lives: directory `ino` under the metadata
/// tree `root`, published as commit `seq`. The commit may be retired by
/// retention; `root` is what keeps the nodes alive (it is a GC root).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotTreeRoot {
    pub seq: u64,
    /// Hex node hash.
    pub root: String,
    pub ino: u64,
}

/// The record version this build writes and reads. Version 1 (an
/// `fs-core` directory `Tree` blob) predates plan 28 and is refused.
pub const SNAPSHOT_RECORD_VERSION: u32 = 2;

impl SnapshotRecord {
    /// A record for directory `tree.ino` of a published tree.
    pub fn new(
        path: impl Into<String>,
        name: impl Into<String>,
        creator: u64,
        tree: SnapshotTreeRoot,
    ) -> Self {
        Self {
            v: SNAPSHOT_RECORD_VERSION,
            path: path.into(),
            name: name.into(),
            created_unix_ms: now_unix_ms(),
            creator,
            tree,
            origin: 0,
            policy_ino: 0,
            refer_bytes: None,
        }
    }

    /// Plan 32 §0.4's trailing fields, for a creator that has them. The
    /// hold is deliberately absent — see the type's doc.
    pub fn with_extensions(
        mut self,
        origin: u8,
        policy_ino: u64,
        refer_bytes: Option<u64>,
    ) -> Self {
        self.origin = origin;
        self.policy_ino = policy_ino;
        self.refer_bytes = refer_bytes;
        self
    }

    pub fn id(&self) -> String {
        snapshot_id(&self.path, &self.name)
    }
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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
        // Plan 30 §M4 item 1: a 409 retries; our own create that landed
        // behind a 412 is ours, not "already exists".
        match crate::cas::put_conditional(
            self.store.as_ref(),
            &layout::snapshot(&record.id()),
            body.into(),
            PutMode::Create,
            crate::cas::Verify::Body,
        )
        .await?
        {
            crate::cas::CasPut::Won(_) => Ok(()),
            crate::cas::CasPut::Lost | crate::cas::CasPut::Missing => {
                Err(StoreError::AlreadyExists)
            }
        }
    }

    pub async fn get(&self, id: &str) -> Result<Option<SnapshotRecord>, StoreError> {
        let result = match self.store.get(&layout::snapshot(id)).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let record: SnapshotRecord = serde_json::from_slice(&result.bytes().await?)?;
        if record.v != SNAPSHOT_RECORD_VERSION {
            return Err(StoreError::Meta(format!(
                "snapshot {id} has unsupported version {}",
                record.v
            )));
        }
        Ok(Some(record))
    }

    pub async fn list(&self) -> Result<Vec<SnapshotRecord>, StoreError> {
        let mut stream = self.store.list(Some(&layout::snapshots_prefix()));
        let mut records = Vec::new();
        while let Some(item) = stream.next().await {
            let meta = item?;
            let bytes = self.store.get(&meta.location).await?.bytes().await?;
            let record: SnapshotRecord = serde_json::from_slice(&bytes)?;
            if record.v != SNAPSHOT_RECORD_VERSION {
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
        let record = SnapshotRecord::new(
            "/data",
            "daily",
            7,
            SnapshotTreeRoot {
                seq: 3,
                root: "ab".repeat(32),
                ino: 9,
            },
        );
        snapshots.create(&record).await.unwrap();
        // Second create of the same (path, name) is refused. Use a
        // second record that collides on the same key but differs in
        // content (a real second attempt is never byte-for-byte the
        // first — even a retry of the same command captures a fresh
        // tree root) so this exercises a genuine conflict rather than
        // the "our own create landed behind a 412" recognition.
        let second = SnapshotRecord::new(
            "/data",
            "daily",
            8,
            SnapshotTreeRoot {
                seq: 4,
                root: "cd".repeat(32),
                ino: 9,
            },
        );
        assert_eq!(second.id(), record.id());
        assert!(matches!(
            snapshots.create(&second).await,
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
