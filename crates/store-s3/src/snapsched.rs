//! The snapshot scheduler's audit trail in the bucket (plan 32 Step 4.1
//! item 4): one object per scheduler tick that did something,
//! `snapsched/journal/<ts>-<nonce>.json`, written create-if-absent the
//! way the GC journal is (`crate::gc::append_journal`), so a lost reply
//! retried lands on its own object and never overwrites another.
//!
//! An entry names, per policy root the tick touched, the root's inode,
//! its path then, the canonical policy, and what happened: the snapshots
//! it created, the creations it skipped (an unchanged subtree, or a name
//! another leader already took), the creations that failed, and the
//! snapshots it deleted. The scheduler's creation step (plan 32 M3)
//! writes `created`/`skipped`/`failed`; expiry (M4) adds `deleted` to the
//! same object. Nothing in the system reads these back to decide
//! anything: they are the record an operator (or a harness oracle)
//! replays retention against.

use crate::error::StoreError;
use object_store::path::Path;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// One tick's record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapSchedJournalEntry {
    /// When the tick ran, by the leader's clock (Unix ms).
    pub ts: i64,
    /// The leading node.
    pub node: u64,
    pub roots: Vec<SnapSchedJournalRoot>,
}

/// What one tick did to one policy root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapSchedJournalRoot {
    pub root_ino: u64,
    /// The root's path at the tick (it may be renamed later; the inode is
    /// the identity).
    pub path: String,
    /// The policy in canonical form.
    pub policy: String,
    #[serde(default)]
    pub created: Vec<SnapSchedJournalSnap>,
    #[serde(default)]
    pub skipped: Vec<SnapSchedJournalSkip>,
    #[serde(default)]
    pub failed: Vec<SnapSchedJournalSkip>,
    #[serde(default)]
    pub deleted: Vec<SnapSchedJournalSnap>,
}

/// A snapshot created (or deleted) by the scheduler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapSchedJournalSnap {
    pub id: String,
    pub name: String,
    pub created_unix_ms: i64,
}

/// A creation that did not produce a snapshot, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapSchedJournalSkip {
    pub name: String,
    pub reason: String,
}

/// `snapsched/journal/<ts as 16 hex digits>-<nonce>.json`: lists in time
/// order, like `gc/journal/`.
pub fn journal_key(ts: i64, nonce: &str) -> Path {
    Path::from(format!("snapsched/journal/{ts:016x}-{nonce}.json"))
}

pub fn journal_prefix() -> Path {
    Path::from("snapsched/journal")
}

/// Write `entry` under a fresh key, create-if-absent (a 409 retries; an
/// object already there can only be this write landed behind a lost
/// reply). Returns the key.
pub async fn append_journal(
    store: &Arc<dyn ObjectStore>,
    entry: &SnapSchedJournalEntry,
) -> Result<Path, StoreError> {
    let key = journal_key(entry.ts, &uuid::Uuid::new_v4().simple().to_string());
    crate::cas::create_content_addressed(store.as_ref(), &key, serde_json::to_vec(entry)?.into())
        .await?;
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt;

    #[tokio::test]
    async fn entries_round_trip_and_list_in_time_order() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let entry = |ts| SnapSchedJournalEntry {
            ts,
            node: 3,
            roots: vec![SnapSchedJournalRoot {
                root_ino: 42,
                path: "/proj".into(),
                policy: "10s:1m".into(),
                created: vec![SnapSchedJournalSnap {
                    id: "i".into(),
                    name: "auto-20260928T140510Z".into(),
                    created_unix_ms: ts,
                }],
                skipped: vec![],
                failed: vec![],
                deleted: vec![],
            }],
        };
        let late = append_journal(&store, &entry(0x2000)).await.unwrap();
        let early = append_journal(&store, &entry(0x1000)).await.unwrap();
        let mut keys: Vec<Path> = store
            .list(Some(&journal_prefix()))
            .map_ok(|m| m.location)
            .try_collect()
            .await
            .unwrap();
        keys.sort();
        assert_eq!(keys, vec![early.clone(), late]);
        let body = store.get(&early).await.unwrap().bytes().await.unwrap();
        let back: SnapSchedJournalEntry = serde_json::from_slice(&body).unwrap();
        assert_eq!(back, entry(0x1000));
    }
}
