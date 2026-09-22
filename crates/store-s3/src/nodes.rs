//! Node registry: each mounting node claims a small, cluster-unique
//! integer id with a CAS-create on `nodes/<id>` (DESIGN.md §2). The id
//! scopes ino allocation (`Meta::set_node_prefix`) and marks log
//! segment origin, so it must never be shared by two live state dirs.
//!
//! From M3.3 the record also carries the host's P2P identity: its
//! `pubkey` (the accept-time allowlist — enrolling requires bucket
//! write, so IAM stays the trust root) and `p2p_addr` (how peers dial
//! it). Both are refreshed by [`publish_p2p`] at mount and periodically.
//!
//! Refreshing is a plain overwrite PUT rather than a CAS swap: a node id
//! is owned by exactly one live state dir (that is the invariant the
//! CAS-create establishes), so the node itself is the only writer of its
//! own record and there is nothing to race with. The CAS on *create* is
//! what matters and is unchanged.
//!
//! # Leave / retirement (phase 4c)
//!
//! Permanent departure is a **tombstone**, not a DELETE:
//! `nodes/<id>.json` stays with `{retired: true, ...}` so
//! [`claim_node_id`] never reuses the numeric id (old log segments and
//! ino prefixes still carry it). [`write_eligible_roster`] and
//! [`list_nodes`] omit retired records. Unmount without [`leave_node`]
//! is a temporary departure — the live record stays and still blocks
//! continuation epochs until an operator retires it.

use crate::error::StoreError;
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: u64,
    pub hostname: String,
    pub created_unix: i64,
    /// Hex Ed25519 host key. Absent on pre-M3.3 records, which simply
    /// means that node has no P2P fast path (it still works over S3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pubkey: Option<String>,
    /// Serialized iroh `EndpointAddr` telling peers how to dial.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p2p_addr: Option<serde_json::Value>,
    /// When the P2P fields were last refreshed, for staleness checks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p2p_updated_unix: Option<i64>,
    /// Running binary version (`git describe` / package version). Absent
    /// on older records; refreshed with [`publish_p2p`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Read-only member: enrolled in the registry but not write-eligible
    /// (DESIGN.md §5.3). Continuation epochs ignore RO nodes when
    /// checking that the live P2P component covers the roster.
    ///
    /// Defaulted so a record written by a binary that predates this
    /// field still decodes as write-eligible — the conservative reading.
    /// The roster's safety does not rest on this default: see
    /// [`write_eligible_roster`], which refuses to answer at all if any
    /// record fails to parse.
    #[serde(default)]
    pub ro: bool,
    /// Permanent departure tombstone (phase 4c). The object stays so the
    /// numeric id is never recycled; epochs and P2P ignore it.
    #[serde(default)]
    pub retired: bool,
    /// When the record was retired; absent on live members.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired_unix: Option<i64>,
}

impl NodeInfo {
    pub fn is_live(&self) -> bool {
        !self.retired
    }

    pub fn is_write_eligible(&self) -> bool {
        !self.retired && !self.ro
    }
}

fn node_key(id: u64) -> object_store::path::Path {
    object_store::path::Path::from(format!("nodes/{id:08x}.json"))
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Claim the next free node id (>= 1). Uses `max(taken)+1` over every
/// registry stem — including retired tombstones — so a middle-id leave
/// never recycles a hole onto a different host.
pub async fn claim_node_id(store: Arc<dyn ObjectStore>) -> Result<u64, StoreError> {
    let prefix = object_store::path::Path::from("nodes");
    let taken: Vec<u64> = store
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter_map(|m| {
            let name = m.location.filename()?.strip_suffix(".json")?.to_string();
            u64::from_str_radix(&name, 16).ok()
        })
        .collect();
    let mut candidate = taken.iter().copied().max().unwrap_or(0) + 1;
    loop {
        let info = NodeInfo {
            node_id: candidate,
            hostname: hostname(),
            created_unix: now_unix(),
            pubkey: None,
            p2p_addr: None,
            p2p_updated_unix: None,
            version: None,
            ro: false,
            retired: false,
            retired_unix: None,
        };
        let body = serde_json::to_vec(&info)?;
        match store
            .put_opts(
                &node_key(candidate),
                PutPayload::from(body),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => return Ok(candidate),
            Err(object_store::Error::AlreadyExists { .. }) => candidate += 1,
            Err(e) => return Err(e.into()),
        }
    }
}

/// Cluster-unique node ids currently claimed in the registry (live and
/// retired). Used as a cheap "is anyone else here?" signal for the split
/// heuristic (a single-node filesystem must never split).
pub async fn list_node_ids(store: Arc<dyn ObjectStore>) -> Result<Vec<u64>, StoreError> {
    let prefix = object_store::path::Path::from("nodes");
    let mut ids: Vec<u64> = store
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter_map(|m| {
            let name = m.location.filename()?.strip_suffix(".json")?.to_string();
            u64::from_str_radix(&name, 16).ok()
        })
        .collect();
    ids.sort_unstable();
    Ok(ids)
}

/// Fetch one registry record. `Ok(None)` if the object is absent.
pub async fn get_node(
    store: Arc<dyn ObjectStore>,
    node_id: u64,
) -> Result<Option<NodeInfo>, StoreError> {
    match store.get(&node_key(node_id)).await {
        Ok(r) => {
            let bytes = r.bytes().await?;
            Ok(Some(serde_json::from_slice(&bytes)?))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Permanently retire `node_id` from the write-eligible roster.
///
/// Writes a tombstone (`retired: true`) rather than deleting the object
/// so the numeric id stays reserved. Idempotent: retiring an already-
/// retired record or a missing record is `Ok` (a missing record means
/// the id was never claimed or was deleted by hand — treating it as
/// success keeps admin leave safe to retry).
pub async fn leave_node(store: Arc<dyn ObjectStore>, node_id: u64) -> Result<(), StoreError> {
    let key = node_key(node_id);
    let existing = match store.get(&key).await {
        Ok(r) => {
            let bytes = r.bytes().await?;
            Some(serde_json::from_slice::<NodeInfo>(&bytes)?)
        }
        Err(object_store::Error::NotFound { .. }) => None,
        Err(e) => return Err(e.into()),
    };
    let Some(existing) = existing else {
        return Ok(());
    };
    if existing.retired {
        return Ok(());
    }
    let tombstone = NodeInfo {
        retired: true,
        retired_unix: Some(now_unix()),
        // Drop dialability so peers stop trying.
        pubkey: None,
        p2p_addr: None,
        p2p_updated_unix: None,
        ..existing
    };
    store
        .put(&key, PutPayload::from(serde_json::to_vec(&tombstone)?))
        .await?;
    Ok(())
}

/// The write-eligible roster: every live non-RO node in the registry.
///
/// Unlike [`list_nodes`] this **fails closed**. A continuation epoch is
/// legal only when the live P2P component covers every write-eligible
/// node (DESIGN.md §5.3), so a roster that silently omits a node is not
/// a degraded answer — it is a wrong one that authorizes writing while
/// an unaccounted-for node may also be writing. Any object under
/// `nodes/` that cannot be read or parsed therefore aborts the whole
/// roster: epoch activation stops (writes freeze, the safe direction)
/// instead of proceeding against an incomplete membership view.
///
/// Retired tombstones are parseable and deliberately omitted — they are
/// not writers.
pub async fn write_eligible_roster(store: Arc<dyn ObjectStore>) -> Result<Vec<u64>, StoreError> {
    let prefix = object_store::path::Path::from("nodes");
    let metas = store.list(Some(&prefix)).try_collect::<Vec<_>>().await?;
    let mut out = Vec::new();
    for m in metas {
        // Only objects whose key is a node id are registry records.
        let is_node_record = m
            .location
            .filename()
            .and_then(|f| f.strip_suffix(".json"))
            .is_some_and(|stem| u64::from_str_radix(stem, 16).is_ok());
        if !is_node_record {
            continue;
        }
        let bytes = store.get(&m.location).await?.bytes().await?;
        let info: NodeInfo = serde_json::from_slice(&bytes).map_err(|e| {
            StoreError::Registry(format!(
                "unparseable node record {}: {e}; refusing to derive a \
                 write-eligible roster from an incomplete registry",
                m.location
            ))
        })?;
        if info.is_write_eligible() {
            out.push(info.node_id);
        }
    }
    out.sort_unstable();
    Ok(out)
}

/// Every **live** node record in the registry. Unparseable and retired
/// records are skipped rather than failing the whole listing: one bad
/// object must not stop the P2P layer from finding its peers, and a
/// tombstone is not dialable.
///
/// Do **not** use this to derive the continuation-epoch roster — see
/// [`write_eligible_roster`], which fails closed instead.
pub async fn list_nodes(store: Arc<dyn ObjectStore>) -> Result<Vec<NodeInfo>, StoreError> {
    let prefix = object_store::path::Path::from("nodes");
    let metas = store.list(Some(&prefix)).try_collect::<Vec<_>>().await?;
    let mut out = Vec::new();
    for m in metas {
        let Ok(res) = store.get(&m.location).await else {
            continue;
        };
        let Ok(bytes) = res.bytes().await else {
            continue;
        };
        match serde_json::from_slice::<NodeInfo>(&bytes) {
            Ok(info) if info.is_live() => out.push(info),
            Ok(_) => {} // retired tombstone: not a peer
            // A corrupt or future-format record must not hide every
            // other peer; the caller degrades to the S3 path for it.
            Err(_) => continue,
        }
    }
    out.sort_by_key(|n| n.node_id);
    Ok(out)
}

/// Refresh this node's P2P identity in its own registry record.
///
/// Plain overwrite PUT: see the module doc — a node id belongs to one
/// live state dir, so this node is the only writer of this key. The
/// record is re-read first so a concurrent schema addition by a newer
/// binary is not clobbered by an older one's narrower view... which it
/// would be anyway, so instead we preserve the immutable identity fields
/// (`created_unix`, `hostname`) and only rewrite the P2P ones plus
/// [`NodeInfo::version`].
///
/// Refuses if the record is retired or missing: a departed / admin-
/// removed node must not silently reappear as live.
pub async fn publish_p2p(
    store: Arc<dyn ObjectStore>,
    node_id: u64,
    pubkey_hex: &str,
    addr: serde_json::Value,
    version: &str,
) -> Result<(), StoreError> {
    let key = node_key(node_id);
    let existing: NodeInfo = match store.get(&key).await {
        Ok(r) => {
            let bytes = r.bytes().await?;
            serde_json::from_slice(&bytes)?
        }
        Err(object_store::Error::NotFound { .. }) => {
            return Err(StoreError::Registry(format!(
                "node {node_id} has no registry record; remount with a fresh \
                 state dir (or --rejoin) rather than reclaiming a deleted id"
            )));
        }
        Err(e) => return Err(e.into()),
    };
    if existing.retired {
        return Err(StoreError::Registry(format!(
            "node {node_id} is retired; cannot publish P2P identity for a left member"
        )));
    }
    let info = NodeInfo {
        node_id,
        hostname: existing.hostname,
        created_unix: existing.created_unix,
        pubkey: Some(pubkey_hex.to_string()),
        p2p_addr: Some(addr),
        p2p_updated_unix: Some(now_unix()),
        version: Some(version.to_string()),
        ro: existing.ro,
        retired: false,
        retired_unix: None,
    };
    store
        .put(&key, PutPayload::from(serde_json::to_vec(&info)?))
        .await?;
    Ok(())
}

/// Mark this node read-only in the registry (or clear the flag).
/// Same overwrite-PUT rule as [`publish_p2p`]: the node id is owned by
/// one live state dir, so this node is the only writer of this key.
pub async fn publish_ro(
    store: Arc<dyn ObjectStore>,
    node_id: u64,
    ro: bool,
) -> Result<(), StoreError> {
    let key = node_key(node_id);
    let existing: Option<NodeInfo> = match store.get(&key).await {
        Ok(r) => r
            .bytes()
            .await
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok()),
        Err(object_store::Error::NotFound { .. }) => None,
        Err(e) => return Err(e.into()),
    };
    if existing.as_ref().is_some_and(|i| i.retired) {
        return Err(StoreError::Registry(format!(
            "node {node_id} is retired; cannot change membership flags"
        )));
    }
    let info = NodeInfo {
        node_id,
        hostname: existing
            .as_ref()
            .map(|i| i.hostname.clone())
            .unwrap_or_else(hostname),
        created_unix: existing
            .as_ref()
            .map(|i| i.created_unix)
            .unwrap_or_else(now_unix),
        pubkey: existing.as_ref().and_then(|i| i.pubkey.clone()),
        p2p_addr: existing.as_ref().and_then(|i| i.p2p_addr.clone()),
        p2p_updated_unix: existing.as_ref().and_then(|i| i.p2p_updated_unix),
        version: existing.as_ref().and_then(|i| i.version.clone()),
        ro,
        retired: false,
        retired_unix: None,
    };
    store
        .put(&key, PutPayload::from(serde_json::to_vec(&info)?))
        .await?;
    Ok(())
}

fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn ids_are_unique_and_monotonic() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = claim_node_id(store.clone()).await.unwrap();
        let b = claim_node_id(store.clone()).await.unwrap();
        let c = claim_node_id(store).await.unwrap();
        assert_eq!((a, b, c), (1, 2, 3));
    }

    /// Publishing P2P identity must keep the immutable fields the
    /// CAS-create established, and must be re-runnable (it refreshes on
    /// a timer).
    #[tokio::test]
    async fn publish_p2p_updates_only_the_p2p_fields() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = claim_node_id(store.clone()).await.unwrap();
        let before = list_nodes(store.clone()).await.unwrap();
        assert_eq!(before.len(), 1);
        assert!(before[0].pubkey.is_none(), "no P2P identity yet");

        let addr = serde_json::json!({"id": "abc", "addrs": []});
        publish_p2p(
            store.clone(),
            id,
            "aa".repeat(32).as_str(),
            addr.clone(),
            "1.0.0-test",
        )
        .await
        .unwrap();
        let after = list_nodes(store.clone()).await.unwrap();
        assert_eq!(after.len(), 1, "must not create a second record");
        assert_eq!(after[0].node_id, id);
        assert_eq!(after[0].hostname, before[0].hostname);
        assert_eq!(after[0].created_unix, before[0].created_unix);
        assert_eq!(after[0].pubkey.as_deref(), Some("aa".repeat(32).as_str()));
        assert_eq!(after[0].p2p_addr, Some(addr));
        assert_eq!(after[0].version.as_deref(), Some("1.0.0-test"));
        assert!(after[0].p2p_updated_unix.is_some());

        // Refresh again: still exactly one record.
        publish_p2p(
            store.clone(),
            id,
            "bb".repeat(32).as_str(),
            serde_json::json!({"id": "def", "addrs": []}),
            "1.0.1-test",
        )
        .await
        .unwrap();
        let again = list_nodes(store.clone()).await.unwrap();
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].pubkey.as_deref(), Some("bb".repeat(32).as_str()));
        assert_eq!(again[0].version.as_deref(), Some("1.0.1-test"));
        // And the id stays claimed, so a new node gets the next one.
        assert_eq!(claim_node_id(store).await.unwrap(), id + 1);
    }

    /// A record without the newer P2P fields must still parse: absent
    /// fields simply mean that node has no fast path. The fixture is
    /// deliberately the original on-disk shape — `ro` is defaulted, not
    /// required, so this stays a real test of a narrower record.
    #[tokio::test]
    async fn records_without_p2p_fields_still_parse() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(
                &node_key(7),
                PutPayload::from(br#"{"node_id":7,"hostname":"old","created_unix":123}"#.to_vec()),
            )
            .await
            .unwrap();
        let nodes = list_nodes(store).await.unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].node_id, 7);
        assert!(nodes[0].pubkey.is_none());
        assert!(!nodes[0].ro, "an absent ro flag means write-eligible");
        assert!(!nodes[0].retired);
    }

    /// The roster must fail CLOSED. A corrupt record that silently
    /// shrinks it would let `component_covers_roster` authorize a
    /// continuation epoch while an unaccounted-for node may be writing.
    #[tokio::test]
    async fn roster_refuses_an_unreadable_registry() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let good = claim_node_id(store.clone()).await.unwrap();
        assert_eq!(write_eligible_roster(store.clone()).await.unwrap(), [good]);

        store
            .put(&node_key(99), PutPayload::from(b"{ truncated".to_vec()))
            .await
            .unwrap();
        // The tolerant listing still degrades gracefully...
        assert_eq!(list_nodes(store.clone()).await.unwrap().len(), 1);
        // ...but the roster refuses to answer at all.
        let err = write_eligible_roster(store)
            .await
            .expect_err("a corrupt record must abort the roster, not shrink it");
        assert!(
            matches!(err, StoreError::Registry(_)),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn roster_excludes_read_only_members() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = claim_node_id(store.clone()).await.unwrap();
        let reader = claim_node_id(store.clone()).await.unwrap();
        publish_ro(store.clone(), reader, true).await.unwrap();
        assert_eq!(write_eligible_roster(store).await.unwrap(), [writer]);
    }

    #[tokio::test]
    async fn publish_ro_is_sticky_across_p2p_refresh() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = claim_node_id(store.clone()).await.unwrap();
        publish_ro(store.clone(), id, true).await.unwrap();
        publish_p2p(
            store.clone(),
            id,
            "aa".repeat(32).as_str(),
            serde_json::json!({"id": "abc", "addrs": []}),
            "1.0.0-test",
        )
        .await
        .unwrap();
        let nodes = list_nodes(store).await.unwrap();
        assert!(nodes[0].ro, "P2P refresh must not clear the RO flag");
        assert_eq!(nodes[0].version.as_deref(), Some("1.0.0-test"));
    }

    /// One corrupt object must not hide every other peer.
    #[tokio::test]
    async fn unparseable_record_is_skipped() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        claim_node_id(store.clone()).await.unwrap();
        store
            .put(&node_key(99), PutPayload::from(b"{ not json".to_vec()))
            .await
            .unwrap();
        let nodes = list_nodes(store).await.unwrap();
        assert_eq!(nodes.len(), 1, "good record still listed");
        assert_eq!(nodes[0].node_id, 1);
    }

    #[tokio::test]
    async fn leave_node_tombstones_and_shrinks_the_roster() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = claim_node_id(store.clone()).await.unwrap();
        let b = claim_node_id(store.clone()).await.unwrap();
        let c = claim_node_id(store.clone()).await.unwrap();
        assert_eq!(
            write_eligible_roster(store.clone()).await.unwrap(),
            [a, b, c]
        );

        leave_node(store.clone(), b).await.unwrap();
        leave_node(store.clone(), b).await.unwrap(); // idempotent

        assert_eq!(write_eligible_roster(store.clone()).await.unwrap(), [a, c]);
        assert_eq!(
            list_nodes(store.clone())
                .await
                .unwrap()
                .iter()
                .map(|n| n.node_id)
                .collect::<Vec<_>>(),
            [a, c],
            "P2P listing omits the tombstone"
        );
        let tomb = get_node(store.clone(), b).await.unwrap().unwrap();
        assert!(tomb.retired);
        assert!(tomb.retired_unix.is_some());
        assert!(tomb.p2p_addr.is_none());

        // Middle-id leave must not recycle the hole.
        let d = claim_node_id(store).await.unwrap();
        assert_eq!(d, c + 1, "retired id {b} must stay reserved");
    }

    #[tokio::test]
    async fn leave_of_missing_record_is_ok() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        leave_node(store, 42).await.unwrap();
    }

    #[tokio::test]
    async fn publish_p2p_refuses_a_retired_record() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = claim_node_id(store.clone()).await.unwrap();
        leave_node(store.clone(), id).await.unwrap();
        let err = publish_p2p(
            store,
            id,
            "aa".repeat(32).as_str(),
            serde_json::json!({"id": "x"}),
            "1.0.0-test",
        )
        .await
        .unwrap_err();
        assert!(matches!(err, StoreError::Registry(_)), "{err}");
    }
}
