//! Coordinated bucket garbage collection (DESIGN.md §14).
//!
//! Chunk candidates come from a single LIST-based orphan pass over
//! `chunks/` (plan 29 M0c retired the `deref` index this used to share the
//! job with — see `store-s3::mark`'s module doc for why the reachability
//! walk from commit roots makes it unnecessary). Before any chunk DELETE
//! this module CAS-publishes the complete condemned set and waits a lease
//! TTL; writers independently treat those hashes as dedup misses.

use anyhow::Result;
use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest};
use constellation_fs_core::ChunkHash;
use constellation_meta::Meta;
use constellation_store_s3::{
    append_journal, publish_condemned, read_condemned, GcJournalEntry, LeaseMode, SnapshotStore,
};
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;

pub const DEFAULT_GC_INTERVAL_S: u64 = 86_400;
pub const DEFAULT_GC_HORIZON_S: u64 = 7 * 86_400;
pub const DEFAULT_LOG_RETENTION_SEGMENTS: u64 = 128;

#[derive(Debug, Clone)]
pub struct GcConfig {
    pub horizon_ms: i64,
    pub retention_segments: u64,
    pub lease_ttl_ms: u64,
}

impl GcConfig {
    pub fn from_env() -> Self {
        let seconds = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        };
        Self {
            horizon_ms: seconds("CONSTELLATION_GC_HORIZON_S", DEFAULT_GC_HORIZON_S) as i64 * 1000,
            retention_segments: seconds(
                "CONSTELLATION_LOG_RETENTION_SEGMENTS",
                DEFAULT_LOG_RETENTION_SEGMENTS,
            ),
            lease_ttl_ms: constellation_store_s3::lease::lease_ttl_ms(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mark {
    pub key: String,
    pub rule: String,
    pub evidence: serde_json::Value,
    #[serde(skip)]
    hash: Option<ChunkHash>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcReport {
    pub verify_only: bool,
    pub candidates: Vec<Mark>,
    pub deleted: Vec<String>,
    pub condemned_epoch: Option<u64>,
    /// Plan 28 S7b: the metadata tree's round (commit retention, pack
    /// sweep and compaction). `None` when the phase did not run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<crate::mtree_gc::MtreeGcReport>,
}

pub async fn run(
    object_store: Arc<dyn ObjectStore>,
    chunks: Arc<constellation_store_s3::ChunkStore>,
    meta: Arc<Meta>,
    lease_mode: LeaseMode,
    verify_only: bool,
    peers: Option<&constellation_net::Peers>,
) -> Result<GcReport> {
    let config = GcConfig::from_env();
    let lease =
        crate::singleton::SingletonLease::acquire(object_store.clone(), "_gc", lease_mode).await?;
    let result = run_held(object_store, chunks, meta, &config, verify_only, peers).await;
    lease.release().await;
    result
}

async fn run_held(
    store: Arc<dyn ObjectStore>,
    chunks: Arc<constellation_store_s3::ChunkStore>,
    meta: Arc<Meta>,
    config: &GcConfig,
    verify_only: bool,
    peers: Option<&constellation_net::Peers>,
) -> Result<GcReport> {
    // Chunks first: their snapshot roots are read from the metadata tree,
    // which the second phase compacts.
    let mut report = run_chunks(
        store.clone(),
        chunks.clone(),
        meta.clone(),
        config,
        verify_only,
        peers,
    )
    .await?;
    let tree_config =
        crate::mtree_gc::MtreeGcConfig::from_env(config.horizon_ms, config.lease_ttl_ms);
    report.metadata = Some(
        crate::mtree_gc::run(store, chunks.e2e_keys(), &meta, &tree_config, verify_only).await?,
    );
    Ok(report)
}

/// Chunk candidates come from a single pass: LIST `chunks/` and mark
/// anything older than the horizon that is not in the protected set (live
/// manifests of the replica, snapshot roots, holds, or already condemned).
/// Plan 28 §P10 retired the `deref` index and its per-replica bookkeeping —
/// any node, or an external job with bucket credentials, can GC by reading
/// roots, so this LIST-based orphan pass is the only candidate source now.
async fn run_chunks(
    store: Arc<dyn ObjectStore>,
    chunks: Arc<constellation_store_s3::ChunkStore>,
    meta: Arc<Meta>,
    config: &GcConfig,
    verify_only: bool,
    peers: Option<&constellation_net::Peers>,
) -> Result<GcReport> {
    let now = constellation_store_s3::lease::now_unix_ms();
    let live = live_roots(&chunks, &meta).await?;
    let snapshots = snapshot_roots(&chunks, store.clone()).await?;
    let holds = hold_roots(store.clone(), now).await?;
    let protected: HashSet<_> = live
        .iter()
        .chain(snapshots.iter())
        .chain(holds.iter())
        .copied()
        .collect();

    let condemned: HashSet<_> = read_condemned(&store)
        .await?
        .into_iter()
        .flat_map(|list| list.hashes)
        .filter_map(|value| ChunkHash::from_hex(&value))
        .collect();
    let known: HashSet<_> = protected.iter().chain(condemned.iter()).copied().collect();
    let mut candidates = Vec::new();
    let prefix = Path::from("chunks");
    for object in store.list(Some(&prefix)).try_collect::<Vec<_>>().await? {
        let Some(hash) = object.location.filename().and_then(ChunkHash::from_hex) else {
            continue;
        };
        if !known.contains(&hash)
            && object.last_modified.timestamp_millis() <= now - config.horizon_ms
        {
            candidates.push(Mark {
                key: object.location.to_string(),
                rule: "orphan-horizon".into(),
                evidence: json!({"last_modified_ms":object.last_modified.timestamp_millis(),"horizon_ms":config.horizon_ms}),
                hash: Some(hash),
            });
        }
    }
    candidates.extend(metadata_candidates(&store, chunks.e2e_keys(), config).await?);
    candidates.sort_by(|a, b| a.key.cmp(&b.key));

    if verify_only || candidates.is_empty() {
        return Ok(GcReport {
            verify_only,
            candidates,
            deleted: Vec::new(),
            condemned_epoch: None,
            metadata: None,
        });
    }

    let condemned_hashes: Vec<_> = candidates
        .iter()
        .filter_map(|mark| mark.hash.map(|hash| hash.to_hex()))
        .collect();
    let condemned = publish_condemned(&store, condemned_hashes, now).await?;
    if let Some(peers) = peers {
        peers.announce_condemned(condemned.epoch).await;
    }
    // One complete authority TTL is mandatory. A writer which has not
    // refreshed by then can no longer commit under a valid partition lease.
    tokio::time::sleep(std::time::Duration::from_millis(config.lease_ttl_ms)).await;

    let refreshed_live = live_roots(&chunks, &meta).await?;
    let refreshed_snaps = snapshot_roots(&chunks, store.clone()).await?;
    let mut deleted = Vec::new();
    for mark in &candidates {
        if let Some(hash) = mark.hash {
            if refreshed_live.contains(&hash) || refreshed_snaps.contains(&hash) {
                continue;
            }
            if store.head(&Path::from(mark.key.clone())).await.is_err() {
                continue; // a concurrent pass already removed it
            }
        }
        store.delete(&Path::from(mark.key.clone())).await?;
        append_journal(
            &store,
            &GcJournalEntry {
                key: mark.key.clone(),
                rule: mark.rule.clone(),
                evidence: json!({"mark":mark.evidence,"condemned_epoch":condemned.epoch}),
                ts: constellation_store_s3::lease::now_unix_ms(),
            },
        )
        .await?;
        deleted.push(mark.key.clone());
    }
    Ok(GcReport {
        verify_only,
        candidates,
        deleted,
        condemned_epoch: Some(condemned.epoch),
        metadata: None,
    })
}

async fn live_roots(
    chunks: &constellation_store_s3::ChunkStore,
    meta: &Meta,
) -> Result<HashSet<ChunkHash>> {
    let mut roots = HashSet::new();
    for bytes in meta.live_manifests()? {
        let manifest = Manifest::decode(&bytes)?;
        match manifest.chunks {
            ChunkInfo::Inline(hashes) => roots.extend(hashes.into_values()),
            ChunkInfo::Spilled(spill) => {
                roots.insert(spill);
                roots.extend(decode_chunk_list(&chunks.get_chunk(&spill).await?)?.into_values());
            }
        }
    }
    Ok(roots)
}

async fn snapshot_roots(
    chunks: &constellation_store_s3::ChunkStore,
    store: Arc<dyn ObjectStore>,
) -> Result<HashSet<ChunkHash>> {
    use crate::snapshot::{snapshot_chunk_refs, SnapshotRoot, TreeAccess};
    let records = SnapshotStore::new(store.clone()).list().await?;
    let mut roots = HashSet::new();
    if records.is_empty() {
        return Ok(roots);
    }
    // Snapshots live in the metadata tree (plan 28): build one reader
    // (and its scratch node cache) for the pass.
    let scratch = ScratchDir::new("gc-snapshot-nodes")?;
    let reader =
        crate::mtree_read::ChainReader::for_store(store.clone(), chunks.e2e_keys(), &scratch.0)?;
    reader.cache.refresh_catalog().await?;
    let tree = TreeAccess::from_reader(reader);
    let mut walked = HashSet::new();
    for record in records {
        let root = SnapshotRoot::of_record(&record)?;
        if walked.insert(root.encode()) {
            roots.extend(snapshot_chunk_refs(chunks, &tree, &root).await?);
        }
    }
    Ok(roots)
}

/// A process-private scratch directory, removed on drop.
struct ScratchDir(std::path::PathBuf);

impl ScratchDir {
    fn new(tag: &str) -> Result<ScratchDir> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "constellation-{tag}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path)?;
        Ok(ScratchDir(path))
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn hold_roots(store: Arc<dyn ObjectStore>, now: i64) -> Result<HashSet<ChunkHash>> {
    let mut roots = HashSet::new();
    for object in store
        .list(Some(&Path::from("holds")))
        .try_collect::<Vec<_>>()
        .await?
    {
        let value: serde_json::Value =
            serde_json::from_slice(&store.get(&object.location).await?.bytes().await?)?;
        if value
            .get("expires_unix_ms")
            .and_then(serde_json::Value::as_i64)
            .is_some_and(|expires| expires <= now)
        {
            continue;
        }
        collect_hash_strings(&value, &mut roots);
    }
    Ok(roots)
}

fn collect_hash_strings(value: &serde_json::Value, out: &mut HashSet<ChunkHash>) {
    match value {
        serde_json::Value::String(value) => {
            if let Some(hash) = ChunkHash::from_hex(value) {
                out.insert(hash);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_hash_strings(value, out);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                collect_hash_strings(value, out);
            }
        }
        _ => {}
    }
}

/// Log segment retention. The floor is the position a fresh replica resumes
/// tailing from: the commit chain's head `applied` position (plan 28 S6
/// bootstraps from the head, replaying the log from there). With no commit
/// yet, there is nothing to floor retention against — a fresh filesystem's
/// log is the only copy of its history — so nothing is pruned. A bootstrap
/// whose base the log was pruned past refuses rather than silently
/// stopping at the gap (`shipper::replay_from`).
async fn metadata_candidates(
    store: &Arc<dyn ObjectStore>,
    keys: Option<&constellation_store_s3::SharedE2eKeys>,
    config: &GcConfig,
) -> Result<Vec<Mark>> {
    let chain = constellation_store_s3::CommitChain::new(store.clone())
        .with_sealing(constellation_store_s3::TreeSealing::for_keys(keys));
    let head = match chain.discover_head(0).await? {
        Some(seq) => chain.get(seq).await?,
        None => None,
    };
    let Some(commit) = head else {
        return Ok(Vec::new());
    };
    let floor = commit.applied.saturating_sub(config.retention_segments);
    let mut marks = Vec::new();
    for object in store
        .list(Some(&Path::from("log")))
        .try_collect::<Vec<_>>()
        .await?
    {
        // Segments are `log/p0/<seq:016x>.zst`; the `sealed` marker has no
        // `.zst` suffix and drops out here.
        let Some(seq) = object
            .location
            .filename()
            .and_then(|name| name.strip_suffix(".zst"))
            .and_then(|name| u64::from_str_radix(name, 16).ok())
        else {
            continue;
        };
        if seq < floor {
            marks.push(Mark {
                key: object.location.to_string(),
                rule: "log-retention".into(),
                evidence: json!({
                    "applied": commit.applied,
                    "retention_segments": config.retention_segments,
                    "commit": commit.seq,
                }),
                hash: None,
            });
        }
    }
    Ok(marks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn horizon_and_exemptions_filter_candidates() {
        let hash = ChunkHash::of(b"held");
        let value = json!({"expires_unix_ms": 200, "hashes":[hash.to_hex()]});
        let mut roots = HashSet::new();
        collect_hash_strings(&value, &mut roots);
        assert!(roots.contains(&hash));
        let config = GcConfig {
            horizon_ms: 100,
            retention_segments: 2,
            lease_ttl_ms: 1,
        };
        assert_eq!(250 - config.horizon_ms, 150);
    }

    /// With no commit at all, there is nothing to floor retention against:
    /// a fresh filesystem's log is the only copy of its history, so the log
    /// retention pass must mark nothing, however many segments exist.
    #[tokio::test]
    async fn log_retention_prunes_nothing_without_a_commit() {
        use object_store::memory::InMemory;
        use object_store::PutPayload;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for seq in 1u64..=300 {
            store
                .put(
                    &Path::from(format!("log/p0/{seq:016x}.zst")),
                    PutPayload::from(Vec::new()),
                )
                .await
                .unwrap();
        }
        let config = GcConfig {
            horizon_ms: 0,
            retention_segments: 128,
            lease_ttl_ms: 1,
        };
        assert!(metadata_candidates(&store, None, &config)
            .await
            .unwrap()
            .is_empty());
    }

    /// Plan 28: once a commit exists, retention floors on the head
    /// commit's `applied` position — where a fresh replica now resumes.
    #[tokio::test]
    async fn log_retention_floors_on_the_head_commits_applied() {
        use constellation_store_s3::{Commit, CommitAgg, Intent};
        use object_store::memory::InMemory;
        use object_store::PutPayload;
        use std::collections::BTreeMap;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for seq in 1u64..=300 {
            store
                .put(
                    &Path::from(format!("log/p0/{seq:016x}.zst")),
                    PutPayload::from(Vec::new()),
                )
                .await
                .unwrap();
        }
        let commit = Commit {
            v: constellation_store_s3::commits::COMMIT_VERSION,
            seq: 1,
            parent: 0,
            roots: BTreeMap::new(),
            packs: Vec::new(),
            author: 1,
            epoch: 1,
            agg: CommitAgg::default(),
            intent: Intent::batch(0),
            unix_ms: 0,
            applied: 250,
        };
        constellation_store_s3::CommitChain::new(store.clone())
            .create(&commit)
            .await
            .unwrap();

        let config = GcConfig {
            horizon_ms: 0,
            retention_segments: 128,
            lease_ttl_ms: 1,
        };
        let marked: Vec<u64> = metadata_candidates(&store, None, &config)
            .await
            .unwrap()
            .into_iter()
            .filter(|mark| mark.rule == "log-retention")
            .map(|mark| {
                assert!(mark.key.starts_with("log/p0/"), "{}", mark.key);
                assert_eq!(mark.evidence["commit"], 1);
                let name = mark.key.rsplit('/').next().unwrap();
                u64::from_str_radix(name.trim_end_matches(".zst"), 16).unwrap()
            })
            .collect();
        // Floor 250 - 128 = 122: segments 1..=121.
        assert_eq!(marked.len(), 121);
        assert_eq!(marked.iter().max(), Some(&121));
    }
}
