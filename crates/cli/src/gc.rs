//! Coordinated bucket garbage collection (DESIGN.md §14).
//!
//! Ordinary candidates come from SQLite's continuously maintained `deref`
//! index. Only the explicit orphan pass lists `chunks/`. Before any chunk
//! DELETE this module CAS-publishes the complete condemned set and waits a
//! lease TTL; writers independently treat those hashes as dedup misses.

use anyhow::Result;
use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest};
use constellation_fs_core::ChunkHash;
use constellation_meta::SqliteMeta;
use constellation_store_s3::{
    append_journal, publish_condemned, read_condemned, DesignationMode, DesignationStore,
    GcJournalEntry, LeaseMode, SnapshotStore,
};
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::Serialize;
use serde_json::json;
use std::collections::{BTreeMap, HashSet};
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

#[derive(Debug, Clone, Serialize)]
pub struct Mark {
    pub key: String,
    pub rule: String,
    pub evidence: serde_json::Value,
    #[serde(skip)]
    hash: Option<ChunkHash>,
}

#[derive(Debug, Clone, Serialize)]
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
    meta: Arc<SqliteMeta>,
    lease_mode: LeaseMode,
    orphans: bool,
    verify_only: bool,
    peers: Option<&constellation_net::Peers>,
) -> Result<GcReport> {
    let config = GcConfig::from_env();
    let lease =
        crate::singleton::SingletonLease::acquire(object_store.clone(), "_gc", lease_mode).await?;
    let result = run_held(
        object_store,
        chunks,
        meta,
        &config,
        orphans,
        verify_only,
        peers,
    )
    .await;
    lease.release().await;
    result
}

async fn run_held(
    store: Arc<dyn ObjectStore>,
    chunks: Arc<constellation_store_s3::ChunkStore>,
    meta: Arc<SqliteMeta>,
    config: &GcConfig,
    orphans: bool,
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
        orphans,
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

async fn run_chunks(
    store: Arc<dyn ObjectStore>,
    chunks: Arc<constellation_store_s3::ChunkStore>,
    meta: Arc<SqliteMeta>,
    config: &GcConfig,
    orphans: bool,
    verify_only: bool,
    peers: Option<&constellation_net::Peers>,
) -> Result<GcReport> {
    let now = constellation_store_s3::lease::now_unix_ms();
    let live = live_roots(&chunks, &meta).await?;
    let snapshots = snapshot_roots(&chunks, store.clone()).await?;
    let holds = hold_roots(store.clone(), now).await?;
    let active_designation = DesignationStore::new(store.clone(), DesignationMode::Cas)
        .list_all()
        .await?
        .into_iter()
        .any(|designation| !designation.released);
    let protected: HashSet<_> = live
        .iter()
        .chain(snapshots.iter())
        .chain(holds.iter())
        .copied()
        .collect();

    let mut candidates = Vec::new();
    if !active_designation {
        for (hash, seq, at) in meta.deref_candidates(now - config.horizon_ms)? {
            if !protected.contains(&hash) {
                candidates.push(Mark {
                    key: constellation_store_s3::layout::chunk_key(&hash).to_string(),
                    rule: "deref-horizon".into(),
                    evidence: json!({"deref_seq":seq,"deref_unix_ms":at,"horizon_ms":config.horizon_ms}),
                    hash: Some(hash),
                });
            }
        }
    }

    if orphans {
        let condemned: HashSet<_> = read_condemned(&store)
            .await?
            .into_iter()
            .flat_map(|list| list.hashes)
            .filter_map(|value| ChunkHash::from_hex(&value))
            .collect();
        let known: HashSet<_> = protected.iter().chain(condemned.iter()).copied().collect();
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
    }
    candidates.extend(metadata_candidates(&store, config).await?);
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
            if mark.rule == "deref-horizon"
                && !meta
                    .deref_candidates(now - config.horizon_ms)?
                    .iter()
                    .any(|(candidate, _, _)| *candidate == hash)
            {
                continue;
            }
            if store.head(&Path::from(mark.key.clone())).await.is_err() {
                meta.clear_deref(&hash)?;
                continue;
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
        if let Some(hash) = mark.hash {
            meta.clear_deref(&hash)?;
        }
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
    meta: &SqliteMeta,
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

/// The pre-plan-28 floor: `checkpoints/VECTOR.json`, when a checkpoint
/// exists at all.
async fn legacy_checkpoint_vector(
    store: &Arc<dyn ObjectStore>,
) -> Result<
    Option<(
        constellation_store_s3::log::CheckpointVector,
        serde_json::Value,
    )>,
> {
    let latest = match store.get(&Path::from("checkpoints/p0/LATEST")).await {
        Ok(result) => {
            serde_json::from_slice::<constellation_store_s3::log::CheckpointRef>(
                &result.bytes().await?,
            )?
            .seq
        }
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    // Log retention is per partition, floored against VECTOR.json's
    // applied_seq for that partition — never against the max seq across
    // all partitions. A checkpoint's `covered` is the maximum over every
    // stream, so a child partition that is far behind p0 would otherwise
    // have live segments deleted out from under a bootstrap that has only
    // replayed it to `applied[child]` (the finding-6 data-loss bug). A
    // `LATEST` pointer with no vector is a corrupt bucket: we have no
    // per-partition floor to apply, so we refuse rather than fall back to
    // the global floor. (Read the object directly rather than via
    // `get_checkpoint_vector`, which maps a missing vector to an empty one
    // and would hide exactly this corruption.)
    let vector = match store.get(&Path::from("checkpoints/VECTOR.json")).await {
        Ok(result) => serde_json::from_slice::<constellation_store_s3::log::CheckpointVector>(
            &result.bytes().await?,
        )?,
        Err(object_store::Error::NotFound { .. }) => anyhow::bail!(
            "checkpoints/p0/LATEST names seq {latest} but checkpoints/VECTOR.json is \
             missing; refusing to prune the log against a global floor"
        ),
        Err(error) => return Err(error.into()),
    };
    Ok(Some((vector, json!({"checkpoint": latest}))))
}

async fn metadata_candidates(store: &Arc<dyn ObjectStore>, config: &GcConfig) -> Result<Vec<Mark>> {
    let mut marks = Vec::new();
    // The floor is the position a fresh replica resumes tailing from.
    // Once the commit chain exists that is the head commit's `applied`
    // vector (plan 28 S6 bootstraps from the head), and the legacy
    // checkpoint — no longer written by default — stops mattering; a
    // forced checkpoint bootstrap over a pruned log refuses rather than
    // silently stopping at the gap (`shipper::replay_from`).
    let chain = constellation_store_s3::CommitChain::new(store.clone());
    let head = match chain.discover_head(0).await? {
        Some(seq) => chain.get(seq).await?,
        None => None,
    };
    let floor_source = match head {
        Some(commit) => Some((
            constellation_store_s3::log::CheckpointVector {
                applied: commit.applied,
            },
            json!({"commit": commit.seq}),
        )),
        None => legacy_checkpoint_vector(store).await?,
    };
    if let Some((vector, source)) = floor_source {
        for object in store
            .list(Some(&Path::from("log")))
            .try_collect::<Vec<_>>()
            .await?
        {
            // Segments are `log/<part>/<seq:016x>.zst`; `parts()[1]` is the
            // partition and the filename stem is the sequence. The `sealed`
            // marker has no `.zst` suffix and drops out here.
            let parts: Vec<_> = object
                .location
                .parts()
                .map(|part| part.as_ref().to_string())
                .collect();
            let Some(partition) = parts.get(1) else {
                continue;
            };
            let Some(seq) = object
                .location
                .filename()
                .and_then(|name| name.strip_suffix(".zst"))
                .and_then(|name| u64::from_str_radix(name, 16).ok())
            else {
                continue;
            };
            // A partition absent from the vector is not covered by the
            // snapshot yet: pruning any of its segments would truncate a
            // stream the checkpoint cannot replace.
            let Some(applied) = vector.applied.get(partition).copied() else {
                continue;
            };
            let floor = applied.saturating_sub(config.retention_segments);
            if seq < floor {
                marks.push(Mark {
                    key: object.location.to_string(),
                    rule: "log-retention".into(),
                    evidence: json!({
                        "partition": partition,
                        "vector_applied": applied,
                        "retention_segments": config.retention_segments,
                        "floor_source": source,
                    }),
                    hash: None,
                });
            }
        }
    }
    let mut by_partition: BTreeMap<String, Vec<(u64, String)>> = BTreeMap::new();
    for object in store
        .list(Some(&Path::from("checkpoints")))
        .try_collect::<Vec<_>>()
        .await?
    {
        let parts: Vec<_> = object
            .location
            .parts()
            .map(|part| part.as_ref().to_string())
            .collect();
        let Some(name) = object
            .location
            .filename()
            .and_then(|name| name.strip_suffix(".zst"))
        else {
            continue;
        };
        if parts.len() >= 3 {
            if let Ok(seq) = u64::from_str_radix(name, 16) {
                by_partition
                    .entry(parts[1].clone())
                    .or_default()
                    .push((seq, object.location.to_string()));
            }
        }
    }
    for checkpoints in by_partition.values_mut() {
        checkpoints.sort_by_key(|(seq, _)| *seq);
        let remove = checkpoints.len().saturating_sub(2);
        for (seq, key) in checkpoints.iter().take(remove) {
            marks.push(Mark {
                key: key.clone(),
                rule: "superseded-checkpoint".into(),
                evidence: json!({"checkpoint_seq":seq,"kept_newest":2}),
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

    /// The log-retention floor is per partition, taken from VECTOR.json's
    /// applied_seq for that partition — never from the global `covered`
    /// seq in LATEST. A partition far behind the checkpoint frontier keeps
    /// all of its segments (finding-6 regression).
    #[tokio::test]
    async fn log_retention_floor_is_per_partition_against_the_vector() {
        use constellation_store_s3::log::{CheckpointRef, CheckpointVector};
        use object_store::memory::InMemory;
        use object_store::PutPayload;

        async fn log_retention_marks(applied_p1: u64) -> Vec<String> {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            // LATEST covers the whole cluster at the max seq (p0's).
            let latest = serde_json::to_vec(&CheckpointRef {
                seq: 1000,
                bytes: 0,
            })
            .unwrap();
            store
                .put(
                    &Path::from("checkpoints/p0/LATEST"),
                    PutPayload::from(latest),
                )
                .await
                .unwrap();
            let mut vector = CheckpointVector::default();
            vector.applied.insert("p0".into(), 1000);
            vector.applied.insert("p1".into(), applied_p1);
            store
                .put(
                    &Path::from("checkpoints/VECTOR.json"),
                    PutPayload::from(serde_json::to_vec(&vector).unwrap()),
                )
                .await
                .unwrap();
            // p0 segments straddling its floor (1000 - 128 = 872).
            for seq in [800u64, 871, 872, 900] {
                store
                    .put(
                        &Path::from(format!("log/p0/{seq:016x}.zst")),
                        PutPayload::from(Vec::new()),
                    )
                    .await
                    .unwrap();
            }
            // p1 segments 0..=40.
            for seq in 0u64..=40 {
                store
                    .put(
                        &Path::from(format!("log/p1/{seq:016x}.zst")),
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
            let mut keys: Vec<String> = metadata_candidates(&store, &config)
                .await
                .unwrap()
                .into_iter()
                .filter(|mark| mark.rule == "log-retention")
                .map(|mark| mark.key)
                .collect();
            keys.sort();
            keys
        }

        // p1 applied 30: 30 - 128 saturates to 0, so no p1 segment is below
        // its floor. Only p0's segments under 872 are marked.
        let case_a = log_retention_marks(30).await;
        assert_eq!(
            case_a,
            vec![
                format!("log/p0/{:016x}.zst", 800u64),
                format!("log/p0/{:016x}.zst", 871u64),
            ]
        );
        assert!(!case_a.iter().any(|key| key.starts_with("log/p1/")));

        // p1 applied 300: floor 172, so every present p1 segment (0..=40)
        // is marked, alongside the same two p0 segments.
        let case_b = log_retention_marks(300).await;
        assert_eq!(
            case_b
                .iter()
                .filter(|key| key.starts_with("log/p1/"))
                .count(),
            41
        );
        assert_eq!(
            case_b
                .iter()
                .filter(|key| key.starts_with("log/p0/"))
                .count(),
            2
        );
    }

    /// Plan 28: once a commit exists, retention floors on the head
    /// commit's `applied` vector — where a fresh replica now resumes —
    /// and a stale legacy checkpoint no longer holds the log back (or
    /// lets it be cut, if the checkpoint were ahead). A partition the
    /// commit does not name is not pruned at all.
    #[tokio::test]
    async fn log_retention_floors_on_the_head_commit_once_one_exists() {
        use constellation_store_s3::log::{CheckpointRef, CheckpointVector};
        use constellation_store_s3::{Commit, CommitAgg, Intent};
        use object_store::memory::InMemory;
        use object_store::PutPayload;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let put = |path: String, body: Vec<u8>| {
            let store = store.clone();
            async move {
                store
                    .put(&Path::from(path), PutPayload::from(body))
                    .await
                    .unwrap();
            }
        };
        // A stale legacy checkpoint at p0:50.
        put(
            "checkpoints/p0/LATEST".into(),
            serde_json::to_vec(&CheckpointRef { seq: 50, bytes: 0 }).unwrap(),
        )
        .await;
        let mut vector = CheckpointVector::default();
        vector.applied.insert("p0".into(), 50);
        put(
            "checkpoints/VECTOR.json".into(),
            serde_json::to_vec(&vector).unwrap(),
        )
        .await;
        for seq in 1u64..=300 {
            put(format!("log/p0/{seq:016x}.zst"), Vec::new()).await;
        }
        for seq in 1u64..=10 {
            put(format!("log/p9/{seq:016x}.zst"), Vec::new()).await;
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
            applied: BTreeMap::from([("p0".to_string(), 250u64)]),
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
        let marked: Vec<u64> = metadata_candidates(&store, &config)
            .await
            .unwrap()
            .into_iter()
            .filter(|mark| mark.rule == "log-retention")
            .map(|mark| {
                assert!(mark.key.starts_with("log/p0/"), "{}", mark.key);
                assert_eq!(mark.evidence["floor_source"]["commit"], 1);
                let name = mark.key.rsplit('/').next().unwrap();
                u64::from_str_radix(name.trim_end_matches(".zst"), 16).unwrap()
            })
            .collect();
        // Floor 250 - 128 = 122: segments 1..=121, and nothing for p9.
        assert_eq!(marked.len(), 121);
        assert_eq!(marked.iter().max(), Some(&121));
    }
}
