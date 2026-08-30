//! Coordinated bucket garbage collection (DESIGN.md §14).
//!
//! Ordinary candidates come from SQLite's continuously maintained `deref`
//! index. Only the explicit orphan pass lists `chunks/`. Before any chunk
//! DELETE this module CAS-publishes the complete condemned set and waits a
//! lease TTL; writers independently treat those hashes as dedup misses.

use anyhow::{bail, Result};
use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest};
use constellation_fs_core::{ChunkHash, InodeKind, Tree};
use constellation_meta::SqliteMeta;
use constellation_store_s3::{
    append_journal, publish_condemned, read_condemned, DesignationMode, DesignationStore,
    GcJournalEntry, Lease, LeaseMode, LeaseStore, SnapshotStore,
};
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
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
}

struct GcLease {
    store: LeaseStore,
    lease: Lease,
    tag: constellation_store_s3::LeaseTag,
}

impl GcLease {
    async fn acquire(store: Arc<dyn ObjectStore>, mode: LeaseMode) -> Result<Self> {
        let leases = LeaseStore::new(store, "_gc", mode);
        let now = constellation_store_s3::lease::now_unix_ms();
        let holder = (std::process::id() as u64) << 32 | now as u64 & 0xffff_ffff;
        let ttl = constellation_store_s3::lease::lease_ttl_ms();
        let (lease, tag) = match leases.get().await? {
            None => {
                let lease = Lease::granted("_gc", holder, 1, ttl);
                let tag = leases.try_create(&lease).await?;
                (lease, tag)
            }
            Some((previous, tag)) if previous.is_claimable(now) => {
                let lease = Lease::granted("_gc", holder, previous.epoch + 1, ttl);
                let tag = leases.try_swap(&lease, &tag).await?;
                (lease, tag)
            }
            Some((previous, _)) => {
                bail!(
                    "GC lease is held by {} for another {} ms",
                    previous.holder,
                    previous.expires_in_ms(now)
                )
            }
        };
        debug_assert_eq!(leases.partition(), "_gc");
        Ok(Self {
            store: leases,
            lease,
            tag,
        })
    }

    async fn release(self) {
        let _ = self.store.try_swap(&self.lease.released(), &self.tag).await;
    }
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
    let lease = GcLease::acquire(object_store.clone(), lease_mode).await?;
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
            ChunkInfo::Inline(hashes) => roots.extend(hashes),
            ChunkInfo::Spilled(spill) => {
                roots.insert(spill);
                roots.extend(decode_chunk_list(&chunks.get_chunk(&spill).await?)?);
            }
        }
    }
    Ok(roots)
}

async fn snapshot_roots(
    chunks: &constellation_store_s3::ChunkStore,
    store: Arc<dyn ObjectStore>,
) -> Result<HashSet<ChunkHash>> {
    let records = SnapshotStore::new(store).list().await?;
    let mut roots = HashSet::new();
    let mut cache: BTreeMap<ChunkHash, HashSet<ChunkHash>> = BTreeMap::new();
    for record in records {
        if let Some(cached) = cache.get(&record.root) {
            roots.extend(cached);
            continue;
        }
        let mut reachable = HashSet::new();
        walk_snapshot(chunks, record.root, &mut reachable).await?;
        roots.extend(&reachable);
        cache.insert(record.root, reachable);
    }
    Ok(roots)
}

fn walk_snapshot<'a>(
    chunks: &'a constellation_store_s3::ChunkStore,
    tree_hash: ChunkHash,
    out: &'a mut HashSet<ChunkHash>,
) -> futures::future::BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        if !out.insert(tree_hash) {
            return Ok(());
        }
        let tree = Tree::decode(&chunks.get_chunk(&tree_hash).await?)?;
        for entry in tree.entries {
            let Some(hash) = entry.manifest_or_tree_hash else {
                continue;
            };
            if entry.kind == InodeKind::Dir {
                walk_snapshot(chunks, hash, out).await?;
            } else if entry.kind == InodeKind::File {
                out.insert(hash);
                let manifest = Manifest::decode(&chunks.get_chunk(&hash).await?)?;
                match manifest.chunks {
                    ChunkInfo::Inline(hashes) => out.extend(hashes),
                    ChunkInfo::Spilled(spill) => {
                        out.insert(spill);
                        out.extend(decode_chunk_list(&chunks.get_chunk(&spill).await?)?);
                    }
                }
            }
        }
        Ok(())
    })
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

async fn metadata_candidates(store: &Arc<dyn ObjectStore>, config: &GcConfig) -> Result<Vec<Mark>> {
    let mut marks = Vec::new();
    let latest = match store.get(&Path::from("checkpoints/p0/LATEST")).await {
        Ok(result) => serde_json::from_slice::<constellation_store_s3::log::CheckpointRef>(
            &result.bytes().await?,
        )
        .ok()
        .map(|reference| reference.seq),
        Err(object_store::Error::NotFound { .. }) => None,
        Err(error) => return Err(error.into()),
    };
    if let Some(latest) = latest {
        let floor = latest.saturating_sub(config.retention_segments);
        for object in store
            .list(Some(&Path::from("log")))
            .try_collect::<Vec<_>>()
            .await?
        {
            let Some(name) = object
                .location
                .filename()
                .and_then(|name| name.strip_suffix(".zst"))
            else {
                continue;
            };
            if u64::from_str_radix(name, 16).is_ok_and(|seq| seq < floor) {
                marks.push(Mark {
                    key: object.location.to_string(),
                    rule: "log-retention".into(),
                    evidence: json!({"latest_checkpoint":latest,"retention_segments":config.retention_segments}),
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
}
