//! Coordinated bucket garbage collection (DESIGN.md §14).
//!
//! Chunk candidates come from a single LIST-based orphan pass over
//! `chunks/` (plan 29 M0c retired the `deref` index this used to share the
//! job with — see `store-s3::mark`'s module doc for why the reachability
//! walk from commit roots makes it unnecessary). Before any chunk DELETE
//! this module CAS-publishes the complete condemned set and waits a lease
//! TTL; writers independently treat those hashes as dedup misses. The
//! `_gc` singleton lease is renewed through the wait and every delete
//! batch is fenced on it (`run_chunks`'s doc has the argument).

use anyhow::{Context, Result};
use constellation_fs_core::ChunkHash;
use constellation_meta::Meta;
use constellation_store_s3::{
    append_journal, publish_condemned, GcJournalEntry, LeaseMode, LogStore, SnapshotRecord,
    SnapshotStore, StoreError,
};
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;

/// How this GC round catches its replica up to the log head before
/// marking, and again after the condemned-list TTL wait, before the
/// final re-check (plan 29 M3a).
///
/// `run_chunks`'s liveness view (`live_roots`) reads `meta.live_manifests`
/// — the *local* replica's view of which chunks are referenced. A
/// replica that has not yet tailed a writer's dedup-hit commit believes
/// an old chunk is unreferenced when it is not; without a tail before
/// marking, that chunk becomes a candidate, and without a second tail
/// after the TTL wait, a dedup that landed *during* the wait is still
/// invisible at delete time. Both tails are mandatory: a round that
/// cannot refresh its replica aborts rather than mark or delete against
/// a view it cannot vouch for.
pub enum GcTail {
    /// GC is running inside the mount daemon (the control-socket path):
    /// ask the live sync task — which owns the real `Shipper`, with its
    /// lease and partition state — to tail every partition, through the
    /// same channel pattern every other cross-thread daemon call uses.
    Daemon(tokio::sync::mpsc::UnboundedSender<crate::sync::SyncRequest>),
    /// GC is running standalone (`constellation gc` with no daemon
    /// holding this state dir, or the periodic in-daemon task before a
    /// sync channel exists): attach a throwaway tail-only `Shipper`
    /// straight to the `LogStore` and `Meta` this round already has, and
    /// tail it. It ships nothing and holds no lease — a GC round never
    /// authors segments — so this is safe even for a read-only member or
    /// mid-reintegration.
    Standalone { logs: LogStore, node_id: u64 },
}

impl GcTail {
    /// A standalone tailer for `meta`'s own state dir: reads the node id
    /// this replica already claimed (`kv_get("node_id")`), or `0` for a
    /// bootstrap-only replica that has never been mounted (nothing in
    /// its journal can collide with a real node's segments in that
    /// case).
    pub fn standalone(logs: LogStore, meta: &Meta) -> Result<Self> {
        let node_id = meta
            .kv_get("node_id")?
            .map(|v| v.parse::<u64>())
            .transpose()
            .context("corrupt node_id in state dir")?
            .unwrap_or(0);
        Ok(GcTail::Standalone { logs, node_id })
    }

    async fn tail_to_head(&self, meta: &Arc<Meta>, lease_mode: LeaseMode) -> Result<()> {
        match self {
            GcTail::Daemon(tx) => {
                let (reply, receive) = tokio::sync::oneshot::channel();
                tx.send(crate::sync::SyncRequest::TailToHead { reply })
                    .map_err(|_| anyhow::anyhow!("sync task is not running"))?;
                receive
                    .await
                    .context("sync task stopped before replying")?
                    .map_err(|message| anyhow::anyhow!(message))
            }
            GcTail::Standalone { logs, node_id } => {
                let mut driver = crate::authority_driver::Standalone::new(
                    meta.clone(),
                    logs.inner(),
                    *node_id,
                    lease_mode,
                );
                driver.tail_to_head().await
            }
        }
    }
}

pub const DEFAULT_GC_INTERVAL_S: u64 = 86_400;
pub const DEFAULT_GC_HORIZON_S: u64 = 7 * 86_400;
pub const DEFAULT_LOG_RETENTION_SEGMENTS: u64 = 128;
/// Plan 30 §M2: default `CONSTELLATION_COMPLETION_RETENTION_S`. An
/// in-doubt op's coverage rule needs every log segment younger than
/// this window to still exist, whatever the head commit's own
/// `retention_segments` floor would otherwise allow deleting — the FUSE
/// deadline (2×lease TTL) keeps any real retry far inside it.
pub const DEFAULT_COMPLETION_RETENTION_S: u64 = 900;

#[derive(Debug, Clone)]
pub struct GcConfig {
    pub horizon_ms: i64,
    pub retention_segments: u64,
    pub lease_ttl_ms: u64,
    /// Plan 30 §M2 coverage rule floor, in milliseconds.
    pub completion_retention_ms: i64,
    /// How snapshot chunks are enumerated (`CONSTELLATION_GC_SNAP_WALK`).
    pub snap_walk: SnapWalkMode,
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
            completion_retention_ms: seconds(
                "CONSTELLATION_COMPLETION_RETENTION_S",
                DEFAULT_COMPLETION_RETENTION_S,
            ) as i64
                * 1000,
            snap_walk: SnapWalkMode::from_env(),
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
    /// Plan 32 §0.3: `snaps/` objects this round re-PUT from their rows
    /// (rule `snap-object-restored`). Orphan objects it deleted are in
    /// `deleted`, like any other key.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub restored: Vec<String>,
    pub condemned_epoch: Option<u64>,
    /// Plan 28 S7b: the metadata tree's round (commit retention, pack
    /// sweep and compaction). `None` when the phase did not run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<crate::mtree_gc::MtreeGcReport>,
    /// Plan 32 §6.1: what the round's LIST of `chunks/` counted (every
    /// chunk object, protected or not, and its stored bytes). A real
    /// round also writes it to `gc/summary.json`, where the accounting
    /// index reads its compression estimate from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub census: Option<constellation_store_s3::ChunkCensus>,
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    object_store: Arc<dyn ObjectStore>,
    chunks: Arc<constellation_store_s3::ChunkStore>,
    meta: Arc<Meta>,
    lease_mode: LeaseMode,
    verify_only: bool,
    peers: Option<&constellation_net::Peers>,
    tail: &GcTail,
) -> Result<GcReport> {
    let config = GcConfig::from_env();
    let mut lease =
        crate::singleton::SingletonLease::acquire(object_store.clone(), "_gc", lease_mode).await?;
    tracing::info!(epoch = lease.epoch(), verify_only, "bucket GC round starts");
    let result = run_held(
        object_store,
        chunks,
        meta,
        lease_mode,
        &config,
        verify_only,
        peers,
        tail,
        &mut lease,
    )
    .await;
    lease.release().await;
    result
}

#[allow(clippy::too_many_arguments)]
async fn run_held(
    store: Arc<dyn ObjectStore>,
    chunks: Arc<constellation_store_s3::ChunkStore>,
    meta: Arc<Meta>,
    lease_mode: LeaseMode,
    config: &GcConfig,
    verify_only: bool,
    peers: Option<&constellation_net::Peers>,
    tail: &GcTail,
    lease: &mut crate::singleton::SingletonLease,
) -> Result<GcReport> {
    // Chunks first: their snapshot roots are read from the metadata tree,
    // which the second phase compacts.
    let mut report = run_chunks(
        store.clone(),
        chunks.clone(),
        meta.clone(),
        lease_mode,
        config,
        verify_only,
        peers,
        tail,
        lease,
    )
    .await?;
    let tree_config =
        crate::mtree_gc::MtreeGcConfig::from_env(config.horizon_ms, config.lease_ttl_ms);
    report.metadata = Some(
        crate::mtree_gc::run(
            store,
            chunks.e2e_keys(),
            &meta,
            &tree_config,
            verify_only,
            lease,
        )
        .await?,
    );
    Ok(report)
}

/// Deletes between two fence checks (`SingletonLease::renew`).
const DELETE_FENCE_EVERY: usize = 64;

/// A chunk round's first half: candidates marked and CAS-published as the
/// condemned pointer. `sweep_chunks` is the second half, after the grace
/// wait.
struct Marked {
    candidates: Vec<Mark>,
    condemned: constellation_store_s3::CondemnedList,
    /// The mark's snapshot walk mode, which the sweep's refresh reuses.
    snap_walk: SnapWalkMode,
    /// What snapshot reconciliation found and did before the mark.
    snaps: SnapFindings,
    census: constellation_store_s3::ChunkCensus,
}

/// Chunk candidates come from a single pass: LIST `chunks/` and mark
/// anything older than the horizon that is not in the protected set (live
/// manifests of the replica, snapshot roots, holds).
/// Plan 28 §P10 retired the `deref` index and its per-replica bookkeeping —
/// any node, or an external job with bucket credentials, can GC by reading
/// roots, so this LIST-based orphan pass is the only candidate source now.
///
/// `live_roots` reads the *local* replica (`meta.live_manifests`), which
/// is only a safe liveness view if the replica is caught up to the log
/// head: a replica lagging behind a writer that just deduplicated a new
/// manifest against an old chunk would otherwise mark — and delete —
/// still-referenced bytes it has not yet learned about (plan 29 M3a).
/// `tail` therefore runs twice: once here, before the first mark, and
/// once more after the condemned-list TTL wait, before the final
/// re-check — a dedup landing during the wait must be visible before
/// deletion, not just before marking. Either tail failing aborts the
/// round with nothing marked or deleted rather than proceed against a
/// replica view this pass could not vouch for.
///
/// # A round's deletes happen only while its own list is current
///
/// `CondemnedView`'s argument (store-s3 `gc.rs`) rests on one premise:
/// every chunk a round deletes is on the condemned pointer from the
/// moment that round published it until the delete lands. Two things
/// hold it up, and neither depends on timing:
///
/// 1. **The `_gc` lease is renewed through the round and every delete
///    batch is fenced on it.** The lease used to be acquired once and
///    never renewed, while the round waits a full TTL after publishing:
///    by delete time it had lapsed, a second round (every daemon runs its
///    own daily tick) could take it and publish a new pointer, and the
///    first round kept deleting under a list that was no longer current.
///    Now the wait renews the lease as it goes (`hold_for`) and the delete
///    loop renews — a CAS on the lease object's ETag — before its first
///    delete and every `DELETE_FENCE_EVERY` deletes after that; a refused
///    renewal (`singleton::Fenced`) stops the round on the spot. A round
///    whose process paused for minutes is stopped the same way: the fence
///    is the store's CAS, not the round's clock.
/// 2. **A new round carries the previous round's undeleted candidates.**
///    The candidate pass no longer skips hashes that are already on the
///    pointer: a chunk that is still present, still older than the
///    horizon and still unprotected is a candidate again, so the pointer
///    the new round publishes lists every chunk an interrupted (or
///    paused) round could still delete. Between a fenced renewal and the
///    delete it guards, the previous round may land at most
///    `DELETE_FENCE_EVERY` deletes — and each of those is of a hash the
///    new list carries (a writer reading the new pointer treats it as
///    condemned and uploads), or of a hash the new round found protected.
///    The protected case cannot dangle either: a manifest naming the hash
///    was committed by a writer whose dedup hit either preceded the old
///    round's publication (then its commit preceded the old round's
///    post-wait tail and re-check, which kept the chunk) or saw the old
///    pointer list the hash and re-uploaded it (then the old round's
///    `HEAD` sees the newer object and keeps it, modulo the `HEAD`→
///    `DELETE` gap portable S3 leaves open, see rule 3 in DESIGN.md §14).
///
/// So a hash absent from the *current* pointer is deleted by no round at
/// all, which is exactly what `CondemnedView` needs of "the round whose
/// list was current".
#[allow(clippy::too_many_arguments)]
async fn run_chunks(
    store: Arc<dyn ObjectStore>,
    chunks: Arc<constellation_store_s3::ChunkStore>,
    meta: Arc<Meta>,
    lease_mode: LeaseMode,
    config: &GcConfig,
    verify_only: bool,
    peers: Option<&constellation_net::Peers>,
    tail: &GcTail,
    lease: &mut crate::singleton::SingletonLease,
) -> Result<GcReport> {
    let marked = match mark_chunks(
        &store,
        &chunks,
        &meta,
        lease_mode,
        config,
        verify_only,
        tail,
        lease,
    )
    .await?
    {
        Ok(marked) => marked,
        Err(report) => return Ok(report),
    };
    if let Some(peers) = peers {
        peers.announce_condemned(marked.condemned.epoch).await;
    }
    // One complete authority TTL is mandatory. A writer which has not
    // refreshed by then can no longer commit under a valid partition
    // lease. The `_gc` lease is renewed along the way: it must still be
    // this round's when the deletes start.
    lease
        .hold_for(std::time::Duration::from_millis(config.lease_ttl_ms))
        .await
        .context("waiting out the condemned-list grace period")?;
    sweep_chunks(&store, &chunks, &meta, lease_mode, tail, lease, marked).await
}

/// The mark phase: tail, reconcile snapshots, list, protect, publish.
/// `Err(report)` is an early, complete report (verify-only, or nothing to
/// condemn).
#[allow(clippy::too_many_arguments)]
async fn mark_chunks(
    store: &Arc<dyn ObjectStore>,
    chunks: &Arc<constellation_store_s3::ChunkStore>,
    meta: &Arc<Meta>,
    lease_mode: LeaseMode,
    config: &GcConfig,
    verify_only: bool,
    tail: &GcTail,
    lease: &mut crate::singleton::SingletonLease,
) -> Result<std::result::Result<Marked, GcReport>> {
    tail.tail_to_head(meta, lease_mode)
        .await
        .context("tailing the metadata log to head before marking chunk GC candidates")?;
    let snaps = reconcile_snapshots(store, meta, lease_mode, config, verify_only, tail, lease)
        .await
        .context("reconciling snaps/ objects with snapshot rows")?;
    let now = constellation_store_s3::lease::now_unix_ms();
    let live = live_roots(chunks, meta).await?;
    let snapshots =
        snapshot_roots_kept(config.snap_walk, chunks, store.clone(), meta, &snaps.kept).await?;
    let holds = hold_roots(store.clone(), now).await?;
    let protected: HashSet<_> = live
        .iter()
        .chain(snapshots.iter())
        .chain(holds.iter())
        .copied()
        .collect();

    let mut candidates = Vec::new();
    let mut census = constellation_store_s3::ChunkCensus {
        as_of_ms: now,
        ..Default::default()
    };
    let prefix = Path::from("chunks");
    for object in store.list(Some(&prefix)).try_collect::<Vec<_>>().await? {
        let Some(hash) = object.location.filename().and_then(ChunkHash::from_hex) else {
            continue;
        };
        census.chunk_objects += 1;
        census.physical_bytes += object.size;
        if !protected.contains(&hash)
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
    candidates.extend(metadata_candidates(store, chunks.e2e_keys(), config, now).await?);
    candidates.sort_by(|a, b| a.key.cmp(&b.key));
    if !verify_only {
        // Advisory (an estimate's input): a failed PUT costs the round
        // nothing.
        if let Err(error) = constellation_store_s3::write_chunk_census(store, &census).await {
            tracing::warn!(%error, "GC: could not write the chunk census");
        }
    }

    if verify_only || candidates.is_empty() {
        return Ok(Err(snaps.into_report(GcReport {
            verify_only,
            candidates,
            deleted: Vec::new(),
            restored: Vec::new(),
            condemned_epoch: None,
            metadata: None,
            census: Some(census),
        })));
    }

    let condemned_hashes: Vec<_> = candidates
        .iter()
        .filter_map(|mark| mark.hash.map(|hash| hash.to_hex()))
        .collect();
    let condemned = publish_condemned(store, condemned_hashes, now).await?;
    Ok(Ok(Marked {
        candidates,
        condemned,
        snap_walk: config.snap_walk,
        snaps,
        census,
    }))
}

/// The sweep phase, after the grace wait: tail again, re-check liveness,
/// and delete behind the lease fence.
#[allow(clippy::too_many_arguments)]
async fn sweep_chunks(
    store: &Arc<dyn ObjectStore>,
    chunks: &Arc<constellation_store_s3::ChunkStore>,
    meta: &Arc<Meta>,
    lease_mode: LeaseMode,
    tail: &GcTail,
    lease: &mut crate::singleton::SingletonLease,
    marked: Marked,
) -> Result<GcReport> {
    let Marked {
        candidates,
        condemned,
        snap_walk,
        snaps,
        census,
    } = marked;
    tail.tail_to_head(meta, lease_mode).await.context(
        "tailing the metadata log to head after the condemned-list wait, before deletion",
    )?;
    let refreshed_live = live_roots(chunks, meta).await?;
    // Orphan objects this round deleted before marking still protect
    // their chunks until it ends (plan 32 §0.3).
    let refreshed_snaps =
        snapshot_roots_kept(snap_walk, chunks, store.clone(), meta, &snaps.kept).await?;
    // Holds are re-read too: a node that applied an unlink of a file it
    // has open publishes its hold asynchronously (`cli::holds`), and the
    // wait above is the window it gets before this round's deletes.
    let refreshed_holds =
        hold_roots(store.clone(), constellation_store_s3::lease::now_unix_ms()).await?;
    let mut deleted = Vec::new();
    for mark in &candidates {
        if let Some(hash) = mark.hash {
            if refreshed_live.contains(&hash)
                || refreshed_snaps.contains(&hash)
                || refreshed_holds.contains(&hash)
            {
                continue;
            }
            let Ok(now_there) = store.head(&Path::from(mark.key.clone())).await else {
                continue; // a concurrent pass already removed it
            };
            // A writer that found the chunk condemned uploaded it again
            // (`CondemnedView`: a condemned hit is re-PUT, not relied on)
            // and may commit a manifest naming it after this round's
            // second tail: the object is not the one marked, so leave it
            // (never less safe: keeping an object cannot dangle anything).
            // A HEAD's time has one-second resolution; the marked object
            // is older than `gc.horizon` in any real round.
            if reuploaded_since_marked(mark, now_there.last_modified.timestamp_millis()) {
                tracing::info!(key = %mark.key, "condemned chunk uploaded again since it was marked; kept");
                continue;
            }
        }
        // The fence: this round's list is current only while this round
        // holds the `_gc` lease, and the lease object's CAS is the proof.
        if deleted.len() % DELETE_FENCE_EVERY == 0 {
            lease.renew().await.with_context(|| {
                format!(
                    "chunk GC stopped after {} delete(s): the round is no longer the lease holder",
                    deleted.len()
                )
            })?;
        }
        store.delete(&Path::from(mark.key.clone())).await?;
        append_journal(
            store,
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
    Ok(snaps.into_report(GcReport {
        verify_only: false,
        candidates,
        deleted,
        restored: Vec::new(),
        condemned_epoch: Some(condemned.epoch),
        metadata: None,
        census: Some(census),
    }))
}

/// Whether the object behind a chunk mark was written again after the
/// listing that marked it (its modification time moved on by more than
/// the one-second resolution of a `HEAD`'s `Last-Modified`).
fn reuploaded_since_marked(mark: &Mark, head_last_modified_ms: i64) -> bool {
    mark.evidence["last_modified_ms"]
        .as_i64()
        .is_some_and(|marked| head_last_modified_ms > marked + 1000)
}

/// What one round's snapshot reconciliation (plan 32 §0.3) found, and,
/// unless it was verify-only, did.
#[derive(Debug, Default)]
struct SnapFindings {
    /// Every finding, as a report candidate (`hash: None`); never handed
    /// to the chunk sweep, which deletes whatever it is given.
    marks: Vec<Mark>,
    /// The orphan objects deleted.
    deleted: Vec<String>,
    /// The objects re-PUT from their rows.
    restored: Vec<String>,
    /// The deleted orphans' records: their chunks stay protected for the
    /// rest of the round, through the mark *and* the sweep's refresh.
    kept: Vec<SnapshotRecord>,
}

impl SnapFindings {
    fn into_report(self, mut report: GcReport) -> GcReport {
        report.candidates.extend(self.marks);
        report.candidates.sort_by(|a, b| a.key.cmp(&b.key));
        report.deleted.extend(self.deleted);
        report.restored.extend(self.restored);
        report
    }
}

/// Plan 32 §0.3: make the two copies of every snapshot agree again.
///
/// A snapshot is a `snaps/<id>.json` object *and* a replicated row, with no
/// transaction spanning them. Create writes the object, then the row;
/// delete removes the row, then the object. An interruption therefore
/// leaves an **object without a row**: an orphan that pins its chunks
/// with no listing and blocks re-creating that `path@name`. Such an object
/// older than `gc.horizon` is deleted (rule `snap-orphan-object`). A
/// younger one may be a create in flight — its row not yet written,
/// shipped or tailed here — and is left alone; so is one younger than a
/// lease TTL whatever the horizon says, the bound a writer's row takes to
/// reach the log (a test horizon of 0 must not race a create).
///
/// A **row without an object** should be impossible, so it is logged at
/// `warn` and the object is re-PUT from the row, create-if-absent (rule
/// `snap-object-restored`). It is *not* impossible to observe, though: a
/// delete on another node removes its row locally and the object at once,
/// and this replica keeps the row until that node's `SnapDelete` is
/// shipped and tailed. Restoring then would resurrect a deleted snapshot's
/// object, so the round first waits a lease TTL (renewing its own lease),
/// tails again, and restores only rows still present whose object is
/// still absent. That wait is only paid when such a row was seen.
///
/// Rows are read before objects are listed: a create landing in between
/// shows up as a young object (left alone), never as a row without its
/// object. Every destructive step renews the `_gc` lease first, as the
/// chunk sweep does. A verify-only round reports what it found, acts on
/// nothing and renews nothing.
#[allow(clippy::too_many_arguments)]
async fn reconcile_snapshots(
    store: &Arc<dyn ObjectStore>,
    meta: &Arc<Meta>,
    lease_mode: LeaseMode,
    config: &GcConfig,
    verify_only: bool,
    tail: &GcTail,
    lease: &mut crate::singleton::SingletonLease,
) -> Result<SnapFindings> {
    use crate::snapshot::SnapshotRoot;
    use constellation_store_s3::{layout, SnapshotTreeRoot};
    use std::collections::BTreeMap;
    let records = SnapshotStore::new(store.clone());
    let rows: BTreeMap<String, constellation_meta::SnapshotRow> = meta
        .snapshots(None)?
        .into_iter()
        .map(|row| (row.id.clone(), row))
        .collect();
    let objects: BTreeMap<String, SnapshotRecord> = records
        .list()
        .await?
        .into_iter()
        .map(|record| (record.id(), record))
        .collect();
    let now = constellation_store_s3::lease::now_unix_ms();
    let floor_ms = config.horizon_ms.max(config.lease_ttl_ms as i64);
    let mut found = SnapFindings::default();

    for (id, record) in objects.iter().filter(|(id, _)| !rows.contains_key(*id)) {
        if record.created_unix_ms > now - floor_ms {
            continue;
        }
        let key = layout::snapshot(id).to_string();
        let mark = Mark {
            key: key.clone(),
            rule: "snap-orphan-object".into(),
            evidence: json!({
                "id": id,
                "path": record.path,
                "name": record.name,
                "created_unix_ms": record.created_unix_ms,
                "horizon_ms": config.horizon_ms,
            }),
            hash: None,
        };
        if !verify_only {
            // Re-read the row just before deleting: one written on this
            // node since the scan (no tail runs in between, so only a
            // local create can) keeps its object.
            if meta.snapshot_by_id(id)?.is_some() {
                continue;
            }
            lease.renew().await.with_context(|| {
                format!("snapshot reconciliation stopped before deleting {key}: the round is no longer the lease holder")
            })?;
            // Kept as a root before it is gone, so no ordering of this
            // round's later steps can see its chunks unprotected.
            found.kept.push(record.clone());
            records.delete(&record.path, &record.name).await?;
            append_journal(
                store,
                &GcJournalEntry {
                    key: key.clone(),
                    rule: mark.rule.clone(),
                    evidence: mark.evidence.clone(),
                    ts: constellation_store_s3::lease::now_unix_ms(),
                },
            )
            .await?;
            tracing::info!(%key, path = %record.path, name = %record.name, "GC: deleted an orphan snapshot object");
            found.deleted.push(key);
        }
        found.marks.push(mark);
    }

    let mut missing = Vec::new();
    for (id, row) in rows.iter().filter(|(id, _)| !objects.contains_key(*id)) {
        let root = match SnapshotRoot::parse(&row.root_hash) {
            Ok(root) => root,
            Err(error) => {
                tracing::warn!(%id, %error, "GC: a snapshot row without its object has an unreadable root; not restored");
                continue;
            }
        };
        let mut record = SnapshotRecord::new(
            &row.path,
            &row.name,
            row.creator,
            SnapshotTreeRoot {
                seq: root.seq,
                root: root.root.to_hex(),
                ino: root.ino,
            },
        )
        .with_extensions(row.origin, row.policy_ino, row.refer_bytes);
        record.created_unix_ms = row.created_unix_ms;
        if record.id() != *id {
            tracing::warn!(%id, path = %row.path, name = %row.name, "GC: a snapshot row is not keyed by its path@name; not restored");
            continue;
        }
        let key = layout::snapshot(id).to_string();
        tracing::warn!(%key, path = %row.path, name = %row.name, "GC: a snapshot row has no snaps/ object");
        let mark = Mark {
            key,
            rule: "snap-object-restored".into(),
            evidence: json!({
                "id": id,
                "path": row.path,
                "name": row.name,
                "created_unix_ms": row.created_unix_ms,
                "root": row.root_hash,
            }),
            hash: None,
        };
        missing.push((record, mark));
    }
    if verify_only {
        found
            .marks
            .extend(missing.into_iter().map(|(_, mark)| mark));
        return Ok(found);
    }
    if missing.is_empty() {
        return Ok(found);
    }
    lease
        .hold_for(std::time::Duration::from_millis(config.lease_ttl_ms))
        .await
        .context("waiting out a lease TTL before restoring snapshot objects")?;
    tail.tail_to_head(meta, lease_mode)
        .await
        .context("tailing the metadata log to head before restoring snapshot objects")?;
    for (record, mark) in missing {
        let id = record.id();
        if meta.snapshot_by_id(&id)?.is_none() || records.get(&id).await?.is_some() {
            tracing::info!(key = %mark.key, "GC: a snapshot row without its object resolved itself (a delete or create in flight)");
            continue;
        }
        lease.renew().await.with_context(|| {
            format!(
                "snapshot reconciliation stopped before restoring {}: the round is no longer the lease holder",
                mark.key
            )
        })?;
        match records.create(&record).await {
            Ok(()) => {}
            // Someone else wrote it meanwhile: there is nothing to restore.
            Err(StoreError::AlreadyExists) => continue,
            Err(error) => return Err(error.into()),
        }
        append_journal(
            store,
            &GcJournalEntry {
                key: mark.key.clone(),
                rule: mark.rule.clone(),
                evidence: mark.evidence.clone(),
                ts: constellation_store_s3::lease::now_unix_ms(),
            },
        )
        .await?;
        tracing::warn!(key = %mark.key, "GC: restored a snapshot object from its row");
        found.restored.push(mark.key.clone());
        found.marks.push(mark);
    }
    Ok(found)
}

async fn live_roots(
    chunks: &constellation_store_s3::ChunkStore,
    meta: &Meta,
) -> Result<HashSet<ChunkHash>> {
    let mut roots = HashSet::new();
    for bytes in meta.live_manifests()? {
        roots.extend(crate::holds::manifest_chunk_hashes(chunks, &bytes).await?);
    }
    Ok(roots)
}

/// How chunk GC enumerates what snapshots keep alive:
/// `CONSTELLATION_GC_SNAP_WALK`, `diff` (the default) or `full`.
///
/// - `diff` (plan 32 §0.2): roots from `snaps/` **and** the replica's
///   snapshot rows (tailed to head by the round), grouped into chains by
///   directory inode; each chain costs one full walk of its oldest
///   snapshot plus one `Tree::diff` per later snapshot
///   ([`crate::snapwalk::ChainWalk::protect_chain`]). Protects a superset
///   of `full`'s set (equal, in practice: the deltas are exact).
/// - `full`: the pre-plan-32 walk of every `snaps/` object's whole
///   subtree, every round, kept for one release as the fallback. An
///   unrecognized value reads as `diff`, with a warning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapWalkMode {
    Diff,
    Full,
}

impl SnapWalkMode {
    pub fn from_env() -> SnapWalkMode {
        match std::env::var("CONSTELLATION_GC_SNAP_WALK").as_deref() {
            Ok("full") => SnapWalkMode::Full,
            Ok("diff") | Err(_) => SnapWalkMode::Diff,
            Ok(other) => {
                tracing::warn!(
                    value = other,
                    "CONSTELLATION_GC_SNAP_WALK is neither `diff` nor `full`; using `diff`"
                );
                SnapWalkMode::Diff
            }
        }
    }
}

#[cfg(test)]
pub(crate) async fn snapshot_roots_with(
    mode: SnapWalkMode,
    chunks: &Arc<constellation_store_s3::ChunkStore>,
    store: Arc<dyn ObjectStore>,
    meta: &Meta,
) -> Result<HashSet<ChunkHash>> {
    snapshot_roots_kept(mode, chunks, store, meta, &[]).await
}

/// [`snapshot_roots_with`], plus `kept`: snapshot records whose object is
/// gone but whose chunks this round must still protect (the orphans its
/// own reconciliation deleted).
async fn snapshot_roots_kept(
    mode: SnapWalkMode,
    chunks: &Arc<constellation_store_s3::ChunkStore>,
    store: Arc<dyn ObjectStore>,
    meta: &Meta,
    kept: &[SnapshotRecord],
) -> Result<HashSet<ChunkHash>> {
    match mode {
        SnapWalkMode::Full => snapshot_roots_full(chunks, store, kept).await,
        SnapWalkMode::Diff => snapshot_roots_by_chain(chunks, store, meta, kept).await,
    }
}

/// Plan 32 §0.2: every snapshot's chunks, one chain at a time.
///
/// A root known only from a replica row (its `snaps/` object missing) is
/// walked like any other. If its tree cannot be read from the bucket, the
/// walk errors and the round marks nothing, so it fails closed until
/// 32-m0c's orphan reconciliation re-PUTs the object from the row.
async fn snapshot_roots_by_chain(
    chunks: &Arc<constellation_store_s3::ChunkStore>,
    store: Arc<dyn ObjectStore>,
    meta: &Meta,
    kept: &[SnapshotRecord],
) -> Result<HashSet<ChunkHash>> {
    use crate::snapshot::{SnapshotRoot, TreeAccess};
    use std::collections::BTreeMap;
    // Every distinct root, from both copies: the bucket object and the
    // replicated row are written without a transaction spanning them, so
    // either may exist alone for a while (plan 32 §0.3), and either one
    // is reason enough to keep the chunks.
    let mut roots: BTreeMap<String, (SnapshotRoot, i64)> = BTreeMap::new();
    let mut note = |root: SnapshotRoot, created: i64| {
        roots
            .entry(root.encode())
            .and_modify(|(_, at)| *at = (*at).min(created))
            .or_insert((root, created));
    };
    for record in SnapshotStore::new(store.clone())
        .list()
        .await?
        .iter()
        .chain(kept)
    {
        note(SnapshotRoot::of_record(record)?, record.created_unix_ms);
    }
    for row in meta.snapshots(None)? {
        match SnapshotRoot::parse(&row.root_hash) {
            Ok(root) => note(root, row.created_unix_ms),
            // Not a tree root, so not walkable by either mode; its
            // `snaps/` object (if any) was parsed above or failed loudly.
            Err(error) => {
                tracing::warn!(id = %row.id, %error, "GC: a snapshot row with an unreadable root")
            }
        }
    }
    let mut protected = HashSet::new();
    if roots.is_empty() {
        return Ok(protected);
    }
    let mut chains: BTreeMap<constellation_fs_core::Ino, Vec<(u64, i64, SnapshotRoot)>> =
        BTreeMap::new();
    for (root, created) in roots.into_values() {
        chains
            .entry(root.ino)
            .or_default()
            .push((root.seq, created, root));
    }
    let scratch = ScratchDir::new("gc-snapshot-nodes")?;
    let reader =
        crate::mtree_read::ChainReader::for_store(store.clone(), chunks.e2e_keys(), &scratch.0)?;
    reader.cache.refresh_catalog().await?;
    let walk = crate::snapwalk::ChainWalk::new(TreeAccess::from_reader(reader), chunks.clone());
    for (ino, mut chain) in chains {
        chain.sort_by_key(|(seq, created, root)| (*seq, *created, root.root));
        let chain: Vec<SnapshotRoot> = chain.into_iter().map(|(_, _, root)| root).collect();
        tracing::debug!(
            dir = ino,
            snapshots = chain.len(),
            "GC: marking a snapshot chain"
        );
        walk.protect_chain(&chain, &mut protected).await?;
    }
    let (_, spill_peak_bytes, spill_evictions) = walk.spill_cache_gauges();
    tracing::debug!(
        spill_fetches = walk.spill_fetches(),
        spill_refetches = walk.spill_refetches(),
        spill_peak_bytes,
        spill_evictions,
        "GC: snapshot chains marked"
    );
    Ok(protected)
}

/// The pre-plan-32 snapshot walk (`CONSTELLATION_GC_SNAP_WALK=full`).
async fn snapshot_roots_full(
    chunks: &constellation_store_s3::ChunkStore,
    store: Arc<dyn ObjectStore>,
    kept: &[SnapshotRecord],
) -> Result<HashSet<ChunkHash>> {
    use crate::snapshot::{snapshot_chunk_refs, SnapshotRoot, TreeAccess};
    let mut records = SnapshotStore::new(store.clone()).list().await?;
    records.extend(kept.iter().cloned());
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

/// Every chunk hash a live (unexpired) `holds/*` object names
/// (`cli::holds`).
pub(crate) async fn hold_roots(
    store: Arc<dyn ObjectStore>,
    now: i64,
) -> Result<HashSet<ChunkHash>> {
    let mut roots = HashSet::new();
    for object in store
        .list(Some(&constellation_store_s3::layout::holds_prefix()))
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
///
/// Plan 30 §M2 coverage rule: whatever the commit-based floor above would
/// otherwise allow, a segment younger than `completion_retention_ms` is
/// never marked — an in-doubt op's resolution against `completed` is
/// only valid if every segment since the op was first sent is still
/// there to have tailed. Retention only ever *widens* what survives, so
/// this can only keep more segments than the seq-based floor alone,
/// never fewer.
pub(crate) async fn metadata_candidates(
    store: &Arc<dyn ObjectStore>,
    keys: Option<&constellation_store_s3::SharedE2eKeys>,
    config: &GcConfig,
    now_ms: i64,
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
        let within_completion_retention =
            object.last_modified.timestamp_millis() > now_ms - config.completion_retention_ms;
        if seq < floor && !within_completion_retention {
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
    use constellation_store_s3::ChunkStore;
    use object_store::memory::InMemory;

    /// A `_gc` lease for a test round, with an explicit TTL.
    async fn gc_lease(
        store: &Arc<dyn ObjectStore>,
        ttl_ms: u64,
    ) -> crate::singleton::SingletonLease {
        crate::singleton::SingletonLease::acquire_with_ttl(
            store.clone(),
            "_gc",
            LeaseMode::Cas,
            ttl_ms,
        )
        .await
        .unwrap()
    }

    fn fast_config() -> GcConfig {
        GcConfig {
            horizon_ms: 0,
            retention_segments: 128,
            lease_ttl_ms: 1,
            completion_retention_ms: 0,
            snap_walk: SnapWalkMode::Diff,
        }
    }

    /// A replica whose only live file names `hash` (an inline manifest).
    fn replica_with_live(node: u64, hash: Option<ChunkHash>) -> Arc<Meta> {
        use constellation_fs_core::manifest::{ChunkInfo, Manifest};
        use constellation_fs_core::types::ROOT_INO;
        use constellation_fs_core::ChunkLayout;
        use constellation_meta::MetaStore;
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        meta.set_node_prefix(node).unwrap();
        if let Some(hash) = hash {
            let file = meta.create(ROOT_INO, "live", 0o644, 0, 0).unwrap();
            let manifest = Manifest {
                layout: ChunkLayout::new(4096),
                file_len: 4,
                chunks: ChunkInfo::Inline([(0u64, hash)].into_iter().collect()),
            }
            .encode();
            meta.set_manifest(file.ino, &manifest, 4).unwrap();
        }
        meta
    }

    /// Carry-over: a hash the current condemned pointer already lists is a
    /// candidate again when it is still present, old and unprotected, so
    /// the pointer the new round publishes carries every chunk an
    /// interrupted or paused round could still delete. (Before: the
    /// candidate pass skipped anything already condemned, and a writer
    /// reading the new pointer trusted a dedup hit on a chunk the old
    /// round was deleting.)
    #[tokio::test]
    async fn a_round_carries_the_previous_rounds_undeleted_candidates() {
        use constellation_store_s3::{read_condemned, CompressionSetting};
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let chunks = Arc::new(ChunkStore::new(store.clone()));
        let old = ChunkHash::of(b"condemned by the previous round");
        let fresh = ChunkHash::of(b"first seen unreferenced by this round");
        for bytes in [
            &b"condemned by the previous round"[..],
            &b"first seen unreferenced by this round"[..],
        ] {
            chunks
                .put_chunk(&ChunkHash::of(bytes), bytes, CompressionSetting::RAW)
                .await
                .unwrap();
        }
        // The previous round's pointer, still current: it lists `old`.
        publish_condemned(&store, vec![old.to_hex()], 1)
            .await
            .unwrap();

        let meta = replica_with_live(1, None);
        let tail = GcTail::standalone(LogStore::new(store.clone()), &meta).unwrap();
        let mut lease = gc_lease(&store, 60_000).await;
        let marked = mark_chunks(
            &store,
            &chunks,
            &meta,
            LeaseMode::Cas,
            &fast_config(),
            false,
            &tail,
            &mut lease,
        )
        .await
        .unwrap()
        .expect("something to condemn");
        let candidates: HashSet<_> = marked.candidates.iter().filter_map(|m| m.hash).collect();
        assert!(
            candidates.contains(&old),
            "the previous round's undeleted candidate is carried"
        );
        assert!(candidates.contains(&fresh));
        let pointer = read_condemned(&store).await.unwrap().unwrap();
        assert_eq!(pointer.epoch, 2);
        assert!(pointer.hashes.contains(&old.to_hex()));
        assert!(pointer.hashes.contains(&fresh.to_hex()));
    }

    /// A round whose lease lapsed mid-wait (a pause of any length: the
    /// test's short TTL stands in for minutes) and whose lease another
    /// round then took deletes nothing more: its first fenced renewal is
    /// refused. The second round's replica sees `w` as live and drops it
    /// from its pointer; the first round's replica does not — without the
    /// fence the first round deleted `w` under a pointer that no longer
    /// listed it, the dangle the writer-side argument excludes.
    #[tokio::test]
    async fn a_paused_round_deletes_nothing_once_another_round_took_the_lease() {
        use constellation_store_s3::CompressionSetting;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let chunks = Arc::new(ChunkStore::new(store.clone()));
        let x = ChunkHash::of(b"unreferenced everywhere");
        let w = ChunkHash::of(b"referenced by the time the second round marks");
        for bytes in [
            &b"unreferenced everywhere"[..],
            &b"referenced by the time the second round marks"[..],
        ] {
            chunks
                .put_chunk(&ChunkHash::of(bytes), bytes, CompressionSetting::RAW)
                .await
                .unwrap();
        }

        // Round 1 marks and publishes, then pauses past its lease TTL.
        let meta_1 = replica_with_live(1, None);
        let tail_1 = GcTail::standalone(LogStore::new(store.clone()), &meta_1).unwrap();
        let mut lease_1 = gc_lease(&store, 30).await;
        let marked_1 = mark_chunks(
            &store,
            &chunks,
            &meta_1,
            LeaseMode::Cas,
            &fast_config(),
            false,
            &tail_1,
            &mut lease_1,
        )
        .await
        .unwrap()
        .expect("round 1 condemns x and w");
        assert_eq!(marked_1.condemned.hashes.len(), 2);
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;

        // Round 2 takes the lapsed lease and runs to completion. Its
        // replica has `w` live (a writer committed a manifest naming it
        // after re-uploading it — here simply: it is live).
        let meta_2 = replica_with_live(2, Some(w));
        let tail_2 = GcTail::standalone(LogStore::new(store.clone()), &meta_2).unwrap();
        let mut lease_2 = gc_lease(&store, 30).await;
        let report_2 = run_chunks(
            store.clone(),
            chunks.clone(),
            meta_2.clone(),
            LeaseMode::Cas,
            &fast_config(),
            false,
            None,
            &tail_2,
            &mut lease_2,
        )
        .await
        .unwrap();
        assert_eq!(
            report_2.deleted,
            vec![constellation_store_s3::layout::chunk_key(&x).to_string()]
        );
        assert!(!chunks.has_chunk(&x).await.unwrap());
        assert!(chunks.has_chunk(&w).await.unwrap());

        // Round 1 resumes: fenced before its first delete.
        let error = sweep_chunks(
            &store,
            &chunks,
            &meta_1,
            LeaseMode::Cas,
            &tail_1,
            &mut lease_1,
            marked_1,
        )
        .await
        .unwrap_err();
        assert!(
            error.downcast_ref::<crate::singleton::Fenced>().is_some(),
            "{error:#}"
        );
        assert!(
            chunks.has_chunk(&w).await.unwrap(),
            "a paused round must not delete under a pointer that is no longer its own"
        );
        lease_2.release().await;
    }

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
            completion_retention_ms: 0,
            snap_walk: SnapWalkMode::Diff,
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
            completion_retention_ms: 0,
            snap_walk: SnapWalkMode::Diff,
        };
        assert!(metadata_candidates(
            &store,
            None,
            &config,
            constellation_store_s3::lease::now_unix_ms()
        )
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
            completion_retention_ms: 0,
            snap_walk: SnapWalkMode::Diff,
        };
        let marked: Vec<u64> = metadata_candidates(
            &store,
            None,
            &config,
            constellation_store_s3::lease::now_unix_ms(),
        )
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

    /// A chunk re-uploaded after it was marked (a writer that found it
    /// condemned PUT it again, and may commit a manifest naming it after
    /// the round's last tail) is not deleted: the object is not the one
    /// the mark saw.
    #[test]
    fn a_chunk_uploaded_again_since_it_was_marked_is_kept() {
        let mark = |marked_ms: i64| Mark {
            key: "chunks/ab/cd/abcd".into(),
            rule: "orphan-horizon".into(),
            evidence: json!({"last_modified_ms": marked_ms, "horizon_ms": 0}),
            hash: None,
        };
        let t = 1_790_000_000_000;
        assert!(!reuploaded_since_marked(&mark(t), t));
        // A HEAD's second resolution: the same object, rounded.
        assert!(!reuploaded_since_marked(&mark(t + 999), t));
        assert!(!reuploaded_since_marked(&mark(t), t + 1000));
        assert!(reuploaded_since_marked(&mark(t), t + 5000));
        let no_evidence = Mark {
            evidence: json!({}),
            ..mark(t)
        };
        assert!(!reuploaded_since_marked(&no_evidence, t + 5000));
    }

    /// DESIGN.md §3 "Unlink while open", across nodes: node A unlinks a
    /// file node B has open (B applied the unlink and keeps the orphan in
    /// its own replica). Nothing in the bucket names the chunk any more,
    /// so a GC round run from A's replica would delete it under B's open
    /// handle — unless B's `holds/<node>.json` claims it. Once B closes
    /// the file and withdraws its hold, the next round reclaims the
    /// chunk.
    #[tokio::test]
    async fn a_chunk_held_open_on_another_node_survives_gc() {
        use constellation_fs_core::types::ROOT_INO;
        use constellation_meta::MetaStore;
        use constellation_store_s3::CompressionSetting;
        use std::sync::Mutex;

        struct Opens(Mutex<Vec<constellation_fs_core::Ino>>);
        impl crate::holds::OpenHandles for Opens {
            fn open_inos(&self) -> Vec<constellation_fs_core::Ino> {
                self.0.lock().unwrap().clone()
            }
        }

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let chunks = Arc::new(ChunkStore::new(store.clone()));
        let content = b"open on node b, unlinked on node a";
        let hash = ChunkHash::of(content);
        chunks
            .put_chunk(&hash, content, CompressionSetting::RAW)
            .await
            .unwrap();

        let meta_a = replica_with_live(1, Some(hash));
        let mut ship_a = crate::authority_driver::Standalone::new(
            meta_a.clone(),
            store.clone(),
            1,
            LeaseMode::Cas,
        );
        let meta_b = Arc::new(Meta::open_in_memory().unwrap());
        meta_b.set_node_prefix(2).unwrap();
        let mut ship_b = crate::authority_driver::Standalone::new(
            meta_b.clone(),
            store.clone(),
            2,
            LeaseMode::Cas,
        );
        assert!(ship_a.acquire().await.unwrap());
        ship_a.sync().await.unwrap();
        ship_b.tail_to_head().await.unwrap();
        let ino = meta_b.child_ino(ROOT_INO, "live").unwrap().unwrap();

        // A unlinks (nobody has it open on A: reaped at once, as the FUSE
        // unlink path does) and ships; B applies it with the file open.
        meta_a.unlink(ROOT_INO, "live").unwrap();
        meta_a.reap_orphan(ino).unwrap();
        ship_a.sync().await.unwrap();
        ship_b.tail_to_head().await.unwrap();
        assert_eq!(meta_b.orphans().unwrap(), vec![ino]);
        assert!(meta_b.manifest(ino).unwrap().is_some());

        let view = Arc::new(Opens(Mutex::new(vec![ino])));
        let sources = Arc::new(crate::holds::HoldSources::default());
        sources.register(
            1,
            Arc::downgrade(&view) as std::sync::Weak<dyn crate::holds::OpenHandles>,
        );
        let holds = crate::holds::Holds::new(
            store.clone(),
            chunks.clone(),
            meta_b.clone(),
            2,
            sources,
            crate::holds::HoldConfig {
                refresh: std::time::Duration::from_millis(10),
                ttl: std::time::Duration::from_secs(60),
            },
        );
        holds.refresh_once().await.unwrap();

        let tail_a = GcTail::standalone(LogStore::new(store.clone()), &meta_a).unwrap();
        let mut lease = gc_lease(&store, 60_000).await;
        let report = run_chunks(
            store.clone(),
            chunks.clone(),
            meta_a.clone(),
            LeaseMode::Cas,
            &fast_config(),
            false,
            None,
            &tail_a,
            &mut lease,
        )
        .await
        .unwrap();
        assert!(
            report.candidates.iter().all(|m| m.hash != Some(hash)),
            "a held chunk is not even a candidate: {:?}",
            report.candidates
        );
        assert!(chunks.has_chunk(&hash).await.unwrap());
        assert!(
            meta_b.manifest(ino).unwrap().is_some(),
            "B still serves its handle from the orphan record"
        );

        // B closes the file: the hold is withdrawn, the orphan reaped, and
        // the next round reclaims the chunk.
        view.0.lock().unwrap().clear();
        holds.refresh_once().await.unwrap();
        assert!(meta_b.orphans().unwrap().is_empty());
        let report = run_chunks(
            store.clone(),
            chunks.clone(),
            meta_a.clone(),
            LeaseMode::Cas,
            &fast_config(),
            false,
            None,
            &tail_a,
            &mut lease,
        )
        .await
        .unwrap();
        assert_eq!(report.deleted.len(), 1);
        assert!(!chunks.has_chunk(&hash).await.unwrap());
        lease.release().await;
    }

    /// Plan 29 M3a: chunk GC's liveness view is the *local* replica
    /// (`live_roots` reads `meta.live_manifests`), so a replica that has
    /// not tailed a writer's dedup-hit commit must not mark that chunk —
    /// `run_chunks`'s own `tail_to_head` call is what keeps it from
    /// doing so. Two in-process replicas share one `LogStore`: node A
    /// writes, node B is GC's (deliberately lagging) target.
    #[tokio::test]
    async fn gc_tails_a_lagging_replica_before_marking_a_deduplicated_chunk() {
        use constellation_fs_core::manifest::{ChunkInfo, Manifest};
        use constellation_fs_core::types::ROOT_INO;
        use constellation_fs_core::{ChunkHash, ChunkLayout};
        use constellation_meta::MetaStore;
        use constellation_store_s3::{ChunkStore, CompressionSetting, LogStore};
        use object_store::memory::InMemory;

        fn manifest_of(hash: ChunkHash, file_len: u64) -> Vec<u8> {
            Manifest {
                layout: ChunkLayout::new(4096),
                file_len,
                chunks: ChunkInfo::Inline([(0u64, hash)].into_iter().collect()),
            }
            .encode()
        }

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let chunks = Arc::new(ChunkStore::new(store.clone()));

        let meta_a = Arc::new(crate::mtree_publish::test_meta());
        meta_a.set_node_prefix(1).unwrap();
        let mut ship_a = crate::authority_driver::Standalone::new(
            meta_a.clone(),
            store.clone(),
            1,
            LeaseMode::Cas,
        );

        let meta_b = Arc::new(crate::mtree_publish::test_meta());
        meta_b.set_node_prefix(2).unwrap();
        let mut ship_b = crate::authority_driver::Standalone::new(
            meta_b.clone(),
            store.clone(),
            2,
            LeaseMode::Cas,
        );

        let content = b"dedup-race-content";
        let hash = ChunkHash::of(content);
        chunks
            .put_chunk(&hash, content, CompressionSetting::RAW)
            .await
            .unwrap();

        // 1. A creates `old` referencing the chunk and ships it; B tails.
        let old = meta_a.create(ROOT_INO, "old", 0o644, 0, 0).unwrap();
        meta_a
            .set_manifest(
                old.ino,
                &manifest_of(hash, content.len() as u64),
                content.len() as u64,
            )
            .unwrap();
        assert!(ship_a.acquire().await.unwrap());
        ship_a.sync().await.unwrap();
        ship_b.tail_to_head().await.unwrap();
        assert!(live_roots(&chunks, &meta_b).await.unwrap().contains(&hash));

        // 2. A unlinks `old` and ships; B tails again, so B now correctly
        //    (at this point) believes the chunk unreferenced.
        meta_a.unlink(ROOT_INO, "old").unwrap();
        ship_a.sync().await.unwrap();
        ship_b.tail_to_head().await.unwrap();
        assert!(!live_roots(&chunks, &meta_b).await.unwrap().contains(&hash));

        // 3. A creates `new` with the *same* content — a dedup hit
        //    against the very chunk B just decided is dead — and ships
        //    it. B is deliberately left behind: it is the "lagging" GC
        //    replica the bug is about.
        let new = meta_a.create(ROOT_INO, "new", 0o644, 0, 0).unwrap();
        meta_a
            .set_manifest(
                new.ino,
                &manifest_of(hash, content.len() as u64),
                content.len() as u64,
            )
            .unwrap();
        ship_a.sync().await.unwrap();
        // (`ship_b.tail_to_head()` deliberately not called here.)

        let config = GcConfig {
            horizon_ms: 0,
            retention_segments: 128,
            lease_ttl_ms: 1,
            completion_retention_ms: 0,
            snap_walk: SnapWalkMode::Diff,
        };
        let tail = GcTail::standalone(LogStore::new(store.clone()), &meta_b).unwrap();
        let mut lease = gc_lease(&store, 60_000).await;
        let report = run_chunks(
            store.clone(),
            chunks.clone(),
            meta_b.clone(),
            LeaseMode::Cas,
            &config,
            false,
            None,
            &tail,
            &mut lease,
        )
        .await
        .unwrap();

        assert!(
            report.candidates.iter().all(|m| m.hash != Some(hash)),
            "the dedup-referenced chunk must never be marked: {:?}",
            report.candidates
        );
        assert!(
            chunks.has_chunk(&hash).await.unwrap(),
            "chunk must survive a GC round run against a lagging replica"
        );
        // The mandatory tail is why: B must now know about `new` too.
        assert!(meta_b.getattr(new.ino).unwrap().is_some());
    }

    /// Plan 32 §0.2: a chunk round over a history of snapshots — a file
    /// rewritten, one deleted, a directory moved out of the snapshotted
    /// tree, a snapshot deleted from the middle of the chain — keeps in
    /// `diff` mode every chunk `full` mode keeps, and the sweep's refresh
    /// (same mode) deletes nothing a snapshot still names.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn per_chain_marking_keeps_everything_the_full_walk_keeps() {
        use constellation_fs_core::manifest::Manifest;
        use constellation_fs_core::types::ROOT_INO;
        use constellation_meta::MetaStore;
        use constellation_store_s3::CompressionSetting;
        const CS: u32 = 4096;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let chunks = Arc::new(ChunkStore::new(store.clone()));
        let meta = Arc::new(crate::mtree_publish::test_meta());
        let (manager, _nodes) = crate::snapshot::test_manager(meta.clone(), chunks.clone(), CS);
        let put = |tag: &'static str| {
            let chunks = chunks.clone();
            async move {
                let hash = ChunkHash::of(tag.as_bytes());
                chunks
                    .put_chunk(&hash, tag.as_bytes(), CompressionSetting::RAW)
                    .await
                    .unwrap();
                hash
            }
        };
        let write = |ino, hashes: Vec<ChunkHash>| {
            let len = hashes.len() as u64 * CS as u64;
            let (manifest, _) = Manifest::from_chunks(CS, len, hashes, 8, ChunkHash::of);
            meta.set_manifest(ino, &manifest.encode(), len).unwrap();
        };
        let mut segment = 0;
        let mut ship = || {
            segment += 1;
            let rows = meta.take_journal(usize::MAX).unwrap();
            let seqs: Vec<u64> = rows.iter().map(|(seq, _)| *seq).collect();
            meta.ack_journal_rows_at(&seqs, segment).unwrap();
        };

        let vol = meta.mkdir(ROOT_INO, "vol", 0o755, 0, 0).unwrap().ino;
        let sub = meta.mkdir(vol, "sub", 0o755, 0, 0).unwrap().ino;
        let file = meta.create(vol, "f", 0o644, 0, 0).unwrap().ino;
        let doomed = meta.create(sub, "d", 0o644, 0, 0).unwrap().ino;
        let (v1, v2, v3, d, live) = (
            put("v1").await,
            put("v2").await,
            put("v3").await,
            put("in a moved-out dir").await,
            put("live").await,
        );
        let orphan = put("never referenced").await;
        write(file, vec![v1]);
        write(doomed, vec![d]);
        ship();
        manager.create("/vol", "s1").await.unwrap();
        write(file, vec![v2]);
        ship();
        manager.create("/vol", "s2").await.unwrap();
        // `sub` leaves the tree, then its file goes; `f` is rewritten.
        meta.rename(vol, "sub", ROOT_INO, "away").unwrap();
        meta.unlink(sub, "d").unwrap();
        write(file, vec![v3]);
        ship();
        manager.create("/vol", "s3").await.unwrap();
        write(file, vec![live]);
        ship();
        manager.create("/vol", "s4").await.unwrap();
        // From the middle: `v2` is now named by no snapshot at all.
        manager.delete("/vol", "s2", false).await.unwrap();
        ship();

        let mut candidates = std::collections::BTreeMap::new();
        for mode in [SnapWalkMode::Full, SnapWalkMode::Diff] {
            let config = GcConfig {
                snap_walk: mode,
                ..fast_config()
            };
            let tail = GcTail::standalone(LogStore::new(store.clone()), &meta).unwrap();
            let mut lease = gc_lease(&store, 60_000).await;
            let report = mark_chunks(
                &store,
                &chunks,
                &meta,
                LeaseMode::Cas,
                &config,
                true,
                &tail,
                &mut lease,
            )
            .await
            .unwrap();
            lease.release().await;
            let Err(report) = report else {
                panic!("verify-only returns its report");
            };
            let marked: HashSet<ChunkHash> =
                report.candidates.iter().filter_map(|m| m.hash).collect();
            candidates.insert(format!("{mode:?}"), marked);
        }
        let (diff, full) = (&candidates["Diff"], &candidates["Full"]);
        assert!(
            diff.is_subset(full),
            "diff mode condemns what full mode keeps: {:?}",
            diff.difference(full).collect::<Vec<_>>()
        );
        assert_eq!(diff, full);
        for kept in [v1, v3, d, live] {
            assert!(!diff.contains(&kept), "{kept:?} is still named");
        }
        assert!(diff.contains(&v2), "only the deleted snapshot named v2");
        assert!(diff.contains(&orphan));

        // A real round in diff mode deletes exactly those, nothing named.
        let mut lease = gc_lease(&store, 60_000).await;
        let tail = GcTail::standalone(LogStore::new(store.clone()), &meta).unwrap();
        let report = run_chunks(
            store.clone(),
            chunks.clone(),
            meta.clone(),
            LeaseMode::Cas,
            &fast_config(),
            false,
            None,
            &tail,
            &mut lease,
        )
        .await
        .unwrap();
        assert_eq!(report.deleted.len(), 2, "{:?}", report.deleted);
        for kept in [v1, v3, d, live] {
            assert!(chunks.has_chunk(&kept).await.unwrap());
        }
        assert!(!chunks.has_chunk(&v2).await.unwrap());
    }

    /// Every GC journal entry, oldest first.
    async fn journal(store: &Arc<dyn ObjectStore>) -> Vec<GcJournalEntry> {
        let mut entries = Vec::new();
        for object in store
            .list(Some(&constellation_store_s3::layout::gc_journal_prefix()))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
        {
            let bytes = store
                .get(&object.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            entries.push(serde_json::from_slice::<GcJournalEntry>(&bytes).unwrap());
        }
        entries.sort_by_key(|entry| entry.ts);
        entries
    }

    /// One snapshot `/vol@s1` whose only file named chunk `frozen`, now
    /// rewritten to `live`: after it, `frozen` is protected by the
    /// snapshot alone.
    struct SnapFixture {
        store: Arc<dyn ObjectStore>,
        chunks: Arc<ChunkStore>,
        meta: Arc<Meta>,
        _nodes: tempfile::TempDir,
        id: String,
        frozen: ChunkHash,
        live: ChunkHash,
    }

    async fn snap_fixture() -> SnapFixture {
        use constellation_fs_core::manifest::Manifest;
        use constellation_fs_core::types::ROOT_INO;
        use constellation_meta::MetaStore;
        use constellation_store_s3::CompressionSetting;
        const CS: u32 = 4096;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let chunks = Arc::new(ChunkStore::new(store.clone()));
        let meta = Arc::new(crate::mtree_publish::test_meta());
        let (manager, nodes) = crate::snapshot::test_manager(meta.clone(), chunks.clone(), CS);
        let mut segment = 0;
        let mut ship = || {
            segment += 1;
            let rows = meta.take_journal(usize::MAX).unwrap();
            let seqs: Vec<u64> = rows.iter().map(|(seq, _)| *seq).collect();
            meta.ack_journal_rows_at(&seqs, segment).unwrap();
        };
        let (frozen, live) = (ChunkHash::of(b"frozen"), ChunkHash::of(b"live"));
        for (hash, bytes) in [(frozen, &b"frozen"[..]), (live, &b"live"[..])] {
            chunks
                .put_chunk(&hash, bytes, CompressionSetting::RAW)
                .await
                .unwrap();
        }
        let write = |ino, hash| {
            let (manifest, _) = Manifest::from_chunks(CS, CS as u64, vec![hash], 8, ChunkHash::of);
            meta.set_manifest(ino, &manifest.encode(), CS as u64)
                .unwrap();
        };
        let vol = meta.mkdir(ROOT_INO, "vol", 0o755, 0, 0).unwrap().ino;
        let file = meta.create(vol, "f", 0o644, 0, 0).unwrap().ino;
        write(file, frozen);
        ship();
        manager.create("/vol", "s1").await.unwrap();
        write(file, live);
        ship();
        SnapFixture {
            store,
            chunks,
            meta,
            _nodes: nodes,
            id: constellation_store_s3::snapshot_id("/vol", "s1"),
            frozen,
            live,
        }
    }

    impl SnapFixture {
        async fn round(&self, config: &GcConfig, verify_only: bool) -> GcReport {
            let mut lease = gc_lease(&self.store, 60_000).await;
            let tail = GcTail::standalone(LogStore::new(self.store.clone()), &self.meta).unwrap();
            let report = match verify_only {
                true => mark_chunks(
                    &self.store,
                    &self.chunks,
                    &self.meta,
                    LeaseMode::Cas,
                    config,
                    true,
                    &tail,
                    &mut lease,
                )
                .await
                .unwrap()
                .err()
                .expect("verify-only returns its report"),
                false => run_chunks(
                    self.store.clone(),
                    self.chunks.clone(),
                    self.meta.clone(),
                    LeaseMode::Cas,
                    config,
                    false,
                    None,
                    &tail,
                    &mut lease,
                )
                .await
                .unwrap(),
            };
            lease.release().await;
            report
        }

        fn key(&self) -> String {
            constellation_store_s3::layout::snapshot(&self.id).to_string()
        }

        async fn object(&self) -> Option<SnapshotRecord> {
            SnapshotStore::new(self.store.clone())
                .get(&self.id)
                .await
                .unwrap()
        }

        /// Drop the row and keep the object: a delete interrupted between
        /// its two steps.
        fn orphan_the_object(&self) {
            use constellation_meta::MetaStore;
            assert!(self.meta.delete_snapshot_by_id(&self.id).unwrap());
            let rows = self.meta.take_journal(usize::MAX).unwrap();
            let seqs: Vec<u64> = rows.iter().map(|(seq, _)| *seq).collect();
            self.meta.ack_journal_rows_at(&seqs, 1_000).unwrap();
        }
    }

    fn rules(report: &GcReport) -> Vec<(&str, &str)> {
        report
            .candidates
            .iter()
            .filter(|mark| mark.key.starts_with("snaps/"))
            .map(|mark| (mark.rule.as_str(), mark.key.as_str()))
            .collect()
    }

    /// Plan 32 §6.1: a real round records the census of its `chunks/`
    /// LIST — every object and its stored bytes — in its report and in
    /// `gc/summary.json`; a verify-only round reports it and writes
    /// nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_round_records_its_chunk_census() {
        let fx = snap_fixture().await;
        let mut objects = 0;
        let mut bytes = 0;
        for object in fx
            .store
            .list(Some(&Path::from("chunks")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
        {
            objects += 1;
            bytes += object.size;
        }
        assert!(objects >= 2);
        let config = GcConfig {
            horizon_ms: 3_600_000,
            ..fast_config()
        };
        let report = fx.round(&config, true).await;
        let census = report.census.expect("a verify-only round reports it");
        assert_eq!(
            (census.chunk_objects, census.physical_bytes),
            (objects, bytes)
        );
        assert!(constellation_store_s3::read_chunk_census(&fx.store)
            .await
            .unwrap()
            .is_none());
        let report = fx.round(&config, false).await;
        let census = report.census.expect("a real round reports it");
        assert_eq!(
            (census.chunk_objects, census.physical_bytes),
            (objects, bytes)
        );
        assert!(census.as_of_ms > 0);
        assert_eq!(
            constellation_store_s3::read_chunk_census(&fx.store)
                .await
                .unwrap(),
            Some(census)
        );
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["census"]["chunk_objects"], objects);
    }

    /// Plan 32 §0.3: a `snaps/` object with no row is left alone while it
    /// is younger than `gc.horizon` (a create in flight writes the object
    /// first), reported but untouched by a verify-only round, and deleted
    /// and journaled by a real one — which still keeps the orphan's chunks
    /// for the rest of that round. The next round, with no root left,
    /// collects them. Both walk modes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_orphan_snapshot_object_is_deleted_after_the_horizon() {
        for mode in [SnapWalkMode::Diff, SnapWalkMode::Full] {
            let fx = snap_fixture().await;
            fx.orphan_the_object();
            let config = GcConfig {
                snap_walk: mode,
                ..fast_config()
            };

            // Younger than the horizon: not even a candidate.
            let young = GcConfig {
                horizon_ms: 3_600_000,
                ..config.clone()
            };
            let report = fx.round(&young, false).await;
            assert!(
                rules(&report).is_empty(),
                "{mode:?}: {:?}",
                report.candidates
            );
            assert!(fx.object().await.is_some());

            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            // Verify-only: reported, not acted on.
            let report = fx.round(&config, true).await;
            assert_eq!(rules(&report), [("snap-orphan-object", fx.key().as_str())]);
            assert!(report.deleted.is_empty());
            assert!(
                fx.object().await.is_some(),
                "{mode:?}: verify-only deleted it"
            );
            assert!(journal(&fx.store).await.is_empty());

            // A real round deletes it, journals it, and keeps its chunk.
            let report = fx.round(&config, false).await;
            assert_eq!(rules(&report), [("snap-orphan-object", fx.key().as_str())]);
            assert!(report.deleted.contains(&fx.key()), "{:?}", report.deleted);
            assert!(fx.object().await.is_none());
            assert!(
                fx.chunks.has_chunk(&fx.frozen).await.unwrap(),
                "{mode:?}: the round that deleted the orphan collected its chunk"
            );
            let entries = journal(&fx.store).await;
            let entry = entries
                .iter()
                .find(|entry| entry.key == fx.key())
                .expect("journaled");
            assert_eq!(entry.rule, "snap-orphan-object");
            assert_eq!(entry.evidence["id"], fx.id.as_str());
            assert_eq!(entry.evidence["path"], "/vol");
            assert_eq!(entry.evidence["name"], "s1");
            assert!(entry.evidence["created_unix_ms"].as_i64().unwrap() > 0);

            // The next round has no root for it any more.
            let report = fx.round(&config, false).await;
            assert!(rules(&report).is_empty());
            assert!(!fx.chunks.has_chunk(&fx.frozen).await.unwrap(), "{mode:?}");
            assert!(fx.chunks.has_chunk(&fx.live).await.unwrap());
        }
    }

    /// Plan 32 §0.3: a row whose object is gone (which should not happen)
    /// gets the object back, rebuilt from the row — plan 32's extensions
    /// included — byte-for-byte the record creation wrote; verify-only
    /// only reports it. The snapshot's chunk is never at risk meanwhile.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_row_without_its_object_gets_it_back() {
        let fx = snap_fixture().await;
        let original = fx.object().await.expect("created");
        assert!(
            original.refer_bytes.is_some(),
            "the fixture exercises the extensions"
        );
        assert!(SnapshotStore::new(fx.store.clone())
            .delete("/vol", "s1")
            .await
            .unwrap());

        let report = fx.round(&fast_config(), true).await;
        assert_eq!(
            rules(&report),
            [("snap-object-restored", fx.key().as_str())]
        );
        assert!(report.restored.is_empty());
        assert!(fx.object().await.is_none(), "verify-only restored it");
        assert!(journal(&fx.store).await.is_empty());

        let report = fx.round(&fast_config(), false).await;
        assert_eq!(
            rules(&report),
            [("snap-object-restored", fx.key().as_str())]
        );
        assert_eq!(report.restored, [fx.key()]);
        assert_eq!(fx.object().await, Some(original));
        assert!(fx.chunks.has_chunk(&fx.frozen).await.unwrap());
        let entries = journal(&fx.store).await;
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_eq!(entries[0].rule, "snap-object-restored");
        assert_eq!(entries[0].key, fx.key());
        assert_eq!(entries[0].evidence["id"], fx.id.as_str());

        // And the copies agree again: nothing more to do.
        let report = fx.round(&fast_config(), false).await;
        assert!(rules(&report).is_empty());
        assert!(report.restored.is_empty());
    }

    /// A row seen without its object that is gone by the time the round
    /// has waited and tailed again was a delete in flight, not damage:
    /// nothing is restored. The deleter's `SnapDelete` lands *in* that
    /// tail — the reconciliation's only one, served here through the
    /// daemon channel — so the row is certainly there at the scan.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delete_in_flight_is_not_restored() {
        let fx = snap_fixture().await;
        assert!(SnapshotStore::new(fx.store.clone())
            .delete("/vol", "s1")
            .await
            .unwrap());
        let config = GcConfig {
            lease_ttl_ms: 1_500,
            ..fast_config()
        };
        let (tx, mut requests) = tokio::sync::mpsc::unbounded_channel();
        let meta = fx.meta.clone();
        let id = fx.id.clone();
        let deleter = tokio::spawn(async move {
            let mut tails = 0;
            while let Some(request) = requests.recv().await {
                let crate::sync::SyncRequest::TailToHead { reply } = request else {
                    panic!("reconciliation asks the sync task for nothing but a tail");
                };
                tails += 1;
                assert!(
                    meta.delete_snapshot_by_id(&id).unwrap(),
                    "the row is there until the tail brings its delete"
                );
                reply.send(Ok(())).unwrap();
            }
            tails
        });
        let tail = GcTail::Daemon(tx);
        let mut lease = gc_lease(&fx.store, 60_000).await;
        let found = reconcile_snapshots(
            &fx.store,
            &fx.meta,
            LeaseMode::Cas,
            &config,
            false,
            &tail,
            &mut lease,
        )
        .await
        .unwrap();
        lease.release().await;
        drop(tail);
        assert_eq!(
            deleter.await.unwrap(),
            1,
            "the row was seen without its object"
        );
        assert!(found.restored.is_empty());
        assert!(found.marks.is_empty(), "{:?}", found.marks);
        assert!(fx.object().await.is_none());
        assert!(journal(&fx.store)
            .await
            .iter()
            .all(|entry| !entry.key.starts_with("snaps/")));
    }

    /// Plan 32 §11 "Compliance and performance" (its "GC round time", as
    /// the mark: the one phase the two modes do differently): one GC mark (verify-only
    /// `mark_chunks`: tail, snapshot reconciliation, live roots, the
    /// `chunks/` LIST, snapshot roots) with `CONSTELLATION_GC_SNAP_WALK`
    /// `full` and `diff`, over 300 snapshots of a 100k-file tree. The tree
    /// is created once (1,000 directories × 100 one-chunk files under
    /// `/vol`); between snapshots `CONSTELLATION_BENCH_TOUCH` (20) random
    /// files are rewritten, so diffs are small. In-process, in-memory
    /// bucket: what it measures is the walk's CPU and node-cache cost, not
    /// S3 latency (which both modes would pay per tree node read).
    ///
    /// Knobs (env): `CONSTELLATION_BENCH_FILES` (100000),
    /// `CONSTELLATION_BENCH_SNAPSHOTS` (300), `CONSTELLATION_BENCH_TOUCH`
    /// (20), `CONSTELLATION_BENCH_FULL_LIMIT_S` (1800: a `full` mark still
    /// running then is stopped, and the per-snapshot rate of the full walk
    /// is measured over a few snapshots instead and extrapolated). Run:
    ///
    /// ```text
    /// cargo test --release -p constellation-engine --lib \
    ///     gc::tests::bench_gc_mark_full_versus_diff -- --ignored --nocapture
    /// ```
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "a measurement, not a check (minutes in release)"]
    async fn bench_gc_mark_full_versus_diff() {
        use constellation_fs_core::manifest::Manifest;
        use constellation_fs_core::types::ROOT_INO;
        use constellation_meta::MetaStore;
        use constellation_store_s3::CompressionSetting;
        use std::time::{Duration, Instant};
        const CS: u32 = 4096;
        const PER_DIR: usize = 100;
        let knob = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(default)
        };
        let files_n = knob("CONSTELLATION_BENCH_FILES", 100_000) as usize;
        let snapshots = knob("CONSTELLATION_BENCH_SNAPSHOTS", 300) as usize;
        let touch = knob("CONSTELLATION_BENCH_TOUCH", 20) as usize;
        let full_limit = Duration::from_secs(knob("CONSTELLATION_BENCH_FULL_LIMIT_S", 1800));

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let chunks = Arc::new(ChunkStore::new(store.clone()));
        let meta = Arc::new(crate::mtree_publish::test_meta());
        let (manager, _nodes) = crate::snapshot::test_manager(meta.clone(), chunks.clone(), CS);
        let write = |ino: u64, tag: String| {
            let chunks = chunks.clone();
            let meta = meta.clone();
            async move {
                let bytes = tag.into_bytes();
                let hash = ChunkHash::of(&bytes);
                chunks
                    .put_chunk(&hash, &bytes, CompressionSetting::RAW)
                    .await
                    .unwrap();
                let len = bytes.len() as u64;
                let (manifest, _) = Manifest::from_chunks(CS, len, vec![hash], 8, ChunkHash::of);
                meta.set_manifest(ino, &manifest.encode(), len).unwrap();
            }
        };
        let mut segment = 0;
        let mut ship = || {
            segment += 1;
            let rows = meta.take_journal(usize::MAX).unwrap();
            let seqs: Vec<u64> = rows.iter().map(|(seq, _)| *seq).collect();
            meta.ack_journal_rows_at(&seqs, segment).unwrap();
        };

        let started = Instant::now();
        let vol = meta.mkdir(ROOT_INO, "vol", 0o755, 0, 0).unwrap().ino;
        let mut files = Vec::with_capacity(files_n);
        let mut dir = vol;
        for f in 0..files_n {
            if f % PER_DIR == 0 {
                dir = meta
                    .mkdir(vol, &format!("d{}", f / PER_DIR), 0o755, 0, 0)
                    .unwrap()
                    .ino;
            }
            let ino = meta.create(dir, &format!("f{f}"), 0o644, 0, 0).unwrap().ino;
            write(ino, format!("file {f} v0")).await;
            files.push(ino);
        }
        let tree_built = started.elapsed();
        // A fixed LCG: the same history on every run.
        let mut state = 0x5eed_u64;
        let mut below = |n: usize| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((state >> 33) as usize) % n
        };
        let mut slowest_snapshot = Duration::ZERO;
        for s in 0..snapshots {
            for c in 0..touch {
                let ino = files[below(files.len())];
                write(ino, format!("file {ino} s{s} c{c}")).await;
            }
            ship();
            let one = Instant::now();
            manager.create("/vol", &format!("s{s:03}")).await.unwrap();
            slowest_snapshot = slowest_snapshot.max(one.elapsed());
        }
        eprintln!(
            "BENCH built {files_n} files in {tree_built:?}; {snapshots} snapshots ({touch} rewrites each, slowest create {slowest_snapshot:?}) in {:?} total",
            started.elapsed()
        );

        let mark = |mode: SnapWalkMode| {
            let (store, chunks, meta) = (store.clone(), chunks.clone(), meta.clone());
            async move {
                let config = GcConfig {
                    snap_walk: mode,
                    ..fast_config()
                };
                let tail = GcTail::standalone(LogStore::new(store.clone()), &meta).unwrap();
                let mut lease = gc_lease(&store, 3_600_000).await;
                let started = Instant::now();
                let report = mark_chunks(
                    &store,
                    &chunks,
                    &meta,
                    LeaseMode::Cas,
                    &config,
                    true,
                    &tail,
                    &mut lease,
                )
                .await
                .unwrap();
                let elapsed = started.elapsed();
                lease.release().await;
                let Err(report) = report else {
                    panic!("verify-only returns its report");
                };
                let candidates: HashSet<ChunkHash> =
                    report.candidates.iter().filter_map(|m| m.hash).collect();
                (elapsed, candidates)
            }
        };
        // Diff first, then full, then diff again (warm caches either way:
        // each mark builds its own scratch node cache).
        let (diff, diff_set) = mark(SnapWalkMode::Diff).await;
        eprintln!("BENCH mark diff: {diff:?} ({} candidates)", diff_set.len());
        // Its own task, so the timer fires even while the mark's walk
        // holds a worker thread (`block_in_place`); a stopped mark is
        // aborted at its next await and its blocking reads finish unseen.
        let full_mark = tokio::spawn(mark(SnapWalkMode::Full));
        let abort = full_mark.abort_handle();
        let stopped = match tokio::time::timeout(full_limit, full_mark).await {
            Ok(Err(panicked)) => panic!("the full mark failed: {panicked}"),
            Ok(Ok((full, full_set))) => {
                eprintln!(
                    "BENCH mark full: {full:?} ({} candidates); full/diff = {:.1}x",
                    full_set.len(),
                    full.as_secs_f64() / diff.as_secs_f64()
                );
                assert_eq!(diff_set, full_set, "both modes condemn the same chunks");
                false
            }
            Err(_) => {
                abort.abort();
                // Stopped: the full walk's cost is one subtree walk per
                // snapshot, so time a few of those (the same function the
                // `full` mark calls, over one shared reader as it does).
                use crate::snapshot::{snapshot_chunk_refs, SnapshotRoot, TreeAccess};
                let sample = 5.min(snapshots);
                let scratch = ScratchDir::new("bench-full-sample").unwrap();
                let reader = crate::mtree_read::ChainReader::for_store(
                    store.clone(),
                    chunks.e2e_keys(),
                    &scratch.0,
                )
                .unwrap();
                reader.cache.refresh_catalog().await.unwrap();
                let tree = TreeAccess::from_reader(reader);
                let records = SnapshotStore::new(store.clone()).list().await.unwrap();
                let started = Instant::now();
                for record in records.iter().take(sample) {
                    let root = SnapshotRoot::of_record(record).unwrap();
                    snapshot_chunk_refs(&chunks, &tree, &root).await.unwrap();
                }
                let per = started.elapsed() / sample as u32;
                eprintln!(
                    "BENCH mark full: STOPPED after {full_limit:?}; per-snapshot full walk {per:?} (mean of {sample}), so ≈ {:?} for {} snapshots",
                    per * records.len() as u32,
                    records.len()
                );
                true
            }
        };
        // The stopped mark still holds the `_gc` lease.
        if !stopped {
            let (diff2, _) = mark(SnapWalkMode::Diff).await;
            eprintln!("BENCH mark diff (again): {diff2:?}");
        }
    }
}
