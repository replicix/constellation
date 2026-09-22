//! Plan 28 S7b: garbage collection of the metadata tree — commit
//! retention, reachability sweep, and paced pack compaction.
//!
//! S7a built the machinery in `store-s3` (`mark`, `Sweep`, `Compactor`)
//! and deliberately decided nothing about *when* or *how fast*. This is
//! that policy, run by whoever holds the `_gc` singleton lease, as one
//! phase of `gc::run` (the daily tick and `constellation gc`).
//!
//! ## One round
//!
//! 1. **Retention.** Commit objects beyond the newest
//!    `CONSTELLATION_COMMIT_RETENTION` that are also older than
//!    `CONSTELLATION_COMMIT_RETENTION_S` are deleted. A commit is never
//!    load-bearing (§P10b): bootstrap uses the head, log retention floors
//!    on the head, and a snapshot retains its own root hash rather than a
//!    commit. The head is always kept.
//! 2. **Mark** from the root set: every retained commit, every `snaps/*`
//!    root, and any metadata node a live `holds/*` names. Consecutive
//!    commits share almost everything, so this costs their differences.
//! 3. **Classify** every pack against the mark (`Sweep::classify`), and
//!    **condemn** the dead, the partially dead, and bodies whose `.idx`
//!    never arrived that are older than the GC horizon.
//! 4. **Wait one lease TTL**, then **re-mark** from the then-current
//!    roots and act only on condemned packs that are *still* dead or
//!    partially dead. This is the delete-vs-dedup handshake: a publisher
//!    never deduplicates a node against a condemned pack and re-checks
//!    the list just before its commit CAS (`mtree_publish`), so any commit
//!    that could still name a node in a condemned pack either landed
//!    before the re-mark (and keeps that pack alive) or re-uploaded the
//!    node into a pack of its own.
//! 5. **Delete** fully dead packs (no bytes moved), then **compact** the
//!    partially dead ones — live nodes rewritten into fresh packs, which
//!    are durable before the originals go (S7a's ordering invariant) —
//!    under `CONSTELLATION_COMPACT_BYTES_PER_S`. The position is saved
//!    in the replica's kv after every batch, so an interrupted round
//!    resumes where it stopped instead of restarting.
//! 6. **Clear** the condemned list.
//!
//! ## What is not swept
//!
//! `blobs/` — values over 1 KiB spilled out of tree nodes — is not
//! collected yet. Spills are rare (a big xattr, a PATH_MAX symlink), an
//! unreferenced blob only costs space, and a blob is content-addressed
//! under the same key every time it is written, so it cannot use the
//! re-upload trick packs use; it needs its own two-mark horizon.

use crate::mtree_read::ChainReader;
use anyhow::{Context, Result};
use constellation_meta::Meta;
use constellation_mtree::NodeHash;
use constellation_store_s3::{
    layout, CommitChain, CompactionPacer, Compactor, PackCatalog, PackHash, Sweep,
};
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

/// Newest commits always kept, whatever their age.
pub const DEFAULT_COMMIT_RETENTION: usize = 64;
/// Commits younger than this are kept, however many there are.
pub const DEFAULT_COMMIT_RETENTION_S: u64 = 86_400;
/// Compaction read budget. §14.5 sized compaction at roughly the commit
/// write rate; 32 MiB/s is well above a busy node's metadata write rate
/// and a small fraction of what S7a's compactor sustains (~260 MiB/s at
/// four threads), so a round finishes promptly without saturating a
/// link.
pub const DEFAULT_COMPACT_BYTES_PER_S: u64 = 32 << 20;
/// Packs per delete/compact batch: the granularity of the saved cursor.
const BATCH_PACKS: usize = 32;

const KV_CURSOR_DEAD: &str = "mtree_gc/cursor/dead";
const KV_CURSOR_COMPACT: &str = "mtree_gc/cursor/compact";

#[derive(Clone, Debug)]
pub struct MtreeGcConfig {
    pub retention: usize,
    pub retention_ms: i64,
    pub compact_bytes_per_s: u64,
    pub threads: usize,
    pub horizon_ms: i64,
    pub grace: Duration,
}

impl MtreeGcConfig {
    /// `CONSTELLATION_COMMIT_RETENTION` (count, default 64, at least 1),
    /// `CONSTELLATION_COMMIT_RETENTION_S` (seconds, default 86400),
    /// `CONSTELLATION_COMPACT_BYTES_PER_S` (bytes/s, default 32 MiB, `0`
    /// unpaced), `CONSTELLATION_GC_THREADS` (S7a's knob). The horizon and
    /// the grace wait come from the chunk GC's config, so both planes
    /// share one notion of "old enough" and one lease TTL.
    pub fn from_env(horizon_ms: i64, lease_ttl_ms: u64) -> MtreeGcConfig {
        let number = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .unwrap_or(default)
        };
        MtreeGcConfig {
            retention: number(
                "CONSTELLATION_COMMIT_RETENTION",
                DEFAULT_COMMIT_RETENTION as u64,
            )
            .max(1) as usize,
            retention_ms: number(
                "CONSTELLATION_COMMIT_RETENTION_S",
                DEFAULT_COMMIT_RETENTION_S,
            ) as i64
                * 1000,
            compact_bytes_per_s: number(
                "CONSTELLATION_COMPACT_BYTES_PER_S",
                DEFAULT_COMPACT_BYTES_PER_S,
            ),
            threads: constellation_store_s3::gc_threads(),
            horizon_ms,
            grace: Duration::from_millis(lease_ttl_ms),
        }
    }
}

/// What a round did (or, verify-only, would do).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MtreeGcReport {
    pub commits: usize,
    pub commits_deleted: Vec<u64>,
    pub live_nodes: usize,
    pub packs: usize,
    pub packs_dead: usize,
    pub packs_partially_dead: usize,
    pub packs_incomplete_old: usize,
    pub packs_deleted: usize,
    pub packs_rewritten: usize,
    pub packs_written: usize,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub delete_failures: usize,
}

/// Sleeps `bytes / rate` before each batch.
struct RatePacer(u64);

impl CompactionPacer for RatePacer {
    fn pace(&self, bytes: u64) -> Duration {
        if self.0 == 0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(bytes as f64 / self.0 as f64)
        }
    }
}

/// Run one round against `store`. `keys` is the filesystem's keyring
/// (node identity is keyed on an E2E filesystem), `meta` holds the
/// restartable cursor.
pub fn run<'a>(
    store: Arc<dyn ObjectStore>,
    keys: Option<&'a constellation_store_s3::SharedE2eKeys>,
    meta: &'a Meta,
    config: &'a MtreeGcConfig,
    verify_only: bool,
) -> futures::future::BoxFuture<'a, Result<MtreeGcReport>> {
    // Boxed with an explicit `Send` bound: the daemon spawns the GC tick,
    // and leaving this future's `Send`-ness to inference inside that
    // spawn trips rustc's higher-ranked lifetime limits.
    Box::pin(run_inner(store, keys, meta, config, verify_only))
}

async fn run_inner(
    store: Arc<dyn ObjectStore>,
    keys: Option<&constellation_store_s3::SharedE2eKeys>,
    meta: &Meta,
    config: &MtreeGcConfig,
    verify_only: bool,
) -> Result<MtreeGcReport> {
    let sealing = constellation_store_s3::TreeSealing::for_keys(keys);
    let chain = CommitChain::new(store.clone()).with_sealing(sealing.clone());
    let mut report = MtreeGcReport::default();
    let seqs = chain.list_from(0).await?;
    if seqs.is_empty() {
        return Ok(report);
    }
    report.commits = seqs.len();
    let now = constellation_store_s3::lease::now_unix_ms();

    // 1. Retention.
    let expired = expired_commits(
        store.clone(),
        sealing.clone(),
        seqs.clone(),
        config.retention,
        config.retention_ms,
        now,
    )
    .await?;
    if !verify_only {
        for seq in expired.clone() {
            store.delete(&layout::commit(seq)).await?;
        }
    }
    report.commits_deleted = expired.clone();

    let scratch = ScratchDir::new()?;
    let reader = ChainReader::for_store(store.clone(), keys, &scratch.0)?;

    // 2-3. Mark and classify.
    let catalog = PackCatalog::load(reader.cache.packs()).await?;
    catalog.attach_to(&reader.cache);
    report.packs = catalog.len();
    let retained = retained_commits(store.clone(), expired.clone()).await?;
    let live = mark_roots(
        store.clone(),
        sealing.clone(),
        reader.cache.clone(),
        retained,
        config.threads,
        now,
    )
    .await?;
    report.live_nodes = live.len();
    let sweep = Sweep::classify(&catalog, &live);
    let incomplete = old_incomplete(&store, &catalog, now - config.horizon_ms).await?;
    report.packs_dead = sweep.fully_dead().len();
    report.packs_partially_dead = sweep.partially_dead().len();
    report.packs_incomplete_old = incomplete.len();
    let condemned: HashSet<PackHash> = sweep
        .fully_dead()
        .into_iter()
        .chain(sweep.partially_dead())
        .chain(incomplete.iter().copied())
        .collect();
    if verify_only || condemned.is_empty() {
        return Ok(report);
    }

    // 4. Handshake: condemn, wait, re-mark.
    constellation_store_s3::publish_condemned_packs(&store, &condemned, now as u64, now).await?;
    tokio::time::sleep(config.grace).await;
    let now = constellation_store_s3::lease::now_unix_ms();
    let retained = retained_commits(store.clone(), expired.clone()).await?;
    reader.cache.refresh_catalog().await?;
    let live = mark_roots(
        store.clone(),
        sealing.clone(),
        reader.cache.clone(),
        retained,
        config.threads,
        now,
    )
    .await?;
    let mut condemned_catalog = PackCatalog::default();
    for pack in &condemned {
        if let Some(entry) = catalog.get(pack) {
            condemned_catalog.insert(*pack, entry.index.clone(), entry.body_bytes);
        }
    }
    let sweep = Sweep::classify(&condemned_catalog, &live);

    // 5. Delete, then compact, from the saved cursors.
    let compactor = Compactor::new(
        reader.cache.packs().clone(),
        reader.config.hasher,
        config.threads,
    )?
    .with_pacer(Arc::new(RatePacer(config.compact_bytes_per_s)));
    let mut cursor = load_cursor(meta, KV_CURSOR_DEAD)?;
    loop {
        let batch = compactor.delete_dead(&sweep, BATCH_PACKS, cursor).await?;
        report.packs_deleted += batch.deleted.len();
        report.delete_failures += batch.delete_failures.len();
        cursor = batch.next;
        save_cursor(meta, KV_CURSOR_DEAD, cursor)?;
        if cursor.is_none() {
            break;
        }
    }
    let mut cursor = load_cursor(meta, KV_CURSOR_COMPACT)?;
    loop {
        let batch = compactor
            .compact(&sweep, &live, BATCH_PACKS, cursor)
            .await?;
        report.packs_rewritten += batch.deleted.len();
        report.packs_written += batch.written.len();
        report.bytes_read += batch.bytes_read;
        report.bytes_written += batch.bytes_written;
        report.delete_failures += batch.delete_failures.len();
        cursor = batch.next;
        save_cursor(meta, KV_CURSOR_COMPACT, cursor)?;
        if cursor.is_none() {
            break;
        }
    }
    // Bodies whose index never arrived: still incomplete after the wait
    // means no writer is finishing them.
    for pack in incomplete.clone() {
        let hex = pack.to_hex();
        let indexed = store.head(&layout::pack_index(&hex)).await.is_ok();
        if !indexed {
            match store.delete(&layout::pack(&hex)).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => report.packs_deleted += 1,
                Err(_) => report.delete_failures += 1,
            }
        }
    }

    // 6. Nothing condemned remains to protect.
    constellation_store_s3::publish_condemned_packs(&store, &HashSet::new(), now as u64, now)
        .await?;
    tracing::info!(
        commits_deleted = report.commits_deleted.len(),
        packs_deleted = report.packs_deleted,
        packs_rewritten = report.packs_rewritten,
        bytes_written = report.bytes_written,
        "metadata tree GC round complete"
    );
    Ok(report)
}

// The helpers below take owned arguments on purpose: the daemon spawns
// the GC tick, and futures holding borrowed iterators or references
// across awaits trip rustc's higher-ranked `Send` inference there.

/// Commits outside the newest `retention` that are also older than
/// `retention_ms`. Only those candidates are fetched (for their age).
async fn expired_commits(
    store: Arc<dyn ObjectStore>,
    sealing: Option<constellation_store_s3::TreeSealing>,
    seqs: Vec<u64>,
    retention: usize,
    retention_ms: i64,
    now: i64,
) -> Result<Vec<u64>> {
    let chain = CommitChain::new(store).with_sealing(sealing);
    let keep_from = seqs.len().saturating_sub(retention);
    let mut expired = Vec::new();
    for seq in seqs.into_iter().take(keep_from) {
        if let Some(commit) = chain.get(seq).await? {
            if now - commit.unix_ms > retention_ms {
                expired.push(seq);
            }
        }
    }
    Ok(expired)
}

/// Every commit on the bucket except `expired`.
async fn retained_commits(store: Arc<dyn ObjectStore>, expired: Vec<u64>) -> Result<Vec<u64>> {
    let expired: HashSet<u64> = expired.into_iter().collect();
    let mut retained = Vec::new();
    for seq in CommitChain::new(store).list_from(0).await? {
        if !expired.contains(&seq) {
            retained.push(seq);
        }
    }
    Ok(retained)
}

/// Every node reachable from the retained commits, the snapshot roots,
/// and any known node a live hold names.
async fn mark_roots(
    store: Arc<dyn ObjectStore>,
    sealing: Option<constellation_store_s3::TreeSealing>,
    cache: Arc<constellation_store_s3::NodeCache>,
    commits: Vec<u64>,
    threads: usize,
    now: i64,
) -> Result<HashSet<NodeHash>> {
    let chain = CommitChain::new(store.clone()).with_sealing(sealing);
    let mut roots: Vec<NodeHash> = Vec::new();
    for seq in commits {
        if let Some(commit) = chain.get(seq).await? {
            for shard in commit.roots.keys() {
                roots.push(
                    commit
                        .root(shard)
                        .with_context(|| format!("commit {seq} shard {shard}: bad root"))?,
                );
            }
        }
    }
    for record in constellation_store_s3::SnapshotStore::new(store.clone())
        .list()
        .await?
    {
        roots.push(crate::snapshot::SnapshotRoot::of_record(&record)?.root);
    }
    // A hold names objects to keep; a metadata node among them is a root.
    // Only nodes some pack holds can be walked, and a hold naming chunks
    // (the common case) names nothing here.
    for hash in hold_hashes(store, now).await? {
        let node = NodeHash(hash);
        if cache.location_of(&node).is_some() {
            roots.push(node);
        }
    }
    let marked =
        tokio::task::spawn_blocking(move || constellation_store_s3::mark(&cache, &roots, threads))
            .await
            .context("metadata mark task")??;
    Ok(marked.nodes)
}

/// Every 32-byte hex hash in a live `holds/*` object.
async fn hold_hashes(store: Arc<dyn ObjectStore>, now: i64) -> Result<Vec<[u8; 32]>> {
    fn collect(value: &serde_json::Value, out: &mut Vec<[u8; 32]>) {
        match value {
            serde_json::Value::String(text) => {
                if let Some(hash) = constellation_fs_core::ChunkHash::from_hex(text) {
                    out.push(hash.0);
                }
            }
            serde_json::Value::Array(values) => values.iter().for_each(|v| collect(v, out)),
            serde_json::Value::Object(values) => values.values().for_each(|v| collect(v, out)),
            _ => {}
        }
    }
    let mut out = Vec::new();
    for object in store
        .list(Some(&object_store::path::Path::from("holds")))
        .try_collect::<Vec<_>>()
        .await?
    {
        let value: serde_json::Value =
            serde_json::from_slice(&store.get(&object.location).await?.bytes().await?)?;
        let expired = value
            .get("expires_unix_ms")
            .and_then(serde_json::Value::as_i64)
            .is_some_and(|expires| expires <= now);
        if !expired {
            collect(&value, &mut out);
        }
    }
    Ok(out)
}

/// Pack bodies with no `.idx` sibling, last modified before `cutoff`.
async fn old_incomplete(
    store: &Arc<dyn ObjectStore>,
    catalog: &PackCatalog,
    cutoff_ms: i64,
) -> Result<Vec<PackHash>> {
    let incomplete: HashSet<PackHash> = catalog.incomplete().iter().copied().collect();
    if incomplete.is_empty() {
        return Ok(Vec::new());
    }
    let mut old = Vec::new();
    for object in store
        .list(Some(&layout::packs_prefix()))
        .try_collect::<Vec<_>>()
        .await?
    {
        let Some(hash) = object.location.filename().and_then(PackHash::from_hex) else {
            continue;
        };
        if incomplete.contains(&hash) && object.last_modified.timestamp_millis() <= cutoff_ms {
            old.push(hash);
        }
    }
    Ok(old)
}

fn load_cursor(meta: &Meta, key: &str) -> Result<Option<PackHash>> {
    Ok(meta
        .kv_get(key)?
        .filter(|hex| !hex.is_empty())
        .and_then(|hex| PackHash::from_hex(&hex)))
}

fn save_cursor(meta: &Meta, key: &str, cursor: Option<PackHash>) -> Result<()> {
    meta.kv_set(key, &cursor.map(|hash| hash.to_hex()).unwrap_or_default())?;
    Ok(())
}

/// A process-private scratch directory for the round's node cache.
struct ScratchDir(std::path::PathBuf);

impl ScratchDir {
    fn new() -> Result<ScratchDir> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "constellation-mtree-gc-{}-{}",
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mtree_publish::TreePublisher;
    use crate::mtree_read::TreeReader;
    use constellation_fs_core::cache::DiskCache;
    use constellation_fs_core::types::ROOT_INO;
    use constellation_meta::MetaStore;
    use constellation_mtree::{record, Hasher, Tree};
    use constellation_store_s3::{BlobStore, NodeCache, PackStore, SHARD0};
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    fn config(retention: usize) -> MtreeGcConfig {
        MtreeGcConfig {
            retention,
            retention_ms: 0,
            compact_bytes_per_s: 0,
            threads: 2,
            horizon_ms: 0,
            grace: Duration::ZERO,
        }
    }

    fn cache(store: &Arc<dyn ObjectStore>, dir: &TempDir) -> Arc<NodeCache> {
        Arc::new(NodeCache::new(
            PackStore::new(store.clone()).with_target_bytes(1 << 20),
            Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap()),
            Hasher::Plain,
            tokio::runtime::Handle::current(),
        ))
    }

    fn publisher(meta: &Arc<Meta>, store: &Arc<dyn ObjectStore>, dir: &TempDir) -> TreePublisher {
        TreePublisher::new(
            meta.clone(),
            cache(store, dir),
            BlobStore::new(store.clone(), Hasher::Plain),
            CommitChain::new(store.clone()),
            record::config(),
            1,
            tokio::runtime::Handle::current(),
        )
    }

    fn packs(store: &Arc<dyn ObjectStore>) -> Vec<object_store::path::Path> {
        futures::executor::block_on(
            store
                .list(Some(&layout::packs_prefix()))
                .map_ok(|meta| meta.location)
                .try_collect::<Vec<_>>(),
        )
        .unwrap()
    }

    /// Every node under `root`, read and verified from a cold cache.
    async fn readable(store: &Arc<dyn ObjectStore>, root: constellation_mtree::NodeHash) -> u64 {
        let dir = TempDir::new().unwrap();
        let cold = cache(store, &dir);
        cold.refresh_catalog().await.unwrap();
        let reader = TreeReader::new(Tree::with_config(cold, record::config()).unwrap(), root);
        tokio::task::spawn_blocking(move || {
            let verified = reader.verify().unwrap();
            assert!(verified.bad_aggregates.is_empty());
            verified.nodes
        })
        .await
        .unwrap()
    }

    /// A round retires old commits, reclaims the packs only they kept
    /// alive — deleting whole dead packs and rewriting partially dead
    /// ones — and leaves every retained commit readable, node by node,
    /// from a cold cache. The saved cursors end cleared.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_round_reclaims_what_retired_commits_kept_and_nothing_else() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let dir = TempDir::new().unwrap();
        let mut writer = publisher(&meta, &store, &dir);
        let d = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap().ino;
        for i in 0..400 {
            meta.create(d, &format!("file-{i:04}"), 0o644, 0, 0)
                .unwrap();
        }
        let first = writer.publish(1).await.unwrap().unwrap();
        // A snapshot of the first commit: retention will retire the
        // commit object, and the snapshot alone must keep its tree.
        let snapped = first.root(SHARD0).unwrap();
        constellation_store_s3::SnapshotStore::new(store.clone())
            .create(&constellation_store_s3::SnapshotRecord::new(
                "/d",
                "first",
                1,
                constellation_store_s3::SnapshotTreeRoot {
                    seq: first.seq,
                    root: snapped.to_hex(),
                    ino: d,
                },
            ))
            .await
            .unwrap();
        // Rewrites that supersede most of the first commit's leaves.
        for round in 0..6 {
            for i in (0..400).step_by(3) {
                let ino = meta.child_ino(d, &format!("file-{i:04}")).unwrap().unwrap();
                meta.setattr(ino, Some(0o600 + round), None, None, None, None, None)
                    .unwrap();
            }
            writer.publish(1).await.unwrap().unwrap();
        }
        let before = packs(&store).len();
        let chain = CommitChain::new(store.clone());
        let seqs = chain.list_from(0).await.unwrap();
        assert_eq!(seqs.len(), 7);

        let report = run(store.clone(), None, &meta, &config(2), false)
            .await
            .unwrap();
        assert_eq!(report.commits_deleted, vec![1, 2, 3, 4, 5]);
        assert!(
            report.packs_dead + report.packs_partially_dead > 0,
            "{report:?}"
        );
        assert!(
            report.packs_deleted + report.packs_rewritten > 0,
            "{report:?}"
        );
        assert!(packs(&store).len() < before, "{report:?}");
        assert_eq!(chain.list_from(0).await.unwrap(), vec![6, 7]);
        for seq in [6, 7] {
            let root = chain.get(seq).await.unwrap().unwrap().root(SHARD0).unwrap();
            assert!(readable(&store, root).await > 0);
        }
        assert!(
            readable(&store, snapped).await > 0,
            "the snapshot's tree was swept"
        );
        assert_eq!(meta.kv_get(KV_CURSOR_DEAD).unwrap().as_deref(), Some(""));
        assert_eq!(meta.kv_get(KV_CURSOR_COMPACT).unwrap().as_deref(), Some(""));
        assert!(constellation_store_s3::read_condemned_packs(&store)
            .await
            .unwrap()
            .is_empty());

        // The publisher keeps working across the compaction: its cached
        // locations for moved nodes are stale, and the miss path finds
        // the replacement packs.
        meta.create(d, "after-gc", 0o644, 0, 0).unwrap();
        let next = writer.publish(1).await.unwrap().unwrap();
        assert!(readable(&store, next.root(SHARD0).unwrap()).await > 0);

        // A second round finds nothing more to do for the retained set.
        let again = run(store.clone(), None, &meta, &config(8), true)
            .await
            .unwrap();
        assert!(again.commits_deleted.is_empty());
    }

    /// The delete-vs-dedup handshake on the publisher side: a pack a GC
    /// round condemns after the publisher planned against it makes the
    /// publish defer and forget the pack, and the retry uploads the
    /// nodes afresh instead of naming a pack about to be deleted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_publisher_never_names_a_condemned_pack() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let dir = TempDir::new().unwrap();
        let d = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap().ino;
        for i in 0..50 {
            meta.create(d, &format!("f{i}"), 0o644, 0, 0).unwrap();
        }
        let mut first = publisher(&meta, &store, &dir);
        let base = first.publish(1).await.unwrap().unwrap();

        // A second publisher over the same bucket rebuilds (a restart
        // that could not prove its tree level), deduplicating every
        // unchanged node against the first commit's packs.
        let dir2 = TempDir::new().unwrap();
        let cache2 = cache(&store, &dir2);
        let condemned: HashSet<PackHash> = base.pack_hashes().unwrap().into_iter().collect();
        cache2.refresh_catalog().await.unwrap();
        cache2.start_dedup_log();
        let tree = Tree::with_config(cache2.clone(), record::config()).unwrap();
        let blobs = BlobStore::new(store.clone(), Hasher::Plain);
        let root = {
            let meta = meta.clone();
            let tree_blobs = blobs.clone();
            tokio::task::spawn_blocking(move || {
                crate::mtree_publish::rebuild_root(&meta, &tree, &tree_blobs).unwrap()
            })
            .await
            .unwrap()
        };
        assert_eq!(Some(root), base.root(SHARD0), "same replica, same tree");
        assert!(
            !cache2.deduped_packs().is_empty(),
            "the rebuild must dedupe"
        );
        // GC condemns those packs after the plan: not sound any more.
        assert!(!cache2.dedup_is_sound(&condemned).await.unwrap());
        // The packs are forgotten, so a replan uploads the nodes itself.
        cache2.set_condemned(condemned.clone());
        cache2.start_dedup_log();
        let tree = Tree::with_config(cache2.clone(), record::config()).unwrap();
        let meta2 = meta.clone();
        tokio::task::spawn_blocking(move || {
            crate::mtree_publish::rebuild_root(&meta2, &tree, &blobs).unwrap()
        })
        .await
        .unwrap();
        assert!(
            cache2.pending_nodes() > 0,
            "condemned nodes must be re-uploaded"
        );
        assert!(cache2
            .deduped_packs()
            .iter()
            .all(|pack| !condemned.contains(pack)));
    }
}
