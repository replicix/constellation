//! Log bootstrap and the segment codec's daemon-side helpers (what is
//! left of the shipper after plan 30 M5 phase 2).
//!
//! The shipping, tailing, publishing and lease sequencing that lived here
//! — one PUT per round, the GET-next probe, own-segment recovery, epoch
//! fencing, the ride-along atime drain, the takeover's epoch marker, the
//! publish cadence — is the authority core's (`constellation_authority::
//! core::jobs`), driven by `crate::authority_driver` in the daemon and by
//! the simulation in tests. The segment envelope itself is
//! `constellation_authority::segment` (postcard, version 2).
//!
//! What stays: building a fresh replica from S3 (`bootstrap`: the commit
//! chain's head plus the log after it, or a genesis replay), and the
//! knobs the driver reads.

use anyhow::{bail, Context, Result};
use constellation_authority::segment::decode;
use constellation_meta::{Meta, MetaStore};
use constellation_store_s3::log::PARTITION;
use constellation_store_s3::LogStore;
use std::collections::HashSet;
use std::sync::Arc;

/// Default for `CONSTELLATION_PUBLISH_IDLE_S`: publish whenever `ns` has
/// dirty keys and this many seconds have passed since the last publish
/// attempt (the every-`publish_every`-segments cadence only fires for a
/// node actively shipping its own writes).
const PUBLISH_IDLE_S_DEFAULT: u64 = 30;

pub fn publish_idle_interval() -> std::time::Duration {
    let secs: u64 = std::env::var("CONSTELLATION_PUBLISH_IDLE_S")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(PUBLISH_IDLE_S_DEFAULT);
    std::time::Duration::from_secs(secs)
}

/// Plan 28 S6: rebuild the replica from the commit chain's head, then
/// tail the log from the commit's `applied` position. `false` when the
/// chain is empty (a fresh filesystem with no commit yet), so the caller
/// falls back to a genesis replay of the whole log.
///
/// A half-written replica is removed on failure, so the fallback (or a
/// retry) starts from nothing rather than from a partial load.
async fn bootstrap_from_tree(db_path: &std::path::Path, log: &LogStore) -> Result<bool> {
    let scratch = db_path.with_extension("bootstrap-nodes");
    let _ = std::fs::remove_dir_all(&scratch);
    let result = async {
        let reader = crate::mtree_read::ChainReader::for_log(log, &scratch)?;
        if reader.head().await?.is_none() {
            return Ok(None);
        }
        let meta = Arc::new(Meta::open(db_path)?);
        let Some(loaded) =
            crate::mtree_read::bootstrap_from_commit(&reader, Arc::clone(&meta)).await?
        else {
            return Ok(None);
        };
        let replayed = replay_from(&meta, log, PARTITION, loaded.commit.applied).await?;
        crate::mtree_publish::remember_loaded_commit(&meta, &loaded.commit)?;
        for ino in meta.orphans()? {
            meta.reap_orphan(ino)?;
        }
        tracing::info!(
            seq = loaded.commit.seq,
            inodes = loaded.inodes,
            dentries = loaded.dentries,
            replayed,
            "bootstrapped metadata replica from commit"
        );
        anyhow::Ok(Some(()))
    }
    .await;
    let _ = std::fs::remove_dir_all(&scratch);
    match result {
        Ok(Some(())) => Ok(true),
        Ok(None) => {
            remove_db(db_path);
            Ok(false)
        }
        Err(e) => {
            remove_db(db_path);
            Err(e.context("bootstrapping the metadata replica from the commit chain"))
        }
    }
}

/// `db_path` is an fjall database directory (plan 29 M1: no more
/// `.db`/`-wal`/`-shm` sibling files to clean up).
fn remove_db(db_path: &std::path::Path) {
    let _ = std::fs::remove_dir_all(db_path);
}

/// Apply `part`'s contiguous log run after `start` and record where it
/// stopped. Shared by both bootstrap paths (from a commit, and genesis).
async fn replay_from(meta: &Meta, log: &LogStore, part: &str, start: u64) -> Result<usize> {
    let part_log = log.with_partition(part);
    let mut applied = start;
    let mut replayed = 0usize;
    let seqs = part_log.list_segments_from(start + 1).await?;
    // A first segment past `start + 1` means retention already pruned the
    // records this base needs. Stopping at the gap would hand back a
    // replica silently missing that history, so this must fail loudly.
    if let Some(&first) = seqs.first() {
        if first > start + 1 {
            bail!(
                "{part}: the log starts at segment {first}, but this bootstrap base \
                 covers only up to {start}; segments in between were pruned"
            );
        }
    }
    for seq in seqs {
        if seq != applied + 1 {
            break;
        }
        let seg = decode(&part_log.get_segment(seq).await?)?;
        replayed += seg.records.len();
        meta.apply_records(&seg.records)
            .with_context(|| format!("replaying {part} log segment {seq}"))?;
        applied = seq;
    }
    meta.set_applied_seq(applied)?;
    Ok(replayed)
}

/// A mount of an existing replica: if the log was pruned past its
/// applied position while it was offline (DESIGN.md §14 "Falling behind
/// segment GC"), rebuild the namespace from the head commit before the
/// sync task tails — the running core would find the gap on its first
/// probe (`Core::gap_check_due`), but a mount should not serve a stale
/// replica even briefly, and a rebuild before any view opens costs no
/// open handle. Returns whether a rebuild happened. The check is one LIST
/// with offset (`LogStore::first_segment_from`), the same question the
/// core asks: nothing at or after `applied + 1` is "at head", `applied +
/// 1` itself is "there is a tail to apply", anything later is a gap.
pub async fn rebuild_if_pruned(
    meta: &Meta,
    log: &LogStore,
    state_dir: &std::path::Path,
) -> Result<bool> {
    let applied = meta.applied_seq()?;
    let next = applied + 1;
    match log
        .with_partition(PARTITION)
        .first_segment_from(next)
        .await
        .context("checking the log for a retention gap")?
    {
        Some(first) if first > next => {
            tracing::warn!(
                applied,
                first_retained = first,
                "the log was pruned past this replica's position while it was \
                 offline; rebuilding the namespace from the head commit"
            );
            // No view is mounted yet: no orphan can be open.
            crate::authority_driver::rebuild_replica(meta, log, state_dir, &HashSet::new)
                .await
                .context("rebuilding the replica after a log retention gap")?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// Build a fresh local replica from S3. Used when the state dir has no
/// metadata DB: a fresh mount, or a read-only member, which bootstraps
/// from commits published by writers exactly like everyone else.
///
/// Plan 29 M0b retired the whole-DB `VACUUM INTO` checkpoint: the source
/// is the commit chain's head when one exists (restore the tree, then
/// replay the log from its `applied` position), or — a genuinely fresh
/// filesystem with no commit yet — a replay of the whole log from seq 1.
pub async fn bootstrap(db_path: &std::path::Path, log: &LogStore) -> Result<()> {
    if bootstrap_from_tree(db_path, log).await? {
        return Ok(());
    }
    let meta = Meta::open(db_path)?;
    let replayed = replay_from(&meta, log, PARTITION, 0).await?;
    for ino in meta.orphans()? {
        meta.reap_orphan(ino)?;
    }
    // Plan 25: a brand-new replica has never written local content, so any
    // `pending_upload` row would be another node's obligation — foreign
    // replay never inserts into that table, so this is normally a no-op;
    // it stays as a defensive clear after bootstrap.
    meta.clear_pending_uploads()
        .context("clearing inherited pending_upload rows after bootstrap")?;
    tracing::info!(replayed, "bootstrapped metadata replica from genesis");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority_driver::Standalone;
    use constellation_meta::MetaStore;
    use constellation_store_s3::LeaseMode;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use std::sync::Arc as StdArc;

    /// A writing node for these tests: the real core over a real `Meta`,
    /// with IO run inline (`Standalone`), and a tree publisher.
    struct Node {
        meta: Arc<Meta>,
        driver: Standalone,
        _nodes: tempfile::TempDir,
    }

    fn node(store: &StdArc<InMemory>, id: u64) -> Node {
        use constellation_fs_core::cache::DiskCache;
        use constellation_mtree::{record, Hasher};
        use constellation_store_s3::{BlobStore, CommitChain, NodeCache, PackStore};
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        meta.set_node_prefix(id).unwrap();
        let backend = store.clone() as StdArc<dyn ObjectStore>;
        let dir = tempfile::TempDir::new().unwrap();
        let cache = Arc::new(NodeCache::new(
            PackStore::new(backend.clone()),
            Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap()),
            Hasher::Plain,
            tokio::runtime::Handle::current(),
        ));
        let mut publisher = crate::mtree_publish::TreePublisher::new(
            meta.clone(),
            cache,
            BlobStore::new(backend.clone(), Hasher::Plain),
            CommitChain::new(backend.clone()),
            record::config(),
            id,
        );
        publisher.restore().unwrap();
        let driver =
            Standalone::with_publisher(meta.clone(), backend, id, LeaseMode::Cas, Some(publisher));
        Node {
            meta,
            driver,
            _nodes: dir,
        }
    }

    impl Node {
        /// What the daemon's sync task does per round: take authority if
        /// it is available, then tail + ship.
        async fn sync(&mut self) {
            assert!(self.driver.acquire().await.unwrap());
            self.driver.sync().await.unwrap();
        }
    }

    #[tokio::test]
    async fn bootstrap_from_genesis_has_no_pending_upload() {
        let dir = tempfile::tempdir().unwrap();
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let log = LogStore::new(store);

        let boot = dir.path().join("boot.db");
        bootstrap(&boot, &log).await.unwrap();
        let restored = Meta::open(&boot).unwrap();
        assert_eq!(restored.pending_upload_count().unwrap(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_publish_commits_the_metadata_tree() {
        use constellation_store_s3::{CommitChain, SHARD0};

        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let backend = store.clone() as StdArc<dyn ObjectStore>;

        let d = a.meta.mkdir(1, "d", 0o755, 0, 0).unwrap();
        a.meta.create(d.ino, "f", 0o644, 0, 0).unwrap();
        a.sync().await;
        a.driver.publish().await.unwrap();

        let chain = CommitChain::new(backend);
        let head = chain.discover_head(0).await.unwrap().expect("a commit");
        let commit = chain.get(head).await.unwrap().unwrap();
        assert_eq!((commit.seq, commit.author), (1, 1));
        assert!(commit.root(SHARD0).is_some());
        assert!(commit.agg.keys >= 6, "{:?}", commit.agg);
        assert_eq!(commit.agg.files, 1, "only regular files count (§P7)");

        // A second publish with nothing new publishes nothing: an empty
        // changed set is not a commit.
        a.driver.publish().await.unwrap();
        assert_eq!(chain.discover_head(0).await.unwrap(), Some(1));
    }

    /// Plan 28 S6: a fresh replica restores the chain head and tails the
    /// log from the commit's vector, and ends up equal — table by table
    /// — to the replica that published it. Every segment the commit
    /// covers is deleted first, so nothing but the commit chain can have
    /// produced the result.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_fresh_replica_bootstraps_from_the_commit_chain() {
        use constellation_meta::{SetXattrMode, SnapshotRow};
        use object_store::ObjectStoreExt;

        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);

        let d = a.meta.mkdir(1, "d", 0o755, 0, 0).unwrap();
        let f = a.meta.create(d.ino, "f", 0o644, 7, 8).unwrap();
        a.meta
            .set_manifest(f.ino, &vec![0xab; 4096], 1 << 20)
            .unwrap();
        a.meta
            .set_xattr(f.ino, "user.small", b"v", SetXattrMode::Set)
            .unwrap();
        let big = a.meta.create(d.ino, "big", 0o600, 0, 0).unwrap();
        for i in 0..40 {
            a.meta
                .set_xattr(big.ino, &format!("user.k{i}"), b"vvvv", SetXattrMode::Set)
                .unwrap();
        }
        a.meta
            .set_xattr(big.ino, "user.huge", &vec![7u8; 40_000], SetXattrMode::Set)
            .unwrap();
        a.meta.symlink(d.ino, "s", &"t".repeat(3000), 0, 0).unwrap();
        a.meta.link(f.ino, 1, "hard").unwrap();
        a.meta.write_quota(Some(1 << 40)).unwrap();
        a.meta
            .record_snapshot(&SnapshotRow::new(
                "snap-1",
                "/d",
                "one",
                "ab".repeat(32),
                42,
            ))
            .unwrap();
        a.sync().await;
        a.driver.publish().await.unwrap();

        // Shipped after the commit: only the log tail carries these.
        a.meta.create(d.ino, "late", 0o644, 0, 0).unwrap();
        a.meta.unlink(d.ino, "s").unwrap();
        a.sync().await;

        let chain = constellation_store_s3::CommitChain::new(store.clone());
        let head = chain
            .get(chain.discover_head(0).await.unwrap().unwrap())
            .await
            .unwrap()
            .unwrap();
        let covered = head.applied;
        assert!(covered >= 1);
        for seq in 1..=covered {
            store
                .delete(&constellation_store_s3::layout::log_segment(PARTITION, seq))
                .await
                .unwrap();
        }

        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path().join("fresh.db");
        bootstrap(&db, &LogStore::new(store.clone())).await.unwrap();
        let fresh = Meta::open(&db).unwrap();
        assert_replicas_equal(&fresh, &a.meta);
        assert_eq!(fresh.applied_seq().unwrap(), a.meta.applied_seq().unwrap());
        assert!(fresh.child_ino(d.ino, "late").unwrap().is_some());
        assert!(fresh
            .chunk_ref_exists(&constellation_fs_core::ChunkHash([0; 32]))
            .is_ok());
    }

    /// DESIGN.md §14 "Falling behind segment GC": a replica the log was
    /// pruned past (offline longer than retention; here `retention_
    /// segments = 2`) rebuilds itself from the head commit instead of
    /// stalling on a deleted slot — at mount (`rebuild_if_pruned`), on a
    /// running tail (the core's gap check), and, the safety half, when it
    /// tries to take the lease: the takeover never CASes across the gap,
    /// so no epoch marker is ever created in a pruned slot (before: the
    /// GET-next `404` read as "at head", the CAS won, and the marker
    /// forked the log at the old position).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_replica_the_log_was_pruned_past_rebuilds_and_never_appends_below_the_head() {
        use object_store::ObjectStoreExt;
        let store = StdArc::new(InMemory::new());
        let backend = store.clone() as StdArc<dyn ObjectStore>;
        let log = LogStore::new(backend.clone());
        let mut a = node(&store, 1);
        // Two followers stop tailing after segment 1: one will tail again
        // (the follower path), one will try to take the lease (the taker
        // path). A third never tails before its mount-time check.
        let mut b = node(&store, 2);
        let mut c = node(&store, 3);
        let dir_d = tempfile::TempDir::new().unwrap();
        let db_d = dir_d.path().join("d.db");
        a.meta.mkdir(1, "d0", 0o755, 0, 0).unwrap();
        a.sync().await;
        b.driver.tail_to_head().await.unwrap();
        c.driver.tail_to_head().await.unwrap();
        bootstrap(&db_d, &log).await.unwrap();
        assert_eq!(b.meta.applied_seq().unwrap(), 1);
        assert_eq!(Meta::open(&db_d).unwrap().applied_seq().unwrap(), 1);

        // A ships a segment per directory and publishes as it goes; the
        // head commit's applied position ends far past 1.
        for i in 1..8 {
            a.meta.mkdir(1, &format!("d{i}"), 0o755, 0, 0).unwrap();
            a.sync().await;
            a.driver.publish().await.unwrap();
        }
        let head = a.meta.applied_seq().unwrap();
        assert!(head >= 8, "{head}");
        // Retention (the real rule, with a two-segment window and no
        // completion floor) prunes everything below `applied - 2`.
        let config = crate::gc::GcConfig {
            horizon_ms: 0,
            retention_segments: 2,
            lease_ttl_ms: 1,
            completion_retention_ms: 0,
            snap_walk: crate::gc::SnapWalkMode::Diff,
        };
        let marks = crate::gc::metadata_candidates(
            &backend,
            None,
            &config,
            constellation_store_s3::lease::now_unix_ms(),
        )
        .await
        .unwrap();
        assert!(!marks.is_empty());
        for mark in &marks {
            store
                .delete(&object_store::path::Path::from(mark.key.as_str()))
                .await
                .unwrap();
        }
        let retained = log.list_segments().await.unwrap();
        let floor = *retained.first().unwrap();
        assert!(
            floor > 2,
            "segment 2 (B's next slot) was pruned: {retained:?}"
        );
        a.driver.shutdown().await.unwrap();

        // The follower path: B's tail-to-head finds the gap and rebuilds.
        b.driver.tail_to_head().await.unwrap();
        assert_replicas_equal(&b.meta, &a.meta);
        assert_eq!(b.meta.applied_seq().unwrap(), a.meta.applied_seq().unwrap());
        assert_eq!(
            log.list_segments().await.unwrap(),
            retained,
            "a follower's rebuild writes nothing to the log"
        );

        // The taker path: C tries to take the released lease from its
        // stale position. The acquisition ends unacquired, the replica is
        // rebuilt, and the log is untouched — no marker in the pruned
        // slot 2.
        assert!(
            !c.driver.acquire().await.unwrap(),
            "an acquisition across a pruned gap must not succeed"
        );
        assert_eq!(
            log.list_segments().await.unwrap(),
            retained,
            "no segment was created below the head"
        );
        assert_replicas_equal(&c.meta, &a.meta);
        // From the rebuilt position the next acquisition succeeds (A
        // released cleanly: no epoch marker is needed) and C's first ship
        // lands at head + 1.
        assert!(c.driver.acquire().await.unwrap());
        assert_eq!(log.list_segments().await.unwrap(), retained);
        c.meta.mkdir(1, "from-c", 0o755, 0, 0).unwrap();
        c.driver.sync().await.unwrap();
        let after = log.list_segments().await.unwrap();
        assert_eq!(after.first(), Some(&floor));
        assert_eq!(*after.last().unwrap(), head + 1, "{after:?}");

        // The mount path: a replica opened at its old position is
        // rebuilt before anything tails.
        let meta_d = Meta::open(&db_d).unwrap();
        assert!(rebuild_if_pruned(&meta_d, &log, dir_d.path())
            .await
            .unwrap());
        assert!(meta_d.applied_seq().unwrap() >= head);
        assert!(meta_d.child_ino(1, "d7").unwrap().is_some());
        assert!(
            !rebuild_if_pruned(&meta_d, &log, dir_d.path())
                .await
                .unwrap(),
            "a replica inside the retained log is left alone"
        );
    }

    /// A retention-gap rebuild on a node with an unlinked file open: the
    /// orphan record (and its manifest) survive the namespace swap, so
    /// the handle keeps reading, and the node's hold still claims the
    /// chunk afterwards; an orphan nobody has open is dropped by the
    /// rebuild like any other.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_rebuild_keeps_the_orphans_a_view_has_open() {
        use constellation_fs_core::manifest::{ChunkInfo, Manifest};
        use constellation_fs_core::{ChunkHash, ChunkLayout};
        use constellation_store_s3::{ChunkStore, CompressionSetting};
        use object_store::ObjectStoreExt;
        use std::sync::Mutex;

        struct Opens(Mutex<Vec<constellation_fs_core::Ino>>);
        impl crate::holds::OpenHandles for Opens {
            fn open_inos(&self) -> Vec<constellation_fs_core::Ino> {
                self.0.lock().unwrap().clone()
            }
        }

        let store = StdArc::new(InMemory::new());
        let backend = store.clone() as StdArc<dyn ObjectStore>;
        let chunks = Arc::new(ChunkStore::new(backend.clone()));
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);
        let content = b"open on b across a rebuild";
        let hash = ChunkHash::of(content);
        chunks
            .put_chunk(&hash, content, CompressionSetting::RAW)
            .await
            .unwrap();
        let manifest = Manifest {
            layout: ChunkLayout::new(4096),
            file_len: content.len() as u64,
            chunks: ChunkInfo::Inline([(0u64, hash)].into_iter().collect()),
        }
        .encode();
        let open = a.meta.create(1, "open", 0o644, 0, 0).unwrap();
        a.meta
            .set_manifest(open.ino, &manifest, content.len() as u64)
            .unwrap();
        let closed = a.meta.create(1, "closed", 0o644, 0, 0).unwrap();
        a.sync().await;
        b.driver.tail_to_head().await.unwrap();
        // A unlinks both (nothing open on A) and ships; B applies with
        // `open` held open by a view.
        a.meta.unlink(1, "open").unwrap();
        a.meta.unlink(1, "closed").unwrap();
        a.meta.reap_orphan(open.ino).unwrap();
        a.meta.reap_orphan(closed.ino).unwrap();
        a.sync().await;
        b.driver.tail_to_head().await.unwrap();
        assert_eq!(b.meta.orphans().unwrap().len(), 2);
        let view = StdArc::new(Opens(Mutex::new(vec![open.ino])));
        let sources = Arc::new(crate::holds::HoldSources::default());
        sources.register(
            1,
            StdArc::downgrade(&view) as std::sync::Weak<dyn crate::holds::OpenHandles>,
        );
        let holds = crate::holds::Holds::new(
            backend.clone(),
            chunks.clone(),
            b.meta.clone(),
            2,
            sources,
            crate::holds::HoldConfig {
                refresh: std::time::Duration::from_millis(10),
                ttl: std::time::Duration::from_secs(60),
            },
        );
        holds.refresh_once().await.unwrap();
        b.driver.set_holds(holds.clone());
        assert_eq!(
            b.meta.orphans().unwrap(),
            vec![open.ino],
            "the closed one reaped"
        );

        // The log moves on and is pruned past B.
        for i in 0..8 {
            a.meta.mkdir(1, &format!("d{i}"), 0o755, 0, 0).unwrap();
            a.sync().await;
            a.driver.publish().await.unwrap();
        }
        let config = crate::gc::GcConfig {
            horizon_ms: 0,
            retention_segments: 2,
            lease_ttl_ms: 1,
            completion_retention_ms: 0,
            snap_walk: crate::gc::SnapWalkMode::Diff,
        };
        for mark in crate::gc::metadata_candidates(
            &backend,
            None,
            &config,
            constellation_store_s3::lease::now_unix_ms(),
        )
        .await
        .unwrap()
        {
            store
                .delete(&object_store::path::Path::from(mark.key.as_str()))
                .await
                .unwrap();
        }
        b.driver.tail_to_head().await.unwrap();
        assert_replicas_equal(&b.meta, &a.meta);

        // The open orphan came through the rebuild; the hold still names
        // its chunk; the last close reaps it and withdraws the hold.
        assert_eq!(b.meta.orphans().unwrap(), vec![open.ino]);
        assert_eq!(b.meta.manifest(open.ino).unwrap(), Some(manifest));
        assert_eq!(b.meta.getattr(open.ino).unwrap().unwrap().nlink, 0);
        holds.refresh_once().await.unwrap();
        let roots = crate::gc::hold_roots(
            backend.clone(),
            constellation_store_s3::lease::now_unix_ms(),
        )
        .await
        .unwrap();
        assert!(roots.contains(&hash));
        view.0.lock().unwrap().clear();
        holds.refresh_once().await.unwrap();
        assert!(b.meta.orphans().unwrap().is_empty());
        assert!(backend
            .head(&constellation_store_s3::layout::hold(2))
            .await
            .is_err());
    }

    /// A bootstrap base older than the log's retention floor must fail
    /// loudly: replaying from the first segment present would hand back
    /// a replica silently missing everything that was pruned.
    #[tokio::test]
    async fn replay_refuses_a_base_the_log_was_pruned_past() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        for i in 0..4 {
            a.meta.mkdir(1, &format!("d{i}"), 0o755, 0, 0).unwrap();
            a.sync().await;
        }
        let log = LogStore::new(store.clone());
        use object_store::ObjectStoreExt;
        store
            .delete(&constellation_store_s3::layout::log_segment(PARTITION, 1))
            .await
            .unwrap();
        let fresh = Meta::open_in_memory().unwrap();
        let err = replay_from(&fresh, &log, PARTITION, 0).await.unwrap_err();
        assert!(err.to_string().contains("pruned"), "{err:#}");
        // From a base the log still covers, the same replay succeeds.
        let covered = Meta::open_in_memory().unwrap();
        assert!(replay_from(&covered, &log, PARTITION, 1).await.unwrap() > 0);
    }

    /// Table-by-table equality of two replicas' shared state, reporting
    /// only the rows that differ (a full dump buries them in manifests).
    fn assert_replicas_equal(got: &Meta, want: &Meta) {
        let got = got.dump_replicated().unwrap();
        let want = want.dump_replicated().unwrap();
        let short = |row: &String| row.chars().take(240).collect::<String>();
        let missing: Vec<String> = want
            .iter()
            .filter(|r| !got.contains(r))
            .map(short)
            .collect();
        let extra: Vec<String> = got
            .iter()
            .filter(|r| !want.contains(r))
            .map(short)
            .collect();
        assert!(
            missing.is_empty() && extra.is_empty(),
            "replicas differ\nmissing: {missing:#?}\nextra: {extra:#?}"
        );
    }
}

#[cfg(test)]
mod root_owner_tests {
    use super::*;
    use crate::authority_driver::Standalone;
    use constellation_meta::MetaStore;
    use constellation_store_s3::LeaseMode;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use std::sync::Arc as StdArc;

    fn node(store: &StdArc<InMemory>, id: u64) -> (Arc<Meta>, Standalone, tempfile::TempDir) {
        use constellation_fs_core::cache::DiskCache;
        use constellation_mtree::{record, Hasher};
        use constellation_store_s3::{BlobStore, CommitChain, NodeCache, PackStore};
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        meta.set_node_prefix(id).unwrap();
        let backend = store.clone() as StdArc<dyn ObjectStore>;
        let dir = tempfile::TempDir::new().unwrap();
        let cache = Arc::new(NodeCache::new(
            PackStore::new(backend.clone()),
            Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap()),
            Hasher::Plain,
            tokio::runtime::Handle::current(),
        ));
        let mut publisher = crate::mtree_publish::TreePublisher::new(
            meta.clone(),
            cache,
            BlobStore::new(backend.clone(), Hasher::Plain),
            CommitChain::new(backend.clone()),
            record::config(),
            id,
        );
        publisher.restore().unwrap();
        let driver =
            Standalone::with_publisher(meta.clone(), backend, id, LeaseMode::Cas, Some(publisher));
        (meta, driver, dir)
    }

    /// Plan 30 M5 round 3 (`fresh-node-bootstrap`): the root directory's
    /// owner, set at the first mount (`adopt_root` through the core), must
    /// survive every later commit — the mid-run cadence publishes included
    /// — so a fresh node bootstrapping from the head commit gets the
    /// user's root, not genesis' `0:0`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_bootstrapped_replica_has_the_adopted_root_owner() {
        bootstrapped_root_owner(false).await;
    }

    /// The same with a log tail past the head commit (the shape main's
    /// `fresh-node-bootstrap` runs always had, which is why the shadowed
    /// genesis root never showed there: the replay's own writes
    /// re-wrote the root above it).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_bootstrapped_replica_with_a_log_tail_has_the_adopted_root_owner() {
        bootstrapped_root_owner(true).await;
    }

    async fn bootstrapped_root_owner(tail: bool) {
        use constellation_fs_core::types::ROOT_INO;
        let store = StdArc::new(InMemory::new());
        let (meta, mut driver, _dir) = node(&store, 1);
        assert!(driver.acquire().await.unwrap());
        let rid = constellation_meta::Rid {
            node: 1,
            incarnation: 1,
            seq: 1,
        };
        let op = constellation_meta::MutateOp::Setattr {
            ino: ROOT_INO,
            mode: None,
            uid: Some(1000),
            gid: Some(1000),
            size: None,
            atime_ns: None,
            mtime_ns: None,
        };
        let outcome = driver.submit(rid, op).await.unwrap();
        assert!(matches!(
            outcome,
            constellation_authority::ClientReply::Outcome(
                constellation_meta::MutateOutcome::Accepted { .. }
            )
        ));
        driver.sync().await.unwrap();
        driver.publish().await.unwrap();
        for i in 0..40 {
            meta.create(ROOT_INO, &format!("f{i}"), 0o644, 1000, 1000)
                .unwrap();
            if i % 10 == 9 {
                driver.sync().await.unwrap();
                driver.publish().await.unwrap();
            }
        }
        driver.sync().await.unwrap();
        let backend = store.clone() as StdArc<dyn ObjectStore>;
        let chain = constellation_store_s3::CommitChain::new(backend.clone());
        let head = chain.discover_head(0).await.unwrap().unwrap();
        assert!(head >= 2, "several commits: {head}");
        if tail {
            meta.create(ROOT_INO, "late", 0o644, 1000, 1000).unwrap();
            driver.sync().await.unwrap();
        }

        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path().join("fresh.db");
        bootstrap(&db, &LogStore::new(backend)).await.unwrap();
        let fresh = Meta::open(&db).unwrap();
        let root = MetaStore::getattr(&fresh, ROOT_INO).unwrap().unwrap();
        assert_eq!((root.uid, root.gid), (1000, 1000), "the fresh root owner");
        let mine = MetaStore::getattr(&*meta, ROOT_INO).unwrap().unwrap();
        assert_eq!((mine.uid, mine.gid), (1000, 1000));
    }
}
