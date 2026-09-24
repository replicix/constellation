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
            tokio::runtime::Handle::current(),
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
            .record_snapshot(&SnapshotRow {
                id: "snap-1".into(),
                path: "/d".into(),
                name: "one".into(),
                root_hash: "ab".repeat(32),
                created_unix_ms: 42,
            })
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
            tokio::runtime::Handle::current(),
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
