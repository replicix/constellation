//! The service against a real stack: an in-memory `Meta`, real publishes
//! into an in-memory bucket, the snapshot manager, and `verify`'s brute
//! force as the oracle (plan 32 §11, "the model test").

use super::service::*;
use super::verify::BruteForce;
use crate::snapshot::{test_manager, SnapshotManager, SnapshotOptions};
use constellation_fs_core::manifest::{ChunkInfo, Manifest, SparseChunks};
use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::{ChunkHash, Ino, InodeKind};
use constellation_meta::{LogRecord, Meta, MetaStore};
use constellation_store_s3::{ChunkStore, CommitChain, CompressionSetting};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const CS: u32 = 4096;
/// Chunk lists longer than this spill (small, so tests spill often).
const INLINE_MAX: usize = 3;

/// A chunk's content is named by its tag and its length, so a hash
/// always has one size (as real content addressing guarantees).
fn chunk(tag: &str, size: u64) -> ChunkHash {
    ChunkHash::of(format!("{tag}/{size}").as_bytes())
}

struct Fixture {
    meta: Arc<Meta>,
    store: Arc<dyn ObjectStore>,
    chunks: Arc<ChunkStore>,
    manager: SnapshotManager,
    state: tempfile::TempDir,
    _nodes: tempfile::TempDir,
    segment: u64,
    taken: u64,
    /// Every segment shipped (`ack`), for a follower replica to apply.
    log: Vec<(u64, Vec<LogRecord>)>,
}

impl Fixture {
    fn new() -> Fixture {
        let meta = Arc::new(crate::mtree_publish::test_meta());
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let chunks = Arc::new(ChunkStore::new(store.clone()));
        let (manager, nodes) = test_manager(meta.clone(), chunks.clone(), CS);
        Fixture {
            meta,
            store,
            chunks,
            manager,
            state: tempfile::TempDir::new().unwrap(),
            _nodes: nodes,
            segment: 0,
            taken: 0,
            log: Vec::new(),
        }
    }

    fn config(mode: SnapAcctMode) -> SnapAcctConfig {
        SnapAcctConfig {
            mode,
            refresh: Duration::from_secs(3600),
            budget: Duration::from_secs(3600),
            max_ops_per_pass: None,
            answer_wait: Duration::ZERO,
            params: super::SnapAcctParams::default(),
        }
    }

    fn service(&self, cfg: SnapAcctConfig) -> Arc<SnapAcctService> {
        self.service_on(cfg, self.meta.clone(), self.state.path())
    }

    /// A service over another replica of the same bucket.
    fn service_on(
        &self,
        cfg: SnapAcctConfig,
        meta: Arc<Meta>,
        state: &std::path::Path,
    ) -> Arc<SnapAcctService> {
        SnapAcctService::new(
            cfg,
            SnapAcctDeps {
                meta,
                chunks: self.chunks.clone(),
                tree: self.manager.tree().unwrap().clone(),
                commits: CommitChain::new(self.store.clone()),
                dir: state.join(DIR),
                fs_uuid: "model".into(),
            },
        )
    }

    /// Apply to `follower` every shipped segment it has not applied yet.
    fn replicate(&self, follower: &Meta) {
        let mine = follower.applied_seq().unwrap();
        for (segment, records) in self.log.iter().filter(|(seq, _)| *seq > mine) {
            follower
                .apply_segment(
                    *segment,
                    1,
                    records,
                    &constellation_meta::TouchSet::default(),
                )
                .unwrap();
        }
    }

    fn mkdir(&self, parent: Ino, name: &str) -> Ino {
        self.meta.mkdir(parent, name, 0o755, 1, 1).unwrap().ino
    }

    /// Write `ino` as chunks `tags` (`""` is a hole), the last one `tail`
    /// bytes long, spilling long lists.
    async fn write(&self, ino: Ino, tags: &[String], tail: u64) {
        let n = tags.len() as u64;
        let file_len = match n {
            0 => 0,
            _ => (n - 1) * CS as u64 + tail,
        };
        let sparse: SparseChunks = tags
            .iter()
            .enumerate()
            .filter(|(_, tag)| !tag.is_empty())
            .map(|(index, tag)| {
                let size = if index as u64 == n - 1 {
                    tail
                } else {
                    CS as u64
                };
                (index as u64, chunk(tag, size))
            })
            .collect();
        let (manifest, blob) =
            Manifest::from_sparse_chunks(CS, file_len, sparse, INLINE_MAX, ChunkHash::of);
        if let (ChunkInfo::Spilled(hash), Some(blob)) = (&manifest.chunks, blob) {
            self.chunks
                .put_chunk(hash, &blob, CompressionSetting::RAW)
                .await
                .unwrap();
        }
        self.meta
            .set_manifest(ino, &manifest.encode(), file_len)
            .unwrap();
    }

    async fn file(&self, parent: Ino, name: &str, tags: &[String], tail: u64) -> Ino {
        let ino = self.meta.create(parent, name, 0o644, 1, 1).unwrap().ino;
        self.write(ino, tags, tail).await;
        ino
    }

    fn ack(&mut self) {
        self.segment += 1;
        let rows = self.meta.take_journal(usize::MAX).unwrap();
        let seqs: Vec<u64> = rows.iter().map(|(seq, _)| *seq).collect();
        self.meta.ack_journal_rows_at(&seqs, self.segment).unwrap();
        self.log
            .push((self.segment, rows.into_iter().map(|(_, r)| r).collect()));
    }

    /// Publish the replica as a commit (the live refresh diffs commits).
    async fn publish(&mut self) {
        self.ack();
        self.manager.publish_commit().await.unwrap();
    }

    async fn snap(&mut self, path: &str) -> String {
        self.ack();
        self.taken += 1;
        let (_, row) = self
            .manager
            .create_with(
                path,
                &format!("s{}", self.taken),
                &SnapshotOptions::default(),
            )
            .await
            .unwrap();
        row.id
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn pick<T: Clone>(&mut self, items: &[T]) -> Option<T> {
        (!items.is_empty()).then(|| items[self.below(items.len())].clone())
    }
}

/// Every `(parent, name, ino, kind)` reachable from the root.
fn entries(meta: &Meta) -> Vec<(Ino, String, Ino, InodeKind)> {
    let mut out = Vec::new();
    let mut stack = vec![ROOT_INO];
    while let Some(parent) = stack.pop() {
        for entry in meta.readdir(parent).unwrap() {
            if entry.kind == InodeKind::Dir {
                stack.push(entry.ino);
            }
            out.push((parent, entry.name, entry.ino, entry.kind));
        }
    }
    out
}

async fn assert_verified(svc: &SnapAcctService, what: &str) -> super::VerifyReport {
    let report = svc.verify().await.unwrap();
    assert_eq!(
        report.mismatches,
        0,
        "{what}: the index disagrees with the brute force:\n{}",
        report.details.join("\n")
    );
    report
}

/// `reclaim` of random subsets through the service API, against the
/// brute force.
async fn check_reclaim(fx: &Fixture, svc: &SnapAcctService, rng: &mut Rng, what: &str) {
    let brute = BruteForce::compute(&fx.meta, fx.manager.tree().unwrap(), &fx.chunks)
        .await
        .unwrap();
    if brute.snapshots.is_empty() {
        return;
    }
    for _ in 0..3 {
        let set: BTreeSet<usize> = (0..brute.snapshots.len())
            .filter(|_| rng.below(3) == 0)
            .collect();
        let ids: Vec<String> = set.iter().map(|&i| brute.snapshots[i].id.clone()).collect();
        let answer = svc.reclaim(&ids).await.unwrap();
        let estimate = answer.ready().expect("a caught-up index answers");
        let exact = brute.reclaim(&set);
        assert_eq!(
            (estimate.bytes, estimate.chunks),
            (exact.bytes, exact.chunks),
            "{what}: reclaim({ids:?})"
        );
    }
    // And one snapshot's numbers through the API.
    let i = rng.below(brute.snapshots.len());
    let numbers = svc
        .snap_numbers(&brute.snapshots[i].id)
        .await
        .unwrap()
        .ready()
        .unwrap();
    let exact = brute.exact(i);
    assert_eq!(
        (numbers.used, numbers.written, numbers.refer, numbers.lsize),
        (exact.used, exact.written, exact.refer, exact.lsize),
        "{what}: numbers of {}",
        brute.snapshots[i].id
    );
}

async fn model_history(seed: u64, steps: usize) {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let mut fx = Fixture::new();
    let a = fx.mkdir(ROOT_INO, "a");
    let nested = fx.mkdir(a, "n");
    let b = fx.mkdir(ROOT_INO, "b");
    fx.mkdir(ROOT_INO, "out");
    let roots = ["/a", "/a/n", "/b", "/"];
    let fixed = [a, nested, b];
    let pool: Vec<String> = (0..12).map(|i| format!("c{i}")).collect();
    let svc = fx.service(Fixture::config(SnapAcctMode::On));
    let mut written: Vec<(Vec<String>, u64)> = Vec::new();
    let mut snapshots: Vec<(String, String)> = Vec::new();
    let mut names = 0u64;
    let mut kinds = [0usize; 12];
    for step in 0..steps {
        let all = entries(&fx.meta);
        let dirs: Vec<Ino> = std::iter::once(ROOT_INO)
            .chain(all.iter().filter(|e| e.3 == InodeKind::Dir).map(|e| e.2))
            .collect();
        let files: Vec<_> = all
            .iter()
            .filter(|e| e.3 == InodeKind::File)
            .cloned()
            .collect();
        let movable: Vec<_> = all
            .iter()
            .filter(|e| e.3 == InodeKind::Dir && !fixed.contains(&e.2))
            .cloned()
            .collect();
        names += 1;
        let name = format!("n{names}");
        let content = |rng: &mut Rng| -> (Vec<String>, u64) {
            let n = 1 + rng.below(6);
            let tags = (0..n)
                .map(|_| match rng.below(7) {
                    0 => String::new(),
                    _ => pool[rng.below(pool.len())].clone(),
                })
                .collect();
            (tags, [CS as u64, 1000, 17][rng.below(3)])
        };
        let kind = rng.below(12);
        kinds[kind] += 1;
        match kind {
            0 | 1 => {
                let parent = rng.pick(&dirs).unwrap();
                let (tags, tail) = content(&mut rng);
                fx.file(parent, &name, &tags, tail).await;
                written.push((tags, tail));
            }
            2 => {
                if let Some(file) = rng.pick(&files) {
                    let (tags, tail) = content(&mut rng);
                    fx.write(file.2, &tags, tail).await;
                    written.push((tags, tail));
                }
            }
            3 => {
                // Truncate: keep a prefix, the new tail shorter.
                if let (Some(file), Some((tags, _))) = (rng.pick(&files), rng.pick(&written)) {
                    let keep = rng.below(tags.len() + 1);
                    fx.write(file.2, &tags[..keep], 11).await;
                }
            }
            4 => {
                // Revert to content written before (an afterlife).
                if let (Some(file), Some((tags, tail))) = (rng.pick(&files), rng.pick(&written)) {
                    fx.write(file.2, &tags, tail).await;
                }
            }
            5 => {
                if let Some(file) = rng.pick(&files) {
                    fx.meta.unlink(file.0, &file.1).unwrap();
                }
            }
            6 => {
                if let Some(file) = rng.pick(&files) {
                    let parent = rng.pick(&dirs).unwrap();
                    fx.meta.link(file.2, parent, &name).unwrap();
                }
            }
            7 => {
                if let Some(file) = rng.pick(&files) {
                    let parent = rng.pick(&dirs).unwrap();
                    let _ = fx.meta.rename(file.0, &file.1, parent, &name);
                }
            }
            8 => {
                // A directory across a policy root's boundary (or not).
                if let Some(dir) = rng.pick(&movable) {
                    let parent = rng.pick(&dirs).unwrap();
                    let _ = fx.meta.rename(dir.0, &dir.1, parent, &name);
                }
            }
            9 => {
                let parent = rng.pick(&dirs).unwrap();
                fx.mkdir(parent, &name);
            }
            10 => {
                // Delete a snapshot, often from the middle of its chain.
                if let Some(at) = (!snapshots.is_empty()).then(|| rng.below(snapshots.len())) {
                    let (path, snap) = snapshots.remove(at);
                    fx.manager.delete(&path, &snap, true).await.unwrap();
                }
            }
            _ => {
                if let Some((path, snap)) = rng.pick(&snapshots) {
                    let held = rng.below(2) == 0;
                    fx.manager
                        .hold(&format!("{path}@{snap}"), held, None, true)
                        .await
                        .unwrap();
                }
            }
        }
        if rng.below(3) == 0 {
            let path = roots[rng.below(roots.len())];
            fx.snap(path).await;
            snapshots.push((path.to_string(), format!("s{}", fx.taken)));
        }
        fx.publish().await;
        assert_verified(&svc, &format!("seed {seed}, step {step}")).await;
        if step % 10 == 9 {
            check_reclaim(&fx, &svc, &mut rng, &format!("seed {seed}, step {step}")).await;
        }
    }
    assert!(
        kinds.iter().all(|&n| n > 0) || steps < 60,
        "seed {seed}: {kinds:?}"
    );
    let stats = svc.stats();
    assert!(stats.steps.load(Ordering::Relaxed) > 0);
    assert!(stats.refreshes.load(Ordering::Relaxed) > 0);
}

/// Plan 32 §11: seeded random histories through the real stack; after
/// every step `USED`/`WRITTEN`/`REFER`/`LSIZE`, the chain and whole-set
/// `reclaim` and the buckets equal the brute force, and random `reclaim`
/// sets and numbers through the query API do too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn model_agrees_with_brute_force() {
    for seed in 1..=6 {
        model_history(seed, 70).await;
    }
}

/// The same, longer and wider (`cargo test -- --ignored`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn model_agrees_with_brute_force_heavy() {
    for seed in 1..=60 {
        model_history(seed, 150).await;
    }
}

/// A fixture with two chains' worth of history: `count` snapshots,
/// alternating `/a` and `/b`, a write between each.
async fn history(fx: &mut Fixture, count: usize) {
    let a = fx.mkdir(ROOT_INO, "a");
    let b = fx.mkdir(ROOT_INO, "b");
    let tags: Vec<String> = (0..8).map(|i| format!("t{i}")).collect();
    let fa = fx.file(a, "f", &tags[..5], 300).await;
    let fb = fx.file(b, "f", &tags[3..], CS as u64).await;
    for i in 0..count {
        let (file, path) = if i % 2 == 0 { (fa, "/a") } else { (fb, "/b") };
        let content: Vec<String> = (0..1 + i % 5).map(|j| format!("w{}", i + j)).collect();
        fx.write(file, &content, 100 + i as u64).await;
        fx.snap(path).await;
    }
    fx.publish().await;
}

/// `auto` with nothing asking does no work at all: no directory, no
/// pass, however many snapshots change. The first query builds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_without_requests_does_no_work() {
    let mut fx = Fixture::new();
    let mut cfg = Fixture::config(SnapAcctMode::Auto);
    cfg.answer_wait = Duration::from_secs(30);
    cfg.refresh = Duration::from_millis(50);
    let svc = fx.service(cfg);
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn(svc.clone().run(stop.clone(), None));
    history(&mut fx, 4).await;
    // A policy root alone does not start anything either.
    fx.meta
        .set_xattr(
            fx.meta.resolve_path("/a").unwrap().unwrap(),
            constellation_meta::SNAPSHOT_POLICY_XATTR,
            b"1h:1d",
            constellation_meta::SetXattrMode::Set,
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!svc.dir().exists(), "auto created the index unasked");
    assert_eq!(svc.stats().passes.load(Ordering::Relaxed), 0);
    assert!(!svc.maintaining());

    // The first size request builds and answers.
    let id = fx.meta.snapshots(None).unwrap()[0].id.clone();
    let answer = svc.snap_numbers(&id).await.unwrap();
    assert!(matches!(answer, SnapAnswer::Ready(_)), "{answer:?}");
    assert!(svc.dir().exists());
    // With a policy root it keeps maintaining after the request.
    assert!(svc.maintaining());
    assert_verified(&svc, "auto after the first request").await;
    stop.store(true, Ordering::Relaxed);
    svc.set_web_ui(false);
    task.abort();
}

/// `off`: nothing opened, every query answers `Off`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn off_answers_off() {
    let mut fx = Fixture::new();
    let svc = fx.service(Fixture::config(SnapAcctMode::Off));
    history(&mut fx, 2).await;
    let id = fx.meta.snapshots(None).unwrap()[0].id.clone();
    assert_eq!(svc.snap_numbers(&id).await.unwrap(), SnapAnswer::Off);
    assert_eq!(svc.reclaim(&[id]).await.unwrap(), SnapAnswer::Off);
    assert_eq!(svc.space(None).await.unwrap(), SnapAnswer::Off);
    assert!(svc.verify().await.is_err());
    assert!(!svc.dir().exists());
}

/// Every number the index holds, by snapshot id (ordinals and chain ids
/// are internal) and the buckets.
async fn snapshot_of_numbers(svc: &SnapAcctService, fx: &Fixture) -> Vec<String> {
    let mut out = Vec::new();
    for row in fx.meta.snapshots(None).unwrap() {
        let numbers = svc.snap_numbers(&row.id).await.unwrap().ready().unwrap();
        out.push(format!(
            "{} {} {} {} {}",
            row.id, numbers.used, numbers.written, numbers.refer, numbers.lsize
        ));
    }
    let space = svc.space(None).await.unwrap().ready().unwrap();
    out.push(format!(
        "{:?} {:?} {:?} {:?}",
        space.snapshots_total, space.unique, space.shared_snapshots_only, space.shared_with_live
    ));
    let ids: Vec<String> = fx
        .meta
        .snapshots(None)
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect();
    out.push(format!(
        "{:?}",
        svc.reclaim(&ids).await.unwrap().ready().unwrap().bytes
    ));
    out
}

/// Deleting the index directory: rebuilt from the rows, identically
/// (tombstones, which are history, excepted).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deleted_index_is_rebuilt_identically() {
    let mut fx = Fixture::new();
    history(&mut fx, 8).await;
    // A deletion in the middle, so the history is not append-only.
    let rows = fx.meta.snapshots(Some("/a")).unwrap();
    fx.manager.delete("/a", &rows[1].name, true).await.unwrap();
    fx.publish().await;
    let svc = fx.service(Fixture::config(SnapAcctMode::On));
    svc.catch_up().await.unwrap();
    let before = snapshot_of_numbers(&svc, &fx).await;
    let dir = svc.dir().to_path_buf();
    drop(svc);
    std::fs::remove_dir_all(&dir).unwrap();
    let svc = fx.service(Fixture::config(SnapAcctMode::On));
    svc.catch_up().await.unwrap();
    assert_eq!(snapshot_of_numbers(&svc, &fx).await, before);
    assert_verified(&svc, "rebuilt").await;
}

/// A build interrupted part-way resumes where it stopped: nothing
/// applied before the restart is walked again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_resumes_a_partial_build() {
    let mut fx = Fixture::new();
    history(&mut fx, 10).await;
    let total = fx.meta.snapshots(None).unwrap().len() as u64;
    let svc = fx.service(Fixture::config(SnapAcctMode::On));
    assert!(
        svc.step(4).await.unwrap(),
        "four operations do not finish ten snapshots"
    );
    assert!(svc.stats().building.load(Ordering::Relaxed));
    let pct = svc.stats().build_progress_pct.load(Ordering::Relaxed);
    assert_eq!(pct, 40);
    // Queries during the build: never a partial number.
    let id = fx.meta.snapshots(None).unwrap()[0].id.clone();
    assert_eq!(
        svc.snap_numbers(&id).await.unwrap(),
        SnapAnswer::Building { pct: 40 }
    );
    assert!(matches!(
        svc.space(None).await.unwrap(),
        SnapAnswer::Building { .. }
    ));
    let first_part =
        svc.stats().first_walks.load(Ordering::Relaxed) + svc.stats().steps.load(Ordering::Relaxed);
    assert_eq!(first_part, 4);
    drop(svc);

    let svc = fx.service(Fixture::config(SnapAcctMode::On));
    svc.catch_up().await.unwrap();
    let second_part =
        svc.stats().first_walks.load(Ordering::Relaxed) + svc.stats().steps.load(Ordering::Relaxed);
    assert_eq!(
        first_part + second_part,
        total,
        "work was redone after the restart"
    );
    assert_eq!(svc.stats().operations.load(Ordering::Relaxed), total - 4);
    assert!(!svc.stats().building.load(Ordering::Relaxed));
    assert_verified(&svc, "resumed").await;
}

/// A snapshot created after the build is applied on the change hint,
/// one created *before* the chain's head (a late row) re-derives the
/// suffix, and a deleted head steps back: all against the brute force.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_rows_and_head_deletion() {
    let mut fx = Fixture::new();
    history(&mut fx, 6).await;
    let svc = fx.service(Fixture::config(SnapAcctMode::On));
    svc.catch_up().await.unwrap();
    // A row that sorts before the head: same chain, an older commit.
    let rows = fx.meta.snapshots(Some("/a")).unwrap();
    let mut late = rows[0].clone();
    late.name = "late".into();
    late.id = constellation_meta::snapshot_id("/a", "late");
    late.created_unix_ms -= 1;
    fx.meta.record_snapshot(&late).unwrap();
    assert_verified(&svc, "late row").await;
    // The head of /b deleted.
    let rows = fx.meta.snapshots(Some("/b")).unwrap();
    let head = rows
        .iter()
        .max_by_key(|r| {
            crate::snapshot::SnapshotRoot::parse(&r.root_hash)
                .unwrap()
                .seq
        })
        .unwrap();
    fx.manager.delete("/b", &head.name, true).await.unwrap();
    assert_verified(&svc, "head deleted").await;
    // Everything deleted: the chains empty out.
    for row in fx.meta.snapshots(None).unwrap() {
        fx.meta.delete_snapshot_by_id(&row.id).unwrap();
    }
    let report = assert_verified(&svc, "all deleted").await;
    assert_eq!(report.snapshots, 0);
    let space = svc.space(None).await.unwrap().ready().unwrap();
    assert_eq!(space.snapshots_total.bytes, 0);
    assert!(space.awaiting_gc.bytes > 0, "freed chunks await GC");
}

/// The live refresh follows the live tree by commit diff, including a
/// spilled list's members, and `space(path)` scopes to the chains under
/// a path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_refresh_and_scoped_space() {
    let mut fx = Fixture::new();
    let a = fx.mkdir(ROOT_INO, "a");
    let b = fx.mkdir(ROOT_INO, "b");
    let big: Vec<String> = (0..6).map(|i| format!("big{i}")).collect();
    let file = fx.file(a, "big", &big, CS as u64).await;
    fx.file(b, "small", &["s".to_string()], 10).await;
    let snap_a = fx.snap("/a").await;
    fx.snap("/b").await;
    fx.publish().await;
    let svc = fx.service(Fixture::config(SnapAcctMode::On));
    svc.catch_up().await.unwrap();
    // Everything is live: nothing is reclaimable.
    let n = svc.snap_numbers(&snap_a).await.unwrap().ready().unwrap();
    assert_eq!(n.used, 0);
    assert!(n.refer > 6 * CS as u64, "{n:?}");
    // Rewrite the big file: its old members (inside a spilled list) are
    // now only the snapshot's.
    fx.write(file, &["other".to_string()], 5).await;
    fx.publish().await;
    let report = assert_verified(&svc, "after the rewrite").await;
    assert_eq!(report.snapshots, 2);
    let n = svc.snap_numbers(&snap_a).await.unwrap().ready().unwrap();
    assert!(n.used > 6 * CS as u64, "{n:?}");
    assert!(n.as_of_seq > 0 && n.as_of_ms > 0);
    // A file sized by truncate alone (no manifest) and a second name of
    // the rewritten file: apparent bytes, each inode once.
    let sparse = fx.meta.create(a, "sparse", 0o644, 1, 1).unwrap().ino;
    fx.meta
        .setattr(sparse, None, None, None, Some(1 << 20), None, None)
        .unwrap();
    fx.meta.link(file, a, "big2").unwrap();
    let scoped = svc.space(Some("/a")).await.unwrap().ready().unwrap();
    assert_eq!(scoped.unique.bytes, n.used);
    assert_eq!(scoped.snapshots_total.bytes, n.used);
    assert_eq!(scoped.live_logical, 5 + (1 << 20));
    let whole = svc.space(None).await.unwrap().ready().unwrap();
    assert_eq!(whole.live_logical, 5 + (1 << 20) + 10);
    let whole = svc.space(None).await.unwrap().ready().unwrap();
    assert_eq!(whole.shared_with_live.chunks, 1, "/b's chunk is still live");
    assert!(whole.compression_ratio.is_none(), "no GC census yet");
    assert_eq!(svc.stats().full_refreshes.load(Ordering::Relaxed), 1);
    drop(svc);

    // With a GC census, the estimate: mean logical size of an indexed
    // chunk over the mean stored size of a chunk object.
    constellation_store_s3::write_chunk_census(
        &fx.store,
        &constellation_store_s3::ChunkCensus {
            chunk_objects: 10,
            physical_bytes: 10 * 1024,
            as_of_ms: 1,
        },
    )
    .await
    .unwrap();
    let svc = fx.service(Fixture::config(SnapAcctMode::On));
    // A restarted service answers once a pass has seen the rows.
    assert!(matches!(
        svc.space(None).await.unwrap(),
        SnapAnswer::Building { .. }
    ));
    svc.catch_up().await.unwrap();
    let whole = svc.space(None).await.unwrap().ready().unwrap();
    let indexed = whole.snapshots_total.bytes + whole.shared_with_live.bytes;
    let chunks = whole.snapshots_total.chunks + whole.shared_with_live.chunks;
    let expected = indexed as f64 / chunks as f64 / 1024.0;
    let ratio = whole.compression_ratio.expect("a census is there");
    assert!((ratio - expected).abs() < 1e-9, "{ratio} vs {expected}");
}

fn dir_bytes(dir: &std::path::Path) -> u64 {
    let mut total = 0;
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let meta = entry.metadata().unwrap();
        // Allocated blocks: fjall preallocates its journal sparsely.
        total += if meta.is_dir() {
            dir_bytes(&entry.path())
        } else {
            std::os::unix::fs::MetadataExt::blocks(&meta) * 512
        };
    }
    total
}

/// Report only: build time and footprint of a 10,000-file tree with 50
/// snapshots of 20 rewrites each (the M0b GC measurement's shape).
/// `cargo test -p constellation-engine --release snapacct::service_tests::measure -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "measurement"]
async fn measure_build_time_and_footprint() {
    let mut fx = Fixture::new();
    let vol = fx.mkdir(ROOT_INO, "vol");
    let mut files = Vec::new();
    for d in 0..100 {
        let dir = fx.mkdir(vol, &format!("d{d}"));
        for f in 0..100 {
            let tags = vec![format!("f{d}.{f}"), format!("g{d}.{f}")];
            files.push(fx.file(dir, &format!("f{f}"), &tags, 1000).await);
        }
    }
    fx.snap("/vol").await;
    let mut rng = Rng(42);
    for s in 0..49 {
        for r in 0..20 {
            let file = files[rng.below(files.len())];
            fx.write(file, &[format!("r{s}.{r}"), format!("q{s}.{r}")], 1000)
                .await;
        }
        fx.snap("/vol").await;
    }
    fx.publish().await;
    let svc = fx.service(Fixture::config(SnapAcctMode::On));
    let started = std::time::Instant::now();
    svc.catch_up().await.unwrap();
    let elapsed = started.elapsed();
    let stats = svc.stats();
    let indexed = stats.indexed_chunks.load(Ordering::Relaxed);
    let tables = stats.index_bytes.load(Ordering::Relaxed);
    let dir = svc.dir().to_path_buf();
    drop(svc);
    let on_disk = dir_bytes(&dir);
    eprintln!(
        "snapshots 50, indexed chunks {indexed}, build {:.2} s, tables {tables} B, \
         directory {on_disk} B allocated ({:.1} B/chunk incl. journal)",
        elapsed.as_secs_f64(),
        on_disk as f64 / indexed as f64
    );
}

/// The recovery wipe never deletes an index a holder still has open: it
/// waits for the next open with nobody else holding it, then the index
/// builds again from the rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wipe_waits_for_the_index_to_be_free() {
    let mut fx = Fixture::new();
    history(&mut fx, 4).await;
    let svc = fx.service(Fixture::config(SnapAcctMode::On));
    svc.catch_up().await.unwrap();
    let before = snapshot_of_numbers(&svc, &fx).await;
    let walks = svc.stats().first_walks.load(Ordering::Relaxed);
    svc.wipe();
    svc.catch_up().await.unwrap();
    assert!(
        svc.stats().first_walks.load(Ordering::Relaxed) > walks,
        "the wiped index was rebuilt from the rows"
    );
    assert_eq!(snapshot_of_numbers(&svc, &fx).await, before);
    assert_verified(&svc, "after a wipe").await;
}

/// A follower replica (applies the holder's shipped segments) with its
/// own index.
fn follower(fx: &Fixture) -> (Arc<Meta>, tempfile::TempDir) {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    fx.replicate(&meta);
    (meta, tempfile::TempDir::new().unwrap())
}

/// Plan 32 §6.2 on a node that is not the publisher: the live refresh
/// never labels flags read from the replica with a commit the replica
/// has not applied. The holder unlinks a file whose chunks a snapshot
/// holds and publishes; a lagging follower refreshes (an index that
/// follows by diff, and a new one that recomputes in full), then applies
/// the unlink: both indexes come to agree with the brute force.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lagging_follower_never_misses_an_unlink() {
    let mut fx = Fixture::new();
    let a = fx.mkdir(ROOT_INO, "a");
    let tags: Vec<String> = (0..2).map(|i| format!("u{i}")).collect();
    fx.file(a, "f", &tags, 100).await;
    let snap = fx.snap("/a").await;
    fx.publish().await;
    let (meta, state) = follower(&fx);
    let svc = fx.service_on(
        Fixture::config(SnapAcctMode::On),
        meta.clone(),
        state.path(),
    );
    svc.catch_up().await.unwrap();
    let n = svc.snap_numbers(&snap).await.unwrap().ready().unwrap();
    assert_eq!(n.used, 0, "the file is live");
    let accounted = n.as_of_seq;

    fx.meta.unlink(a, "f").unwrap();
    fx.publish().await;
    // The follower has not applied the unlink yet.
    svc.catch_up().await.unwrap();
    let lagging_as_of = svc
        .snap_numbers(&snap)
        .await
        .unwrap()
        .ready()
        .unwrap()
        .as_of_seq;
    let fresh_state = tempfile::TempDir::new().unwrap();
    let fresh = fx.service_on(
        Fixture::config(SnapAcctMode::On),
        meta.clone(),
        fresh_state.path(),
    );
    fresh.catch_up().await.unwrap();
    let fresh_as_of = fresh.stats().as_of_seq.load(Ordering::Relaxed);

    fx.replicate(&meta);
    for (svc, what) in [(&svc, "by diff"), (&fresh, "recomputed")] {
        assert_verified(svc, &format!("follower {what}, unlink applied")).await;
        let n = svc.snap_numbers(&snap).await.unwrap().ready().unwrap();
        assert_eq!(
            n.used,
            100 + CS as u64,
            "{what}: only the snapshot holds the file"
        );
        assert!(n.as_of_seq > accounted, "{what}");
    }
    // While lagging, neither labelled its flags with the newer commit.
    assert_eq!(
        lagging_as_of, accounted,
        "the flags stayed at the old commit"
    );
    assert!(fresh_as_of <= accounted, "a recompute on a lagging replica");
    assert!(svc.stats().refreshes_deferred.load(Ordering::Relaxed) > 0);
}

/// The unsafe direction: the holder rewrites a file back to content a
/// snapshot holds (its chunks become live again). A lagging follower
/// must not keep them reclaimable once it has applied the rewrite.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lagging_follower_never_misses_a_rewrite_back() {
    let mut fx = Fixture::new();
    let a = fx.mkdir(ROOT_INO, "a");
    let old: Vec<String> = (0..2).map(|i| format!("x{i}")).collect();
    let file = fx.file(a, "f", &old, 100).await;
    let snap = fx.snap("/a").await;
    fx.write(file, &["y".to_string()], 7).await;
    fx.publish().await;
    let (meta, state) = follower(&fx);
    let svc = fx.service_on(
        Fixture::config(SnapAcctMode::On),
        meta.clone(),
        state.path(),
    );
    svc.catch_up().await.unwrap();
    let freed = svc
        .reclaim(std::slice::from_ref(&snap))
        .await
        .unwrap()
        .ready()
        .unwrap();
    assert_eq!(freed.bytes, 100 + CS as u64);

    fx.write(file, &old, 100).await;
    fx.publish().await;
    svc.catch_up().await.unwrap();
    let fresh_state = tempfile::TempDir::new().unwrap();
    let fresh = fx.service_on(
        Fixture::config(SnapAcctMode::On),
        meta.clone(),
        fresh_state.path(),
    );
    fresh.catch_up().await.unwrap();

    fx.replicate(&meta);
    for (svc, what) in [(&svc, "by diff"), (&fresh, "recomputed")] {
        assert_verified(svc, &format!("follower {what}, rewrite applied")).await;
        let freed = svc
            .reclaim(std::slice::from_ref(&snap))
            .await
            .unwrap()
            .ready()
            .unwrap();
        assert_eq!(freed.bytes, 0, "{what}: the content is live again");
    }
}

/// Snapshot ids name `path@name`: `/x@s1` of one directory deleted and
/// re-created over another directory (a lower inode, so its chain is
/// visited first) before the index sees either change. Deletions are
/// applied across every chain first, so the re-created snapshot can be
/// appended.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recreated_snapshot_id_moves_between_chains() {
    let mut fx = Fixture::new();
    let low = fx.mkdir(ROOT_INO, "y");
    let high = fx.mkdir(ROOT_INO, "x");
    assert!(low < high);
    fx.file(low, "f", &["low".to_string()], 10).await;
    fx.file(high, "f", &["high".to_string()], 20).await;
    async fn snap(fx: &mut Fixture) -> String {
        fx.ack();
        fx.manager
            .create_with("/x", "s1", &SnapshotOptions::default())
            .await
            .unwrap()
            .1
            .id
    }
    let first = snap(&mut fx).await;
    fx.publish().await;
    let svc = fx.service(Fixture::config(SnapAcctMode::On));
    svc.catch_up().await.unwrap();

    fx.meta.rename(ROOT_INO, "x", ROOT_INO, "old").unwrap();
    fx.meta.rename(ROOT_INO, "y", ROOT_INO, "x").unwrap();
    fx.manager.delete("/x", "s1", true).await.unwrap();
    let second = snap(&mut fx).await;
    assert_eq!(first, second, "the same id");
    fx.publish().await;
    svc.catch_up().await.unwrap();
    assert_eq!(svc.stats().stalled_chains.load(Ordering::Relaxed), 0);
    assert_verified(&svc, "re-created over another directory").await;
    let n = svc.snap_numbers(&second).await.unwrap().ready().unwrap();
    assert_eq!(n.refer, 10);
}

/// A chain that cannot be applied (its snapshot's tree is unreadable)
/// stalls alone: the other chains are applied, the stall is counted,
/// and every query answers `Building` rather than numbers missing a
/// chain. Once the row is gone, answers resume.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_chain_does_not_stop_the_others() {
    let mut fx = Fixture::new();
    history(&mut fx, 4).await;
    let a = fx.meta.resolve_path("/a").unwrap().unwrap();
    let broken_dir = fx.mkdir(ROOT_INO, "c");
    assert!(broken_dir > a);
    let rows = fx.meta.snapshots(Some("/a")).unwrap();
    let mut broken = rows[0].clone();
    let mut root = crate::snapshot::SnapshotRoot::parse(&broken.root_hash).unwrap();
    root.root = constellation_mtree::NodeHash([7; 32]);
    root.ino = 1;
    broken.root_hash = root.encode();
    broken.path = "/".into();
    broken.name = "broken".into();
    broken.id = constellation_meta::snapshot_id("/", "broken");
    // Built first: a stall after the first build must not answer Ready.
    let svc = fx.service(Fixture::config(SnapAcctMode::On));
    svc.catch_up().await.unwrap();
    fx.meta.record_snapshot(&broken).unwrap();
    svc.catch_up().await.unwrap();
    assert_eq!(svc.stats().stalled_chains.load(Ordering::Relaxed), 1);
    assert!(svc.stats().building.load(Ordering::Relaxed));
    let id = rows[1].id.clone();
    assert!(matches!(
        svc.snap_numbers(&id).await.unwrap(),
        SnapAnswer::Building { .. }
    ));
    assert!(matches!(
        svc.space(None).await.unwrap(),
        SnapAnswer::Building { .. }
    ));
    // The other chain is applied around the stalled one (it sorts after
    // it): a new snapshot of /b is one more operation.
    let b = fx.meta.resolve_path("/b").unwrap().unwrap();
    assert!(b > 1);
    fx.file(b, "late", &["late".to_string()], 10).await;
    fx.snap("/b").await;
    fx.publish().await;
    let before = svc.stats().operations.load(Ordering::Relaxed);
    svc.catch_up().await.unwrap();
    assert_eq!(svc.stats().stalled_chains.load(Ordering::Relaxed), 1);
    assert!(svc.stats().operations.load(Ordering::Relaxed) > before);
    // Removing the bad row is all it takes.
    let operations = svc.stats().operations.load(Ordering::Relaxed);
    fx.meta.delete_snapshot_by_id(&broken.id).unwrap();
    svc.catch_up().await.unwrap();
    assert_eq!(svc.stats().stalled_chains.load(Ordering::Relaxed), 0);
    assert!(
        svc.stats().operations.load(Ordering::Relaxed) - operations <= 1,
        "nothing but the stalled chain's cleanup was left"
    );
    assert!(svc.snap_numbers(&id).await.unwrap().ready().is_some());
    assert_verified(&svc, "after the stalled row went").await;
}

/// After the first build, a pass cut short by its budget leaves the
/// index behind the rows: queries answer `Building`, not numbers that
/// leave out the snapshots not applied yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partial_index_never_answers_ready() {
    let mut fx = Fixture::new();
    history(&mut fx, 4).await;
    let svc = fx.service(Fixture::config(SnapAcctMode::On));
    svc.catch_up().await.unwrap();
    let id = fx.meta.snapshots(None).unwrap()[0].id.clone();
    assert!(svc.snap_numbers(&id).await.unwrap().ready().is_some());
    let b = fx.meta.resolve_path("/b").unwrap().unwrap();
    for i in 0..3 {
        fx.file(b, &format!("more{i}"), &[format!("m{i}")], 10)
            .await;
        fx.snap("/b").await;
    }
    fx.publish().await;
    assert!(svc.step(1).await.unwrap(), "one operation of three");
    assert!(matches!(
        svc.snap_numbers(&id).await.unwrap(),
        SnapAnswer::Building { .. }
    ));
    assert!(matches!(
        svc.reclaim(std::slice::from_ref(&id)).await.unwrap(),
        SnapAnswer::Building { .. }
    ));
    assert!(svc.stats().building.load(Ordering::Relaxed));
    svc.catch_up().await.unwrap();
    assert!(svc.snap_numbers(&id).await.unwrap().ready().is_some());
    assert_verified(&svc, "after the budgeted passes").await;
}

/// A full recompute of the live flags fetches the live spilled lists
/// under the pass's time budget, resuming with the lists not recorded
/// yet: every pass moves, no list is fetched twice, and the result is
/// the unbudgeted one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_recompute_is_budgeted_and_resumes() {
    let mut fx = Fixture::new();
    let a = fx.mkdir(ROOT_INO, "a");
    let lists = 6;
    for i in 0..lists {
        let tags: Vec<String> = (0..INLINE_MAX + 2).map(|j| format!("l{i}.{j}")).collect();
        fx.file(a, &format!("f{i}"), &tags, 50).await;
    }
    fx.snap("/a").await;
    fx.publish().await;
    let svc = fx.service(Fixture::config(SnapAcctMode::On));
    let mut passes = 0;
    while svc.step_for(Duration::ZERO).await.unwrap() {
        passes += 1;
        assert!(passes < 100, "a budgeted pass must make progress");
        if passes < lists {
            assert_eq!(
                svc.stats().full_refreshes.load(Ordering::Relaxed),
                0,
                "flags change only once every list is in"
            );
        }
    }
    assert!(passes >= lists - 1, "{passes} passes for {lists} lists");
    assert_eq!(svc.stats().full_refreshes.load(Ordering::Relaxed), 1);
    assert_verified(&svc, "after a budgeted recompute").await;
}
