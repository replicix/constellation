//! §0.2 — commit cost across write shapes, and the §P10b steady-state
//! question: does the bucket footprint plateau under retention + GC?

use std::collections::HashSet;
use std::time::{Duration, Instant};

use constellation_meta::LogRecord;
use rand::prelude::*;
use rand::rngs::SmallRng;
use serde::Serialize;

use crate::corpus::Corpus;
use crate::keys::{self, Attrs, KIND_FILE};
use crate::node::Hash;
use crate::stats::{bytes, Report};
use crate::store::{Store, ZSTD_LEVEL};
use crate::tree::{Mut, Tree};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// One directory at a time — rsync, `git checkout`, a build.
    Clustered,
    /// A build tree: a few dozen directories in flight.
    SemiClustered,
    /// `chmod` by random ino — the honest worst case.
    Scattered,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::Clustered => "clustered (one directory)",
            Shape::SemiClustered => "semi-clustered (build tree)",
            Shape::Scattered => "scattered (random-ino chmod)",
        }
    }
}

/// The plan 28 §P2 commit object: a tiny CAS'd pointer at a version.
#[derive(Serialize)]
struct Commit {
    seq: u64,
    parent: u64,
    roots: std::collections::BTreeMap<String, String>,
    packs: Vec<String>,
    author: u64,
    epoch: u64,
    agg: (u64, u64),
    intent: (String, u64),
    unix_ms: u64,
}

fn commit_object_bytes(seq: u64, root: &Hash, packs: usize, ops: u64) -> usize {
    let mut roots = std::collections::BTreeMap::new();
    roots.insert("0".to_string(), hex(root));
    let c = Commit {
        seq,
        parent: seq - 1,
        roots,
        packs: (0..packs)
            .map(|i| hex(&crate::corpus::fake_hash(seq, i as u64)))
            .collect(),
        author: 7,
        epoch: 19,
        agg: (1_402_331_136, 11_903_221),
        intent: ("batch".into(), ops),
        unix_ms: 0,
    };
    serde_json::to_vec(&c).unwrap().len()
}

fn hex(h: &Hash) -> String {
    h.iter().map(|b| format!("{b:02x}")).collect()
}

/// The same operations as they ship today: postcard `LogRecord`s in a
/// segment envelope, zstd level 3 (`store-s3::log`).
fn todays_segment_bytes(records: &[LogRecord]) -> (usize, usize) {
    #[derive(Serialize)]
    struct Envelope<'a> {
        v: u32,
        node: u64,
        epoch: u64,
        records: &'a [LogRecord],
    }
    let raw = postcard::to_allocvec(&Envelope {
        v: 2,
        node: 1,
        epoch: 1,
        records,
    })
    .unwrap();
    let z = zstd::encode_all(&raw[..], ZSTD_LEVEL).unwrap().len();
    (raw.len(), z)
}

/// `VmHWM` — what a node would actually need resident.
fn peak_rss() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok())
        })
        .map(|kb| kb * 1024)
        .unwrap_or(0)
}

fn attrs(size: u64, mtime: i64) -> Attrs {
    Attrs {
        kind: KIND_FILE,
        mode: 0o100644,
        uid: 1000,
        gid: 1000,
        nlink: 1,
        size,
        mtime_ns: mtime,
        ctime_ns: mtime,
        atime_ns: mtime,
    }
}

/// Build one batch of `ops` operations in the requested shape, returning
/// both the key mutations and the log records the same batch produces
/// today.
pub fn batch(
    corpus: &Corpus,
    shape: Shape,
    ops: usize,
    next_ino: &mut u64,
    rng: &mut SmallRng,
) -> (Vec<Mut>, Vec<LogRecord>) {
    let mut muts: Vec<Mut> = Vec::with_capacity(ops * 3);
    let mut recs: Vec<LogRecord> = Vec::with_capacity(ops);
    match shape {
        Shape::Clustered | Shape::SemiClustered => {
            let dirs = if shape == Shape::Clustered {
                1
            } else {
                (ops / 64).clamp(2, 64)
            };
            let parents: Vec<u64> = (0..dirs)
                .map(|_| {
                    let i = rng.random_range(0..corpus.recs.len());
                    corpus.recs[i].parent
                })
                .collect();
            for i in 0..ops {
                let parent = parents[i % parents.len()];
                let ino = *next_ino;
                *next_ino += 1;
                let name = format!("new{ino:012x}.o");
                let size = rng.random_range(0..64 * 1024);
                let a = attrs(size, 1_760_000_000_000_000_000 + i as i64);
                let chunks = [crate::corpus::fake_hash(ino, 0)];
                if keys::has_inode_record(a.kind) {
                    muts.push((
                        keys::inode_key(ino),
                        Some(keys::inode_val(&a, &chunks, &[])),
                    ));
                }
                muts.push((
                    keys::dentry_key(parent, name.as_bytes()),
                    Some(keys::dentry_val_with(ino, &a, &chunks, &[])),
                ));
                muts.push((
                    keys::rdentry_key(ino, parent, name.as_bytes()),
                    Some(Vec::new()),
                ));
                recs.push(LogRecord::Create {
                    parent,
                    name: name.clone(),
                    ino,
                    mode: 0o100644,
                    uid: 1000,
                    gid: 1000,
                    time_ns: a.mtime_ns,
                });
                recs.push(LogRecord::WriteManifest {
                    ino,
                    base_manifest: None,
                    manifest: crate::corpus::fake_hash(ino, 0).to_vec(),
                    size,
                    time_ns: a.mtime_ns,
                });
            }
        }
        Shape::Scattered => {
            for _ in 0..ops {
                let i = rng.random_range(0..corpus.recs.len());
                let ino = corpus.ino(i);
                let r = &corpus.recs[i];
                let mut a = corpus.attrs(r);
                a.mode = 0o100600;
                let chunks: Vec<[u8; 32]> = (0..r.nchunks)
                    .map(|c| crate::corpus::fake_hash(ino, c as u64))
                    .collect();
                if keys::has_inode_record(a.kind) {
                    muts.push((
                        keys::inode_key(ino),
                        Some(keys::inode_val(&a, &chunks, &[])),
                    ));
                }
                // §P6 as written: the dentry carries the attr copy
                // READDIRPLUS returns, so a chmod writes it too — which is
                // the second key S1 is pricing. `Enc::NoCopy` skips it;
                // `Enc::DentryAuth` writes only it.
                if keys::enc() != keys::Enc::NoCopy {
                    muts.push((
                        keys::dentry_key(r.parent, corpus.name(r)),
                        Some(keys::dentry_val_with(ino, &a, &chunks, &[])),
                    ));
                }
                recs.push(LogRecord::Setattr {
                    ino,
                    mode: Some(0o100600),
                    uid: None,
                    gid: None,
                    size: None,
                    atime_ns: None,
                    mtime_ns: None,
                    time_ns: a.mtime_ns,
                });
            }
        }
    }
    muts.sort_by(|a, b| a.0.cmp(&b.0));
    muts.dedup_by(|a, b| a.0 == b.0);
    (muts, recs)
}

pub fn measure_batch(
    store: &Store,
    t: &Tree<'_>,
    root: &Hash,
    retain: &[Hash],
    muts: &[Mut],
    recs: &[LogRecord],
    ops: usize,
) -> (u64, u64, u64, f64, f64, f64) {
    store.counters.reset();
    let start = Instant::now();
    let new_root = t.apply(root, muts);
    let cpu = start.elapsed().as_secs_f64();
    let o = std::sync::atomic::Ordering::Relaxed;
    let (nodes, nbytes, zbytes) = (
        store.counters.new_nodes.load(o),
        store.counters.new_bytes.load(o),
        store.counters.new_zbytes.load(o),
    );
    let packs = (zbytes as f64 / (1 << 20) as f64).ceil().max(1.0) as u64;
    let commit_b = commit_object_bytes(4211, &new_root, packs as usize, ops as u64);
    let total_z = zbytes + commit_b as u64;
    let (_, today_z) = todays_segment_bytes(recs);
    let bpo = total_z as f64 / ops as f64;
    let today_bpo = today_z as f64 / ops as f64;
    store.flush();
    let mut keep = retain.to_vec();
    keep.push(*root);
    let live: HashSet<Hash> = t.reachable(&keep);
    store.sweep_packs(&live);
    (nodes, nbytes, zbytes, bpo, today_bpo, cpu)
}

fn emit_row(
    rep: &mut Report,
    label: &str,
    ops: usize,
    keys: usize,
    nodes: u64,
    nbytes: u64,
    zbytes: u64,
    bpo: f64,
    today_bpo: f64,
    cpu: f64,
) {
    let packs = (zbytes as f64 / (1 << 20) as f64).ceil().max(1.0) as u64;
    rep.line(format!(
        "| {} | {} | {} | {} | {} | {} | {} | {:.0} | {:.0} | {:.2}× | {:.0} ms |",
        label,
        ops,
        keys,
        nodes,
        bytes(nbytes),
        bytes(zbytes),
        packs,
        bpo,
        today_bpo,
        bpo / today_bpo.max(1e-9),
        cpu * 1e3,
    ));
}

pub fn commit_cost(rep: &mut Report, store: &Store, root: &Hash, corpus: &Corpus) {
    let t = Tree::new(store);
    rep.head("### 0.2 Commit cost by write shape");
    rep.line("Every commit is applied to the same census-scale root, so the numbers are the marginal cost of a batch against a 23.8M-key filesystem.");
    rep.blank();
    rep.line("| Shape | ops | keys written | nodes written | bytes | zstd bytes | packs | B/op (zstd) | today's segment B/op | ratio | CPU |");
    rep.line("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    let mut rows: Vec<(Shape, usize, f64, f64)> = Vec::new();
    for shape in [Shape::Clustered, Shape::SemiClustered, Shape::Scattered] {
        for ops in [1_000usize, 10_000, 100_000] {
            let mut rng = SmallRng::seed_from_u64(0xc0ffee + ops as u64);
            let mut next_ino = corpus.len() as u64 + 1_000_000;
            let (muts, recs) = batch(corpus, shape, ops, &mut next_ino, &mut rng);
            let (nodes, nbytes, zbytes, bpo, today_bpo, cpu) =
                measure_batch(store, &t, root, &[], &muts, &recs, ops);
            rows.push((shape, ops, bpo, today_bpo));
            emit_row(
                rep,
                shape.name(),
                ops,
                muts.len(),
                nodes,
                nbytes,
                zbytes,
                bpo,
                today_bpo,
                cpu,
            );
        }
    }
    let clustered_ok = rows
        .iter()
        .filter(|(s, _, _, _)| *s == Shape::Clustered)
        .all(|(_, _, bpo, today)| *bpo <= 2.0 * *today);
    rep.blank();
    rep.line(format!(
        "**Gate 0.2a: clustered ≤ 2× today's log bytes per op → {}.** Scattered is documented, not gated.",
        if clustered_ok { "PASS" } else { "FAIL" }
    ));

    aged_commit_cost(rep, store, root, corpus);
}

/// A live `(ino, parent, name)` after the aging generations — enough to
/// build directory-local setattr batches whose inos have decorrelated
/// from directory order.
pub struct LiveFile {
    pub ino: u64,
    pub parent: u64,
    pub name: String,
    pub size: u64,
    pub mtime: i64,
    pub nchunks: u8,
}

/// Age the tree the way a real filesystem ages: create, unlink, and
/// rename across generations so a directory's children's inos are no
/// longer a contiguous run in the `0x01` keyspace. That is the property
/// the fresh-tree clustered numbers silently rely on (`alloc_ino` is
/// `counter++` during a one-pass import).
pub fn age_tree(
    store: &Store,
    t: &Tree<'_>,
    root: &Hash,
    corpus: &Corpus,
    generations: usize,
    ops_per_gen: usize,
) -> (Hash, Vec<LiveFile>, u64, f64) {
    let mut rng = SmallRng::seed_from_u64(0xa9e);
    let mut next_ino = corpus.len() as u64 + 50_000_000;
    // Seed from the corpus: every file is live under its birth parent.
    let mut live: Vec<LiveFile> = (0..corpus.recs.len())
        .filter(|&i| corpus.recs[i].kind == KIND_FILE)
        .map(|i| {
            let r = &corpus.recs[i];
            LiveFile {
                ino: corpus.ino(i),
                parent: r.parent,
                name: String::from_utf8_lossy(corpus.name(r)).into_owned(),
                size: r.size,
                mtime: r.mtime_ns,
                nchunks: r.nchunks,
            }
        })
        .collect();
    // The directories aging is churned around. §14.8's aged corpus
    // understated ino-scatter because the tracked set was a uniform 2M
    // sample of 10.8M files: a 10k-child directory kept ~1.7k tracked
    // children, so the "densest directory" the setattr batch could find
    // after aging was six times smaller than the fresh one and the
    // comparison changed two things at once. Keep every child of the
    // widest directories, and churn exactly those directories each
    // generation, so the aged densest directory is as wide as the fresh
    // one and the only thing that moved is where its children's inos are.
    let hot: Vec<u64> = {
        let mut h = corpus.big_dirs.clone();
        h.sort_unstable();
        h.dedup();
        h.truncate(8);
        h
    };
    let hot_set: std::collections::HashSet<u64> = hot.iter().copied().collect();
    // Cap the tracked set so rename/unlink picks stay O(1); the tree
    // still holds the full census, we just age a working subset hard.
    if live.len() > 2_000_000 {
        let (mut keep, mut rest): (Vec<LiveFile>, Vec<LiveFile>) =
            live.into_iter().partition(|f| hot_set.contains(&f.parent));
        rest.shuffle(&mut rng);
        rest.truncate(2_000_000usize.saturating_sub(keep.len()));
        keep.append(&mut rest);
        live = keep;
    }
    let parents: Vec<u64> = {
        let mut p: Vec<u64> = corpus.recs.iter().map(|r| r.parent).collect();
        p.sort_unstable();
        p.dedup();
        p
    };
    let original = *root;
    let mut root = *root;
    let t0 = Instant::now();
    // One create, as the aging generations and the churn both write it.
    let create = |muts: &mut Vec<Mut>, ino: u64, parent: u64, name: &str, a: &Attrs| {
        let chunks = [crate::corpus::fake_hash(ino, 0)];
        if keys::has_inode_record(a.kind) {
            muts.push((keys::inode_key(ino), Some(keys::inode_val(a, &chunks, &[]))));
        }
        muts.push((
            keys::dentry_key(parent, name.as_bytes()),
            Some(keys::dentry_val_with(ino, a, &chunks, &[])),
        ));
        muts.push((
            keys::rdentry_key(ino, parent, name.as_bytes()),
            Some(Vec::new()),
        ));
    };
    let unlink = |muts: &mut Vec<Mut>, f: &LiveFile| {
        if keys::has_inode_record(KIND_FILE) {
            muts.push((keys::inode_key(f.ino), None));
        }
        muts.push((keys::dentry_key(f.parent, f.name.as_bytes()), None));
        muts.push((keys::rdentry_key(f.ino, f.parent, f.name.as_bytes()), None));
    };

    for g in 0..generations {
        let mut muts: Vec<Mut> = Vec::with_capacity(ops_per_gen * 4);
        let n = ops_per_gen / 3;
        // Every create this generation makes, before any ino is handed
        // out. Two sources: ordinary creates into random directories, and
        // the churn — the widest directories replace a quarter of their
        // children, keeping the child count where it started, the way a
        // build output directory or a Maildir does every day. The list is
        // shuffled before allocation because a real `alloc_ino` interleaves
        // every writer's creates, and it is that interleaving, not the
        // creates themselves, that puts a dense directory's `0x01` records
        // in a different part of the keyspace from its contiguous `0x02`
        // run.
        let mut pending: Vec<u64> = (0..n)
            .map(|_| parents[rng.random_range(0..parents.len())])
            .collect();
        {
            let mut by_hot: std::collections::HashMap<u64, Vec<usize>> =
                std::collections::HashMap::new();
            for (i, f) in live.iter().enumerate() {
                if hot_set.contains(&f.parent) {
                    by_hot.entry(f.parent).or_default().push(i);
                }
            }
            let mut doomed: Vec<usize> = Vec::new();
            for (&d, idxs) in by_hot.iter() {
                let mut pick = idxs.clone();
                pick.shuffle(&mut rng);
                pick.truncate(idxs.len() / 4);
                for &i in &pick {
                    unlink(&mut muts, &live[i]);
                    pending.push(d);
                }
                doomed.extend(pick);
            }
            doomed.sort_unstable_by(|a, b| b.cmp(a));
            for i in doomed {
                live.swap_remove(i);
            }
        }
        pending.shuffle(&mut rng);
        for (i, parent) in pending.into_iter().enumerate() {
            let ino = next_ino;
            next_ino += 1;
            let name = format!("age{g:02}-{ino:x}");
            let size = rng.random_range(0..64 * 1024);
            let a = attrs(size, 1_760_000_000_000_000_000 + (g * ops_per_gen + i) as i64);
            create(&mut muts, ino, parent, &name, &a);
            live.push(LiveFile {
                ino,
                parent,
                name,
                size,
                mtime: a.mtime_ns,
                nchunks: 1,
            });
        }
        // Renames: move a file to a different parent — the ino stays,
        // the directory membership changes. This is what decorrelates
        // `0x01` order from `0x02` order.
        for _ in 0..n {
            if live.is_empty() {
                break;
            }
            let i = rng.random_range(0..live.len());
            let f = &live[i];
            let new_parent = parents[rng.random_range(0..parents.len())];
            if new_parent == f.parent {
                continue;
            }
            let new_name = format!("mv-{ino:x}", ino = f.ino);
            let a = attrs(f.size, f.mtime);
            let chunks: Vec<[u8; 32]> = (0..f.nchunks)
                .map(|c| crate::corpus::fake_hash(f.ino, c as u64))
                .collect();
            muts.push((keys::dentry_key(f.parent, f.name.as_bytes()), None));
            muts.push((keys::rdentry_key(f.ino, f.parent, f.name.as_bytes()), None));
            muts.push((
                keys::dentry_key(new_parent, new_name.as_bytes()),
                Some(keys::dentry_val_with(f.ino, &a, &chunks, &[])),
            ));
            muts.push((
                keys::rdentry_key(f.ino, new_parent, new_name.as_bytes()),
                Some(Vec::new()),
            ));
            live[i].parent = new_parent;
            live[i].name = new_name;
        }
        // Unlinks: free slots so the live set does not grow forever.
        for _ in 0..n {
            if live.len() < 10_000 {
                break;
            }
            let i = rng.random_range(0..live.len());
            let f = live.swap_remove(i);
            unlink(&mut muts, &f);
        }
        muts.sort_by(|a, b| a.0.cmp(&b.0));
        muts.dedup_by(|a, b| a.0 == b.0);
        root = t.apply(&root, &muts);
        // Keep the original root alive so the fresh/aged comparison can
        // measure against both trees in the same store.
        if g % 2 == 1 {
            store.flush();
            let live_h: HashSet<Hash> = t.reachable(&[original, root]);
            store.sweep_packs(&live_h);
        }
    }
    (root, live, next_ino, t0.elapsed().as_secs_f64())
}

/// The widest directory in the tracked set, and its children's indices.
pub fn densest_dir(files: &[LiveFile]) -> (u64, Vec<usize>) {
    let mut by_parent: std::collections::HashMap<u64, Vec<usize>> =
        std::collections::HashMap::new();
    for (i, f) in files.iter().enumerate() {
        by_parent.entry(f.parent).or_default().push(i);
    }
    by_parent
        .into_iter()
        .max_by_key(|(_, v)| v.len())
        .unwrap_or((1, Vec::new()))
}

/// How far a directory's children have walked from a contiguous run in
/// the `0x01` keyspace: children, and the number of distinct 1024-ino
/// buckets they fall into. On a fresh one-pass import a burst-filled
/// directory's children are consecutive inos, so the second number is the
/// first divided by 1024; the more it exceeds that, the more `0x01` leaves
/// a directory-local setattr has to touch.
pub fn ino_scatter(files: &[LiveFile], idxs: &[usize]) -> (usize, usize) {
    let mut buckets: Vec<u64> = idxs.iter().map(|&i| files[i].ino / 1024).collect();
    buckets.sort_unstable();
    buckets.dedup();
    (idxs.len(), buckets.len())
}

/// Directory-local setattr: the write shape whose cost the plan says
/// drifts toward scattered as the tree ages (dentry leaves stay
/// clustered; inode leaves do not).
pub fn clustered_touch_batch(
    files: &[LiveFile],
    ops: usize,
    rng: &mut SmallRng,
) -> (Vec<Mut>, Vec<LogRecord>) {
    let (_parent, idxs) = densest_dir(files);
    if idxs.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let mut muts = Vec::with_capacity(ops * 2);
    let mut recs = Vec::with_capacity(ops);
    for _ in 0..ops {
        let f = &files[idxs[rng.random_range(0..idxs.len())]];
        let mut a = attrs(f.size, f.mtime);
        a.mode = 0o100600;
        let chunks: Vec<[u8; 32]> = (0..f.nchunks)
            .map(|c| crate::corpus::fake_hash(f.ino, c as u64))
            .collect();
        if keys::has_inode_record(a.kind) {
            muts.push((
                keys::inode_key(f.ino),
                Some(keys::inode_val(&a, &chunks, &[])),
            ));
        }
        if keys::enc() != keys::Enc::NoCopy {
            muts.push((
                keys::dentry_key(f.parent, f.name.as_bytes()),
                Some(keys::dentry_val_with(f.ino, &a, &chunks, &[])),
            ));
        }
        recs.push(LogRecord::Setattr {
            ino: f.ino,
            mode: Some(0o100600),
            uid: None,
            gid: None,
            size: None,
            atime_ns: None,
            mtime_ns: None,
            time_ns: a.mtime_ns,
        });
    }
    muts.sort_by(|a, b| a.0.cmp(&b.0));
    muts.dedup_by(|a, b| a.0 == b.0);
    (muts, recs)
}

pub fn fresh_touch_files(corpus: &Corpus) -> Vec<LiveFile> {
    (0..corpus.recs.len())
        .filter(|&i| corpus.recs[i].kind == KIND_FILE)
        .map(|i| {
            let r = &corpus.recs[i];
            LiveFile {
                ino: corpus.ino(i),
                parent: r.parent,
                name: String::from_utf8_lossy(corpus.name(r)).into_owned(),
                size: r.size,
                mtime: r.mtime_ns,
                nchunks: r.nchunks,
            }
        })
        .collect()
}

fn aged_commit_cost(rep: &mut Report, store: &Store, root: &Hash, corpus: &Corpus) {
    let t = Tree::new(store);
    rep.blank();
    rep.head("### 0.2 aged tree — create/unlink/rename generations");
    rep.line(
        "Fresh import keeps `0x01` order correlated with directory order (`alloc_ino` is `counter++`). After years of create/unlink/rename the dentry leaves of a directory stay clustered while its inode leaves scatter. Age the corpus with 10 generations, then re-measure both create-shaped clustered commits and directory-local setattr (the shape the aging gate is about).",
    );

    let (aged_root, aged_live, mut next_ino, age_secs) =
        age_tree(store, &t, root, corpus, 10, 20_000);
    let fresh_files = fresh_touch_files(corpus);
    let (_, fresh_idxs) = densest_dir(&fresh_files);
    let (fresh_n, fresh_b) = ino_scatter(&fresh_files, &fresh_idxs);
    let (_, aged_idxs) = densest_dir(&aged_live);
    let (aged_n, aged_b) = ino_scatter(&aged_live, &aged_idxs);
    rep.line(format!(
        "Aged in {:.1} s (10 × 20k create/rename/unlink ops plus a quarter-of-children churn over the 8 widest directories); tracked live files after aging: {}.",
        age_secs,
        aged_live.len()
    ));
    rep.line(format!(
        "Densest tracked directory: fresh {fresh_n} children over {fresh_b} distinct 1024-ino buckets, aged {aged_n} children over {aged_b}. The churn is what keeps the aged directory as wide as the fresh one, so the only thing the comparison moves is where its children's `0x01` records live."
    ));
    rep.blank();
    rep.line("| Tree | Shape | ops | keys written | nodes written | bytes | zstd bytes | packs | B/op (zstd) | today's segment B/op | ratio | CPU |");
    rep.line("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");

    let mut aged_touch_ratios: Vec<(f64, f64)> = Vec::new();
    let mut aged_create_ratios: Vec<(f64, f64)> = Vec::new();

    let retain = [*root, aged_root];
    for (tree_label, base, files) in [
        ("fresh", root, &fresh_files[..]),
        ("aged", &aged_root, &aged_live[..]),
    ] {
        for ops in [1_000usize, 10_000, 100_000] {
            {
                let mut rng = SmallRng::seed_from_u64(0xc0ffee + ops as u64 + next_ino);
                let (muts, recs) = batch(corpus, Shape::Clustered, ops, &mut next_ino, &mut rng);
                let (nodes, nbytes, zbytes, bpo, today_bpo, cpu) =
                    measure_batch(store, &t, base, &retain, &muts, &recs, ops);
                let packs = (zbytes as f64 / (1 << 20) as f64).ceil().max(1.0) as u64;
                rep.line(format!(
                    "| {} | clustered create | {} | {} | {} | {} | {} | {} | {:.0} | {:.0} | {:.2}× | {:.0} ms |",
                    tree_label,
                    ops,
                    muts.len(),
                    nodes,
                    bytes(nbytes),
                    bytes(zbytes),
                    packs,
                    bpo,
                    today_bpo,
                    bpo / today_bpo.max(1e-9),
                    cpu * 1e3,
                ));
                if tree_label == "aged" {
                    aged_create_ratios.push((bpo, today_bpo));
                }
            }
            {
                let mut rng = SmallRng::seed_from_u64(0x70c4 + ops as u64);
                let (muts, recs) = clustered_touch_batch(files, ops, &mut rng);
                let real_ops = recs.len().max(1);
                let (nodes, nbytes, zbytes, bpo, today_bpo, cpu) =
                    measure_batch(store, &t, base, &retain, &muts, &recs, real_ops);
                let packs = (zbytes as f64 / (1 << 20) as f64).ceil().max(1.0) as u64;
                rep.line(format!(
                    "| {} | clustered touch (one dir setattr) | {} | {} | {} | {} | {} | {} | {:.0} | {:.0} | {:.2}× | {:.0} ms |",
                    tree_label,
                    real_ops,
                    muts.len(),
                    nodes,
                    bytes(nbytes),
                    bytes(zbytes),
                    packs,
                    bpo,
                    today_bpo,
                    bpo / today_bpo.max(1e-9),
                    cpu * 1e3,
                ));
                if tree_label == "aged" {
                    aged_touch_ratios.push((bpo, today_bpo));
                }
            }
        }
    }

    // Gate: aged clustered (directory-local setattr — the shape that
    // actually drifts) ≤ 4× today's log bytes. Also report create, which
    // stays cheap because new inos are still sequential.
    let touch_ok = aged_touch_ratios
        .iter()
        .all(|(bpo, today)| *bpo <= 4.0 * *today);
    let create_ok = aged_create_ratios
        .iter()
        .all(|(bpo, today)| *bpo <= 4.0 * *today);
    let worst_touch = aged_touch_ratios
        .iter()
        .map(|(b, t)| b / t.max(1e-9))
        .fold(0.0_f64, f64::max);
    let worst_create = aged_create_ratios
        .iter()
        .map(|(b, t)| b / t.max(1e-9))
        .fold(0.0_f64, f64::max);
    rep.blank();
    rep.line(format!(
        "**Gate 0.2 aged: clustered ≤ 4× today's log bytes per op → {} for directory-local setattr (worst {:.2}×), {} for create (worst {:.2}×).** The gate is about setattr: creates keep sequential `alloc_ino` regardless of age.",
        if touch_ok { "PASS" } else { "FAIL" },
        worst_touch,
        if create_ok { "PASS" } else { "FAIL" },
        worst_create,
    ));
}

pub struct Sustained {
    pub minutes: f64,
    pub commit_ops: usize,
    pub retention: usize,
    pub gc_every: usize,
    /// Share of commits that are random-ino `chmod` rather than
    /// directory-local work. Real workloads are overwhelmingly clustered;
    /// this knob prices how much a scattered minority costs.
    pub scattered_pct: u64,
    /// Where the packs live. The steady-state run keeps leaves on disk
    /// and only the interior in RAM — the residency a real node has —
    /// because holding a second census-scale tree in memory measures the
    /// benchmark rather than the design.
    pub pack_dir: std::path::PathBuf,
    pub leaf_cache_bytes: usize,
}

/// The §P10b question. A steady-state writer (creates balanced by
/// deletes, plus scattered setattrs) commits flat out against a retention
/// window; GC marks from the retained roots and sweeps whole-dead packs.
pub fn sustained(rep: &mut Report, corpus: &Corpus, cfg: &Sustained) {
    let _ = std::fs::remove_dir_all(&cfg.pack_dir);
    let store = Store::packed(cfg.pack_dir.clone(), 1 << 20, true, cfg.leaf_cache_bytes);
    let t = Tree::new(&store);
    let build0 = Instant::now();
    let mut root = t.build_sorted(corpus.all_entries());
    let base_pack_bytes = store.pack_bytes_total();
    rep.head("### 0.2b Steady state under retention (P10b)");
    rep.line(format!(
        "Base tree: {} of packs ({} packs) built in {:.0} s. Retention window {} commits, GC every {} commits, {} ops/commit, wall clock {:.0} min.",
        bytes(base_pack_bytes),
        store.pack_count(),
        build0.elapsed().as_secs_f64(),
        cfg.retention,
        cfg.gc_every,
        cfg.commit_ops,
        cfg.minutes
    ));
    rep.line(format!(
        "Leaves live in packs on disk behind a {} LRU; only the {} of interior nodes is resident, which is the residency a real node has.",
        bytes(cfg.leaf_cache_bytes as u64),
        bytes(store.resident_bytes())
    ));
    rep.line(format!(
        "Commit mix: {}% scattered random-ino chmod, 25% semi-clustered (build tree), the rest clustered (one directory); creations are unlinked {} commits later so the live set stays at census size.",
        cfg.scattered_pct, cfg.retention
    ));

    let mut rng = SmallRng::seed_from_u64(0x51ead1);
    let mut next_ino = corpus.len() as u64 + 10_000_000;
    let mut roots: Vec<Hash> = vec![root];
    // Names created per commit, so later commits can delete them and the
    // live set stays at census size.
    let mut created: std::collections::VecDeque<Vec<(u64, u64, String)>> =
        std::collections::VecDeque::new();
    let mut commits = 0u64;
    let mut ops_total = 0u64;
    let mut superseded_bytes = 0u64;
    let mut commit_bytes = 0u64;
    let deadline = Instant::now() + Duration::from_secs_f64(cfg.minutes * 60.0);
    let start = Instant::now();
    let mut samples: Vec<(f64, u64, u64, u64)> = Vec::new();
    let mut last_gc_live = 0u64;
    let mut gc_time = Duration::ZERO;
    let mut whole_dead_total = 0usize;
    let mut partial_total = 0usize;
    let mut compacted_packs = 0usize;
    let mut compacted_bytes = 0u64;

    while Instant::now() < deadline {
        let shape = match commits % 100 {
            n if n < cfg.scattered_pct => Shape::Scattered,
            n if n < cfg.scattered_pct + 25 => Shape::SemiClustered,
            _ => Shape::Clustered,
        };
        let (mut muts, _) = batch(corpus, shape, cfg.commit_ops, &mut next_ino, &mut rng);
        let mut mine: Vec<(u64, u64, String)> = Vec::new();
        if shape != Shape::Scattered {
            // Recover (ino, parent, name) from the dentry keys just built,
            // so a later commit can unlink them and the live set stays at
            // census size — steady state is the thing being measured.
            for (k, v) in muts.iter() {
                if k[0] == keys::K_DENTRY {
                    let parent = u64::from_be_bytes(k[1..9].try_into().unwrap());
                    let name = String::from_utf8_lossy(&k[9..]).to_string();
                    mine.push((keys::dentry_ino(v.as_ref().unwrap()), parent, name));
                }
            }
        }
        // Retire the creations from `retention` commits ago.
        if created.len() >= cfg.retention {
            if let Some(old) = created.pop_front() {
                for (ino, parent, name) in old {
                    muts.push((keys::inode_key(ino), None));
                    muts.push((keys::dentry_key(parent, name.as_bytes()), None));
                    muts.push((keys::rdentry_key(ino, parent, name.as_bytes()), None));
                }
            }
        }
        created.push_back(mine);
        muts.sort_by(|a, b| a.0.cmp(&b.0));
        muts.dedup_by(|a, b| a.0 == b.0);

        store.counters.reset();
        root = t.apply(&root, &muts);
        let o = std::sync::atomic::Ordering::Relaxed;
        superseded_bytes += store.counters.new_zbytes.load(o);
        commit_bytes += commit_object_bytes(commits + 1, &root, 1, cfg.commit_ops as u64) as u64;
        roots.push(root);
        commits += 1;
        ops_total += cfg.commit_ops as u64;

        if roots.len() > cfg.retention {
            let excess = roots.len() - cfg.retention;
            roots.drain(0..excess);
        }
        if commits.is_multiple_of(cfg.gc_every as u64) {
            let g0 = Instant::now();
            let live: HashSet<Hash> = t.reachable(&roots);
            let (whole, partial, live_b, _dead_b) = store.pack_liveness(&live);
            let (_swept, _freed) = store.sweep_packs(&live);
            // Compaction is not optional here: P10b's plateau claim is a
            // claim about a system that has a compactor, and the point of
            // this run is to price it.
            let (cp, crw, _cfreed) = store.compact(&live, 0.5, 1 << 30);
            compacted_packs += cp;
            compacted_bytes += crw;
            gc_time += g0.elapsed();
            whole_dead_total += whole;
            partial_total += partial;
            last_gc_live = live_b;
            samples.push((
                start.elapsed().as_secs_f64(),
                commits,
                store.pack_bytes_total(),
                live_b,
            ));
        }
    }
    let wall = start.elapsed().as_secs_f64();

    rep.blank();
    rep.line("| t (s) | commits | ops | bucket bytes (packs) | live bytes | dead-but-unswept |");
    rep.line("|---|---:|---:|---:|---:|---:|");
    for (t_s, c, total, live) in samples.iter().step_by((samples.len() / 12).max(1)) {
        rep.line(format!(
            "| {:.0} | {} | {} | {} | {} | {} |",
            t_s,
            c,
            c * cfg.commit_ops as u64,
            bytes(*total),
            bytes(*live),
            bytes(total.saturating_sub(*live))
        ));
    }
    if let Some(last) = samples.last() {
        rep.line(format!(
            "| {:.0} | {} | {} | {} | {} | {} |",
            last.0,
            last.1,
            last.1 * cfg.commit_ops as u64,
            bytes(last.2),
            bytes(last.3),
            bytes(last.2.saturating_sub(last.3))
        ));
    }

    let per_day = superseded_bytes as f64 / wall * 86_400.0;
    let p10b_rate = superseded_bytes as f64 / commits as f64 * (86_400.0 / 5.0);
    // Retention makes the footprint a sawtooth: a generation of retained
    // roots keeps its nodes alive until the window slides past them and
    // the compactor catches up. Compare half-run means, not endpoints.
    let half = samples.len() / 2;
    let mean = |s: &[(f64, u64, u64, u64)]| {
        s.iter().map(|x| x.2 as f64).sum::<f64>() / s.len().max(1) as f64
    };
    let plateau = if samples.len() >= 4 {
        (mean(&samples[half..]) - mean(&samples[..half])) / mean(&samples[..half])
    } else {
        f64::NAN
    };
    rep.blank();
    rep.line(format!(
        "- {} commits × {} ops in {:.0} s = **{:.0} commits/s**, {:.0} k ops/s (single-threaded, no S3 round trip).",
        commits,
        cfg.commit_ops,
        wall,
        commits as f64 / wall,
        ops_total as f64 / wall / 1e3
    ));
    rep.line(format!(
        "- Superseded (rewritten) node bytes: **{}** total, {} per commit; **{}/day** at this machine's flat-out rate and **{}/day** at P10b's one-commit-per-5 s assumption (plan says ~7.4 GiB/day).",
        bytes(superseded_bytes),
        bytes(superseded_bytes / commits.max(1)),
        bytes(per_day as u64),
        bytes(p10b_rate as u64)
    ));
    rep.line(format!(
        "- Commit objects: {} total ({} B each) — retention is a policy knob, not a correctness one.",
        bytes(commit_bytes),
        commit_bytes / commits.max(1)
    ));
    rep.line(format!(
        "- Packs at GC time: **{:.1}% died whole** (deletable at zero rewrite cost), {:.1}% needed compaction.",
        100.0 * whole_dead_total as f64 / (whole_dead_total + partial_total).max(1) as f64,
        100.0 * partial_total as f64 / (whole_dead_total + partial_total).max(1) as f64,
    ));
    rep.line(format!(
        "- Compactor: {} packs rewritten, {} of live nodes moved = **{}/s** of rewrite bandwidth, {:.0}% of the {}/s the commits themselves write.",
        compacted_packs,
        bytes(compacted_bytes),
        bytes((compacted_bytes as f64 / wall) as u64),
        100.0 * compacted_bytes as f64 / superseded_bytes.max(1) as f64,
        bytes((superseded_bytes as f64 / wall) as u64),
    ));
    rep.line(format!(
        "- GC + compaction cost: {:.0} s of {:.0} s wall ({} marks over the retained window).",
        gc_time.as_secs_f64(),
        wall,
        commits / cfg.gc_every as u64
    ));
    rep.line(format!(
        "- Footprint: mean {} over the first half of the run, {} over the second — drift **{:+.1}%** (live {} at the last GC). The sawtooth is the retention window: a generation of retained roots holds its nodes until the window slides past them.",
        bytes(mean(&samples[..half]) as u64),
        bytes(mean(&samples[half..]) as u64),
        plateau * 100.0,
        bytes(last_gc_live)
    ));
    rep.line(format!(
        "- Peak RSS of the writer: **{}** — {} of that is the resident interior; leaves were served from packs on disk.",
        bytes(peak_rss()),
        bytes(store.resident_bytes())
    ));
    rep.line(format!(
        "**Gate 0.2b: steady-state footprint plateaus → {}.**",
        if plateau.is_finite() && plateau < 0.05 {
            "PASS"
        } else {
            "FAIL"
        }
    ));

    reaper(rep, &store, &t, root, corpus, next_ino);
}

/// §P9's unlink-then-reap: removing a subtree is one key write, and the
/// descendants become garbage a rate-budgeted reaper collects.
fn reaper(
    rep: &mut Report,
    store: &Store,
    t: &Tree<'_>,
    root: Hash,
    corpus: &Corpus,
    mut next_ino: u64,
) {
    // Build a ~1M-key subtree: 333k files in one directory.
    let victim_parent = corpus.recs[corpus.recs.len() / 2].parent;
    let victim_ino = next_ino;
    next_ino += 1;
    let a = attrs(4096, 1_770_000_000_000_000_000);
    let mut muts: Vec<Mut> = vec![
        (
            keys::inode_key(victim_ino),
            Some(keys::inode_val(&a, &[], &[])),
        ),
        (
            keys::dentry_key(victim_parent, b"victim"),
            Some(keys::dentry_val(victim_ino, &a)),
        ),
        (
            keys::rdentry_key(victim_ino, victim_parent, b"victim"),
            Some(Vec::new()),
        ),
    ];
    let files = 333_333usize;
    for i in 0..files {
        let ino = next_ino + i as u64;
        let name = format!("v{i:08}");
        if keys::has_inode_record(KIND_FILE) {
            muts.push((
                keys::inode_key(ino),
                Some(keys::inode_val(&attrs(1024, a.mtime_ns), &[], &[])),
            ));
        }
        muts.push((
            keys::dentry_key(victim_ino, name.as_bytes()),
            Some(keys::dentry_val(ino, &attrs(1024, a.mtime_ns))),
        ));
        muts.push((
            keys::rdentry_key(ino, victim_ino, name.as_bytes()),
            Some(Vec::new()),
        ));
    }
    muts.sort_by(|a, b| a.0.cmp(&b.0));
    let total_keys = muts.len();
    let root = t.apply(&root, &muts);

    // The unlink itself: one dentry key (plus its reverse index).
    store.counters.reset();
    let start = Instant::now();
    let unlinked = t.apply(&root, &{
        let mut m: Vec<Mut> = vec![
            (keys::dentry_key(victim_parent, b"victim"), None),
            (
                keys::rdentry_key(victim_ino, victim_parent, b"victim"),
                None,
            ),
        ];
        m.sort();
        m
    });
    let unlink_s = start.elapsed().as_secs_f64();
    let o = std::sync::atomic::Ordering::Relaxed;
    let unlink_nodes = store.counters.new_nodes.load(o);

    // Reap: enumerate the now-unreachable keys and delete them in
    // rate-budgeted batches with a restartable cursor (the key itself).
    let start = Instant::now();
    let mut reaped = 0usize;
    let mut cur = unlinked;
    loop {
        let batch: Vec<Vec<u8>> = t.collect_prefix(&cur, &keys::dentry_prefix(victim_ino), 20_000);
        if batch.is_empty() {
            break;
        }
        let mut m: Vec<Mut> = Vec::with_capacity(batch.len() * 3);
        for k in &batch {
            let ino = keys::dentry_ino(&t.get(&cur, k).unwrap());
            let name = &k[9..];
            if keys::has_inode_record(KIND_FILE) {
                m.push((keys::inode_key(ino), None));
            }
            m.push((keys::rdentry_key(ino, victim_ino, name), None));
            m.push((k.clone(), None));
        }
        m.sort_by(|a, b| a.0.cmp(&b.0));
        reaped += m.len();
        cur = t.apply(&cur, &m);
    }
    let reap_s = start.elapsed().as_secs_f64();

    rep.blank();
    rep.line(format!(
        "- Unlink of a {}-key subtree: {} key writes, {} nodes, {:.2} ms — O(1) in the commit (§P9).",
        total_keys, 2, unlink_nodes, unlink_s * 1e3
    ));
    rep.line(format!(
        "- Reaper: {} unreachable keys removed in {:.1} s = **{:.0} k keys/s** single-threaded, in restartable 20k-key batches.",
        reaped,
        reap_s,
        reaped as f64 / reap_s / 1e3
    ));
}
