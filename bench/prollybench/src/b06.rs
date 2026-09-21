//! S1 — settling the dentry attr copy.
//!
//! §P6 keeps a denormalized copy of the inode attributes inside the `0x02`
//! dentry value so READDIRPLUS is a pure sequential scan. §14.2 confirmed
//! the payoff (a cold `ls -la` touches one distinct pack) and §14.4/§14.8
//! confirmed the price (every `setattr` writes two keys in two distant
//! ranges). The alternative was never measured, so this module measures
//! all three candidate shapes — [`Enc::Copy`], [`Enc::NoCopy`], and the
//! plan's own escape hatch [`Enc::DentryAuth`] — through the same code
//! paths, against the same corpus, on a fresh and an aged tree.
//!
//! The variants are measured one at a time and the tables are emitted at
//! the end: three census-scale trees do not fit in memory together, and
//! nothing here needs them to.

use std::sync::atomic::Ordering;
use std::time::Instant;

use rand::rngs::SmallRng;
use rand::SeedableRng;
use rayon::prelude::*;

use crate::b02::{self, Shape};
use crate::corpus::Corpus;
use crate::keys::{self, Enc};
use crate::node::Hash;
use crate::stats::{bytes, Report};
use crate::store::Store;
use crate::tree::Tree;

pub const VARIANTS: [Enc; 3] = [Enc::Copy, Enc::NoCopy, Enc::DentryAuth];

/// Threads for the concurrent read rows. §14.9 found the tier-(b) miss
/// path scales to 6.4× at 8, which is the asymmetry S1 has to weigh.
const READ_THREADS: usize = 8;

pub struct Cfg {
    pub pack_dir: std::path::PathBuf,
    /// Directories the `ls -la` rows scan.
    pub dirs: usize,
    pub age_generations: usize,
    pub age_ops: usize,
}

struct Structure {
    keys: u64,
    levels: usize,
    leaf_nodes: u64,
    leaf_bytes: u64,
    zleaf_bytes: u64,
    interior_bytes: u64,
}

struct Setattr {
    tree: &'static str,
    shape: &'static str,
    ops: usize,
    keys: usize,
    nodes: u64,
    zbytes: u64,
    bpo: f64,
    today: f64,
}

struct Ls {
    tree: &'static str,
    tier: &'static str,
    dirents: f64,
    packs: f64,
    reads: f64,
    ms_1: f64,
    ms_8: f64,
}

struct Getattr {
    tier: &'static str,
    per_s_1: f64,
    per_s_8: f64,
    reads: f64,
}

struct Out {
    enc: Enc,
    structure: Structure,
    setattr: Vec<Setattr>,
    ls: Vec<Ls>,
    getattr: Vec<Getattr>,
    dense_fresh: (usize, usize),
    dense_aged: (usize, usize),
    age_secs: f64,
}

/// `getattr(ino)` in the current encoding, returning the value and the
/// offset the attrs start at. Under `Enc::DentryAuth` a `nlink == 1`
/// entry has no inode record, so the read is a `0x04` probe for its one
/// name followed by a point read of its dentry: two leaves in two distant
/// ranges where the other shapes read one. Directories keep their `0x01`
/// record and are read directly — the caller knows which it is because
/// `alloc_ino` is ours to shape, so the branch costs nothing.
fn getattr(t: &Tree<'_>, root: &Hash, ino: u64, kind: u8) -> Option<(Vec<u8>, usize)> {
    if keys::has_inode_record(kind) {
        return t.get(root, &keys::inode_key(ino)).map(|v| (v, 0));
    }
    let rk = t.collect_prefix(root, &keys::rdentry_prefix(ino), 1);
    let rk = rk.first()?;
    let parent = u64::from_be_bytes(rk[9..17].try_into().ok()?);
    t.get(root, &keys::dentry_key(parent, &rk[17..]))
        .map(|v| (v, 8))
}

/// One `ls -la`: the `0x02` range scan, plus — only where the encoding
/// forces it — a point read per child into `0x01`.
fn ls_la(t: &Tree<'_>, root: &Hash, dir: u64) -> usize {
    let ents = t.collect_entries_prefix(root, &keys::dentry_prefix(dir), usize::MAX);
    if keys::enc() == Enc::NoCopy {
        for (_, v) in &ents {
            std::hint::black_box(t.get(root, &keys::inode_key(keys::dentry_ino(v))));
        }
    }
    ents.len()
}

fn pool(threads: usize) -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .expect("rayon pool")
}

/// Distinct packs and pack reads are properties of the operation, not of
/// the thread count, so they are traced on the single-threaded pass; the
/// concurrent pass measures wall clock only. `prep` restores the tier's
/// residency before each pass — without it the concurrent pass would run
/// against a cache the serial pass just filled, and the cold rows would
/// not be cold.
fn ls_rows(
    store: &Store,
    root: &Hash,
    dirs: &[u64],
    tree: &'static str,
    tier: &'static str,
    prep: &dyn Fn(),
) -> Ls {
    let t = Tree::new(store);
    prep();
    store.counters.reset();
    let (mut packs, mut dirents) = (0usize, 0usize);
    let start = Instant::now();
    for &d in dirs {
        store.start_trace();
        dirents += ls_la(&t, root, d);
        packs += store.take_trace();
    }
    let ms_1 = start.elapsed().as_secs_f64() * 1e3 / dirs.len() as f64;
    let reads = store.counters.pack_reads.load(Ordering::Relaxed) as f64 / dirs.len() as f64;

    let p = pool(READ_THREADS);
    prep();
    let start = Instant::now();
    p.install(|| {
        dirs.par_iter().for_each(|&d| {
            std::hint::black_box(ls_la(&t, root, d));
        })
    });
    let ms_8 = start.elapsed().as_secs_f64() * 1e3 / dirs.len() as f64;

    Ls {
        tree,
        tier,
        dirents: dirents as f64 / dirs.len() as f64,
        packs: packs as f64 / dirs.len() as f64,
        reads,
        ms_1,
        ms_8,
    }
}

fn getattr_rows(
    store: &Store,
    root: &Hash,
    inos: &[(u64, u8)],
    tier: &'static str,
    prep: &dyn Fn(),
) -> Getattr {
    let t = Tree::new(store);
    prep();
    store.counters.reset();
    let start = Instant::now();
    for &(i, k) in inos {
        std::hint::black_box(getattr(&t, root, i, k));
    }
    let per_s_1 = inos.len() as f64 / start.elapsed().as_secs_f64().max(1e-9);
    let reads = store.counters.pack_reads.load(Ordering::Relaxed) as f64 / inos.len() as f64;

    let p = pool(READ_THREADS);
    prep();
    let start = Instant::now();
    p.install(|| {
        inos.par_iter().for_each(|&(i, k)| {
            std::hint::black_box(getattr(&t, root, i, k));
        })
    });
    let per_s_8 = inos.len() as f64 / start.elapsed().as_secs_f64().max(1e-9);

    Getattr {
        tier,
        per_s_1,
        per_s_8,
        reads,
    }
}

fn setattr_rows(
    store: &Store,
    t: &Tree<'_>,
    base: &Hash,
    retain: &[Hash],
    corpus: &Corpus,
    files: &[b02::LiveFile],
    tree: &'static str,
    out: &mut Vec<Setattr>,
) {
    for ops in [1_000usize, 10_000, 100_000] {
        {
            let mut rng = SmallRng::seed_from_u64(0x70c4 + ops as u64);
            let (muts, recs) = b02::clustered_touch_batch(files, ops, &mut rng);
            let real = recs.len().max(1);
            let (nodes, _, zbytes, bpo, today, _) =
                b02::measure_batch(store, t, base, retain, &muts, &recs, real);
            out.push(Setattr {
                tree,
                shape: "clustered setattr (one directory)",
                ops: real,
                keys: muts.len(),
                nodes,
                zbytes,
                bpo,
                today,
            });
        }
        {
            let mut rng = SmallRng::seed_from_u64(0xc0ffee + ops as u64);
            let mut next = corpus.len() as u64 + 1_000_000;
            let (muts, recs) = b02::batch(corpus, Shape::Scattered, ops, &mut next, &mut rng);
            let (nodes, _, zbytes, bpo, today, _) =
                b02::measure_batch(store, t, base, retain, &muts, &recs, ops);
            out.push(Setattr {
                tree,
                shape: "scattered setattr (random ino)",
                ops,
                keys: muts.len(),
                nodes,
                zbytes,
                bpo,
                today,
            });
        }
    }
}

fn measure(rep: &mut Report, corpus: &Corpus, cfg: &Cfg, enc: Enc) -> Out {
    keys::set_enc(enc);
    let store = Store::logical_packs(crate::store::DEFAULT_PACK_BYTES);
    let t = Tree::new(&store);
    let t0 = Instant::now();
    let root = t.build_sorted(corpus.all_entries());
    let (levels, _) = t.level_stats(&root);
    let structure = Structure {
        keys: t.agg(&root).keys,
        levels: levels.len(),
        leaf_nodes: levels[0].nodes,
        leaf_bytes: levels[0].bytes,
        interior_bytes: levels[1..].iter().map(|s| s.bytes).sum(),
        zleaf_bytes: crate::b01::leaf_zstd_bytes(&store, &root),
    };
    rep.line(format!(
        "- `{}`: built {} keys in {:.0} s, {} of leaves.",
        enc.flag(),
        structure.keys,
        t0.elapsed().as_secs_f64(),
        bytes(structure.leaf_bytes)
    ));

    // Reads, against packs written in key order — the residency §14.2
    // measured. Tier (b) keeps the interior resident behind a 64 MiB leaf
    // cache; tier (c) drops everything, including the page cache.
    let dir = cfg.pack_dir.join(enc.flag());
    let packed = crate::export_packs(&store, &root, &dir);
    let mut dirs: Vec<u64> = corpus.big_dirs.clone();
    dirs.sort_unstable();
    dirs.dedup();
    dirs.truncate(cfg.dirs);
    let mut ls = Vec::new();
    let mut ga = Vec::new();
    let warm = |s: &Store| s.set_cache_budget(64 << 20);
    let cold = |s: &Store| {
        s.drop_resident();
        s.set_cache_budget(0);
        s.evict_page_cache();
    };
    ls.push(ls_rows(&packed, &root, &dirs, "fresh", "(b) 64 MiB", &|| {
        warm(&packed)
    }));
    let inos: Vec<(u64, u8)> = corpus
        .sample_inos(100_000, 42)
        .into_iter()
        .map(|i| (i, corpus.recs[(i - 2) as usize].kind))
        .collect();
    ga.push(getattr_rows(&packed, &root, &inos, "(b) 64 MiB", &|| {
        warm(&packed)
    }));
    ls.push(ls_rows(&packed, &root, &dirs, "fresh", "(c) cold", &|| {
        cold(&packed)
    }));
    ga.push(getattr_rows(
        &packed,
        &root,
        &inos[..inos.len().min(20_000)],
        "(c) cold",
        &|| cold(&packed),
    ));
    drop(packed);
    let _ = std::fs::remove_dir_all(&dir);

    // Writes: the §14.4 and §14.8 setattr rows, fresh then aged.
    let mut setattr = Vec::new();
    let fresh_files = b02::fresh_touch_files(corpus);
    let (_, fresh_idxs) = b02::densest_dir(&fresh_files);
    let dense_fresh = b02::ino_scatter(&fresh_files, &fresh_idxs);
    setattr_rows(
        &store,
        &t,
        &root,
        &[root],
        corpus,
        &fresh_files,
        "fresh",
        &mut setattr,
    );

    let (aged_root, aged_live, _, age_secs) =
        b02::age_tree(&store, &t, &root, corpus, cfg.age_generations, cfg.age_ops);
    let (_, aged_idxs) = b02::densest_dir(&aged_live);
    let dense_aged = b02::ino_scatter(&aged_live, &aged_idxs);
    setattr_rows(
        &store,
        &t,
        &aged_root,
        &[root, aged_root],
        corpus,
        &aged_live,
        "aged",
        &mut setattr,
    );

    // The aged `ls -la`: the churned directories are wide and their
    // children's inos are no longer a run, which is where the no-copy
    // variant is supposed to lose.
    let dir = cfg.pack_dir.join(format!("{}-aged", enc.flag()));
    let packed = crate::export_packs(&store, &aged_root, &dir);
    ls.push(ls_rows(
        &packed,
        &aged_root,
        &dirs,
        "aged",
        "(b) 64 MiB",
        &|| warm(&packed),
    ));
    ls.push(ls_rows(&packed, &aged_root, &dirs, "aged", "(c) cold", &|| {
        cold(&packed)
    }));
    drop(packed);
    let _ = std::fs::remove_dir_all(&dir);

    Out {
        enc,
        structure,
        setattr,
        ls,
        getattr: ga,
        dense_fresh,
        dense_aged,
        age_secs,
    }
}

pub fn settle_attr_copy(rep: &mut Report, corpus: &Corpus, cfg: &Cfg) {
    rep.head("### S1 — settling the dentry attr copy");
    rep.line(
        "Three `0x02` value shapes through the same code paths against the same corpus: \
         `copy` is §P6 as written (ino + a denormalized attr copy), `nocopy` is ino + kind \
         (so `ls -la` is a range scan plus a point read per child into `0x01`), and \
         `dentry-auth` is the plan's escape hatch — for `nlink == 1` the dentry *is* the \
         record, there is no `0x01` key, and `getattr(ino)` hops through `0x04` first. \
         Directories keep their `0x01` record in all three.",
    );
    rep.blank();

    let outs: Vec<Out> = VARIANTS
        .iter()
        .map(|&e| measure(rep, corpus, cfg, e))
        .collect();
    keys::set_enc(Enc::Copy);

    rep.blank();
    rep.line("#### Footprint at census scale");
    rep.line(
        "The copy inflates every replica, not just writes. zstd leaf bytes are every leaf \
         compressed at the pack writer's level, not a sample: the variants change which key \
         range dominates the first leaves, so a sampled ratio would not be comparable.",
    );
    rep.blank();
    rep.line("| Variant | keys | levels | leaves | leaf bytes | zstd leaf bytes | interior bytes | vs `copy` (zstd leaves) |");
    rep.line("|---|---:|---:|---:|---:|---:|---:|---:|");
    let base_z = outs[0].structure.zleaf_bytes as f64;
    for o in &outs {
        let z = o.structure.zleaf_bytes as f64;
        rep.line(format!(
            "| {} | {} | {} | {} | {} | {} | {} | {:+.1}% |",
            o.enc.flag(),
            o.structure.keys,
            o.structure.levels,
            o.structure.leaf_nodes,
            bytes(o.structure.leaf_bytes),
            bytes(o.structure.zleaf_bytes),
            bytes(o.structure.interior_bytes),
            100.0 * (z - base_z) / base_z,
        ));
    }

    rep.blank();
    rep.line("#### `setattr` bytes per op");
    rep.line(format!(
        "The §14.4 and §14.8 rows recomputed per variant. Aged with {} generations of \
         create/rename/unlink plus a quarter-of-children churn over the 8 widest \
         directories, so the aged densest directory stays wide (fresh {} children over {} \
         distinct 1024-ino buckets; aged {} over {}) and the only thing that moved is where \
         its children's `0x01` records live.",
        cfg.age_generations,
        outs[0].dense_fresh.0,
        outs[0].dense_fresh.1,
        outs[0].dense_aged.0,
        outs[0].dense_aged.1,
    ));
    rep.blank();
    rep.line("| Tree | Shape | ops | Variant | keys written | nodes | zstd bytes | B/op | today's segment B/op | ratio |");
    rep.line("|---|---|---:|---|---:|---:|---:|---:|---:|---:|");
    for i in 0..outs[0].setattr.len() {
        for o in &outs {
            let r = &o.setattr[i];
            rep.line(format!(
                "| {} | {} | {} | {} | {} | {} | {} | {:.0} | {:.0} | {:.2}× |",
                r.tree,
                r.shape,
                r.ops,
                o.enc.flag(),
                r.keys,
                r.nodes,
                bytes(r.zbytes),
                r.bpo,
                r.today,
                r.bpo / r.today.max(1e-9),
            ));
        }
    }

    rep.blank();
    rep.line("#### `ls -la` of a wide directory");
    rep.line(format!(
        "{} directories of ≥1000 entries, scanned whole. Distinct packs and pack reads are \
         properties of the operation and are traced on the single-threaded pass. The tier is \
         re-prepared before each pass — cache cleared for (b), page cache dropped for (c) — so \
         the 8-thread column is a second cold run rather than a replay against the warm cache \
         the serial pass just filled.",
        cfg.dirs
    ));
    rep.blank();
    rep.line("| Tree | Tier | Variant | dirents/dir | distinct packs/dir | pack reads/dir | ms/dir (1 thread) | ms/dir (8 threads) | speed-up |");
    rep.line("|---|---|---|---:|---:|---:|---:|---:|---:|");
    for i in 0..outs[0].ls.len() {
        for o in &outs {
            let r = &o.ls[i];
            rep.line(format!(
                "| {} | {} | {} | {:.0} | {:.2} | {:.1} | {:.3} | {:.3} | {:.2}× |",
                r.tree,
                r.tier,
                o.enc.flag(),
                r.dirents,
                r.packs,
                r.reads,
                r.ms_1,
                r.ms_8,
                r.ms_1 / r.ms_8.max(1e-9),
            ));
        }
    }

    rep.blank();
    rep.line("#### `getattr(ino)` — what the escape hatch costs");
    rep.line(
        "`copy` and `nocopy` read one `0x01` leaf. `dentry-auth` has no `0x01` record for a \
         `nlink == 1` file, so it probes `0x04 | ino` for the name and then reads the dentry: \
         two leaves in two distant ranges.",
    );
    rep.blank();
    rep.line("| Tier | Variant | getattr/s (1 thread) | getattr/s (8 threads) | scaling | pack reads/getattr |");
    rep.line("|---|---|---:|---:|---:|---:|");
    for i in 0..outs[0].getattr.len() {
        for o in &outs {
            let r = &o.getattr[i];
            rep.line(format!(
                "| {} | {} | {:.0} k/s | {:.0} k/s | {:.2}× | {:.2} |",
                r.tier,
                o.enc.flag(),
                r.per_s_1 / 1e3,
                r.per_s_8 / 1e3,
                r.per_s_8 / r.per_s_1.max(1e-9),
                r.reads,
            ));
        }
    }

    rep.blank();
    rep.line(format!(
        "Aging took {:.0}–{:.0} s per variant.",
        outs.iter().map(|o| o.age_secs).fold(f64::MAX, f64::min),
        outs.iter().map(|o| o.age_secs).fold(0.0_f64, f64::max),
    ));
}

/// Small-scale correctness: whatever the encoding, the tree must still
/// answer the two questions FUSE asks — `lookup(parent, name)` and
/// `getattr(ino)` — with the same attrs for every entry.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::Attrs;

    #[test]
    fn every_variant_answers_lookup_and_getattr_alike() {
        let _g = keys::ENC_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let corpus = crate::corpus::generate(20_000, 7);
        let mut expected: Option<Vec<(u64, Attrs)>> = None;
        for enc in VARIANTS {
            keys::set_enc(enc);
            let store = Store::memory();
            let t = Tree::new(&store);
            let root = t.build_sorted(corpus.all_entries());
            let mut got: Vec<(u64, Attrs)> = Vec::new();
            for i in (0..corpus.len()).step_by(97) {
                let r = &corpus.recs[i];
                let ino = corpus.ino(i);
                let d = t
                    .get(&root, &keys::dentry_key(r.parent, corpus.name(r)))
                    .expect("dentry");
                assert_eq!(keys::dentry_ino(&d), ino);
                assert_eq!(keys::dentry_kind(&d), r.kind);
                let (v, at) = getattr(&t, &root, ino, r.kind).expect("getattr");
                got.push((ino, keys::read_attrs(&v[at..])));
            }
            match &expected {
                None => expected = Some(got),
                Some(e) => assert_eq!(*e, got, "{:?} disagrees with {:?}", enc, VARIANTS[0]),
            }
            // The §P7 aggregates are read off the root in every shape.
            assert_eq!(t.agg(&root).files as usize, corpus.file_count());
        }
        keys::set_enc(Enc::Copy);
    }

    /// The no-copy `ls -la` returns what the copy returns, from two ranges
    /// instead of one — the substitution S1 is pricing.
    #[test]
    fn ls_la_agrees_across_variants() {
        let _g = keys::ENC_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let corpus = crate::corpus::generate(20_000, 8);
        let dir = corpus
            .big_dirs
            .first()
            .copied()
            .unwrap_or(corpus.recs[0].parent);
        let mut expected = 0usize;
        for enc in VARIANTS {
            keys::set_enc(enc);
            let store = Store::memory();
            let t = Tree::new(&store);
            let root = t.build_sorted(corpus.all_entries());
            let n = ls_la(&t, &root, dir);
            if enc == VARIANTS[0] {
                expected = n;
                assert!(expected > 0);
            } else {
                assert_eq!(n, expected);
            }
        }
        keys::set_enc(Enc::Copy);
    }
}