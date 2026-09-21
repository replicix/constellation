//! §0.1 — structure, lookup and scan throughput, and the three FUSE
//! shapes the key encoding is supposed to make scale-invariant.

use std::time::Instant;

use crate::corpus::Corpus;
use crate::keys;
use crate::node::{Hash, NodeRef};
use crate::stats::{bytes, ns, rate, Report, Samples};
use crate::store::Store;
use crate::tree::{Mut, Tree};

/// What the pack writer's zstd would achieve on the leaves, sampled over
/// the first `sample` of them in key order — which is how they are packed.
pub fn leaf_zstd_ratio(store: &Store, root: &Hash, sample: usize) -> f64 {
    let (mut raw, mut comp) = (0u64, 0u64);
    let mut stack = vec![*root];
    let mut leaves = 0;
    while let Some(h) = stack.pop() {
        let b = store.get(&h);
        let n = NodeRef::new(&b);
        if n.level == 0 {
            raw += b.len() as u64;
            comp += zstd::encode_all(&b[..], crate::store::ZSTD_LEVEL)
                .unwrap()
                .len() as u64;
            leaves += 1;
            if leaves >= sample {
                break;
            }
        } else {
            for i in (0..n.count).rev() {
                stack.push(n.child(i).0);
            }
        }
    }
    comp as f64 / raw.max(1) as f64
}

/// Every leaf compressed, not a sample of them. The sampled ratio above
/// reads the first 256 leaves in key order, which is representative only
/// while the `0x01` range looks like the rest of the tree — and the whole
/// point of the S1 variants is that it need not.
pub fn leaf_zstd_bytes(store: &Store, root: &Hash) -> u64 {
    use rayon::prelude::*;
    let mut leaves: Vec<Hash> = Vec::new();
    let mut stack = vec![*root];
    while let Some(h) = stack.pop() {
        let b = store.get(&h);
        let n = NodeRef::new(&b);
        if n.level == 0 {
            leaves.push(h);
        } else {
            for i in (0..n.count).rev() {
                stack.push(n.child(i).0);
            }
        }
    }
    leaves
        .par_iter()
        .map(|h| {
            zstd::encode_all(&store.get(h)[..], crate::store::ZSTD_LEVEL)
                .unwrap()
                .len() as u64
        })
        .sum()
}

pub fn structure(rep: &mut Report, store: &Store, root: &Hash, corpus: &Corpus) {
    let t = Tree::new(store);
    let (levels, mut leaf_counts) = t.level_stats(root);
    let agg = t.agg(root);

    rep.head("### Tree structure at census scale");
    rep.line("| Level | Nodes | Entries | Encoded bytes | Mean entries/node | Mean node bytes |");
    rep.line("|---|---:|---:|---:|---:|---:|");
    for (l, s) in levels.iter().enumerate() {
        rep.line(format!(
            "| {} | {} | {} | {} | {:.0} | {} |",
            if l == 0 {
                "0 (leaf)".into()
            } else {
                l.to_string()
            },
            s.nodes,
            s.entries,
            bytes(s.bytes),
            s.entries as f64 / s.nodes as f64,
            bytes(s.bytes / s.nodes.max(1)),
        ));
    }
    let leaf_bytes = levels[0].bytes;
    let interior_bytes: u64 = levels[1..].iter().map(|s| s.bytes).sum();
    leaf_counts.sort_unstable();
    let pick = |p: f64| leaf_counts[((leaf_counts.len() - 1) as f64 * p) as usize];

    let ratio = leaf_zstd_ratio(store, root, 256);

    rep.blank();
    rep.line(format!(
        "- Keys: **{}** ({} inodes + {} dentries + {} reverse dentries + {} spilled xattrs)",
        agg.keys,
        corpus.len(),
        corpus.len(),
        corpus.len(),
        corpus.spilled.len() * corpus.xattrs_per_spilled
    ));
    rep.line(format!(
        "- Levels: **{}** ({} interior hops + 1 leaf)",
        levels.len(),
        levels.len() - 1
    ));
    rep.line(format!(
        "- Interior bytes (all non-leaf nodes): **{}**; leaf bytes: **{}** (zstd ≈ {} at {:.0}%)",
        bytes(interior_bytes),
        bytes(leaf_bytes),
        bytes((leaf_bytes as f64 * ratio) as u64),
        ratio * 100.0
    ));
    rep.line(format!(
        "- Leaf entries/node: p1 {}, p50 {}, p99 {}, max {} (geometric, no clamp — see `node.rs`)",
        pick(0.01),
        pick(0.5),
        pick(0.99),
        leaf_counts.last().copied().unwrap_or(0)
    ));
    rep.line(format!(
        "- Aggregates from the root node alone: {} files, {} logical bytes",
        agg.files,
        bytes(agg.bytes)
    ));
}

fn timed<F: FnMut(usize)>(n: usize, mut f: F) -> (f64, Samples) {
    let mut s = Samples::with_capacity(n);
    let start = Instant::now();
    for i in 0..n {
        let t0 = Instant::now();
        f(i);
        s.push_ns(t0.elapsed().as_nanos());
    }
    (start.elapsed().as_secs_f64(), s)
}

struct TierResult {
    lookup: (f64, u32, u32, u64),
    getattr: (f64, u32, u32, u64),
    readdir: (f64, u32, u32, u64),
}

fn tier(store: &Store, root: &Hash, corpus: &Corpus, n: usize, dirs: &[u64]) -> TierResult {
    let t = Tree::new(store);
    let dkeys = corpus.sample_dentries(n, 42);
    let inos = corpus.sample_inos(n, 42);

    store.counters.reset();
    let (secs, mut s) = timed(n, |i| {
        let v = t.get(root, &dkeys[i]);
        debug_assert!(v.is_some());
        std::hint::black_box(v);
    });
    let reads = store
        .counters
        .pack_reads
        .load(std::sync::atomic::Ordering::Relaxed);
    let lookup = (n as f64 / secs, s.pct(0.5), s.pct(0.99), reads);

    store.counters.reset();
    let (secs, mut s) = timed(n, |i| {
        let v = t.get(root, &keys::inode_key(inos[i]));
        std::hint::black_box(v);
    });
    let reads = store
        .counters
        .pack_reads
        .load(std::sync::atomic::Ordering::Relaxed);
    let getattr = (n as f64 / secs, s.pct(0.5), s.pct(0.99), reads);

    // readdir: full sequential scans of real directories, measured per
    // dirent returned.
    store.counters.reset();
    let mut entries = 0u64;
    let mut s = Samples::with_capacity(dirs.len());
    let mut packs_per_dir = 0usize;
    let start = Instant::now();
    for d in dirs {
        store.start_trace();
        let t0 = Instant::now();
        let (cnt, _) = t.scan_prefix(root, &keys::dentry_prefix(*d), usize::MAX);
        s.push_ns(t0.elapsed().as_nanos() / cnt.max(1) as u128);
        entries += cnt as u64;
        packs_per_dir += store.take_trace();
    }
    let secs = start.elapsed().as_secs_f64();
    let reads = store
        .counters
        .pack_reads
        .load(std::sync::atomic::Ordering::Relaxed);
    let readdir = (
        entries as f64 / secs,
        s.pct(0.5),
        s.pct(0.99),
        (packs_per_dir / dirs.len().max(1)) as u64,
    );
    let _ = reads;

    TierResult {
        lookup,
        getattr,
        readdir,
    }
}

pub fn throughput(
    rep: &mut Report,
    mem: &Store,
    packed: &Store,
    root: &Hash,
    corpus: &Corpus,
    samples: usize,
) {
    let mut dirs: Vec<u64> = corpus.big_dirs.iter().copied().take(64).collect();
    if dirs.len() < 8 {
        dirs = corpus.recs.iter().map(|r| r.parent).take(512).collect();
        dirs.sort_unstable();
        dirs.dedup();
    }

    rep.head("### Lookup / scan throughput by residency tier");
    rep.line("Single-threaded. (a) everything resident; (b) interior resident, leaves in packs on disk behind a small LRU; (c) cold — nothing cached, page cache dropped.");
    rep.blank();
    rep.line("| Tier | lookup(parent,name) | p50 | p99 | getattr(ino) | p50 | p99 | readdir | p50/dirent | pack reads/lookup | pack reads/`ls -la` |");
    rep.line("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");

    let a = tier(mem, root, corpus, samples, &dirs);
    let row = |rep: &mut Report, name: &str, r: &TierResult, n: usize| {
        rep.line(format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.2} | {} |",
            name,
            rate(n as u64, n as f64 / r.lookup.0),
            ns(r.lookup.1),
            ns(r.lookup.2),
            rate(n as u64, n as f64 / r.getattr.0),
            ns(r.getattr.1),
            ns(r.getattr.2),
            rate(1, 1.0 / r.readdir.0),
            ns(r.readdir.1),
            r.lookup.3 as f64 / n as f64,
            r.readdir.3,
        ))
    };
    row(rep, "(a) resident", &a, samples);

    let bn = samples.min(200_000);
    let b = tier(packed, root, corpus, bn, &dirs[..dirs.len().min(16)]);
    row(rep, "(b) interior resident", &b, bn);

    // Tier (b) is only defined up to a cache size, and that is the whole
    // question for a client that must not hold the filesystem in RAM.
    rep.blank();
    rep.line("| leaf cache (b) | lookup | p50 | p99 | pack reads/lookup |");
    rep.line("|---|---:|---:|---:|---:|");
    for mib in [64usize, 256, 1024, 4096] {
        packed.set_cache_budget(mib << 20);
        let n = if mib >= 1024 {
            samples.min(400_000)
        } else {
            samples.min(200_000)
        };
        // Warm the cache with one pass so the measurement is steady-state.
        let warm = tier(packed, root, corpus, n / 4, &dirs[..dirs.len().min(4)]);
        let _ = warm;
        let r = tier(packed, root, corpus, n, &dirs[..dirs.len().min(4)]);
        rep.line(format!(
            "| {} MiB | {} | {} | {} | {:.2} |",
            mib,
            rate(n as u64, n as f64 / r.lookup.0),
            ns(r.lookup.1),
            ns(r.lookup.2),
            r.lookup.3 as f64 / n as f64,
        ));
    }

    packed.drop_resident();
    packed.set_cache_budget(0);
    packed.evict_page_cache();
    let cn = samples.min(50_000);
    let c = tier(packed, root, corpus, cn, &dirs[..dirs.len().min(4)]);
    row(rep, "(c) cold", &c, cn);

    rep.blank();
    rep.line("ADR-9 baseline on the same machine (`bench/dbbench`, SQLite mutex engine): 1.04 M lookups/s isolated, 0.71 M readdir dirs/s, 0.98 M getattr/s; DESIGN §11's census figures are 578 k lookups/s and 521 k readdir/s.");
    rep.blank();
    let gate_a = a.lookup.0 >= 300_000.0;
    let gate_b = b.lookup.0 >= 150_000.0;
    rep.line(format!(
        "**Gate 0.1: (a) ≥ 300 k lookups/s → {} ({:.0} k/s); (b) ≥ 150 k lookups/s → {} ({:.0} k/s).**",
        if gate_a { "PASS" } else { "FAIL" },
        a.lookup.0 / 1e3,
        if gate_b { "PASS" } else { "FAIL" },
        b.lookup.0 / 1e3,
    ));
}

/// The three shapes §0.1 calls out: paged readdir, rename of a directory
/// with a large subtree, and `listxattr` on a spilled set.
pub fn fuse_shapes(rep: &mut Report, store: &Store, root: &Hash, corpus: &Corpus) {
    let t = Tree::new(store);
    rep.head("### FUSE shapes");

    // Paged readdir: 100 dirents per FUSE page, resuming from the key.
    let dir = corpus
        .big_dirs
        .first()
        .copied()
        .unwrap_or(corpus.recs[0].parent);
    let prefix = keys::dentry_prefix(dir);
    let all = t.collect_prefix(root, &prefix, usize::MAX);
    let mut pages = 0u64;
    let mut seen = 0usize;
    let mut cursor = prefix.clone();
    let start = Instant::now();
    loop {
        // Each page re-seeks from the last key returned, exactly as a
        // FUSE readdir cursor would.
        let page = t.collect_from(root, &cursor, &prefix, 101);
        let page: Vec<Vec<u8>> = if pages == 0 {
            page
        } else {
            page.into_iter().skip(1).collect()
        };
        if page.is_empty() {
            break;
        }
        seen += page.len();
        cursor = page.last().unwrap().clone();
        pages += 1;
    }
    let secs = start.elapsed().as_secs_f64();
    rep.line(format!(
        "- Paged `readdir` of a {}-entry directory: {} dirents in {} pages of 100, {:.1} µs/page, resumption is an exact key seek (no offset table).",
        all.len(),
        seen,
        pages,
        secs * 1e6 / pages.max(1) as f64
    ));

    // listxattr on a spilled set.
    let ino = corpus.spilled[corpus.spilled.len() / 2];
    let start = Instant::now();
    let (n, b) = t.scan_prefix(root, &keys::xattr_prefix(ino), usize::MAX);
    rep.line(format!(
        "- `listxattr` on a spilled set: {n} names, {} scanned in {:.1} µs (one range scan).",
        bytes(b),
        start.elapsed().as_secs_f64() * 1e6
    ));

    // Rename invariance: same op against directories of very different size.
    rep.blank();
    rep.line("| rename of a directory with N descendants | N | keys written | nodes written | bytes | time |");
    rep.line("|---|---:|---:|---:|---:|---:|");
    let mut sizes: Vec<(u64, u64)> = Vec::new();
    for d in corpus.big_dirs.iter().take(3) {
        let n = t.scan_prefix(root, &keys::dentry_prefix(*d), usize::MAX).0;
        sizes.push((*d, n as u64));
    }
    if let Some(r) = corpus
        .recs
        .iter()
        .enumerate()
        .find(|(_, r)| r.kind == keys::KIND_DIR)
    {
        sizes.push((r.0 as u64 + 2, 0));
    }
    for (dir_ino, n) in sizes {
        let (muts, elapsed, st) = rename_dir(&t, root, corpus, dir_ino);
        rep.line(format!(
            "| `mv d{dir_ino:x} d{dir_ino:x}.renamed` | {} | {} | {} | {} | {:.1} µs |",
            if n == 0 {
                "leaf dir".to_string()
            } else {
                n.to_string()
            },
            muts,
            st.0,
            bytes(st.1),
            elapsed * 1e6
        ));
    }
}

/// A rename touches the dentry, the reverse dentry, and the two parents —
/// never the descendants, whose keys carry the directory's *ino*.
pub fn rename_dir(
    t: &Tree<'_>,
    root: &Hash,
    corpus: &Corpus,
    dir_ino: u64,
) -> (usize, f64, (u64, u64)) {
    let idx = (dir_ino - 2) as usize;
    let rec = &corpus.recs[idx];
    let name = corpus.name(rec).to_vec();
    let mut new_name = name.clone();
    new_name.extend_from_slice(b".renamed");
    let dval = t
        .get(root, &keys::dentry_key(rec.parent, &name))
        .expect("dentry");
    let pkey = keys::inode_key(rec.parent);
    let pval = t.get(root, &pkey).unwrap_or_default();
    let mut muts: Vec<Mut> = vec![
        (keys::dentry_key(rec.parent, &name), None),
        (keys::dentry_key(rec.parent, &new_name), Some(dval)),
        (keys::rdentry_key(dir_ino, rec.parent, &name), None),
        (
            keys::rdentry_key(dir_ino, rec.parent, &new_name),
            Some(Vec::new()),
        ),
        (pkey, Some(pval)),
    ];
    muts.sort();
    muts.dedup_by(|a, b| a.0 == b.0);
    let before = (
        t.store
            .counters
            .puts
            .load(std::sync::atomic::Ordering::Relaxed),
        t.store
            .counters
            .put_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    let start = Instant::now();
    let _new = t.apply(root, &muts);
    let elapsed = start.elapsed().as_secs_f64();
    let after = (
        t.store
            .counters
            .puts
            .load(std::sync::atomic::Ordering::Relaxed),
        t.store
            .counters
            .put_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    (
        muts.len(),
        elapsed,
        (after.0 - before.0, after.1 - before.1),
    )
}
