//! §0.5 — thread scaling.
//!
//! Every number in §14 was single-threaded; the daemon is not
//! (CONVENTIONS: host-sized concurrent event loops). Immutable
//! content-addressed nodes are the ideal read-parallel structure: a
//! lookup is a pure function of `(root, key)` over a node set nobody can
//! mutate. Measure that, then the write-side pieces that §14.5 said ate
//! most of a core (mark + compaction) and a shard-parallel commit build.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use rayon::prelude::*;

use crate::b02::{self, Shape};
use crate::corpus::Corpus;
use crate::node::Hash;
use crate::stats::{bytes, rate, Report};
use crate::store::Store;
use crate::tree::{Mut, Tree};

const THREADS: &[usize] = &[1, 2, 4, 8, 16, 32];

pub fn thread_scaling(
    rep: &mut Report,
    mem: &Store,
    packed: &Store,
    root: &Hash,
    corpus: &Corpus,
    samples: usize,
) {
    rep.head("### 0.5 Thread scaling");
    rep.line(
        "Immutable content-addressed nodes: a lookup is a pure function of `(root, key)`. \
         Tier (b) uses a 64 MiB leaf cache — the size at which the single-threaded gate failed.",
    );
    packed.set_cache_budget(64 << 20);
    packed.clear_cache();

    let dkeys = corpus.sample_dentries(samples, 42);
    let n = samples.min(500_000);

    rep.blank();
    rep.line("| Threads | (a) lookup/s | (a) scaling | (b) 64 MiB lookup/s | (b) scaling | (b) pack reads/lookup |");
    rep.line("|---:|---:|---:|---:|---:|---:|");

    let mut base_a = 0.0_f64;
    let mut base_b = 0.0_f64;
    let mut best_b_at_le_8 = 0.0_f64;

    for &threads in THREADS {
        // Warm (b) once per thread count so the measurement is steady-state.
        if threads == 1 {
            parallel_lookups(packed, root, &dkeys[..n / 4], 1);
        }
        let (a_rate, _) = parallel_lookups(mem, root, &dkeys[..n], threads);
        let (b_rate, b_reads) = parallel_lookups(packed, root, &dkeys[..n], threads);
        if threads == 1 {
            base_a = a_rate;
            base_b = b_rate;
        }
        if threads <= 8 {
            best_b_at_le_8 = best_b_at_le_8.max(b_rate);
        }
        rep.line(format!(
            "| {} | {} | {:.2}× | {} | {:.2}× | {:.2} |",
            threads,
            rate(1, 1.0 / a_rate),
            a_rate / base_a.max(1.0),
            rate(1, 1.0 / b_rate),
            b_rate / base_b.max(1.0),
            b_reads as f64 / n as f64,
        ));
    }

    let gate = best_b_at_le_8 >= 150_000.0;
    rep.blank();
    rep.line(format!(
        "**Gate 0.5: tier (b) ≥ 150 k/s aggregate at ≤ 8 threads with a 64 MiB leaf cache → {} ({:.0} k/s best).**",
        if gate { "PASS" } else { "FAIL" },
        best_b_at_le_8 / 1e3,
    ));

    commit_shards(rep, mem, root, corpus);
    mark_and_compact(rep, packed, root, corpus);
}

fn parallel_lookups(
    store: &Store,
    root: &Hash,
    keys: &[Vec<u8>],
    threads: usize,
) -> (f64, u64) {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .expect("pool");
    store.counters.reset();
    let t = Tree::new(store);
    let found = AtomicU64::new(0);
    let start = Instant::now();
    pool.install(|| {
        keys.par_iter().for_each(|k| {
            let v = t.get(root, k);
            if v.is_some() {
                found.fetch_add(1, Ordering::Relaxed);
            }
            std::hint::black_box(v);
        });
    });
    let secs = start.elapsed().as_secs_f64().max(1e-9);
    let reads = store.counters.pack_reads.load(Ordering::Relaxed);
    let _ = found.load(Ordering::Relaxed);
    (keys.len() as f64 / secs, reads)
}

/// §P4-shaped commit: split a 32k-op batch into N key-range shards,
/// apply each shard from the common root in parallel, then fold the
/// resulting roots with three-way merge. Compare against a single-threaded
/// apply of the same batch.
fn commit_shards(rep: &mut Report, store: &Store, root: &Hash, corpus: &Corpus) {
    use rand::rngs::SmallRng;
    use rand::SeedableRng;

    rep.blank();
    rep.line("#### Commit build across shards");
    rep.line(
        "A 32k-op clustered batch, applied whole (1 thread) versus split into N key-range \
         shards applied in parallel from the same root and merged left-to-right.",
    );
    rep.blank();
    rep.line("| Shards | wall | ops/s | nodes written | vs 1-thread |");
    rep.line("|---:|---:|---:|---:|---:|");

    let mut rng = SmallRng::seed_from_u64(0x5_7a2d);
    let mut next_ino = corpus.len() as u64 + 80_000_000;
    let ops = 32_000usize;
    let (muts, _) = b02::batch(corpus, Shape::Clustered, ops, &mut next_ino, &mut rng);
    let t = Tree::new(store);

    store.counters.reset();
    let t0 = Instant::now();
    let _ = t.apply(root, &muts);
    let seq = t0.elapsed().as_secs_f64();
    let o = Ordering::Relaxed;
    let seq_nodes = store.counters.new_nodes.load(o);
    // Drop the commit so we measure against the same base.
    store.flush();
    let live: std::collections::HashSet<Hash> = t.reachable(std::slice::from_ref(root));
    store.sweep_packs(&live);

    rep.line(format!(
        "| 1 (whole) | {:.0} ms | {} | {} | 1.00× |",
        seq * 1e3,
        rate(ops as u64, seq),
        seq_nodes,
    ));

    for &shards in &[2usize, 4, 8, 16] {
        let chunk = (muts.len() + shards - 1) / shards;
        let parts: Vec<&[Mut]> = muts.chunks(chunk.max(1)).collect();
        store.counters.reset();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(shards)
            .build()
            .unwrap();
        let t0 = Instant::now();
        let roots: Vec<Hash> = pool.install(|| {
            parts
                .par_iter()
                .map(|part| t.apply(root, part))
                .collect()
        });
        // Fold with pairwise merge against the base root as ancestor.
        let mut acc = roots[0];
        for r in &roots[1..] {
            match t.merge(root, &acc, r) {
                Ok(h) => acc = h,
                Err(conflicts) => {
                    // Disjoint key ranges should not conflict; if they do,
                    // fall back to applying the remaining muts serially.
                    let _ = conflicts;
                    acc = t.apply(root, &muts);
                    break;
                }
            }
        }
        let wall = t0.elapsed().as_secs_f64();
        let nodes = store.counters.new_nodes.load(o);
        let _ = acc;
        store.flush();
        let live: std::collections::HashSet<Hash> = t.reachable(std::slice::from_ref(root));
        store.sweep_packs(&live);
        rep.line(format!(
            "| {} | {:.0} ms | {} | {} | {:.2}× |",
            shards,
            wall * 1e3,
            rate(ops as u64, wall),
            nodes,
            seq / wall.max(1e-9),
        ));
    }
}

fn mark_and_compact(rep: &mut Report, packed: &Store, root: &Hash, corpus: &Corpus) {
    use rand::rngs::SmallRng;
    use rand::SeedableRng;

    rep.blank();
    rep.line("#### Mark + compaction parallelism");
    rep.line(
        "Produce a dirty pack set with commits from a single tip (no retention window — \
         superseded nodes are dead), then mark and compact at 1…32 threads. §14.5 spent \
         939 of 1,205 s on single-threaded GC; the question is whether that divides by cores.",
    );

    let t = Tree::new(packed);
    let mut rng = SmallRng::seed_from_u64(0x9c);
    let mut next_ino = corpus.len() as u64 + 90_000_000;
    let mut cur = *root;
    // Mix of clustered and scattered so packs end up partially dead rather
    // than whole-dead (scattered punches holes across many packs).
    for i in 0..48 {
        let shape = if i % 5 == 0 {
            Shape::Scattered
        } else {
            Shape::Clustered
        };
        let (muts, _) = b02::batch(corpus, shape, 8_000, &mut next_ino, &mut rng);
        cur = t.apply(&cur, &muts);
    }
    packed.flush();
    // Mark from the tip only: every superseded node is garbage, which is
    // what the compactor is for. Retention is measured in §0.2b.
    let tip = [cur];
    let live_set = t.reachable(&tip);
    let (whole, partial, live_b, dead_b) = packed.pack_liveness(&live_set);
    rep.line(format!(
        "Dirty tip: {} whole-dead packs, {} partial, {} live / {} dead bytes.",
        whole,
        partial,
        bytes(live_b),
        bytes(dead_b),
    ));

    rep.blank();
    rep.line("| Threads | mark | mark scaling | compact (≤512 MiB budget) | compact MiB/s | compact scaling |");
    rep.line("|---:|---:|---:|---:|---:|---:|");

    let mark_times: Vec<(usize, f64)> = THREADS
        .iter()
        .map(|&threads| {
            let t0 = Instant::now();
            let s = t.reachable_par(&tip, threads);
            let secs = t0.elapsed().as_secs_f64();
            assert_eq!(s.len(), live_set.len());
            (threads, secs)
        })
        .collect();

    // Compact at each thread count needs a fresh dirty set: compaction
    // consumes victims. Re-dirty with a fixed burst before each trial.
    let budget = 512 << 20;
    let mut base_mark = 0.0_f64;
    let mut base_c = 0.0_f64;
    let mut compact_rows: Vec<(usize, f64, u64)> = Vec::new();
    for &threads in THREADS {
        for i in 0..12 {
            let shape = if i % 3 == 0 {
                Shape::Scattered
            } else {
                Shape::Clustered
            };
            let (muts, _) = b02::batch(corpus, shape, 4_000, &mut next_ino, &mut rng);
            cur = t.apply(&cur, &muts);
        }
        packed.flush();
        let tip = [cur];
        let live = t.reachable_par(&tip, threads.max(1));
        let t0 = Instant::now();
        let (_packs, rewritten, _freed) = packed.compact_par(&live, 0.5, budget, threads);
        let secs = t0.elapsed().as_secs_f64().max(1e-9);
        compact_rows.push((threads, secs, rewritten));
    }

    for &(threads, mark_secs) in &mark_times {
        if threads == 1 {
            base_mark = mark_secs;
        }
        let (c_secs, c_bytes) = compact_rows
            .iter()
            .find(|(th, _, _)| *th == threads)
            .map(|(_, s, b)| (*s, *b))
            .unwrap_or((0.0, 0));
        if threads == 1 {
            base_c = c_secs;
        }
        let mibs = (c_bytes as f64 / (1 << 20) as f64) / c_secs.max(1e-9);
        rep.line(format!(
            "| {} | {:.1} ms | {:.2}× | {:.0} ms / {} | {:.1} | {:.2}× |",
            threads,
            mark_secs * 1e3,
            base_mark / mark_secs.max(1e-9),
            c_secs * 1e3,
            bytes(c_bytes),
            mibs,
            if c_bytes == 0 || base_c == 0.0 {
                0.0
            } else {
                // Scale by throughput (bytes/s), not wall: each trial's
                // dirty set differs slightly.
                let base_thr = compact_rows[0].2 as f64 / base_c;
                let thr = c_bytes as f64 / c_secs;
                thr / base_thr.max(1.0)
            },
        ));
    }
}
