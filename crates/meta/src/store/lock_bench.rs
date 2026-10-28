//! Benchmarks of the store's single writer lock as a root sequencer sees
//! it: how long its background passes hold it, and what that does to the
//! latency of the ops queued behind them. Every test here is `#[ignore]`d
//! (run with `--ignored --nocapture`); the numbers are printed as `BENCH`
//! lines, one measurement per line, `key=value` pairs.
//!
//! Environment (all optional):
//! - `CST_LOCKBENCH_DIR`: where the databases live (a directory on the file
//!   system under test; default the system temp dir). Every test makes and
//!   removes its own subdirectory.
//! - `CST_LOCKBENCH_N`: rows in the `completed` table (default 200000).
//! - `CST_LOCKBENCH_STALE_PCT`: percentage of them past retention when a
//!   prune runs (default 25: the prune runs every quarter of the window).
//! - `CST_LOCKBENCH_BACKLOG`: unshipped journal rows at the start of the
//!   model run (default 20000).
//! - `CST_LOCKBENCH_SECS`, `_WORKERS`, `_THINK_US`, `_PRUNE_EVERY_S`,
//!   `_SHIP_MS`, `_SHIP_BATCH`: the model's run time, FUSE-worker threads,
//!   a worker's pause between ops, the prune cadence, the ship cadence and
//!   rows per ship.
//! - `CST_LOCKBENCH_OFF`: comma list of `prune`, `ship`, `recent` to leave
//!   out of the model run (the ablation); `prunecall` keeps the prune's
//!   rows being written (the bulk write of the aged rows) but not the
//!   `prune_completed` call.
//! - `CST_LOCKBENCH_INO=memory`: the model's workers take their inos from a
//!   counter in memory instead of `allocate_ino` (what an allocator that
//!   hands numbers out from memory would cost the root at best).

use crate::mutate::{execute, MutateOp};
use crate::rid::Rid;
use crate::store::Meta;
use crate::MetaStore;
use constellation_fs_core::types::ROOT_INO;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn bench_dir(tag: &str) -> PathBuf {
    let base = std::env::var("CST_LOCKBENCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());
    let dir = base.join(format!("cst-lockbench-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn pct(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i]
}

fn line(name: &str, us: &mut [u64], extra: &str) {
    us.sort_unstable();
    eprintln!(
        "BENCH {name} count={} p50_us={} p90_us={} p99_us={} p999_us={} max_us={} {extra}",
        us.len(),
        pct(us, 0.50),
        pct(us, 0.90),
        pct(us, 0.99),
        pct(us, 0.999),
        us.last().copied().unwrap_or(0),
    );
}

/// `n` rows in `completed`, the first `stale` of them recorded at time 0 and
/// the rest at `fresh_at`, written in small chunks with a pause between
/// them (the writer lock goes to whoever asks first, so chunks in a row
/// would be one long hold: filling must not be a stall of its own).
fn fill_completed(meta: &Meta, node: u64, n: u64, stale: u64, fresh_at: i64) {
    let mut seq = 0u64;
    while seq < n {
        let mut tx = meta.db.write_tx();
        for _ in 0..250.min(n - seq) {
            let at = if seq < stale { 0 } else { fresh_at };
            let rid = Rid {
                node,
                incarnation: 1,
                seq,
            };
            tx.insert(
                &meta.completed,
                rid.to_key(),
                Meta::encode_completed_row(seq + 1, at),
            );
            seq += 1;
        }
        tx.commit().unwrap();
        std::thread::sleep(Duration::from_micros(500));
    }
}

fn create_op(name: String, ino: u64) -> MutateOp {
    MutateOp::Create {
        parent: ROOT_INO,
        name,
        ino,
        mode: 0o644,
        uid: 0,
        gid: 0,
    }
}

/// What one `prune_completed` costs for table sizes around the root's.
#[test]
#[ignore]
fn bench_prune_cost() {
    let max = env_u64("CST_LOCKBENCH_N", 200_000);
    let stale_pct = env_u64("CST_LOCKBENCH_STALE_PCT", 25);
    let mut n = 10_000u64;
    while n <= max {
        let dir = bench_dir("prune");
        let meta = Meta::open(&dir).unwrap();
        let stale = n * stale_pct / 100;
        fill_completed(&meta, 9, n, stale, 1_000_000);
        let t = Instant::now();
        let pruned = meta.prune_completed(1_000_000 + 100, 500).unwrap();
        let took = t.elapsed();
        assert_eq!(pruned, stale);
        // The second prune walks the tombstones the first one left.
        let t = Instant::now();
        meta.prune_completed(1_000_000 + 100, 500).unwrap();
        let again = t.elapsed();
        eprintln!(
            "BENCH prune_cost rows={n} stale={stale} first_ms={:.2} second_ms={:.2}",
            took.as_secs_f64() * 1e3,
            again.as_secs_f64() * 1e3
        );
        drop(meta);
        let _ = std::fs::remove_dir_all(&dir);
        n *= 3;
    }
}

/// What a ship's acknowledgement costs under the lock for a journal
/// backlog of `B` rows behind the ones it ships.
#[test]
#[ignore]
fn bench_ack_cost() {
    let max = env_u64("CST_LOCKBENCH_BACKLOG", 20_000);
    let mut backlog = 1_000u64;
    while backlog <= max {
        let dir = bench_dir("ack");
        let meta = Meta::open(&dir).unwrap();
        for n in 0..backlog + 200 {
            execute(
                &meta,
                &create_op(format!("f{n}"), (3 << 40) | n),
                Some(Rid {
                    node: 3,
                    incarnation: 1,
                    seq: n,
                }),
            )
            .unwrap();
        }
        let mut us = Vec::new();
        for i in 0..100u64 {
            // Two rows (an op and its completion) at a time off the front.
            let rows = meta.take_journal(2).unwrap();
            let seqs: Vec<u64> = rows.into_iter().map(|(s, _)| s).collect();
            let t = Instant::now();
            meta.ack_journal_rows_at(&seqs, i + 1).unwrap();
            us.push(t.elapsed().as_micros() as u64);
        }
        line(
            "ack_cost",
            &mut us,
            &format!("backlog_rows={}", backlog * 2),
        );
        drop(meta);
        let _ = std::fs::remove_dir_all(&dir);
        backlog *= 4;
    }
}

/// The per-ship `prune_recent_shipped` for a `recent` map of `K` entries
/// (forwarding peers' unacknowledged windows; the cap is 4096 per
/// requester incarnation).
#[test]
#[ignore]
fn bench_recent_cost() {
    for incarnations in [1u32, 4, 16] {
        let dir = bench_dir("recent");
        let meta = Meta::open(&dir).unwrap();
        let per = crate::store::MAX_RECENT_PER_INCARNATION as u64;
        for inc in 1..=incarnations {
            for seq in 0..per {
                let rid = Rid {
                    node: 7,
                    incarnation: inc,
                    seq,
                };
                execute(
                    &meta,
                    &create_op(
                        format!("r{inc}-{seq}"),
                        (7 << 40) | (inc as u64 * per + seq),
                    ),
                    Some(rid),
                )
                .unwrap();
                meta.remember_outcome(rid, &[]);
            }
        }
        let mut us = Vec::new();
        for _ in 0..30 {
            let t = Instant::now();
            // Nothing is shipped: every entry is looked up and kept.
            meta.prune_recent_shipped(0).unwrap();
            us.push(t.elapsed().as_micros() as u64);
        }
        line(
            "recent_cost",
            &mut us,
            &format!("recent_entries={}", per * incarnations as u64),
        );
        drop(meta);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// `allocate_ino` followed by the create: the second write transaction's
/// cost, and the number of writer-lock acquisitions per create.
#[test]
#[ignore]
fn bench_ino_cost() {
    let dir = bench_dir("ino");
    let meta = Meta::open(&dir).unwrap();
    let acquisitions = Arc::new(AtomicU64::new(0));
    {
        let acquisitions = acquisitions.clone();
        meta.set_writer_wait_hook_for_tests(Some(Arc::new(move || {
            acquisitions.fetch_add(1, Relaxed);
        })));
    }
    let rounds = env_u64("CST_LOCKBENCH_N", 20_000).min(20_000);
    let mut alloc = Vec::new();
    let mut create = Vec::new();
    let mut both = Vec::new();
    for n in 0..rounds {
        let t0 = Instant::now();
        let ino = meta.allocate_ino(ROOT_INO).unwrap();
        let t1 = Instant::now();
        execute(
            &meta,
            &create_op(format!("f{n}"), ino),
            Some(Rid {
                node: 3,
                incarnation: 1,
                seq: n,
            }),
        )
        .unwrap();
        let t2 = Instant::now();
        alloc.push((t1 - t0).as_micros() as u64);
        create.push((t2 - t1).as_micros() as u64);
        both.push((t2 - t0).as_micros() as u64);
    }
    let total = acquisitions.load(Relaxed);
    meta.set_writer_wait_hook_for_tests(None);
    let extra = format!(
        "lock_acquisitions_per_create={:.2}",
        total as f64 / rounds as f64
    );
    line("ino_alloc", &mut alloc, "");
    line("ino_create", &mut create, "");
    line("ino_alloc_plus_create", &mut both, &extra);
    drop(meta);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A root under the load of its FUSE workers, with its authority thread's
/// ships and the periodic prune: the latency of the workers' ops, overall and with each background pass left out in turn
/// (`CST_LOCKBENCH_OFF`).
#[test]
#[ignore]
fn bench_root_model() {
    let n = env_u64("CST_LOCKBENCH_N", 200_000);
    let stale_pct = env_u64("CST_LOCKBENCH_STALE_PCT", 25);
    let backlog = env_u64("CST_LOCKBENCH_BACKLOG", 20_000);
    let secs = env_u64("CST_LOCKBENCH_SECS", 30);
    let workers = env_u64("CST_LOCKBENCH_WORKERS", 4);
    let think = env_u64("CST_LOCKBENCH_THINK_US", 2_000);
    let prune_every = env_u64("CST_LOCKBENCH_PRUNE_EVERY_S", 5);
    let ship_ms = env_u64("CST_LOCKBENCH_SHIP_MS", 20);
    let ship_batch = env_u64("CST_LOCKBENCH_SHIP_BATCH", 64) as usize;
    let ino_in_memory = std::env::var("CST_LOCKBENCH_INO").is_ok_and(|v| v == "memory");
    let off_list = std::env::var("CST_LOCKBENCH_OFF").unwrap_or_default();
    let off = |what: &str| off_list.split(',').any(|w| w == what);

    let dir = bench_dir("model");
    let meta = Arc::new(Meta::open(&dir).unwrap());
    let now_ms = constellation_fs_core::types::now_ns() / 1_000_000;
    let retention_ms = 900_000i64;
    // Rows of the other nodes' ops, recorded just now; each prune round adds
    // a quarter of the table's size in rows that are past retention.
    fill_completed(&meta, 9, n, 0, now_ms);
    for i in 0..backlog {
        execute(
            &meta,
            &create_op(format!("b{i}"), (5 << 40) | i),
            Some(Rid {
                node: 5,
                incarnation: 1,
                seq: i,
            }),
        )
        .unwrap();
    }
    // Forwarding peers' unacknowledged window.
    if !off("recent") {
        for seq in 0..4096u64 {
            let rid = Rid {
                node: 6,
                incarnation: 1,
                seq,
            };
            execute(
                &meta,
                &create_op(format!("p{seq}"), (6 << 40) | seq),
                Some(rid),
            )
            .unwrap();
            meta.remember_outcome(rid, &[]);
        }
    }

    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();
    let ship_us: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    // (start µs since t0, latency µs, 0 = alloc+create, 1 = unlink)
    let ops: Arc<Mutex<Vec<(u64, u64, u8)>>> = Arc::new(Mutex::new(Vec::new()));

    let mut handles = Vec::new();
    for w in 0..workers {
        let (meta, stop, ops) = (meta.clone(), stop.clone(), ops.clone());
        handles.push(std::thread::spawn(move || {
            let mut mine = Vec::new();
            let mut seq = 0u64;
            while !stop.load(Relaxed) {
                seq += 1;
                let name = format!("w{w}-{seq}");
                let rid = |s: u64| Rid {
                    node: 100 + w,
                    incarnation: 1,
                    seq: s,
                };
                let a = Instant::now();
                let ino = if ino_in_memory {
                    (60 + w) << 40 | seq
                } else {
                    meta.allocate_ino(ROOT_INO).unwrap()
                };
                execute(&meta, &create_op(name.clone(), ino), Some(rid(seq * 2))).unwrap();
                let b = Instant::now();
                execute(
                    &meta,
                    &MutateOp::Unlink {
                        parent: ROOT_INO,
                        name,
                    },
                    Some(rid(seq * 2 + 1)),
                )
                .unwrap();
                let c = Instant::now();
                mine.push(((a - t0).as_micros() as u64, (b - a).as_micros() as u64, 0u8));
                mine.push(((b - t0).as_micros() as u64, (c - b).as_micros() as u64, 1u8));
                std::thread::sleep(Duration::from_micros(think));
            }
            ops.lock().unwrap().extend(mine);
        }));
    }
    if !off("ship") {
        let (meta, stop, ship_us) = (meta.clone(), stop.clone(), ship_us.clone());
        handles.push(std::thread::spawn(move || {
            let mut applied = 0u64;
            while !stop.load(Relaxed) {
                let rows = meta.take_journal(ship_batch).unwrap();
                let seqs: Vec<u64> = rows.into_iter().map(|(s, _)| s).collect();
                if !seqs.is_empty() {
                    applied += 1;
                    let t = Instant::now();
                    meta.ack_journal_rows_at(&seqs, applied).unwrap();
                    ship_us.lock().unwrap().push(t.elapsed().as_micros() as u64);
                }
                std::thread::sleep(Duration::from_millis(ship_ms));
            }
        }));
    }
    let mut prune_us = Vec::new();
    let mut windows = Vec::new();
    let mut pruned_total = 0u64;
    let end = t0 + Duration::from_secs(secs);
    let mut next_prune = t0 + Duration::from_secs(prune_every);
    let mut round = 0u64;
    while Instant::now() < end {
        std::thread::sleep(Duration::from_millis(20));
        if off("prune") || Instant::now() < next_prune {
            continue;
        }
        // Age a quarter of the table, as the clock does over a quarter of
        // the window. The rows are written in chunks, so the workers see the
        // fill as any other writer's work, not as a hold.
        round += 1;
        let stale = n * stale_pct / 100;
        fill_completed(&meta, 20 + round, stale, stale, now_ms);
        let a = Instant::now();
        let pruned = if off("prunecall") {
            0
        } else {
            meta.prune_completed(now_ms + retention_ms + 1_000, retention_ms)
                .unwrap()
        };
        let b = Instant::now();
        pruned_total += pruned;
        windows.push(((a - t0).as_micros() as u64, (b - t0).as_micros() as u64));
        prune_us.push((b - a).as_micros() as u64);
        next_prune = Instant::now() + Duration::from_secs(prune_every);
    }
    stop.store(true, Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    let ops = ops.lock().unwrap();
    let secs_run = t0.elapsed().as_secs_f64();
    let unlinks: Vec<&(u64, u64, u8)> = ops.iter().filter(|o| o.2 == 1).collect();
    let mut all: Vec<u64> = unlinks.iter().map(|o| o.1).collect();
    let slow = all.iter().filter(|&&l| l >= 50_000).count();
    let mut in_prune: Vec<u64> = ops
        .iter()
        .filter(|o| windows.iter().any(|&(a, b)| o.0 < b && o.0 + o.1 > a))
        .map(|o| o.1)
        .collect();
    let mut created: Vec<u64> = ops.iter().filter(|o| o.2 == 0).map(|o| o.1).collect();
    let mut every: Vec<u64> = ops.iter().map(|o| o.1).collect();
    let slow_all = every.iter().filter(|&&l| l >= 50_000).count();
    let mut ships = ship_us.lock().unwrap().clone();
    eprintln!(
        "BENCH model off=[{off_list}] completed_rows={n} backlog_ops={backlog} workers={workers} secs={secs_run:.0}"
    );
    let extra = format!(
        "ops_per_s={:.0} unlinks_ge_50ms={slow} prunes={} pruned={pruned_total}",
        ops.len() as f64 / secs_run,
        prune_us.len()
    );
    line(
        "model_op",
        &mut every,
        &format!(
            "ops_ge_50ms={slow_all} ops_per_s={:.0}",
            ops.len() as f64 / secs_run
        ),
    );
    line("model_unlink", &mut all, &extra);
    line("model_alloc_create", &mut created, "");
    line("model_op_during_prune", &mut in_prune, "");
    line("model_prune_call", &mut prune_us, "");
    line("model_ship_ack", &mut ships, "");
    drop(ops);
    drop(meta);
    let _ = std::fs::remove_dir_all(&dir);
}
