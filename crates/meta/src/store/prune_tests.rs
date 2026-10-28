//! `Meta::prune_completed`: the writer lock is held for a bounded batch of
//! rows at a time, never for the whole table, and the exactly-once window
//! of a rid (the `completed` row dedup answers from) ends where it did.

use crate::rid::Rid;
use crate::store::prune_hook::{self, At};
use crate::store::Meta;
use fjall::Readable;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

const NOW: i64 = 10_000_000;
const RETENTION: i64 = 60_000;
/// Past retention at `NOW`.
const OLD: i64 = NOW - RETENTION - 1;
/// Inside the window at `NOW`.
const FRESH: i64 = NOW - RETENTION + 1;

fn rid(seq: u64) -> Rid {
    Rid {
        node: 9,
        incarnation: 1,
        seq,
    }
}

fn put(meta: &Meta, rows: impl IntoIterator<Item = (u64, i64)>) {
    let mut tx = meta.db.write_tx();
    for (seq, at) in rows {
        tx.insert(
            &meta.completed,
            rid(seq).to_key(),
            Meta::encode_completed_row(seq + 1, at),
        );
    }
    tx.commit().unwrap();
}

fn has(meta: &Meta, seq: u64) -> bool {
    meta.completed_position(rid(seq)).unwrap().is_some()
}

/// Fail-before: the prune read and removed the whole table in one write
/// transaction, so every writer of a root queued behind it for as long as
/// the table is long (80 ms at 270,000 rows, whole seconds on a root that
/// has served a busy cluster for the retention window). Counted by the
/// writer-lock acquisitions: one for the whole table before, one per batch
/// of at most 256 rows now.
#[test]
fn the_prune_takes_the_writer_lock_for_a_bounded_batch_at_a_time() {
    let meta = Meta::open_in_memory().unwrap();
    let rows = 5000u64;
    put(&meta, (0..rows).map(|s| (s, OLD)));
    let acquisitions = Arc::new(AtomicU64::new(0));
    {
        let acquisitions = acquisitions.clone();
        meta.set_writer_wait_hook_for_tests(Some(Arc::new(move || {
            acquisitions.fetch_add(1, Relaxed);
        })));
    }
    assert_eq!(meta.prune_completed(NOW, RETENTION).unwrap(), rows);
    meta.set_writer_wait_hook_for_tests(None);
    let batches = acquisitions.load(Relaxed);
    assert!(
        batches >= rows.div_ceil(256),
        "{rows} rows were removed in {batches} write transactions: more than 256 rows under one hold of the writer lock"
    );
    assert!(!has(&meta, 0) && !has(&meta, rows - 1));
}

/// The writer lock is not handed to the longest waiter (a thread that lets
/// go of it and asks again takes it back first), so batches of a prune in a
/// row would still keep another writer out for the whole of it: the prune
/// stays away between batches. A writer that asks all along is served
/// within a few batches' time, not after the last one.
#[test]
fn a_writer_is_served_while_the_prune_runs() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let rows = 60_000u64;
    put(&meta, (0..rows).map(|s| (s, OLD)));
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let probe = {
        let meta = meta.clone();
        let running = running.clone();
        std::thread::spawn(move || {
            let mut worst = std::time::Duration::ZERO;
            let mut n = 0u64;
            while running.load(Relaxed) {
                let t = std::time::Instant::now();
                meta.kv_set("probe", &n.to_string()).unwrap();
                worst = worst.max(t.elapsed());
                n += 1;
                std::thread::sleep(std::time::Duration::from_micros(300));
            }
            (worst, n)
        })
    };
    let t = std::time::Instant::now();
    assert_eq!(meta.prune_completed(NOW, RETENTION).unwrap(), rows);
    let took = t.elapsed();
    running.store(false, Relaxed);
    let (worst, writes) = probe.join().unwrap();
    // Relative to the prune's own time, so a loaded host slows both: one
    // transaction for the whole prune kept the writer out for all of it.
    assert!(
        worst * 4 < took,
        "a writer waited {worst:?} for the lock during a prune of {took:?} ({writes} writes got in)"
    );
}

/// A row that a segment recorded again between the prune's snapshot and
/// its removal (the rid's window starts over with the fresh `recorded_at`)
/// is still the answer a retry of that rid gets: the removal decides on the
/// row as it is under the lock, not as the snapshot had it.
#[test]
fn a_row_recorded_again_after_the_snapshot_is_not_removed() {
    let meta = Meta::open_in_memory().unwrap();
    put(&meta, (0..600).map(|s| (s, OLD)));
    // One in the first batch, one in the last.
    prune_hook::set(Some(Box::new(|meta, at| {
        if at == At::Candidates {
            put(meta, [(3, FRESH), (599, FRESH)]);
        }
    })));
    let removed = meta.prune_completed(NOW, RETENTION).unwrap();
    prune_hook::set(None);
    assert_eq!(removed, 598);
    assert!(has(&meta, 3) && has(&meta, 599));
    assert!(!has(&meta, 0) && !has(&meta, 4) && !has(&meta, 598));
}

/// Rows written after the snapshot are not the prune's to look at, whatever
/// their time says (a re-applied segment's row for an old rid): they are
/// there for the next prune, not removed unseen, and not counted.
#[test]
fn a_row_written_after_the_snapshot_waits_for_the_next_prune() {
    let meta = Meta::open_in_memory().unwrap();
    put(&meta, (0..10).map(|s| (s, OLD)));
    prune_hook::set(Some(Box::new(|meta, at| {
        if at == At::Candidates {
            put(meta, [(100, OLD), (101, FRESH)]);
        }
    })));
    assert_eq!(meta.prune_completed(NOW, RETENTION).unwrap(), 10);
    prune_hook::set(None);
    assert!(has(&meta, 100) && has(&meta, 101));
    assert_eq!(meta.prune_completed(NOW, RETENTION).unwrap(), 1);
    assert!(!has(&meta, 100) && has(&meta, 101));
}

/// A row removed by someone else after the snapshot is not counted twice,
/// and the fresh rows around stale ones stay, across batch boundaries.
#[test]
fn the_window_ends_where_it_did_and_a_vanished_row_is_not_counted() {
    let meta = Meta::open_in_memory().unwrap();
    // Every third row is inside the window.
    put(
        &meta,
        (0..1000).map(|s| (s, if s % 3 == 0 { FRESH } else { OLD })),
    );
    prune_hook::set(Some(Box::new(|meta, at| {
        if at == At::Candidates {
            let mut tx = meta.db.write_tx();
            tx.remove(&meta.completed, rid(1).to_key());
            tx.remove(&meta.completed, rid(998).to_key());
            tx.commit().unwrap();
        }
    })));
    let removed = meta.prune_completed(NOW, RETENTION).unwrap();
    prune_hook::set(None);
    let stale = (0..1000u64).filter(|s| s % 3 != 0).count() as u64;
    assert_eq!(removed, stale - 2);
    for s in 0..1000u64 {
        assert_eq!(has(&meta, s), s % 3 == 0, "row {s}");
    }
}

/// A row too short to carry a time is never removed, with any batching.
#[test]
fn a_row_without_a_time_stays() {
    let meta = Meta::open_in_memory().unwrap();
    put(&meta, (0..300).map(|s| (s, OLD)));
    {
        let mut tx = meta.db.write_tx();
        tx.insert(&meta.completed, rid(1000).to_key(), vec![1, 2, 3]);
        tx.commit().unwrap();
    }
    assert_eq!(meta.prune_completed(NOW, RETENTION).unwrap(), 300);
    let r = meta.db.read_tx();
    assert!(r
        .get(&meta.completed, rid(1000).to_key())
        .unwrap()
        .is_some());
}
