//! Plan 30 M2: a `Completed { rid }` marker belongs to the op `execute`
//! ran for it, never to a journal write another thread made at the same
//! time (a snapshot row here). The rid travels through a thread-local, so
//! the concurrent writer's transaction cannot take it.
//!
//! What `execute` *returns* is not a transaction, though: it is
//! `peek_journal_after(tip-before-the-op)` — every journal row appended
//! since the call started, by any thread (`mutate::execute_inner`). With a
//! concurrent writer on the same `Meta` that list legitimately ends in the
//! other thread's rows, so "the completion is last" is not the guarantee
//! and asserting it is a race. The guarantee is *where the marker sits*:
//! `journal::append_tx` takes the thread-local rid inside the op's own
//! `fjall` write transaction, which holds the single-writer lock for its
//! whole lifetime — so `Completed { rid }` gets the seq directly after the
//! op's own record, for this rid and no other, and the concurrent writer's
//! rows can only land entirely before or entirely after that pair. That
//! adjacency is what production reads the marker by
//! (`engine::coop::fresh::written_chunks` attributes a transaction to the
//! rid whose completion follows its first record), so it is what this test
//! asserts.

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{execute_mutate, LogRecord, Meta, MetaStore, MutateOp, Rid, SnapshotRow};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[test]
fn completion_marker_never_attaches_to_a_concurrent_writer() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    const OPS: u64 = 300;
    // The concurrent writer runs for as long as the op loop does, rather
    // than for a fixed count: a fixed 300 snapshots finish in a few
    // milliseconds, so most ops used to execute with nobody else writing
    // and never exercised the race at all (measured: 2 of 300 ops saw a
    // foreign row in a typical run).
    let stop = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicU64::new(0));
    let snapshots = {
        let meta = meta.clone();
        let stop = stop.clone();
        let written = written.clone();
        std::thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Acquire) {
                meta.record_snapshot(&SnapshotRow::new(
                    format!("s{i}"),
                    "/",
                    format!("s{i}"),
                    "00",
                    0,
                ))
                .unwrap();
                i += 1;
                written.store(i, Ordering::Release);
            }
        })
    };
    // Don't start measuring until the writer is actually running.
    let deadline = Instant::now() + Duration::from_secs(60);
    while written.load(Ordering::Acquire) == 0 {
        assert!(
            Instant::now() < deadline,
            "the concurrent journal writer never got scheduled"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let mut foreign_rows = 0usize;
    for seq in 0..OPS {
        let ino = meta.allocate_ino(ROOT_INO).unwrap();
        let op = MutateOp::Create {
            parent: ROOT_INO,
            name: format!("f{seq}"),
            ino,
            mode: 0o644,
            uid: 0,
            gid: 0,
        };
        let rid = Rid {
            node: 1,
            incarnation: 1,
            seq,
        };
        let records = execute_mutate(&meta, &op, Some(rid)).unwrap();
        let completions: Vec<(usize, Rid)> = records
            .iter()
            .enumerate()
            .filter_map(|(i, r)| match r {
                LogRecord::Completed { rid } => Some((i, *rid)),
                _ => None,
            })
            .collect();
        assert_eq!(
            completions.len(),
            1,
            "op {seq} saw a missing, foreign or duplicated completion: {records:?}"
        );
        let (at, carried) = completions[0];
        assert_eq!(
            carried, rid,
            "op {seq} did not carry its own completion: {records:?}"
        );
        assert!(
            at > 0
                && matches!(&records[at - 1], LogRecord::Create { name, .. } if *name == format!("f{seq}")),
            "op {seq}'s completion did not ride in its own transaction: {records:?}"
        );
        // Everything else in the list is the snapshot thread's — rows that
        // committed between this op's starting tip and the read, before or
        // after the op's own pair. None of them took the rid.
        for (i, r) in records.iter().enumerate() {
            if i == at || i == at - 1 {
                continue;
            }
            assert!(
                matches!(r, LogRecord::SnapCreate2 { .. }),
                "op {seq} returned a record neither its own nor the concurrent writer's: {r:?}"
            );
        }
        // The durable half of the same transaction: the `completed` row
        // naming this rid, written by the very `append_tx` call that
        // appended the marker above.
        assert!(
            meta.completed_position(rid).unwrap().is_some(),
            "op {seq} journaled its completion without recording the rid"
        );
        foreign_rows += records.len() - 2;
    }
    stop.store(true, Ordering::Release);
    snapshots.join().unwrap();
    // Non-vacuity: at least one op's window has to have contained one of
    // the other thread's rows, or the interleaving this test exists for
    // never happened and the assertions above proved nothing.
    assert!(
        foreign_rows > 0,
        "no op ran concurrently with the journal writer"
    );

    let journal: Vec<LogRecord> = meta
        .take_journal(usize::MAX)
        .unwrap()
        .into_iter()
        .map(|(_, r)| r)
        .collect();
    let completions = journal
        .iter()
        .filter(|r| matches!(r, LogRecord::Completed { .. }))
        .count();
    assert_eq!(completions as u64, OPS, "every op completed exactly once");
    for pair in journal.windows(2) {
        if let LogRecord::Completed { .. } = &pair[1] {
            assert!(
                matches!(pair[0], LogRecord::Create { .. }),
                "a completion followed a non-op record: {:?}",
                pair[0]
            );
        }
    }
}
