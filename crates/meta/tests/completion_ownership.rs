//! Plan 30 M2: a `Completed { rid }` marker belongs to the op `execute`
//! ran for it, never to a journal write another thread made at the same
//! time (a snapshot row here). The rid travels through a thread-local, so
//! the concurrent writer's transaction cannot take it. The op's outcome
//! is collected the same way (`journal::OwnRows`), so it is exactly the
//! op's own transaction — `[Create, Completed { rid }]` — and never one of
//! the other thread's rows, however they interleave.
//!
//! In the journal itself the guarantee is *where the marker sits*:
//! `journal::append_tx` takes the thread-local rid inside the op's own
//! `fjall` write transaction, which holds the single-writer lock for its
//! whole lifetime — so `Completed { rid }` gets the seq directly after the
//! op's own record, for this rid and no other, and the concurrent writer's
//! rows can only land entirely before or entirely after that pair. That
//! adjacency is what production reads the marker by
//! (`engine::coop::fresh::written_chunks` attributes a transaction to the
//! rid whose completion follows its first record), so the final journal
//! scan asserts it per op.

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
        // Its outcome is its own transaction: no snapshot row committed
        // meanwhile rides along.
        assert!(
            matches!(
                records.as_slice(),
                [LogRecord::Create { name, .. }, LogRecord::Completed { rid: r }]
                    if *r == rid && *name == format!("f{seq}")
            ),
            "op {seq}'s outcome is not its own transaction: {records:?}"
        );
        // The durable half of the same transaction: the `completed` row
        // naming this rid, written by the very `append_tx` call that
        // appended the marker above.
        assert!(
            meta.completed_position(rid).unwrap().is_some(),
            "op {seq} journaled its completion without recording the rid"
        );
    }
    stop.store(true, Ordering::Release);
    snapshots.join().unwrap();

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
    // Per-pair adjacency: each completion directly follows its own op's
    // record, under that op's rid.
    for pair in journal.windows(2) {
        if let LogRecord::Completed { rid } = &pair[1] {
            assert!(
                matches!(&pair[0], LogRecord::Create { name, .. } if *name == format!("f{}", rid.seq)),
                "completion {rid:?} did not follow its own op's record: {:?}",
                pair[0]
            );
        }
    }
    // Non-vacuity: the snapshot writer's rows have to have landed between
    // two ops' pairs at least once — i.e. some op ran while the other
    // thread was writing — or the interleaving this test exists for never
    // happened and the outcome assertions above proved nothing.
    let mut between_ops = 0usize;
    let mut pending = 0usize;
    let mut seen_op = false;
    for r in &journal {
        match r {
            LogRecord::SnapCreate2 { .. } if seen_op => pending += 1,
            LogRecord::Create { .. } => {
                between_ops += pending;
                pending = 0;
                seen_op = true;
            }
            _ => {}
        }
    }
    assert!(
        between_ops > 0,
        "no snapshot row landed between two ops: no op ran concurrently with the journal writer"
    );
}
