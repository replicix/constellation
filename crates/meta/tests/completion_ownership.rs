//! Plan 30 M2: a `Completed { rid }` marker belongs to the op `execute`
//! ran for it, never to a journal write another thread made at the same
//! time (a snapshot row here). The rid travels through a thread-local, so
//! the concurrent writer's transaction cannot take it.

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{execute_mutate, LogRecord, Meta, MetaStore, MutateOp, Rid, SnapshotRow};
use std::sync::Arc;

#[test]
fn completion_marker_never_attaches_to_a_concurrent_writer() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    const OPS: u64 = 300;
    let snapshots = {
        let meta = meta.clone();
        std::thread::spawn(move || {
            for i in 0..OPS {
                meta.record_snapshot(&SnapshotRow::new(
                    format!("s{i}"),
                    "/",
                    format!("s{i}"),
                    "00",
                    0,
                ))
                .unwrap();
            }
        })
    };
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
        assert!(
            matches!(records.last(), Some(LogRecord::Completed { rid: r }) if *r == rid),
            "op {seq} did not carry its own completion: {records:?}"
        );
    }
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
