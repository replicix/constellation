//! Plan 46 M1.2 step 5 (c3-spec D3): a successor re-ships its predecessor's
//! sealed tail verbatim, in one store transaction with the strand
//! (`Meta::reship_at_takeover`). A record that does not apply is
//! `TailDiverged` and nothing changes.

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{
    execute_mutate, BackupTx, LogRecord, Meta, MetaStore, MutateOp, PendingClass, Rid, RowCommit,
};
use constellation_types::Code;

const EPOCH_PREV: u64 = 1;

fn rid(node: u64, seq: u64) -> Rid {
    Rid {
        node,
        incarnation: 1,
        seq,
    }
}

fn ino(n: u64) -> u64 {
    (1 << 40) | n
}

fn create(parent: u64, name: &str, ino: u64) -> LogRecord {
    LogRecord::Create {
        parent,
        name: name.into(),
        ino,
        mode: 0o644,
        uid: 0,
        gid: 0,
        time_ns: 10,
    }
}

fn tx(first: u64, last: u64, records: Vec<LogRecord>) -> BackupTx {
    BackupTx {
        first,
        last,
        records,
        origin: (0, 0),
    }
}

fn raw_ns(meta: &Meta) -> Vec<(Vec<u8>, Vec<u8>)> {
    meta.read_consistent(|snap| meta.ns_dump_at(snap)).unwrap()
}

/// The takeover's re-ship of `txs` (the sealed tail of epoch 1, contiguous
/// through its last row) under epoch 2.
fn reship(
    meta: &Meta,
    txs: &[BackupTx],
) -> Result<constellation_meta::ReshipReport, constellation_meta::MetaError> {
    assert!(meta.backup_append(EPOCH_PREV, txs).is_ok());
    let through = txs.iter().map(|t| t.last).max().unwrap_or(0);
    let proof = Default::default();
    let carried = Default::default();
    meta.reship_at_takeover(
        Some(constellation_meta::StrandArgs {
            epoch: 2,
            own_gens: &[],
            abandoned: None,
            carried: &carried,
            proof: &proof,
        }),
        Some(constellation_meta::TailArgs {
            epoch: EPOCH_PREV,
            through,
            config_version: 3,
            class: None,
        }),
    )
}

/// A tail transaction with an `Atime` and a `Refused` is journaled exactly,
/// in its order: the records as the sealer holds them, behind the identity
/// the re-ship gives it. (Before: `Atime` dropped, `Refused` moved behind
/// the rows.)
#[test]
fn a_sealed_tail_is_applied_verbatim() {
    let meta = Meta::open_in_memory().unwrap();
    let recs = vec![
        create(ROOT_INO, "a", ino(1)),
        LogRecord::Atime {
            ino: ino(1),
            atime_ns: 20,
            time_ns: 21,
        },
        LogRecord::Refused {
            rid: rid(2, 2),
            code: Code::Exists,
        },
        LogRecord::Completed { rid: rid(2, 1) },
    ];
    let report = reship(&meta, &[tx(1, 4, recs.clone())]).unwrap();
    assert_eq!(report.tail_applied, 1);
    assert!(meta.lookup(ROOT_INO, "a").unwrap().is_some());
    let journal = meta.journal_txs_from(1, 1000).unwrap();
    assert_eq!(journal.len(), 1, "{journal:?}");
    let mut want = vec![LogRecord::TailOf {
        epoch: EPOCH_PREV,
        jseq: 1,
    }];
    want.extend(recs);
    assert_eq!(journal[0].records, want);
    assert!(meta.completed_position(rid(2, 1)).unwrap().is_some());
    assert!(meta.completed_outcome(rid(2, 2)).unwrap().is_some());
    // Kept as the member copy of its epoch, and the tail is cleared.
    assert!(meta.member_copy(EPOCH_PREV).unwrap().is_some());
    assert!(meta.backup_tail(EPOCH_PREV).unwrap().is_empty());
}

/// A tail record whose parent is missing does not apply: `TailDiverged`,
/// the journal and the namespace unchanged, the tail still held.
#[test]
fn a_diverging_tail_applies_nothing() {
    let meta = Meta::open_in_memory().unwrap();
    let before = raw_ns(&meta);
    let good = tx(
        1,
        2,
        vec![
            create(ROOT_INO, "a", ino(1)),
            LogRecord::Completed { rid: rid(2, 1) },
        ],
    );
    // Its parent (an inode no one created) is missing.
    let bad = tx(
        3,
        4,
        vec![
            create(ino(99), "b", ino(2)),
            LogRecord::Completed { rid: rid(2, 2) },
        ],
    );
    let err = reship(&meta, &[good.clone(), bad.clone()]).unwrap_err();
    assert!(
        matches!(
            err,
            constellation_meta::MetaError::TailDiverged {
                prev: EPOCH_PREV,
                first: 3,
                last: 4,
                ..
            }
        ),
        "{err}"
    );
    assert_eq!(raw_ns(&meta), before, "not even the first row stayed");
    assert_eq!(meta.journal_len().unwrap(), 0);
    assert!(meta.completed_position(rid(2, 1)).unwrap().is_none());
    assert_eq!(meta.backup_tail(EPOCH_PREV).unwrap(), vec![good, bad]);
    assert!(meta.member_copy(EPOCH_PREV).unwrap().is_none());
}

/// Strict under the tail: a durable create of an inode this replica already
/// holds is not "the same record applied again" (the tolerant apply's
/// reading, a silent `Done`) but a divergence: the rid is not done here, so
/// the row is not one already re-shipped. Nothing changes. (Before: the
/// create was swallowed and the tail applied.)
#[test]
fn a_create_of_an_existing_inode_diverges() {
    let meta = Meta::open_in_memory().unwrap();
    execute_mutate(
        &meta,
        &MutateOp::Create {
            parent: ROOT_INO,
            name: "x".into(),
            ino: ino(7),
            mode: 0o644,
            uid: 0,
            gid: 0,
        },
        Some(rid(9, 1)),
    )
    .unwrap();
    let before = raw_ns(&meta);
    let journal = meta.journal_len().unwrap();
    let bad = tx(
        1,
        2,
        vec![
            create(ROOT_INO, "y", ino(7)),
            LogRecord::Completed { rid: rid(2, 1) },
        ],
    );
    let err = reship(&meta, std::slice::from_ref(&bad)).unwrap_err();
    assert!(
        matches!(err, constellation_meta::MetaError::TailDiverged { .. }),
        "{err}"
    );
    assert_eq!(raw_ns(&meta), before);
    assert_eq!(meta.journal_len().unwrap(), journal);
    assert_eq!(meta.backup_tail(EPOCH_PREV).unwrap(), vec![bad]);
}

/// The one way a row of the tail may already be here is its rid done (the
/// log carried it): it is skipped and counted (`tail_done`, which the core
/// exports as `reship.tail_rid_done` and the sim fails a seed for), not
/// re-created and not a divergence.
#[test]
fn a_row_whose_rid_is_done_is_skipped_and_counted() {
    let meta = Meta::open_in_memory().unwrap();
    execute_mutate(
        &meta,
        &MutateOp::Create {
            parent: ROOT_INO,
            name: "x".into(),
            ino: ino(7),
            mode: 0o644,
            uid: 0,
            gid: 0,
        },
        Some(rid(2, 1)),
    )
    .unwrap();
    let journal = meta.journal_len().unwrap();
    let same = tx(
        1,
        2,
        vec![
            create(ROOT_INO, "x", ino(7)),
            LogRecord::Completed { rid: rid(2, 1) },
        ],
    );
    let report = reship(&meta, &[same]).unwrap();
    assert_eq!((report.tail_done, report.tail_applied), (1, 0));
    assert_eq!(meta.journal_len().unwrap(), journal);
}

/// This node's own unshipped row of the old tenure, journaled as the holder
/// of epoch 1 (an ungated row: the strand rolls it back and queues its
/// replay).
fn successor_with_an_own_row() -> Meta {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    {
        let _c = PendingClass::set(RowCommit::LONE);
        execute_mutate(
            &meta,
            &MutateOp::Create {
                parent: ROOT_INO,
                name: "own".into(),
                ino: ino(7),
                mode: 0o644,
                uid: 0,
                gid: 0,
            },
            Some(rid(3, 1)),
        )
        .unwrap();
    }
    meta.set_holder_epoch(0);
    assert_eq!(txs_in_journal(&meta), 1);
    meta
}

fn txs_in_journal(meta: &Meta) -> usize {
    // (A rolled-back row leaves a hole: its seqs, no records.)
    meta.journal_txs_from(1, 1000)
        .unwrap()
        .iter()
        .filter(|t| !t.records.is_empty())
        .count()
}

/// Atomicity (D3, V3): a tail that fails on its last transaction leaves no
/// strand behind. The successor's own unshipped row is still in its
/// journal, nothing is queued for replay, and the namespace is as it was; a
/// crash between the two transactions of the old sequence left the strand
/// without the tail it made room for. With a tail that applies, both land.
#[test]
fn a_failing_tail_leaves_no_strand_without_its_tail() {
    let meta = successor_with_an_own_row();
    let before = raw_ns(&meta);
    let good = tx(
        1,
        2,
        vec![
            create(ROOT_INO, "b", ino(2)),
            LogRecord::Completed { rid: rid(2, 2) },
        ],
    );
    let bad = tx(
        3,
        4,
        vec![
            create(ino(99), "c", ino(3)),
            LogRecord::Completed { rid: rid(2, 3) },
        ],
    );
    assert!(reship(&meta, &[good.clone(), bad]).is_err());
    assert_eq!(raw_ns(&meta), before);
    assert_eq!(txs_in_journal(&meta), 1, "the strand did not land");
    assert!(meta.pending_replays().unwrap().is_empty());
    assert!(meta.lookup(ROOT_INO, "own").unwrap().is_some());
    assert!(meta.lookup(ROOT_INO, "b").unwrap().is_none());
    // The same successor with a tail that applies: strand and tail together.
    let meta2 = successor_with_an_own_row();
    let report = reship(&meta2, &[good]).unwrap();
    assert_eq!(report.tail_applied, 1);
    assert_eq!(report.stranded.locals, 1);
    assert_eq!(txs_in_journal(&meta2), 1, "the tail's row only");
    let queued = meta2.pending_replays().unwrap();
    assert_eq!(
        queued.iter().map(|q| q.rid).collect::<Vec<_>>(),
        [rid(3, 1)]
    );
    assert!(meta2.lookup(ROOT_INO, "b").unwrap().is_some());
}

/// c3-spec §10: the one-transaction re-ship holds the writer lock for the
/// whole tail. Applies a tail of `RESHIP_ROWS` transactions (default
/// 10,000) while a second thread writes to the same store as fast as it
/// can, and prints the re-ship's duration (the hold) and the longest wait
/// of the concurrent writer. Run in release:
/// `cargo test --release -p constellation-meta --test reship -- --ignored --nocapture`.
#[test]
#[ignore = "a measurement, not a check: run in release with --nocapture"]
fn reship_lock_hold_over_a_large_tail() {
    let rows: u64 = std::env::var("RESHIP_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);
    let meta = std::sync::Arc::new(Meta::open_in_memory().unwrap());
    let txs: Vec<BackupTx> = (0..rows)
        .map(|i| {
            tx(
                2 * i + 1,
                2 * i + 2,
                vec![
                    create(ROOT_INO, &format!("t{i}"), ino(1000 + i)),
                    LogRecord::Completed { rid: rid(2, i + 1) },
                ],
            )
        })
        .collect();
    meta.backup_append(EPOCH_PREV, &txs).unwrap();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let (meta, stop) = (meta.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut worst = std::time::Duration::ZERO;
            let mut n = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let t = std::time::Instant::now();
                execute_mutate(
                    &meta,
                    &MutateOp::Create {
                        parent: ROOT_INO,
                        name: format!("w{n}"),
                        ino: ino(900_000_000 + n),
                        mode: 0o644,
                        uid: 0,
                        gid: 0,
                    },
                    Some(rid(9, n + 1)),
                )
                .unwrap();
                worst = worst.max(t.elapsed());
                n += 1;
            }
            (n, worst)
        })
    };
    std::thread::sleep(std::time::Duration::from_millis(50));
    let proof = Default::default();
    let carried = Default::default();
    let t = std::time::Instant::now();
    let report = meta
        .reship_at_takeover(
            Some(constellation_meta::StrandArgs {
                epoch: 2,
                own_gens: &[],
                abandoned: None,
                carried: &carried,
                proof: &proof,
            }),
            Some(constellation_meta::TailArgs {
                epoch: EPOCH_PREV,
                through: 2 * rows,
                config_version: 3,
                class: None,
            }),
        )
        .unwrap();
    let hold = t.elapsed();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let (writes, worst) = writer.join().unwrap();
    assert_eq!(report.tail_applied as u64, rows);
    eprintln!(
        "reship: {rows} tail rows in {hold:?} ({:?} per row); a concurrent writer did {writes} writes, longest {worst:?}",
        hold / rows as u32
    );
}

/// Plan 46 M1.4 step 4: a hold member's copy of what it acknowledged is a
/// committed streamed transaction. When the member claims the lease after
/// the owner's retire (no tail names the row: the owner is gone), the strand
/// rolls the row back and queues it as a replay by rid, to ship as the
/// member's own; taken out silently (the rule for a row a tail re-applies),
/// the row the owner answered as held was lost with the owner.
#[test]
fn a_member_keeps_a_row_it_acknowledged_when_no_tail_names_it() {
    let meta = Meta::open_in_memory().unwrap();
    let recs = vec![
        create(ROOT_INO, "held", ino(1)),
        LogRecord::Completed { rid: rid(1, 1) },
    ];
    meta.install_streamed_committed(EPOCH_PREV, 1, 1, 1, &recs, true, (0, 0))
        .unwrap();
    assert!(meta.lookup(ROOT_INO, "held").unwrap().is_some());
    let proof = Default::default();
    let carried = Default::default();
    meta.reship_at_takeover(
        Some(constellation_meta::StrandArgs {
            epoch: 2,
            own_gens: &[],
            abandoned: None,
            carried: &carried,
            proof: &proof,
        }),
        None,
    )
    .unwrap();
    let queued = meta.pending_replays().unwrap();
    assert_eq!(queued.len(), 1, "{queued:?}");
    assert_eq!(queued[0].rid, rid(1, 1));
    assert!(queued[0].refused.is_none());
    assert!(matches!(&queued[0].op, MutateOp::Records { records } if *records == recs));
}

/// The same row with a tail that names it: the tail re-applies it, the
/// member's entry is taken out and nothing is queued (applied once).
#[test]
fn a_member_row_a_tail_names_is_applied_once() {
    let meta = Meta::open_in_memory().unwrap();
    let recs = vec![
        create(ROOT_INO, "held", ino(1)),
        LogRecord::Completed { rid: rid(1, 1) },
    ];
    meta.install_streamed_committed(EPOCH_PREV, 1, 1, 1, &recs, true, (0, 0))
        .unwrap();
    let report = reship(&meta, &[tx(1, 1, recs)]).unwrap();
    assert_eq!(report.tail_applied, 1);
    assert!(meta.pending_replays().unwrap().is_empty());
    assert!(meta.lookup(ROOT_INO, "held").unwrap().is_some());
}

/// Plan 46 M1.4 step 4 (review SHOULD-2): only the transactions the takeover
/// re-applies name a member copy. One past the sealed prefix (`through`) is
/// counted `tail_beyond_gap` and never applied, so a copy named only by it
/// is the only one left: it strands and its rid replays.
#[test]
fn a_member_copy_named_only_by_a_tx_past_the_gap_survives_as_a_replay() {
    let meta = Meta::open_in_memory().unwrap();
    let first = vec![
        create(ROOT_INO, "first", ino(1)),
        LogRecord::Completed { rid: rid(1, 1) },
    ];
    let beyond = vec![
        create(ROOT_INO, "beyond", ino(2)),
        LogRecord::Completed { rid: rid(1, 2) },
    ];
    meta.install_streamed_committed(EPOCH_PREV, 1, 2, 2, &beyond, true, (0, 0))
        .unwrap();
    let txs = [tx(1, 1, first), tx(2, 2, beyond)];
    assert!(meta.backup_append(EPOCH_PREV, &txs).is_ok());
    let proof = Default::default();
    let carried = Default::default();
    let report = meta
        .reship_at_takeover(
            Some(constellation_meta::StrandArgs {
                epoch: 2,
                own_gens: &[],
                abandoned: None,
                carried: &carried,
                proof: &proof,
            }),
            Some(constellation_meta::TailArgs {
                epoch: EPOCH_PREV,
                through: 1,
                config_version: 3,
                class: None,
            }),
        )
        .unwrap();
    assert_eq!(report.tail_applied, 1);
    assert_eq!(report.tail_beyond_gap, 1);
    let queued = meta.pending_replays().unwrap();
    assert_eq!(queued.len(), 1, "{queued:?}");
    assert_eq!(queued[0].rid, rid(1, 2));
}
