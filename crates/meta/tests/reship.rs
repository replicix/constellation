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
            marker: &[],
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

/// ADR-51: a later epoch's tail may carry the re-ship of an earlier epoch's
/// row (its holder took the row over, then died holding it). The takeover
/// journals the row once, under the first epoch, and the second copy is a
/// dedup by identity: a normal sight, counted as `tail_dedup` and never as
/// `tail_done` (an outcome of the rid the log carried under another
/// identity, which the sim fails a seed for).
#[test]
fn a_row_two_epochs_tails_carry_is_a_dedup_not_a_done_rid() {
    let meta = Meta::open_in_memory().unwrap();
    let recs = vec![
        create(ROOT_INO, "x", ino(7)),
        LogRecord::Completed { rid: rid(2, 1) },
    ];
    let mut again = vec![LogRecord::TailOf {
        epoch: EPOCH_PREV,
        jseq: 1,
    }];
    again.extend(recs.clone());
    assert!(meta.backup_append(EPOCH_PREV, &[tx(1, 2, recs)]).is_ok());
    assert!(meta.backup_append(2, &[tx(1, 3, again)]).is_ok());
    let report = meta
        .reship_epochs_at_takeover(
            None,
            &[
                constellation_meta::TailArgs {
                    epoch: EPOCH_PREV,
                    through: 2,
                    config_version: 3,
                    class: None,
                },
                constellation_meta::TailArgs {
                    epoch: 2,
                    through: 3,
                    config_version: 3,
                    class: None,
                },
            ],
        )
        .unwrap();
    assert_eq!(
        (report.tail_applied, report.tail_dedup, report.tail_done),
        (1, 1, 0),
        "{report:?}"
    );
    assert_eq!(txs_in_journal(&meta), 1);
}

/// A node that takes the lease again after its own tenure (a claimant asked
/// for its tail, the claim failed, it won the lease itself) reads its own
/// sealed tail back while its own journal still holds the very rows (a root's
/// append of a delegate's row stays through a takeover). A tail row that is
/// that journal row, the same epoch and journal seq, is not an outcome "from
/// the log under another identity": it stays where it is, to ship as the
/// node's own, and is counted as itself (`tail_same_row`), never as
/// `tail_done` (`far-placement` seeds 352 and 772).
#[test]
fn a_tail_row_that_is_the_nodes_own_journal_row_is_not_a_done_rid() {
    let meta = successor_with_an_own_row();
    let journal = meta.journal_len().unwrap();
    // The tail row is the journal's own row, record for record.
    let own = meta.journal_txs_from(1, 1000).unwrap();
    assert_eq!(own.len(), 1);
    let same = tx(own[0].first, own[0].last, own[0].records.clone());
    assert!(meta
        .backup_append(EPOCH_PREV, std::slice::from_ref(&same))
        .is_ok());
    let report = meta
        .reship_at_takeover(
            None,
            Some(constellation_meta::TailArgs {
                epoch: EPOCH_PREV,
                through: 2,
                config_version: 3,
                class: None,
            }),
        )
        .unwrap();
    assert_eq!(report.tail_done, 0, "{report:?}");
    assert_eq!((report.tail_same_row, report.tail_applied), (1, 0));
    assert_eq!(meta.journal_len().unwrap(), journal);
    assert_eq!(txs_in_journal(&meta), 1);
}

/// The same (epoch, journal seq) as the node's own row but other content is
/// not the node's own row: the successor's takeover fails with a diverged
/// tail instead of leaving it in place as itself.
#[test]
fn a_tail_row_that_only_shares_the_name_of_the_own_row_diverges() {
    let meta = successor_with_an_own_row();
    let own = meta.journal_txs_from(1, 1000).unwrap();
    let mut records = own[0].records.clone();
    // Same rid (so it is a "done" rid), another name.
    for r in records.iter_mut() {
        if let LogRecord::Create { name, .. } = r {
            *name = "other".into();
        }
    }
    assert_ne!(records, own[0].records, "the test did not change a record");
    let other = tx(own[0].first, own[0].last, records);
    assert!(meta
        .backup_append(EPOCH_PREV, std::slice::from_ref(&other))
        .is_ok());
    let journal = meta.journal_len().unwrap();
    let err = meta
        .reship_at_takeover(
            None,
            Some(constellation_meta::TailArgs {
                epoch: EPOCH_PREV,
                through: own[0].last,
                config_version: 3,
                class: None,
            }),
        )
        .expect_err("a tail of another content was taken for the own row");
    assert!(
        matches!(err, constellation_meta::MetaError::TailDiverged { .. }),
        "{err:?}"
    );
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
                marker: &[],
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
            marker: &[],
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

/// ADR-51: `promote_streamed` makes the speculative streamed copies of one
/// source's epoch, through a seq, committed in both rows of the entry. A
/// takeover then queues a promoted copy as a committed replay and a copy
/// that stayed speculative as a plain one; another source's, another
/// epoch's and a later row's copies stay speculative.
#[test]
fn promote_streamed_flips_gated_in_both_rows() {
    let meta = Meta::open_in_memory().unwrap();
    let row = |n: u64| {
        vec![
            create(ROOT_INO, &format!("r{n}"), ino(n)),
            LogRecord::Completed { rid: rid(1, n) },
        ]
    };
    meta.install_streamed_committed(EPOCH_PREV, 1, 1, 1, &row(1), false, (0, 0))
        .unwrap();
    meta.install_streamed_committed(EPOCH_PREV, 1, 2, 2, &row(2), false, (0, 0))
        .unwrap();
    meta.install_streamed_committed(EPOCH_PREV + 1, 1, 1, 1, &row(3), false, (0, 0))
        .unwrap();
    // Another source's copy of the same epoch.
    meta.install_streamed_committed(EPOCH_PREV, 9, 3, 3, &row(4), false, (0, 0))
        .unwrap();
    assert_eq!(meta.promote_streamed(EPOCH_PREV, 1, 1).unwrap(), 1);
    assert_eq!(meta.promote_streamed(EPOCH_PREV, 1, 1).unwrap(), 0, "once");
    let proof = Default::default();
    let carried = Default::default();
    meta.reship_at_takeover(
        Some(constellation_meta::StrandArgs {
            epoch: EPOCH_PREV + 2,
            own_gens: &[],
            abandoned: None,
            carried: &carried,
            proof: &proof,
            marker: &[],
        }),
        None,
    )
    .unwrap();
    let queued = meta.pending_replays().unwrap();
    let gated = |n: u64| {
        queued
            .iter()
            .find(|q| q.rid == rid(1, n))
            .map(|q| q.class.gated())
    };
    assert_eq!(gated(1), Some(true), "{queued:?}");
    assert_eq!(gated(2), Some(false), "{queued:?}");
    assert_eq!(gated(3), Some(false), "{queued:?}");
    assert_eq!(gated(4), Some(false), "{queued:?}");
}

/// ADR-51: `kept_streamed` lists the committed (gated) streamed copies of one
/// epoch from one source, in journal order, with the delegation origin each
/// arrived with; speculative copies, other sources' and other epochs' are not
/// listed.
#[test]
fn kept_streamed_lists_committed_copies_of_a_source() {
    let meta = Meta::open_in_memory().unwrap();
    let row = |n: u64| {
        vec![
            create(ROOT_INO, &format!("k{n}"), ino(n)),
            LogRecord::Completed { rid: rid(1, n) },
        ]
    };
    meta.install_streamed_committed(EPOCH_PREV, 1, 1, 1, &row(1), true, (0, 0))
        .unwrap();
    meta.install_streamed_committed(EPOCH_PREV, 1, 2, 2, &row(2), true, (7, 3))
        .unwrap();
    // Speculative, then a committed one past it.
    meta.install_streamed_committed(EPOCH_PREV, 1, 3, 3, &row(3), false, (0, 0))
        .unwrap();
    // Another source's, another epoch's.
    meta.install_streamed_committed(EPOCH_PREV, 9, 4, 4, &row(4), true, (0, 0))
        .unwrap();
    meta.install_streamed_committed(EPOCH_PREV + 1, 1, 1, 1, &row(5), true, (0, 0))
        .unwrap();
    let kept = meta.kept_streamed(EPOCH_PREV, 1).unwrap();
    let shape: Vec<_> = kept.iter().map(|t| (t.first, t.last, t.origin)).collect();
    assert_eq!(shape, vec![(1, 1, (0, 0)), (2, 2, (7, 3))]);
    assert_eq!(kept[0].records, row(1));
    assert!(meta.kept_streamed(EPOCH_PREV, 5).unwrap().is_empty());
}

/// ADR-51: the operator abandoned the sources of a kept fetch that had not
/// answered. A committed copy the carried tail names retires on carry and is
/// never queued; a copy past the tail strands and replays by rid.
#[test]
fn an_abandon_after_partial_answers_queues_only_what_the_tail_does_not_name() {
    let meta = Meta::open_in_memory().unwrap();
    let row = |n: u64| {
        vec![
            create(ROOT_INO, &format!("a{n}"), ino(n)),
            LogRecord::Completed { rid: rid(1, n) },
        ]
    };
    meta.install_streamed_committed(EPOCH_PREV, 1, 1, 1, &row(1), true, (0, 0))
        .unwrap();
    meta.install_streamed_committed(EPOCH_PREV, 1, 2, 2, &row(2), true, (0, 0))
        .unwrap();
    // The claim carried the first row only (what answered).
    meta.backup_append(EPOCH_PREV, &[tx(1, 1, row(1))]).unwrap();
    let proof = constellation_meta::OrphanProof {
        upto: Some(EPOCH_PREV),
        upto_through: 1,
        no_tail: None,
    };
    let carried = Default::default();
    meta.reship_at_takeover(
        Some(constellation_meta::StrandArgs {
            epoch: EPOCH_PREV + 1,
            own_gens: &[],
            abandoned: Some(EPOCH_PREV),
            carried: &carried,
            proof: &proof,
            marker: &[],
        }),
        Some(constellation_meta::TailArgs {
            epoch: EPOCH_PREV,
            through: 1,
            config_version: 3,
            class: None,
        }),
    )
    .unwrap();
    let queued = meta.pending_replays().unwrap();
    let rids: Vec<_> = queued.iter().map(|q| q.rid).collect();
    assert_eq!(rids, vec![rid(1, 2)], "{queued:?}");
    assert!(meta.lookup(ROOT_INO, "a1").unwrap().is_some());
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
                marker: &[],
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

/// ADR-51 (M5): a member that is not the claimant applies the marker segment
/// of a partial abandon before the segments that carry the tail's rows. The
/// copies at or below the record's `through_known` are in the carried tail
/// and stay (they retire on carry); the ones past it strand and replay by
/// rid. Fails on the parent: the record's `through_known` was never read, so
/// every copy of the epoch stranded and replayed while the claim carried it.
#[test]
fn a_returning_member_keeps_tail_named_copies_after_a_partial_abandon() {
    let meta = Meta::open_in_memory().unwrap();
    let row = |n: u64| {
        vec![
            create(ROOT_INO, &format!("m{n}"), ino(n)),
            LogRecord::Completed { rid: rid(1, n) },
        ]
    };
    for n in 1..=5 {
        meta.install_streamed_committed(EPOCH_PREV, 1, n, n, &row(n), true, (0, 0))
            .unwrap();
    }
    // The claim carried rows 1..=3 and the operator abandoned the rest.
    let marker = vec![
        LogRecord::TailFollowsThrough {
            prev_epoch: EPOCH_PREV,
            through: 3,
        },
        LogRecord::GroupAbandoned {
            epoch: EPOCH_PREV,
            gen: 0,
            through_known: 3,
        },
    ];
    meta.apply_segment_rows(1, EPOCH_PREV + 1, 0, &[], &[], &marker, &Default::default())
        .unwrap();
    let rids: Vec<_> = meta
        .pending_replays()
        .unwrap()
        .iter()
        .map(|q| q.rid)
        .collect();
    assert_eq!(
        rids,
        vec![rid(1, 4), rid(1, 5)],
        "only the uncarried replay"
    );
    let kept: Vec<_> = meta
        .kept_streamed(EPOCH_PREV, 1)
        .unwrap()
        .iter()
        .map(|t| t.first)
        .collect();
    assert_eq!(
        kept,
        vec![1, 2, 3],
        "the carried copies wait for their tail"
    );
}

/// ADR-57: the takeover gate rolls back this node's own acknowledged row of
/// an older tenure into its member copy, and the namespace lacks it until the
/// log or a tail carries it again: reads of its keys are held, and the gate's
/// tail re-apply that carries the row's identity releases them.
#[test]
fn the_gate_holds_a_rolled_back_acknowledged_row_until_its_tail_re_applies_it() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    {
        let _c = PendingClass::set(RowCommit::ROOT_GROUP);
        execute_mutate(
            &meta,
            &MutateOp::Create {
                parent: ROOT_INO,
                name: "held".into(),
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
    let key = [constellation_meta::ReadKey::Dentry(ROOT_INO, "held".into())];
    let other = [constellation_meta::ReadKey::Dentry(ROOT_INO, "z".into())];
    let proof = Default::default();
    let carried = Default::default();
    // The strand alone (no tail names the row): rolled back, held.
    meta.reship_at_takeover(
        Some(constellation_meta::StrandArgs {
            epoch: 2,
            own_gens: &[],
            abandoned: None,
            carried: &carried,
            proof: &proof,
            marker: &[],
        }),
        None,
    )
    .unwrap();
    assert!(meta.lookup(ROOT_INO, "held").unwrap().is_none());
    assert!(meta.pending_replays().unwrap().is_empty());
    assert!(meta.member_copy_touches(&key), "held between the two");
    assert!(!meta.member_copy_touches(&other));
    // The tail that carries the row's identity re-applies it.
    let recs = vec![
        create(ROOT_INO, "held", ino(7)),
        LogRecord::Completed { rid: rid(3, 1) },
    ];
    let report = reship(&meta, &[tx(1, 1, recs)]).unwrap();
    assert_eq!(report.tail_applied, 1);
    assert!(meta.lookup(ROOT_INO, "held").unwrap().is_some());
    assert!(!meta.member_copy_touches(&key), "released by the tail");
}

// ---------------------------------------------------- ADR-59 part B (B2)

fn drop_held(idents: &[(u64, u64)]) -> LogRecord {
    LogRecord::DropHeld {
        ino: 9,
        chunks: Vec::new(),
        rolled_back: idents.iter().map(|i| (*i, None)).collect(),
        fills: Vec::new(),
        queued: Vec::new(),
    }
}

/// B2, 1714's shape (D2): a copy of a row the drop rolled back reaches this
/// node's backup tail after the log carried the `DropHeld`. It is stored as a
/// hole (the copy stores consult `retired` at ingest), so the successor's
/// gate meets no row of it. Fail before the ingest rule: the row landed as
/// `Completed`, behind its own refusal. (The gate's own `tail_retired` rule,
/// for a copy a source serves, is pinned in the core's tests.)
#[test]
fn a_tail_row_a_drop_held_of_the_log_retired_is_not_reshipped() {
    let meta = Meta::open_in_memory().unwrap();
    meta.apply_segment_rows(
        1,
        EPOCH_PREV,
        0,
        &[9],
        &[(0, 0)],
        &[
            drop_held(&[(EPOCH_PREV, 1)]),
            LogRecord::Refused {
                rid: rid(2, 1),
                code: Code::Io,
            },
        ],
        &Default::default(),
    )
    .unwrap();
    let seed = vec![
        create(ROOT_INO, "x", ino(7)),
        LogRecord::Completed { rid: rid(2, 1) },
    ];
    let other = vec![
        create(ROOT_INO, "y", ino(8)),
        LogRecord::Completed { rid: rid(2, 2) },
    ];
    let report = reship(&meta, &[tx(1, 2, seed), tx(3, 4, other)]).unwrap();
    assert_eq!(
        (report.tail_retired, report.tail_applied, report.tail_done),
        (0, 1, 0),
        "{report:?}"
    );
    assert!(meta.lookup(ROOT_INO, "x").unwrap().is_none());
    assert!(
        meta.lookup(ROOT_INO, "y").unwrap().is_some(),
        "each other row once"
    );
}

/// B3 (ii) (D1, D2): the dropper's record shipped and it died before the
/// apply. The log carries the `DropHeld`: this node's member copy of the
/// seed is a hole (no live row), and the successor's gate re-ships no row of the seed, each other row once.
#[test]
fn a_dropper_that_dies_after_its_record_shipped_leaves_holes_and_no_row() {
    use constellation_meta::MemberTx;
    let meta = Meta::open_in_memory().unwrap();
    let seed = vec![
        create(ROOT_INO, "x", ino(7)),
        LogRecord::Completed { rid: rid(2, 1) },
    ];
    let live = |meta: &Meta| -> usize {
        meta.member_copy_rows(EPOCH_PREV)
            .unwrap()
            .map_or(0, |(_, rows)| {
                rows.iter().filter(|r| !r.tx.records.is_empty()).count()
            })
    };
    meta.member_copy_put_rows(
        EPOCH_PREV,
        1,
        &[MemberTx {
            tx: tx(1, 2, seed.clone()),
            class: RowCommit::ROOT_GROUP,
            rid: Some(rid(2, 1)),
            op: None,
            lock_tag: Default::default(),
        }],
    )
    .unwrap();
    assert_eq!(live(&meta), 1);
    meta.apply_segment_rows(
        1,
        EPOCH_PREV,
        0,
        &[9],
        &[(0, 0)],
        &[drop_held(&[(EPOCH_PREV, 1)])],
        &Default::default(),
    )
    .unwrap();
    assert_eq!(live(&meta), 0, "the member copy of the seed is a hole");
    let other = vec![
        create(ROOT_INO, "y", ino(8)),
        LogRecord::Completed { rid: rid(2, 2) },
    ];
    let report = reship(&meta, &[tx(1, 2, seed), tx(3, 4, other)]).unwrap();
    assert_eq!(report.tail_applied, 1, "{report:?}");
    assert!(
        meta.lookup(ROOT_INO, "x").unwrap().is_none(),
        "no row of the seed"
    );
    assert!(meta.lookup(ROOT_INO, "y").unwrap().is_some());
}

/// B2 (MUST-B2): a tail row whose rid has *any* outcome here is done, a
/// `Refused` one included. Fail before: only an `Executed` outcome counted.
#[test]
fn a_tail_row_whose_rid_was_refused_here_is_done() {
    let meta = Meta::open_in_memory().unwrap();
    meta.apply_segment_rows(
        1,
        EPOCH_PREV,
        0,
        &[],
        &[],
        &[LogRecord::Refused {
            rid: rid(2, 1),
            code: Code::Io,
        }],
        &Default::default(),
    )
    .unwrap();
    let seed = vec![
        create(ROOT_INO, "x", ino(7)),
        LogRecord::Completed { rid: rid(2, 1) },
    ];
    let report = reship(&meta, &[tx(1, 2, seed)]).unwrap();
    assert_eq!(
        (report.tail_done, report.tail_applied),
        (1, 0),
        "{report:?}"
    );
    assert!(meta.lookup(ROOT_INO, "x").unwrap().is_none());
}

/// B2 (D2): a `DropHeld` in the sealed tail that the log does not carry is
/// its deposed dropper's undecided drop: fenced, and what it names is not
/// retired (the rows re-ship as the acknowledged rows they are).
#[test]
fn a_drop_held_the_log_lacks_is_fenced() {
    let meta = Meta::open_in_memory().unwrap();
    let seed = vec![
        create(ROOT_INO, "x", ino(7)),
        LogRecord::Completed { rid: rid(2, 1) },
    ];
    let report = reship(
        &meta,
        &[
            tx(1, 2, seed),
            tx(3, 3, vec![drop_held(&[(EPOCH_PREV, 1)])]),
        ],
    )
    .unwrap();
    assert_eq!(
        (
            report.tail_drop_fenced,
            report.tail_applied,
            report.tail_retired
        ),
        (1, 1, 0),
        "{report:?}"
    );
    assert!(meta.lookup(ROOT_INO, "x").unwrap().is_some());
    assert!(!meta.row_retired(EPOCH_PREV, 1));
}

/// ADR-59 part B, 846's shape (one hole writer for the backup tail): the
/// holder resent a hole over rows its journal no longer has, which merged two
/// transactions' spans; its next resend streams the first span as a narrower
/// hole again, and a late copy of the second transaction. The wider hole stays
/// (no span of the tail shrinks) and the late copy is not kept. A drop of the
/// log then retires the first transaction's identity; a late copy of it is no
/// copy either. Fail before: the narrower hole was stored over the wider one
/// (a debug build stopped at `backup_append`'s assertion "a stored span ...
/// keyed inside 77..=78 ends past it"; a release build stored the narrow hole
/// and kept the late copy of 79..=80 beside it).
#[test]
fn a_narrower_hole_resent_over_a_wider_one_leaves_the_wider_and_no_late_copy() {
    let meta = Meta::open_in_memory().unwrap();
    let live = |first: u64, last: u64, name: &str, n: u64| {
        tx(
            first,
            last,
            vec![
                create(ROOT_INO, name, ino(n)),
                LogRecord::Completed { rid: rid(2, n) },
            ],
        )
    };
    let hole = |first: u64, last: u64| tx(first, last, Vec::new());
    meta.backup_append(EPOCH_PREV, &[live(77, 78, "a", 1), live(79, 80, "b", 2)])
        .unwrap();
    // One hole over both (rows the journal no longer has).
    meta.backup_append(EPOCH_PREV, &[hole(77, 80)]).unwrap();
    let spans = |meta: &Meta| -> Vec<(u64, u64, usize)> {
        meta.backup_tail(EPOCH_PREV)
            .unwrap()
            .iter()
            .map(|t| (t.first, t.last, t.records.len()))
            .collect()
    };
    assert_eq!(spans(&meta), vec![(77, 80, 0)]);
    // The resend: the first span as a narrower hole, the second as a copy.
    meta.backup_append(EPOCH_PREV, &[hole(77, 78), live(79, 80, "b", 2)])
        .unwrap();
    assert_eq!(spans(&meta), vec![(77, 80, 0)], "one hole, no copy");
    // A drop of the log retires the first transaction's identity; a late copy
    // of it lands on the hole and is not kept.
    meta.apply_segment_rows(
        1,
        EPOCH_PREV,
        0,
        &[9],
        &[(0, 0)],
        &[drop_held(&[(EPOCH_PREV, 77)])],
        &Default::default(),
    )
    .unwrap();
    meta.backup_append(EPOCH_PREV, &[live(77, 78, "a", 1)])
        .unwrap();
    assert_eq!(spans(&meta), vec![(77, 80, 0)], "no copy of a retired row");
}
