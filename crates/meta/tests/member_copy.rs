//! Plan 43 C1-4: nothing committed is re-executed — member copies, the tail
//! identity, and the committed requester/subscriber entries that survive a
//! takeover (the store half; the core's half is in `authority`).

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{
    execute_mutate, BackupTx, LogRecord, MemberTx, Meta, MetaStore, MutateOp, PendingClass,
    Position, Rid, RowClass, TouchSet,
};

const HOLDER: u64 = 1;

fn rid(seq: u64) -> Rid {
    Rid {
        node: 2,
        incarnation: 1,
        seq,
    }
}

fn ino(n: u64) -> u64 {
    (HOLDER << 40) | n
}

fn create(name: &str, ino: u64, t: i64) -> LogRecord {
    LogRecord::Create {
        parent: ROOT_INO,
        name: name.into(),
        ino,
        mode: 0o644,
        uid: 0,
        gid: 0,
        time_ns: t,
    }
}

fn create_op(name: &str, ino: u64) -> MutateOp {
    MutateOp::Create {
        parent: ROOT_INO,
        name: name.into(),
        ino,
        mode: 0o644,
        uid: 0,
        gid: 0,
    }
}

fn completed(r: Rid) -> LogRecord {
    LogRecord::Completed { rid: r }
}

fn raw_ns(meta: &Meta) -> Vec<(Vec<u8>, Vec<u8>)> {
    meta.read_consistent(|snap| meta.ns_dump_at(snap)).unwrap()
}

fn shipped(seq: u64) -> Position {
    Position {
        seq,
        ..Position::ZERO
    }
}

/// Run `create name` as the holder at epoch 1 under `class`, rid `seq`.
fn holder_create(meta: &Meta, class: RowClass, name: &str, n: u64, seq: u64) {
    let _c = PendingClass::set(class);
    execute_mutate(meta, &create_op(name, ino(n)), Some(rid(seq))).unwrap();
}

fn apply_rows(meta: &Meta, seq: u64, epoch: u64, through: u64, rows: &[u64], recs: &[LogRecord]) {
    meta.apply_segment_rows(seq, epoch, through, rows, &[], recs, &TouchSet::default())
        .unwrap();
}

fn mtx(first: u64, last: u64, class: RowClass, records: Vec<LogRecord>) -> MemberTx {
    MemberTx {
        tx: BackupTx {
            first,
            last,
            records,
            origin: (0, 0),
        },
        class,
        rid: None,
        op: None,
        lock_tag: Default::default(),
    }
}

// ------------------------------------------------ a deposed group tenure

/// A deposed holder's gated rows are rolled back and kept as the member copy
/// of their epoch; only its lone rows are queued for replay.
#[test]
fn strand_below_epoch_moves_gated_rows_to_the_member_copy() {
    let meta = Meta::open_in_memory().unwrap();
    let before = raw_ns(&meta);
    meta.set_holder_epoch(1);
    holder_create(&meta, RowClass::RootGroup, "a", 1, 1);
    holder_create(&meta, RowClass::Lone, "b", 2, 2);
    meta.set_holder_epoch(0);
    let stranded = meta.strand_below_epoch(2).unwrap();
    assert_eq!(stranded.member_copied, 1);
    assert_eq!(stranded.locals, 1);
    assert_eq!(raw_ns(&meta), before, "both rolled back");
    assert_eq!(meta.journal_len().unwrap(), 0);
    let queued = meta.pending_replays().unwrap();
    assert_eq!(
        queued.iter().map(|q| q.rid).collect::<Vec<_>>(),
        vec![rid(2)],
        "only the lone row is replayed"
    );
    let (info, rows) = meta.member_copy_rows(1).unwrap().expect("a member copy");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].class, RowClass::RootGroup);
    assert_eq!(rows[0].rid, Some(rid(1)));
    assert!(rows[0].op.is_some());
    assert_eq!(info.through, rows[0].tx.last);
    assert!(meta.member_copy_carries(rid(1)).unwrap());
    assert!(!meta.member_copy_carries(rid(2)).unwrap());
    assert_eq!(
        meta.completed_position(rid(1)).unwrap(),
        None,
        "this node no longer claims to have executed it"
    );
}

/// The ungated rows keep today's behaviour: queued for replay by rid.
#[test]
fn an_ungated_row_is_still_queued() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    holder_create(&meta, RowClass::Archive, "a", 1, 1);
    holder_create(&meta, RowClass::NonStealable, "b", 2, 2);
    meta.set_holder_epoch(0);
    // (An Archive row is gated by Q12 but a never-acknowledged one: it goes
    // to the member copy and is dropped at the next marker.)
    let stranded = meta.strand_below_epoch(2).unwrap();
    assert_eq!(stranded.member_copied, 1);
    assert_eq!(stranded.locals, 1);
    assert_eq!(
        meta.pending_replays()
            .unwrap()
            .iter()
            .map(|q| q.rid)
            .collect::<Vec<_>>(),
        vec![rid(2)]
    );
}

// ------------------------------------- committed requester/subscriber entries

/// A committed shadow is not stranded by a later epoch's segment; the
/// successor's segment that completes its rid retires it.
#[test]
fn a_committed_shadow_survives_a_takeover_and_retires_by_the_re_ship() {
    let meta = Meta::open_in_memory().unwrap();
    let recs = vec![create("a", ino(1), 10), completed(rid(1))];
    assert!(meta
        .install_shadow_committed(
            rid(1),
            1,
            0,
            &create_op("a", ino(1)),
            &Default::default(),
            &recs,
            true
        )
        .unwrap());
    // Another node's row of epoch 2 arrives (the marker and an unrelated
    // transaction): the shadow stays.
    let other = vec![create("b", ino(2), 11), completed(rid(2))];
    apply_rows(&meta, 1, 2, 2, &[1, 2], &other);
    assert!(meta.has_outstanding_speculation(), "not stranded");
    assert!(meta.pending_replays().unwrap().is_empty());
    assert!(meta.lookup(ROOT_INO, "a").unwrap().is_some());
    // The successor re-ships the row (with its identity): retired.
    let reship = vec![
        LogRecord::TailOf { epoch: 1, jseq: 1 },
        create("a", ino(1), 10),
        completed(rid(1)),
    ];
    apply_rows(&meta, 2, 2, 5, &[3, 4, 5], &reship);
    assert!(!meta.has_outstanding_speculation());
    assert!(meta.pending_replays().unwrap().is_empty());
    assert_eq!(meta.speculation_counts().unwrap().outstanding, 0);
}

/// The same shadow installed as uncommitted strands as before.
#[test]
fn an_uncommitted_shadow_still_strands_at_a_later_epoch() {
    let meta = Meta::open_in_memory().unwrap();
    let recs = vec![create("a", ino(1), 10), completed(rid(1))];
    assert!(meta
        .install_shadow(rid(1), 1, &create_op("a", ino(1)), &recs)
        .unwrap());
    let other = vec![create("b", ino(2), 11), completed(rid(2))];
    apply_rows(&meta, 1, 2, 2, &[1, 2], &other);
    assert_eq!(meta.pending_replays().unwrap().len(), 1);
    assert!(meta.lookup(ROOT_INO, "a").unwrap().is_none());
}

/// A committed streamed transaction retires when a segment carries its
/// `TailOf`, and the segment's records are applied once (the namespace is
/// what the log alone produces).
#[test]
fn a_tail_origin_retires_a_streamed_entry() {
    let tx1 = vec![create("a", ino(1), 10), completed(rid(1))];
    let reference = Meta::open_in_memory().unwrap();
    let reship = vec![
        LogRecord::TailOf { epoch: 1, jseq: 1 },
        create("a", ino(1), 10),
        completed(rid(1)),
    ];
    apply_rows(&reference, 1, 2, 3, &[1, 2, 3], &reship);

    let meta = Meta::open_in_memory().unwrap();
    meta.install_streamed_committed(1, HOLDER, 1, 2, &tx1, true)
        .unwrap();
    assert!(meta.has_outstanding_speculation());
    // A marker of the successor: the entry is not stranded by it.
    apply_rows(&meta, 1, 2, 0, &[], &[]);
    assert!(meta.has_outstanding_speculation());
    assert!(meta.pending_replays().unwrap().is_empty());
    apply_rows(&meta, 2, 2, 3, &[1, 2, 3], &reship);
    assert!(!meta.has_outstanding_speculation());
    assert!(meta.pending_replays().unwrap().is_empty());
    // (The root's own times are the wall clock of the apply; the namespace
    // below it is what the log alone produces.)
    let names = |m: &Meta| -> Vec<(String, u64)> {
        m.readdir(ROOT_INO)
            .unwrap()
            .into_iter()
            .map(|e| (e.name, e.ino))
            .collect()
    };
    assert_eq!(names(&meta), names(&reference));
    assert_eq!(names(&meta), vec![("a".to_string(), ino(1))]);
}

/// Only an operator's abandon of the group strands a committed entry; it is
/// then replayed, and counted as a committed rollback.
#[test]
fn an_abandon_strands_a_committed_entry() {
    let meta = Meta::open_in_memory().unwrap();
    let recs = vec![create("a", ino(1), 10), completed(rid(1))];
    meta.install_shadow_committed(
        rid(1),
        1,
        0,
        &create_op("a", ino(1)),
        &Default::default(),
        &recs,
        true,
    )
    .unwrap();
    let marker = vec![LogRecord::GroupAbandoned {
        epoch: 1,
        gen: 0,
        through_known: 0,
    }];
    let applied = meta
        .apply_segment_rows(1, 2, 0, &[], &[], &marker, &TouchSet::default())
        .unwrap();
    assert_eq!(applied.stranded.shadows, 1);
    assert_eq!(applied.stranded.committed_rolled_back, 1);
    assert_eq!(meta.pending_replays().unwrap().len(), 1);
}

/// The takeover gate takes its own committed entries out without queueing
/// them (the tail it re-applies carries the rows) — unless the group was
/// abandoned.
#[test]
fn the_takeover_gate_supersedes_a_committed_entry_without_queueing() {
    for abandoned in [None, Some(1)] {
        let meta = Meta::open_in_memory().unwrap();
        let before = raw_ns(&meta);
        let recs = vec![create("a", ino(1), 10), completed(rid(1))];
        meta.install_shadow_committed(
            rid(1),
            1,
            0,
            &create_op("a", ino(1)),
            &Default::default(),
            &recs,
            true,
        )
        .unwrap();
        meta.install_streamed_committed(
            1,
            HOLDER,
            5,
            6,
            &[create("c", ino(3), 12), completed(rid(3))],
            true,
        )
        .unwrap();
        let stranded = meta.strand_for_takeover(2, &[], abandoned).unwrap();
        assert_eq!(raw_ns(&meta), before, "rolled back");
        assert!(!meta.has_outstanding_speculation());
        match abandoned {
            None => {
                assert_eq!(stranded.superseded, 2);
                assert_eq!(stranded.committed_rolled_back, 0);
                assert!(meta.pending_replays().unwrap().is_empty());
            }
            Some(_) => {
                assert_eq!(stranded.superseded, 0);
                assert_eq!(stranded.committed_rolled_back, 2);
                assert_eq!(meta.pending_replays().unwrap().len(), 2);
            }
        }
    }
}

/// A rebuild of the namespace keeps a committed shadow (re-installed on the
/// rebuilt namespace) and drops it only if the log it was rebuilt from
/// completes the rid.
#[test]
fn a_rebuild_keeps_a_committed_shadow() {
    let meta = Meta::open_in_memory().unwrap();
    let recs = vec![create("a", ino(1), 10), completed(rid(1))];
    meta.install_shadow_committed(
        rid(1),
        1,
        0,
        &create_op("a", ino(1)),
        &Default::default(),
        &recs,
        true,
    )
    .unwrap();
    // A gated row of this node's own journal: member copy; a lone one: queue.
    meta.set_holder_epoch(1);
    holder_create(&meta, RowClass::RootGroup, "g", 4, 4);
    holder_create(&meta, RowClass::Lone, "l", 5, 5);
    meta.set_holder_epoch(0);
    let side = Meta::open_in_memory().unwrap();
    meta.replace_ns_from_rebuilt(&side, &Default::default())
        .unwrap();
    assert!(
        meta.lookup(ROOT_INO, "a").unwrap().is_some(),
        "the committed shadow is on the rebuilt namespace"
    );
    assert!(meta.lookup(ROOT_INO, "g").unwrap().is_none());
    assert!(meta.has_outstanding_speculation());
    assert_eq!(
        meta.pending_replays()
            .unwrap()
            .iter()
            .map(|q| q.rid)
            .collect::<Vec<_>>(),
        vec![rid(5)],
        "the lone row replays; the committed shadow and the gated row do not"
    );
    assert!(meta.member_copy_carries(rid(4)).unwrap());
    // The log the rebuild came from completes the shadow's rid: retired.
    let meta = Meta::open_in_memory().unwrap();
    meta.install_shadow_committed(
        rid(1),
        1,
        0,
        &create_op("a", ino(1)),
        &Default::default(),
        &recs,
        true,
    )
    .unwrap();
    let side = Meta::open_in_memory().unwrap();
    apply_rows(&side, 1, 2, 2, &[1, 2], &recs);
    meta.replace_ns_from_rebuilt(&side, &Default::default())
        .unwrap();
    assert!(!meta.has_outstanding_speculation());
    assert!(meta.lookup(ROOT_INO, "a").unwrap().is_some());
}

// -------------------------------------------------------- the drop rules

fn put(meta: &Meta, rows: &[MemberTx]) {
    meta.member_copy_put_rows(1, 3, rows).unwrap();
}

/// The archive carries a row (its identity, or its own epoch's segment): the
/// copy is dropped.
#[test]
fn the_member_copy_is_dropped_when_the_archive_carries_it() {
    let meta = Meta::open_in_memory().unwrap();
    put(
        &meta,
        &[
            mtx(1, 2, RowClass::RootGroup, vec![create("a", ino(1), 10)]),
            mtx(3, 3, RowClass::RootGroup, vec![create("b", ino(2), 11)]),
            mtx(4, 4, RowClass::RootGroup, vec![create("c", ino(3), 12)]),
        ],
    );
    // A re-ship names the first by identity.
    let reship = vec![
        LogRecord::TailOf { epoch: 1, jseq: 1 },
        create("a", ino(1), 10),
    ];
    let applied = meta
        .apply_segment_rows(1, 2, 2, &[1, 2], &[], &reship, &TouchSet::default())
        .unwrap();
    assert_eq!(applied.member.retired, 1);
    let (_, rows) = meta.member_copy_rows(1).unwrap().unwrap();
    assert_eq!(rows.iter().map(|r| r.tx.first).collect::<Vec<_>>(), [3, 4]);
    // The holder's own epoch's late segment carries the second by row.
    let late = vec![create("b", ino(2), 11)];
    let applied = meta
        .apply_segment_rows(2, 1, 3, &[3], &[], &late, &TouchSet::default())
        .unwrap();
    assert_eq!(applied.member.retired, 1);
    let (_, rows) = meta.member_copy_rows(1).unwrap().unwrap();
    assert_eq!(rows.iter().map(|r| r.tx.first).collect::<Vec<_>>(), [4]);
    // The last by another re-ship; the copy is gone.
    let reship = vec![
        LogRecord::TailOf { epoch: 1, jseq: 4 },
        create("c", ino(3), 12),
    ];
    apply_rows(&meta, 3, 2, 4, &[3, 4], &reship);
    assert_eq!(meta.member_copy(1).unwrap(), None);
    assert!(meta.member_copy_epochs().unwrap().is_empty());
}

/// A marker's `TailFollowsThrough` drops the rows the sealer's hold did not
/// reach (never committed).
#[test]
fn tail_follows_drops_member_rows_past_through() {
    let meta = Meta::open_in_memory().unwrap();
    put(
        &meta,
        &[
            mtx(1, 1, RowClass::RootGroup, vec![create("a", ino(1), 10)]),
            mtx(2, 2, RowClass::RootGroup, vec![create("b", ino(2), 11)]),
            mtx(3, 3, RowClass::RootGroup, vec![create("c", ino(3), 12)]),
        ],
    );
    let marker = vec![LogRecord::TailFollowsThrough {
        prev_epoch: 1,
        through: 2,
    }];
    let applied = meta
        .apply_segment_rows(1, 2, 0, &[], &[], &marker, &TouchSet::default())
        .unwrap();
    assert_eq!(applied.member.dropped, 1);
    let (_, rows) = meta.member_copy_rows(1).unwrap().unwrap();
    assert_eq!(rows.iter().map(|r| r.tx.first).collect::<Vec<_>>(), [1, 2]);
}

/// A marker of a later epoch that announces no tail of this epoch drops the
/// rows nobody was told were committed; a group-committed row stays until
/// coverage (it may be another tenure's inherited tail).
#[test]
fn a_marker_without_tail_follows_drops_only_archive_rows() {
    let meta = Meta::open_in_memory().unwrap();
    put(
        &meta,
        &[
            mtx(1, 1, RowClass::RootGroup, vec![create("a", ino(1), 10)]),
            mtx(2, 2, RowClass::Archive, vec![create("b", ino(2), 11)]),
            mtx(3, 3, RowClass::Lone, vec![create("c", ino(3), 12)]),
        ],
    );
    let applied = meta
        .apply_segment_rows(1, 2, 0, &[], &[], &[], &TouchSet::default())
        .unwrap();
    assert_eq!(applied.member.dropped, 2);
    let (_, rows) = meta.member_copy_rows(1).unwrap().unwrap();
    assert_eq!(rows.iter().map(|r| r.tx.first).collect::<Vec<_>>(), [1]);
    // A later marker (a third epoch) announcing another epoch's tail does
    // not drop it either.
    let marker = vec![LogRecord::TailFollowsThrough {
        prev_epoch: 2,
        through: 9,
    }];
    apply_rows(&meta, 2, 3, 0, &[], &marker);
    assert_eq!(meta.member_copy_rows(1).unwrap().unwrap().1.len(), 1);
}

/// The operator abandoned the group: its member rows (but a lone node's
/// own) are queued as the lone branch's replay, once.
#[test]
fn group_abandoned_moves_the_member_copy_to_the_replay_queue() {
    let meta = Meta::open_in_memory().unwrap();
    let mut row1 = mtx(
        1,
        2,
        RowClass::RootGroup,
        vec![create("a", ino(1), 10), completed(rid(1))],
    );
    row1.rid = Some(rid(1));
    row1.op = Some(create_op("a", ino(1)));
    let row2 = mtx(
        3,
        3,
        RowClass::RootGroup,
        vec![create("b", ino(2), 11), completed(rid(2))],
    );
    let mut lone = mtx(4, 4, RowClass::Lone, vec![create("c", ino(3), 12)]);
    lone.rid = Some(rid(3));
    lone.op = Some(create_op("c", ino(3)));
    put(&meta, &[row1, row2, lone]);
    let marker = vec![LogRecord::GroupAbandoned {
        epoch: 1,
        gen: 0,
        through_known: 4,
    }];
    let applied = meta
        .apply_segment_rows(1, 2, 0, &[], &[], &marker, &TouchSet::default())
        .unwrap();
    assert_eq!(applied.member.abandoned, 2);
    assert_eq!(meta.member_copy(1).unwrap(), None);
    let queued: Vec<Rid> = meta
        .pending_replays()
        .unwrap()
        .iter()
        .map(|q| q.rid)
        .collect();
    assert_eq!(queued, vec![rid(1), rid(2)]);
}

/// The copy, kept across a restart of the store, is dropped by the same
/// rules (the flag that skips the rules on a segment without one is
/// recomputed at open).
#[test]
fn a_member_copy_is_ruled_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("m.db");
    {
        let meta = Meta::open(&db).unwrap();
        put(
            &meta,
            &[mtx(
                1,
                1,
                RowClass::RootGroup,
                vec![create("a", ino(1), 10)],
            )],
        );
        meta.close().unwrap();
    }
    let meta = Meta::open(&db).unwrap();
    let reship = vec![
        LogRecord::TailOf { epoch: 1, jseq: 1 },
        create("a", ino(1), 10),
    ];
    let applied = meta
        .apply_segment_rows(1, 2, 2, &[1, 2], &[], &reship, &TouchSet::default())
        .unwrap();
    assert_eq!(applied.member.retired, 1);
    assert_eq!(meta.member_copy(1).unwrap(), None);
    let _ = shipped(0);
}
