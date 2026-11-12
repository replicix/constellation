//! Plan 43 C1-4: nothing committed is re-executed — member copies, the tail
//! identity, and the committed requester/subscriber entries that survive a
//! takeover (the store half; the core's half is in `authority`).

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{
    execute_mutate, BackupTx, GroupOn, LogRecord, MemberTx, Meta, MetaStore, MutateOp,
    PendingClass, Position, ReadKey, Rid, RowCommit, TouchSet,
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
fn holder_create(meta: &Meta, class: RowCommit, name: &str, n: u64, seq: u64) {
    let _c = PendingClass::set(class);
    execute_mutate(meta, &create_op(name, ino(n)), Some(rid(seq))).unwrap();
}

fn apply_rows(meta: &Meta, seq: u64, epoch: u64, through: u64, rows: &[u64], recs: &[LogRecord]) {
    meta.apply_segment_rows(seq, epoch, through, rows, &[], recs, &TouchSet::default())
        .unwrap();
}

fn mtx(first: u64, last: u64, class: RowCommit, records: Vec<LogRecord>) -> MemberTx {
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
    holder_create(&meta, RowCommit::ROOT_GROUP, "a", 1, 1);
    holder_create(&meta, RowCommit::LONE, "b", 2, 2);
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
    assert_eq!(rows[0].class, RowCommit::ROOT_GROUP);
    assert_eq!(rows[0].rid, Some(rid(1)));
    assert!(rows[0].op.is_some());
    // (A recovery's strand holds reads of it, as a segment's does.)
    assert!(meta.member_copy_touches(&[ReadKey::Dentry(ROOT_INO, "a".into())]));
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
    holder_create(&meta, RowCommit::ARCHIVE, "a", 1, 1);
    holder_create(&meta, RowCommit::FINAL, "b", 2, 2);
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

/// `long-delegated-late-answers` seed 5839 (lease-read knob, main before
/// 9fe5531b): a deposed root's committed own row, stranded by the
/// successor's first segment, moves to the member copy and leaves the
/// namespace until the successor's re-ship lands. Meanwhile a delegate's
/// row that this root appended after it (a marker written after the data)
/// stays visible, and a reader of the data saw it vanish: no queued replay
/// touched its keys, so the session wait did not hold the read. A member
/// copy row the namespace lacks holds reads of its keys, as a queued
/// replay does, until the log carries it.
#[test]
fn a_committed_row_moved_to_the_member_copy_holds_its_readers() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    holder_create(&meta, RowCommit::ROOT_GROUP, "data", 1, 1);
    meta.set_holder_epoch(0);
    let key = [ReadKey::Dentry(ROOT_INO, "data".into())];
    assert!(meta.lookup(ROOT_INO, "data").unwrap().is_some());
    // The successor's first segment (epoch 2, an unrelated row).
    let other = vec![create("b", ino(2), 11), completed(rid(2))];
    apply_rows(&meta, 1, 2, 2, &[1, 2], &other);
    assert!(
        meta.lookup(ROOT_INO, "data").unwrap().is_none(),
        "rolled back"
    );
    assert!(meta.member_copy_carries(rid(1)).unwrap());
    assert!(meta.pending_replays().unwrap().is_empty(), "not queued");
    assert!(
        meta.member_copy_touches(&key),
        "a read of the rolled-back committed row must wait for its re-ship"
    );
    assert!(!meta.member_copy_touches(&[ReadKey::Dentry(ROOT_INO, "unrelated".into())]));
    // The successor re-ships the row with its identity: visible again, and
    // nothing holds the read any more.
    let reship = vec![
        LogRecord::TailOf { epoch: 1, jseq: 1 },
        create("data", ino(1), 10),
        completed(rid(1)),
    ];
    apply_rows(&meta, 2, 2, 5, &[3, 4, 5], &reship);
    assert!(meta.lookup(ROOT_INO, "data").unwrap().is_some());
    assert!(
        !meta.member_copy_carries(rid(1)).unwrap(),
        "retired by the re-ship"
    );
    assert!(!meta.member_copy_touches(&key));
}

/// A deposed root's recovery (`Core::recover_deposed`) strands before the
/// successor's first segment arrives: its committed own row goes to the
/// member copy by `strand_below_epoch`, and a delegate's row it appended
/// after it stays applied (the journal is not empty), so the recovery
/// rebuilds from the log, serving reads meanwhile. Reads of the row's keys
/// wait for the re-ship, as after a segment's strand.
#[test]
fn a_recovery_strand_holds_its_readers_until_the_reship() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    holder_create(&meta, RowCommit::ROOT_GROUP, "data", 1, 1);
    meta.set_holder_epoch(0);
    let key = [ReadKey::Dentry(ROOT_INO, "data".into())];
    assert_eq!(meta.strand_below_epoch(2).unwrap().member_copied, 1);
    assert!(meta.lookup(ROOT_INO, "data").unwrap().is_none());
    assert!(meta.pending_replays().unwrap().is_empty(), "not queued");
    assert!(
        meta.member_copy_touches(&key),
        "a read of the rolled-back committed row must wait for its re-ship"
    );
    // The successor's first segment strands nothing more; still held.
    let other = vec![create("b", ino(2), 11), completed(rid(2))];
    apply_rows(&meta, 1, 2, 2, &[1, 2], &other);
    assert!(meta.member_copy_touches(&key));
    let reship = vec![
        LogRecord::TailOf { epoch: 1, jseq: 1 },
        create("data", ino(1), 10),
        completed(rid(1)),
    ];
    apply_rows(&meta, 2, 2, 5, &[3, 4, 5], &reship);
    assert!(meta.lookup(ROOT_INO, "data").unwrap().is_some());
    assert!(!meta.member_copy_carries(rid(1)).unwrap());
    assert!(!meta.member_copy_touches(&key), "released by the re-ship");
}

/// ADR-57 (H2): the hold is persisted with the strand that took the row out
/// of the served state, so a restart inside the re-ship window keeps it. Both
/// kinds: a gated own row (member copy of its epoch) and a delegate row the
/// deposed root appended (member copy of its generation).
#[test]
fn a_hold_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("m.db");
    let a = [ReadKey::Dentry(ROOT_INO, "a".into())];
    let own = [ReadKey::Dentry(ROOT_INO, "data".into())];
    let other = [ReadKey::Dentry(ROOT_INO, "z".into())];
    {
        let meta = Meta::open(&db).unwrap();
        meta.set_holder_epoch(1);
        holder_create(&meta, RowCommit::ROOT_GROUP, "data", 1, 1);
        meta.apply_delegate_tx(
            &[create("a", ino(2), 11), completed(rid(2))],
            Some(rid(2)),
            5,
            1,
            Position::ZERO,
        )
        .unwrap();
        let stranded = meta.strand_deposed(2).unwrap();
        assert_eq!(stranded.member_copied, 1);
        assert_eq!(stranded.deleg_member_copied, 1);
        assert!(meta.member_copy_touches(&a));
        assert!(meta.member_copy_touches(&own));
        meta.close().unwrap();
    }
    let meta = Meta::open(&db).unwrap();
    assert!(
        meta.member_copy_touches(&a),
        "the delegate row's hold is loaded at open"
    );
    assert!(
        meta.member_copy_touches(&own),
        "the gated own row's hold is loaded at open"
    );
    assert!(!meta.member_copy_touches(&other));
    // The re-ship of the own row releases its hold, only its own.
    let reship = vec![
        LogRecord::TailOf { epoch: 1, jseq: 1 },
        create("data", ino(1), 10),
        completed(rid(1)),
    ];
    apply_rows(&meta, 1, 2, 3, &[1, 2, 3], &reship);
    assert!(!meta.member_copy_touches(&own));
    assert!(meta.member_copy_touches(&a));
    meta.deleg_backup_clear(5).unwrap();
    assert!(!meta.member_copy_touches(&a));
    meta.close().unwrap();
    drop(meta);
    let meta = Meta::open(&db).unwrap();
    assert!(
        !meta.member_copy_touches(&a) && !meta.member_copy_touches(&own),
        "a release is persisted too"
    );
}

/// A deposed root's append of a delegate's row `a` (gen 5, idx 1, rid 2), held.
fn deposed_with_a_delegate_row(meta: &Meta) {
    meta.set_holder_epoch(1);
    meta.apply_delegate_tx(
        &[create("a", ino(2), 11), completed(rid(2))],
        Some(rid(2)),
        5,
        1,
        Position::ZERO,
    )
    .unwrap();
    let stranded = meta.strand_deposed(2).unwrap();
    assert_eq!(stranded.deleg_member_copied, 1);
}

/// ADR-57 review MUST-1: the segment that ends a generation releases the
/// holds of its rows in its own transaction. Without `deleg_backup_clear`
/// (which runs after the commit, best effort: a crash or an error between
/// left the holds for good) the hold is gone, and stays gone across a
/// restart.
#[test]
fn the_segment_that_ends_a_generation_releases_its_holds_itself() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("m.db");
    let a = [ReadKey::Dentry(ROOT_INO, "a".into())];
    {
        let meta = Meta::open(&db).unwrap();
        deposed_with_a_delegate_row(&meta);
        assert!(meta.member_copy_touches(&a));
        // The end, cut below the held row: it never returns.
        apply_rows(
            &meta,
            1,
            2,
            1,
            &[1],
            &[LogRecord::DelegateNodeEnded {
                node: 2,
                gen: 5,
                through: 0,
            }],
        );
        assert!(
            !meta.member_copy_touches(&a),
            "released with the end, not by a later best-effort clear"
        );
        meta.close().unwrap();
    }
    let meta = Meta::open(&db).unwrap();
    assert!(!meta.member_copy_touches(&a), "and persisted so");
    assert!(meta.read_holds().is_empty());
}

/// ADR-57 review MUST-1: a rebuild whose log ended the generation (the side
/// replica carries the end) releases the holds its reset set on the
/// generation's rows.
#[test]
fn a_rebuild_whose_log_ended_the_generation_releases_its_holds() {
    let meta = Meta::open_in_memory().unwrap();
    deposed_with_a_delegate_row(&meta);
    meta.set_holder_epoch(0);
    let a = [ReadKey::Dentry(ROOT_INO, "a".into())];
    let side = Meta::open_in_memory().unwrap();
    apply_rows(
        &side,
        1,
        2,
        1,
        &[1],
        &[LogRecord::DelegateNodeEnded {
            node: 2,
            gen: 5,
            through: 0,
        }],
    );
    // (The rebuild strands nothing more of it: the row left the journal.)
    meta.replace_ns_from_rebuilt(&side, &Default::default())
        .unwrap();
    assert!(!meta.member_copy_touches(&a));
}

/// ADR-57 review MUST-1: a hold of a generation that has a persisted end
/// (left by an older crash between the end and the release) is dropped when
/// the holds are loaded at open, and a row of an ended generation is not
/// held to begin with.
#[test]
fn a_hold_of_an_ended_generation_is_never_loaded_nor_set() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("m.db");
    let a = [ReadKey::Dentry(ROOT_INO, "a".into())];
    {
        let meta = Meta::open(&db).unwrap();
        deposed_with_a_delegate_row(&meta);
        assert!(meta.member_copy_touches(&a));
        // The end landed, its release did not.
        meta.kv_set("deleg_void_cut:5", "0").unwrap();
        meta.close().unwrap();
    }
    let meta = Meta::open(&db).unwrap();
    assert!(!meta.member_copy_touches(&a), "not loaded");
    assert!(meta.read_holds().is_empty());
    // A strand after the end holds nothing of it either.
    let meta = Meta::open_in_memory().unwrap();
    meta.kv_set("deleg_void_cut:5", "0").unwrap();
    deposed_with_a_delegate_row(&meta);
    assert!(!meta.member_copy_touches(&a), "not set");
}

/// ADR-57 review SHOULD-1: a delegate's row this root appended that never
/// returns under its `(gen, idx)` (the delegate re-executed it by rid under
/// another identity) is released when the log carries its rid.
#[test]
fn a_held_delegate_row_is_released_when_the_log_carries_its_rid() {
    let meta = Meta::open_in_memory().unwrap();
    deposed_with_a_delegate_row(&meta);
    let a = [ReadKey::Dentry(ROOT_INO, "a".into())];
    assert!(meta.member_copy_touches(&a));
    // Another row of the log: still held.
    apply_rows(
        &meta,
        1,
        2,
        1,
        &[1],
        &[create("b", ino(3), 12), completed(rid(3))],
    );
    assert!(meta.member_copy_touches(&a));
    // The rid under another origin ((0, 0): no generation index reaches it).
    apply_rows(
        &meta,
        2,
        2,
        2,
        &[2],
        &[create("a", ino(2), 11), completed(rid(2))],
    );
    assert!(meta.lookup(ROOT_INO, "a").unwrap().is_some());
    assert!(!meta.member_copy_touches(&a), "released by the rid");
}

/// ADR-57 (`locks-unlinked-delegated-dcrash-pause` seeds 651, 1439, 1687,
/// 1897): an own gated row whose client retried by rid, so that the log
/// carries the rid under another identity (no tail names the copy), is
/// released when the log carries the rid, and a row stranded after the log
/// carried it is not held at all.
#[test]
fn a_held_own_row_is_released_when_the_log_carries_its_rid() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    holder_create(&meta, RowCommit::ROOT_GROUP, "data", 1, 1);
    meta.set_holder_epoch(0);
    assert_eq!(meta.strand_below_epoch(2).unwrap().member_copied, 1);
    let key = [ReadKey::Dentry(ROOT_INO, "data".into())];
    assert!(meta.member_copy_touches(&key));
    // The retry executed at the new holder: the rid is in the log, under
    // another identity (no `TailOf`).
    apply_rows(
        &meta,
        1,
        2,
        2,
        &[1, 2],
        &[create("data", ino(9), 20), completed(rid(1))],
    );
    assert!(meta.lookup(ROOT_INO, "data").unwrap().is_some());
    assert!(!meta.member_copy_touches(&key), "released by the rid");
}

/// ADR-57 round-2 MUST-1: the lazy reap of holds whose row is back
/// (`completed[rid]` is there) must not take a hold whose strand has not
/// committed. The strand indexes its hold and removes `completed[rid]` in one
/// transaction; a reader that runs meanwhile sees the committed snapshot,
/// where the row looks back; the reap that follows blocks on the writer until
/// the strand commits, and (before the fix) removed the hold the strand had
/// just committed, with the row rolled back and nothing left to hold its
/// readers. Fails before: the hold is gone after the commit.
#[test]
fn the_reap_does_not_take_a_hold_whose_strand_is_in_flight() {
    use std::sync::mpsc;
    use std::time::Duration;
    let meta = Meta::open_in_memory().unwrap();
    let (sig, seen) = mpsc::channel::<()>();
    let sig = std::sync::Mutex::new(sig);
    meta.set_reap_hook(Box::new(move || {
        let _ = sig.lock().unwrap().send(());
    }));
    let a = [ReadKey::Dentry(ROOT_INO, "a".into())];
    std::thread::scope(|scope| {
        let mut reader = None;
        meta.test_strand_hold_while(5, 1, "a", rid(9), || {
            reader = Some(scope.spawn(|| {
                // The strand's hold is indexed, its transaction open.
                assert!(
                    meta.member_copy_touches(&a),
                    "an uncommitted hold is held, not back"
                );
                meta.reap_completed_holds_for_tests();
            }));
            // Where the bug is, the reap has chosen its victim by now and
            // waits for the writer; where it is fixed it never gets there.
            let _ = seen.recv_timeout(Duration::from_millis(800));
        })
        .unwrap();
        reader.unwrap().join().unwrap();
    });
    assert!(
        meta.member_copy_touches(&a),
        "the committed hold survived the reap"
    );
    assert_eq!(meta.read_holds(), vec![(5, 1)]);
    assert!(!meta.reconcile_read_holds(), "its key is committed");
}

/// ADR-57 review SHOULD-3: a hold indexed in memory by a transaction that
/// was dropped has no key behind it and no staged release; the thread's next
/// flush, and a reconcile, take it out of the index.
#[test]
fn a_hold_of_a_dropped_transaction_does_not_stay() {
    let a = [ReadKey::Dentry(ROOT_INO, "a".into())];
    let meta = Meta::open_in_memory().unwrap();
    meta.test_index_hold_in_dropped_tx(5, 1, "a").unwrap();
    assert!(meta.member_copy_touches(&a), "indexed before the commit");
    assert!(meta.reconcile_read_holds());
    assert!(!meta.member_copy_touches(&a), "reconciled");
    // The thread's next flush does the same.
    meta.test_index_hold_in_dropped_tx(5, 1, "a").unwrap();
    meta.flush_held_releases_for_tests();
    assert!(!meta.member_copy_touches(&a), "flushed");
    // A committed hold stays through both.
    let meta = Meta::open_in_memory().unwrap();
    deposed_with_a_delegate_row(&meta);
    let a_key = [ReadKey::Dentry(ROOT_INO, "a".into())];
    assert!(!meta.reconcile_read_holds());
    meta.flush_held_releases_for_tests();
    assert!(meta.member_copy_touches(&a_key));
}

/// ADR-57 review SHOULD-4: a hold indexed after a read took the hold epoch
/// (and passed its wait) is a race the read re-checks; one indexed before,
/// or on other keys, is not.
#[test]
fn a_read_that_waited_before_a_hold_was_indexed_sees_the_race() {
    let a = [ReadKey::Dentry(ROOT_INO, "a".into())];
    let z = [ReadKey::Dentry(ROOT_INO, "z".into())];
    let meta = Meta::open_in_memory().unwrap();
    let e = meta.hold_epoch();
    assert!(!meta.hold_raced(e, &a));
    deposed_with_a_delegate_row(&meta);
    assert!(meta.hold_raced(e, &a), "a hold of its keys came in");
    assert!(!meta.hold_raced(e, &z), "other keys: no race");
    assert!(
        !meta.hold_raced(meta.hold_epoch(), &a),
        "taken after: held, not raced"
    );
}

/// ADR-57 review SHOULD-6: a lock grant's floor wait (`session_wait_floor`)
/// does not run the hold phase: it returns at once over a held key, where
/// the read itself waits the span out and is refused.
#[test]
fn a_floor_wait_does_not_wait_for_a_hold() {
    let meta = Meta::open_in_memory().unwrap();
    deposed_with_a_delegate_row(&meta);
    meta.set_read_hold_span(std::time::Duration::from_millis(300));
    let a = [ReadKey::Dentry(ROOT_INO, "a".into())];
    let t = std::time::Instant::now();
    let w = meta.session_wait_floor(&a, &Position::ZERO);
    assert!(
        !matches!(
            w,
            constellation_meta::SessionWait::Unserved(_)
                | constellation_meta::SessionWait::Interrupted(_)
        ),
        "{w:?}"
    );
    assert!(t.elapsed() < std::time::Duration::from_millis(150));
    assert!(matches!(
        meta.session_wait_at(&a, &Position::ZERO),
        constellation_meta::SessionWait::Unserved(_)
    ));
}

/// ADR-57 review SHOULD-7: a read waiting for a held row ends with its
/// caller's interrupt, long before the read hold span.
#[test]
fn a_held_read_ends_with_the_callers_interrupt() {
    let meta = Meta::open_in_memory().unwrap();
    deposed_with_a_delegate_row(&meta);
    meta.set_read_hold_span(std::time::Duration::from_secs(60));
    let a = [ReadKey::Dentry(ROOT_INO, "a".into())];
    let cancelled = std::sync::atomic::AtomicBool::new(false);
    let t = std::time::Instant::now();
    let w = std::thread::scope(|sc| {
        sc.spawn(|| {
            std::thread::sleep(std::time::Duration::from_millis(100));
            cancelled.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        meta.session_wait_cancellable(&a, &Position::ZERO, &|| {
            cancelled.load(std::sync::atomic::Ordering::SeqCst)
        })
    });
    assert!(
        matches!(w, constellation_meta::SessionWait::Interrupted(_)),
        "{w:?}"
    );
    assert!(t.elapsed() < std::time::Duration::from_secs(5));
    assert!(meta.member_copy_touches(&a), "the hold stays");
}

/// ADR-57: a row that never leaves the member copy (a leak) does not let a
/// read of its keys through: after the read hold span the read is refused
/// (`Unserved`, the engine's `EIO`), never answered without the row, whatever
/// the session budget (a budget of 0 included); the hold stays until the
/// row is released, and an unrelated key answers at once.
#[test]
fn a_held_read_is_refused_after_the_span_never_answered() {
    use constellation_meta::{Admission, SessionWait};
    use std::time::Duration;
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    holder_create(&meta, RowCommit::ROOT_GROUP, "data", 1, 1);
    meta.set_holder_epoch(0);
    let key = [ReadKey::Dentry(ROOT_INO, "data".into())];
    let dir = [ReadKey::Dir(ROOT_INO)];
    let other = [ReadKey::Dentry(ROOT_INO, "z".into())];
    apply_rows(
        &meta,
        1,
        2,
        2,
        &[1, 2],
        &[create("b", ino(2), 11), completed(rid(2))],
    );
    assert!(meta.member_copy_carries(rid(1)).unwrap());
    meta.set_read_hold_span(Duration::from_millis(60));
    assert_eq!(meta.read_admission(&key), Admission::Held);
    assert_eq!(
        meta.read_admission(&dir),
        Admission::Held,
        "its parent's listing too"
    );
    assert_eq!(meta.read_admission(&other), Admission::Ready);
    assert!(matches!(
        meta.session_wait(&other),
        SessionWait::Fast | SessionWait::Covered
    ));
    for budget_ms in [2_000, 0] {
        meta.session().set_budget_ms(budget_ms);
        let t = std::time::Instant::now();
        match meta.session_wait(&key) {
            SessionWait::Unserved(d) => assert!(d >= Duration::from_millis(60), "{d:?}"),
            other => panic!("budget {budget_ms}: a held read was answered: {other:?}"),
        }
        assert!(t.elapsed() >= Duration::from_millis(60));
    }
    let stats = meta.read_hold_stats();
    assert_eq!((stats.holds, stats.refused), (1, 2), "{stats:?}");
    // Still held: the cap that used to let it through is gone.
    assert_eq!(meta.read_admission(&key), Admission::Held);
    assert!(meta.member_copy_carries(rid(1)).unwrap());
    // The re-ship releases it, and a waiting read is let go meanwhile.
    meta.set_read_hold_span(Duration::from_secs(30));
    meta.session().set_budget_ms(2_000);
    std::thread::scope(|scope| {
        let waiter = scope.spawn(|| meta.session_wait(&key));
        std::thread::sleep(Duration::from_millis(100));
        let reship = vec![
            LogRecord::TailOf { epoch: 1, jseq: 1 },
            create("data", ino(1), 10),
            completed(rid(1)),
        ];
        apply_rows(&meta, 2, 2, 5, &[3, 4, 5], &reship);
        let waited = waiter.join().unwrap();
        assert!(
            matches!(
                waited,
                SessionWait::Waited(_) | SessionWait::Fast | SessionWait::Covered
            ),
            "{waited:?}"
        );
    });
    assert_eq!(meta.read_admission(&key), Admission::Ready);
}

/// The successor's copy of the rows it re-ships (`member_copy_put`, after it
/// applied them) and a claimant's seal snapshot are in its namespace: no
/// read of their keys waits.
#[test]
fn a_row_the_namespace_already_has_does_not_hold_its_readers() {
    let meta = Meta::open_in_memory().unwrap();
    let r = create("data", ino(1), 10);
    apply_rows(&meta, 1, 1, 2, &[1, 2], &[r.clone(), completed(rid(1))]);
    assert!(meta.lookup(ROOT_INO, "data").unwrap().is_some());
    let row = mtx(1, 2, RowCommit::ROOT_GROUP, vec![r, completed(rid(1))]);
    meta.member_copy_put_rows(1, 1, &[row]).unwrap();
    assert!(meta.member_copy_carries(rid(1)).unwrap());
    assert!(!meta.member_copy_touches(&[ReadKey::Dentry(ROOT_INO, "data".into())]));
    assert!(!meta.member_copy_touches(&[ReadKey::Dir(ROOT_INO)]));
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
    meta.install_streamed_committed(1, HOLDER, 1, 2, &tx1, true, (0, 0))
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
/// abandoned. A committed streamed transaction that no tail names (plan 46
/// M1.4 step 4: a hold member's copy of what it acknowledged, the owner
/// retired) is the only copy left: it is queued as a committed replay.
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
            (0, 0),
        )
        .unwrap();
        let stranded = meta.strand_for_takeover(2, &[], abandoned).unwrap();
        assert_eq!(raw_ns(&meta), before, "rolled back");
        assert!(!meta.has_outstanding_speculation());
        match abandoned {
            None => {
                assert_eq!(stranded.superseded, 1);
                assert_eq!(stranded.committed_rolled_back, 1);
                let queued = meta.pending_replays().unwrap();
                assert_eq!(queued.len(), 1, "{queued:?}");
                assert_eq!(queued[0].rid, rid(3));
            }
            Some(_) => {
                assert_eq!(stranded.superseded, 0);
                assert_eq!(stranded.committed_rolled_back, 2);
                assert_eq!(meta.pending_replays().unwrap().len(), 2);
            }
        }
    }
}

/// The claimant's own takeover gate keeps no copy of an own `Archive` row of
/// an older epoch that the log does not carry: its marker landed first, so the
/// row was never committed nor answered (rule 4's reason, which the claimant
/// never applies to its own marker). Rolled back, not queued; the requester
/// retries by rid. The drop survives a restart (no copy comes back).
#[test]
fn the_takeover_gate_keeps_no_copy_of_an_own_archive_row_of_an_older_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("m.db");
    {
        let meta = Meta::open(&db).unwrap();
        let before = raw_ns(&meta);
        meta.set_holder_epoch(1);
        holder_create(&meta, RowCommit::group(3, GroupOn::Archive), "a", 1, 1);
        meta.set_holder_epoch(0);
        let stranded = meta.strand_for_takeover(2, &[], None).unwrap();
        assert_eq!(stranded.archive_dropped, 1);
        assert_eq!(stranded.member_copied, 0);
        assert!(stranded.any());
        assert_eq!(stranded.committed_rolled_back, 0);
        assert_eq!(meta.member_copy_rows(1).unwrap(), None);
        assert!(meta.pending_replays().unwrap().is_empty());
        assert_eq!(meta.completed_position(rid(1)).unwrap(), None);
        assert_eq!(raw_ns(&meta), before, "rolled back");
        assert_eq!(meta.journal_len().unwrap(), 0);
        meta.close().unwrap();
    }
    let meta = Meta::open(&db).unwrap();
    assert!(meta.member_copy_epochs().unwrap().is_empty());
}

/// Control: the gate still copies an own root-group row (kept until
/// coverage; it may be the inherited tail of a tenure that has not shipped).
#[test]
fn the_takeover_gate_still_copies_an_own_root_group_row() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    holder_create(&meta, RowCommit::ROOT_GROUP, "a", 1, 1);
    meta.set_holder_epoch(0);
    let stranded = meta.strand_for_takeover(2, &[], None).unwrap();
    assert_eq!(stranded.member_copied, 1);
    assert_eq!(stranded.archive_dropped, 0);
    assert!(meta.member_copy_rows(1).unwrap().is_some());
}

/// Control (scope): a deposition still copies the same `Archive` row; the
/// successor's marker drops it by rule 4.
#[test]
fn a_deposition_still_copies_an_own_archive_row() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    holder_create(&meta, RowCommit::group(3, GroupOn::Archive), "a", 1, 1);
    meta.set_holder_epoch(0);
    let stranded = meta.strand_below_epoch(2).unwrap();
    assert_eq!(stranded.member_copied, 1);
    assert_eq!(stranded.archive_dropped, 0);
    assert!(meta.member_copy_rows(1).unwrap().is_some());
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
    holder_create(&meta, RowCommit::ROOT_GROUP, "g", 4, 4);
    holder_create(&meta, RowCommit::LONE, "l", 5, 5);
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

/// ADR-57 (P3): a rebuild from the log replaces the namespace with one that
/// lacks this node's acknowledged rows the log does not carry yet (a gated own
/// row; a delegate row this root appended): reads of their keys are held. A
/// log that carries them holds nothing.
#[test]
fn a_rebuild_holds_reads_of_the_acknowledged_rows_its_log_lacks() {
    let g = [ReadKey::Dentry(ROOT_INO, "g".into())];
    let d = [ReadKey::Dentry(ROOT_INO, "d".into())];
    let other = [ReadKey::Dentry(ROOT_INO, "z".into())];
    let build = || {
        let meta = Meta::open_in_memory().unwrap();
        meta.set_holder_epoch(1);
        holder_create(&meta, RowCommit::ROOT_GROUP, "g", 4, 4);
        meta.apply_delegate_tx(
            &[create("d", ino(6), 12), completed(rid(6))],
            Some(rid(6)),
            5,
            1,
            Position::ZERO,
        )
        .unwrap();
        meta.set_holder_epoch(0);
        meta
    };
    // The log lacks both.
    let meta = build();
    let side = Meta::open_in_memory().unwrap();
    meta.replace_ns_from_rebuilt(&side, &Default::default())
        .unwrap();
    assert!(meta.lookup(ROOT_INO, "g").unwrap().is_none());
    assert!(meta.lookup(ROOT_INO, "d").unwrap().is_none());
    assert!(meta.member_copy_touches(&g), "the gated row is held");
    assert!(meta.member_copy_touches(&d), "the delegate row is held");
    assert!(!meta.member_copy_touches(&other));
    // The log carries both: the rebuilt namespace has them, nothing is held.
    let meta = build();
    let side = Meta::open_in_memory().unwrap();
    side.apply_segment_rows(
        1,
        2,
        3,
        &[1, 2, 3],
        &[(0, 0), (0, 0), (5, 1)],
        &[
            LogRecord::TailOf { epoch: 1, jseq: 1 },
            create("g", ino(4), 10),
            create("d", ino(6), 12),
        ],
        &TouchSet::default(),
    )
    .unwrap();
    meta.replace_ns_from_rebuilt(&side, &Default::default())
        .unwrap();
    assert!(meta.lookup(ROOT_INO, "g").unwrap().is_some());
    assert!(meta.lookup(ROOT_INO, "d").unwrap().is_some());
    assert!(!meta.member_copy_touches(&g));
    assert!(!meta.member_copy_touches(&d));
}

/// A rebuild retires a committed streamed transaction that the log it was
/// rebuilt from carries, whether its own epoch's segment shipped it or a
/// successor re-shipped it (`TailOf`) — also when it completes no rid. Fails
/// on the parent: such an entry was re-installed on a namespace that holds
/// it already, its records applied a second time.
#[test]
fn a_rebuild_retires_a_committed_streamed_entry_the_log_carries() {
    let tx1 = vec![create("a", ino(1), 10)];
    let reship = vec![
        LogRecord::TailOf { epoch: 1, jseq: 1 },
        create("a", ino(1), 10),
    ];
    for (epoch, recs) in [(1u64, tx1.clone()), (2, reship)] {
        let meta = Meta::open_in_memory().unwrap();
        meta.install_streamed_committed(1, HOLDER, 1, 1, &tx1, true, (0, 0))
            .unwrap();
        let side = Meta::open_in_memory().unwrap();
        apply_rows(&side, 1, epoch, 1, &[1], &recs);
        let rebuilt = meta.replace_ns_from_rebuilt(&side, &Default::default());
        assert!(rebuilt.is_ok(), "epoch {epoch}: {rebuilt:?}");
        assert!(
            !meta.has_outstanding_speculation(),
            "epoch {epoch}: the entry the log carries is retired"
        );
        assert_eq!(raw_ns(&meta), raw_ns(&side), "epoch {epoch}: applied once");
    }
    // Not carried (the log stops before it): re-installed, as before.
    let meta = Meta::open_in_memory().unwrap();
    meta.install_streamed_committed(1, HOLDER, 2, 2, &tx1, true, (0, 0))
        .unwrap();
    let side = Meta::open_in_memory().unwrap();
    apply_rows(&side, 1, 1, 1, &[1], &[create("b", ino(2), 9)]);
    meta.replace_ns_from_rebuilt(&side, &Default::default())
        .unwrap();
    assert!(meta.has_outstanding_speculation());
    assert!(meta.lookup(ROOT_INO, "a").unwrap().is_some());
}

/// A delegate's committed answer (`gen` != 0: its generation's group held
/// the row) installs a committed shadow too: a later root epoch's segment
/// does not strand it (the delegate re-streams the row to the next root),
/// and the segment that completes its rid retires it. Fails on the parent
/// (placement-hot seeds 539, 1746, 1798: only a root's answer made a
/// committed shadow, and the requester rolled the acknowledged row back and
/// queued it at the root's takeover).
#[test]
fn a_delegates_committed_shadow_survives_a_root_takeover() {
    let meta = Meta::open_in_memory().unwrap();
    let recs = vec![create("a", ino(1), 10), completed(rid(1))];
    assert!(meta
        .install_shadow_committed(
            rid(1),
            1,
            7,
            &create_op("a", ino(1)),
            &Default::default(),
            &recs,
            true
        )
        .unwrap());
    let other = vec![create("b", ino(2), 11), completed(rid(2))];
    apply_rows(&meta, 1, 2, 2, &[1, 2], &other);
    assert!(meta.has_outstanding_speculation(), "not stranded");
    assert!(meta.pending_replays().unwrap().is_empty());
    assert!(meta.lookup(ROOT_INO, "a").unwrap().is_some());
    // The next root appends the delegate's re-streamed row.
    meta.apply_segment_rows(
        2,
        2,
        4,
        &[3, 4],
        &[(7, 1), (7, 1)],
        &recs,
        &TouchSet::default(),
    )
    .unwrap();
    assert!(!meta.has_outstanding_speculation());
    assert!(meta.pending_replays().unwrap().is_empty());
    assert!(meta.lookup(ROOT_INO, "a").unwrap().is_some());
}

/// The takeover gate takes a delegate's committed shadow out only when the
/// tail it re-applies carries the row (the predecessor appended it); one it
/// does not carry stays, for the delegate's re-stream to replace in place.
/// Fails on the parent (the shadow was stranded and queued).
#[test]
fn the_takeover_gate_keeps_a_delegates_committed_shadow_its_tail_lacks() {
    for carried in [false, true] {
        let meta = Meta::open_in_memory().unwrap();
        let recs = vec![create("a", ino(1), 10), completed(rid(1))];
        meta.install_shadow_committed(
            rid(1),
            1,
            7,
            &create_op("a", ino(1)),
            &Default::default(),
            &recs,
            true,
        )
        .unwrap();
        let tail: std::collections::HashSet<Rid> = if carried {
            [rid(1)].into()
        } else {
            Default::default()
        };
        let stranded = meta
            .strand_for_takeover_carrying(2, &[], None, &tail, &Default::default())
            .unwrap();
        assert!(
            meta.pending_replays().unwrap().is_empty(),
            "carried {carried}"
        );
        if carried {
            assert_eq!(stranded.superseded, 1);
            assert!(!meta.has_outstanding_speculation());
        } else {
            assert!(!stranded.any());
            assert!(meta.has_outstanding_speculation());
            assert!(meta.lookup(ROOT_INO, "a").unwrap().is_some());
        }
    }
}

/// A rebuild of the namespace from a log that re-shipped rows of an epoch
/// (`TailOf`) drops the member copy this node already held of them: no
/// segment applied here runs the drop rules for that re-ship. Fails on the
/// parent (placement-hot seed 904: the copy outlived the archive).
#[test]
fn a_rebuild_drops_the_member_rows_its_log_re_shipped() {
    let meta = Meta::open_in_memory().unwrap();
    put(
        &meta,
        &[
            mtx(4, 4, RowCommit::ROOT_GROUP, vec![create("a", ino(1), 10)]),
            mtx(5, 5, RowCommit::ROOT_GROUP, vec![create("b", ino(2), 11)]),
        ],
    );
    let side = Meta::open_in_memory().unwrap();
    let reship = vec![
        LogRecord::TailOf { epoch: 1, jseq: 4 },
        create("a", ino(1), 10),
    ];
    apply_rows(&side, 1, 2, 2, &[1, 2], &reship);
    meta.replace_ns_from_rebuilt(&side, &Default::default())
        .unwrap();
    let (_, rows) = meta.member_copy_rows(1).unwrap().expect("one row is left");
    assert_eq!(
        rows.iter().map(|r| r.tx.first).collect::<Vec<_>>(),
        [5],
        "the re-shipped row left, the other stays"
    );
    let reship = vec![
        LogRecord::TailOf { epoch: 1, jseq: 5 },
        create("b", ino(2), 11),
    ];
    let side2 = Meta::open_in_memory().unwrap();
    apply_rows(&side2, 1, 2, 2, &[1, 2], &reship);
    meta.replace_ns_from_rebuilt(&side2, &Default::default())
        .unwrap();
    assert_eq!(meta.member_copy(1).unwrap(), None);
}

/// A committed stream entry of an earlier tenure retires by the segment that
/// completes (or refuses) its rid under no identity it names: the next root
/// appended the delegate's re-stream of a row the old root had appended and
/// streamed, from no tail of the old tenure (a claimant's tail fetched from a
/// member copy holds none of a delegate's rows). The segment's copy is
/// applied once. The completion's fails on main (the entry was redone over
/// the segment and stayed outstanding for good); the refusal's retired by the
/// segment's rewind already, and is matched by its rid now too.
#[test]
fn a_committed_stream_entry_retires_by_its_rid_without_a_tail_identity() {
    for refusal in [false, true] {
        let tx1 = if refusal {
            vec![LogRecord::Refused {
                rid: rid(1),
                code: constellation_types::Code::Exists,
            }]
        } else {
            vec![create("a", ino(1), 10), completed(rid(1))]
        };
        let rows: Vec<u64> = (1..=tx1.len() as u64).collect();
        let reference = Meta::open_in_memory().unwrap();
        apply_rows(&reference, 1, 2, rows.len() as u64, &rows, &tx1);
        let meta = Meta::open_in_memory().unwrap();
        meta.install_streamed_committed(1, HOLDER, 5, 4 + tx1.len() as u64, &tx1, true, (0, 0))
            .unwrap();
        apply_rows(&meta, 1, 2, 0, &[], &[]);
        assert!(meta.has_outstanding_speculation(), "refusal {refusal}");
        let origins = vec![(7u64, 1u64); tx1.len()];
        meta.apply_segment_rows(
            2,
            2,
            rows.len() as u64,
            &rows,
            &origins,
            &tx1,
            &TouchSet::default(),
        )
        .unwrap();
        assert!(!meta.has_outstanding_speculation(), "refusal {refusal}");
        assert!(meta.pending_replays().unwrap().is_empty());
        // (The root's own times are the wall clock of the apply; the
        // namespace below it is what the log alone produces.)
        let names = |m: &Meta| -> Vec<(String, u64)> {
            m.readdir(ROOT_INO)
                .unwrap()
                .into_iter()
                .map(|e| (e.name, e.ino))
                .collect()
        };
        assert_eq!(names(&meta), names(&reference), "refusal {refusal}");
    }
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
            mtx(1, 2, RowCommit::ROOT_GROUP, vec![create("a", ino(1), 10)]),
            mtx(3, 3, RowCommit::ROOT_GROUP, vec![create("b", ino(2), 11)]),
            mtx(4, 4, RowCommit::ROOT_GROUP, vec![create("c", ino(3), 12)]),
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
            mtx(1, 1, RowCommit::ROOT_GROUP, vec![create("a", ino(1), 10)]),
            mtx(2, 2, RowCommit::ROOT_GROUP, vec![create("b", ino(2), 11)]),
            mtx(3, 3, RowCommit::ROOT_GROUP, vec![create("c", ino(3), 12)]),
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

/// A deposed holder's two committed rows, rolled back into the member copy of
/// epoch 1 with a read hold each (`strand_below_epoch`).
fn deposed_with_two_rows(meta: &Meta) -> [Vec<ReadKey>; 2] {
    meta.set_holder_epoch(1);
    holder_create(meta, RowCommit::ROOT_GROUP, "a", 1, 1);
    holder_create(meta, RowCommit::ROOT_GROUP, "b", 2, 2);
    meta.set_holder_epoch(0);
    assert_eq!(meta.strand_below_epoch(2).unwrap().member_copied, 2);
    let keys = [
        vec![ReadKey::Dentry(ROOT_INO, "a".into())],
        vec![ReadKey::Dentry(ROOT_INO, "b".into())],
    ];
    assert!(meta.member_copy_touches(&keys[0]) && meta.member_copy_touches(&keys[1]));
    keys
}

/// A claim of epoch 3 over an epoch 2 whose marker never landed carries no
/// tail (`TailFollowsThrough { 2, 0 }`): the rows epoch 1's deposed holder
/// kept are no one's to re-ship. Their copies go with their read holds, or
/// the holds outlive the run and reads of the keys fail at the bound.
/// (Fails before: only the epoch the marker names was settled, and a
/// group-committed row of an older one stayed for good.)
#[test]
fn a_claim_without_a_tail_drops_the_older_epochs_copies_and_holds() {
    let meta = Meta::open_in_memory().unwrap();
    let keys = deposed_with_two_rows(&meta);
    // A marker that names a tail it carries leaves the copies to it.
    let named = vec![LogRecord::TailFollowsThrough {
        prev_epoch: 2,
        through: 9,
    }];
    apply_rows(&meta, 1, 3, 0, &[], &named);
    assert_eq!(meta.member_copy_rows(1).unwrap().unwrap().1.len(), 2);
    assert!(meta.member_copy_touches(&keys[0]));
    let none = vec![LogRecord::TailFollowsThrough {
        prev_epoch: 2,
        through: 0,
    }];
    let applied = meta
        .apply_segment_rows(2, 4, 0, &[], &[], &none, &TouchSet::default())
        .unwrap();
    assert_eq!(applied.member.dropped, 2);
    assert_eq!(meta.member_copy(1).unwrap(), None);
    assert!(meta.read_holds().is_empty(), "{:?}", meta.read_holds());
    assert!(!meta.member_copy_touches(&keys[0]) && !meta.member_copy_touches(&keys[1]));
}

/// ADR-57 round-2 MUST-2: the rule of a claim without a tail is a fact the
/// replica keeps (`member_settled_below`), so a copy made after the marker was
/// applied is judged by it too. A namespace rebuilt from a log that carries
/// the marker (`dcrash-pause` seed 558) adopts it: the rows its reset moves
/// out of the journal are not kept as a copy, and not held (before: the
/// epoch-1 row stayed with its hold, and reads of its keys failed at the
/// bound for good); copies already there go.
#[test]
fn a_rebuild_from_a_log_with_a_no_tail_marker_keeps_no_older_copy_nor_hold() {
    let side = || {
        let side = Meta::open_in_memory().unwrap();
        let none = vec![LogRecord::TailFollowsThrough {
            prev_epoch: 2,
            through: 0,
        }];
        apply_rows(&side, 1, 3, 0, &[], &none);
        side
    };
    let a = [ReadKey::Dentry(ROOT_INO, "a".into())];
    // Rows still in the journal when the rebuild strands them.
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    holder_create(&meta, RowCommit::ROOT_GROUP, "a", 1, 1);
    meta.set_holder_epoch(0);
    meta.replace_ns_from_rebuilt(&side(), &Default::default())
        .unwrap();
    assert_eq!(meta.member_copy(1).unwrap(), None, "no copy of epoch 1");
    assert!(meta.read_holds().is_empty(), "{:?}", meta.read_holds());
    assert!(!meta.member_copy_touches(&a));
    // Copies made before the rebuild go with it.
    let meta = Meta::open_in_memory().unwrap();
    let keys = deposed_with_two_rows(&meta);
    meta.replace_ns_from_rebuilt(&side(), &Default::default())
        .unwrap();
    assert_eq!(meta.member_copy(1).unwrap(), None);
    assert!(meta.read_holds().is_empty(), "{:?}", meta.read_holds());
    assert!(!meta.member_copy_touches(&keys[0]));
    // An acknowledged row lost by the rule is counted as abandoned.
    assert_eq!(meta.read_hold_stats().abandoned, 2);
}

/// The fact outlives the segment: a strand that runs after a marker was
/// applied in an earlier transaction judges its rows by it.
#[test]
fn a_strand_after_a_no_tail_marker_was_applied_keeps_no_older_copy() {
    let meta = Meta::open_in_memory().unwrap();
    let none = vec![LogRecord::TailFollowsThrough {
        prev_epoch: 2,
        through: 0,
    }];
    // (A copy-less replica applies the marker; nothing to drop yet.)
    apply_rows(&meta, 1, 3, 0, &[], &none);
    // The rows of epoch 1 come to the journal after, and a later pass
    // strands them: the marker was applied in an earlier transaction.
    meta.set_holder_epoch(1);
    holder_create(&meta, RowCommit::ROOT_GROUP, "a", 1, 1);
    meta.set_holder_epoch(0);
    meta.strand_below_epoch(4).unwrap();
    assert_eq!(meta.member_copy(1).unwrap(), None);
    assert!(meta.read_holds().is_empty());
    // A marker that named a tail of the epoch leaves it to that tail.
    let meta = Meta::open_in_memory().unwrap();
    let named = vec![LogRecord::TailFollowsThrough {
        prev_epoch: 2,
        through: 9,
    }];
    apply_rows(&meta, 1, 3, 0, &[], &named);
    meta.set_holder_epoch(1);
    holder_create(&meta, RowCommit::ROOT_GROUP, "a", 1, 1);
    meta.set_holder_epoch(0);
    meta.strand_below_epoch(4).unwrap();
    assert!(meta.member_copy(1).unwrap().is_some());
    assert_eq!(meta.read_holds().len(), 1);
}

fn takeover_with_marker(meta: &Meta, epoch: u64, marker: &[LogRecord]) {
    let carried = Default::default();
    let proof = Default::default();
    meta.reship_at_takeover(
        Some(constellation_meta::StrandArgs {
            epoch,
            own_gens: &[],
            abandoned: None,
            carried: &carried,
            proof: &proof,
            marker,
        }),
        None,
    )
    .unwrap();
}

/// The claimant never applies its own marker as a segment: its own copies of
/// rows past a sealed `through` (or of the epochs a claim without a tail
/// settles) go at its gate, with their holds. (Fails before: the claimant's
/// copy and hold of an epoch-1 row nobody re-ships stayed for good,
/// `locks-unlinked-delegated-dcrash-pause` seeds 651, 5767.)
#[test]
fn the_claimant_applies_its_own_markers_drops_to_its_copies_and_holds() {
    // No tail at all: both rows go.
    let meta = Meta::open_in_memory().unwrap();
    let keys = deposed_with_two_rows(&meta);
    takeover_with_marker(
        &meta,
        3,
        &[LogRecord::TailFollowsThrough {
            prev_epoch: 2,
            through: 0,
        }],
    );
    assert_eq!(meta.member_copy(1).unwrap(), None);
    assert!(meta.read_holds().is_empty(), "{:?}", meta.read_holds());
    assert!(!meta.member_copy_touches(&keys[0]) && !meta.member_copy_touches(&keys[1]));
    // A tail of epoch 1 that reaches the first row only: the second row,
    // past `through`, goes; the first stays held until its re-ship.
    let meta = Meta::open_in_memory().unwrap();
    let keys = deposed_with_two_rows(&meta);
    let (_, rows) = meta.member_copy_rows(1).unwrap().unwrap();
    let through = rows[0].tx.last;
    takeover_with_marker(
        &meta,
        2,
        &[LogRecord::TailFollowsThrough {
            prev_epoch: 1,
            through,
        }],
    );
    let (_, rows) = meta.member_copy_rows(1).unwrap().unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        meta.member_copy_touches(&keys[0]),
        "the carried row stays held"
    );
    assert!(
        !meta.member_copy_touches(&keys[1]),
        "the row past through is dropped"
    );
    assert_eq!(meta.read_holds().len(), 1);
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
            mtx(1, 1, RowCommit::ROOT_GROUP, vec![create("a", ino(1), 10)]),
            mtx(2, 2, RowCommit::ARCHIVE, vec![create("b", ino(2), 11)]),
            mtx(3, 3, RowCommit::LONE, vec![create("c", ino(3), 12)]),
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
/// Every member of an abandoned group that holds a transaction completing
/// no rid (a local manifest commit, a snapshot row) queues it under the same
/// rid, the transaction's identity, so that the holder executing the replays
/// runs it once whichever members come back. Fails on the parent: each node
/// queued it under a rid of its own (`node prefix`, journal seq), and the
/// holder executed it once per member.
#[test]
fn an_abandoned_rid_less_row_is_queued_under_one_rid_on_every_member() {
    let queued_on = |node: u64, epoch: u64, records: Vec<LogRecord>| -> Vec<Rid> {
        let meta = Meta::open_in_memory().unwrap();
        meta.set_node_prefix(node).unwrap();
        meta.member_copy_put_rows(epoch, 3, &[mtx(3, 3, RowCommit::ROOT_GROUP, records)])
            .unwrap();
        let marker = vec![LogRecord::GroupAbandoned {
            epoch,
            gen: 0,
            through_known: 0,
        }];
        let applied = meta
            .apply_segment_rows(1, epoch + 1, 0, &[], &[], &marker, &TouchSet::default())
            .unwrap();
        assert_eq!(applied.member.abandoned, 1, "node {node}");
        meta.pending_replays()
            .unwrap()
            .iter()
            .map(|q| q.rid)
            .collect()
    };
    let plain = vec![create("m", ino(7), 10)];
    let holder = queued_on(HOLDER, 1, plain.clone());
    assert_eq!(holder.len(), 1);
    assert_eq!(
        queued_on(5, 1, plain),
        holder,
        "a backup queues the same rid"
    );
    // A re-shipped transaction: its identity is its `TailOf`'s, on every
    // member, of whichever epoch's copy (here a successor's, epoch 2, which
    // re-journaled epoch 1's row 3 as its own row 3).
    let reshipped = vec![
        LogRecord::TailOf { epoch: 1, jseq: 3 },
        create("m", ino(7), 10),
    ];
    assert_eq!(queued_on(6, 2, reshipped), holder);
}

#[test]
fn group_abandoned_moves_the_member_copy_to_the_replay_queue() {
    let meta = Meta::open_in_memory().unwrap();
    let mut row1 = mtx(
        1,
        2,
        RowCommit::ROOT_GROUP,
        vec![create("a", ino(1), 10), completed(rid(1))],
    );
    row1.rid = Some(rid(1));
    row1.op = Some(create_op("a", ino(1)));
    let row2 = mtx(
        3,
        3,
        RowCommit::ROOT_GROUP,
        vec![create("b", ino(2), 11), completed(rid(2))],
    );
    let mut lone = mtx(4, 4, RowCommit::LONE, vec![create("c", ino(3), 12)]);
    lone.rid = Some(rid(3));
    lone.op = Some(create_op("c", ino(3)));
    put(&meta, &[row1, row2, lone]);
    let marker = vec![LogRecord::GroupAbandoned {
        epoch: 1,
        gen: 0,
        through_known: 0,
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
                RowCommit::ROOT_GROUP,
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

// ------------------------------------------- a holder's drop retracts its copies

/// The copies a member holds of a transaction the holder's `repair
/// drop-held` rolled back (`RolledBack`): a successor re-appended node 2's
/// op as `TailOf { 1, 3 }` and streamed it to the forwarder as committed;
/// the drop rolled it back and the forwarder applies the record in the
/// segment that carries it. The streamed entry is rolled back and not
/// queued, its member-copy row becomes a hole (the copy stays contiguous),
/// and a row the record does not name is kept. Without the record the
/// committed entry stays for good (`strand`s only run for `gated: false`
/// entries and earlier epochs): the member is diverged and never quiesces.
#[test]
fn a_streamed_committed_row_named_by_rolled_back_is_retracted() {
    let meta = Meta::open_in_memory().unwrap();
    let before = raw_ns(&meta);
    let reship = vec![
        LogRecord::TailOf { epoch: 1, jseq: 3 },
        create("a", ino(1), 10),
        completed(rid(1)),
    ];
    meta.install_streamed_committed(2, HOLDER, 1, 3, &reship, true, (0, 0))
        .unwrap();
    assert!(meta.has_outstanding_speculation());
    // Its member copy of epoch 1, and a row of the same copy nobody rolled back.
    meta.member_copy_put_rows(
        1,
        1,
        &[
            mtx(
                3,
                3,
                RowCommit::ROOT_GROUP,
                vec![create("a", ino(1), 10), completed(rid(1))],
            ),
            mtx(
                4,
                4,
                RowCommit::ROOT_GROUP,
                vec![create("k", ino(7), 13), completed(rid(7))],
            ),
        ],
    )
    .unwrap();

    // The successor's segment: the record, then its later rows.
    let seg = vec![
        LogRecord::RolledBack {
            idents: vec![(1, 3), (9, 9)],
        },
        create("b", ino(2), 11),
        completed(rid(2)),
    ];
    apply_rows(&meta, 1, 2, 6, &[4, 5, 6], &seg);

    assert!(
        !meta.has_outstanding_speculation(),
        "the entry is retracted"
    );
    assert!(
        meta.lookup(ROOT_INO, "a").unwrap().is_none(),
        "its effect is rolled back"
    );
    assert!(meta.lookup(ROOT_INO, "b").unwrap().is_some());
    assert!(
        meta.pending_replays().unwrap().is_empty(),
        "no replay of it here: the holder's queue owns it: {:?}",
        meta.pending_replays().unwrap()
    );
    assert!(
        raw_ns(&meta).len() > before.len(),
        "b is new (and nothing of a is left)"
    );
    let (_, rows) = meta.member_copy_rows(1).unwrap().expect("the copy stays");
    assert_eq!(
        rows.iter()
            .map(|r| (r.tx.first, r.tx.records.len()))
            .collect::<Vec<_>>(),
        vec![(3, 0), (4, 2)],
        "the rolled-back row is a hole of the same span; the other is kept"
    );
    assert!(!meta.member_copy_carries(rid(1)).unwrap());
    assert!(meta.member_copy_carries(rid(7)).unwrap());
}

/// The same record, streamed (the transaction that carries the drop's
/// `Refused` rows reaches the forwarder before any segment does): the
/// committed entry is retracted at the install, an own op's entry included,
/// and an uncommitted one too.
#[test]
fn a_streamed_rolled_back_record_retracts_committed_and_own_entries() {
    let meta = Meta::open_in_memory().unwrap();
    let a = vec![create("a", ino(1), 10), completed(rid(1))];
    let b = vec![create("b", ino(2), 11), completed(rid(2))];
    meta.install_streamed_committed(1, HOLDER, 1, 2, &a, true, (0, 0))
        .unwrap();
    meta.install_streamed_committed(1, HOLDER, 3, 4, &b, false, (0, 0))
        .unwrap();
    // Node 2's own op (a shadow its reply installed), adopted by the stream.
    let c = vec![create("c", ino(3), 12), completed(rid(3))];
    meta.install_shadow_committed(
        rid(3),
        1,
        0,
        &create_op("c", ino(3)),
        &Default::default(),
        &c,
        true,
    )
    .unwrap();
    meta.install_streamed_committed(1, HOLDER, 5, 6, &c, true, (0, 0))
        .unwrap();
    assert!(meta.has_outstanding_speculation());

    let drop_tx = vec![
        LogRecord::RolledBack {
            idents: vec![(1, 1), (1, 3), (1, 5)],
        },
        LogRecord::Refused {
            rid: rid(9),
            code: constellation_types::Code::Io,
        },
    ];
    meta.install_streamed_committed(1, HOLDER, 7, 8, &drop_tx, true, (0, 0))
        .unwrap();
    for name in ["a", "b", "c"] {
        assert!(meta.lookup(ROOT_INO, name).unwrap().is_none(), "{name}");
    }
    assert!(
        meta.pending_replays().unwrap().is_empty(),
        "{:?}",
        meta.pending_replays().unwrap()
    );
    // Only the record's own transaction is left outstanding.
    meta.install_streamed_committed(1, HOLDER, 9, 10, &[create("d", ino(4), 14)], true, (0, 0))
        .unwrap();
    assert!(meta.lookup(ROOT_INO, "d").unwrap().is_some());
}

/// A backup's tail: the record, appended, turns the tail transactions of
/// the identities it names into holes, whether named by their place or by
/// their `TailOf`; the acknowledgement stays contiguous.
#[test]
fn an_appended_rolled_back_record_turns_the_tail_transactions_into_holes() {
    let meta = Meta::open_in_memory().unwrap();
    let tx = |first: u64, last: u64, records: Vec<LogRecord>| BackupTx {
        first,
        last,
        records,
        origin: (0, 0),
    };
    meta.backup_append(
        2,
        &[
            tx(1, 2, vec![create("a", ino(1), 10), completed(rid(1))]),
            tx(
                3,
                4,
                vec![
                    LogRecord::TailOf { epoch: 1, jseq: 7 },
                    create("b", ino(2), 11),
                    completed(rid(2)),
                ],
            ),
            tx(5, 6, vec![create("c", ino(3), 12), completed(rid(3))]),
        ],
    )
    .unwrap();
    meta.backup_append(
        2,
        &[tx(
            7,
            8,
            vec![
                LogRecord::RolledBack {
                    idents: vec![(2, 1), (1, 7)],
                },
                LogRecord::Refused {
                    rid: rid(9),
                    code: constellation_types::Code::Io,
                },
            ],
        )],
    )
    .unwrap();
    let tail = meta.backup_tail(2).unwrap();
    assert_eq!(
        tail.iter()
            .map(|t| (t.first, t.last, t.records.len()))
            .collect::<Vec<_>>(),
        vec![(1, 2, 0), (3, 4, 0), (5, 6, 2), (7, 8, 2)],
    );
    assert_eq!(meta.backup_acked(2).unwrap(), 8, "contiguous still");
}

/// A `RolledBack` record that reaches a member on an uncommitted stream (an
/// epoch hold's tip, a lone tenure's) retracts nothing: the dropping holder
/// may be deposed or dead before the transaction commits, and the member's
/// copy alone may carry the row (its client was told it is committed). The
/// log's segment that carries the record retracts it then, and in the
/// install's own transaction when the stream says committed.
#[test]
fn an_uncommitted_rolled_back_record_retracts_nothing_until_the_log_carries_it() {
    let meta = Meta::open_in_memory().unwrap();
    let reship = vec![
        LogRecord::TailOf { epoch: 1, jseq: 3 },
        create("a", ino(1), 10),
        completed(rid(1)),
    ];
    meta.install_streamed_committed(2, HOLDER, 1, 3, &reship, true, (0, 0))
        .unwrap();
    meta.member_copy_put_rows(
        1,
        1,
        &[mtx(
            3,
            3,
            RowCommit::ROOT_GROUP,
            vec![create("a", ino(1), 10), completed(rid(1))],
        )],
    )
    .unwrap();
    let drop_tx = vec![
        LogRecord::RolledBack {
            idents: vec![(1, 3)],
        },
        LogRecord::Refused {
            rid: rid(9),
            code: constellation_types::Code::Io,
        },
    ];
    // The uncommitted stream: nothing is given up.
    meta.install_streamed_committed(2, HOLDER, 4, 5, &drop_tx, false, (0, 0))
        .unwrap();
    assert!(meta.has_outstanding_speculation());
    assert!(meta.lookup(ROOT_INO, "a").unwrap().is_some());
    assert!(
        meta.member_copy_carries(rid(1)).unwrap(),
        "the member's copy still carries the row"
    );
    // The log carries the record: now.
    apply_rows(&meta, 1, 2, 5, &[4, 5], &drop_tx);
    assert!(meta.lookup(ROOT_INO, "a").unwrap().is_none());
    assert!(!meta.member_copy_carries(rid(1)).unwrap());
}

fn streamed(meta: &Meta, epoch: u64, first: u64, last: u64, recs: &[LogRecord]) {
    meta.install_streamed_committed(epoch, HOLDER, first, last, recs, true, (0, 0))
        .unwrap();
}

/// Plan 46 M1.4 step 4 (review SHOULD-1): a claim that proves its
/// predecessor's tenure has no tail proves it of *that epoch only*: an older
/// epoch's committed copy may still ride a tail a later claim fetches (plan
/// 43 D2), so it stays; the proven epoch's own copy is the only one left and
/// strands.
#[test]
fn a_no_tail_proof_strands_only_the_epoch_it_names() {
    let meta = Meta::open_in_memory().unwrap();
    let old = vec![create("old", ino(1), 10), completed(rid(1))];
    let proven = vec![create("proven", ino(2), 11), completed(rid(2))];
    streamed(&meta, 1, 1, 1, &old);
    streamed(&meta, 2, 1, 1, &proven);
    let marker = [LogRecord::TailFollowsThrough {
        prev_epoch: 2,
        through: 0,
    }];
    meta.apply_segment_rows(1, 3, 0, &[], &[], &marker, &TouchSet::default())
        .unwrap();
    let queued = meta.pending_replays().unwrap();
    assert_eq!(queued.len(), 1, "{queued:?}");
    assert_eq!(queued[0].rid, rid(2));
    assert!(meta.lookup(ROOT_INO, "old").unwrap().is_some());
    assert!(meta.lookup(ROOT_INO, "proven").unwrap().is_none());
}

/// Plan 46 M1.4 step 4 (review SHOULD-3): a self-sealed holder re-claiming
/// its own epoch re-ships its tail over several segments. The copies the
/// first segment passes without naming stay until the re-ship names them (a
/// later segment's `TailOf`): rolled back at the first and re-applied at the
/// second they would vanish for a while, and a copy without a `Completed`
/// rid would never come back.
#[test]
fn a_reship_spanning_segments_keeps_the_copies_it_has_not_named_yet() {
    let meta = Meta::open_in_memory().unwrap();
    let a = vec![create("a", ino(1), 10), completed(rid(1))];
    let b = vec![create("b", ino(2), 11), completed(rid(2))];
    streamed(&meta, 2, 1, 1, &a);
    streamed(&meta, 2, 2, 2, &b);
    // First segment of the re-ship: the marker (the tail reaches seq 2) and
    // the first transaction, behind the old journal's seqs.
    let mut seg1 = vec![
        LogRecord::TailFollowsThrough {
            prev_epoch: 2,
            through: 2,
        },
        LogRecord::TailOf { epoch: 2, jseq: 1 },
    ];
    seg1.extend(a.iter().cloned());
    apply_rows(&meta, 1, 2, 3, &[3], &seg1);
    assert!(meta.pending_replays().unwrap().is_empty());
    assert!(meta.lookup(ROOT_INO, "a").unwrap().is_some());
    assert!(meta.lookup(ROOT_INO, "b").unwrap().is_some());
    let mut seg2 = vec![LogRecord::TailOf { epoch: 2, jseq: 2 }];
    seg2.extend(b.iter().cloned());
    apply_rows(&meta, 2, 2, 4, &[4], &seg2);
    assert!(meta.pending_replays().unwrap().is_empty());
    assert!(!meta.has_outstanding_speculation());
    assert!(meta.lookup(ROOT_INO, "b").unwrap().is_some());
}

/// ... and a copy past what the marker announced (the sealed prefix) is not
/// waited for.
#[test]
fn a_reship_does_not_wait_for_a_copy_past_the_sealed_prefix() {
    let meta = Meta::open_in_memory().unwrap();
    let a = vec![create("a", ino(1), 10), completed(rid(1))];
    let b = vec![create("b", ino(2), 11), completed(rid(2))];
    streamed(&meta, 2, 1, 1, &a);
    streamed(&meta, 2, 2, 2, &b);
    let mut seg1 = vec![
        LogRecord::TailFollowsThrough {
            prev_epoch: 2,
            through: 1,
        },
        LogRecord::TailOf { epoch: 2, jseq: 1 },
    ];
    seg1.extend(a.iter().cloned());
    apply_rows(&meta, 1, 2, 3, &[3], &seg1);
    let queued = meta.pending_replays().unwrap();
    assert_eq!(queued.len(), 1, "{queued:?}");
    assert_eq!(queued[0].rid, rid(2));
}
