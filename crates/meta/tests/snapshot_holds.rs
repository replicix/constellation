//! Plan 32 §0.4: snapshot rows carry holds and owner namespaces, and the
//! log carries them convergently.
//!
//! What is checked here is replay, not policy: a replica that tails the
//! records a writer journaled must end up with byte-identical `ns` rows,
//! whether the writer emitted the new `SnapCreate2` or an old peer emitted
//! `SnapCreate`. The *rules* about who may hold and release what live in
//! the engine and the control layer, where the role is known.

use constellation_meta::{LogRecord, Meta, MetaStore, SnapshotRow};

/// The journal tail, acked as shipped so the next call sees only what was
/// written after it (what a shipper does).
fn drain(meta: &Meta) -> Vec<LogRecord> {
    let rows = meta.take_journal(usize::MAX).unwrap();
    let seqs: Vec<u64> = rows.iter().map(|(seq, _)| *seq).collect();
    meta.ack_journal_rows_at(&seqs, 1).unwrap();
    rows.into_iter().map(|(_, record)| record).collect()
}

fn snapshot_rows(meta: &Meta) -> Vec<SnapshotRow> {
    meta.snapshots(None).unwrap()
}

/// The row a writer produces and the row a follower replays from that
/// writer's records are the same bytes — the whole point of keeping the
/// snapshot row in `ns` in published shape.
#[test]
fn a_held_snapshot_replays_byte_identically() {
    let writer = Meta::open_in_memory().unwrap();
    let follower = Meta::open_in_memory().unwrap();

    let mut row = SnapshotRow::new("id0", "/data", "daily", "mtree:3:ab:9", 1_700_000_000_000);
    row.origin = 1;
    row.policy_ino = 4096;
    row.held = true;
    row.creator = 11;
    row.held_by = Some("csi:content-uid".into());
    row.refer_bytes = Some(81 << 30);
    writer.record_snapshot(&row).unwrap();

    let records = drain(&writer);
    // Created held: the create record plus its hold, in that order, in one
    // transaction, so no replica ever sees it briefly unheld.
    assert!(
        matches!(
            records.as_slice(),
            [
                LogRecord::SnapCreate2 { id, origin: 1, policy_ino: 4096, creator: 11,
                                         refer_bytes: Some(_), .. },
                LogRecord::SnapHold { held: true, by: Some(by), .. },
            ] if id == "id0" && by == "csi:content-uid"
        ),
        "{records:?}"
    );
    follower.apply_records(&records).unwrap();
    assert_eq!(snapshot_rows(&follower), snapshot_rows(&writer));
    assert_eq!(snapshot_rows(&follower).first().unwrap(), &row);

    // And so does releasing it.
    writer
        .set_snapshot_hold("id0", false, Some("csi:content-uid"), false)
        .unwrap();
    let records = drain(&writer);
    assert!(
        matches!(
            records.as_slice(),
            [LogRecord::SnapHold {
                held: false,
                by: None,
                ..
            }]
        ),
        "{records:?}"
    );
    follower.apply_records(&records).unwrap();
    let released = snapshot_rows(&follower).remove(0);
    assert!(!released.held);
    assert_eq!(released.owner(), None, "releasing forgets the owner too");
    assert_eq!(snapshot_rows(&follower), snapshot_rows(&writer));
    // The rest of the row is untouched by the hold's coming and going.
    assert_eq!(released.refer_bytes, Some(81 << 30));
    assert_eq!(released.origin, 1);
}

/// A journal written before this change carries `SnapCreate`. Replay of it
/// still produces a row — a manual, unheld one.
#[test]
fn a_journal_with_the_old_create_variant_still_replays() {
    let meta = Meta::open_in_memory().unwrap();
    meta.apply_records(&[
        LogRecord::SnapCreate {
            id: "old".into(),
            path: "/data".into(),
            name: "friday".into(),
            root_hash: "mtree:1:cd:9".into(),
            created_unix_ms: 1_600_000_000_000,
        },
        LogRecord::SnapCreate2 {
            id: "new".into(),
            path: "/data".into(),
            name: "saturday".into(),
            root_hash: "mtree:2:ef:9".into(),
            created_unix_ms: 1_600_000_001_000,
            origin: 1,
            policy_ino: 9,
            creator: 3,
            refer_bytes: Some(4096),
        },
        // A hold arriving for the snapshot created two records ago.
        LogRecord::SnapHold {
            id: "new".into(),
            held: true,
            by: Some("user:attila".into()),
        },
        // …and one for a snapshot that does not exist here at all: a
        // no-op, never an error (the delete won the log order).
        LogRecord::SnapHold {
            id: "vanished".into(),
            held: true,
            by: None,
        },
    ])
    .unwrap();

    let rows = snapshot_rows(&meta);
    assert_eq!(rows.len(), 2, "{rows:?}");
    let old = rows.iter().find(|r| r.id == "old").unwrap();
    assert_eq!(old.origin, 0);
    assert!(!old.held);
    assert_eq!(old.creator, 0);
    assert_eq!(old.refer_bytes, None);
    let new = rows.iter().find(|r| r.id == "new").unwrap();
    assert_eq!(new.origin, 1);
    assert_eq!(new.policy_ino, 9);
    assert_eq!(new.creator, 3);
    assert_eq!(new.refer_bytes, Some(4096));
    assert!(new.held);
    assert_eq!(new.owner(), Some("user:attila"));
}

/// A row claiming an owner but no hold cannot exist: it is stored — and
/// journaled — as unowned, so the writer and a replica that replays its
/// records never disagree about the row's bytes.
#[test]
fn an_owner_without_a_hold_is_normalized_away() {
    let writer = Meta::open_in_memory().unwrap();
    let follower = Meta::open_in_memory().unwrap();
    let mut row = SnapshotRow::new("id0", "/", "s", "h", 1);
    row.held_by = Some("user:attila".into());
    writer.record_snapshot(&row).unwrap();
    let stored = snapshot_rows(&writer).remove(0);
    assert!(!stored.held);
    assert_eq!(stored.owner(), None);

    let records = drain(&writer);
    assert_eq!(
        records.len(),
        1,
        "no hold record for an unheld row: {records:?}"
    );
    follower.apply_records(&records).unwrap();
    assert_eq!(snapshot_rows(&follower), snapshot_rows(&writer));
}

/// `snapshot_by_id` answers without the scan, and a hold on a snapshot
/// that is not there is `None`, not an error.
#[test]
fn holds_are_addressed_by_id() {
    let meta = Meta::open_in_memory().unwrap();
    meta.record_snapshot(&SnapshotRow::new("id0", "/", "s", "h", 1))
        .unwrap();
    assert_eq!(meta.snapshot_by_id("id0").unwrap().unwrap().name, "s");
    assert!(meta.snapshot_by_id("nope").unwrap().is_none());
    assert!(meta
        .set_snapshot_hold("nope", true, None, false)
        .unwrap()
        .is_none());

    // A plain hold: held, with no recorded owner (today's behavior).
    let held = meta
        .set_snapshot_hold("id0", true, None, false)
        .unwrap()
        .unwrap();
    assert!(held.held);
    assert_eq!(held.owner(), None);
    // An empty owner is not an owner.
    let held = meta
        .set_snapshot_hold("id0", true, Some(""), false)
        .unwrap()
        .unwrap();
    assert_eq!(held.owner(), None);
}

/// The owner rule is enforced *inside* the transaction that writes the
/// hold, not by a caller that read the row first: a refusal leaves the row
/// and the journal exactly as they were, and `force` is what gets past it.
///
/// This is the only place the rule can be atomic — two concurrent holds on
/// the same unheld snapshot checked against their own read transactions
/// would both pass, and the loser would be told it owns a hold it does not.
#[test]
fn the_owner_rule_is_part_of_the_hold_transaction() {
    let meta = Meta::open_in_memory().unwrap();
    meta.record_snapshot(&SnapshotRow::new("id0", "/", "s", "h", 1))
        .unwrap();
    drain(&meta);
    meta.set_snapshot_hold("id0", true, Some("csi:x"), false)
        .unwrap()
        .unwrap();
    drain(&meta);

    // Taking it over, and releasing it as somebody else, are both refused,
    // and the refusal names the recorded owner.
    for (held, by) in [
        (true, Some("user:attila")),
        (false, Some("user:attila")),
        (false, None),
    ] {
        let error = meta
            .set_snapshot_hold("id0", held, by, false)
            .expect_err("a foreign owner changed the hold");
        assert!(format!("{error}").contains("csi:x"), "{error}");
        assert_eq!(error.code(), constellation_types::Code::Invalid);
    }
    // Nothing was written: not the row, not the journal.
    let row = meta.snapshot_by_id("id0").unwrap().unwrap();
    assert!(row.held);
    assert_eq!(row.owner(), Some("csi:x"));
    assert!(drain(&meta).is_empty(), "a refusal journaled something");

    // Its own owner, and an admin's `force`, both get through.
    let row = meta
        .set_snapshot_hold("id0", true, Some("csi:x"), false)
        .unwrap()
        .unwrap();
    assert_eq!(row.owner(), Some("csi:x"));
    let row = meta
        .set_snapshot_hold("id0", true, Some("user:attila"), true)
        .unwrap()
        .unwrap();
    assert_eq!(row.owner(), Some("user:attila"));
    let row = meta
        .set_snapshot_hold("id0", false, None, true)
        .unwrap()
        .unwrap();
    assert!(!row.held);
    // An unheld snapshot has no owner to protect, so anyone may hold it.
    let row = meta
        .set_snapshot_hold("id0", true, Some("csi:y"), false)
        .unwrap()
        .unwrap();
    assert_eq!(row.owner(), Some("csi:y"));
}

/// Plan 32 §0.3: a delete computes the row key from `snapshot_id(path,
/// name)` and removes it with a point lookup, journaling the same
/// `SnapDelete` it always did; an id with no row is `false`, and writes
/// nothing.
#[test]
fn snapshots_are_deleted_by_their_computed_id() {
    let meta = Meta::open_in_memory().unwrap();
    let daily = constellation_meta::snapshot_id("/data", "daily");
    let weekly = constellation_meta::snapshot_id("/data", "weekly");
    meta.record_snapshot(&SnapshotRow::new(&daily, "/data", "daily", "h", 1))
        .unwrap();
    meta.record_snapshot(&SnapshotRow::new(&weekly, "/data", "weekly", "h", 2))
        .unwrap();
    drain(&meta);

    assert!(!meta.delete_snapshot_by_id("no-such-id").unwrap());
    assert!(!meta.delete_snapshot("/data", "monthly").unwrap());
    assert!(drain(&meta).is_empty(), "a miss journaled something");

    assert!(meta.delete_snapshot("/data", "daily").unwrap());
    assert!(meta.delete_snapshot_by_id(&weekly).unwrap());
    let records = drain(&meta);
    assert!(
        matches!(
            records.as_slice(),
            [
                LogRecord::SnapDelete { id: a, path: pa, name: na },
                LogRecord::SnapDelete { id: b, path: pb, name: nb },
            ] if a == &daily && pa == "/data" && na == "daily"
                && b == &weekly && pb == "/data" && nb == "weekly"
        ),
        "{records:?}"
    );
    assert!(snapshot_rows(&meta).is_empty());
    assert!(!meta.delete_snapshot_by_id(&daily).unwrap(), "already gone");

    // The replayed delete removes the same row on a follower.
    let follower = Meta::open_in_memory().unwrap();
    let writer = Meta::open_in_memory().unwrap();
    writer
        .record_snapshot(&SnapshotRow::new(&daily, "/data", "daily", "h", 1))
        .unwrap();
    follower.apply_records(&drain(&writer)).unwrap();
    assert!(writer.delete_snapshot("/data", "daily").unwrap());
    follower.apply_records(&drain(&writer)).unwrap();
    assert!(snapshot_rows(&follower).is_empty());
}
