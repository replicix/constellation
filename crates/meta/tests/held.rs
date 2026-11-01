//! Plan 30 §M4 item 2: poison-record isolation (`store::held`).
//!
//! One pending chunk gone from the local cache holds back only the
//! transactions that need it — its manifest, and whatever touches a key a
//! held transaction touched — while everything else ships, out of journal
//! order. These tests pin down that the held work stays exactly M3b
//! speculation: the publish view is still the log prefix, a segment
//! inserted before local work lands after the out-of-order shipped rows, a
//! deposition rolls the held rows back, and `drop_held` turns them into a
//! conflict copy plus replays.

use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::{ChunkHash, ChunkInfo, ChunkLayout, Ino, Manifest};
use constellation_meta::{LogRecord, Meta, MetaStore, MutateOp, PublishBasis, TouchSet};
use constellation_types::Code;
use std::collections::BTreeMap;

fn manifest_naming(hash: ChunkHash, len: u64) -> Vec<u8> {
    Manifest {
        layout: ChunkLayout::new(4096),
        file_len: len,
        chunks: ChunkInfo::Inline(BTreeMap::from([(0u64, hash)])),
    }
    .encode()
}

fn batch(meta: &Meta) -> Vec<(u64, LogRecord)> {
    meta.take_journal_grouped(10_000)
        .unwrap()
        .into_iter()
        .flat_map(|(_, b)| b)
        .collect()
}

fn ship(meta: &Meta, rows: &[(u64, LogRecord)], segment: u64) {
    let seqs: Vec<u64> = rows.iter().map(|(s, _)| *s).collect();
    meta.ack_journal_rows_at(&seqs, segment).unwrap();
}

fn basis(meta: &Meta) -> PublishBasis {
    meta.read_consistent(|snap| meta.publish_basis_at(snap))
        .unwrap()
}

/// `ns` as the publisher would see it through `basis`, without the root
/// inode (its genesis timestamps differ between two replicas).
fn published_view(meta: &Meta, basis: &PublishBasis) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut map: BTreeMap<Vec<u8>, Vec<u8>> = meta.ns_dump().unwrap().into_iter().collect();
    if let PublishBasis::Substituted(view) = basis {
        for (k, v) in view.iter() {
            match v {
                Some(v) => {
                    map.insert(k.to_vec(), v.to_vec());
                }
                None => {
                    map.remove(k);
                }
            }
        }
    }
    map.remove(&constellation_mtree::keys::inode(ROOT_INO));
    map
}

/// A replica that replayed exactly `records`: the log prefix.
fn replayed(records: &[LogRecord]) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let r = Meta::open_in_memory().unwrap();
    r.apply_records(records).unwrap();
    let mut map: BTreeMap<Vec<u8>, Vec<u8>> = r.ns_dump().unwrap().into_iter().collect();
    map.remove(&constellation_mtree::keys::inode(ROOT_INO));
    map
}

fn records(rows: &[(u64, LogRecord)]) -> Vec<LogRecord> {
    rows.iter().map(|(_, r)| r.clone()).collect()
}

fn mode_of(meta: &Meta, ino: Ino) -> u32 {
    meta.getattr(ino).unwrap().unwrap().mode & 0o7777
}

struct Setup {
    meta: Meta,
    lost: ChunkHash,
    broken: Ino,
    other: Ino,
}

/// A holder with: `broken` created (ships), its manifest naming a lost
/// chunk (the seed), a chmod of it (a dependent), and `other` created
/// after both (independent).
fn setup(epoch: u64) -> Setup {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(epoch);
    let lost = ChunkHash::of(b"bytes that are gone");
    let broken = meta.create(ROOT_INO, "broken", 0o644, 0, 0).unwrap().ino;
    meta.set_manifest_dirty(
        broken,
        None,
        &manifest_naming(lost, 19),
        19,
        None,
        None,
        &[lost],
    )
    .unwrap();
    meta.setattr(broken, Some(0o600), None, None, None, None, None)
        .unwrap();
    let other = meta.create(ROOT_INO, "other", 0o644, 0, 0).unwrap().ino;
    meta.note_unrecoverable_chunks(&[(lost, broken)], true)
        .unwrap();
    Setup {
        meta,
        lost,
        broken,
        other,
    }
}

#[test]
fn only_the_poisoned_manifest_and_its_dependents_are_held() {
    let s = setup(1);
    let rows = batch(&s.meta);
    let recs = records(&rows);
    assert!(
        recs.iter().all(|r| !matches!(
            r,
            LogRecord::WriteManifest { .. } | LogRecord::Setattr { .. }
        )),
        "the seed and its dependent must not ship: {recs:?}"
    );
    let names: Vec<&str> = recs
        .iter()
        .filter_map(|r| match r {
            LogRecord::Create { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(names, vec!["broken", "other"], "everything else ships");
    let held = s.meta.held_summary();
    assert_eq!(held.transactions, 2, "{held:?}");
    assert_eq!(held.inodes[&s.broken].missing, vec![s.lost]);
    assert_eq!(held.inodes[&s.broken].seeds, 1);
    assert!(!held.opaque);

    // Before and after the ship, the publish view is the log prefix.
    let b = basis(&s.meta);
    assert!(matches!(b, PublishBasis::Substituted(_)), "{b:?}");
    ship(&s.meta, &rows, 1);
    let b = basis(&s.meta);
    assert!(matches!(b, PublishBasis::Substituted(_)), "{b:?}");
    assert_eq!(published_view(&s.meta, &b), replayed(&recs));

    // Still journaled, still captured, still held next round.
    assert_eq!(s.meta.speculation_counts().unwrap().local, 2);
    assert!(batch(&s.meta).is_empty());
    let left = s.meta.take_journal(usize::MAX).unwrap();
    assert!(left
        .iter()
        .any(|(_, r)| matches!(r, LogRecord::WriteManifest { ino, .. } if *ino == s.broken)));
    // The replica itself still shows the held work.
    assert_eq!(mode_of(&s.meta, s.broken), 0o600);
}

/// Our own segment found again by a tail (after a restart) matches the
/// journal as a subsequence of whole transactions, skipping held ones.
#[test]
fn an_own_segment_matches_the_journal_around_held_transactions() {
    let s = setup(1);
    let rows = batch(&s.meta);
    let recs = records(&rows);
    let refs: Vec<&LogRecord> = recs.iter().collect();
    let seqs = s.meta.match_own_segment(&refs).unwrap().expect("a match");
    assert_eq!(seqs, rows.iter().map(|(q, _)| *q).collect::<Vec<_>>());
    // Records the journal does not hold do not match.
    let stranger = LogRecord::Create {
        parent: ROOT_INO,
        name: "stranger".into(),
        ino: 99,
        mode: 0o644,
        uid: 0,
        gid: 0,
        time_ns: 1,
    };
    assert!(s.meta.match_own_segment(&[&stranger]).unwrap().is_none());
    assert_eq!(s.meta.match_own_segment(&[]).unwrap(), Some(vec![]));
}

/// A late segment tailed while held rows are outstanding goes before the
/// local work but *after* the rows that already shipped out of order: a
/// record in it that depends on one of those (a chmod of `other`) must see
/// it.
#[test]
fn a_segment_inserted_before_held_work_lands_after_the_rows_that_shipped() {
    let s = setup(2);
    let rows = batch(&s.meta);
    let recs = records(&rows);
    ship(&s.meta, &rows, 1);
    let late = vec![LogRecord::Setattr {
        ino: s.other,
        mode: Some(0o640),
        uid: None,
        gid: None,
        size: None,
        atime_ns: None,
        mtime_ns: None,
        time_ns: i64::MAX / 2,
    }];
    // Epoch 1 < 2: a late write of the previous holder, not a deposition.
    let applied = s
        .meta
        .apply_segment(2, 1, &late, &TouchSet::default())
        .unwrap();
    assert!(applied.inserted_before_local);
    assert!(!applied.stranded.any());
    assert_eq!(mode_of(&s.meta, s.other), 0o640);
    assert_eq!(mode_of(&s.meta, s.broken), 0o600, "held work redone on top");
    let mut log = recs.clone();
    log.extend(late.clone());
    let b = basis(&s.meta);
    assert_eq!(published_view(&s.meta, &b), replayed(&log));
}

/// A deposition strands the held rows like any unshipped work; the rows
/// that shipped out of order stay.
#[test]
fn a_deposition_rolls_back_held_rows_and_keeps_the_shipped_ones() {
    let s = setup(1);
    let rows = batch(&s.meta);
    let recs = records(&rows);
    ship(&s.meta, &rows, 1);
    let stranded = s.meta.strand_below_epoch(2).unwrap();
    assert_eq!(stranded.locals, 2);
    assert_eq!(s.meta.journal_len().unwrap(), 0);
    let mut now: BTreeMap<Vec<u8>, Vec<u8>> = s.meta.ns_dump().unwrap().into_iter().collect();
    now.remove(&constellation_mtree::keys::inode(ROOT_INO));
    assert_eq!(now, replayed(&recs));
    assert_eq!(s.meta.pending_replays().unwrap().len(), 2);
}

/// `repair drop-held`: the seed becomes a refused replay (its conflict
/// copy carries the manifest with the lost chunk as a hole), the
/// dependent is rolled back and queued for replay, the unrecoverable
/// pending row goes, and nothing is held any more.
#[test]
fn drop_held_turns_the_seed_into_a_conflict_copy_and_replays_the_rest() {
    let s = setup(1);
    let rows = batch(&s.meta);
    ship(&s.meta, &rows, 1);
    let dropped = s.meta.drop_held(s.broken, 1234).unwrap();
    assert_eq!(
        (dropped.dropped, dropped.requeued, dropped.pending_removed),
        (1, 1, 1)
    );
    assert_eq!(
        s.meta.journal_len().unwrap(),
        1,
        "the dropped op's refusal waits to ship"
    );
    assert!(s.meta.pending_uploads().unwrap().is_empty());
    assert!(s.meta.unrecoverable_chunks().unwrap().is_empty());
    assert_eq!(s.meta.held_summary().transactions, 0);
    assert_eq!(
        mode_of(&s.meta, s.broken),
        0o644,
        "the chmod was rolled back"
    );
    assert!(s.meta.manifest(s.broken).unwrap().is_none());

    let queued = s.meta.pending_replays().unwrap();
    assert_eq!(queued.len(), 2, "{queued:?}");
    let seed = &queued[0];
    let refusal = seed.refused.as_ref().expect("the seed is refused");
    assert!(refusal.reason.contains("drop-held"), "{refusal:?}");
    assert!(seed.dropped, "the drop's own refusal: {seed:?}");
    assert_eq!(refusal.ts_unix, 1234);
    let MutateOp::SetManifest { ino, manifest, .. } = &seed.op else {
        panic!("{:?}", seed.op);
    };
    assert_eq!(*ino, s.broken);
    let kept = Manifest::decode(manifest).unwrap();
    assert_eq!(kept.file_len, 19);
    assert_eq!(kept.chunks, ChunkInfo::Inline(BTreeMap::new()));
    let dependent = &queued[1];
    assert!(dependent.refused.is_none());
    // Written through `MetaStore::setattr` (no `execute`, so no op of its
    // own): replayed from its records.
    assert!(
        matches!(&dependent.op, MutateOp::Records { records } if records
            .iter()
            .any(|r| matches!(r, LogRecord::Setattr { mode: Some(0o600), .. }))),
        "{:?}",
        dependent.op
    );
    // Nothing is held any more: the next plan ships everything — the
    // dropped op's outcome (fix "capture under an epoch hold": a
    // requester's shadow of it rolls back on it).
    let recs = records(&batch(&s.meta));
    assert!(
        matches!(recs.as_slice(), [LogRecord::Refused { rid, code: Code::Io }] if *rid == seed.rid),
        "{recs:?}"
    );
    assert!(
        s.meta.drop_held(s.broken, 1).is_err(),
        "nothing left to drop"
    );
}

/// `repair drop-held --remote` (fix "capture under an epoch hold"): a
/// manifest another node forwarded naming a chunk still pending there
/// (`enroll_remote_chunks`) is *deferred*, not held — the plan ships
/// around it, and plain `drop-held` has nothing to drop. When that node
/// is gone for good, `--remote` declares the chunk unrecoverable and
/// drops it the held way: the seed becomes a refused replay (a conflict
/// copy with the chunk as a hole), its dependent is rolled back and
/// queued, the remote-marked pending row goes, and the plan is clean.
#[test]
fn drop_held_remote_drops_a_transaction_deferred_on_a_departed_nodes_chunk() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    let away = ChunkHash::of(b"only on the node that died");
    let file = meta.create(ROOT_INO, "file", 0o644, 0, 0).unwrap().ino;
    // The forward arrives: its chunk is enrolled before the op executes.
    meta.enroll_remote_chunks(file, &[away], 7).unwrap();
    meta.set_manifest_with_base(file, None, &manifest_naming(away, 19), 19, None, None)
        .unwrap();
    meta.setattr(file, Some(0o600), None, None, None, None, None)
        .unwrap();
    let other = meta.create(ROOT_INO, "other", 0o644, 0, 0).unwrap().ino;

    // Deferred, not held: the create of `file` and everything unrelated
    // ships; the manifest and its chmod wait.
    let rows = batch(&meta);
    let recs = records(&rows);
    assert!(
        recs.iter().all(|r| !matches!(
            r,
            LogRecord::WriteManifest { .. } | LogRecord::Setattr { .. }
        )),
        "{recs:?}"
    );
    assert!(recs
        .iter()
        .any(|r| matches!(r, LogRecord::Create { ino, .. } if *ino == other)));
    let summary = meta.held_summary();
    assert_eq!(
        (summary.transactions, summary.deferred),
        (0, 2),
        "{summary:?}"
    );
    assert_eq!(meta.remote_chunks().unwrap().len(), 1);
    ship(&meta, &rows, 1);
    let err = meta.drop_held(file, 1).unwrap_err().to_string();
    assert!(err.contains("--remote"), "{err}");

    let dropped = meta.drop_held_remote(file, 4321).unwrap();
    assert_eq!(
        (dropped.dropped, dropped.requeued, dropped.pending_removed),
        (1, 1, 1)
    );
    assert_eq!(meta.journal_len().unwrap(), 1, "the refusal waits to ship");
    assert!(meta.pending_uploads().unwrap().is_empty());
    assert!(meta.remote_chunks().unwrap().is_empty());
    assert!(meta.unrecoverable_chunks().unwrap().is_empty());
    assert_eq!(mode_of(&meta, file), 0o644, "the chmod was rolled back");
    assert!(meta.manifest(file).unwrap().is_none());
    let queued = meta.pending_replays().unwrap();
    assert_eq!(queued.len(), 2, "{queued:?}");
    let refusal = queued[0].refused.as_ref().expect("the seed is refused");
    assert!(refusal.reason.contains("drop-held"), "{refusal:?}");
    assert_eq!(refusal.ts_unix, 4321);
    let MutateOp::SetManifest { ino, manifest, .. } = &queued[0].op else {
        panic!("{:?}", queued[0].op);
    };
    assert_eq!(*ino, file);
    let kept = Manifest::decode(manifest).unwrap();
    assert_eq!(kept.file_len, 19);
    assert_eq!(kept.chunks, ChunkInfo::Inline(BTreeMap::new()), "a hole");
    assert!(queued[1].refused.is_none(), "the dependent replays");
    // The dropped op's outcome is in the journal, to ship: a requester's
    // shadow of it is rolled back by it, and a retry by rid is refused.
    let recs = records(&batch(&meta));
    assert!(
        matches!(recs.as_slice(), [LogRecord::Refused { rid, code: Code::Io }] if *rid == queued[0].rid),
        "{recs:?}"
    );
    assert!(meta.drop_held_remote(file, 1).is_err(), "nothing left");
}

/// Fix "capture under an epoch hold": a refusal writes nothing, but its
/// op *observed* the keys it was refused on (`JournalTx::observed`). One
/// refused because of the deferred transaction's state — a link of the
/// deferred file's inode — is deferred with it; one on an unrelated name
/// ships. Shipped ahead, the log carried a refusal its own prefix could
/// not explain (flex-crash seed 481: `rename f2 f3` refused `ENOENT` at
/// a position where `f2` existed).
#[test]
fn a_refusal_that_observed_a_deferred_transactions_keys_is_deferred_with_it() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    let away = ChunkHash::of(b"pending on another node");
    let file = meta.create(ROOT_INO, "file", 0o644, 0, 0).unwrap().ino;
    meta.enroll_remote_chunks(file, &[away], 7).unwrap();
    meta.set_manifest_with_base(file, None, &manifest_naming(away, 19), 19, None, None)
        .unwrap();
    let rid = |seq: u64| constellation_meta::Rid {
        node: 3,
        incarnation: 1,
        seq,
    };
    // Refused because `file` (whose manifest is deferred) already exists
    // under that name: it looked at the file's inode.
    meta.journal_refusal(
        rid(1),
        Code::Exists,
        Some(&MutateOp::Link {
            ino: file,
            parent: ROOT_INO,
            name: "file".into(),
        }),
    )
    .unwrap();
    // Refused on a name nothing deferred touched.
    meta.journal_refusal(
        rid(2),
        Code::NotFound,
        Some(&MutateOp::Unlink {
            parent: ROOT_INO,
            name: "missing".into(),
        }),
    )
    .unwrap();
    let recs = records(&batch(&meta));
    let refused: Vec<u64> = recs
        .iter()
        .filter_map(|r| match r {
            LogRecord::Refused { rid, .. } => Some(rid.seq),
            _ => None,
        })
        .collect();
    assert_eq!(
        refused,
        vec![2],
        "only the unrelated refusal ships: {recs:?}"
    );
    assert!(
        !recs
            .iter()
            .any(|r| matches!(r, LogRecord::WriteManifest { .. })),
        "the manifest is deferred"
    );
    let summary = meta.held_summary();
    assert_eq!(
        summary.deferred, 2,
        "the manifest and the refusal on it: {summary:?}"
    );
}

/// A chunk that turns up after all (its pending row acked) stops
/// poisoning: the next plan ships everything.
#[test]
fn an_acked_pending_row_stops_poisoning() {
    let s = setup(1);
    s.meta.ack_upload(&s.lost, s.broken).unwrap();
    assert!(s.meta.unrecoverable_chunks().unwrap().is_empty());
    let recs = records(&batch(&s.meta));
    assert!(recs
        .iter()
        .any(|r| matches!(r, LogRecord::WriteManifest { .. })));
    assert_eq!(s.meta.held_summary().transactions, 0);
}

/// Without capture (journaled while not holding) a transaction has
/// no known key set: once one is held, everything after it is held too —
/// the pre-M4 behaviour from that point on.
#[test]
fn uncaptured_transactions_after_a_held_one_are_held() {
    let s = setup(0);
    let recs = records(&batch(&s.meta));
    let names: Vec<&str> = recs
        .iter()
        .filter_map(|r| match r {
            LogRecord::Create { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(names, vec!["broken"], "only what precedes the seed ships");
    let held = s.meta.held_summary();
    assert!(held.opaque, "{held:?}");
    assert_eq!(held.transactions, 3);
    assert!(
        s.meta.drop_held(s.broken, 1).is_err(),
        "uncaptured transactions cannot be rolled back"
    );
}

// ---- plan 30 §M7: deferral behind chunks still uploading ----

/// A write-back burst's manifest names a chunk that is still uploading:
/// it is *deferred* — not held (nothing is lost, no repair is needed) —
/// while the unrelated marker created after it ships at once; once the
/// chunk is acked the manifest ships too. The segment's `through` never
/// claims the deferred row.
#[test]
fn a_manifest_whose_chunk_is_uploading_is_deferred_and_others_ship() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    let pending = ChunkHash::of(b"a burst's chunk, still uploading");
    let big = meta.create(ROOT_INO, "big", 0o644, 0, 0).unwrap().ino;
    meta.set_manifest_dirty(
        big,
        None,
        &manifest_naming(pending, 33),
        33,
        None,
        None,
        &[pending],
    )
    .unwrap();
    let marker = meta.create(ROOT_INO, "marker", 0o644, 0, 0).unwrap().ino;
    let rows = batch(&meta);
    let recs = records(&rows);
    assert!(
        !recs
            .iter()
            .any(|r| matches!(r, LogRecord::WriteManifest { ino, .. } if *ino == big)),
        "the manifest waits for its chunk: {recs:?}"
    );
    assert!(
        recs.iter()
            .any(|r| matches!(r, LogRecord::Create { ino, .. } if *ino == marker)),
        "the marker ships ahead of it: {recs:?}"
    );
    let held = meta.held_summary();
    assert_eq!(held.transactions, 0, "deferred work is not held: {held:?}");
    assert_eq!(held.deferred, 1, "{held:?}");
    let seqs: Vec<u64> = rows.iter().map(|(s, _)| *s).collect();
    let through = meta.journal_through_after(&seqs).unwrap();
    assert!(
        through < *seqs.iter().max().unwrap(),
        "`through` {through} claims the deferred row shipped ({seqs:?})"
    );
    ship(&meta, &rows, 1);
    assert!(batch(&meta).is_empty(), "nothing else is shippable yet");

    meta.ack_upload(&pending, big).unwrap();
    let rest = batch(&meta);
    assert!(
        records(&rest)
            .iter()
            .any(|r| matches!(r, LogRecord::WriteManifest { ino, .. } if *ino == big)),
        "the manifest ships once its chunk is up: {rest:?}"
    );
    // Chunk close-stall-followup: the segment carrying the deferred
    // manifest claims everything through the marker, which shipped ahead
    // of it — not just its own rows (a node that observed the holder's
    // position past the marker waited for the next segment).
    let rest_seqs: Vec<u64> = rest.iter().map(|(s, _)| *s).collect();
    let last = *seqs.iter().chain(&rest_seqs).max().unwrap();
    assert!(
        rest_seqs.iter().max().unwrap() < &last,
        "the marker came after"
    );
    assert_eq!(meta.journal_through_after(&rest_seqs).unwrap(), last);
    ship(&meta, &rest, 2);
    assert_eq!(meta.journal_len().unwrap(), 0);
    assert_eq!(meta.journal_acked_seq().unwrap(), last);
}

/// A later change to the deferred file depends on its manifest and waits
/// with it; nothing is skipped out of dependency order.
#[test]
fn a_dependent_of_a_deferred_manifest_waits_with_it() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    let pending = ChunkHash::of(b"still uploading");
    let big = meta.create(ROOT_INO, "big", 0o644, 0, 0).unwrap().ino;
    meta.set_manifest_dirty(
        big,
        None,
        &manifest_naming(pending, 15),
        15,
        None,
        None,
        &[pending],
    )
    .unwrap();
    meta.setattr(big, Some(0o600), None, None, None, None, None)
        .unwrap();
    let rows = batch(&meta);
    assert!(
        records(&rows).iter().all(|r| !matches!(
            r,
            LogRecord::WriteManifest { .. } | LogRecord::Setattr { .. }
        )),
        "{rows:?}"
    );
    assert_eq!(meta.held_summary().deferred, 2);
    ship(&meta, &rows, 1);
    meta.ack_upload(&pending, big).unwrap();
    let rest = records(&batch(&meta));
    let manifest_at = rest
        .iter()
        .position(|r| matches!(r, LogRecord::WriteManifest { .. }))
        .expect("manifest");
    let chmod_at = rest
        .iter()
        .position(|r| matches!(r, LogRecord::Setattr { .. }))
        .expect("chmod");
    assert!(
        manifest_at < chmod_at,
        "journal order within the deferred work"
    );
    assert_eq!(mode_of(&meta, big), 0o600);
}

/// Chunk close-stall-metered (review must-fix 1): a sequencer tells a
/// forwarder which of its pending chunks an op's transaction waits for
/// (`Meta::remote_blockers`) — not only the chunks its own manifest
/// names, but those of every deferred transaction it depends on by key.
/// A `chmod` or a `rename` of a file right after node 7's `back` close of
/// it names no chunk, yet the ship plan defers it with the close until
/// node 7 reports its chunk up; an op on an unrelated file waits for
/// nothing of node 7's, and nothing waits for another node.
#[test]
fn an_op_depending_on_a_deferred_close_waits_for_the_forwarders_chunks() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    let away = ChunkHash::of(b"pending on node 7");
    let rid = |seq: u64| constellation_meta::Rid {
        node: 7,
        incarnation: 1,
        seq,
    };
    let file = meta.create(ROOT_INO, "file", 0o644, 0, 0).unwrap().ino;
    let other = meta.create(ROOT_INO, "other", 0o644, 0, 0).unwrap().ino;
    let ex = |seq: u64, op: MutateOp| {
        constellation_meta::execute_mutate(&meta, &op, Some(rid(seq))).unwrap()
    };
    let setattr = |ino| MutateOp::Setattr {
        ino,
        mode: Some(0o600),
        uid: None,
        gid: None,
        size: None,
        atime_ns: None,
        mtime_ns: None,
    };
    // Unshipped too, but journaled before the close.
    let before = ex(5, setattr(other));
    meta.enroll_remote_chunks(file, &[away], 7).unwrap();
    let close = ex(
        1,
        MutateOp::SetManifest {
            ino: file,
            base_manifest: None,
            manifest: manifest_naming(away, 19),
            size: 19,
            mtime_ns: None,
            ctime_ns: None,
        },
    );
    let chmod = ex(2, setattr(file));
    let unrelated = ex(3, setattr(other));
    let rename = ex(
        4,
        MutateOp::Rename {
            parent: ROOT_INO,
            name: "file".into(),
            new_parent: ROOT_INO,
            new_name: "moved".into(),
            noreplace: false,
        },
    );
    let blockers = |seq: u64, records: &[LogRecord], node: u64| {
        meta.remote_blockers(rid(seq), 0, records, node, None)
            .unwrap()
    };
    assert_eq!(blockers(1, &close, 7).inos, vec![file], "its own manifest");
    let dependent = blockers(2, &chmod, 7);
    assert_eq!(dependent.inos, vec![file], "the chmod depends on the close");
    assert!(dependent.through.is_some());
    assert_eq!(blockers(4, &rename, 7).inos, vec![file], "and the rename");
    // Chunk close-stall-followup: an op on an unrelated file does not wait
    // for the close, but the position it was evaluated at does (node 7
    // observes it), so its answer names the close's inode too; one
    // journaled before the close waits for nothing of node 7's.
    let unrelated = blockers(3, &unrelated, 7);
    assert_eq!(unrelated.inos, vec![file], "its position: {unrelated:?}");
    assert!(
        unrelated.through.is_some(),
        "still unshipped: {unrelated:?}"
    );
    let free = blockers(5, &before, 7);
    assert!(free.inos.is_empty(), "{free:?}");
    assert!(free.through.is_some(), "still unshipped: {free:?}");
    assert!(blockers(2, &chmod, 8).inos.is_empty(), "not node 8's");
    // The plan agrees: the close, the chmod and the rename are deferred.
    assert!(records(&batch(&meta))
        .iter()
        .all(|r| !matches!(r, LogRecord::Setattr { ino, .. } if *ino == file)),);
    assert_eq!(meta.held_summary().deferred, 3);
    // Reported up: nothing waits for node 7 any more.
    meta.ack_remote_chunks(&[away]).unwrap();
    assert!(blockers(2, &chmod, 7).inos.is_empty());
    assert!(blockers(3, &[], 7).inos.is_empty());
}

/// Plan 45 follow-up (the authority sim's `flex+links` seed 4): a rename
/// of a name onto another name of the same inode is a no-op that
/// completes its rid on a row of its own. That row observed the link that
/// made the two names one inode; deferred with it, it ships after it — not
/// ahead, where the log put an `Ok` before the state it was evaluated on —
/// and a later op of the directory ships after it (seeds 191 and 335: the
/// log put a create invoked after the rename returned ahead of it).
#[test]
fn a_same_inode_rename_completes_behind_the_deferred_link_it_observed() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    let away = ChunkHash::of(b"pending on another node");
    let file = meta.create(ROOT_INO, "file", 0o644, 0, 0).unwrap().ino;
    meta.enroll_remote_chunks(file, &[away], 7).unwrap();
    meta.set_manifest_with_base(file, None, &manifest_naming(away, 19), 19, None, None)
        .unwrap();
    // Deferred too: it writes the deferred file's inode.
    meta.link(file, ROOT_INO, "alias").unwrap();
    let rid = constellation_meta::Rid {
        node: 3,
        incarnation: 1,
        seq: 1,
    };
    let rename = MutateOp::Rename {
        parent: ROOT_INO,
        name: "file".into(),
        new_parent: ROOT_INO,
        new_name: "alias".into(),
        noreplace: false,
    };
    let rows = constellation_meta::execute_mutate(&meta, &rename, Some(rid)).unwrap();
    assert!(
        matches!(rows.as_slice(), [LogRecord::Completed { rid: r }] if *r == rid),
        "{rows:?}"
    );
    // Its `Ok` returned: a create in the directory invoked after it
    // follows it in the log.
    meta.create(ROOT_INO, "later", 0o644, 0, 0).unwrap();
    let completes = |r: &LogRecord| matches!(r, LogRecord::Completed { rid: c } if *c == rid);
    let later = |r: &LogRecord| matches!(r, LogRecord::Create { name, .. } if name == "later");
    let recs = records(&batch(&meta));
    assert!(
        !recs.iter().any(completes),
        "the completion shipped ahead of the link it observed: {recs:?}"
    );
    assert!(
        !recs.iter().any(later),
        "a create invoked after the rename returned shipped ahead of it: {recs:?}"
    );
    meta.ack_remote_chunks(&[away]).unwrap();
    let recs = records(&batch(&meta));
    let link = recs
        .iter()
        .position(|r| matches!(r, LogRecord::Link { name, .. } if name == "alias"));
    let done = recs.iter().position(completes);
    let created = recs.iter().position(later);
    assert!(
        link.is_some() && done.is_some() && created.is_some() && link < done && done < created,
        "the link, the completion, then the create: {recs:?}"
    );
}

/// Plan 45 follow-up (the authority sim's `metered-shared+links` seed
/// 449): a record that applied as a no-op writes nothing — here a
/// stranded op's `unlink f1` replayed after `rename f1 f0` took the name —
/// but the log replays it by name wherever it ships. Shipped ahead of the
/// deferred rename, every replica unlinked `f1` and then skipped the
/// rename, while the holder had renamed the file. The rows a transaction
/// names count as its keys.
#[test]
fn a_no_op_record_ships_behind_the_deferred_transaction_of_its_name() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    let away = ChunkHash::of(b"pending on another node");
    let file = meta.create(ROOT_INO, "f1", 0o644, 0, 0).unwrap().ino;
    meta.enroll_remote_chunks(file, &[away], 7).unwrap();
    meta.set_manifest_with_base(file, None, &manifest_naming(away, 19), 19, None, None)
        .unwrap();
    meta.rename(ROOT_INO, "f1", ROOT_INO, "f0").unwrap();
    let unlink = LogRecord::Unlink {
        parent: ROOT_INO,
        name: "f1".into(),
        time_ns: 1,
    };
    meta.apply_records_journaled(std::slice::from_ref(&unlink))
        .unwrap();
    assert_eq!(
        meta.lookup(ROOT_INO, "f0").unwrap().map(|e| e.ino),
        Some(file)
    );
    let renamed = |r: &LogRecord| matches!(r, LogRecord::Rename { .. });
    let unlinked = |r: &LogRecord| matches!(r, LogRecord::Unlink { name, .. } if name == "f1");
    let recs = records(&batch(&meta));
    assert!(
        !recs.iter().any(renamed),
        "the rename is deferred: {recs:?}"
    );
    assert!(
        !recs.iter().any(unlinked),
        "the no-op unlink of the name shipped ahead of the rename: {recs:?}"
    );
    meta.ack_remote_chunks(&[away]).unwrap();
    let recs = records(&batch(&meta));
    let (r, u) = (
        recs.iter().position(renamed),
        recs.iter().position(unlinked),
    );
    assert!(r.is_some() && u.is_some() && r < u, "{recs:?}");
}

// ---------------------------------------------------------------------
// Plan 46 M0.2 (R1): `repair drop-held` and another node's unanswered row.
// ---------------------------------------------------------------------

fn rid_of(node: u64, seq: u64) -> constellation_meta::Rid {
    constellation_meta::Rid {
        node,
        incarnation: 1,
        seq,
    }
}

fn chmod(ino: Ino, mode: u32) -> MutateOp {
    MutateOp::Setattr {
        ino,
        mode: Some(mode),
        uid: None,
        gid: None,
        size: None,
        atime_ns: None,
        mtime_ns: None,
    }
}

/// Execute `op` as `rid`, journaled under `class` (what the core sets
/// around its call).
fn exec_as(
    meta: &Meta,
    class: constellation_meta::RowClass,
    rid: constellation_meta::Rid,
    op: &MutateOp,
) {
    let _class = constellation_meta::PendingClass::set(class);
    constellation_meta::execute_mutate(meta, op, Some(rid)).unwrap();
}

/// A node 1 holder with `broken` created (ships), its manifest naming a
/// lost chunk (the seed), and the chunk marked unrecoverable.
fn node_one_with_a_seed() -> (Meta, Ino) {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_node_prefix(1).unwrap();
    meta.set_holder_epoch(1);
    let lost = ChunkHash::of(b"bytes that are gone");
    let broken = meta.create(ROOT_INO, "broken", 0o644, 0, 0).unwrap().ino;
    let rows = batch(&meta);
    ship(&meta, &rows, 1);
    meta.set_manifest_dirty(
        broken,
        None,
        &manifest_naming(lost, 19),
        19,
        None,
        None,
        &[lost],
    )
    .unwrap();
    meta.note_unrecoverable_chunks(&[(lost, broken)], true)
        .unwrap();
    (meta, broken)
}

/// R1: a rolled-back row of another node whose answer was never given is
/// not queued as a copy of its op — its requester holds the one copy that
/// may execute — but as a placeholder keeping its place, the keys it
/// touched and the time it was dropped. The rows whose answer may have
/// been given (a group's), this node's own, and the seed keep what they
/// had.
#[test]
fn drop_held_marks_remote_rows_await_retry() {
    use constellation_meta::RowClass::{Archive, Lone, RootGroup};
    let (meta, broken) = node_one_with_a_seed();
    // Another node's unanswered rows: gated by the archive, which a held
    // (unshipped) row never reaches.
    exec_as(&meta, Archive, rid_of(7, 1), &chmod(broken, 0o600));
    // Another node's row its group held: answered, perhaps.
    exec_as(&meta, RootGroup, rid_of(8, 1), &chmod(broken, 0o640));
    // This node's own client.
    exec_as(&meta, Lone, rid_of(1, 5), &chmod(broken, 0o660));
    // Another node's row on keys nothing held touches: ships, not dropped.
    let other = meta.create(ROOT_INO, "other", 0o644, 0, 0).unwrap().ino;
    exec_as(&meta, Archive, rid_of(7, 2), &chmod(other, 0o600));

    let dropped = meta.drop_held(broken, 1234).unwrap();
    assert_eq!((dropped.dropped, dropped.requeued), (1, 3), "{dropped:?}");

    let queue = meta.pending_replays().unwrap();
    let by = |rid| {
        queue
            .iter()
            .find(|q| q.rid == rid)
            .unwrap_or_else(|| panic!("{rid:?} not queued: {queue:?}"))
    };
    // The unanswered remote row: a placeholder.
    let p = by(rid_of(7, 1));
    let await_retry = p
        .await_retry
        .as_ref()
        .unwrap_or_else(|| panic!("an unanswered remote row stays a copy of its op: {p:?}"));
    assert!(await_retry.keys.inos.contains(&broken), "{await_retry:?}");
    assert_eq!(await_retry.since_ms, 1_234_000, "wall ms of the drop");
    assert!(p.is_await_retry() && p.refused.is_none());
    assert!(
        !p.class.gated(),
        "a placeholder is never a gated copy: {p:?}"
    );
    assert!(!p.foreign);
    assert_eq!(
        meta.completed_outcome(p.rid).unwrap(),
        None,
        "the rid has no outcome: its requester's retry executes it"
    );
    // A group's row, this node's own: executable, as before.
    let g = by(rid_of(8, 1));
    assert!(g.await_retry.is_none(), "{g:?}");
    assert!(
        matches!(
            &g.op,
            MutateOp::Setattr {
                mode: Some(0o640),
                ..
            }
        ),
        "{g:?}"
    );
    assert_eq!(g.class, RootGroup);
    let own = by(rid_of(1, 5));
    assert!(own.await_retry.is_none(), "{own:?}");
    assert!(
        matches!(
            &own.op,
            MutateOp::Setattr {
                mode: Some(0o660),
                ..
            }
        ),
        "{own:?}"
    );
    // The seed is still the refused replay of its conflict copy.
    let seed = queue
        .iter()
        .find(|q| q.refused.is_some())
        .expect("the seed is refused");
    assert!(seed.await_retry.is_none());
    // The independent row was never held.
    assert!(!queue.iter().any(|q| q.rid == rid_of(7, 2)), "{queue:?}");
    assert_eq!(queue.len(), 4, "{queue:?}");
}

/// R1: the queue keeps every rolled-back row in journal order — the
/// placeholders among the executable entries — so the drain can run the
/// dependents of a remote row behind it.
#[test]
fn drop_held_keeps_dependents_in_journal_order() {
    use constellation_meta::RowClass::{Archive, Lone};
    let (meta, broken) = node_one_with_a_seed();
    let rename = MutateOp::Rename {
        parent: ROOT_INO,
        name: "broken".into(),
        new_parent: ROOT_INO,
        new_name: "moved".into(),
        noreplace: false,
    };
    let create = |name: &str, ino| MutateOp::Create {
        parent: ROOT_INO,
        name: name.into(),
        ino,
        mode: 0o644,
        uid: 0,
        gid: 0,
    };
    // seed, an own chmod, a remote rename (rid 7/1), an own create in the
    // directory the rename touched, a remote create in it (rid 8/1), and
    // an own chmod of the renamed file.
    exec_as(&meta, Lone, rid_of(1, 5), &chmod(broken, 0o600));
    exec_as(&meta, Archive, rid_of(7, 1), &rename);
    exec_as(&meta, Lone, rid_of(1, 6), &create("x", 1 << 40));
    exec_as(&meta, Archive, rid_of(8, 1), &create("y", (1 << 40) + 1));
    exec_as(&meta, Lone, rid_of(1, 7), &chmod(broken, 0o640));

    let dropped = meta.drop_held(broken, 1).unwrap();
    assert_eq!((dropped.dropped, dropped.requeued), (1, 5), "{dropped:?}");
    let queue = meta.pending_replays().unwrap();
    let order: Vec<_> = queue
        .iter()
        .map(|q| {
            (
                q.rid.node,
                q.rid.seq,
                q.is_await_retry(),
                q.refused.is_some(),
            )
        })
        .collect();
    // The seed's rid is the journal seq of its transaction on this node.
    let seed = queue.iter().find(|q| q.refused.is_some()).unwrap().rid;
    assert_eq!(
        order,
        vec![
            (seed.node, seed.seq, false, true),
            (1, 5, false, false),
            (7, 1, true, false),
            (1, 6, false, false),
            (8, 1, true, false),
            (1, 7, false, false),
        ],
        "journal order, the placeholders in their place: {queue:?}"
    );
    assert!(
        queue.windows(2).all(|w| w[0].queue_seq < w[1].queue_seq),
        "queue order is the journal's"
    );
    // The rename's placeholder names both dentries and the directory.
    let keys = &queue[2].await_retry.as_ref().expect("placeholder").keys;
    assert!(keys.dentries.contains(&(ROOT_INO, "broken".to_string())));
    assert!(keys.dentries.contains(&(ROOT_INO, "moved".to_string())));
    assert!(keys.shared.contains(&ROOT_INO));
    // A read of a name only a placeholder stands for waits like a read of
    // a queued replay's: the requester's retry writes it again.
    use constellation_meta::ReadKey;
    assert!(meta.replay_touches(&[ReadKey::Dentry(ROOT_INO, "y".into())]));
    assert!(!meta.replay_touches(&[ReadKey::Dentry(ROOT_INO, "unrelated".into())]));
}

/// Review item 8: the queue is decoded once per generation of the queue,
/// and every change to it — an entry queued, refused, marked, forgotten —
/// is a new generation.
#[test]
fn the_queue_snapshot_is_cached_and_every_change_invalidates_it() {
    use constellation_meta::RowClass::Archive;
    use std::sync::Arc;
    let (meta, broken) = node_one_with_a_seed();
    assert!(meta.queue_snapshot().unwrap().is_empty());
    exec_as(&meta, Archive, rid_of(7, 1), &chmod(broken, 0o600));
    meta.drop_held(broken, 5).unwrap();
    let first = meta.queue_snapshot().unwrap();
    assert_eq!(first.entries.len(), 2);
    assert!(
        Arc::ptr_eq(&first, &meta.queue_snapshot().unwrap()),
        "cached"
    );
    // Queued again: a new generation.
    meta.queue_replay(rid_of(1, 50), &chmod(broken, 0o640), &Default::default())
        .unwrap();
    let second = meta.queue_snapshot().unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
    assert_eq!(second.entries.len(), 3);
    // Refused in place: the count is the same, the generation is not.
    let seq = second.entries[2].queue_seq;
    meta.mark_replay_refused(
        seq,
        constellation_meta::Refusal {
            reason: "x".into(),
            ts_unix: 1,
        },
    )
    .unwrap();
    let third = meta.queue_snapshot().unwrap();
    assert!(!Arc::ptr_eq(&second, &third));
    assert!(third.entries[2].refused.is_some());
    // Marked unacked in place.
    meta.mark_replay_unacked(second.entries[1].queue_seq)
        .unwrap();
    let fourth = meta.queue_snapshot().unwrap();
    assert!(!Arc::ptr_eq(&third, &fourth));
    // Forgotten.
    meta.forget_replay(seq).unwrap();
    let fifth = meta.queue_snapshot().unwrap();
    assert!(!Arc::ptr_eq(&fourth, &fifth));
    assert_eq!(fifth.entries.len(), 2);
}

/// Review MUST 1 (a), the FUSE fast path: an own write on keys the queue
/// holds an entry for does not take the fast path (the core parks it
/// behind the queue); one on other keys does.
#[test]
fn the_fast_path_refuses_an_op_on_keys_the_queue_holds() {
    use constellation_meta::RowClass::Archive;
    let (meta, broken) = node_one_with_a_seed();
    let other = meta.create(ROOT_INO, "other", 0o644, 0, 0).unwrap().ino;
    exec_as(&meta, Archive, rid_of(7, 1), &chmod(broken, 0o600));
    assert!(
        meta.root_fast_path(&chmod(broken, 0o700)).is_some(),
        "empty queue"
    );
    meta.drop_held(broken, 5).unwrap();
    assert!(
        meta.root_fast_path(&chmod(broken, 0o700)).is_none(),
        "behind the placeholder and the seed"
    );
    assert!(meta.root_fast_path(&chmod(other, 0o700)).is_some());
}

/// Review MUST 1 (b): a read waits for a placeholder's names and its
/// directory's listing, not for the shared parent's attributes or for a
/// sibling name.
#[test]
fn a_read_of_a_sibling_under_a_placeholders_shared_parent_is_not_blocked() {
    use constellation_meta::ReadKey;
    use constellation_meta::RowClass::Archive;
    let (meta, broken) = node_one_with_a_seed();
    let rename = MutateOp::Rename {
        parent: ROOT_INO,
        name: "broken".into(),
        new_parent: ROOT_INO,
        new_name: "moved".into(),
        noreplace: false,
    };
    exec_as(&meta, Archive, rid_of(7, 1), &rename);
    let create = MutateOp::Create {
        parent: ROOT_INO,
        name: "y".into(),
        ino: 1 << 40,
        mode: 0o644,
        uid: 0,
        gid: 0,
    };
    exec_as(&meta, Archive, rid_of(8, 1), &create);
    meta.drop_held(broken, 1).unwrap();
    let queue = meta.pending_replays().unwrap();
    assert_eq!(
        queue.iter().filter(|q| q.is_await_retry()).count(),
        2,
        "{queue:?}"
    );
    assert!(meta.replay_touches(&[ReadKey::Dentry(ROOT_INO, "y".into())]));
    assert!(
        meta.replay_touches(&[ReadKey::Dir(ROOT_INO)]),
        "the listing changes"
    );
    assert!(
        !meta.replay_touches(&[ReadKey::Dentry(ROOT_INO, "sibling".into())]),
        "a sibling name"
    );
    assert!(
        !meta.replay_touches(&[ReadKey::Ino(ROOT_INO)]),
        "the shared parent's own attributes"
    );
}

/// Review item 9: a placeholder holds the planner's keys — what its row
/// wrote, not only what its records name: an unlink names a dentry and its
/// directory, but writes the inode it takes a name from too, and a later
/// `chmod` of that inode waits behind it.
#[test]
fn a_placeholder_holds_the_keys_its_row_wrote_not_only_the_ones_it_names() {
    use constellation_meta::RowClass::Archive;
    let (meta, broken) = node_one_with_a_seed();
    let unlink = MutateOp::Unlink {
        parent: ROOT_INO,
        name: "broken".into(),
    };
    exec_as(&meta, Archive, rid_of(7, 1), &unlink);
    meta.drop_held(broken, 1).unwrap();
    let queue = meta.pending_replays().unwrap();
    let keys = &queue
        .iter()
        .find(|q| q.rid == rid_of(7, 1))
        .and_then(|q| q.await_retry.as_ref())
        .expect("a placeholder")
        .keys;
    assert!(keys.dentries.contains(&(ROOT_INO, "broken".to_string())));
    assert!(keys.shared.contains(&ROOT_INO));
    assert!(
        keys.inos.contains(&broken),
        "the inode the unlink wrote is missing: {keys:?}"
    );
}

/// Review item 10: a remote row whose rid the log already carries (a
/// segment completed it) is not a placeholder: the drain finds the rid done
/// and forgets the entry; nobody is going to retry what completed.
#[test]
fn a_remote_row_the_log_completed_is_not_a_placeholder() {
    use constellation_meta::RowClass::Archive;
    let (meta, broken) = node_one_with_a_seed();
    exec_as(&meta, Archive, rid_of(7, 1), &chmod(broken, 0o600));
    exec_as(&meta, Archive, rid_of(7, 2), &chmod(broken, 0o640));
    // The log carries the first one's completion (a late segment).
    meta.apply_segment(
        1,
        1,
        &[LogRecord::Completed { rid: rid_of(7, 1) }],
        &TouchSet::default(),
    )
    .unwrap();
    meta.drop_held(broken, 1).unwrap();
    let queue = meta.pending_replays().unwrap();
    let entry = |rid| queue.iter().find(|q| q.rid == rid).expect("queued");
    assert!(!entry(rid_of(7, 1)).is_await_retry(), "{queue:?}");
    assert!(entry(rid_of(7, 2)).is_await_retry());
}

// ---------------------------------------------------------------------
// A drop keeps the pending row of a lost chunk an op in flight names.
// ---------------------------------------------------------------------

/// A holder with `file` created and shipped, one pending upload of `lost`
/// for it and the chunk recorded as unrecoverable: the state the upload
/// pass leaves while the close that will name the chunk has not journaled
/// its manifest yet (a handoff pause, a parked forward, a reattached write
/// session).
fn lost_chunk_before_its_manifest() -> (Meta, Ino, ChunkHash) {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_node_prefix(1).unwrap();
    meta.set_holder_epoch(1);
    let lost = ChunkHash::of(b"bytes that are gone");
    let file = meta.create(ROOT_INO, "file", 0o644, 0, 0).unwrap().ino;
    let rows = batch(&meta);
    ship(&meta, &rows, 1);
    meta.add_pending_upload(&lost, file).unwrap();
    meta.note_unrecoverable_chunks(&[(lost, file)], true)
        .unwrap();
    (meta, file, lost)
}

/// The op executes after the drop: its manifest must still be held, not
/// shipped naming a chunk S3 never received.
fn assert_the_manifest_is_held(meta: &Meta, file: Ino, lost: ChunkHash) {
    meta.set_manifest_with_base(file, None, &manifest_naming(lost, 19), 19, None, None)
        .unwrap();
    let recs = records(&batch(meta));
    assert!(
        !recs
            .iter()
            .any(|r| matches!(r, LogRecord::WriteManifest { .. })),
        "the manifest naming the lost chunk shipped: {recs:?}"
    );
    assert_eq!(meta.held_summary().transactions, 1);
}

#[test]
fn a_drop_keeps_the_pending_row_of_a_lost_chunk_a_live_op_names() {
    let (meta, file, lost) = lost_chunk_before_its_manifest();
    let live = rid_of(1, 14);
    let dropped = meta
        .drop_held_knowing(
            file,
            1234,
            false,
            &|_| false,
            &|h| if *h == lost { vec![live] } else { Vec::new() },
            &constellation_meta::LiveSessions::default(),
        )
        .unwrap();
    assert_eq!(dropped.dropped, 0);
    assert_eq!(dropped.pending_removed, 0, "the row stays: {dropped:?}");
    assert_eq!(dropped.kept_pending, vec![lost]);
    assert_eq!(dropped.kept_for, vec![live]);
    assert!(dropped.describe().contains("run drop-held again"));
    assert_eq!(meta.pending_uploads().unwrap(), vec![(lost, file)]);
    assert_eq!(meta.unrecoverable_chunks().unwrap(), vec![(lost, file)]);
    assert_the_manifest_is_held(&meta, file, lost);

    // Once the op has journaled, the next drop is the seed's: the row goes.
    let dropped = meta.drop_held(file, 1235).unwrap();
    assert_eq!((dropped.dropped, dropped.pending_removed), (1, 1));
    assert!(dropped.kept_pending.is_empty());
    assert!(meta.pending_uploads().unwrap().is_empty());
    assert!(meta.unrecoverable_chunks().unwrap().is_empty());
}

/// An op that is no longer in flight (it settled, or was refused) keeps
/// nothing; a chunk no op names goes as before.
#[test]
fn a_drop_removes_the_row_of_a_lost_chunk_no_op_names() {
    let (meta, file, lost) = lost_chunk_before_its_manifest();
    let dropped = meta
        .drop_held_knowing(
            file,
            1,
            false,
            &|_| false,
            &|_| Vec::new(),
            &constellation_meta::LiveSessions::default(),
        )
        .unwrap();
    assert_eq!(dropped.pending_removed, 1);
    assert!(dropped.kept_pending.is_empty() && dropped.kept_for.is_empty());
    assert!(meta.pending_uploads().unwrap().is_empty());
    assert!(meta.unrecoverable_chunks().unwrap().is_empty());
    let _ = lost;
}

/// An open write session of the inode (the engine's `WriteState` that
/// sealed the chunk) keeps it as well, with no rid to name; so does a
/// session whose flush is in flight (the engine cannot read its manifest).
#[test]
fn a_drop_keeps_the_pending_row_of_a_chunk_a_write_session_names() {
    for sessions in [
        constellation_meta::LiveSessions {
            hashes: [ChunkHash::of(b"bytes that are gone")].into(),
            in_flight: false,
        },
        constellation_meta::LiveSessions {
            hashes: Default::default(),
            in_flight: true,
        },
    ] {
        let (meta, file, lost) = lost_chunk_before_its_manifest();
        let dropped = meta
            .drop_held_knowing(file, 1, false, &|_| false, &|_| Vec::new(), &sessions)
            .unwrap();
        assert_eq!(dropped.pending_removed, 0, "{dropped:?}");
        assert_eq!(dropped.kept_pending, vec![lost]);
        assert!(dropped.kept_for.is_empty());
        assert!(dropped.missing.contains(&lost));
        assert_the_manifest_is_held(&meta, file, lost);
    }
}

/// An op the drop itself rolled back is a seed, refused with its conflict
/// copy: it keeps nothing held.
#[test]
fn an_op_the_drop_rolled_back_does_not_keep_its_chunk() {
    use constellation_meta::RowClass::Lone;
    let meta = Meta::open_in_memory().unwrap();
    meta.set_node_prefix(1).unwrap();
    meta.set_holder_epoch(1);
    let lost = ChunkHash::of(b"bytes that are gone");
    let file = meta.create(ROOT_INO, "file", 0o644, 0, 0).unwrap().ino;
    let rows = batch(&meta);
    ship(&meta, &rows, 1);
    meta.add_pending_upload(&lost, file).unwrap();
    let seed = rid_of(1, 9);
    exec_as(
        &meta,
        Lone,
        seed,
        &MutateOp::SetManifest {
            ino: file,
            base_manifest: None,
            manifest: manifest_naming(lost, 19),
            size: 19,
            mtime_ns: None,
            ctime_ns: None,
        },
    );
    meta.note_unrecoverable_chunks(&[(lost, file)], true)
        .unwrap();
    // The core still lists the executed op among its clients (it awaits
    // its reply): the drop's own seed, not a reason to keep the chunk.
    let dropped = meta
        .drop_held_knowing(
            file,
            1,
            false,
            &|_| false,
            &|_| vec![seed],
            &constellation_meta::LiveSessions::default(),
        )
        .unwrap();
    assert_eq!(dropped.rolled_back, vec![seed], "{dropped:?}");
    assert!(dropped.kept_pending.is_empty(), "{dropped:?}");
    assert_eq!(dropped.pending_removed, 1);
}
