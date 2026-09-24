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
    meta.set_manifest_dirty(broken, None, &manifest_naming(lost, 19), 19, &[lost])
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
    assert_eq!(s.meta.journal_len().unwrap(), 0);
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
    // Nothing is held any more: the next plan ships everything.
    assert!(batch(&s.meta).is_empty());
    assert!(
        s.meta.drop_held(s.broken, 1).is_err(),
        "nothing left to drop"
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

/// Without capture (holder capture off, or not holding) a transaction has
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
    meta.set_manifest_dirty(big, None, &manifest_naming(pending, 33), 33, &[pending])
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
    ship(&meta, &rest, 2);
    assert_eq!(meta.journal_len().unwrap(), 0);
}

/// A later change to the deferred file depends on its manifest and waits
/// with it; nothing is skipped out of dependency order.
#[test]
fn a_dependent_of_a_deferred_manifest_waits_with_it() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    let pending = ChunkHash::of(b"still uploading");
    let big = meta.create(ROOT_INO, "big", 0o644, 0, 0).unwrap().ino;
    meta.set_manifest_dirty(big, None, &manifest_naming(pending, 15), 15, &[pending])
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
