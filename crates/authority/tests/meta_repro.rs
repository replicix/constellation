//! Meta-level documentation of what the simulation found (plan 30 M5
//! phase 1): a requester that tails the holder's segments through
//! `Meta::apply_segment` converges with a plain replay as long as every
//! shadow or hint is installed on the base the holder evaluated the op
//! against — and diverges when a hint is installed on an older base. No
//! core, no simulation: two `Meta`s and the public speculation API only.
//!
//! The holder's op sequence is the one simulation seed 200 of the bug-B
//! configuration produced (rename-heavy, three names, three nodes).

use constellation_authority::Replica;
use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{execute_mutate, LogRecord, Meta, MetaStore, MutateOp, Rid};

fn rid(node: u64, seq: u64) -> Rid {
    Rid {
        node,
        incarnation: 1,
        seq,
    }
}

fn create(meta: &Meta, name: &str) -> MutateOp {
    MutateOp::Create {
        parent: ROOT_INO,
        name: name.into(),
        ino: meta.allocate_ino(ROOT_INO).unwrap(),
        mode: 0o644,
        uid: 0,
        gid: 0,
    }
}

fn unlink(name: &str) -> MutateOp {
    MutateOp::Unlink {
        parent: ROOT_INO,
        name: name.into(),
    }
}

fn rename(a: &str, b: &str) -> MutateOp {
    MutateOp::Rename {
        parent: ROOT_INO,
        name: a.into(),
        new_parent: ROOT_INO,
        new_name: b.into(),
    }
}

fn listing(meta: &Meta) -> Vec<String> {
    let mut v: Vec<String> = MetaStore::readdir(meta, ROOT_INO)
        .unwrap()
        .into_iter()
        .map(|e| format!("{}={:#x}", e.name, e.ino))
        .collect();
    v.sort();
    v
}

type Segment = (u64, Rid, MutateOp, Vec<LogRecord>);

/// The holder executes the ops in order; each op is one segment.
fn holder_log() -> Vec<Segment> {
    let holder = Meta::open_in_memory().unwrap();
    holder.set_node_prefix(1).unwrap();
    holder.set_holder_epoch(1);
    let n1 = Meta::open_in_memory().unwrap();
    n1.set_node_prefix(1).unwrap();
    let n2 = Meta::open_in_memory().unwrap();
    n2.set_node_prefix(2).unwrap();
    let n3 = Meta::open_in_memory().unwrap();
    n3.set_node_prefix(3).unwrap();
    let ops: Vec<(Rid, MutateOp)> = vec![
        (rid(1, 3), create(&n1, "f0")),   // 1
        (rid(2, 1), unlink("f0")),        // 2
        (rid(3, 1), create(&n3, "f2")),   // 3
        (rid(3, 2), unlink("f2")),        // 4
        (rid(1, 6), create(&n1, "f1")),   // 5
        (rid(1, 8), create(&n1, "f0")),   // 6
        (rid(3, 4), unlink("f0")),        // 7
        (rid(1, 10), create(&n1, "f0")),  // 8
        (rid(2, 3), unlink("f0")),        // 9
        (rid(1, 11), create(&n1, "f0")),  // 10
        (rid(3, 9), rename("f1", "f2")),  // 11
        (rid(2, 5), create(&n2, "f1")),   // 12
        (rid(3, 11), unlink("f1")),       // 13
        (rid(1, 15), rename("f2", "f1")), // 14
        (rid(3, 13), rename("f0", "f1")), // 15
        (rid(1, 16), create(&n1, "f0")),  // 16
        (rid(3, 15), rename("f1", "f2")), // 17
        (rid(2, 7), unlink("f0")),        // 18
        (rid(2, 8), create(&n2, "f1")),   // 19
    ];
    ops.into_iter()
        .enumerate()
        .map(|(i, (rid, op))| {
            let records = execute_mutate(&holder, &op, Some(rid)).unwrap();
            (i as u64 + 1, rid, op, records)
        })
        .collect()
}

fn fresh(prefix: u64) -> Meta {
    let m = Meta::open_in_memory().unwrap();
    m.set_node_prefix(prefix).unwrap();
    m
}

fn apply(meta: &Meta, segments: &[Segment], seq: u64) {
    let (_, _, _, records) = &segments[seq as usize - 1];
    Replica::apply_segment(meta, seq, 1, 0, &[], &[], records).unwrap();
}

fn listing_at(segments: &[Segment], seq: u64) -> Vec<String> {
    let m = fresh(9);
    for s in 1..=seq {
        apply(&m, segments, s);
    }
    listing(&m)
}

/// Node 2's history: every one of its own forwarded ops installed as a
/// shadow right before its segment arrives (the base the holder reported
/// was applied), everything else tailed plainly. Converges at every step.
#[test]
fn shadows_installed_on_the_holders_base_converge() {
    let segments = holder_log();
    let requester = fresh(2);
    for seq in 1..=19u64 {
        let (_, rid, op, records) = &segments[seq as usize - 1];
        if rid.node == 2 {
            assert!(Replica::install_shadow(&requester, *rid, 1, 0, op, records).unwrap());
        }
        apply(&requester, &segments, seq);
        assert_eq!(
            listing(&requester),
            listing_at(&segments, seq),
            "after seq {seq}"
        );
    }
    assert!(!requester.has_outstanding_speculation());
}

/// A shadow installed while the holder's next segment carries a rename
/// of other names (seq 17), then that segment: still converges — the
/// speculation log applies the segment on top and retires the shadow when
/// its own segment (18) arrives.
#[test]
fn a_shadow_survives_an_unrelated_rename_tailed_under_it() {
    let segments = holder_log();
    let requester = fresh(2);
    for seq in 1..=16 {
        apply(&requester, &segments, seq);
    }
    let (_, rid18, op18, records18) = &segments[17];
    assert!(matches!(op18, MutateOp::Unlink { .. }));
    assert!(Replica::install_shadow(&requester, *rid18, 1, 0, op18, records18).unwrap());
    apply(&requester, &segments, 17);
    let mut expected = listing_at(&segments, 17);
    expected.retain(|e| !e.starts_with("f0="));
    assert_eq!(listing(&requester), expected);
    apply(&requester, &segments, 18);
    apply(&requester, &segments, 19);
    assert!(!requester.has_outstanding_speculation());
    assert_eq!(listing(&requester), listing_at(&segments, 19));
}

/// An `EEXIST` hint whose entry the holder read *after* two unshipped
/// renames of `f1` (seqs 14 and 15), installed on a replica at applied
/// 13. The hint's entry (`f1 -> f0's inode`) lands next to the
/// still-present `f0`, and the renames used to evict the wrong inode:
/// plan 30 §M3a's known window, which the authority core avoids by
/// refusing such a base (`PeerMsg::MutateReply::base`). EC2 campaign 4
/// B-2 closed it in the meta layer too: a tailed segment overlapping
/// outstanding speculation goes in *under* it (the hint is rolled back,
/// the renames applied, the hint redone or retired), so it converges.
#[test]
fn a_hint_installed_on_a_stale_base_converges() {
    let segments = holder_log();
    let requester = fresh(2);
    for seq in 1..=13 {
        apply(&requester, &segments, seq);
    }
    let f1_after_15 = {
        let m = fresh(9);
        for seq in 1..=15 {
            apply(&m, &segments, seq);
        }
        Replica::entry_as_record(&m, ROOT_INO, "f1").unwrap()
    };
    assert!(Replica::install_hint(&requester, rid(2, 99), &[f1_after_15], 16, 1, 0).unwrap());
    for seq in 14..=19 {
        apply(&requester, &segments, seq);
    }
    assert!(!requester.has_outstanding_speculation());
    assert_eq!(listing(&requester), listing_at(&segments, 19));
}
