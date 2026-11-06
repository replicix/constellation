//! `Meta::replace_ns_from_rebuilt` copies the delegation table's row without
//! `write_table_tx`, which is what sets the fast path's `delegations_any`
//! flag: a node whose old table was empty, rebuilt onto a table with a live
//! delegation, kept the flag false, and `root_fast_path` and
//! `read_delegated` skipped the ownership check. Once it became the root it
//! executed an op on a key the delegate owns, locally.

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{LogRecord, Meta, MutateOp, SubtreeKind, TouchSet};

#[test]
fn a_rebuild_onto_a_table_with_a_live_delegation_turns_the_fast_path_check_on() {
    let side = Meta::open_in_memory().unwrap();
    side.apply_segment(
        1,
        1,
        &[LogRecord::delegate_one(
            4,
            (1 << 32) | 1,
            ROOT_INO,
            SubtreeKind::Placed,
        )],
        &TouchSet::default(),
    )
    .unwrap();
    let op = MutateOp::Create {
        parent: ROOT_INO,
        name: "x".into(),
        ino: (1u64 << 40) | 1,
        mode: 0o644,
        uid: 0,
        gid: 0,
    };
    assert!(side.delegations_any());
    assert!(
        side.root_fast_path(&op).is_none(),
        "the side's table owns it"
    );

    let meta = Meta::open_in_memory().unwrap();
    assert!(!meta.delegations_any(), "an empty table");
    meta.replace_ns_from_rebuilt(&side, &Default::default())
        .unwrap();
    assert!(meta.delegation_table().iter().next().is_some());
    assert!(meta.delegations_any());
    assert!(
        meta.root_fast_path(&op).is_none(),
        "a key under a live delegation is not the root's to execute locally"
    );
    assert!(meta.read_delegated(ROOT_INO, None));
}

/// M1.2 step 8a: a `DelegateNode` of two subtrees, a later one that grows
/// the live stream, a `SubtreeReleased` and an end, through the store: the
/// ownership walk resolves a dentry under either subtree to the one stream.
#[test]
fn the_stores_walk_resolves_both_subtrees_to_one_stream() {
    use constellation_meta::delegation::Ownership;
    use constellation_meta::SubtreeGrant;
    let meta = Meta::open_in_memory().unwrap();
    let mkdir = |name: &str, ino: u64| LogRecord::Mkdir {
        parent: ROOT_INO,
        name: name.into(),
        ino,
        mode: 0o755,
        uid: 0,
        gid: 0,
        time_ns: 1,
    };
    let (a, b, c) = (1u64 << 40 | 2, 1u64 << 40 | 3, 1u64 << 40 | 4);
    meta.apply_records(&[mkdir("a", a), mkdir("b", b), mkdir("c", c)])
        .unwrap();
    let gen = (1u64 << 32) | 1;
    let grant = |dir, kind| SubtreeGrant { dir, kind };
    meta.apply_records(&[LogRecord::DelegateNode {
        node: 4,
        gen,
        dirs: vec![grant(a, SubtreeKind::Placed)],
    }])
    .unwrap();
    // The grow: same node, same generation.
    meta.apply_records(&[LogRecord::DelegateNode {
        node: 4,
        gen,
        dirs: vec![grant(b, SubtreeKind::Sticky)],
    }])
    .unwrap();
    let table = meta.delegation_table();
    assert_eq!(table.streams().count(), 1);
    assert_eq!(table.stream(4, gen).unwrap().subtrees.len(), 2);
    let owner = |dir: u64, name: &str| {
        let mut k = TouchSet::default();
        k.dentries.insert((dir, name.to_string()));
        meta.resolve_ownership(&k)
    };
    for dir in [a, b] {
        match owner(dir, "x") {
            Ownership::Delegated(d) => assert_eq!((d.node, d.gen), (4, gen)),
            o => panic!("{o:?}"),
        }
    }
    assert_eq!(owner(c, "x"), Ownership::Root);
    // One rename between the two subtrees is the stream's own op.
    let mut between = TouchSet::default();
    between.dentries.insert((a, "x".into()));
    between.dentries.insert((b, "y".into()));
    assert!(matches!(
        meta.resolve_ownership(&between),
        Ownership::Delegated(_)
    ));
    // A partial release: only `a` leaves.
    meta.apply_records(&[LogRecord::SubtreeReleased {
        node: 4,
        gen,
        dir: a,
        at: 3,
    }])
    .unwrap();
    assert_eq!(owner(a, "x"), Ownership::Root);
    assert!(matches!(owner(b, "x"), Ownership::Delegated(_)));
    assert!(meta.delegations_any());
    // The end, with its cut.
    meta.apply_records(&[LogRecord::DelegateNodeEnded {
        node: 4,
        gen,
        through: 3,
    }])
    .unwrap();
    assert_eq!(owner(b, "x"), Ownership::Root);
    assert!(meta.delegation_table().is_empty());
}
