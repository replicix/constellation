//! `Meta::replace_ns_from_rebuilt` copies the delegation table's row without
//! `write_table_tx`, which is what sets the fast path's `delegations_any`
//! flag: a node whose old table was empty, rebuilt onto a table with a live
//! delegation, kept the flag false, and `root_fast_path` and
//! `read_delegated` skipped the ownership check. Once it became the root it
//! executed an op on a key the delegate owns, locally.

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{LogRecord, Meta, MutateOp, TouchSet};

#[test]
fn a_rebuild_onto_a_table_with_a_live_delegation_turns_the_fast_path_check_on() {
    let side = Meta::open_in_memory().unwrap();
    side.apply_segment(
        1,
        1,
        &[LogRecord::Delegate {
            dir: ROOT_INO,
            node: 4,
            gen: (1 << 32) | 1,
            designated: false,
            range: (0, 0),
        }],
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
