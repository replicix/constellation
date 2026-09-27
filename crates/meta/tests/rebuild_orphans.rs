//! `Meta::replace_ns_from_rebuilt`'s `keep_orphans`: a namespace rebuild
//! (a deposition recovery, a retention-gap rebuild) keeps the orphan
//! records some view still has open — the manifest a handle reads
//! through — and drops the rest; a kept inode the rebuilt namespace
//! still names is not an orphan there and the namespace record wins.

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{Meta, MetaStore};
use std::collections::HashSet;

#[test]
fn a_rebuild_keeps_the_orphans_that_are_held_open_and_drops_the_rest() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_node_prefix(1).unwrap();
    // `relinked` first: the side replica allocates it first too, so the
    // two agree on its inode.
    let relinked = meta.create(ROOT_INO, "relinked", 0o644, 0, 0).unwrap();
    let open = meta.create(ROOT_INO, "open", 0o644, 0, 0).unwrap();
    meta.set_manifest(open.ino, &[0xab; 64], 4096).unwrap();
    let closed = meta.create(ROOT_INO, "closed", 0o644, 0, 0).unwrap();
    meta.set_manifest(closed.ino, &[0xcd; 64], 4096).unwrap();
    for name in ["open", "closed", "relinked"] {
        meta.unlink(ROOT_INO, name).unwrap();
    }
    assert_eq!(meta.orphans().unwrap().len(), 3);

    // The rebuilt namespace (the log's view): a fresh tree that still
    // names `relinked` (this node's unlink of it never shipped).
    let side = Meta::open_in_memory().unwrap();
    side.set_node_prefix(1).unwrap();
    let side_relinked = side.create(ROOT_INO, "relinked", 0o644, 0, 0).unwrap();
    assert_eq!(side_relinked.ino, relinked.ino, "same allocation order");
    // Burn the allocations `open` and `closed` took, so the side's own
    // directory does not land on either inode.
    for name in ["p-open", "p-closed"] {
        let p = side.create(ROOT_INO, name, 0o644, 0, 0).unwrap();
        side.unlink(ROOT_INO, name).unwrap();
        side.reap_orphan(p.ino).unwrap();
    }
    let dir = side.mkdir(ROOT_INO, "from-the-log", 0o755, 0, 0).unwrap();
    assert!(dir.ino != open.ino && dir.ino != closed.ino);

    let keep: HashSet<_> = [open.ino, relinked.ino].into_iter().collect();
    meta.replace_ns_from_rebuilt(&side, &keep).unwrap();

    // The open orphan survives with its manifest; the closed one is gone.
    assert_eq!(meta.orphans().unwrap(), vec![open.ino]);
    assert_eq!(meta.manifest(open.ino).unwrap(), Some(vec![0xab; 64]));
    let attr = meta.getattr(open.ino).unwrap().unwrap();
    assert_eq!(attr.nlink, 0);
    assert!(meta.getattr(closed.ino).unwrap().is_none());
    // The re-linked inode is the namespace's, not an orphan.
    assert!(meta.lookup(ROOT_INO, "relinked").unwrap().is_some());
    assert_eq!(meta.getattr(relinked.ino).unwrap().unwrap().nlink, 1);
    assert!(meta.lookup(ROOT_INO, "from-the-log").unwrap().is_some());
    // And it still reaps normally at the last close.
    meta.reap_orphan(open.ino).unwrap();
    assert!(meta.orphans().unwrap().is_empty());
    assert!(meta.getattr(open.ino).unwrap().is_none());
}
