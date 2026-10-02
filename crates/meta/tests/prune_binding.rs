//! Prune-policy binding queries (plan 22, Step 2): root discovery and
//! nearest-ancestor inheritance over a real in-memory replica; and plan
//! 32's snapshot-policy root discovery, which shares the index.

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::prune::PRUNE_XATTR;
use constellation_meta::{Meta, MetaStore, SetXattrMode};

fn set(meta: &Meta, ino: u64, value: &str) {
    meta.set_xattr(ino, PRUNE_XATTR, value.as_bytes(), SetXattrMode::Set)
        .unwrap();
}

#[test]
fn effective_policy_resolves_nearest_ancestor() {
    let meta = Meta::open_in_memory().unwrap();
    let keep = meta.mkdir(ROOT_INO, "keep", 0o755, 0, 0).unwrap();
    let build = meta.mkdir(keep.ino, "build", 0o755, 0, 0).unwrap();
    let file = meta.create(build.ino, "artifact.o", 0o644, 0, 0).unwrap();

    // A policy on `build` and an `off` opt-out on `keep`.
    set(&meta, build.ino, "age(90d)");
    set(&meta, keep.ino, "off");

    // The file inherits build's policy (nearest ancestor wins).
    let (root, expr) = meta.effective_prune_policy(file.ino).unwrap().unwrap();
    assert_eq!(root, build.ino);
    assert_eq!(expr, "age(90d)");

    // `keep` itself resolves to its own `off`.
    let (root, expr) = meta.effective_prune_policy(keep.ino).unwrap().unwrap();
    assert_eq!(root, keep.ino);
    assert_eq!(expr, "off");

    // An unmarked sibling of `keep` has no effective policy.
    let other = meta.mkdir(ROOT_INO, "other", 0o755, 0, 0).unwrap();
    assert!(meta.effective_prune_policy(other.ino).unwrap().is_none());
}

#[test]
fn prune_roots_lists_every_marked_directory() {
    let meta = Meta::open_in_memory().unwrap();
    let a = meta.mkdir(ROOT_INO, "a", 0o755, 0, 0).unwrap();
    let b = meta.mkdir(ROOT_INO, "b", 0o755, 0, 0).unwrap();
    set(&meta, a.ino, "age(30d)");
    set(&meta, b.ino, "lru(high=500G, low=300G)");

    let mut roots = meta.prune_roots().unwrap();
    roots.sort_by_key(|(ino, _)| *ino);
    assert_eq!(roots.len(), 2);
    assert!(roots
        .iter()
        .any(|(ino, e)| *ino == a.ino && e == "age(30d)"));
    assert!(roots
        .iter()
        .any(|(ino, e)| *ino == b.ino && e == "lru(high=500G, low=300G)"));

    // Removing the marker drops it from discovery.
    meta.remove_xattr(a.ino, PRUNE_XATTR).unwrap();
    assert_eq!(meta.prune_roots().unwrap().len(), 1);
}

/// Plan 32 Step 3.1: snapshot-policy roots come from the same index,
/// keyed by inode, so a rename keeps the root (its identity is the
/// inode) and removing the directory drops it. The two xattrs never mix.
#[test]
fn snapshot_policy_roots_follow_the_inode_across_a_rename() {
    use constellation_meta::snapsched::SNAPSHOT_POLICY_XATTR;
    let meta = Meta::open_in_memory().unwrap();
    let proj = meta.mkdir(ROOT_INO, "proj", 0o755, 0, 0).unwrap();
    let db = meta.mkdir(proj.ino, "db", 0o755, 0, 0).unwrap();
    meta.set_xattr(
        proj.ino,
        SNAPSHOT_POLICY_XATTR,
        b"1h:1d 1d:7d",
        SetXattrMode::Set,
    )
    .unwrap();
    meta.set_xattr(db.ino, SNAPSHOT_POLICY_XATTR, b"5m:1d", SetXattrMode::Set)
        .unwrap();
    set(&meta, proj.ino, "age(30d)");

    let want = vec![
        (proj.ino, "1h:1d 1d:7d".to_string()),
        (db.ino, "5m:1d".to_string()),
    ];
    assert_eq!(meta.snapshot_policy_roots().unwrap(), want);
    assert_eq!(
        meta.prune_roots().unwrap(),
        vec![(proj.ino, "age(30d)".to_string())]
    );

    meta.rename(ROOT_INO, "proj", ROOT_INO, "projects").unwrap();
    assert_eq!(meta.snapshot_policy_roots().unwrap(), want);
    assert_eq!(meta.path_of(proj.ino).unwrap(), "/projects");

    meta.rmdir(proj.ino, "db").unwrap();
    assert_eq!(
        meta.snapshot_policy_roots().unwrap(),
        vec![(proj.ino, "1h:1d 1d:7d".to_string())]
    );
    meta.remove_xattr(proj.ino, SNAPSHOT_POLICY_XATTR).unwrap();
    assert!(meta.snapshot_policy_roots().unwrap().is_empty());
    assert_eq!(meta.prune_roots().unwrap().len(), 1);
}
