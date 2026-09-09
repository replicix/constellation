//! Prune-policy binding queries (plan 22, Step 2): root discovery and
//! nearest-ancestor inheritance over a real in-memory replica.

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::prune::PRUNE_XATTR;
use constellation_meta::{MetaStore, SetXattrMode, SqliteMeta};

fn set(meta: &SqliteMeta, ino: u64, value: &str) {
    meta.set_xattr(ino, PRUNE_XATTR, value.as_bytes(), SetXattrMode::Set)
        .unwrap();
}

#[test]
fn effective_policy_resolves_nearest_ancestor() {
    let meta = SqliteMeta::open_in_memory().unwrap();
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
    let meta = SqliteMeta::open_in_memory().unwrap();
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
