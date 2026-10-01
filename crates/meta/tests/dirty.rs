//! Plan 29 M2: every write to `ns` must record the touched key in
//! `dirty`, in the same transaction, so a publish's read set can be
//! exactly `dirty_snapshot` rather than something re-derived from the
//! journal.
//!
//! Checked generically rather than by hand per API: dump `ns` before and
//! after each mutating call, compute the exact key set that changed
//! (added, removed, or the same key with a different value), and assert
//! `dirty_snapshot` covers it. Over-approximation (a key marked dirty
//! that did not actually change — plan 29 M2's doc for
//! `clear_dirty_upto`) is fine and expected; under-approximation is the
//! bug this test exists to catch, because it is exactly the kind of
//! omission `docs/plans/v1/done/29-fjall-metadata-engine.md`'s M2 section
//! warns a hand-picked key list would eventually miss.

use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::InodeKind;
use constellation_meta::{CloneSpec, LogRecord, Meta, MetaStore, SetXattrMode, SnapshotRow};
use std::collections::{BTreeMap, BTreeSet};

fn changed_keys(before: &[(Vec<u8>, Vec<u8>)], after: &[(Vec<u8>, Vec<u8>)]) -> BTreeSet<Vec<u8>> {
    let before: BTreeMap<&[u8], &[u8]> = before
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
    let after: BTreeMap<&[u8], &[u8]> = after
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
    let mut out = BTreeSet::new();
    for (k, v) in &after {
        if before.get(k) != Some(v) {
            out.insert(k.to_vec());
        }
    }
    for k in before.keys() {
        if !after.contains_key(k) {
            out.insert(k.to_vec());
        }
    }
    out
}

fn current_dirty(meta: &Meta) -> BTreeSet<Vec<u8>> {
    meta.read_consistent(|snap| meta.dirty_snapshot(snap))
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect()
}

/// Assert every key that changed in `ns` since `before` is presently
/// dirty. `dirty` only grows across the calls in this test (nothing
/// clears it), so a key found dirty by an earlier check stays valid
/// evidence for a later one — this only ever adds assertions.
fn assert_changes_are_dirty(meta: &Meta, before: &[(Vec<u8>, Vec<u8>)], what: &str) {
    let after = meta.ns_dump().unwrap();
    let changed = changed_keys(before, &after);
    let dirty = current_dirty(meta);
    for key in &changed {
        assert!(
            dirty.contains(key),
            "{what}: key {key:02x?} changed in ns but is not in dirty"
        );
    }
}

macro_rules! step {
    ($meta:expr, $label:expr, $body:expr) => {{
        let before = $meta.ns_dump().unwrap();
        $body;
        assert_changes_are_dirty(&$meta, &before, $label);
    }};
}

#[test]
fn every_mutating_api_dirties_the_keys_it_changes() {
    let meta = Meta::open_in_memory().unwrap();
    // Genesis's root-inode insert already dirtied `keys::inode(ROOT_INO)`
    // (see `bootstrap_dirties_the_genesis_root` below); this test only
    // cares about mutations from here on.

    let dir_ino;
    step!(meta, "mkdir", {
        dir_ino = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap().ino;
    });

    let f1;
    step!(meta, "create", {
        f1 = meta.create(dir_ino, "f1", 0o644, 1, 1).unwrap().ino;
    });

    step!(meta, "symlink", {
        meta.symlink(dir_ino, "s1", "target", 0, 0).unwrap();
    });

    step!(meta, "mknod", {
        meta.mknod(
            dir_ino,
            "n1",
            InodeKind::Fifo,
            0o600,
            0,
            0,
            Default::default(),
        )
        .unwrap();
    });

    let other = meta.mkdir(ROOT_INO, "other", 0o755, 0, 0).unwrap().ino;

    step!(meta, "link", {
        meta.link(f1, other, "hardlink").unwrap();
    });

    step!(meta, "setattr", {
        meta.setattr(f1, Some(0o600), None, None, None, None, None)
            .unwrap();
    });

    step!(meta, "set_manifest", {
        meta.set_manifest(f1, &[1, 2, 3, 4], 4096).unwrap();
    });

    step!(meta, "set_xattr", {
        meta.set_xattr(f1, "user.a", b"one", SetXattrMode::Set)
            .unwrap();
    });

    // Push the set over XATTR_INLINE so it spills to 0x03 keys.
    step!(meta, "set_xattr (spills to 0x03)", {
        for i in 0..40 {
            meta.set_xattr(f1, &format!("user.k{i}"), b"vvvvvvvv", SetXattrMode::Set)
                .unwrap();
        }
    });

    step!(meta, "remove_xattr", {
        meta.remove_xattr(f1, "user.a").unwrap();
    });

    step!(meta, "rename", {
        meta.rename(dir_ino, "s1", other, "moved").unwrap();
    });

    meta.create(other, "victim", 0o644, 0, 0).unwrap();
    step!(meta, "rename over an existing file", {
        meta.create(dir_ino, "src2", 0o644, 0, 0).unwrap();
        meta.rename(dir_ino, "src2", other, "victim").unwrap();
    });

    step!(meta, "unlink", {
        meta.unlink(other, "hardlink").unwrap();
    });

    meta.mkdir(ROOT_INO, "empty", 0o755, 0, 0).unwrap();
    step!(meta, "rmdir", {
        meta.rmdir(ROOT_INO, "empty").unwrap();
    });

    step!(meta, "record_snapshot", {
        meta.record_snapshot(&SnapshotRow::new("snap1", "/", "s", "abc", 0))
            .unwrap();
    });

    // Plan 32 §0.4's hold rewrites the snapshot row in place, which is the
    // easiest kind of `ns` write to forget to dirty: the key already
    // existed, so nothing about it looks new.
    step!(meta, "set_snapshot_hold", {
        meta.set_snapshot_hold("snap1", true, Some("user:attila"), false)
            .unwrap()
            .expect("the row this test just recorded");
    });

    step!(meta, "delete_snapshot", {
        meta.delete_snapshot("/", "s").unwrap();
    });

    step!(meta, "write_quota", {
        meta.write_quota(Some(1 << 30)).unwrap();
    });

    step!(meta, "eager_clone", {
        meta.eager_clone(
            "/src",
            "snap1",
            "abc",
            "cloned",
            &[CloneSpec {
                parent_index: None,
                name: "cloned".into(),
                kind: InodeKind::Dir,
                mode: 0o755,
                uid: 0,
                gid: 0,
                size: 0,
                mtime_ns: 1,
                target: None,
                manifest: None,
                xattrs: vec![],
            }],
        )
        .unwrap();
    });

    step!(meta, "publish_file", {
        let ino = meta.allocate_ino(ROOT_INO).unwrap();
        meta.publish_file(
            ROOT_INO,
            "published",
            ino,
            0o644,
            0,
            0,
            1,
            &[9, 9, 9],
            3,
            &[],
            false,
        )
        .unwrap();
    });

    step!(meta, "set_manifest_dirty", {
        let ino = meta.child_ino(ROOT_INO, "published").unwrap().unwrap();
        meta.set_manifest_dirty(ino, None, &[1, 2], 2, &[]).unwrap();
    });

    step!(meta, "set_manifest_with_base", {
        let ino = meta.child_ino(ROOT_INO, "published").unwrap().unwrap();
        let base = meta.manifest(ino).unwrap().unwrap();
        meta.set_manifest_with_base(ino, Some(&base), &[3, 4], 2)
            .unwrap();
    });

    step!(meta, "apply_records (replay)", {
        let records = vec![LogRecord::Mkdir {
            parent: ROOT_INO,
            name: "replayed".into(),
            ino: (7u64 << 40) | 1,
            mode: 0o755,
            uid: 0,
            gid: 0,
            time_ns: 1,
        }];
        meta.apply_records(&records).unwrap();
    });

    step!(meta, "replace_ns_from_rebuilt", {
        let side = Meta::open_in_memory().unwrap();
        side.mkdir(ROOT_INO, "reconciled", 0o755, 0, 0).unwrap();
        meta.replace_ns_from_rebuilt(&side, &Default::default())
            .unwrap();
    });
}

/// `apply_atime` never touches the tree at all (§P6 excludes atime from
/// it), so it must dirty nothing — the read path must never look like a
/// write to the publisher.
#[test]
fn apply_atime_dirties_nothing() {
    let meta = Meta::open_in_memory().unwrap();
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let before = current_dirty(&meta);
    meta.apply_atime(&[(f.ino, 1_000, 1_000)]).unwrap();
    assert_eq!(current_dirty(&meta), before, "atime must never dirty ns");
}

/// Genesis (a brand-new store with no commit to bootstrap from) has to
/// dirty its own root-inode insert, because a real bootstrap's
/// `clear_all_dirty` is the only other place that dirty set could start
/// clean from — see `crates/meta/src/store/bootstrap.rs`.
#[test]
fn bootstrap_dirties_the_genesis_root() {
    let meta = Meta::open_in_memory().unwrap();
    let dirty = current_dirty(&meta);
    assert!(
        dirty.contains(&constellation_mtree::keys::inode(ROOT_INO)),
        "genesis must dirty the root inode key"
    );
}

/// `clear_all_dirty` (the bootstrap-ingestion path) removes exactly the
/// genesis guess, and a subsequent mutation is dirtied normally again.
#[test]
fn clear_all_dirty_empties_the_set_and_tracking_resumes() {
    let meta = Meta::open_in_memory().unwrap();
    assert!(!current_dirty(&meta).is_empty());
    meta.clear_all_dirty().unwrap();
    assert!(current_dirty(&meta).is_empty());

    meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    assert!(
        !current_dirty(&meta).is_empty(),
        "tracking must resume after a clear"
    );
}

/// The heart of `clear_dirty_upto`: a key re-dirtied after the publish's
/// snapshot was taken must survive the clear, because the publish never
/// saw the value that re-dirtied it.
#[test]
fn clear_dirty_upto_keeps_a_key_re_dirtied_after_the_snapshot() {
    let meta = Meta::open_in_memory().unwrap();
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let key = constellation_mtree::keys::inode(f.ino);

    // The publish's snapshot: whatever is dirty right now.
    let observed = meta
        .read_consistent(|snap| meta.dirty_snapshot(snap))
        .unwrap();

    // A concurrent write re-dirties the same key with a fresh counter,
    // as if another writer (or the same one, on the next round) touched
    // it again before the publish's clear runs.
    meta.setattr(f.ino, Some(0o600), None, None, None, None, None)
        .unwrap();

    meta.clear_dirty_upto(&observed).unwrap();
    assert!(
        current_dirty(&meta).contains(&key),
        "a key re-dirtied after the snapshot must survive the clear"
    );
}

/// The other half: a key whose counter has not moved since the snapshot
/// is retired normally.
#[test]
fn clear_dirty_upto_retires_an_unchanged_key() {
    let meta = Meta::open_in_memory().unwrap();
    let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let key = constellation_mtree::keys::inode(f.ino);

    let observed = meta
        .read_consistent(|snap| meta.dirty_snapshot(snap))
        .unwrap();
    meta.clear_dirty_upto(&observed).unwrap();
    assert!(!current_dirty(&meta).contains(&key));
}
