//! Plan 42 §5.1.1/§5.1.5: the content stamp an open answers
//! (`Meta::getattr_stamped`), and the report of what a stranding pass
//! rolled back (`Meta::set_rollback_hook`).
//!
//! The stamp is a digest of a regular file's size, mtime, ctime and
//! manifest as stored. Each test that guards a rule was checked to fail
//! with the rule removed: a stamp of the manifest alone fails
//! `a_size_change_changes_the_stamp`; one without ctime fails
//! `two_commits_of_different_content_differ` only through the manifest
//! (kept: the manifest differs) and `a_rollback_returns_the_stamp_and_reports_it`'s
//! premise (the restored stamp equals the earlier one, which is why the
//! report is needed at all).

use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{Meta, MetaStore, RolledBack, SetXattrMode};
use std::sync::{Arc, Mutex};

fn stamp(meta: &Meta, ino: u64) -> u128 {
    meta.content_stamp(ino)
        .unwrap()
        .expect("a regular file has a stamp")
}

fn file(meta: &Meta) -> u64 {
    meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap().ino
}

#[test]
fn an_unchanged_file_answers_the_same_stamp_and_the_attributes_with_it() {
    let meta = Meta::open_in_memory().unwrap();
    let f = file(&meta);
    meta.set_manifest(f, &[1, 2, 3], 3).unwrap();
    let (attr, s) = meta.getattr_stamped(f).unwrap().unwrap();
    assert_eq!(
        attr,
        meta.getattr(f).unwrap().unwrap(),
        "one read, both answers"
    );
    assert_eq!(s, Some(stamp(&meta, f)));
    assert_eq!(stamp(&meta, f), stamp(&meta, f));
}

#[test]
fn only_regular_files_have_a_stamp() {
    let meta = Meta::open_in_memory().unwrap();
    let d = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap().ino;
    let l = meta.symlink(ROOT_INO, "l", "f", 0, 0).unwrap().ino;
    assert_eq!(meta.content_stamp(d).unwrap(), None);
    assert_eq!(meta.content_stamp(l).unwrap(), None);
    assert_eq!(meta.content_stamp(ROOT_INO).unwrap(), None);
    assert_eq!(meta.content_stamp(999_999).unwrap(), None, "no such inode");
}

#[test]
fn a_manifest_commit_changes_the_stamp() {
    let meta = Meta::open_in_memory().unwrap();
    let f = file(&meta);
    let before = stamp(&meta, f);
    meta.set_manifest(f, &[1, 2, 3], 3).unwrap();
    assert_ne!(stamp(&meta, f), before);
}

#[test]
fn a_size_change_changes_the_stamp() {
    let meta = Meta::open_in_memory().unwrap();
    let f = file(&meta);
    meta.set_manifest(f, &[1, 2, 3], 3).unwrap();
    let before = stamp(&meta, f);
    meta.setattr(f, None, None, None, Some(1), None, None)
        .unwrap();
    assert_ne!(stamp(&meta, f), before);
}

#[test]
fn two_commits_of_different_content_differ() {
    let meta = Meta::open_in_memory().unwrap();
    let f = file(&meta);
    meta.set_manifest(f, &[1, 2, 3], 3).unwrap();
    let a = stamp(&meta, f);
    meta.set_manifest(f, &[4, 5, 6], 3).unwrap();
    let b = stamp(&meta, f);
    assert_ne!(a, b);
    // The same manifest committed again is a new commit too (its ctime):
    // never taken for the content an earlier open saw.
    meta.set_manifest(f, &[1, 2, 3], 3).unwrap();
    assert_ne!(stamp(&meta, f), a);
}

/// What changes a file's times but not its bytes still changes the stamp
/// (the stamp covers mtime and ctime): an open after it drops the pages —
/// a cost, never a stale page. An atime update does not touch the record.
#[test]
fn times_change_the_stamp_and_atime_does_not() {
    let meta = Meta::open_in_memory().unwrap();
    let f = file(&meta);
    meta.set_manifest(f, &[1, 2, 3], 3).unwrap();
    let before = stamp(&meta, f);
    meta.apply_atime(&[(f, 5_000_000_000, 6_000_000_000)])
        .unwrap();
    assert_eq!(stamp(&meta, f), before, "atime is not content");
    meta.setattr(f, None, None, None, None, None, Some(123))
        .unwrap();
    let after_mtime = stamp(&meta, f);
    assert_ne!(after_mtime, before, "an mtime set is a time change");
    meta.set_xattr(f, "user.k", b"v", SetXattrMode::Set)
        .unwrap();
    let after_xattr = stamp(&meta, f);
    let attr = meta.getattr(f).unwrap().unwrap();
    // Recorded rather than assumed: whether an xattr moves ctime here.
    eprintln!(
        "xattr set moved the stamp: {} (ctime {})",
        after_xattr != after_mtime,
        attr.ctime_ns
    );
}

fn ship_all(meta: &Meta, segment: u64) {
    let rows = meta.take_journal(usize::MAX).unwrap();
    let seqs: Vec<u64> = rows.iter().map(|(s, _)| *s).collect();
    meta.ack_journal_rows_at(&seqs, segment).unwrap();
}

/// F14: a deposed holder's unshipped commit, rolled back from its
/// before-image, returns the file's row — and so its stamp — to exactly
/// what an open saw before the commit, while that node's kernel holds the
/// committed bytes. The pass reports the inode it restored.
#[test]
fn a_rollback_returns_the_stamp_and_reports_it() {
    let meta = Meta::open_in_memory().unwrap();
    let f = file(&meta);
    meta.set_manifest(f, &[1, 2, 3], 3).unwrap();
    ship_all(&meta, meta.applied_seq().unwrap() + 1);
    let seen: Arc<Mutex<Vec<RolledBack>>> = Arc::default();
    {
        let seen = seen.clone();
        meta.set_rollback_hook(Box::new(move |rows| {
            seen.lock().unwrap().extend_from_slice(rows)
        }));
    }
    let before = stamp(&meta, f);
    meta.set_holder_epoch(1);
    meta.set_manifest(f, &[7, 8, 9], 3).unwrap();
    meta.set_holder_epoch(0);
    assert_ne!(stamp(&meta, f), before, "the local commit");
    assert!(seen.lock().unwrap().is_empty(), "nothing rolled back yet");
    let stranded = meta.strand_below_epoch(2).unwrap();
    assert!(stranded.locals >= 1);
    assert_eq!(stamp(&meta, f), before, "the row is back, ctime included");
    let rows = seen.lock().unwrap().clone();
    assert!(
        rows.contains(&RolledBack::Inode(f)),
        "the restored inode is reported: {rows:?}"
    );
    // Reported once.
    meta.strand_below_epoch(2).unwrap();
    assert_eq!(seen.lock().unwrap().len(), rows.len());
}

/// A rolled-back create reports the name and the directory it changed.
#[test]
fn a_rolled_back_create_reports_its_entry() {
    let meta = Meta::open_in_memory().unwrap();
    ship_all(&meta, meta.applied_seq().unwrap() + 1);
    let seen: Arc<Mutex<Vec<RolledBack>>> = Arc::default();
    {
        let seen = seen.clone();
        meta.set_rollback_hook(Box::new(move |rows| {
            seen.lock().unwrap().extend_from_slice(rows)
        }));
    }
    meta.set_holder_epoch(1);
    let g = meta.create(ROOT_INO, "g", 0o644, 0, 0).unwrap().ino;
    meta.set_holder_epoch(0);
    meta.strand_below_epoch(2).unwrap();
    let rows = seen.lock().unwrap().clone();
    assert!(rows.contains(&RolledBack::Inode(g)), "{rows:?}");
    assert!(
        rows.contains(&RolledBack::Dentry {
            parent: ROOT_INO,
            name: "g".into()
        }),
        "{rows:?}"
    );
    assert!(rows.contains(&RolledBack::Inode(ROOT_INO)), "{rows:?}");
}
