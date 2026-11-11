//! ADR-59: the capture says, per key, whether a transaction wrote it only
//! by a directory's commutative merge or exclusively (`spec::Capture`).

use super::spec::{self, Capture, MergeDelta, MergeEntry};
use super::{local, ns, Meta};
use crate::MetaStore;
use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::Ino;
use constellation_mtree::keys;

/// A holder with `/a/p` shipped and nothing outstanding.
fn holder() -> (Meta, Ino, Ino) {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_holder_epoch(1);
    let a = meta.mkdir(ROOT_INO, "a", 0o755, 0, 0).unwrap().ino;
    let p = meta.mkdir(a, "p", 0o755, 0, 0).unwrap().ino;
    let rows: Vec<u64> = meta
        .take_journal_grouped(10_000)
        .unwrap()
        .into_iter()
        .flat_map(|(_, b)| b)
        .map(|(s, _)| s)
        .collect();
    meta.ack_journal_rows_at(&rows, 1).unwrap();
    (meta, a, p)
}

/// The keys the newest outstanding transaction wrote, and those of them
/// it wrote only by merge.
fn last_tx(meta: &Meta) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let r = meta.db.read_tx();
    let heads = local::read_journal_tx_heads(&r, meta).unwrap();
    let seq = heads
        .iter()
        .rev()
        .find_map(|(_, head)| head.spec_seq)
        .expect("a captured transaction");
    let row = spec::row_view(&r, meta, seq).unwrap().unwrap();
    (
        row.keys,
        row.merge.into_iter().map(|(key, _)| key).collect(),
    )
}

#[test]
fn a_create_merges_its_parent_and_the_parents_name_in_the_grandparent() {
    let (meta, a, p) = holder();
    meta.create(p, "f", 0o644, 0, 0).unwrap();
    let (keys_written, merged) = last_tx(&meta);
    let mut want = vec![keys::inode(p), keys::dentry(a, b"p")];
    want.sort();
    assert_eq!(merged, want, "written: {keys_written:?}");
    assert!(keys_written.contains(&keys::dentry(p, b"f")));
    assert!(
        !merged.contains(&keys::dentry(p, b"f")),
        "the new name is exclusive"
    );
}

#[test]
fn a_mkdir_merges_the_parents_link_count_and_times() {
    let (meta, _a, p) = holder();
    meta.mkdir(p, "sub", 0o755, 0, 0).unwrap();
    let r = meta.db.read_tx();
    let heads = local::read_journal_tx_heads(&r, &meta).unwrap();
    let seq = heads.iter().rev().find_map(|(_, h)| h.spec_seq).unwrap();
    let row: spec::SpecRowView = spec::row_view(&r, &meta, seq).unwrap().unwrap();
    let delta = row
        .merge
        .iter()
        .find_map(|(k, e)| match e {
            MergeEntry::Inode(d) if *k == keys::inode(p) => Some(*d),
            _ => None,
        })
        .expect("the parent's record is a merge");
    assert_eq!(delta.nlink, 1);
    assert!(delta.mtime_ns.is_some());
}

#[test]
fn a_rename_of_the_directory_itself_is_exclusive_on_it() {
    let (meta, a, p) = holder();
    meta.rename(a, "p", a, "q").unwrap();
    let (written, merged) = last_tx(&meta);
    assert!(written.contains(&keys::inode(p)));
    assert!(
        !merged.contains(&keys::inode(p)),
        "the moved directory's own record"
    );
    assert!(!merged.contains(&keys::dentry(a, b"p")));
    assert!(!merged.contains(&keys::dentry(a, b"q")));
    assert!(
        merged.contains(&keys::inode(a)),
        "its old and new parent merge"
    );
}

#[test]
fn an_explicit_utimes_or_chmod_of_the_directory_is_exclusive() {
    let (meta, a, p) = holder();
    meta.setattr(p, None, None, None, None, Some(1), Some(2))
        .unwrap();
    let (written, merged) = last_tx(&meta);
    assert!(written.contains(&keys::inode(p)));
    assert!(merged.is_empty(), "utimes of p: {merged:?}");
    meta.setattr(p, Some(0o700), None, None, None, None, None)
        .unwrap();
    let (_, merged) = last_tx(&meta);
    assert!(merged.is_empty(), "chmod of p: {merged:?}");
    let _ = a;
}

#[test]
fn one_exclusive_write_makes_the_key_exclusive_for_the_transaction() {
    let (meta, a, p) = holder();
    let capture = Capture::new();
    let mut tx = meta.db.write_tx();
    let dirty = meta.dirty_capturing(&capture);
    super::misc::touch_times_tx(&mut tx, &meta.ns, dirty, p, 5_000).unwrap();
    // The same transaction then sets p's mode "as is".
    let mut rec = ns::get_inode_record(&tx, &meta.ns, p).unwrap().unwrap();
    let before = rec.attrs;
    rec.attrs.mode = 0o700;
    ns::put_inode_record(&mut tx, &meta.ns, dirty, p, &rec, Some(before)).unwrap();
    let captured = capture.finish();
    assert!(
        captured.merge.is_empty(),
        "inode(p) and p's name in a are exclusive: {:?}",
        captured.merge
    );
    let _ = a;
}

#[test]
fn merges_of_one_key_in_one_transaction_add_up() {
    let (meta, _a, p) = holder();
    let capture = Capture::new();
    let mut tx = meta.db.write_tx();
    let dirty = meta.dirty_capturing(&capture);
    super::misc::bump_nlink_tx(&mut tx, &meta.ns, dirty, p, 1, 5_000).unwrap();
    super::misc::touch_times_tx(&mut tx, &meta.ns, dirty, p, 7_000).unwrap();
    super::misc::bump_nlink_tx(&mut tx, &meta.ns, dirty, p, 1, 6_000).unwrap();
    let merged = capture.finish().merge;
    let got = merged
        .iter()
        .find_map(|(k, e)| match e {
            MergeEntry::Inode(d) if *k == keys::inode(p) => Some(*d),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        got,
        MergeDelta {
            nlink: 2,
            mtime_ns: Some(7_000),
            ctime_ns: 7_000
        }
    );
}
