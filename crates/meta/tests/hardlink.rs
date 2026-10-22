//! Plan 45: what an inode's names cost, and what form their dentries hold.
//!
//! A `0x02` dentry carries a copy of its inode's attributes only while the
//! inode is a directory or has one name; a name of a hard-linked inode is
//! the remote form (`ino` + `kind`), and the inode's `0x01` record is the
//! only place its attributes live. These tests drive the operations through
//! the public API and read the result out of the namespace dump:
//!
//! - a `link`, an `unlink`, a `rename`, a `chmod` and a manifest commit
//!   change the same few keys at 500 names as at 2 (the plan's
//!   fail-before: today they rewrite every name's copy);
//! - the 1 → 2 → 3 → 2 → 1 → 0 transitions, a rename over a hard-linked
//!   target, an exchange of two hard-linked files, `setattr` and a
//!   manifest commit at three names, all leave every dentry in the form
//!   its inode decides, on the node that did them and on a replica that
//!   replays the log record by record;
//! - a lookup of a remote entry answers what `getattr` answers.

use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::Ino;
use constellation_meta::{LogRecord, Meta, MetaStore};
use constellation_mtree::keys::Key;
use constellation_mtree::record::{Attrs, DentryRecord, InodeRecord, Kind};
use std::collections::{BTreeMap, BTreeSet};

fn dump(meta: &Meta) -> BTreeMap<Vec<u8>, Vec<u8>> {
    meta.ns_dump().unwrap().into_iter().collect()
}

/// Keys of `ns` that differ between two dumps: added, removed, or changed.
fn changed(before: &BTreeMap<Vec<u8>, Vec<u8>>, after: &BTreeMap<Vec<u8>, Vec<u8>>) -> usize {
    after
        .iter()
        .filter(|(k, v)| before.get(*k) != Some(*v))
        .count()
        + before.keys().filter(|k| !after.contains_key(*k)).count()
}

/// The dentry `(parent, name)` as stored: the copy it holds, `None` for the
/// remote form.
fn form(meta: &Meta, parent: Ino, name: &str) -> Option<Attrs> {
    let key = constellation_mtree::keys::dentry(parent, name.as_bytes());
    let value = dump(meta).remove(&key).expect("the dentry exists");
    DentryRecord::decode(&value).unwrap().attrs
}

/// Every dentry of `meta` is in the form its inode decides, judged from the
/// dump alone and by the number of names the `0x04` index gives the inode
/// (not by `nlink`, which must agree with it): a copy equal to the inode's
/// attributes for a directory or a one-name inode, the remote form with the
/// inode's kind otherwise.
fn assert_forms(meta: &Meta, label: &str) {
    let dump = dump(meta);
    let mut names: BTreeMap<u64, u32> = BTreeMap::new();
    for key in dump.keys() {
        if let Ok(Key::RDentry { ino, .. }) = Key::parse(key) {
            *names.entry(ino).or_default() += 1;
        }
    }
    let mut checked = 0;
    for (key, value) in &dump {
        let Ok(Key::Dentry { parent_ino, name }) = Key::parse(key) else {
            continue;
        };
        let name = String::from_utf8_lossy(name);
        let d = DentryRecord::decode(value).unwrap();
        let inode = dump
            .get(&constellation_mtree::keys::inode(d.ino))
            .unwrap_or_else(|| panic!("{label}: {parent_ino}/{name}: dangling"));
        let rec = InodeRecord::decode(inode).unwrap();
        let n = names[&d.ino];
        if rec.attrs.kind != Kind::Dir {
            assert_eq!(
                rec.attrs.nlink, n,
                "{label}: nlink of {} vs its names",
                d.ino
            );
        }
        if rec.attrs.kind == Kind::Dir || n == 1 {
            assert_eq!(
                d.attrs,
                Some(rec.attrs),
                "{label}: {parent_ino}/{name}: the copy of inode {} (names {n}) is missing or stale",
                d.ino
            );
        } else {
            assert_eq!(
                d.attrs, None,
                "{label}: {parent_ino}/{name}: a copy under one of {n} names of inode {}",
                d.ino
            );
            assert_eq!(d.kind, rec.attrs.kind, "{label}: {parent_ino}/{name}: kind");
        }
        checked += 1;
    }
    assert!(checked > 0, "{label}: no dentries");
}

struct World {
    meta: Meta,
    d: Ino,
    e: Ino,
    f: Ino,
}

/// A file `d/f` with `n` names in all (`d/f` and `n - 1` links in `e`).
fn world(n: u32) -> World {
    let meta = Meta::open_in_memory().unwrap();
    let d = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap().ino;
    let e = meta.mkdir(ROOT_INO, "e", 0o755, 0, 0).unwrap().ino;
    let f = meta.create(d, "f", 0o644, 0, 0).unwrap().ino;
    meta.set_manifest(f, b"manifest", 4096).unwrap();
    for i in 1..n {
        meta.link(f, e, &format!("l{i}")).unwrap();
    }
    World { meta, d, e, f }
}

/// The keys one operation changes, at `n` names.
fn key_counts(n: u32) -> [usize; 6] {
    let w = world(n);
    let m = &w.meta;
    let mut counts = [0; 6];
    let mut measure = |slot: usize, op: &dyn Fn()| {
        let before = dump(m);
        op();
        counts[slot] = changed(&before, &dump(m));
    };
    measure(0, &|| {
        m.link(w.f, w.e, "extra").unwrap();
    });
    measure(1, &|| m.unlink(w.e, "extra").unwrap());
    measure(2, &|| {
        m.setattr(w.f, Some(0o600), None, None, None, None, None)
            .unwrap();
    });
    measure(3, &|| m.set_manifest(w.f, b"manifest-2", 8192).unwrap());
    measure(4, &|| m.rename(w.d, "f", w.d, "f2").unwrap());
    measure(5, &|| {
        m.setattr(w.f, None, Some(7), Some(8), None, None, Some(5))
            .unwrap();
    });
    counts
}

/// The plan's fail-before: today a `link` at 500 names rewrites 500 copies,
/// and so do `unlink`, `chmod`, a manifest commit and a `rename`. The
/// number of keys they change does not depend on the number of names.
#[test]
fn link_cost_is_flat_in_the_link_count() {
    let two = key_counts(2);
    let many = key_counts(500);
    assert_eq!(
        many, two,
        "link, unlink, chmod, manifest commit, rename, chown at 500 names change {many:?} keys, at 2 names {two:?}"
    );
    // link: the inode, the new name's two keys, the directory and its copy.
    assert!(many[0] <= 6, "{many:?}");
    assert!(many[1] <= 6, "{many:?}");
    // chmod, a manifest commit and a chown: the inode record alone.
    assert_eq!((many[2], many[3], many[5]), (1, 1, 1), "{many:?}");
    // rename: two keys out, two in, the inode, the directory and its copy.
    assert!(many[4] <= 8, "{many:?}");
}

/// The 1 → 2 transition costs one key more than a link at n >= 2: the
/// first name's copy becomes the remote form.
#[test]
fn the_first_link_rewrites_the_one_existing_name_and_no_other() {
    let w = world(1);
    let before = dump(&w.meta);
    assert!(form(&w.meta, w.d, "f").is_some());
    w.meta.link(w.f, w.e, "l1").unwrap();
    let after = dump(&w.meta);
    assert_eq!(changed(&before, &after), key_counts(2)[0] + 1);
    assert_eq!(form(&w.meta, w.d, "f"), None);
    assert_eq!(form(&w.meta, w.e, "l1"), None);
    assert_forms(&w.meta, "after the first link");
}

#[test]
fn unlink_to_one_name_materialises_the_copy() {
    let w = world(3);
    let m = &w.meta;
    assert_eq!(form(m, w.d, "f"), None);
    m.unlink(w.e, "l2").unwrap();
    assert_eq!(form(m, w.d, "f"), None, "two names: both remote");
    assert_forms(m, "at two names");
    m.unlink(w.e, "l1").unwrap();
    let attrs = m.getattr(w.f).unwrap().unwrap();
    assert_eq!(attrs.nlink, 1);
    let copy = form(m, w.d, "f").expect("the survivor holds the copy again");
    assert_eq!(
        (copy.nlink, copy.size, copy.mode, copy.ctime_ns),
        (1, attrs.size, attrs.mode, attrs.ctime_ns)
    );
    assert_forms(m, "at one name");
    // The last name goes: the inode is an orphan, nothing dangles.
    m.unlink(w.d, "f").unwrap();
    assert_forms(m, "at none");
}

#[test]
fn lookup_of_a_remote_entry_equals_getattr() {
    let w = world(3);
    let m = &w.meta;
    let reads = m.inode_reads();
    let looked = m.lookup(w.e, "l1").unwrap().unwrap();
    assert_eq!(
        m.inode_reads(),
        reads + 1,
        "a remote entry costs one 0x01 read"
    );
    assert_eq!(looked, m.getattr(w.f).unwrap().unwrap());
    assert_eq!(looked.nlink, 3);
    // After a change through another name, the same.
    m.setattr(w.f, Some(0o600), None, None, Some(5), None, None)
        .unwrap();
    let looked = m.lookup(w.d, "f").unwrap().unwrap();
    assert_eq!(looked, m.getattr(w.f).unwrap().unwrap());
    assert_eq!((looked.mode & 0o7777, looked.size), (0o600, 5));
    // A name that is its inode's only name answers from its copy.
    let single = m.create(w.d, "single", 0o644, 0, 0).unwrap().ino;
    let reads = m.inode_reads();
    assert_eq!(
        m.lookup(w.d, "single").unwrap().unwrap(),
        m.getattr(single).unwrap().unwrap()
    );
    assert_eq!(m.inode_reads(), reads, "a copy answers without 0x01");
}

#[test]
fn a_rename_over_a_hard_linked_target_keeps_the_form() {
    let w = world(2); // f: d/f, e/l1
    let m = &w.meta;
    let h = m.create(w.d, "h", 0o644, 0, 0).unwrap().ino;
    // h replaces one of f's two names: f keeps one name and its copy.
    m.rename(w.d, "h", w.e, "l1").unwrap();
    assert_forms(m, "after the rename over one of two names");
    assert!(form(m, w.d, "f").is_some(), "the survivor holds the copy");
    assert!(form(m, w.e, "l1").is_some(), "h's new name holds its copy");
    assert_eq!(m.lookup(w.e, "l1").unwrap().unwrap().ino, h);
    assert_eq!(m.getattr(w.f).unwrap().unwrap().nlink, 1);
    // A hard-linked inode renamed over another file: it keeps its form.
    m.link(w.f, w.e, "l2").unwrap();
    let g = m.create(w.d, "g", 0o644, 0, 0).unwrap().ino;
    m.rename(w.d, "f", w.d, "g").unwrap();
    assert_forms(m, "after a hard-linked file replaced a file");
    assert_eq!(form(m, w.d, "g"), None);
    assert_eq!(m.getattr(w.f).unwrap().unwrap().nlink, 2);
    assert_eq!(
        m.getattr(g).unwrap().unwrap().nlink,
        0,
        "the replaced file is an orphan"
    );
    // Renaming one name of three: all stay remote.
    m.link(w.f, w.e, "l3").unwrap();
    m.rename(w.e, "l3", w.d, "moved").unwrap();
    assert_forms(m, "after renaming one of three names");
    assert_eq!(form(m, w.d, "moved"), None);
}

#[test]
fn an_exchange_of_two_hard_linked_files_keeps_the_form() {
    let w = world(2);
    let m = &w.meta;
    let g = m.create(w.d, "g", 0o600, 0, 0).unwrap().ino;
    m.link(g, w.e, "gl").unwrap();
    m.exchange(w.d, "f", w.d, "g").unwrap();
    assert_forms(m, "after the exchange of two hard-linked files");
    assert_eq!(m.lookup(w.d, "f").unwrap().unwrap().ino, g);
    assert_eq!(m.lookup(w.d, "g").unwrap().unwrap().ino, w.f);
    assert_eq!(form(m, w.d, "f"), None);
    assert_eq!(form(m, w.d, "g"), None);
    // A hard-linked file against a single-name one.
    let s = m.create(w.d, "s", 0o644, 0, 0).unwrap().ino;
    m.exchange(w.d, "g", w.d, "s").unwrap();
    assert_forms(
        m,
        "after the exchange of a hard-linked and a single-name file",
    );
    assert_eq!(m.lookup(w.d, "s").unwrap().unwrap().ino, w.f);
    assert_eq!(m.lookup(w.d, "g").unwrap().unwrap().ino, s);
    assert!(form(m, w.d, "g").is_some());
    assert_eq!(form(m, w.d, "s"), None);
}

#[test]
fn recursive_size_of_a_hard_link_tree_reads_each_inode_once() {
    let w = world(1);
    let m = &w.meta;
    let others: Vec<Ino> = (0..5)
        .map(|i| {
            let ino = m.create(w.d, &format!("o{i}"), 0o644, 0, 0).unwrap().ino;
            m.set_manifest(ino, b"m", 100 + i).unwrap();
            ino
        })
        .collect();
    let single_sizes: u64 = (0..5).map(|i| 100 + i).sum();
    assert_eq!(m.recursive_size(w.d).unwrap(), (4096 + single_sizes, 6));
    assert_eq!(m.inode_reads(), 0, "single names are read from the scan");
    for i in 0..40 {
        m.link(w.f, w.d, &format!("hl{i}")).unwrap();
    }
    for &ino in &others[..2] {
        m.link(ino, w.e, &format!("x{ino}")).unwrap();
    }
    // f's 41 names count 4096 bytes each (once per name, as before); the
    // two linked others count again under `e`.
    let (bytes, files) = m.recursive_size(ROOT_INO).unwrap();
    let want_bytes = 41 * 4096 + single_sizes + 100 + 101;
    assert_eq!((bytes, files), (want_bytes, 41 + 5 + 2));
    // f once, the two linked others once each: three `0x01` reads.
    assert_eq!(m.inode_reads(), 3);
}

/// The long scenario: 1 → 2 → 3 → 2 → 1 → 0, a manifest commit, a chmod
/// and an xattr at three names, a rename and an exchange, each step checked
/// on the node and on a replica replaying the log one record batch at a
/// time; the two end with the same dentries.
#[test]
fn every_transition_keeps_each_dentry_in_the_right_form_locally_and_on_replay() {
    let a = Meta::open_in_memory().unwrap();
    let b = Meta::open_in_memory().unwrap();
    let mut seen = 0u64;
    let mut step = |label: &str| {
        let rows = a.peek_journal_after(seen).unwrap();
        seen = rows.last().map_or(seen, |(seq, _)| *seq);
        let records: Vec<LogRecord> = rows.into_iter().map(|(_, r)| r).collect();
        b.apply_records(&records).unwrap();
        assert_forms(&a, &format!("{label} (local)"));
        assert_forms(&b, &format!("{label} (replay)"));
        let (da, db) = (dump(&a), dump(&b));
        let dentries = |dump: &BTreeMap<Vec<u8>, Vec<u8>>| -> BTreeMap<Vec<u8>, bool> {
            dump.iter()
                .filter(|(k, _)| k.first() == Some(&0x02))
                .map(|(k, v)| (k.clone(), DentryRecord::decode(v).unwrap().is_remote()))
                .collect()
        };
        assert_eq!(dentries(&da), dentries(&db), "{label}: the forms differ");
        let remote_bytes = |dump: &BTreeMap<Vec<u8>, Vec<u8>>| -> BTreeSet<(Vec<u8>, Vec<u8>)> {
            dump.iter()
                .filter(|(k, v)| k.first() == Some(&0x02) && v.len() == 9)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };
        assert_eq!(
            remote_bytes(&da),
            remote_bytes(&db),
            "{label}: remote bytes"
        );
    };

    let d = a.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap().ino;
    let e = a.mkdir(ROOT_INO, "e", 0o755, 0, 0).unwrap().ino;
    let f = a.create(d, "f", 0o644, 0, 0).unwrap().ino;
    a.set_manifest(f, b"m1", 100).unwrap();
    step("one name");
    a.link(f, e, "l2").unwrap();
    step("1 -> 2");
    a.link(f, e, "l3").unwrap();
    step("2 -> 3");
    a.set_manifest(f, b"m2", 250).unwrap();
    step("a manifest commit at three names");
    a.setattr(f, Some(0o600), Some(7), Some(8), Some(10), None, Some(5))
        .unwrap();
    step("a setattr at three names");
    a.set_xattr(f, "user.k", b"v", constellation_meta::SetXattrMode::Set)
        .unwrap();
    a.remove_xattr(f, "user.k").unwrap();
    step("an xattr at three names");
    a.rename(e, "l3", d, "l3-moved").unwrap();
    step("a rename of one of three names");
    a.unlink(d, "l3-moved").unwrap();
    step("3 -> 2");
    let g = a.create(d, "g", 0o644, 0, 0).unwrap().ino;
    a.link(g, e, "gl").unwrap();
    step("a second hard-linked file");
    a.exchange(d, "f", d, "g").unwrap();
    step("an exchange of two hard-linked files");
    a.create(d, "h", 0o644, 0, 0).unwrap();
    a.rename(d, "h", e, "l2").unwrap(); // over one of f's two names
    step("a rename over a hard-linked target");
    assert_eq!(a.getattr(f).unwrap().unwrap().nlink, 1);
    a.unlink(e, "gl").unwrap();
    step("2 -> 1");
    a.unlink(d, "f").unwrap();
    a.unlink(d, "g").unwrap();
    step("1 -> 0");
}

/// Replicas that apply the same records reach the same bytes however the
/// operations were done: a node that built a file's names one `link` at a
/// time and a replica that got the whole log hold the same `0x02` values.
#[test]
fn replicas_converge_on_the_dentry_bytes() {
    let a = world(8);
    a.meta.unlink(a.e, "l3").unwrap();
    a.meta.unlink(a.e, "l5").unwrap();
    a.meta.rename(a.e, "l1", a.d, "moved").unwrap();
    let b = Meta::open_in_memory().unwrap();
    let rows = a.meta.peek_journal_after(0).unwrap();
    let records: Vec<LogRecord> = rows.into_iter().map(|(_, r)| r).collect();
    b.apply_records(&records).unwrap();
    assert_forms(&a.meta, "node");
    assert_forms(&b, "replica");
    let dentries = |m: &Meta| -> Vec<(Vec<u8>, Vec<u8>)> {
        dump(m)
            .into_iter()
            .filter(|(k, _)| k.first() == Some(&0x02) || k.first() == Some(&0x04))
            .collect()
    };
    let remote_only = |m: &Meta| -> Vec<(Vec<u8>, Vec<u8>)> {
        dentries(m)
            .into_iter()
            .filter(|(k, v)| k.first() == Some(&0x04) || v.len() == 9)
            .collect()
    };
    assert_eq!(remote_only(&a.meta), remote_only(&b));
}
