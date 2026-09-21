//! The §P6 codec's properties, asserted against a real tree.
//!
//! The unit tests beside `keys.rs` and `record.rs` check the encoding in
//! isolation. These check the three claims that are only meaningful once
//! keys are *in* a tree, and that later steps depend on by name:
//!
//! - **encoded byte order is the filesystem's order.** The tree sorts
//!   bytes and nothing else, so `readdir` returning names in order,
//!   `listxattr` being a scan, and a readdir cursor being a key are all
//!   consequences of the encoding rather than of code anyone can fix
//!   later.
//! - **a range scan is bounded.** One directory's dentries are one
//!   contiguous range and a scan of it cannot run into the next
//!   directory — the property behind §14.2's one-pack-per-directory
//!   result and S4's measurement of it.
//! - **the §P7 aggregate counts each file once.** §P6 stores a file's
//!   attrs twice by design; the projection reads only the authoritative
//!   copy. Asserted against a model total computed independently of the
//!   tree, because an aggregate that is wrong the same way on every
//!   replica is attested by the root hash and therefore invisible.
//!
//! Plus §12's mechanical *no mutable field is in the tree* test, which
//! is the guard on §P5's retracted secondary indexes and on atime.

use std::collections::BTreeMap;

use constellation_mtree::keys::{self, Field, Key, KeyError, Subsystem, RESERVED_RANGES};
use constellation_mtree::record::{
    self, Attrs, BlobHash, DentryRecord, InodeRecord, Kind, Payload, RDENTRY_VALUE, VALUE_SPILL,
    XATTR_INLINE,
};
use constellation_mtree::{Agg, MemoryNodeStore, Tree};
use rand::prelude::*;
use rand::rngs::SmallRng;

/// `alloc_ino` builds inos as `(node_prefix << 40) | counter`
/// (`meta::sqlite::INO_PREFIX_SHIFT`).
const INO_PREFIX_SHIFT: u32 = 40;

fn ino_of(node: u64, counter: u64) -> u64 {
    (node << INO_PREFIX_SHIFT) | counter
}

fn blob_hash(bytes: &[u8]) -> BlobHash {
    BlobHash(*blake3::hash(bytes).as_bytes())
}

fn attrs(kind: Kind, size: u64, mtime_ns: i64) -> Attrs {
    Attrs {
        kind,
        mode: 0o644,
        uid: 1000,
        gid: 1000,
        nlink: 1,
        size,
        mtime_ns,
        ctime_ns: mtime_ns,
        rdev: 0,
    }
}

fn tree() -> Tree<MemoryNodeStore> {
    Tree::with_config(MemoryNodeStore::new(), record::config()).expect("the §P6 config is valid")
}

/// A small filesystem, as the model the tree is checked against: inos in
/// several allocation-prefix buckets (so the ino ordering cases are
/// exercised by the real key set, not only by the unit tests), a few
/// hostile names, hard links, and xattr sets on both sides of the inline
/// budget.
type XattrSet = Vec<(Vec<u8>, Vec<u8>)>;

struct Model {
    /// ino -> (attrs, inline xattr set)
    inodes: BTreeMap<u64, (Attrs, XattrSet)>,
    /// (parent, name) -> ino
    dentries: BTreeMap<(u64, Vec<u8>), u64>,
}

impl Model {
    fn build(seed: u64) -> Model {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut inodes = BTreeMap::new();
        let mut dentries = BTreeMap::new();
        let names: [&[u8]; 5] = [b"plain", b"with\x00nul", b"with/slash", b"\xff\xfe", b"z"];

        inodes.insert(1, (attrs(Kind::Dir, 0, 1), Vec::new()));
        for dir in 0..8u64 {
            // Directories from one allocation prefix, their children
            // from another, so a directory's dentries and its children's
            // inode records are deliberately far apart in the keyspace.
            let dir_ino = ino_of(1, 100 + dir);
            inodes.insert(dir_ino, (attrs(Kind::Dir, 0, 2 + dir as i64), Vec::new()));
            dentries.insert((1, format!("d{dir}").into_bytes()), dir_ino);
            for child in 0..12u64 {
                let ino = ino_of(2 + child % 3, dir * 1000 + child);
                let size = rng.random_range(0..1_000_000u64);
                let mtime = rng.random_range(1..2_000_000_000i64);
                let kind = match child % 4 {
                    0 => Kind::Symlink,
                    1 => Kind::Dir,
                    _ => Kind::File,
                };
                let xattrs = if child % 5 == 0 {
                    vec![(b"security.selinux".to_vec(), b"unconfined_u".to_vec())]
                } else {
                    Vec::new()
                };
                inodes.insert(ino, (attrs(kind, size, mtime), xattrs));
                let name = if child < names.len() as u64 {
                    names[child as usize].to_vec()
                } else {
                    format!("f{child}").into_bytes()
                };
                dentries.insert((dir_ino, name), ino);
                // Every fourth file gets a second link from the root, so
                // the dentry copy exists more than once for one inode —
                // which is the case a double-counting projection gets
                // wrong.
                if kind == Kind::File && child % 4 == 2 {
                    dentries.insert((1, format!("link-{dir}-{child}").into_bytes()), ino);
                }
            }
        }
        Model { inodes, dentries }
    }

    /// Every key the §P6 codec writes for this filesystem.
    fn keys(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for (ino, (attrs, xattrs)) in &self.inodes {
            let plan = record::plan_inode(*attrs, None, None, xattrs, blob_hash);
            out.push((keys::inode(*ino), plan.record.encode()));
            if plan.xattrs == record::XattrPlacement::Spilled {
                for (name, value) in xattrs {
                    let (payload, _) = record::place_value(value.clone(), blob_hash);
                    out.push((keys::xattr(*ino, name), payload.encode()));
                }
            }
        }
        for ((parent, name), ino) in &self.dentries {
            let (attrs, _) = &self.inodes[ino];
            out.push((
                keys::dentry(*parent, name),
                DentryRecord::new(*ino, *attrs).encode(),
            ));
            out.push((keys::rdentry(*ino, *parent, name), RDENTRY_VALUE.to_vec()));
        }
        out.sort();
        out.dedup_by(|a, b| a.0 == b.0);
        out
    }

    /// The answer `statfs`/`du` must give, computed without the tree:
    /// reachable file bytes and file count, one inode counted once
    /// however many names point at it.
    fn total(&self) -> Agg {
        let mut agg = Agg::EMPTY;
        for (attrs, _) in self.inodes.values() {
            if attrs.kind.holds_data() {
                agg.bytes += attrs.size;
                agg.files += 1;
            }
            agg.max_mtime = agg.max_mtime.max(attrs.mtime_ns);
        }
        agg
    }
}

#[test]
fn the_aggregate_counts_each_file_once_and_ignores_the_dentry_copy() {
    let model = Model::build(0xC0FFEE);
    let tree = tree();
    let entries = model.keys();
    let root = tree.build(entries.clone()).unwrap();

    let mut expected = model.total();
    expected.keys = entries.len() as u64;
    assert_eq!(
        tree.aggregate(&root).unwrap(),
        expected,
        "the §P7 aggregate must equal the independently computed model total"
    );

    // The guard that makes the above non-vacuous: the same model has
    // 2× as many dentry attr copies as inode records for the hard-linked
    // files, so a projection that counted `0x02` would inflate the
    // totals. Build a tree from the inode records alone and assert the
    // byte and file totals are identical — i.e. the dentries contributed
    // nothing but keys.
    let inodes_only: Vec<(Vec<u8>, Vec<u8>)> = entries
        .iter()
        .filter(|(key, _)| matches!(Key::parse(key), Ok(Key::Inode { .. })))
        .cloned()
        .collect();
    let inode_root = tree.build(inodes_only.clone()).unwrap();
    let inode_agg = tree.aggregate(&inode_root).unwrap();
    assert_eq!(inode_agg.bytes, expected.bytes);
    assert_eq!(inode_agg.files, expected.files);
    assert_eq!(inode_agg.max_mtime, expected.max_mtime);
    assert_eq!(inode_agg.keys, inodes_only.len() as u64);

    // And the copies really are there to be miscounted: more dentry
    // records than inode records, over the same inodes.
    let dentry_count = entries
        .iter()
        .filter(|(key, _)| matches!(Key::parse(key), Ok(Key::Dentry { .. })))
        .count();
    assert!(dentry_count > inodes_only.len());
    let copied: u64 = entries
        .iter()
        .filter_map(|(key, value)| match Key::parse(key) {
            Ok(Key::Dentry { .. }) => Some(DentryRecord::decode(value).unwrap().attrs),
            _ => None,
        })
        .filter(|attrs| attrs.kind.holds_data())
        .map(|attrs| attrs.size)
        .sum();
    assert!(copied > 0, "the attr copy must carry sizes to be a hazard");

    // An incremental commit keeps the aggregate exact: growing a file
    // rewrites its `0x01` record and its dentry copies, and only the
    // first may move the total.
    let (ino, (before, xattrs)) = model
        .inodes
        .iter()
        .find(|(_, (a, _))| a.kind.holds_data())
        .unwrap();
    let grown = Attrs {
        size: before.size + 4096,
        ..*before
    };
    let mut edits = vec![(
        keys::inode(*ino),
        Some(
            record::plan_inode(grown, None, None, xattrs, blob_hash)
                .record
                .encode(),
        ),
    )];
    for ((parent, name), target) in &model.dentries {
        if target == ino {
            edits.push((
                keys::dentry(*parent, name),
                Some(DentryRecord::new(*ino, grown).encode()),
            ));
        }
    }
    edits.sort();
    let next = tree.apply(&root, &edits).unwrap();
    assert_eq!(
        tree.aggregate(&next).unwrap().bytes,
        expected.bytes + 4096,
        "one file grew by 4 KiB, however many dentries copy its attrs"
    );
}

#[test]
fn a_directory_scan_is_one_contiguous_range_and_stops_at_the_next() {
    let model = Model::build(7);
    let tree = tree();
    let root = tree.build(model.keys()).unwrap();

    for dir in 0..8u64 {
        let dir_ino = ino_of(1, 100 + dir);
        let range = keys::dentries_of(dir_ino);
        let scanned = tree
            .range(&root, range.start(), range.prefix(), usize::MAX)
            .unwrap();
        let expected: Vec<Vec<u8>> = model
            .dentries
            .keys()
            .filter(|(parent, _)| *parent == dir_ino)
            .map(|(_, name)| name.clone())
            .collect();
        assert!(!expected.is_empty());
        assert_eq!(scanned.len(), expected.len(), "directory {dir_ino}");
        for ((key, value), name) in scanned.iter().zip(&expected) {
            match Key::parse(key).unwrap() {
                Key::Dentry {
                    parent_ino,
                    name: n,
                } => {
                    assert_eq!(parent_ino, dir_ino, "the scan left the directory");
                    assert_eq!(n, name.as_slice(), "scan order must be name order");
                }
                other => panic!("the scan left the 0x02 range: {other:?}"),
            }
            // READDIRPLUS reads attrs straight out of the scan.
            let record = DentryRecord::decode(value).unwrap();
            assert_eq!(record.attrs, model.inodes[&record.ino].0);
        }
        // The bound is exclusive and lands exactly on the next
        // directory's first possible key, so a cursor walking past the
        // end of this directory cannot report the next one's entries.
        assert_eq!(range.end(), keys::dentries_of(dir_ino + 1).start());
        assert!(scanned.iter().all(|(key, _)| range.contains(key)));
    }

    // The same for an inode's xattrs, and for its names in the reverse
    // index.
    let spilled: Vec<(Vec<u8>, Vec<u8>)> = (0..60)
        .map(|i| (format!("user.k{i:02}").into_bytes(), vec![b'v'; 8]))
        .collect();
    assert_eq!(
        record::place_xattrs(&spilled),
        record::XattrPlacement::Spilled
    );
    let victim = ino_of(9, 1);
    let neighbour = victim + 1;
    let mut edits: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
    for ino in [victim, neighbour] {
        for (name, value) in &spilled {
            let (payload, _) = record::place_value(value.clone(), blob_hash);
            edits.push((keys::xattr(ino, name), Some(payload.encode())));
        }
        edits.push((keys::rdentry(ino, 1, b"n"), Some(RDENTRY_VALUE.to_vec())));
    }
    edits.sort();
    let root = tree.apply(&root, &edits).unwrap();

    let range = keys::xattrs_of(victim);
    let listed = tree
        .range(&root, range.start(), range.prefix(), usize::MAX)
        .unwrap();
    assert_eq!(listed.len(), spilled.len(), "listxattr is one range scan");
    for (key, _) in &listed {
        match Key::parse(key).unwrap() {
            Key::Xattr { ino, .. } => assert_eq!(ino, victim),
            other => panic!("{other:?}"),
        }
    }
    let names = keys::names_of(victim);
    let found = tree
        .range(&root, names.start(), names.prefix(), usize::MAX)
        .unwrap();
    assert_eq!(found.len(), 1);
    assert!(names.contains(&found[0].0));
    assert!(!names.contains(&keys::rdentry(neighbour, 1, b"n")));
}

/// Byte order is numeric and lexical order for every field of every
/// range, including a hostile name and the ino boundary cases
/// `alloc_ino` actually produces.
#[test]
fn encoded_order_is_the_order_the_filesystem_means() {
    let mut rng = SmallRng::seed_from_u64(99);
    let inos = {
        let mut inos: Vec<u64> = vec![
            0,
            1,
            (1 << INO_PREFIX_SHIFT) - 1,
            1 << INO_PREFIX_SHIFT,
            (1 << INO_PREFIX_SHIFT) + 1,
            ino_of(0xff_ffff, (1 << INO_PREFIX_SHIFT) - 1),
            u64::MAX - 1,
            u64::MAX,
        ];
        inos.extend((0..64).map(|_| ino_of(rng.random_range(0..0xff_ffff), rng.random())));
        inos.sort_unstable();
        inos.dedup();
        inos
    };
    let names: Vec<Vec<u8>> = {
        let mut names: Vec<Vec<u8>> = vec![
            vec![0x00],
            vec![0x00, 0x00],
            b"a".to_vec(),
            b"a\x00".to_vec(),
            b"a/b".to_vec(),
            b"a\xff".to_vec(),
            vec![0x2f],
            vec![0xff; keys::NAME_MAX],
        ];
        names.extend((0..16).map(|_| {
            let len = rng.random_range(1..12);
            (0..len).map(|_| rng.random()).collect()
        }));
        names.sort();
        names.dedup();
        names
    };

    // Each field in turn: vary it, hold the rest, and assert the encoded
    // order matches the field's own order.
    for pair in inos.windows(2) {
        let (lo, hi) = (pair[0], pair[1]);
        assert!(keys::inode(lo) < keys::inode(hi));
        assert!(keys::dentry(lo, b"n") < keys::dentry(hi, b"n"));
        assert!(keys::xattr(lo, b"user.a") < keys::xattr(hi, b"user.a"));
        assert!(keys::rdentry(lo, 5, b"n") < keys::rdentry(hi, 5, b"n"));
        assert!(keys::rdentry(5, lo, b"n") < keys::rdentry(5, hi, b"n"));
        // The higher-order field dominates every lower-order one: no
        // parent, name or id can lift a key into the next inode's range.
        assert!(keys::rdentry(lo, u64::MAX, &[0xff; 64]) < keys::rdentry(hi, 0, &[0x00]));
        assert!(keys::dentry(lo, &[0xff; 64]) < keys::dentry(hi, &[0x00]));
    }
    for pair in names.windows(2) {
        let (lo, hi) = (&pair[0], &pair[1]);
        assert!(lo < hi, "test data must ascend");
        assert!(keys::dentry(3, lo) < keys::dentry(3, hi), "{lo:?} {hi:?}");
        assert!(keys::xattr(3, lo) < keys::xattr(3, hi), "{lo:?} {hi:?}");
        assert!(keys::rdentry(3, 4, lo) < keys::rdentry(3, 4, hi));
    }
    for pair in keys::SUBSYSTEMS.windows(2) {
        assert!(keys::subsystem(pair[0], &[0xff; 32]) < keys::subsystem(pair[1], b""));
    }

    // And the whole key set sorts the way a tree cursor will walk it: by
    // range, then by ino, then by name.
    let mut all: Vec<Vec<u8>> = Vec::new();
    for ino in inos.iter().take(8) {
        all.push(keys::inode(*ino));
        for name in names.iter().take(4) {
            all.push(keys::dentry(*ino, name));
            all.push(keys::xattr(*ino, name));
            all.push(keys::rdentry(*ino, *ino, name));
        }
    }
    let mut sorted = all.clone();
    sorted.sort();
    all.sort_by_key(|key| match Key::parse(key).unwrap() {
        Key::Inode { ino } => (0u8, ino, 0u64, Vec::new()),
        Key::Dentry { parent_ino, name } => (1, parent_ino, 0, name.to_vec()),
        Key::Xattr { ino, name } => (2, ino, 0, name.to_vec()),
        Key::RDentry {
            ino,
            parent_ino,
            name,
        } => (3, ino, parent_ino, name.to_vec()),
        Key::Subsystem { .. } => unreachable!(),
    });
    assert_eq!(all, sorted, "byte order must be the semantic order");
}

/// §12's mechanical test. Two halves, because there are two ways a
/// mutable field gets into a key: through a key builder, and through a
/// record the builder is handed.
#[test]
fn no_mutable_field_is_in_the_tree() {
    // 1. Every field the codec admits to keying is immutable for the
    //    lifetime of its entity, and the classification partitions
    //    `Field::ALL` so a new field must pick a side.
    let samples = [
        Key::Inode { ino: 1 },
        Key::Dentry {
            parent_ino: 1,
            name: b"n",
        },
        Key::Xattr {
            ino: 1,
            name: b"user.a",
        },
        Key::RDentry {
            ino: 1,
            parent_ino: 2,
            name: b"n",
        },
        Key::Subsystem {
            subsystem: Subsystem::Snapshot,
            id: b"s",
        },
    ];
    assert_eq!(samples.len(), keys::RANGES.len());
    for key in samples {
        for field in key.fields() {
            assert!(
                field.immutable(),
                "0x{:02x} keys a mutable field: {field:?}",
                key.range()
            );
        }
    }
    for field in Field::ALL {
        assert_eq!(
            field.immutable(),
            samples.iter().any(|key| key.fields().contains(&field)),
            "{field:?} must be keyed iff it is immutable"
        );
    }
    assert!(!Field::Atime.in_tree(), "§P6 excludes atime from the tree");
    assert!(Field::ALL
        .iter()
        .filter(|f| !f.in_tree())
        .eq([Field::Atime].iter()));

    // 2. The bytes. Give one entity a record in which every mutable
    //    field holds a recognisable pattern, then assert that no key
    //    built for it contains any of those patterns, and that changing
    //    all of them moves no key. This is the half that catches a new
    //    `to_be_bytes` call in a key builder, which the table above
    //    could be edited to hide.
    let ino = ino_of(3, 42);
    let parent = ino_of(1, 7);
    let name: &[u8] = b"victim";
    let loud = Attrs {
        kind: Kind::File,
        mode: 0xdead_beef,
        uid: 0xcafe_f00d,
        gid: 0xfeed_face,
        nlink: 0x0bad_cafe,
        size: 0x1122_3344_5566_7788,
        mtime_ns: 0x0102_0304_0506_0708,
        ctime_ns: 0x1918_1716_1514_1312,
        rdev: 0x2122_2324_2526_2728,
    };
    let quiet = Attrs {
        kind: Kind::File,
        mode: 0o600,
        uid: 0,
        gid: 0,
        nlink: 9,
        size: 0,
        mtime_ns: 1,
        ctime_ns: 2,
        rdev: 0,
    };
    let patterns: Vec<Vec<u8>> = vec![
        loud.mode.to_be_bytes().to_vec(),
        loud.mode.to_le_bytes().to_vec(),
        loud.uid.to_be_bytes().to_vec(),
        loud.gid.to_be_bytes().to_vec(),
        loud.nlink.to_be_bytes().to_vec(),
        loud.size.to_be_bytes().to_vec(),
        loud.size.to_le_bytes().to_vec(),
        loud.mtime_ns.to_be_bytes().to_vec(),
        loud.mtime_ns.to_le_bytes().to_vec(),
        loud.ctime_ns.to_be_bytes().to_vec(),
        loud.rdev.to_be_bytes().to_vec(),
    ];
    let xattrs = vec![(b"user.a".to_vec(), vec![0x5a; 16])];
    let manifest = vec![0x77; 64];

    // Every key the codec writes for this entity, under both records.
    let keys_for = |attrs: Attrs| -> Vec<Vec<u8>> {
        let plan = record::plan_inode(attrs, Some(manifest.clone()), None, &xattrs, blob_hash);
        assert_eq!(plan.xattrs, record::XattrPlacement::Inline);
        vec![
            keys::inode(ino),
            keys::dentry(parent, name),
            keys::rdentry(ino, parent, name),
            keys::xattr(ino, &xattrs[0].0),
        ]
    };
    assert_eq!(keys_for(loud), keys_for(quiet), "a key moved with a value");
    for key in keys_for(loud) {
        for pattern in &patterns {
            assert!(
                !key.windows(pattern.len()).any(|w| w == &pattern[..]),
                "key {key:02x?} contains a mutable field's bytes {pattern:02x?}"
            );
        }
        // The value carries them; only the key may not.
        assert!(Key::parse(&key).is_ok());
    }
    let value = record::plan_inode(loud, Some(manifest), None, &xattrs, blob_hash)
        .record
        .encode();
    assert!(
        patterns
            .iter()
            .any(|pattern| value.windows(pattern.len()).any(|w| w == &pattern[..])),
        "the mutable fields must really be in the record, or this test is vacuous"
    );

    // 3. atime has no representation at all: there is no field to set,
    //    and the record's length is pinned, so adding one fails loudly.
    assert_eq!(record::ATTRS_LEN, 49);
    assert_eq!(
        InodeRecord::new(quiet).encode().len(),
        record::ATTRS_LEN + 1
    );
}

#[test]
fn nothing_encodes_into_the_reserved_span() {
    let mut rng = SmallRng::seed_from_u64(5);
    let mut produced: Vec<Vec<u8>> = Vec::new();
    for _ in 0..2_000 {
        let ino: u64 = rng.random();
        let parent: u64 = rng.random();
        let len = rng.random_range(1..40);
        let name: Vec<u8> = (0..len).map(|_| rng.random()).collect();
        produced.push(keys::inode(ino));
        produced.push(keys::dentry(parent, &name));
        produced.push(keys::xattr(ino, &name));
        produced.push(keys::rdentry(ino, parent, &name));
        produced.push(keys::subsystem(
            keys::SUBSYSTEMS[rng.random_range(0..keys::SUBSYSTEMS.len())],
            &name,
        ));
    }
    for range in [
        keys::whole_range(keys::RANGE_INODE),
        keys::dentries_of(rng.random()),
        keys::xattrs_of(rng.random()),
        keys::names_of(rng.random()),
        keys::records_of(Subsystem::Hold),
    ] {
        produced.push(range.start().to_vec());
        produced.push(range.end().to_vec());
    }
    for key in &produced {
        if let Some(range) = key.first() {
            assert!(
                !RESERVED_RANGES.contains(range),
                "0x{range:02x} is reserved (§P5 retracted the indexes that lived there)"
            );
        }
    }
    // The reserved span is refused on the way in as well, so a key from
    // a future writer that claimed one is a decode error rather than a
    // silently ignored range.
    for range in RESERVED_RANGES {
        assert_eq!(
            Key::parse(&[range, 0, 1]),
            Err(KeyError::ReservedRange(range))
        );
    }
}

/// The inline/spill boundaries, crossed in both directions with the
/// tree actually holding the results — the unit tests check the
/// decision, this checks that both sides of it are storable and
/// readable, and that a spilled value leaves the `0x01` record alone.
#[test]
fn the_inline_and_spill_boundaries_round_trip_through_a_tree() {
    let tree = tree();
    let ino = ino_of(4, 11);
    let base = attrs(Kind::File, 10, 20);

    let inline_set = vec![(b"user.a".to_vec(), vec![0u8; XATTR_INLINE - 2 - 4 - 6])];
    assert_eq!(record::xattr_section_len(&inline_set), XATTR_INLINE);
    let mut spilled_set = inline_set.clone();
    spilled_set[0].1.push(0);

    let inline_plan = record::plan_inode(base, None, None, &inline_set, blob_hash);
    assert_eq!(inline_plan.xattrs, record::XattrPlacement::Inline);
    let root = tree
        .build(vec![(keys::inode(ino), inline_plan.record.encode())])
        .unwrap();
    let stored =
        InodeRecord::decode(&tree.get(&root, &keys::inode(ino)).unwrap().unwrap()).unwrap();
    assert_eq!(stored.xattrs, inline_set);
    assert!(tree
        .range(
            &root,
            keys::xattrs_of(ino).start(),
            keys::xattrs_of(ino).prefix(),
            16
        )
        .unwrap()
        .is_empty());

    // One byte more and the whole set moves to 0x03 keys; the record
    // keeps none of it, so `listxattr` is a scan and never both.
    let spilled_plan = record::plan_inode(base, None, None, &spilled_set, blob_hash);
    assert_eq!(spilled_plan.xattrs, record::XattrPlacement::Spilled);
    assert!(spilled_plan.record.xattrs.is_empty());
    let (payload, blob) = record::place_value(spilled_set[0].1.clone(), blob_hash);
    assert!(blob.is_none(), "still under VALUE_SPILL");
    let root = tree
        .apply(
            &root,
            &[
                (keys::inode(ino), Some(spilled_plan.record.encode())),
                (keys::xattr(ino, &spilled_set[0].0), Some(payload.encode())),
            ]
            .into_iter()
            .collect::<Vec<_>>(),
        )
        .unwrap();
    let listed = tree
        .range(
            &root,
            keys::xattrs_of(ino).start(),
            keys::xattrs_of(ino).prefix(),
            16,
        )
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(
        Payload::decode(&listed[0].1).unwrap(),
        Payload::Inline(spilled_set[0].1.clone())
    );

    // A 64 KiB xattr — Linux's ceiling — spills to a blob, and the key
    // still holds a bounded value.
    let huge = vec![0x5a; 64 * 1024];
    let (payload, blob) = record::place_value(huge.clone(), blob_hash);
    assert_eq!(blob.as_deref(), Some(huge.as_slice()));
    let encoded = payload.encode();
    assert!(encoded.len() <= VALUE_SPILL);
    let root = tree
        .apply(&root, &[(keys::xattr(ino, b"user.big"), Some(encoded))])
        .unwrap();
    match Payload::decode(
        &tree
            .get(&root, &keys::xattr(ino, b"user.big"))
            .unwrap()
            .unwrap(),
    )
    .unwrap()
    {
        Payload::Spilled(hash) => assert_eq!(hash, blob_hash(&huge)),
        other => panic!("expected a spilled value, got {other:?}"),
    }
    // Every value in the tree respects §P1's bound.
    let mut cursor = tree.cursor(&root).unwrap();
    while let Some((_, value)) = cursor.entry().unwrap() {
        assert!(value.len() <= VALUE_SPILL);
        cursor.next().unwrap();
    }
}
