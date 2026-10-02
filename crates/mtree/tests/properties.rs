//! The properties this crate exists to guarantee (plan 28 §P1, §14.6).
//!
//! These are the real specification. Every one of them is a statement
//! about canonicality — that a tree's bytes are a function of its key
//! set and nothing else — and every later step of plan 28 depends on
//! one of them by name: the commit chain needs delete-then-reinsert to
//! return the original root, the change journal needs diff to cost
//! O(difference), optimistic commit needs merges to be deterministic
//! and conflicts to be exact, and ADR-5's no-migration promise needs
//! the node encoding pinned.
//!
//! They deliberately use only the public API, and they run against key
//! sets small enough for a debug build: §14 measured these same
//! properties at 100k–35.8M keys, and what is being checked here is the
//! logic, not the scale. Where a property is about *cost* rather than
//! about equality it is asserted against the store's node-read counter,
//! which is the only honest way to state it.

use std::collections::BTreeMap;

use constellation_mtree::{
    Agg, ChangeKind, Config, Edit, MemoryNodeStore, Merged, MtreeError, NodeHash, NodeRef, Tree,
};
use rand::prelude::*;
use rand::rngs::SmallRng;

/// An inode-shaped key: a range byte then a big-endian id, so byte
/// order is numeric order the way the §P6 codec will guarantee.
fn key(i: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.push(0x01);
    out.extend_from_slice(&i.to_be_bytes());
    out
}

/// A value shaped like the §P6 inode record the aggregate projection
/// below reads: 8 bytes of size, 8 bytes of mtime, then filler.
fn value(i: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    out.extend_from_slice(&(i * 10).to_be_bytes());
    out.extend_from_slice(&(1_700_000_000_000_000_000i64 + i as i64).to_be_bytes());
    out.extend_from_slice(format!("inode-{i}").as_bytes());
    out
}

fn pair(i: u64) -> (Vec<u8>, Vec<u8>) {
    (key(i), value(i))
}

fn upsert(i: u64) -> Edit {
    (key(i), Some(value(i)))
}

fn delete(i: u64) -> Edit {
    (key(i), None)
}

/// A stand-in for the §P6 projection: every key in the `0x01` range is
/// an authoritative inode record and contributes its bytes, one file,
/// and its mtime.
fn inode_agg(k: &[u8], v: &[u8]) -> Agg {
    if k.first() != Some(&0x01) || v.len() < 16 {
        return Agg::EMPTY;
    }
    Agg {
        bytes: u64::from_be_bytes(v[0..8].try_into().unwrap()),
        files: 1,
        keys: 0,
        max_mtime: i64::from_be_bytes(v[8..16].try_into().unwrap()),
    }
}

fn tree() -> Tree<MemoryNodeStore> {
    Tree::with_config(
        MemoryNodeStore::new(),
        Config::default().with_leaf_agg(inode_agg),
    )
    .expect("default config is valid")
}

fn tree_with(config: Config) -> Tree<MemoryNodeStore> {
    Tree::with_config(MemoryNodeStore::new(), config).expect("valid config")
}

/// Insert `keys` in the order given, in batches of `batch`, and return
/// the resulting root.
fn apply_in_order(
    tree: &Tree<MemoryNodeStore>,
    keys: &[u64],
    batch: usize,
) -> Result<NodeHash, MtreeError> {
    let mut root = tree.empty()?;
    for chunk in keys.chunks(batch.max(1)) {
        let mut edits: Vec<Edit> = chunk.iter().copied().map(upsert).collect();
        edits.sort();
        root = tree.apply(&root, &edits)?;
    }
    Ok(root)
}

// -------------------------------------------------------------------
// 1. Order independence
// -------------------------------------------------------------------

/// The property the whole design rests on (§P1): shape is a function of
/// the key set, never of insertion order. §14.6 got 50/50 at 100k keys;
/// this is the same 50 orders at a size a debug build can afford.
#[test]
fn fifty_insertion_orders_yield_one_root_hash() {
    let tree = tree();
    let n = 3_000u64;
    let bulk = tree.build((0..n).map(pair)).unwrap();
    let mut keys: Vec<u64> = (0..n).collect();
    let mut rng = SmallRng::seed_from_u64(7);
    for round in 0..50 {
        keys.shuffle(&mut rng);
        // Vary the batch size too: a batch boundary is another place
        // history could leak into the shape if the algorithm were not
        // context-free.
        let batch = 1 + (round * 37) % 400;
        assert_eq!(
            apply_in_order(&tree, &keys, batch).unwrap(),
            bulk,
            "round {round} (batch {batch}) disagreed with the bulk build"
        );
    }
}

/// Keyed trees are canonical too: §P13 changes every hash and nothing
/// else. Order independence has to survive the change, because E2E
/// mode is not a separate algorithm.
#[test]
fn a_keyed_tree_is_canonical_and_differs_in_every_hash() {
    let plain = tree();
    let keyed = tree_with(Config::keyed([0x5a; 32]).with_leaf_agg(inode_agg));
    let n = 1_500u64;
    let plain_bulk = plain.build((0..n).map(pair)).unwrap();
    let keyed_bulk = keyed.build((0..n).map(pair)).unwrap();
    assert_ne!(plain_bulk, keyed_bulk);

    let mut keys: Vec<u64> = (0..n).collect();
    keys.shuffle(&mut SmallRng::seed_from_u64(11));
    assert_eq!(apply_in_order(&keyed, &keys, 97).unwrap(), keyed_bulk);

    // Same contents, different addressing.
    for i in [0u64, 1, 999, 1_499] {
        assert_eq!(keyed.get(&keyed_bulk, &key(i)).unwrap(), Some(value(i)));
    }
    // A different key is a different tree.
    let other = tree_with(Config::keyed([0x5b; 32]).with_leaf_agg(inode_agg));
    assert_ne!(other.build((0..n).map(pair)).unwrap(), keyed_bulk);
}

// -------------------------------------------------------------------
// 2. Incremental equals bulk, byte for byte
// -------------------------------------------------------------------

/// A root-hash match already implies byte-identity of every reachable
/// node — that is what content addressing means — but assert the node
/// *sets* as well, so a failure says whether the trees diverged in
/// shape or only at the root.
#[test]
fn an_incrementally_built_tree_is_byte_identical_to_a_bulk_one() {
    let tree = tree();
    let mut rng = SmallRng::seed_from_u64(3);
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = (0..6_000).map(pair).collect();
    let mut root = tree
        .build(model.iter().map(|(k, v)| (k.clone(), v.clone())))
        .unwrap();

    for round in 0..12 {
        let mut edits: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        for _ in 0..300 {
            let i: u64 = rng.random_range(0..9_000);
            if rng.random_bool(0.35) {
                edits.insert(key(i), None);
            } else {
                let mut v = value(i);
                v.extend_from_slice(b"-edited");
                edits.insert(key(i), Some(v));
            }
        }
        for (k, v) in &edits {
            match v {
                Some(v) => model.insert(k.clone(), v.clone()),
                None => model.remove(k),
            };
        }
        let batch: Vec<Edit> = edits.into_iter().collect();
        root = tree.apply(&root, &batch).unwrap();

        let bulk = tree
            .build(model.iter().map(|(k, v)| (k.clone(), v.clone())))
            .unwrap();
        assert_eq!(root, bulk, "round {round}");
        assert_eq!(
            tree.reachable(&[root]).unwrap(),
            tree.reachable(&[bulk]).unwrap(),
            "round {round}: same root, different node set (impossible)"
        );
        assert_eq!(tree.aggregate(&root).unwrap().keys, model.len() as u64);
    }

    // And every key reads back.
    for (k, v) in &model {
        assert_eq!(tree.get(&root, k).unwrap().as_ref(), Some(v));
    }
}

/// The §P7 aggregates are a monoid over the key order, so the root's
/// aggregate must equal a recomputation from the model — at every
/// interior level, not just at the root.
#[test]
fn aggregates_equal_a_recomputation_from_the_model() {
    let tree = tree();
    let n = 4_000u64;
    let root = tree.build((0..n).map(pair)).unwrap();
    let expected = (0..n).fold(Agg::EMPTY, |acc, i| {
        acc.combine(inode_agg(&key(i), &value(i)))
    });
    let got = tree.aggregate(&root).unwrap();
    assert_eq!(got.bytes, expected.bytes);
    assert_eq!(got.files, n);
    assert_eq!(got.keys, n);
    assert_eq!(got.max_mtime, expected.max_mtime);

    // Deleting the newest key must lower the max mtime, which is the
    // interesting half of the monoid: it is a max, not a sum, so it is
    // the field a maintained counter column would get wrong.
    let after = tree.apply(&root, &[delete(n - 1)]).unwrap();
    assert_eq!(
        tree.aggregate(&after).unwrap().max_mtime,
        inode_agg(&key(n - 2), &value(n - 2)).max_mtime
    );
    assert_eq!(tree.aggregate(&after).unwrap().keys, n - 1);
}

// -------------------------------------------------------------------
// 3. Delete then reinsert
// -------------------------------------------------------------------

#[test]
fn delete_then_reinsert_returns_the_original_root() {
    let tree = tree();
    let n = 5_000u64;
    let root = tree.build((0..n).map(pair)).unwrap();
    let mut rng = SmallRng::seed_from_u64(11);
    let mut victims: Vec<u64> = (0..n).choose_multiple(&mut rng, 800);
    victims.sort_unstable();

    let deletes: Vec<Edit> = victims.iter().copied().map(delete).collect();
    let after = tree.apply(&root, &deletes).unwrap();
    assert_ne!(after, root);
    for i in &victims {
        assert_eq!(tree.get(&after, &key(*i)).unwrap(), None);
    }

    let inserts: Vec<Edit> = victims.iter().copied().map(upsert).collect();
    assert_eq!(tree.apply(&after, &inserts).unwrap(), root);

    // Deleting everything returns the empty root, not a degenerate
    // chain of single-child interior nodes.
    let all: Vec<Edit> = (0..n).map(delete).collect();
    assert_eq!(tree.apply(&root, &all).unwrap(), tree.empty().unwrap());
}

// -------------------------------------------------------------------
// 4. Diff cost tracks the difference, not the state
// -------------------------------------------------------------------

/// §14.6's table, reproduced as an assertion. Two independent claims:
/// per-change cost falls as the change set grows, and the cost of a
/// fixed change set does not grow with the tree.
#[test]
fn diff_cost_tracks_the_difference_and_not_the_state() {
    let tree = tree();
    let n = 20_000u64;
    let root = tree.build((0..n).map(pair)).unwrap();
    let mut rng = SmallRng::seed_from_u64(23);

    let mut per_change: Vec<(usize, f64, u64)> = Vec::new();
    for changes in [1usize, 10, 100, 1_000] {
        let mut victims: Vec<u64> = (0..n).choose_multiple(&mut rng, changes);
        victims.sort_unstable();
        let edits: Vec<Edit> = victims
            .iter()
            .map(|i| (key(*i), Some(b"changed".to_vec())))
            .collect();
        let other = tree.apply(&root, &edits).unwrap();

        tree.store().reset_counters();
        let found = tree.diff(&root, &other).unwrap();
        let reads = tree.store().reads();
        assert_eq!(found.len(), changes, "diff missed changes");
        assert!(found.iter().all(|(_, kind)| *kind == ChangeKind::Modified));
        per_change.push((changes, reads as f64 / changes as f64, reads));
    }

    // A one-key diff of a 20k-key tree must not walk it: the whole
    // tree is ~200 leaves plus interior, and a diff that read them all
    // would be O(state).
    assert!(
        per_change[0].2 < 40,
        "a one-key diff read {} nodes",
        per_change[0].2
    );
    // Per-change cost falls monotonically, as §14.6 measured.
    for window in per_change.windows(2) {
        assert!(
            window[1].1 <= window[0].1,
            "per-change cost rose from {:?} to {:?}",
            window[0],
            window[1]
        );
    }

    // The other half: hold the change set at one key and grow the tree
    // by two orders of magnitude. Cost may grow with *depth* (a diff
    // has to descend), never with size.
    let mut one_key_reads = Vec::new();
    for size in [500u64, 5_000, 50_000] {
        let big = tree_with(Config::default().with_leaf_agg(inode_agg));
        let root = big.build((0..size).map(pair)).unwrap();
        let other = big.apply(&root, &[(key(7), Some(b"x".to_vec()))]).unwrap();
        big.store().reset_counters();
        assert_eq!(big.diff(&root, &other).unwrap().len(), 1);
        one_key_reads.push(big.store().reads());
    }
    assert!(
        one_key_reads[2] < one_key_reads[0] * 4,
        "one-key diff cost grew with the tree: {one_key_reads:?}"
    );

    // Identical roots cost nothing at all.
    tree.store().reset_counters();
    assert!(tree.diff(&root, &root).unwrap().is_empty());
    assert_eq!(tree.store().reads(), 0);
}

#[test]
fn diff_names_additions_removals_and_modifications_exactly() {
    let tree = tree();
    let root = tree.build((0..4_000).map(pair)).unwrap();
    let edits: Vec<Edit> = vec![
        (key(10), Some(b"new".to_vec())),
        (key(2_000), None),
        (key(1_000_000), Some(b"added".to_vec())),
    ];
    let other = tree.apply(&root, &edits).unwrap();
    let found = tree.diff(&root, &other).unwrap();
    assert_eq!(
        found,
        vec![
            (key(10), ChangeKind::Modified),
            (key(2_000), ChangeKind::Removed),
            (key(1_000_000), ChangeKind::Added),
        ]
    );
    // And the delta round-trips: applying it to `root` reproduces
    // `other` exactly, which is what makes merge possible at all.
    let delta = tree.delta(&root, &other).unwrap();
    assert_eq!(tree.apply(&root, &delta).unwrap(), other);
}

/// `diff_each` visits exactly `diff`'s keys in order, and stopping it
/// after `n` keys yields those `n` and reports that it stopped.
#[test]
fn diff_each_is_diff_and_stops_when_asked() {
    let tree = tree();
    let root = tree.build((0..4_000).map(pair)).unwrap();
    let edits: Vec<Edit> = (0..50)
        .map(|i| (key(i * 70), Some(b"changed".to_vec())))
        .collect();
    let other = tree.apply(&root, &edits).unwrap();
    let all = tree.diff(&root, &other).unwrap();
    assert_eq!(all.len(), 50);
    let mut seen = Vec::new();
    let finished = tree
        .diff_each(&root, &other, |key, kind| {
            seen.push((key.to_vec(), kind));
            true
        })
        .unwrap();
    assert!(finished);
    assert_eq!(seen, all);
    let mut seen = Vec::new();
    let finished = tree
        .diff_each(&root, &other, |key, kind| {
            seen.push((key.to_vec(), kind));
            seen.len() < 7
        })
        .unwrap();
    assert!(!finished);
    assert_eq!(seen, all[..7]);
    assert!(tree.diff_each(&root, &root, |_, _| false).unwrap());
}

// -------------------------------------------------------------------
// 5. Disjoint merges agree
// -------------------------------------------------------------------

#[test]
fn disjoint_branches_merge_to_one_root_from_both_directions() {
    let tree = tree();
    let base = tree.build((0..5_000).map(pair)).unwrap();
    let a = tree
        .apply(
            &base,
            &(0..400)
                .map(|i| (key(i * 2), Some(b"a".to_vec())))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    let b = tree
        .apply(
            &base,
            &(0..400)
                .map(|i| (key(i * 2 + 1), Some(b"b".to_vec())))
                .collect::<Vec<_>>(),
        )
        .unwrap();

    let Merged::Root(ab) = tree.merge(&base, &a, &b).unwrap() else {
        panic!("disjoint branches conflicted");
    };
    let Merged::Root(ba) = tree.merge(&base, &b, &a).unwrap() else {
        panic!("disjoint branches conflicted");
    };
    assert_eq!(ab, ba, "merge is not commutative in its result");
    assert_eq!(tree.get(&ab, &key(0)).unwrap(), Some(b"a".to_vec()));
    assert_eq!(tree.get(&ab, &key(1)).unwrap(), Some(b"b".to_vec()));
    assert_eq!(tree.aggregate(&ab).unwrap().keys, 5_000);

    // Insertions and deletions in disjoint ranges also merge.
    let c = tree
        .apply(&base, &(0..100).map(|i| delete(i * 2)).collect::<Vec<_>>())
        .unwrap();
    let d = tree
        .apply(&base, &(10_000..10_100).map(upsert).collect::<Vec<_>>())
        .unwrap();
    let Merged::Root(cd) = tree.merge(&base, &c, &d).unwrap() else {
        panic!("disjoint branches conflicted");
    };
    let Merged::Root(dc) = tree.merge(&base, &d, &c).unwrap() else {
        panic!("disjoint branches conflicted");
    };
    assert_eq!(cd, dc);
    assert_eq!(tree.aggregate(&cd).unwrap().keys, 5_000 - 100 + 100);
}

// -------------------------------------------------------------------
// 6. Overlapping merges conflict exactly
// -------------------------------------------------------------------

/// "Exactly" in both directions: no key that both branches touched may
/// be missing, and no key only one of them touched may be present. A
/// superset would turn legal concurrency into spurious failures.
#[test]
fn overlapping_branches_report_exactly_the_overlapping_keys() {
    let tree = tree();
    let base = tree.build((0..4_000).map(pair)).unwrap();

    let a_keys: Vec<u64> = (0..600).collect();
    let b_keys: Vec<u64> = (400..1_000).collect();
    let expected: Vec<Vec<u8>> = (400..600).map(key).collect();

    let a = tree
        .apply(
            &base,
            &a_keys
                .iter()
                .map(|i| (key(*i), Some(b"a".to_vec())))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    let b = tree
        .apply(
            &base,
            &b_keys
                .iter()
                .map(|i| (key(*i), Some(b"b".to_vec())))
                .collect::<Vec<_>>(),
        )
        .unwrap();

    let Merged::Conflicts(mut conflicts) = tree.merge(&base, &a, &b).unwrap() else {
        panic!("overlapping branches merged");
    };
    conflicts.sort();
    assert_eq!(conflicts, expected);

    // Symmetric.
    let Merged::Conflicts(mut reversed) = tree.merge(&base, &b, &a).unwrap() else {
        panic!("overlapping branches merged");
    };
    reversed.sort();
    assert_eq!(reversed, expected);

    // A delete on one side and an update on the other is a conflict,
    // and an *identical* edit on both sides still is: resolving it as
    // "no conflict" would be a policy decision, and policy belongs to
    // the caller (§12's POSIX errno matrix), not here.
    let del = tree.apply(&base, &[delete(7)]).unwrap();
    let upd = tree.apply(&base, &[(key(7), Some(b"u".to_vec()))]).unwrap();
    assert_eq!(
        tree.merge(&base, &del, &upd).unwrap(),
        Merged::Conflicts(vec![key(7)])
    );
    let same = tree.apply(&base, &[(key(7), Some(b"u".to_vec()))]).unwrap();
    assert_eq!(upd, same);
    assert_eq!(
        tree.merge(&base, &upd, &same).unwrap(),
        Merged::Conflicts(vec![key(7)])
    );

    // An untouched branch merges to the other branch unchanged.
    assert_eq!(tree.merge(&base, &base, &a).unwrap(), Merged::Root(a));
    assert_eq!(tree.merge(&base, &a, &base).unwrap(), Merged::Root(a));
}

// -------------------------------------------------------------------
// 7. The entry clamp preserves canonicality, including under deletes
// -------------------------------------------------------------------

/// §14.1's correction, asserted. A node sealed by the clamp rather than
/// by a boundary key has *not* re-synchronized with its right
/// neighbour, so `apply` has to absorb it; deletes are what force that
/// path, because deleting a boundary key is what makes a clamped run
/// run on into the node beside it.
#[test]
fn the_entry_clamp_keeps_the_tree_canonical_under_deletes() {
    for (min, max) in [(1usize, usize::MAX), (1, 256), (1, 32), (8, 64), (16, 16)] {
        let tree = tree_with(
            Config::default()
                .with_leaf_agg(inode_agg)
                .with_entry_clamps(min, max),
        );
        let n = 6_000u64;
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = (0..n).map(pair).collect();
        let bulk = tree
            .build(model.iter().map(|(k, v)| (k.clone(), v.clone())))
            .unwrap();

        let mut rng = SmallRng::seed_from_u64(min as u64 * 1_000 + max as u64 % 1_000);
        let mut keys: Vec<u64> = (0..n).collect();
        keys.shuffle(&mut rng);
        assert_eq!(
            apply_in_order(&tree, &keys, 277).unwrap(),
            bulk,
            "min={min} max={max}: incremental build disagreed"
        );

        // Deletes, in several rounds so a clamped run has to absorb a
        // neighbour that a previous round already rewrote.
        let mut root = bulk;
        for round in 0..4 {
            let mut victims: Vec<u64> = model
                .keys()
                .map(|k| u64::from_be_bytes(k[1..9].try_into().unwrap()))
                .choose_multiple(&mut rng, 400);
            victims.sort_unstable();
            let edits: Vec<Edit> = victims.iter().copied().map(delete).collect();
            root = tree.apply(&root, &edits).unwrap();
            for i in &victims {
                model.remove(&key(*i));
            }
            let bulk = tree
                .build(model.iter().map(|(k, v)| (k.clone(), v.clone())))
                .unwrap();
            assert_eq!(
                root, bulk,
                "min={min} max={max} round {round}: a delete broke canonicality"
            );
        }

        // And the clamps were actually honoured.
        let census = tree.census(&root).unwrap();
        assert!(
            census.leaf_entries.iter().all(|count| *count <= max),
            "min={min} max={max}: a leaf held more than the clamp"
        );
        if max < 64 {
            // Small clamps must actually build a taller tree, or the
            // test above proved nothing about the clamp path.
            assert!(census.levels.len() >= 3, "min={min} max={max}: too flat");
        }
    }
}

// -------------------------------------------------------------------
// 8. Format pinning
// -------------------------------------------------------------------

/// This is an **on-bucket** format that ADR-5 promises not to migrate,
/// so a change to the node encoding is a breaking change: it changes
/// every node hash, and therefore every commit, snapshot and clone that
/// names one. If this test fails and the encoding change was
/// deliberate, bump `FORMAT_VERSION` and say so in the plan — do not
/// update the constants below and move on.
#[test]
fn the_node_encoding_and_root_hashes_are_pinned() {
    // A single hand-checkable leaf node, byte for byte.
    let encoded = constellation_mtree::node::encode(
        0,
        &[
            constellation_mtree::Entry::leaf(b"a".to_vec(), b"1".to_vec()),
            constellation_mtree::Entry::leaf(b"bb".to_vec(), b"22".to_vec()),
        ],
    );
    let expected_hex = concat!(
        "4d54524501",   // magic "MTRE", version 1
        "00",           // level 0
        "02000000",     // 2 entries
        "00000000",     // offset table: entry 0 at body+0
        "06000000",     //               entry 1 at body+6
        "0100010061",   // klen 1, vlen 1, "a", ...
        "31",           // "1"
        "020002006262", // klen 2, vlen 2, "bb"
        "3232",         // "22"
    );
    assert_eq!(hex(&encoded), expected_hex);
    assert_eq!(
        NodeRef::parse(&encoded).unwrap().entries().unwrap().len(),
        2
    );

    // An interior node, including the varint aggregate encoding.
    let interior = constellation_mtree::node::encode(
        1,
        &[constellation_mtree::Entry::child(
            b"a".to_vec(),
            NodeHash([0x11; 32]),
            Agg {
                bytes: 300,
                files: 2,
                keys: 5,
                max_mtime: 1,
            },
        )],
    );
    assert_eq!(
        hex(&interior),
        concat!(
            "4d54524501",
            "01",       // level 1
            "01000000", // 1 entry
            "00000000", // offset table
            "010061",   // klen 1, "a"
            "1111111111111111111111111111111111111111111111111111111111111111",
            "ac02", // bytes = 300
            "02",   // files
            "05",   // keys
            "01",   // max_mtime
        )
    );

    // A whole tree over a fixed key set, plain and keyed. These two
    // hashes are the format's fingerprint: they cover the encoding, the
    // boundary function, the entry clamps, the aggregate projection and
    // the level-collapse rule all at once.
    let plain = Tree::with_config(
        MemoryNodeStore::new(),
        Config::default().with_leaf_agg(inode_agg),
    )
    .unwrap();
    let plain_root = plain.build((0..1_000).map(pair)).unwrap();
    assert_eq!(
        plain_root.to_hex(),
        "07c1d2559d30a2745080ea96c5a63f4d3bf9ffdf0122458ced56fa5c17abe5de"
    );

    let keyed = Tree::with_config(
        MemoryNodeStore::new(),
        Config::keyed([0x5a; 32]).with_leaf_agg(inode_agg),
    )
    .unwrap();
    assert_eq!(
        keyed.build((0..1_000).map(pair)).unwrap().to_hex(),
        "e8fe697231b652587df57ae7464ed08ec8ada625d87c3a4eb4097eb3a6d4172e"
    );

    // The structure that root describes, so a failure says which half
    // moved.
    let census = plain.census(&plain_root).unwrap();
    assert_eq!(census.levels.len(), 2);
    assert_eq!(census.levels[0].nodes, 11);
    assert_eq!(census.levels[1].nodes, 1);
    assert_eq!(census.levels[0].entries, 1_000);
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// -------------------------------------------------------------------
// Seeded fuzz
// -------------------------------------------------------------------

/// Random op sequences against a `BTreeMap` oracle, with variable key
/// lengths and value sizes so the offset table and the `u16` length
/// fields see more than one shape. Cheap on purpose: this runs in
/// `cargo test --workspace`.
#[test]
fn seeded_fuzz_agrees_with_a_btreemap() {
    for seed in 0..8u64 {
        let tree = tree();
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut root = tree.empty().unwrap();

        for _ in 0..25 {
            let mut edits: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
            for _ in 0..60 {
                let len = rng.random_range(1..12usize);
                let mut k: Vec<u8> = (0..len).map(|_| rng.random_range(0..6u8)).collect();
                k.insert(0, 0x01);
                if rng.random_bool(0.3) && !model.is_empty() {
                    edits.insert(k, None);
                } else {
                    let vlen = rng.random_range(0..64usize);
                    edits.insert(k, Some(vec![rng.random::<u8>(); vlen]));
                }
            }
            for (k, v) in &edits {
                match v {
                    Some(v) => model.insert(k.clone(), v.clone()),
                    None => model.remove(k),
                };
            }
            let batch: Vec<Edit> = edits.into_iter().collect();
            root = tree.apply(&root, &batch).unwrap();

            let bulk = tree
                .build(model.iter().map(|(k, v)| (k.clone(), v.clone())))
                .unwrap();
            assert_eq!(root, bulk, "seed {seed}");

            // The cursor sees exactly the model, in order.
            let mut cursor = tree.cursor(&root).unwrap();
            let mut seen = 0usize;
            for (k, v) in &model {
                let (ck, cv) = cursor.entry().unwrap().expect("cursor ended early");
                assert_eq!(ck, k.as_slice());
                assert_eq!(cv, v.as_slice());
                cursor.next().unwrap();
                seen += 1;
            }
            assert!(cursor.entry().unwrap().is_none());
            assert_eq!(seen, model.len());
        }
    }
}

/// Node bytes arrive off a network, so every corruption has to be an
/// error and never a panic. Walks a seeded set of truncations and
/// single-byte flips through `parse`, and where `parse` accepts the
/// buffer, reads every accessor.
#[test]
fn corrupted_nodes_are_rejected_and_never_panic() {
    let valid = constellation_mtree::node::encode(
        0,
        &(0u32..40)
            .map(|i| {
                constellation_mtree::Entry::leaf(
                    i.to_be_bytes().to_vec(),
                    vec![i as u8; (i % 17) as usize],
                )
            })
            .collect::<Vec<_>>(),
    );
    let interior = constellation_mtree::node::encode(
        2,
        &(0u32..20)
            .map(|i| {
                constellation_mtree::Entry::child(
                    i.to_be_bytes().to_vec(),
                    NodeHash([i as u8; 32]),
                    Agg {
                        bytes: i as u64 * 1_000,
                        files: i as u64,
                        keys: i as u64,
                        max_mtime: i as i64,
                    },
                )
            })
            .collect::<Vec<_>>(),
    );

    let mut rng = SmallRng::seed_from_u64(99);
    let mut accepted = 0usize;
    for original in [&valid, &interior] {
        for _ in 0..4_000 {
            let mut buf = original.clone();
            match rng.random_range(0..3) {
                0 => buf.truncate(rng.random_range(0..buf.len())),
                1 => {
                    let at = rng.random_range(0..buf.len());
                    buf[at] ^= 1 << rng.random_range(0..8);
                }
                _ => {
                    let at = rng.random_range(0..buf.len());
                    buf[at] = rng.random();
                }
            }
            if let Ok(node) = NodeRef::parse(&buf) {
                accepted += 1;
                for i in 0..node.count() {
                    let _ = node.key(i);
                    let _ = node.leaf_value(i);
                    let _ = node.child(i);
                }
                let _ = node.entries();
                let _ = node.aggregate(constellation_mtree::no_leaf_agg);
                let _ = node.search(b"probe");
                let _ = node.descend(b"probe");
            }
        }
    }
    // A mutation that lands in a value byte is legitimately still a
    // valid node, so some must be accepted — otherwise this test would
    // be passing by rejecting everything.
    assert!(accepted > 0, "every corruption was rejected; check parse()");

    // And the accessors are bounds-checked even without `parse`: a
    // header-only check must not be a licence to read out of bounds.
    // This buffer has a well-formed header and a last offset pointing
    // off the end, which only an accessor can notice.
    let mut lying = valid.clone();
    let last_offset = 10 + (40 - 1) * 4;
    lying[last_offset..last_offset + 4].copy_from_slice(&0xffff_ff00u32.to_le_bytes());
    let node = NodeRef::new(&lying).unwrap();
    assert!(node.validate().is_err());
    for i in 0..node.count() {
        let _ = node.key(i);
        let _ = node.leaf_value(i);
    }
}

/// The empty tree's root is a pinned constant too: it is what a fresh
/// filesystem's first commit names.
#[test]
fn the_empty_root_is_pinned() {
    let tree = Tree::new(MemoryNodeStore::new());
    assert_eq!(
        tree.empty().unwrap().to_hex(),
        "ebc1996838f530f7194fd218cdae494c68a4e45733c45bbc867f97eb35bdb92f"
    );
    assert!(matches!(
        Tree::with_config(
            MemoryNodeStore::new(),
            Config::default().with_entry_clamps(0, 1)
        ),
        Err(MtreeError::Config(_))
    ));
}
