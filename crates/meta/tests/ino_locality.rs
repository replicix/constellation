//! Plan 28 §S1b acceptance tests: directory-local ino allocation
//! (`constellation_meta::store::alloc_ino_tx`, `INO_BLOCK_SIZE`).
//!
//! The old allocator was a single global `counter++` shared by every
//! directory, so an aged, multi-directory workload — files created in
//! many directories over time rather than one directory at a time —
//! scatters a directory's children across the whole per-node ino range
//! (§14.10 measured ~7× write amplification from exactly this). The new
//! allocator gives each directory its own lazily-reserved block (see
//! `INO_BLOCK_SIZE`'s doc comment for the exact rule), so an interleaved
//! workload should still cluster each directory's children into a small
//! number of blocks.

use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::Ino;
use constellation_meta::store::INO_BLOCK_SIZE;
use constellation_meta::{Meta, MetaStore};
use std::collections::BTreeMap;

/// The 1024-ino bucket a raw counter value falls into — the same
/// granularity §14.10 measured spread with, and `INO_BLOCK_SIZE`'s
/// value on purpose.
fn bucket_of(ino: Ino) -> u64 {
    (ino & ((1u64 << 40) - 1)) / 1024
}

/// Interleave file creation across `n_dirs` directories: round-robin
/// one create per directory per round, `rounds` times. This is the
/// pathological order for a global counter (every directory's Nth file
/// lands `n_dirs` counter values after its (N-1)th) and a realistic
/// shape for an aged, multi-tenant filesystem.
fn interleaved_workload(meta: &Meta, n_dirs: usize, rounds: usize) -> Vec<Vec<Ino>> {
    let dirs: Vec<Ino> = (0..n_dirs)
        .map(|i| {
            meta.mkdir(ROOT_INO, &format!("d{i}"), 0o755, 0, 0)
                .unwrap()
                .ino
        })
        .collect();
    let mut children: Vec<Vec<Ino>> = vec![Vec::new(); n_dirs];
    for round in 0..rounds {
        for (i, &dir) in dirs.iter().enumerate() {
            let ino = meta
                .create(dir, &format!("f{round}"), 0o644, 0, 0)
                .unwrap()
                .ino;
            children[i].push(ino);
        }
    }
    children
}

#[test]
fn interleaved_creates_cluster_each_directorys_children_into_few_blocks() {
    let meta = Meta::open_in_memory().unwrap();
    let n_dirs = 20;
    let rounds = 200; // 200 files/directory: well under one INO_BLOCK_SIZE.
    let children = interleaved_workload(&meta, n_dirs, rounds);

    for (i, inos) in children.iter().enumerate() {
        let buckets: std::collections::BTreeSet<u64> =
            inos.iter().map(|&ino| bucket_of(ino)).collect();
        assert!(
            buckets.len() <= 2,
            "directory {i} with {} children spread across {} distinct {INO_BLOCK_SIZE}-ino buckets: {:?}",
            inos.len(),
            buckets.len(),
            buckets
        );
    }

    // Cross-check against what a global counter would have done: under
    // round-robin creation the raw counter stride between a directory's
    // consecutive files is close to n_dirs, so consecutive children are
    // almost never in the same 1024-bucket once n_dirs is a sizeable
    // fraction of 1024. The new allocator must still be tightly
    // clustered under that same interleaving.
    for inos in &children {
        let min = *inos.iter().min().unwrap();
        let max = *inos.iter().max().unwrap();
        assert!(
            max - min < INO_BLOCK_SIZE * 2,
            "directory's children span {} inos, wider than {} blocks",
            max - min,
            2
        );
    }
}

#[test]
fn a_directory_that_exceeds_one_block_gets_a_second_contiguous_block() {
    let meta = Meta::open_in_memory().unwrap();
    let dir = meta.mkdir(ROOT_INO, "big", 0o755, 0, 0).unwrap().ino;
    let n = (INO_BLOCK_SIZE * 2 + 5) as usize;
    let mut inos = Vec::with_capacity(n);
    for i in 0..n {
        inos.push(meta.create(dir, &format!("f{i}"), 0o644, 0, 0).unwrap().ino);
    }
    let buckets: std::collections::BTreeSet<u64> = inos.iter().map(|&ino| bucket_of(ino)).collect();
    // Just over two blocks' worth of children must land in exactly
    // three buckets (the overflow rule: a full block hands the next
    // create a fresh one), never scattered further.
    assert_eq!(buckets.len(), 3, "buckets: {buckets:?}");
}

#[test]
fn unrelated_directories_never_collide_on_the_same_ino() {
    let meta = Meta::open_in_memory().unwrap();
    let children = interleaved_workload(&meta, 30, 50);
    let mut seen = BTreeMap::new();
    for (dir_idx, inos) in children.iter().enumerate() {
        for &ino in inos {
            if let Some(prev) = seen.insert(ino, dir_idx) {
                panic!("ino {ino} allocated to both directory {prev} and {dir_idx}");
            }
        }
    }
}

// ------------------------------------------------- write-amplification

/// Reimplements the *old* (pre-§S1b) allocator exactly: a single global
/// `counter++` with no per-directory locality. Kept here, test-only, as
/// the comparison baseline the plan asks for — the production allocator
/// no longer has this code path.
struct GlobalCounterAllocator {
    next: std::cell::Cell<u64>,
}

impl GlobalCounterAllocator {
    fn new(start: u64) -> Self {
        Self {
            next: std::cell::Cell::new(start),
        }
    }

    fn alloc(&self) -> u64 {
        let ino = self.next.get();
        self.next.set(ino + 1);
        ino
    }
}

/// Build the same interleaved, aged workload under the old global
/// counter and the new block allocator, then measure the cost of a
/// *directory-local* scattered update against each aged tree — the
/// §14.8/§14.10 methodology: `chmod -R`/`setattr`-style operations that
/// rewrite every child inode of one directory rewrite the `0x01` leaves
/// those inos happen to fall into. Under the old global counter an aged
/// directory's children are scattered across the whole ino range (their
/// interleaved creation order), so this touches many leaves; under the
/// new block allocator they cluster into one or two, so it touches few.
/// The one-shot *creation* cost is deliberately not what this measures:
/// a round's new inos are contiguous under either policy immediately
/// after allocation (they were just handed out back-to-back), so a
/// same-round publish cannot see the difference — only a later,
/// directory-scoped touch of an already-aged tree can.
///
/// `#[ignore]`d: a measurement, not a correctness gate — run explicitly
/// with `cargo test --release -p constellation-meta --test ino_locality
/// -- --ignored --nocapture`.
#[test]
#[ignore]
fn measure_directory_local_setattr_amplification_old_vs_new_allocator() {
    use constellation_mtree::keys;
    use constellation_mtree::record::{Attrs, InodeRecord, Kind};
    use constellation_mtree::{MemoryNodeStore, Tree};

    const N_DIRS: usize = 64;
    const ROUNDS: usize = 400; // 25,600 files total: an aged, multi-directory tree.

    fn dir_ino(i: usize) -> u64 {
        (1u64 << 40) | (i as u64 + 2)
    }

    fn inode_bytes(_ino: u64, mtime_ns: i64) -> Vec<u8> {
        let attrs = Attrs {
            kind: Kind::File,
            mode: 0o644,
            uid: 0,
            gid: 0,
            nlink: 1,
            size: 0,
            mtime_ns,
            ctime_ns: mtime_ns,
            rdev: Default::default(),
        };
        InodeRecord::new(attrs).encode()
    }

    /// Build the aged tree: one `Tree::apply` per round (inode + dentry
    /// for every directory's new file that round), returning the store,
    /// the final root, and every directory's file inos in creation order.
    fn build_aged(
        alloc: &mut impl FnMut(usize, usize) -> u64,
    ) -> (
        MemoryNodeStore,
        constellation_mtree::NodeHash,
        Vec<Vec<u64>>,
    ) {
        let store = MemoryNodeStore::new();
        let tree = Tree::new(&store);
        let mut root = tree.empty().unwrap();
        let mut children: Vec<Vec<u64>> = (0..N_DIRS).map(|_| Vec::with_capacity(ROUNDS)).collect();
        for round in 0..ROUNDS {
            let mut edits = Vec::with_capacity(N_DIRS * 2);
            for (d, dir_children) in children.iter_mut().enumerate() {
                let fino = alloc(d, round);
                dir_children.push(fino);
                edits.push((keys::inode(fino), Some(inode_bytes(fino, 0))));
                edits.push((
                    keys::dentry(dir_ino(d), format!("f{round}").as_bytes()),
                    Some(inode_bytes(fino, 0)),
                ));
            }
            edits.sort_by(|a, b| a.0.cmp(&b.0));
            root = tree.apply(&root, &edits).unwrap();
        }
        (store, root, children)
    }

    let old = GlobalCounterAllocator::new((1u64 << 40) | (N_DIRS as u64 + 2));
    let (old_store, old_root, old_children) = build_aged(&mut |_d, _round| old.alloc());

    let meta = Meta::open_in_memory().unwrap();
    let dirs: Vec<Ino> = (0..N_DIRS)
        .map(|i| {
            meta.mkdir(ROOT_INO, &format!("d{i}"), 0o755, 0, 0)
                .unwrap()
                .ino
        })
        .collect();
    let (new_store, new_root, new_children) = build_aged(&mut |d, round| {
        meta.create(dirs[d], &format!("f{round}"), 0o644, 0, 0)
            .unwrap()
            .ino
    });

    // Directory-local "setattr" over every child of directory 0: bump
    // every one of its files' mtime in a single commit, exactly
    // §14.8/§14.10's directory-local scattered-vs-clustered probe.
    fn touch_directory(
        store: &MemoryNodeStore,
        root: &constellation_mtree::NodeHash,
        inos: &[u64],
    ) -> u64 {
        let tree = Tree::new(store);
        let edits: Vec<_> = inos
            .iter()
            .map(|&ino| (keys::inode(ino), Some(inode_bytes(ino, 1))))
            .collect();
        store.reset_counters();
        tree.apply(root, &edits).unwrap();
        store.distinct_writes()
    }

    let old_touch = touch_directory(&old_store, &old_root, &old_children[0]);
    let new_touch = touch_directory(&new_store, &new_root, &new_children[0]);

    eprintln!(
        "directory-local setattr over {} files, {N_DIRS} directories, {ROUNDS} aged interleaved rounds:\n\
         old (global counter):   {old_touch} distinct node writes\n\
         new (directory blocks): {new_touch} distinct node writes\n\
         ratio (old/new): {:.2}x",
        old_children[0].len(),
        old_touch as f64 / new_touch.max(1) as f64
    );
    assert!(
        new_touch < old_touch,
        "new allocator's directory-local setattr should rewrite fewer nodes than the old \
         global counter's on this aged, interleaved workload: old={old_touch} new={new_touch}"
    );
}
