//! Synthetic census-like namespace generator, adapted from
//! `bench/prollybench/src/corpus.rs` for plan 28 §11b's engine bake-off.
//!
//! Differs from prollybench's generator in the ways that matter for a
//! *realistic* engine comparison rather than a pure tree-shape study:
//! inos are allocated from one global, ever-increasing counter (so a
//! directory's children scatter over "time" exactly as §S1b describes),
//! a small fraction of files carry hard links, and every produced
//! record is a `constellation_mtree::record` value — the same bytes
//! every engine under test stores, so the comparison is over engine
//! mechanics and not over a second, unaudited encoding.

use constellation_mtree::record::{Attrs, Kind};
use rand::prelude::*;
use rand::rngs::SmallRng;
use std::sync::atomic::{AtomicU64, Ordering};

/// One filesystem entry as the generator produces it. `ino` is already
/// assigned from the global counter at generation time (bulk load), or
/// at churn time (aging) — never derived from position, so lookups by
/// index still see realistic scatter.
#[derive(Clone, Debug)]
pub struct Rec {
    pub ino: u64,
    pub parent: u64,
    pub name: Vec<u8>,
    pub kind: Kind,
    pub mode: u32,
    pub size: u64,
    pub mtime_ns: i64,
    pub nlink: u32,
    /// `None`: no xattr. `Some(small)`: one inline label. `Some(big)`:
    /// a set that spills past `XATTR_INLINE` into the `0x03` range.
    pub xattrs: Vec<(Vec<u8>, Vec<u8>)>,
    pub symlink_target: Option<Vec<u8>>,
}

impl Rec {
    pub fn attrs(&self) -> Attrs {
        Attrs {
            kind: self.kind,
            mode: self.mode,
            uid: 1000,
            gid: 1000,
            nlink: self.nlink,
            size: self.size,
            mtime_ns: self.mtime_ns,
            ctime_ns: self.mtime_ns,
            rdev: 0,
        }
    }
}

/// Extra hard-link dentries: `(parent, name, target_ino)`.
#[derive(Clone, Debug)]
pub struct ExtraLink {
    pub parent: u64,
    pub name: Vec<u8>,
    pub ino: u64,
}

pub struct Corpus {
    pub recs: Vec<Rec>,
    pub extra_links: Vec<ExtraLink>,
    /// Directories with >= 1000 children — cold `ls -la` / paged
    /// readdir targets, built in one incremental burst (mtree §14.10
    /// shape) rather than by a bulk loader.
    pub big_dirs: Vec<u64>,
    pub dir_count: usize,
    pub total_logical_bytes: u64,
}

/// Global ino allocator, shared by corpus generation and the aging
/// workload so churn continues the same sequence a live filesystem
/// would (§S1b: one counter, never per-directory).
pub struct InoAlloc(AtomicU64);

impl InoAlloc {
    pub fn starting_at(next: u64) -> InoAlloc {
        InoAlloc(AtomicU64::new(next))
    }
    pub fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
    pub fn peek(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

fn pick_size(rng: &mut SmallRng) -> u64 {
    let r: f64 = rng.random();
    if r < 0.75 {
        rng.random_range(0..16 * 1024)
    } else if r < 0.93 {
        rng.random_range(16 * 1024..128 * 1024)
    } else if r < 0.99 {
        rng.random_range(128 * 1024..1024 * 1024)
    } else {
        rng.random_range(1024 * 1024..16 * 1024 * 1024)
    }
}

fn fanout_cap(rng: &mut SmallRng) -> u32 {
    // Most directories are small (tens of entries); a long tail is
    // wide. Matches prollybench's shape, tuned for a census-like median.
    if rng.random_bool(0.85) {
        rng.random_range(2..40)
    } else if rng.random_bool(0.7) {
        rng.random_range(40..300)
    } else {
        rng.random_range(300..4000)
    }
}

fn gen_name(rng: &mut SmallRng, ino: u64, is_dir: bool) -> Vec<u8> {
    let base = if is_dir { "d" } else { "f" };
    // Realistic length spread: short names common, a tail of long ones
    // (hashes, timestamps, UUID-ish build artifacts).
    let suffix_len = if rng.random_bool(0.6) {
        rng.random_range(0..6)
    } else if rng.random_bool(0.9) {
        rng.random_range(6..20)
    } else {
        rng.random_range(20..80)
    };
    let mut s = format!("{base}{ino:x}");
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-";
    for _ in 0..suffix_len {
        s.push(ALPHABET[rng.random_range(0..ALPHABET.len())] as char);
    }
    s.into_bytes()
}

/// Generate `entries` inodes under root ino 1, with realistic fanout, a
/// few wide directories, ~10% of files carrying an inline xattr, a rare
/// spilled xattr set, hard links and symlinks. `ino_alloc` should start
/// at 2 for a fresh corpus.
pub fn generate(entries: usize, seed: u64, ino_alloc: &InoAlloc) -> Corpus {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut recs: Vec<Rec> = Vec::with_capacity(entries);
    let mut extra_links = Vec::new();
    let mut open: Vec<(u64, u32, u32)> = vec![(1, 0, fanout_cap(&mut rng))];
    let mut big_dirs = Vec::new();
    let mut dir_count = 1usize; // root
    let mut total_logical_bytes = 0u64;
    let mut filling: Option<(u64, u32)> = None;
    let base_time: i64 = 1_750_000_000_000_000_000;
    let mut all_file_idx: Vec<usize> = Vec::new();

    for i in 0..entries {
        let ino = ino_alloc.next();
        let mut forced_file = false;
        let parent = match &mut filling {
            Some((dir, left)) => {
                *left -= 1;
                let d = *dir;
                if *left == 0 {
                    filling = None;
                }
                forced_file = true;
                d
            }
            None => {
                let pick = rng.random_range(0..open.len());
                let parent = open[pick].0;
                open[pick].1 += 1;
                if open[pick].1 >= open[pick].2 {
                    if open[pick].1 >= 1000 {
                        big_dirs.push(open[pick].0);
                    }
                    open.swap_remove(pick);
                    if open.is_empty() {
                        open.push((recs.last().map(|r| r.parent).unwrap_or(1), 0, fanout_cap(&mut rng)));
                    }
                }
                parent
            }
        };
        let is_dir = !forced_file && rng.random_bool(0.06);
        if is_dir && filling.is_none() && rng.random_bool(0.008) && entries - i > 6_000 {
            let width = rng.random_range(1_000..6_000);
            filling = Some((ino, width));
            big_dirs.push(ino);
        }
        let is_link = !is_dir && rng.random_bool(0.01);
        let name = gen_name(&mut rng, ino, is_dir);
        let size = if is_dir {
            4096
        } else if is_link {
            rng.random_range(8..80)
        } else {
            pick_size(&mut rng)
        };
        if !is_dir && !is_link {
            total_logical_bytes += size;
        }
        let xattrs = if is_dir {
            Vec::new()
        } else if rng.random_bool(0.10) {
            if rng.random_bool(0.02) {
                // Rare: a set that spills past XATTR_INLINE (256 B).
                (0..6)
                    .map(|j| (format!("user.big{j}").into_bytes(), vec![b'x'; 64]))
                    .collect()
            } else {
                vec![(
                    b"security.selinux".to_vec(),
                    b"unconfined_u:object_r:user_home_t:s0".to_vec(),
                )]
            }
        } else {
            Vec::new()
        };
        if is_dir {
            open.push((ino, 0, fanout_cap(&mut rng)));
            dir_count += 1;
        } else if !is_link {
            all_file_idx.push(recs.len());
        }
        recs.push(Rec {
            ino,
            parent,
            name,
            kind: if is_dir {
                Kind::Dir
            } else if is_link {
                Kind::Symlink
            } else {
                Kind::File
            },
            mode: if is_dir { 0o40755 } else { 0o100644 },
            size,
            mtime_ns: base_time + i as i64 * 1_000_000,
            nlink: 1,
            xattrs,
            symlink_target: is_link.then(|| b"../target/of/symlink".to_vec()),
        });
    }

    // Hard links: ~0.05% of files get a second name elsewhere.
    let n_links = if all_file_idx.is_empty() { 0 } else { (all_file_idx.len() / 2000).max(1) };
    for _ in 0..n_links {
        let ridx = all_file_idx[rng.random_range(0..all_file_idx.len())];
        let ino = recs[ridx].ino;
        let parent = recs[rng.random_range(0..recs.len())].parent;
        let name = gen_name(&mut rng, ino, false);
        extra_links.push(ExtraLink { parent, name, ino });
        recs[ridx].nlink += 1;
    }

    Corpus {
        recs,
        extra_links,
        big_dirs,
        dir_count,
        total_logical_bytes,
    }
}
