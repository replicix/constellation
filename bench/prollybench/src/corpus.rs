//! Synthetic census-scale namespace.
//!
//! Shaped after the reference census in plan 28 §3 (11.9M entries,
//! 1.3 TiB, small-file median) rather than after uniform random data:
//! value sizes, directory fanout, and name lengths all drive node size,
//! and node size is what every number in §0.1 and §0.2 is denominated in.
//!
//! Entries are held in a flat arena, not as a `Vec<(Vec<u8>, Vec<u8>)>`:
//! at 23.8M keys the per-allocation overhead of the obvious
//! representation is larger than the tree being measured.

use rand::prelude::*;
use rand::rngs::SmallRng;

use crate::keys::{self, Attrs, KIND_DIR, KIND_FILE, KIND_LINK};

pub struct Rec {
    pub parent: u64,
    pub name_off: u32,
    pub name_len: u8,
    pub kind: u8,
    pub nchunks: u8,
    pub mode: u32,
    pub size: u64,
    pub mtime_ns: i64,
}

pub struct Corpus {
    pub recs: Vec<Rec>,
    pub names: Vec<u8>,
    /// `recs` indices ordered by `(parent, name)` — the `0x02` key order.
    pub by_dentry: Vec<u32>,
    /// Inos whose xattr set spilled past `XATTR_INLINE` into `0x03`.
    pub spilled: Vec<u64>,
    pub xattrs_per_spilled: usize,
    /// Directories with at least ~1000 children, for the cold `ls -la`
    /// and readdir measurements.
    pub big_dirs: Vec<u64>,
    pub total_bytes: u64,
    pub dir_count: usize,
}

impl Corpus {
    pub fn ino(&self, idx: usize) -> u64 {
        idx as u64 + 2
    }

    pub fn name(&self, r: &Rec) -> &[u8] {
        &self.names[r.name_off as usize..r.name_off as usize + r.name_len as usize]
    }

    pub fn attrs(&self, r: &Rec) -> Attrs {
        Attrs {
            kind: r.kind,
            mode: r.mode,
            uid: 1000,
            gid: 1000,
            nlink: 1,
            size: r.size,
            mtime_ns: r.mtime_ns,
            ctime_ns: r.mtime_ns,
            atime_ns: r.mtime_ns,
        }
    }

    pub fn len(&self) -> usize {
        self.recs.len()
    }

    #[cfg(test)]
    pub fn file_count(&self) -> usize {
        self.recs.iter().filter(|r| r.kind == KIND_FILE).count()
    }

    /// The manifest and xattr set that belong to this entry's
    /// authoritative record, wherever the current encoding puts it.
    pub fn payload(&self, idx: usize) -> (Vec<[u8; 32]>, Vec<(Vec<u8>, Vec<u8>)>) {
        let r = &self.recs[idx];
        let ino = self.ino(idx);
        let chunks: Vec<[u8; 32]> = (0..r.nchunks).map(|c| fake_hash(ino, c as u64)).collect();
        let xattrs = if r.kind == KIND_FILE && ino.is_multiple_of(3) {
            // The common non-empty case: one small label, inlined by
            // the §P6 rule instead of becoming two more keys.
            vec![(
                b"security.selinux".to_vec(),
                b"unconfined_u:object_r:user_home_t:s0".to_vec(),
            )]
        } else {
            Vec::new()
        };
        (chunks, xattrs)
    }

    /// `0x01` records, already in key order (ino ascending). Ino 1 is the
    /// mount root, which has no dentry. Under `Enc::DentryAuth` the
    /// `nlink == 1` entries have no `0x01` key at all.
    pub fn inode_entries(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_ {
        let root = keys::Attrs {
            kind: KIND_DIR,
            mode: 0o40755,
            uid: 0,
            gid: 0,
            nlink: 1,
            size: 4096,
            mtime_ns: 1_750_000_000_000_000_000,
            ctime_ns: 1_750_000_000_000_000_000,
            atime_ns: 1_750_000_000_000_000_000,
        };
        std::iter::once((keys::inode_key(1), keys::inode_val(&root, &[], &[]))).chain(
            (0..self.recs.len()).filter_map(move |i| {
                let r = &self.recs[i];
                if !keys::has_inode_record(r.kind) {
                    return None;
                }
                let (chunks, xattrs) = self.payload(i);
                Some((
                    keys::inode_key(self.ino(i)),
                    keys::inode_val(&self.attrs(r), &chunks, &xattrs),
                ))
            }),
        )
    }

    /// `0x02` records in `(parent, name)` order.
    pub fn dentry_entries(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_ {
        self.by_dentry.iter().map(move |&i| {
            let r = &self.recs[i as usize];
            let ino = self.ino(i as usize);
            // Only the dentry-authoritative shape reads them, and building
            // them for 11.9M entries that ignore them is pure overhead.
            let (chunks, xattrs) = if keys::enc() == keys::Enc::DentryAuth {
                self.payload(i as usize)
            } else {
                (Vec::new(), Vec::new())
            };
            (
                keys::dentry_key(r.parent, self.name(r)),
                keys::dentry_val_with(ino, &self.attrs(r), &chunks, &xattrs),
            )
        })
    }

    /// `0x03` records for the spilled xattr sets, in key order.
    pub fn xattr_entries(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_ {
        self.spilled.iter().flat_map(move |&ino| {
            (0..self.xattrs_per_spilled).map(move |j| {
                (
                    keys::xattr_key(ino, format!("user.attr{j:04}").as_bytes()),
                    vec![b'v'; 96],
                )
            })
        })
    }

    /// `0x04` reverse dentries, in key order (ino ascending).
    pub fn rdentry_entries(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_ {
        (0..self.recs.len()).map(move |i| {
            let r = &self.recs[i];
            (
                keys::rdentry_key(self.ino(i), r.parent, self.name(r)),
                Vec::new(),
            )
        })
    }

    pub fn all_entries(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_ {
        self.inode_entries()
            .chain(self.dentry_entries())
            .chain(self.xattr_entries())
            .chain(self.rdentry_entries())
    }

    pub fn key_count(&self) -> u64 {
        let inodes = if keys::enc() == keys::Enc::DentryAuth {
            self.dir_count as u64
        } else {
            self.recs.len() as u64
        };
        inodes
            + self.recs.len() as u64 * 2
            + 1
            + (self.spilled.len() * self.xattrs_per_spilled) as u64
    }

    /// A sample of `(parent, name)` pairs for random lookups.
    pub fn sample_dentries(&self, n: usize, seed: u64) -> Vec<Vec<u8>> {
        let mut rng = SmallRng::seed_from_u64(seed);
        (0..n)
            .map(|_| {
                let r = &self.recs[rng.random_range(0..self.recs.len())];
                keys::dentry_key(r.parent, self.name(r))
            })
            .collect()
    }

    pub fn sample_inos(&self, n: usize, seed: u64) -> Vec<u64> {
        let mut rng = SmallRng::seed_from_u64(seed ^ 0x5eed);
        (0..n)
            .map(|_| self.ino(rng.random_range(0..self.recs.len())))
            .collect()
    }
}

pub fn fake_hash(a: u64, b: u64) -> [u8; 32] {
    let mut h = [0u8; 32];
    let mut x = a.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ b.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    for c in h.chunks_mut(8) {
        x ^= x >> 30;
        x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x ^= x >> 27;
        c.copy_from_slice(&x.to_le_bytes());
    }
    h
}

/// File sizes with a small-file median and a modest tail, tuned so the
/// corpus lands near the census's ~117 KiB per entry (1.3 TiB / 11.9M).
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

/// `entries` inodes in a directory tree with realistic fanout, plus
/// `spill_dirs` directories forced wide enough to exercise a paged
/// readdir, and `spilled` inodes carrying an out-of-line xattr set.
pub fn generate(entries: usize, seed: u64) -> Corpus {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut recs: Vec<Rec> = Vec::with_capacity(entries);
    let mut names: Vec<u8> = Vec::with_capacity(entries * 13);
    // Recently created directories, so writes cluster the way a real
    // filesystem's do (§P6: write locality == read locality).
    let mut dirs: Vec<u64> = vec![1];
    // (ino, children so far, fanout cap). Most directories are small;
    // a few percent are wide, which is where paged readdir and the cold
    // `ls -la` measurements come from.
    let mut open: Vec<(u64, u32, u32)> = vec![(1, 0, 4000)];
    let mut total_bytes = 0u64;
    let mut big_dirs: Vec<u64> = Vec::new();

    // A wide directory is filled in one burst, the way a real one is
    // created (an extracted tarball, a Maildir, a build output).
    let mut filling: Option<(u64, u32)> = None;

    for i in 0..entries {
        let ino = i as u64 + 2;
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
                        open.push((*dirs.last().unwrap(), 0, fanout_cap(&mut rng)));
                    }
                }
                parent
            }
        };
        let is_dir = !forced_file && rng.random_bool(0.09);
        if is_dir && filling.is_none() && rng.random_bool(0.01) && entries - i > 6_000 {
            filling = Some((ino, rng.random_range(1_000..5_000)));
            big_dirs.push(ino);
        }
        let is_link = !is_dir && rng.random_bool(0.01);
        let name_off = names.len() as u32;
        let base = if is_dir { "d" } else { "f" };
        let name = format!("{base}{ino:x}{}", suffix(&mut rng));
        names.extend_from_slice(name.as_bytes());
        let size = if is_dir {
            4096
        } else if is_link {
            32
        } else {
            pick_size(&mut rng)
        };
        if !is_dir && !is_link {
            total_bytes += size;
        }
        recs.push(Rec {
            parent,
            name_off,
            name_len: name.len() as u8,
            kind: if is_dir {
                KIND_DIR
            } else if is_link {
                KIND_LINK
            } else {
                KIND_FILE
            },
            nchunks: if is_dir || is_link {
                0
            } else {
                (1 + size / (1024 * 1024)).min(8) as u8
            },
            mode: if is_dir { 0o40755 } else { 0o100644 },
            size,
            mtime_ns: 1_750_000_000_000_000_000 + i as i64 * 1_000_000,
        });
        if is_dir {
            dirs.push(ino);
            let slot = (ino, 0, fanout_cap(&mut rng));
            if open.len() < 64 {
                open.push(slot);
            } else if rng.random_bool(0.5) {
                let victim = rng.random_range(0..open.len());
                if open[victim].1 >= 1000 {
                    big_dirs.push(open[victim].0);
                }
                open[victim] = slot;
            }
        }
    }

    for (d, n, _) in &open {
        if *n >= 1000 {
            big_dirs.push(*d);
        }
    }

    let mut by_dentry: Vec<u32> = (0..recs.len() as u32).collect();
    {
        use rayon::prelude::*;
        let names_ref = &names;
        let recs_ref = &recs;
        by_dentry.par_sort_unstable_by(|&a, &b| {
            let (ra, rb) = (&recs_ref[a as usize], &recs_ref[b as usize]);
            let na = &names_ref[ra.name_off as usize..ra.name_off as usize + ra.name_len as usize];
            let nb = &names_ref[rb.name_off as usize..rb.name_off as usize + rb.name_len as usize];
            ra.parent.cmp(&rb.parent).then_with(|| na.cmp(nb))
        });
    }

    // A slice of files carries an xattr set too large to inline, so it
    // spills into `0x03` keys (§P6 / Appendix B).
    let xattrs_per_spilled = 20;
    assert!(
        xattrs_per_spilled * (16 + 96) > keys::XATTR_INLINE,
        "the spilled set must actually exceed the inline budget"
    );
    let spilled: Vec<u64> = (0..recs.len())
        .step_by(2000)
        .map(|i| i as u64 + 2)
        .collect();

    let dir_count = recs.iter().filter(|r| r.kind == KIND_DIR).count();
    Corpus {
        recs,
        names,
        by_dentry,
        spilled,
        xattrs_per_spilled,
        big_dirs,
        total_bytes,
        dir_count,
    }
}

fn fanout_cap(rng: &mut SmallRng) -> u32 {
    let r: f64 = rng.random();
    if r < 0.90 {
        rng.random_range(6..40)
    } else if r < 0.99 {
        rng.random_range(100..400)
    } else {
        rng.random_range(1000..5000)
    }
}

fn suffix(rng: &mut SmallRng) -> String {
    const ALPHA: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789_-.";
    let n = rng.random_range(3..12);
    (0..n)
        .map(|_| ALPHA[rng.random_range(0..ALPHA.len())] as char)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_streams_are_sorted_and_disjoint() {
        let _g = keys::ENC_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let c = generate(20_000, 1);
        for enc in [keys::Enc::Copy, keys::Enc::NoCopy, keys::Enc::DentryAuth] {
            keys::set_enc(enc);
            for stream in [
                c.inode_entries().collect::<Vec<_>>(),
                c.dentry_entries().collect::<Vec<_>>(),
                c.xattr_entries().collect::<Vec<_>>(),
                c.rdentry_entries().collect::<Vec<_>>(),
            ] {
                assert!(stream.windows(2).all(|w| w[0].0 < w[1].0));
            }
            let all: Vec<_> = c.all_entries().map(|(k, _)| k).collect();
            assert!(all.windows(2).all(|w| w[0] < w[1]));
            assert_eq!(all.len() as u64, c.key_count());
        }
        keys::set_enc(keys::Enc::Copy);
    }

    /// Dropping the attr copy must not change *which* entries exist, only
    /// what the `0x02` value carries; the dentry-authoritative shape drops
    /// the `0x01` key for every `nlink == 1` entry and nothing else.
    #[test]
    fn variants_differ_only_where_they_should() {
        let _g = keys::ENC_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let c = generate(20_000, 2);
        let dkeys = |e: keys::Enc| {
            keys::set_enc(e);
            c.dentry_entries().map(|(k, _)| k).collect::<Vec<_>>()
        };
        assert_eq!(dkeys(keys::Enc::Copy), dkeys(keys::Enc::NoCopy));
        assert_eq!(dkeys(keys::Enc::Copy), dkeys(keys::Enc::DentryAuth));
        keys::set_enc(keys::Enc::Copy);
        let with = c.inode_entries().count();
        keys::set_enc(keys::Enc::DentryAuth);
        let without = c.inode_entries().count();
        assert_eq!(with - without, c.recs.len() - c.dir_count);
        keys::set_enc(keys::Enc::Copy);
    }
}
