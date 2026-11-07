//! C8a: an in-memory cuckoo filter in front of an exact mirror kept on
//! local disk. A filter hit is confirmed by a disk probe, so no false
//! positive leaves the node and the mirror stays exact (repair works to
//! any depth). The disk side is a sorted run of u64 keys with a sparse
//! in-memory index (every 128th key) and an in-memory overlay of
//! recent adds/removes, merged into a fresh file when the overlay
//! passes a threshold. A probe is one 1 KiB `pread` plus a binary
//! search inside the block.

use super::cuckoo::CuckooSet;
use super::span;
use crate::acc::Acc;
use crate::mirror::{LocationMirror, SeqState};
use constellation_net::reconcile::{Fingerprint, Key, Range};
use std::collections::BTreeSet;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

const BLOCK_KEYS: usize = 128;
static FILE_ID: AtomicU64 = AtomicU64::new(0);

/// Directory for the on-disk mirrors (`COOP_DIGEST_DISK`, else the
/// system temp dir).
pub fn disk_dir() -> PathBuf {
    std::env::var_os("COOP_DIGEST_DISK").map_or_else(std::env::temp_dir, PathBuf::from)
}

pub struct DiskExact {
    path: PathBuf,
    file: File,
    n_disk: usize,
    sparse: Vec<u64>,
    added: BTreeSet<u64>,
    removed: BTreeSet<u64>,
    merge_at: usize,
    pub merges: u64,
    pub bytes_written: u64,
    pub probes: std::cell::Cell<u64>,
}

impl DiskExact {
    pub fn from_sorted(keys: &[Key]) -> Self {
        let id = FILE_ID.fetch_add(1, Ordering::Relaxed);
        let path = disk_dir().join(format!("coop-digest-{}-{id}.run", std::process::id()));
        let mut d = Self {
            file: File::create(&path)
                .and_then(|_| File::open(&path))
                .expect("disk mirror file"),
            path,
            n_disk: 0,
            sparse: Vec::new(),
            added: BTreeSet::new(),
            removed: BTreeSet::new(),
            merge_at: 16_384,
            merges: 0,
            bytes_written: 0,
            probes: std::cell::Cell::new(0),
        };
        d.write_run(keys);
        d
    }

    fn write_run(&mut self, keys: &[Key]) {
        let mut tmp = self.path.clone();
        tmp.set_extension("tmp");
        let mut w = std::io::BufWriter::new(File::create(&tmp).expect("tmp run"));
        for &k in keys {
            w.write_all(&k.to_le_bytes()).expect("write run");
        }
        w.flush().expect("flush run");
        drop(w);
        std::fs::rename(&tmp, &self.path).expect("rename run");
        self.file = File::open(&self.path).expect("reopen run");
        self.n_disk = keys.len();
        self.bytes_written += keys.len() as u64 * 8;
        self.sparse = keys.iter().step_by(BLOCK_KEYS).copied().collect();
        self.merge_at = (keys.len() / 200).max(16_384);
    }

    fn read_block(&self, b: usize) -> Vec<u64> {
        let first = b * BLOCK_KEYS;
        let cnt = BLOCK_KEYS.min(self.n_disk - first);
        let mut buf = vec![0u8; cnt * 8];
        self.file
            .read_exact_at(&mut buf, first as u64 * 8)
            .expect("pread");
        self.probes.set(self.probes.get() + 1);
        buf.as_chunks::<8>()
            .0
            .iter()
            .map(|c| u64::from_le_bytes(*c))
            .collect()
    }

    fn disk_has(&self, k: Key) -> bool {
        if self.n_disk == 0 {
            return false;
        }
        let b = self.sparse.partition_point(|&s| s <= k);
        if b == 0 {
            return false;
        }
        self.read_block(b - 1).binary_search(&k).is_ok()
    }

    pub fn has(&self, k: Key) -> bool {
        if self.added.contains(&k) {
            return true;
        }
        if self.removed.contains(&k) {
            return false;
        }
        self.disk_has(k)
    }

    /// Believed keys in `r`, sorted.
    pub fn keys_in(&self, r: &Range) -> Vec<Key> {
        let mut out = Vec::new();
        if self.n_disk > 0 {
            let b0 = self
                .sparse
                .partition_point(|&s| s <= r.lo())
                .saturating_sub(1);
            let b1 = self.sparse.partition_point(|&s| s <= r.hi());
            for b in b0..b1.min(self.sparse.len()) {
                out.extend(
                    self.read_block(b)
                        .into_iter()
                        .filter(|&k| r.contains(k) && !self.removed.contains(&k)),
                );
            }
        }
        out.extend(self.added.iter().copied().filter(|&k| r.contains(k)));
        out.sort_unstable();
        out.dedup();
        out
    }

    fn set_add(&mut self, k: Key) {
        if !self.removed.remove(&k) {
            self.added.insert(k);
        }
    }
    fn set_del(&mut self, k: Key) {
        if !self.added.remove(&k) {
            self.removed.insert(k);
        }
    }

    fn maybe_merge(&mut self) {
        if self.added.len() + self.removed.len() < self.merge_at {
            return;
        }
        let mut all: Vec<Key> = Vec::with_capacity(self.n_disk + self.added.len());
        let mut buf = vec![0u8; self.n_disk * 8];
        self.file.read_exact_at(&mut buf, 0).expect("read run");
        for c in buf.as_chunks::<8>().0 {
            let k = u64::from_le_bytes(*c);
            if !self.removed.contains(&k) {
                all.push(k);
            }
        }
        all.extend(self.added.iter().copied());
        all.sort_unstable();
        all.dedup();
        self.added.clear();
        self.removed.clear();
        self.merges += 1;
        self.write_run(&all);
    }

    pub fn mem_bytes(&self) -> usize {
        self.sparse.capacity() * 8 + (self.added.len() + self.removed.len()) * 48
    }
}

impl Drop for DiskExact {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub struct Hybrid {
    c: CuckooSet,
    d: DiskExact,
    total: Acc,
    seq: SeqState,
    pub local_grows: u64,
}

impl Hybrid {
    pub fn from_sorted(keys: &[Key], owner_n: usize, f: u32, target: usize) -> Self {
        Self {
            c: CuckooSet::from_sorted(keys, owner_n, f, target),
            d: DiskExact::from_sorted(keys),
            total: Acc::of(keys),
            seq: SeqState::default(),
            local_grows: 0,
        }
    }

    pub fn disk(&self) -> &DiskExact {
        &self.d
    }

    fn regrow(&mut self) {
        for r in self.c.dirty_ranges() {
            let ks = self.d.keys_in(&r);
            self.c.replace_range(&r, &ks);
            self.local_grows += 1;
        }
    }
}

impl LocationMirror for Hybrid {
    fn label(&self) -> String {
        "C8a cuckoo over disk exact".into()
    }
    fn seq_view(&self) -> SeqState {
        self.seq
    }
    fn seq(&mut self) -> &mut SeqState {
        &mut self.seq
    }
    fn len(&self) -> usize {
        self.total.count as usize
    }
    fn root_fp(&self) -> Fingerprint {
        self.total.fingerprint()
    }
    fn contains(&self, k: Key) -> bool {
        self.c.contains(k) && self.d.has(k)
    }
    fn add(&mut self, k: Key, exact: bool) -> bool {
        if !exact && self.contains(k) {
            return true;
        }
        if !self.c.add(k, true) {
            self.regrow();
            self.c.add(k, true);
        }
        self.d.set_add(k);
        self.total.add(k);
        self.regrow();
        self.d.maybe_merge();
        true
    }
    fn del(&mut self, k: Key, exact: bool) {
        if !exact && !self.contains(k) {
            return;
        }
        self.c.del(k, true);
        self.d.set_del(k);
        self.total.sub(k);
        self.d.maybe_merge();
    }
    fn acc(&self, r: &Range) -> (Fingerprint, u64) {
        if r.bits == 0 {
            return (self.total.fingerprint(), self.total.count);
        }
        if r.bits <= self.c.shard_bits() {
            return self.c.acc(r);
        }
        let a = Acc::of(&self.d.keys_in(r));
        (a.fingerprint(), a.count)
    }
    fn replace_range(&mut self, r: &Range, keys: &[Key]) {
        let old = self.d.keys_in(r);
        self.total.unmerge(&Acc::of(&old));
        self.total.merge(&Acc::of(keys));
        for &k in &old {
            if keys.binary_search(&k).is_err() {
                self.d.set_del(k);
            }
        }
        for &k in keys {
            if old.binary_search(&k).is_err() {
                self.d.set_add(k);
            }
        }
        let sb = self.c.shard_bits();
        let (a, b) = span(r, sb);
        for i in a..=b {
            let sr = Range {
                bits: sb,
                path: i as u64,
            };
            let ks = self.d.keys_in(&sr);
            self.c.replace_range(&sr, &ks);
        }
        self.d.maybe_merge();
    }
    fn self_bytes(&self) -> usize {
        self.c.self_bytes() + self.d.mem_bytes()
    }
    fn keys(&self) -> Option<Vec<Key>> {
        Some(self.d.keys_in(&Range::ROOT))
    }
}
