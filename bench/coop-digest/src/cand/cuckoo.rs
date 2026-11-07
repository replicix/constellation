//! C3: a cuckoo filter per shard (4-way buckets, `f`-bit fingerprints,
//! any bucket count via `alt = (H(fp) - i) mod m`), plus the shard's
//! exact accumulator. The receiver cannot enumerate a filter, so a
//! shard whose filter fills up, or that differs from the owner below
//! shard depth, is refetched as `Items` and rebuilt.

use super::{ideal_bits, shard_idx, span};
use crate::acc::Acc;
use crate::mirror::{LocationMirror, SeqState};
use crate::util::mix64;
use constellation_net::reconcile::{Fingerprint, Key, Range};

const B: usize = 4;
const MAX_KICKS: usize = 500;
/// A shard is built (or rebuilt) with this much headroom.
const BUILD_LOAD: f64 = 0.85;

#[derive(Debug, Default, Clone, Copy)]
pub struct CuckooStats {
    /// Inserts that ended in an eviction chain that found no room (the
    /// displaced fingerprint is parked in the victim slot).
    pub insert_failures: u64,
    /// Inserts refused because the victim slot was already taken.
    pub rejected: u64,
    /// Shards rebuilt because they filled (a grow refetch).
    pub grow_rebuilds: u64,
    /// Shards rebuilt by a repair session.
    pub repair_rebuilds: u64,
}

pub struct CShard {
    acc: Acc,
    m: u32,
    victim: Option<(u32, u32)>,
    in_dirty: bool,
    data: Box<[u8]>,
}

fn slot_get(d: &[u8], f: u32, slot: usize) -> u32 {
    let bit = slot * f as usize;
    let b = bit / 8;
    let v = u64::from_le_bytes(d[b..b + 8].try_into().unwrap());
    ((v >> (bit % 8)) & ((1u64 << f) - 1)) as u32
}

fn slot_set(d: &mut [u8], f: u32, slot: usize, val: u32) {
    let bit = slot * f as usize;
    let b = bit / 8;
    let sh = bit % 8;
    let mask = ((1u64 << f) - 1) << sh;
    let v = u64::from_le_bytes(d[b..b + 8].try_into().unwrap());
    let v = (v & !mask) | (u64::from(val) << sh);
    d[b..b + 8].copy_from_slice(&v.to_le_bytes());
}

fn alt(m: u32, fp: u32, i: u32) -> u32 {
    let hh = mix64(u64::from(fp) ^ 0xC2B2_AE3D_27D4_EB4F);
    let h = ((u64::from(hh as u32) * u64::from(m)) >> 32) as u32;
    let x = h + m - i;
    if x >= m {
        x - m
    } else {
        x
    }
}

fn data_len(m: u32, f: u32) -> usize {
    (m as usize * B * f as usize).div_ceil(8) + 8
}

impl CShard {
    fn with_buckets(m: u32, f: u32) -> Self {
        Self {
            acc: Acc::default(),
            m,
            victim: None,
            in_dirty: false,
            data: vec![0u8; data_len(m, f)].into_boxed_slice(),
        }
    }

    fn hash(&self, k: Key, f: u32) -> (u32, u32, u32) {
        let h = mix64(k ^ 0x1234_5678_9ABC_DEF1);
        let mask = ((1u64 << f) - 1) as u32;
        let mut fp = ((h >> 32) as u32) & mask;
        if fp == 0 {
            fp = 1;
        }
        let i1 = ((u64::from(h as u32) * u64::from(self.m)) >> 32) as u32;
        (fp, i1, alt(self.m, fp, i1))
    }

    fn bucket_has(&self, f: u32, i: u32, fp: u32) -> Option<usize> {
        (0..B).find(|&s| slot_get(&self.data, f, i as usize * B + s) == fp)
    }

    fn contains(&self, k: Key, f: u32) -> bool {
        let (fp, i1, i2) = self.hash(k, f);
        if self.bucket_has(f, i1, fp).is_some() || self.bucket_has(f, i2, fp).is_some() {
            return true;
        }
        self.victim
            .is_some_and(|(vi, vf)| vf == fp && (vi == i1 || vi == i2))
    }

    /// Place `fp` (candidate buckets `i1`/`alt`). `None` on success, else
    /// the fingerprint left without a home.
    fn place(&mut self, f: u32, fp: u32, i1: u32, salt: u64) -> Option<(u32, u32)> {
        let m = self.m;
        let i2 = alt(m, fp, i1);
        for i in [i1, i2] {
            for s in 0..B {
                if slot_get(&self.data, f, i as usize * B + s) == 0 {
                    slot_set(&mut self.data, f, i as usize * B + s, fp);
                    return None;
                }
            }
        }
        let mut cur = fp;
        let mut i = if salt & 1 == 0 { i1 } else { i2 };
        let mut r = salt;
        for _ in 0..MAX_KICKS {
            r = mix64(r);
            let slot = i as usize * B + (r & 3) as usize;
            let old = slot_get(&self.data, f, slot);
            slot_set(&mut self.data, f, slot, cur);
            cur = old;
            i = alt(m, cur, i);
            for s in 0..B {
                if slot_get(&self.data, f, i as usize * B + s) == 0 {
                    slot_set(&mut self.data, f, i as usize * B + s, cur);
                    return None;
                }
            }
        }
        Some((i, cur))
    }

    fn load(&self, count: u64) -> f64 {
        count as f64 / (self.m as f64 * B as f64)
    }
}

pub struct CuckooSet {
    sb: u8,
    f: u32,
    shards: Vec<CShard>,
    total: Acc,
    dirty: Vec<u32>,
    pub stats: CuckooStats,
    seq: SeqState,
}

impl CuckooSet {
    pub fn from_sorted(keys: &[Key], owner_n: usize, f: u32, target: usize) -> Self {
        let sb = ideal_bits(owner_n.max(keys.len()), target.max(1));
        let mut s = Self {
            sb,
            f,
            shards: Vec::with_capacity(1 << sb),
            total: Acc::default(),
            dirty: Vec::new(),
            stats: CuckooStats::default(),
            seq: SeqState::default(),
        };
        let mut pos = 0usize;
        for i in 0..(1usize << sb) {
            let start = pos;
            while pos < keys.len() && shard_idx(keys[pos], sb) == i {
                pos += 1;
            }
            let sh = s.build_shard(&keys[start..pos]);
            s.shards.push(sh);
        }
        s.total = Acc::of(keys);
        s
    }

    pub fn shard_bits(&self) -> u8 {
        self.sb
    }

    pub fn load_of(&self, i: usize) -> f64 {
        self.shards[i].load(self.shards[i].acc.count)
    }

    /// Mean and maximum load factor over non-empty shards.
    pub fn load_stats(&self) -> (f64, f64) {
        let mut sum = 0.0;
        let mut mx: f64 = 0.0;
        let mut c = 0usize;
        for i in 0..self.shards.len() {
            if self.shards[i].acc.count > 0 {
                let l = self.load_of(i);
                sum += l;
                mx = mx.max(l);
                c += 1;
            }
        }
        (sum / c.max(1) as f64, mx)
    }

    fn build_shard(&self, keys: &[Key]) -> CShard {
        let mut m = ((keys.len() as f64 / (B as f64 * BUILD_LOAD)).ceil() as u32).max(1);
        loop {
            let mut sh = CShard::with_buckets(m, self.f);
            let mut ok = true;
            for &k in keys {
                let (fp, i1, _) = sh.hash(k, self.f);
                if sh.place(self.f, fp, i1, k).is_some() {
                    ok = false;
                    break;
                }
            }
            if ok {
                sh.acc = Acc::of(keys);
                return sh;
            }
            m = m + m / 10 + 1;
        }
    }

    fn mark_dirty(&mut self, i: usize) {
        if !self.shards[i].in_dirty {
            self.shards[i].in_dirty = true;
            self.dirty.push(i as u32);
        }
    }
}

impl LocationMirror for CuckooSet {
    fn label(&self) -> String {
        format!("C3 cuckoo f={}", self.f)
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
        self.shards[shard_idx(k, self.sb)].contains(k, self.f)
    }
    fn add(&mut self, k: Key, exact: bool) -> bool {
        let i = shard_idx(k, self.sb);
        let f = self.f;
        if !exact && self.shards[i].contains(k, f) {
            return true;
        }
        if self.shards[i].victim.is_some() {
            self.stats.rejected += 1;
            self.mark_dirty(i);
            return false;
        }
        let (fp, i1, _) = self.shards[i].hash(k, f);
        match self.shards[i].place(f, fp, i1, k) {
            None => {}
            Some(v) => {
                self.shards[i].victim = Some(v);
                self.stats.insert_failures += 1;
                self.mark_dirty(i);
            }
        }
        self.shards[i].acc.add(k);
        self.total.add(k);
        true
    }
    fn del(&mut self, k: Key, _exact: bool) {
        let i = shard_idx(k, self.sb);
        let f = self.f;
        let (fp, i1, i2) = self.shards[i].hash(k, f);
        let mut removed = false;
        for b in [i1, i2] {
            if let Some(s) = self.shards[i].bucket_has(f, b, fp) {
                slot_set(&mut self.shards[i].data, f, b as usize * B + s, 0);
                removed = true;
                break;
            }
        }
        if !removed {
            if let Some((vi, vf)) = self.shards[i].victim {
                if vf == fp && (vi == i1 || vi == i2) {
                    self.shards[i].victim = None;
                    removed = true;
                }
            }
        } else if let Some((vi, vf)) = self.shards[i].victim {
            if self.shards[i].place(f, vf, vi, u64::from(vf)).is_none() {
                self.shards[i].victim = None;
            }
        }
        if removed {
            self.shards[i].acc.sub(k);
            self.total.sub(k);
        }
    }
    fn granularity(&self) -> Option<u8> {
        Some(self.sb)
    }
    fn acc(&self, r: &Range) -> (Fingerprint, u64) {
        if r.bits == 0 {
            return (self.total.fingerprint(), self.total.count);
        }
        let (a, b) = span(r, self.sb);
        let mut acc = Acc::default();
        for sh in &self.shards[a..=b] {
            acc.merge(&sh.acc);
        }
        (acc.fingerprint(), acc.count)
    }
    fn replace_range(&mut self, r: &Range, keys: &[Key]) {
        let (a, b) = span(r, self.sb);
        for i in a..=b {
            let lo = keys.partition_point(|&k| shard_idx(k, self.sb) < i);
            let hi = keys.partition_point(|&k| shard_idx(k, self.sb) <= i);
            let new = self.build_shard(&keys[lo..hi]);
            if self.shards[i].in_dirty {
                self.stats.grow_rebuilds += 1;
            } else {
                self.stats.repair_rebuilds += 1;
            }
            self.total.unmerge(&self.shards[i].acc);
            self.total.merge(&new.acc);
            self.shards[i] = new;
        }
    }
    fn self_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.data.len() + std::mem::size_of::<CShard>())
            .sum()
    }
    fn dirty_ranges(&mut self) -> Vec<Range> {
        let sb = self.sb;
        let out = self
            .dirty
            .drain(..)
            .filter(|&i| self.shards[i as usize].in_dirty)
            .map(|i| Range {
                bits: sb,
                path: u64::from(i),
            })
            .collect();
        out
    }
}

/// A single standalone filter for the load / insert-failure experiments.
pub struct LoneFilter {
    sh: CShard,
    f: u32,
    pub count: u64,
    pub failures: u64,
}

impl LoneFilter {
    pub fn with_slots(slots: usize, f: u32) -> Self {
        Self {
            sh: CShard::with_buckets((slots / B).max(1) as u32, f),
            f,
            count: 0,
            failures: 0,
        }
    }
    pub fn capacity(&self) -> usize {
        self.sh.m as usize * B
    }
    /// True if stored without a failed eviction chain; a failed chain
    /// leaves the displaced item as the victim and later inserts fail.
    pub fn insert(&mut self, k: Key) -> bool {
        if self.sh.victim.is_some() {
            self.failures += 1;
            return false;
        }
        let (fp, i1, _) = self.sh.hash(k, self.f);
        match self.sh.place(self.f, fp, i1, k) {
            None => {
                self.count += 1;
                true
            }
            Some(v) => {
                self.sh.victim = Some(v);
                self.count += 1;
                self.failures += 1;
                false
            }
        }
    }
    pub fn remove(&mut self, k: Key) -> bool {
        let (fp, i1, i2) = self.sh.hash(k, self.f);
        for b in [i1, i2] {
            if let Some(s) = self.sh.bucket_has(self.f, b, fp) {
                slot_set(&mut self.sh.data, self.f, b as usize * B + s, 0);
                self.count -= 1;
                if let Some((vi, vf)) = self.sh.victim {
                    if self.sh.place(self.f, vf, vi, u64::from(vf)).is_none() {
                        self.sh.victim = None;
                    }
                }
                return true;
            }
        }
        false
    }
    pub fn contains(&self, k: Key) -> bool {
        self.sh.contains(k, self.f)
    }
    pub fn has_victim(&self) -> bool {
        self.sh.victim.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::{peer_keys, Rng};

    #[test]
    fn cuckoo_has_no_false_negatives_and_removes() {
        let keys = peer_keys(5, 2, 50_000);
        let mut s = CuckooSet::from_sorted(&keys, keys.len(), 16, 256);
        for &k in &keys {
            assert!(s.contains(k));
        }
        for &k in keys.iter().step_by(2) {
            s.del(k, true);
        }
        for (i, &k) in keys.iter().enumerate() {
            if i % 2 == 1 {
                assert!(s.contains(k), "lost {k}");
            }
        }
        assert_eq!(s.len(), keys.len() / 2);
        let mut r = Rng::new(1);
        let fp = (0..200_000).filter(|_| s.contains(r.next())).count();
        assert!(fp < 200, "fp {fp}");
    }

    #[test]
    fn lone_filter_fills_to_high_load() {
        let mut f = LoneFilter::with_slots(4096, 16);
        let mut r = Rng::new(3);
        let mut n = 0;
        while f.insert(r.next()) {
            n += 1;
        }
        assert!(n as f64 / f.capacity() as f64 > 0.9);
    }
}
