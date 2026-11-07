//! C6b: implicit shard prefix plus a truncated suffix per key. The
//! shard's exact accumulator is maintained from the full keys in
//! deltas and items, so the fingerprint chain and RBSR down to shard
//! depth work unchanged; membership is approximate (`w` suffix bits
//! after the shard prefix). Duplicate suffixes are stored as repeated
//! entries, which is the per-suffix refcount.

use super::{ideal_bits, shard_idx, span};
use crate::acc::Acc;
use crate::mirror::{LocationMirror, SeqState};
use constellation_net::reconcile::{Fingerprint, Key, Range};

struct SfxShard {
    acc: Acc,
    v: Vec<u8>,
}

pub struct SuffixSet {
    sb: u8,
    wb: usize,
    shards: Vec<SfxShard>,
    total: Acc,
    seq: SeqState,
}

impl SuffixSet {
    pub fn from_sorted(keys: &[Key], owner_n: usize, wb: usize, target: usize) -> Self {
        let sb = ideal_bits(owner_n.max(keys.len()), target.max(1));
        let mut s = Self {
            sb,
            wb,
            shards: Vec::with_capacity(1 << sb),
            total: Acc::default(),
            seq: SeqState::default(),
        };
        let mut pos = 0usize;
        for i in 0..(1usize << sb) {
            let start = pos;
            while pos < keys.len() && shard_idx(keys[pos], sb) == i {
                pos += 1;
            }
            s.shards.push(s.make_shard(&keys[start..pos]));
        }
        s.total = Acc::of(keys);
        s
    }

    pub fn shard_bits(&self) -> u8 {
        self.sb
    }

    fn sfx(&self, k: Key) -> u64 {
        (k << self.sb) >> (64 - 8 * self.wb)
    }

    fn make_shard(&self, full: &[Key]) -> SfxShard {
        let mut sf: Vec<u64> = full.iter().map(|&k| self.sfx(k)).collect();
        sf.sort_unstable();
        let mut v = Vec::with_capacity(sf.len() * self.wb);
        for x in sf {
            v.extend_from_slice(&x.to_be_bytes()[8 - self.wb..]);
        }
        SfxShard {
            acc: Acc::of(full),
            v,
        }
    }

    fn entry(&self, v: &[u8], i: usize) -> u64 {
        let mut x = 0u64;
        for b in &v[i * self.wb..(i + 1) * self.wb] {
            x = (x << 8) | u64::from(*b);
        }
        x
    }

    /// `(position of the first entry >= s, whether it equals s)`.
    fn lower_bound(&self, v: &[u8], s: u64) -> (usize, bool) {
        let n = v.len() / self.wb;
        let (mut lo, mut hi) = (0usize, n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.entry(v, mid) < s {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        (lo, lo < n && self.entry(v, lo) == s)
    }
}

impl LocationMirror for SuffixSet {
    fn label(&self) -> String {
        format!("C6b suffix w={}", self.wb * 8)
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
        let sh = &self.shards[shard_idx(k, self.sb)];
        self.lower_bound(&sh.v, self.sfx(k)).1
    }
    fn add(&mut self, k: Key, exact: bool) -> bool {
        let i = shard_idx(k, self.sb);
        let s = self.sfx(k);
        let (pos, found) = self.lower_bound(&self.shards[i].v, s);
        if found && !exact {
            return true;
        }
        let wb = self.wb;
        let bytes = s.to_be_bytes();
        let sh = &mut self.shards[i];
        sh.v.splice(pos * wb..pos * wb, bytes[8 - wb..].iter().copied());
        sh.acc.add(k);
        self.total.add(k);
        true
    }
    fn del(&mut self, k: Key, _exact: bool) {
        let i = shard_idx(k, self.sb);
        let (pos, found) = self.lower_bound(&self.shards[i].v, self.sfx(k));
        if found {
            let wb = self.wb;
            let sh = &mut self.shards[i];
            sh.v.drain(pos * wb..(pos + 1) * wb);
            sh.acc.sub(k);
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
            let new = self.make_shard(&keys[lo..hi]);
            self.total.unmerge(&self.shards[i].acc);
            self.total.merge(&new.acc);
            self.shards[i] = new;
        }
    }
    fn self_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.v.capacity() + std::mem::size_of::<SfxShard>())
            .sum()
    }
}
