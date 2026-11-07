//! C4: a rank-select quotient filter (`qfilter` 0.3.1, resizeable) per
//! shard plus the shard's exact accumulator; the same protocol shape as
//! C3. The filter grows by doubling (one remainder bit per doubling) up
//! to twice the size it was built for; past that it refuses inserts and
//! the shard is refetched, as C3's full shards are.

use super::{ideal_bits, shard_idx, span};
use crate::acc::Acc;
use crate::mirror::{LocationMirror, SeqState};
use crate::util::mix64;
use constellation_net::reconcile::{Fingerprint, Key, Range};
use qfilter::Filter;

#[derive(Debug, Default, Clone, Copy)]
pub struct QuotientStats {
    pub insert_failures: u64,
    pub grow_rebuilds: u64,
    pub repair_rebuilds: u64,
}

struct QShard {
    acc: Acc,
    f: Filter,
    dirty: bool,
}

fn h(k: Key) -> u64 {
    mix64(k ^ 0x5851_F42D_4C95_7F2D)
}

pub struct QuotientSet {
    sb: u8,
    err: f64,
    shards: Vec<QShard>,
    total: Acc,
    dirty: Vec<u32>,
    pub stats: QuotientStats,
    seq: SeqState,
}

impl QuotientSet {
    pub fn from_sorted(keys: &[Key], owner_n: usize, err_bits: u32, target: usize) -> Self {
        let sb = ideal_bits(owner_n.max(keys.len()), target.max(1));
        let mut s = Self {
            sb,
            err: 0.5f64.powi(err_bits as i32),
            shards: Vec::with_capacity(1 << sb),
            total: Acc::of(keys),
            dirty: Vec::new(),
            stats: QuotientStats::default(),
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
        s
    }

    fn build_shard(&self, keys: &[Key]) -> QShard {
        let cap = keys.len().max(8) as u64;
        let mut f = Filter::new_resizeable(cap, cap * 2, self.err).expect("quotient filter shape");
        for &k in keys {
            f.insert_fingerprint(true, h(k))
                .expect("build within capacity");
        }
        QShard {
            acc: Acc::of(keys),
            f,
            dirty: false,
        }
    }

    pub fn shard_bits(&self) -> u8 {
        self.sb
    }
}

impl LocationMirror for QuotientSet {
    fn label(&self) -> String {
        format!("C4 quotient eps=2^-{}", (-self.err.log2()).round() as u32)
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
        self.shards[shard_idx(k, self.sb)]
            .f
            .contains_fingerprint(h(k))
    }
    fn add(&mut self, k: Key, exact: bool) -> bool {
        let i = shard_idx(k, self.sb);
        if !exact && self.shards[i].f.contains_fingerprint(h(k)) {
            return true;
        }
        if self.shards[i].f.insert_fingerprint(true, h(k)).is_err() {
            self.stats.insert_failures += 1;
            if !self.shards[i].dirty {
                self.shards[i].dirty = true;
                self.dirty.push(i as u32);
            }
            return false;
        }
        self.shards[i].acc.add(k);
        self.total.add(k);
        true
    }
    fn del(&mut self, k: Key, _exact: bool) {
        let i = shard_idx(k, self.sb);
        if self.shards[i].f.remove_fingerprint(h(k)) {
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
            if self.shards[i].dirty {
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
            .map(|s| s.f.memory_usage() + std::mem::size_of::<QShard>())
            .sum()
    }
    fn dirty_ranges(&mut self) -> Vec<Range> {
        let sb = self.sb;
        let out = self
            .dirty
            .drain(..)
            .filter(|&i| self.shards[i as usize].dirty)
            .map(|i| Range {
                bits: sb,
                path: u64::from(i),
            })
            .collect();
        out
    }
}
