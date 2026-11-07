//! C6a: the exact mirror with each shard stored Elias-Fano coded
//! (implicit shard prefix, low bits packed, high bits unary). Same
//! protocol, same repair, zero false positives; lookups decode a shard.

use super::{ideal_bits, shard_idx, span};
use crate::acc::Acc;
use crate::mirror::{LocationMirror, SeqState};
use constellation_net::reconcile::{Fingerprint, Key, Range};

const SHARD_TARGET: usize = 64;

struct EfShard {
    acc: Acc,
    n: u32,
    l: u8,
    data: Box<[u8]>,
}

fn get_bits(buf: &[u8], pos: usize, len: u32) -> u64 {
    if len == 0 {
        return 0;
    }
    let first = pos / 8;
    let end = (pos + len as usize).div_ceil(8);
    let mut v: u128 = 0;
    for (i, b) in buf[first..end].iter().enumerate() {
        v |= u128::from(*b) << (8 * i);
    }
    let mask = if len >= 64 {
        u64::MAX
    } else {
        (1u64 << len) - 1
    };
    ((v >> (pos % 8)) as u64) & mask
}

fn put_bits(buf: &mut [u8], pos: usize, len: u32, x: u64) {
    if len == 0 {
        return;
    }
    let first = pos / 8;
    let end = (pos + len as usize).div_ceil(8);
    let v = u128::from(x) << (pos % 8);
    for i in 0..(end - first) {
        buf[first + i] |= (v >> (8 * i)) as u8;
    }
}

fn ceil_log2(n: usize) -> u32 {
    if n <= 1 {
        0
    } else {
        usize::BITS - (n - 1).leading_zeros()
    }
}

/// Encode sorted suffixes below `2^w`.
fn encode(w: u32, sfx: &[u64]) -> (u8, Box<[u8]>) {
    let n = sfx.len();
    if n == 0 {
        return (0, Box::new([]));
    }
    let l = w.saturating_sub(ceil_log2(n));
    let max_h = if l >= 64 { 0 } else { sfx[n - 1] >> l };
    let total = n * l as usize + n + max_h as usize + 1;
    let mut buf = vec![0u8; total.div_ceil(8)];
    let base = n * l as usize;
    for (i, &s) in sfx.iter().enumerate() {
        let lo = if l >= 64 { s } else { s & ((1u64 << l) - 1) };
        put_bits(&mut buf, i * l as usize, l, lo);
        let h = if l >= 64 { 0 } else { s >> l } as usize;
        let p = base + h + i;
        buf[p / 8] |= 1 << (p % 8);
    }
    (l as u8, buf.into_boxed_slice())
}

struct EfIter<'a> {
    buf: &'a [u8],
    n: usize,
    l: u32,
    i: usize,
    base: usize,
    hi_pos: usize,
}

impl Iterator for EfIter<'_> {
    type Item = u64;
    fn next(&mut self) -> Option<u64> {
        if self.i >= self.n {
            return None;
        }
        let mut p = self.hi_pos;
        loop {
            let byte = self.buf[p / 8] >> (p % 8);
            if byte != 0 {
                p += byte.trailing_zeros() as usize;
                break;
            }
            p = (p / 8 + 1) * 8;
        }
        self.hi_pos = p + 1;
        let high = (p - self.base - self.i) as u64;
        let lo = get_bits(self.buf, self.i * self.l as usize, self.l);
        self.i += 1;
        Some(if self.l >= 64 {
            lo
        } else {
            (high << self.l) | lo
        })
    }
}

impl EfShard {
    fn iter(&self) -> EfIter<'_> {
        EfIter {
            buf: &self.data,
            n: self.n as usize,
            l: u32::from(self.l),
            i: 0,
            base: self.n as usize * self.l as usize,
            hi_pos: self.n as usize * self.l as usize,
        }
    }
}

pub struct EfSet {
    sb: u8,
    shards: Vec<EfShard>,
    total: Acc,
    seq: SeqState,
}

impl EfSet {
    fn w(&self) -> u32 {
        64 - u32::from(self.sb)
    }
    fn prefix(&self, i: usize) -> u64 {
        if self.sb == 0 {
            0
        } else {
            (i as u64) << self.w()
        }
    }
    fn suffix_mask(&self) -> u64 {
        if self.sb == 0 {
            u64::MAX
        } else {
            (1u64 << self.w()) - 1
        }
    }

    pub fn from_sorted(keys: &[Key], target: usize) -> Self {
        let sb = ideal_bits(keys.len(), target.max(1));
        let mut s = Self {
            sb,
            shards: Vec::new(),
            total: Acc::default(),
            seq: SeqState::default(),
        };
        let mut pos = 0usize;
        for i in 0..(1usize << sb) {
            let start = pos;
            while pos < keys.len() && shard_idx(keys[pos], sb) == i {
                pos += 1;
            }
            let part = &keys[start..pos];
            s.shards.push(s.make_shard(part));
        }
        s.total = Acc::of(keys);
        s
    }

    fn make_shard(&self, full: &[Key]) -> EfShard {
        let m = self.suffix_mask();
        let sfx: Vec<u64> = full.iter().map(|k| k & m).collect();
        let (l, data) = encode(self.w(), &sfx);
        EfShard {
            acc: Acc::of(full),
            n: full.len() as u32,
            l,
            data,
        }
    }

    fn shard_keys(&self, i: usize) -> Vec<Key> {
        let p = self.prefix(i);
        self.shards[i].iter().map(|s| p | s).collect()
    }

    fn put(&mut self, i: usize, keys: &[Key]) {
        let old = self.shards[i].acc;
        let sh = self.make_shard(keys);
        self.total.unmerge(&old);
        self.total.merge(&sh.acc);
        self.shards[i] = sh;
    }

    fn maybe_resize(&mut self) {
        let n = self.total.count as usize;
        let shards = self.shards.len();
        let grow = n > shards * SHARD_TARGET * 2 && self.sb < 16;
        let shrink = self.sb > 0 && n < shards * SHARD_TARGET / 4;
        if grow || shrink {
            let all: Vec<Key> = (0..shards).flat_map(|i| self.shard_keys(i)).collect();
            let seq = self.seq;
            *self = Self::from_sorted(&all, SHARD_TARGET);
            self.seq = seq;
        }
    }
}

impl LocationMirror for EfSet {
    fn label(&self) -> String {
        "C6a EF-coded exact".into()
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
        let s = k & self.suffix_mask();
        for v in sh.iter() {
            if v >= s {
                return v == s;
            }
        }
        false
    }
    fn add(&mut self, k: Key, _exact: bool) -> bool {
        let i = shard_idx(k, self.sb);
        let mut ks = self.shard_keys(i);
        if let Err(p) = ks.binary_search(&k) {
            ks.insert(p, k);
            self.put(i, &ks);
            self.maybe_resize();
        }
        true
    }
    fn del(&mut self, k: Key, _exact: bool) {
        let i = shard_idx(k, self.sb);
        let mut ks = self.shard_keys(i);
        if let Ok(p) = ks.binary_search(&k) {
            ks.remove(p);
            self.put(i, &ks);
            self.maybe_resize();
        }
    }
    fn acc(&self, r: &Range) -> (Fingerprint, u64) {
        if r.bits == 0 {
            return (self.total.fingerprint(), self.total.count);
        }
        let (a, b) = span(r, self.sb);
        let acc = if r.bits <= self.sb {
            let mut acc = Acc::default();
            for sh in &self.shards[a..=b] {
                acc.merge(&sh.acc);
            }
            acc
        } else {
            let ks: Vec<Key> = self
                .shard_keys(a)
                .into_iter()
                .filter(|&k| k >= r.lo() && k <= r.hi())
                .collect();
            Acc::of(&ks)
        };
        (acc.fingerprint(), acc.count)
    }
    fn replace_range(&mut self, r: &Range, keys: &[Key]) {
        let (a, b) = span(r, self.sb);
        for i in a..=b {
            let mut ks: Vec<Key> = self
                .shard_keys(i)
                .into_iter()
                .filter(|&k| !r.contains(k))
                .collect();
            ks.extend(keys.iter().copied().filter(|&k| shard_idx(k, self.sb) == i));
            ks.sort_unstable();
            ks.dedup();
            self.put(i, &ks);
        }
        self.maybe_resize();
    }
    fn self_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.data.len() + std::mem::size_of::<EfShard>())
            .sum()
    }
    fn keys(&self) -> Option<Vec<Key>> {
        Some(
            (0..self.shards.len())
                .flat_map(|i| self.shard_keys(i))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::{peer_keys, Rng};

    #[test]
    fn ef_roundtrip_and_ops() {
        for &n in &[0usize, 1, 5, 64, 1000, 20_000] {
            let keys = peer_keys(3, 1, n);
            let s = EfSet::from_sorted(&keys, 64);
            assert_eq!(s.keys().unwrap(), keys);
            for &k in keys.iter().take(500) {
                assert!(s.contains(k));
            }
            let mut r = Rng::new(9);
            for _ in 0..500 {
                let k = r.next();
                assert_eq!(s.contains(k), keys.binary_search(&k).is_ok());
            }
        }
        // sb = 0 and a single huge value
        let s = EfSet::from_sorted(&[7, u64::MAX], 64);
        assert!(s.contains(u64::MAX) && s.contains(7) && !s.contains(8));
    }
}
