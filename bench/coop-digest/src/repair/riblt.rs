//! Rateless IBLT (Yang, Gilad, Alizadeh, SIGCOMM 2024) over u64 keys.
//!
//! The owner streams coded symbols of its whole set; the receiver
//! builds the same symbols over its own keys, subtracts, and peels.
//! Item `x` appears in symbol `j` with probability `1 / (1 + j/2)`;
//! the next index after `i` is drawn by inverting that survival
//! function, `gap = ceil((i + 1.5) * (2^16 / sqrt(u + 1) - 1))` for a
//! 32-bit uniform `u` (the reference implementation's mapping).
//! A symbol is `(count, xor of keys, xor of 32-bit key checksums)`; on
//! the wire the count is the deviation from `N / (1 + j/2)` (the
//! receiver knows `N` from the owner's summary), as a zigzag varint.

use crate::mirror::{LocationMirror, RepairStats};
use crate::util::mix64;
use constellation_net::message::SIGNED_ENVELOPE;
use constellation_net::reconcile::KeySet;
use serde::Serialize;
use std::time::Instant;

pub fn chk(x: u64) -> u32 {
    (mix64(x ^ 0x517C_C1B7_2722_0A95) >> 32) as u32
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct Cell {
    pub count: i64,
    pub sum: u64,
    pub chk: u32,
}

impl Cell {
    fn is_zero(&self) -> bool {
        self.count == 0 && self.sum == 0 && self.chk == 0
    }
}

#[derive(Clone, Copy)]
struct Map {
    prng: u64,
    next: u64,
}

impl Map {
    fn new(x: u64) -> Self {
        Map {
            prng: mix64(x ^ 0xA076_1D64_78BD_642F) | 1,
            next: 0,
        }
    }
    fn advance(&mut self) {
        let r = self.prng.wrapping_mul(0xDA94_2042_E4DD_58B5);
        self.prng = r;
        let u = (r >> 32) as f64 + 1.0;
        let g = ((self.next as f64 + 1.5) * (65536.0 / u.sqrt() - 1.0)).ceil();
        self.next += (g as u64).max(1);
    }
}

pub struct Encoder {
    items: Vec<(u64, Map)>,
    pub cells: Vec<Cell>,
}

impl Encoder {
    pub fn new(keys: impl Iterator<Item = u64>) -> Self {
        Self {
            items: keys.map(|x| (x, Map::new(x))).collect(),
            cells: Vec::new(),
        }
    }
    pub fn extend_to(&mut self, m: usize) {
        if m <= self.cells.len() {
            return;
        }
        self.cells.resize(m, Cell::default());
        let m = m as u64;
        for (x, map) in &mut self.items {
            while map.next < m {
                let c = &mut self.cells[map.next as usize];
                c.count += 1;
                c.sum ^= *x;
                c.chk ^= chk(*x);
                map.advance();
            }
        }
    }
}

pub struct Decoder {
    local: Encoder,
    diff: Vec<Cell>,
    decoded: Vec<(u64, i64, Map)>,
    nonzero: usize,
    /// Keys the owner has and the mirror lacks.
    pub added: Vec<u64>,
    /// Keys the mirror has and the owner lacks.
    pub removed: Vec<u64>,
}

impl Decoder {
    pub fn new(mirror_keys: impl Iterator<Item = u64>) -> Self {
        Self {
            local: Encoder::new(mirror_keys),
            diff: Vec::new(),
            decoded: Vec::new(),
            nonzero: 0,
            added: Vec::new(),
            removed: Vec::new(),
        }
    }

    pub fn done(&self) -> bool {
        !self.diff.is_empty() && self.nonzero == 0
    }

    fn modify(&mut self, idx: usize, x: u64, dir: i64) {
        let was = self.diff[idx].is_zero();
        let c = &mut self.diff[idx];
        c.count -= dir;
        c.sum ^= x;
        c.chk ^= chk(x);
        let now = c.is_zero();
        if was && !now {
            self.nonzero += 1;
        } else if !was && now {
            self.nonzero -= 1;
        }
    }

    /// Owner symbols `m0..m0+cells.len()`, where `m0` is the number
    /// already pushed.
    pub fn push(&mut self, cells: &[Cell]) {
        let m0 = self.diff.len();
        let m1 = m0 + cells.len();
        self.local.extend_to(m1);
        let mut stack: Vec<usize> = Vec::new();
        for (j, c) in cells.iter().enumerate() {
            let l = self.local.cells[m0 + j];
            let d = Cell {
                count: c.count - l.count,
                sum: c.sum ^ l.sum,
                chk: c.chk ^ l.chk,
            };
            if !d.is_zero() {
                self.nonzero += 1;
            }
            self.diff.push(d);
            stack.push(m0 + j);
        }
        // Earlier decoded items also sit in the new symbols.
        let mut decoded = std::mem::take(&mut self.decoded);
        for (x, dir, map) in &mut decoded {
            while (map.next as usize) < m1 {
                self.modify(map.next as usize, *x, *dir);
                stack.push(map.next as usize);
                map.advance();
            }
        }
        self.decoded = decoded;
        while let Some(j) = stack.pop() {
            let c = self.diff[j];
            if (c.count == 1 || c.count == -1) && chk(c.sum) == c.chk {
                let (x, dir) = (c.sum, c.count);
                if dir == 1 {
                    self.added.push(x);
                } else {
                    self.removed.push(x);
                }
                let mut map = Map::new(x);
                while (map.next as usize) < m1 {
                    let idx = map.next as usize;
                    self.modify(idx, x, dir);
                    if idx != j {
                        stack.push(idx);
                    }
                    map.advance();
                }
                self.decoded.push((x, dir, map));
            }
        }
    }
}

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}
fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

fn expected(n: u64, j: usize) -> i64 {
    (n as f64 / (1.0 + j as f64 / 2.0)).round() as i64
}

/// Wire encoding of owner symbols `start..` for a set of `n` keys.
pub fn encode_cells(start: usize, cells: &[Cell], n: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(cells.len() * 14);
    for (i, c) in cells.iter().enumerate() {
        let mut v = zigzag(c.count - expected(n, start + i));
        while v >= 0x80 {
            out.push((v as u8) | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
        out.extend_from_slice(&c.sum.to_le_bytes());
        out.extend_from_slice(&c.chk.to_le_bytes());
    }
    out
}

pub fn decode_cells(start: usize, buf: &[u8], n: u64) -> Vec<Cell> {
    let mut out = Vec::new();
    let mut p = 0usize;
    while p < buf.len() {
        let mut v = 0u64;
        let mut sh = 0;
        loop {
            let b = buf[p];
            p += 1;
            v |= u64::from(b & 0x7F) << sh;
            if b & 0x80 == 0 {
                break;
            }
            sh += 7;
        }
        let sum = u64::from_le_bytes(buf[p..p + 8].try_into().unwrap());
        let ck = u32::from_le_bytes(buf[p + 8..p + 12].try_into().unwrap());
        p += 12;
        out.push(Cell {
            count: unzigzag(v) + expected(n, start + out.len()),
            sum,
            chk: ck,
        });
    }
    out
}

#[derive(Serialize)]
struct Batch<'a> {
    start: u32,
    cells: &'a [u8],
}

#[derive(Serialize)]
struct Request {
    have: u32,
    want: u32,
}

fn framed<T: Serialize>(t: &T) -> usize {
    postcard::to_allocvec(t).map(|v| v.len()).unwrap_or(0) + SIGNED_ENVELOPE
}

/// Repair `m` (which must enumerate its keys) from `owner` with RIBLT.
/// Batches start at `first` symbols and double up to `cap` per frame,
/// one round trip each. Reports the stats; `Ok` means the mirror ends
/// equal to the owner.
pub fn riblt_repair<M: LocationMirror + ?Sized>(
    m: &mut M,
    owner: &KeySet,
    first: usize,
    cap: usize,
) -> (RepairStats, bool) {
    let mut st = RepairStats::default();
    let n = owner.len() as u64;
    let t0 = Instant::now();
    let Some(mkeys) = m.keys() else {
        st.failed = true;
        return (st, false);
    };
    let mut dec = Decoder::new(mkeys.into_iter());
    st.recv_ns += t0.elapsed().as_nanos() as u64;
    let t1 = Instant::now();
    let mut enc = Encoder::new(owner.iter());
    st.owner_ns += t1.elapsed().as_nanos() as u64;
    let mut have = 0usize;
    let mut batch = first;
    while !dec.done() {
        if st.rounds > 4000 {
            st.failed = true;
            return (st, false);
        }
        let t = Instant::now();
        enc.extend_to(have + batch);
        let bytes = encode_cells(have, &enc.cells[have..have + batch], n);
        st.owner_ns += t.elapsed().as_nanos() as u64;
        st.req_bytes += framed(&Request {
            have: have as u32,
            want: batch as u32,
        });
        st.reply_bytes += framed(&Batch {
            start: have as u32,
            cells: &bytes,
        });
        st.rounds += 1;
        let t = Instant::now();
        let cells = decode_cells(have, &bytes, n);
        dec.push(&cells);
        st.recv_ns += t.elapsed().as_nanos() as u64;
        have += batch;
        batch = (batch * 2).min(cap);
    }
    let t = Instant::now();
    for &k in &dec.removed {
        m.del(k, false);
    }
    for &k in &dec.added {
        m.add(k, false);
    }
    st.recv_ns += t.elapsed().as_nanos() as u64;
    st.items_keys = dec.added.len() + dec.removed.len();
    let ok = m.root_fp() == owner.root_fingerprint() && m.len() == owner.len();
    (st, ok)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cand::exact::Exact;
    use crate::util::{peer_keys, Rng};

    #[test]
    fn riblt_repairs_small_and_large_differences() {
        for &d in &[1usize, 10, 1000, 20_000] {
            let base = peer_keys(1, 1, 200_000);
            let mut owner = KeySet::from_keys(base.iter().copied());
            let mut r = Rng::new(d as u64);
            for i in 0..d {
                if i % 2 == 0 {
                    owner.insert(r.next());
                } else {
                    owner.remove(base[r.below(base.len() as u64) as usize]);
                }
            }
            let mut m = Exact::from_keys(&base);
            let (st, ok) = riblt_repair(&mut m, &owner, 32, 2048);
            assert!(ok && !st.failed, "d={d}");
            let diffs = st.items_keys;
            eprintln!(
                "d={d} diffs={diffs} bytes={} rounds={}",
                st.bytes(),
                st.rounds
            );
        }
    }
}
