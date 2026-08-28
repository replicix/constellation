//! Compact bloom filter over chunk hashes (DESIGN.md §7).
//!
//! Parameters are chosen for ~1% FPR at the advertised load:
//! 10 bits per entry and k = 7 hashes (ln(2) × 10 ≈ 6.9). Membership
//! tests are local — a false positive only wastes one peer round-trip
//! before the requester falls back to S3.
//!
//! A single filter is capped at [`MAX_BITS_BYTES`] so a gossip frame
//! still fits. Caches larger than that (~13k chunks, ~52 GiB at 4 MiB)
//! are split into [`bucket_count_for`] prefix buckets, each its own
//! message. A 4 TiB cache is 128 buckets × 16 KiB ≈ 2 MiB of RSS at
//! the receiver and a background trickle of one bucket per digest
//! interval, not a 1.25 MiB flood every 30 s.
//!
//! Hashing is double-hashing over the blake3 chunk identity itself
//! (`h_i = h1 + i·h2 mod m`). No extra dependency; the chunk hash is
//! already uniformly distributed.

use serde::{Deserialize, Serialize};

/// 32-byte chunk identity (blake3 of plaintext). Kept as a raw array so
/// this crate does not depend on `fs-core`.
pub type Hash = [u8; 32];

/// Bits per inserted element. Together with [`K`] this is the DESIGN.md
/// "~10 bits/entry" budget.
pub const BITS_PER_ENTRY: usize = 10;
/// Independent probes. Optimal k ≈ ln(2)·(m/n) ≈ 7 at 10 bits/entry.
pub const K: u32 = 7;
/// One raw gossiped bucket stays inside
/// [`crate::message::GOSSIP_CONTENT_LIMIT`] with signed-envelope
/// overhead. Larger caches split into this many-entry slices rather
/// than saturating one filter — see [`bucket_count_for`].
pub const MAX_BITS_BYTES: usize = 16 * 1024;
/// Entries that fill one bucket at [`BITS_PER_ENTRY`] without raising FPR.
pub const ENTRIES_PER_BUCKET: usize = (MAX_BITS_BYTES * 8) / BITS_PER_ENTRY;
/// Receiver and sender ceiling. 256 × 16 KiB = 4 MiB, ~3.3M chunks
/// (~13 TiB at 4 MiB) before FPR starts to climb.
pub const MAX_BUCKETS: u32 = 256;
/// Reject a wire `k` outside this range: a peer sending `k = u32::MAX`
/// would otherwise turn every `contains` into a multi-billion loop.
const MIN_K: u32 = 1;
const MAX_K: u32 = 32;

/// How many hash-prefix buckets a cache of `n` chunks needs so each
/// gossip message stays at [`MAX_BITS_BYTES`]. Power of two so the
/// index is a mask. A 4 TiB cache (1M × 4 MiB chunks) lands on 128.
pub fn bucket_count_for(n: usize) -> u32 {
    let need = n.div_ceil(ENTRIES_PER_BUCKET).max(1) as u32;
    need.next_power_of_two().min(MAX_BUCKETS)
}

/// Which bucket of `buckets` holds `hash`. Each peer picks its own
/// `buckets` from its own cache size; the receiver must use *that*
/// peer's count, not a fleet-wide constant.
pub fn bucket_index(hash: &Hash, buckets: u32) -> u32 {
    let n = buckets.max(1);
    let hi = u32::from_be_bytes(hash[0..4].try_into().unwrap());
    if n.is_power_of_two() {
        hi & (n - 1)
    } else {
        hi % n
    }
}

/// A bloom filter of chunk hashes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bloom {
    /// Packed bits, little-endian within each byte.
    pub bits: Vec<u8>,
    pub nbits: u64,
    pub k: u32,
    /// Inserted count (for resize/FPR diagnostics), not a capacity.
    pub n: u64,
}

impl Bloom {
    /// Empty filter sized for `n` expected entries.
    pub fn with_capacity(n: usize) -> Self {
        let want = n.max(1).saturating_mul(BITS_PER_ENTRY).max(64);
        let nbits = want.min(MAX_BITS_BYTES * 8) as u64;
        let nbytes = nbits.div_ceil(8) as usize;
        Self {
            bits: vec![0u8; nbytes],
            nbits,
            k: K,
            n: 0,
        }
    }

    pub fn from_hashes(hashes: &[Hash]) -> Self {
        let mut b = Self::with_capacity(hashes.len());
        for h in hashes {
            b.insert(h);
        }
        b
    }

    pub fn insert(&mut self, hash: &Hash) {
        for i in self.indexes(hash) {
            let byte = (i / 8) as usize;
            let bit = (i % 8) as u8;
            if let Some(slot) = self.bits.get_mut(byte) {
                *slot |= 1 << bit;
            }
        }
        self.n = self.n.saturating_add(1);
    }

    pub fn contains(&self, hash: &Hash) -> bool {
        if self.nbits == 0 {
            return false;
        }
        self.indexes(hash).all(|i| {
            let byte = (i / 8) as usize;
            let bit = (i % 8) as u8;
            self.bits
                .get(byte)
                .is_some_and(|slot| slot & (1 << bit) != 0)
        })
    }

    /// Apply an add-only delta (removals are not encoded; they just
    /// raise FPR until the next full snapshot).
    pub fn add_all(&mut self, hashes: &[Hash]) {
        for h in hashes {
            self.insert(h);
        }
    }

    fn indexes(&self, hash: &Hash) -> impl Iterator<Item = u64> {
        let m = self.nbits.max(1);
        let h1 = u64::from_le_bytes(hash[0..8].try_into().unwrap());
        let mut h2 = u64::from_le_bytes(hash[8..16].try_into().unwrap());
        if h2 % 2 == 0 {
            h2 = h2.wrapping_add(1);
        }
        let k = self.k;
        (0..k).map(move |i| h1.wrapping_add((i as u64).wrapping_mul(h2)) % m)
    }

    /// Validate a compact-wire bit vector before admitting it to the
    /// per-peer digest map.
    pub fn from_wire_bytes(nbits: u64, k: u32, n: u64, bits: Vec<u8>) -> Option<Self> {
        if !(MIN_K..=MAX_K).contains(&k) {
            return None;
        }
        if nbits == 0 || nbits > (MAX_BITS_BYTES as u64) * 8 {
            return None;
        }
        let nbytes = nbits.div_ceil(8) as usize;
        if bits.len() != nbytes || bits.len() > MAX_BITS_BYTES {
            return None;
        }
        Some(Self { bits, nbits, k, n })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(n: u8) -> Hash {
        let mut b = [0u8; 32];
        b[0] = n;
        b[31] = n.wrapping_add(1);
        *blake3::hash(&b).as_bytes()
    }

    #[test]
    fn inserted_members_are_found() {
        let mut b = Bloom::with_capacity(64);
        let items: Vec<_> = (0..40).map(h).collect();
        for x in &items {
            b.insert(x);
        }
        for x in &items {
            assert!(b.contains(x), "false negative on {x:?}");
        }
    }

    /// At 10 bits/entry and k=7 the theoretical FPR is ~0.8%. A 2000-
    /// probe sample at n=400 must stay well under 5% or the parameters
    /// are wrong.
    #[test]
    fn fpr_at_target_load_is_about_one_percent() {
        let n = 400usize;
        let mut b = Bloom::with_capacity(n);
        for i in 0..n {
            b.insert(&mix(&h(i as u8), i as u64));
        }
        let mut fp = 0usize;
        let probes = 2000usize;
        for i in 0..probes {
            let miss = mix(&h(255), 10_000 + i as u64);
            if b.contains(&miss) {
                fp += 1;
            }
        }
        let rate = fp as f64 / probes as f64;
        assert!(
            rate < 0.05,
            "FPR {rate:.4} ({fp}/{probes}) is far above the ~1% target"
        );
    }

    #[test]
    fn wire_roundtrip_preserves_membership() {
        let hashes: Vec<_> = (0..20).map(h).collect();
        let b = Bloom::from_hashes(&hashes);
        let again = Bloom::from_wire_bytes(b.nbits, b.k, b.n, b.bits.clone()).unwrap();
        assert_eq!(again.bits, b.bits);
        for x in &hashes {
            assert!(again.contains(x));
        }
    }

    #[test]
    fn add_only_delta_raises_membership() {
        let mut b = Bloom::with_capacity(16);
        b.insert(&h(1));
        b.add_all(&[h(2), h(3)]);
        assert!(b.contains(&h(1)) && b.contains(&h(2)) && b.contains(&h(3)));
    }

    fn mix(h: &Hash, salt: u64) -> Hash {
        let mut b = *h;
        b[16..24].copy_from_slice(&salt.to_le_bytes());
        *blake3::hash(&b).as_bytes()
    }

    #[test]
    fn a_small_cache_is_one_bucket() {
        assert_eq!(bucket_count_for(100), 1);
        assert_eq!(bucket_count_for(ENTRIES_PER_BUCKET), 1);
    }

    /// 20k chunks is already past the old single-frame cap (~13k). The
    /// partitioned layout must keep every bucket inside MAX_BITS_BYTES
    /// or we are back to saturating FPR.
    #[test]
    fn a_cache_past_one_frame_splits_and_each_bucket_fits() {
        let n = 20_000usize;
        let nb = bucket_count_for(n);
        assert!(nb >= 2, "20k entries must not share one 16 KiB filter");
        let hashes: Vec<_> = (0..n as u64).map(|i| mix(&h(1), i)).collect();
        let mut per = vec![0usize; nb as usize];
        for h in &hashes {
            per[bucket_index(h, nb) as usize] += 1;
        }
        for (i, &c) in per.iter().enumerate() {
            let b = Bloom::with_capacity(c);
            assert!(
                b.bits.len() <= MAX_BITS_BYTES,
                "bucket {i} is {} bytes for {c} entries",
                b.bits.len()
            );
        }
        let blooms: Vec<Bloom> = (0..nb)
            .map(|i| {
                let subset: Vec<_> = hashes
                    .iter()
                    .copied()
                    .filter(|h| bucket_index(h, nb) == i)
                    .collect();
                Bloom::from_hashes(&subset)
            })
            .collect();
        for h in &hashes {
            let i = bucket_index(h, nb) as usize;
            assert!(blooms[i].contains(h), "false negative in bucket {i}");
        }
    }

    #[test]
    fn four_tib_at_4mib_chunks_stays_inside_the_bucket_ceiling() {
        let chunks = (4 * 1024usize * 1024 * 1024 * 1024) / (4 * 1024 * 1024);
        let nb = bucket_count_for(chunks);
        assert!(nb <= MAX_BUCKETS, "{nb} buckets for 4 TiB");
        assert!(nb >= 64, "{nb} is too few for 1M chunks");
    }

    #[test]
    fn from_wire_rejects_a_pathological_k() {
        let b = Bloom::from_hashes(&[h(1)]);
        assert!(Bloom::from_wire_bytes(b.nbits, 0, 1, b.bits.clone()).is_none());
        assert!(Bloom::from_wire_bytes(b.nbits, u32::MAX, 1, b.bits.clone()).is_none());
        assert!(Bloom::from_wire_bytes(b.nbits, 7, 1, b.bits).is_some());
    }

    #[test]
    fn from_wire_rejects_an_oversized_bit_vector() {
        let huge = (MAX_BITS_BYTES as u64) * 8 + 8;
        assert!(Bloom::from_wire_bytes(huge, 7, 1, Vec::new()).is_none());
    }

    /// Two peers may advertise different bucket counts for the same
    /// hash; each lookup must use *that* peer's count.
    #[test]
    fn the_same_hash_lands_in_different_buckets_under_different_counts() {
        let h = mix(&h(9), 42);
        let a = bucket_index(&h, 2);
        let b = bucket_index(&h, 4);
        assert!(a < 2 && b < 4);
        assert_eq!(bucket_index(&h, 2), a);
        assert_eq!(bucket_index(&h, 4), b);
    }
}
