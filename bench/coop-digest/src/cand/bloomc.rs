//! Reference: the existing Bloom digest mode (`constellation_net::bloom`),
//! one filter per hash-prefix bucket, snapshots each digest interval and
//! add-only deltas in between. Removals take effect only at the next
//! snapshot.

use crate::mirror::{LocationMirror, SeqState};
use crate::util::mix64;
use constellation_net::bloom::{bucket_count_for, bucket_index, Bloom, Hash};
use constellation_net::message::{wire_len, Payload};
use constellation_net::reconcile::{
    decode_keys, Delta, DeltaOutcome, Fingerprint, Key, KeySet, Range, Summary,
};

/// A 32-byte "chunk hash" whose first 8 bytes are the key (as
/// `reconcile::key_of` reads it) and whose remaining bytes are
/// deterministic and uniform.
pub fn hash_of(k: Key) -> Hash {
    let mut h = [0u8; 32];
    h[0..8].copy_from_slice(&k.to_be_bytes());
    for i in 1..4 {
        h[i * 8..i * 8 + 8]
            .copy_from_slice(&mix64(k.wrapping_add(i as u64 * 0x9E37_79B9)).to_le_bytes());
    }
    h
}

pub struct BloomMirror {
    buckets: Vec<Bloom>,
    nb: u32,
    n: usize,
    seq: SeqState,
}

impl BloomMirror {
    pub fn from_keys(keys: &[Key]) -> Self {
        let nb = bucket_count_for(keys.len());
        let mut counts = vec![0usize; nb as usize];
        for &k in keys {
            counts[bucket_index(&hash_of(k), nb) as usize] += 1;
        }
        let mut buckets: Vec<Bloom> = counts.iter().map(|&c| Bloom::with_capacity(c)).collect();
        for &k in keys {
            let h = hash_of(k);
            buckets[bucket_index(&h, nb) as usize].insert(&h);
        }
        Self {
            buckets,
            nb,
            n: keys.len(),
            seq: SeqState::default(),
        }
    }

    pub fn bucket_count(&self) -> u32 {
        self.nb
    }

    /// Wire bytes of one full snapshot round (every bucket's
    /// `CacheDigest`), as the real payload.
    pub fn snapshot_round_wire(&self) -> usize {
        self.buckets
            .iter()
            .enumerate()
            .map(|(i, b)| {
                wire_len(&Payload::CacheDigest {
                    node_id: 1,
                    generation: 1,
                    bits: b.bits.clone(),
                    nbits: b.nbits,
                    k: b.k,
                    n: b.n,
                    bucket: i as u32,
                    buckets: self.nb,
                })
            })
            .sum()
    }
}

/// Wire bytes of an add-only delta of `adds` keys.
pub fn delta_wire(adds: &[Key]) -> usize {
    wire_len(&Payload::CacheDigestDelta {
        node_id: 1,
        generation: 1,
        adds: adds.iter().map(|&k| hash_of(k)).collect(),
        buckets: 1,
    })
}

impl LocationMirror for BloomMirror {
    fn label(&self) -> String {
        "Bloom (10 b/key, k=7)".into()
    }
    fn seq_view(&self) -> SeqState {
        self.seq
    }
    fn seq(&mut self) -> &mut SeqState {
        &mut self.seq
    }
    fn len(&self) -> usize {
        self.n
    }
    fn root_fp(&self) -> Fingerprint {
        [0; 16]
    }
    fn contains(&self, k: Key) -> bool {
        let h = hash_of(k);
        self.buckets[bucket_index(&h, self.nb) as usize].contains(&h)
    }
    fn add(&mut self, k: Key, _exact: bool) -> bool {
        let h = hash_of(k);
        self.buckets[bucket_index(&h, self.nb) as usize].insert(&h);
        self.n += 1;
        true
    }
    fn del(&mut self, _k: Key, _exact: bool) {}
    fn acc(&self, _r: &Range) -> (Fingerprint, u64) {
        ([0; 16], 0)
    }
    fn replace_range(&mut self, _r: &Range, _keys: &[Key]) {}
    fn self_bytes(&self) -> usize {
        self.buckets.iter().map(|b| b.bits.capacity() + 40).sum()
    }
    fn repairs(&self) -> bool {
        false
    }
    fn on_interval(&mut self, owner: &KeySet) {
        let keys: Vec<Key> = owner.iter().collect();
        *self = Self::from_keys(&keys);
    }
    fn apply_delta(&mut self, d: &Delta) -> DeltaOutcome {
        if d.seq <= self.seq.last_seq && self.seq.incarnation == Some(d.incarnation) {
            return DeltaOutcome::Stale;
        }
        self.seq.incarnation = Some(d.incarnation);
        if let Some(adds) = decode_keys(0, &d.adds) {
            for k in adds {
                self.add(k, true);
            }
        }
        self.seq.last_seq = d.seq;
        DeltaOutcome::Exact
    }
    fn matches(&mut self, _s: &Summary) -> bool {
        true
    }
}
