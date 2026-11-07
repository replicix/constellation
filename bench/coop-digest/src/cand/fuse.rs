//! C5: static binary-fuse filters (`xorf::BinaryFuse16`) per prefix
//! bucket with an exact overlay of keys added and removed since the
//! snapshot. `hit = added(k) || (base(k) && !removed(k))`. Snapshots are
//! built by the owner and shipped; the receiver also rebuilds a bucket
//! from `Items` when a repair refetches it.

use super::{ideal_bits, shard_idx, span};
use crate::acc::Acc;
use crate::mirror::{LocationMirror, SeqState};
use constellation_net::message::{MAX_FRAME, SIGNED_ENVELOPE};
use constellation_net::reconcile::{Fingerprint, Key, Range};
use serde::Serialize;
use std::convert::TryFrom;
use xorf::{BinaryFuse16, Filter};

/// `Descriptor::DMA_LEN` of xorf 0.13: u64 seed + 3 x u32.
const DESCRIPTOR_BYTES: usize = 20;

#[derive(Serialize)]
struct SnapMsg {
    node_id: u64,
    bucket: u32,
    seq: u64,
    descriptor: [u8; DESCRIPTOR_BYTES],
    fingerprints: Vec<u8>,
}

/// Wire bytes of a snapshot frame for `f` (checked against `MAX_FRAME`).
pub fn snapshot_wire(f: &BinaryFuse16) -> usize {
    let m = SnapMsg {
        node_id: 1,
        bucket: 1,
        seq: 1,
        descriptor: [0; DESCRIPTOR_BYTES],
        fingerprints: vec![0u8; f.fingerprints.len() * 2],
    };
    let n = postcard::to_allocvec(&m).map(|v| v.len()).unwrap_or(0) + SIGNED_ENVELOPE;
    assert!(n <= MAX_FRAME, "snapshot frame {n} exceeds MAX_FRAME");
    n
}

/// Owner-side snapshot build from a bucket's sorted keys.
pub fn build_snapshot(keys: &[Key]) -> Option<BinaryFuse16> {
    if keys.is_empty() {
        None
    } else {
        BinaryFuse16::try_from(keys).ok()
    }
}

struct FBucket {
    acc: Acc,
    filter: Option<BinaryFuse16>,
    added: Vec<u64>,
    removed: Vec<u64>,
    changes: u32,
}

impl FBucket {
    fn base_has(&self, k: Key) -> bool {
        self.filter.as_ref().is_some_and(|f| f.contains(&k))
    }
    fn hit(&self, k: Key) -> bool {
        if self.added.binary_search(&k).is_ok() {
            return true;
        }
        if self.removed.binary_search(&k).is_ok() {
            return false;
        }
        self.base_has(k)
    }
}

pub struct FuseSet {
    bb: u8,
    buckets: Vec<FBucket>,
    total: Acc,
    seq: SeqState,
}

impl FuseSet {
    pub fn from_sorted(keys: &[Key], owner_n: usize, target: usize) -> Self {
        let bb = ideal_bits(owner_n.max(keys.len()), target.max(1));
        let mut s = Self {
            bb,
            buckets: Vec::with_capacity(1 << bb),
            total: Acc::of(keys),
            seq: SeqState::default(),
        };
        let mut pos = 0usize;
        for i in 0..(1usize << bb) {
            let start = pos;
            while pos < keys.len() && shard_idx(keys[pos], bb) == i {
                pos += 1;
            }
            s.buckets.push(Self::make(&keys[start..pos]));
        }
        s
    }

    fn make(keys: &[Key]) -> FBucket {
        let filter = build_snapshot(keys);
        let added = if filter.is_none() {
            keys.to_vec()
        } else {
            Vec::new()
        };
        FBucket {
            acc: Acc::of(keys),
            filter,
            added,
            removed: Vec::new(),
            changes: 0,
        }
    }

    pub fn bucket_bits(&self) -> u8 {
        self.bb
    }

    /// Install an owner snapshot for bucket `i` (overlay is cleared:
    /// the snapshot already reflects every change up to it).
    pub fn apply_snapshot(&mut self, i: usize, f: Option<BinaryFuse16>) {
        let b = &mut self.buckets[i];
        b.filter = f;
        b.added.clear();
        b.removed.clear();
        b.changes = 0;
    }

    /// Entries in all overlays (adds + removes).
    pub fn overlay_entries(&self) -> usize {
        self.buckets
            .iter()
            .map(|b| b.added.len() + b.removed.len())
            .sum()
    }
}

impl LocationMirror for FuseSet {
    fn label(&self) -> String {
        "C5 fuse16+overlay".into()
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
        self.buckets[shard_idx(k, self.bb)].hit(k)
    }
    fn add(&mut self, k: Key, exact: bool) -> bool {
        let i = shard_idx(k, self.bb);
        let b = &mut self.buckets[i];
        if let Ok(p) = b.removed.binary_search(&k) {
            b.removed.remove(p);
        } else {
            if !exact && b.hit(k) {
                return true;
            }
            if let Err(p) = b.added.binary_search(&k) {
                b.added.insert(p, k);
            }
        }
        b.acc.add(k);
        b.changes += 1;
        self.total.add(k);
        true
    }
    fn del(&mut self, k: Key, exact: bool) {
        let i = shard_idx(k, self.bb);
        let b = &mut self.buckets[i];
        if let Ok(p) = b.added.binary_search(&k) {
            b.added.remove(p);
        } else if (exact || b.base_has(k)) && b.removed.binary_search(&k).is_err() {
            let p = b.removed.binary_search(&k).unwrap_err();
            b.removed.insert(p, k);
        } else {
            return;
        }
        b.acc.sub(k);
        b.changes += 1;
        self.total.sub(k);
    }
    fn granularity(&self) -> Option<u8> {
        Some(self.bb)
    }
    fn acc(&self, r: &Range) -> (Fingerprint, u64) {
        if r.bits == 0 {
            return (self.total.fingerprint(), self.total.count);
        }
        let (a, b) = span(r, self.bb);
        let mut acc = Acc::default();
        for x in &self.buckets[a..=b] {
            acc.merge(&x.acc);
        }
        (acc.fingerprint(), acc.count)
    }
    fn replace_range(&mut self, r: &Range, keys: &[Key]) {
        let (a, b) = span(r, self.bb);
        for i in a..=b {
            let lo = keys.partition_point(|&k| shard_idx(k, self.bb) < i);
            let hi = keys.partition_point(|&k| shard_idx(k, self.bb) <= i);
            let nb = Self::make(&keys[lo..hi]);
            self.total.unmerge(&self.buckets[i].acc);
            self.total.merge(&nb.acc);
            self.buckets[i] = nb;
        }
    }
    fn self_bytes(&self) -> usize {
        self.buckets
            .iter()
            .map(|b| {
                std::mem::size_of::<FBucket>()
                    + b.filter.as_ref().map_or(0, |f| f.fingerprints.len() * 2)
                    + (b.added.capacity() + b.removed.capacity()) * 8
            })
            .sum()
    }
}
