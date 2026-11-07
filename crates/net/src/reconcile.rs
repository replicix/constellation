//! Exact chunk-location reconciliation (plan 30 §M15).
//!
//! Replaces the cooperative cache's bloom digests: every node keeps an
//! exact *mirror* of each peer's servable chunk set and keeps it
//! current with range-based set reconciliation (RBSR, the idea behind
//! Negentropy) plus pushed deltas. A peer lookup is then a binary
//! search, and a mirror never claims a chunk its owner never held.
//!
//! # Why not the `negentropy` crate
//!
//! `negentropy` (rust-nostr, MIT, maintained) was the obvious candidate
//! and was rejected on fit, not quality:
//!
//! * its items are `(u64 timestamp, 32-byte id)` = 40 bytes, and a
//!   receiver here stores one mirror per peer — 5x the memory of the
//!   8-byte keys below, for a 4 TiB peer cache 40 MiB instead of 8 MiB;
//! * its storage is a sealed sorted vector: every cache insert/evict
//!   re-seals (O(n log n)), and every range fingerprint is an O(range)
//!   scan. The chunk set here changes every 250 ms tick;
//! * the protocol is symmetric (both sides learn have/need to converge
//!   to the union). Here only the *requester* learns, and it must mirror
//!   the owner's set exactly — including removals — not merge it;
//! * P2P frames here are signed postcard with a hard 64 KiB cap
//!   ([`crate::message::MAX_FRAME`]); its framing is its own.
//!
//! # Keys
//!
//! A chunk's reconciliation key is the first 8 bytes of its hash, big
//! endian ([`key_of`]). Chunk hashes are uniform, so keys are too; two
//! distinct chunks share a key with probability ~n/2^64 per lookup
//! (~5e-14 for a million-chunk peer), which is what "exact" means here.
//! A collision could only make one peer fetch decline, never corrupt
//! data: every fetched chunk is hash-verified.
//!
//! # Fingerprint
//!
//! A range's fingerprint is `blake3(count ‖ Σk ‖ Σmix(k))[..16]` with
//! wrapping 64-bit sums and `mix` the splitmix64 finalizer. The sums are
//! additive, so a set maintains them incrementally in O(1) per insert or
//! removal, and a range's accumulator is the sum of its sub-ranges'.
//! Two independent 64-bit sums plus the count make an accidental match
//! between different random sets ~2^-128. They are not collision
//! resistant against a writer who crafts chunk contents to collide on
//! purpose (generalized birthday): the worst such an attacker achieves
//! is a stale mirror, i.e. a declined peer fetch that falls back to S3.
//!
//! # Ranges
//!
//! A range is an aligned key prefix `(bits, path)`: all keys whose top
//! `bits` bits equal `path`. Splitting a range yields its 16 children
//! (4 more prefix bits). Because keys are uniform hashes, aligned
//! prefixes are already balanced; Negentropy's item-count-balanced
//! boundaries buy nothing here and would cost a sorted rank structure.
//!
//! # Rounds
//!
//! The mirror holder (the *initiator*) sends [`Query`]s: range, its
//! mirror's fingerprint and count for that range. The owner (the
//! *responder*, [`respond`]) compares each against its live set:
//!
//! * equal → no answer (the range is settled);
//! * owner holds ≤ [`ITEMS_THRESHOLD`] keys there, or the initiator
//!   holds none (initial sync), and it fits → [`Answer::Items`]: the
//!   owner's exact keys, which *replace* the mirror's range;
//! * otherwise → [`Answer::Children`]: fingerprint and count of each of
//!   the 16 children. The initiator settles the equal ones and empty
//!   ones locally and queries the rest next round.
//!
//! Every reply is bounded by [`REPLY_BUDGET`] (the responder stops and
//! reports how many queries it `processed`; the rest are re-sent), and
//! every request by [`MAX_QUERIES_PER_REQUEST`]. A session therefore
//! makes progress every round and finishes in about
//! `log16(n / ITEMS_THRESHOLD)` rounds plus one per REPLY_BUDGET of
//! differing keys. The walk is depth first ([`Session`]), so every
//! `Items` answer lands in the mirror within a few rounds; a gap too
//! large for [`MAX_ROUNDS`] (tens of thousands of changes on a peer of
//! millions of keys) ends the session part-way, and the next one
//! settles what was applied with no answer and continues from there.
//!
//! # Delta-log replay
//!
//! Most gaps are a few lost or reordered gossip deltas, and the owner
//! still has them: it keeps its recent [`Delta`]s in a bounded
//! [`DeltaLog`]. A mirror that was exact at owner `seq` N and learns
//! the owner is further along first asks [`respond_replay`] for the
//! deltas after N, applies them in order ([`Mirror::apply_replay`]) and
//! accepts the result only if it equals the owner's summary (count and
//! root fingerprint) at the last replayed `seq`. Anything else (the log
//! was truncated by size or age, a silent burst or a restart left no
//! deltas, the fingerprint differs) falls back to the range-based
//! session above, which is always exact. Replaying costs the missed
//! deltas' own bytes (11-17x less than RBSR for the gaps measured in
//! plan 47 P1) and no fingerprint walk.
//!
//! # Incremental updates
//!
//!
//! RBSR is the repair path. The steady state is a pushed [`Delta`] per
//! publish tick (adds and removes, 8-byte keys, a few bytes each on the
//! wire) chained by owner `seq` and by the before/after root
//! fingerprints, plus a periodic [`Summary`] heartbeat. A mirror whose
//! root equals a delta's `base` is exactly the owner's set before that
//! delta, so applying it keeps the mirror exact; a gap (lost or
//! reordered gossip, restart, reconnect) shows up as a root mismatch
//! and costs one RBSR session over only the ranges that differ.

use serde::{Deserialize, Serialize};

/// Reconciliation key: the top 64 bits of a chunk hash.
pub type Key = u64;

/// 16-byte range fingerprint.
pub type Fingerprint = [u8; 16];

/// Prefix bits added per split: fan-out 16.
pub const FANOUT_BITS: u8 = 4;
/// Owner holds at most this many keys in a disputed range → it sends
/// them instead of splitting. 32 keys ≈ 230 wire bytes, about the cost
/// of the 16 child fingerprints a split would send.
pub const ITEMS_THRESHOLD: u64 = 32;
/// Upper bound on the answer bytes of one reply. Sized so the whole
/// signed frame stays under [`crate::message::MAX_FRAME`] (64 KiB)
/// with the per-answer estimate below and the envelope.
pub const REPLY_BUDGET: usize = 48 * 1024;
/// At most this many ranges per request (~37 bytes each ⇒ ~19 KiB).
pub const MAX_QUERIES_PER_REQUEST: usize = 512;
/// Hard cap on rounds of one session; see the module doc.
pub const MAX_ROUNDS: usize = 256;
/// Keys (adds + removes) per gossiped [`Delta`]. At ≤ 10 bytes per
/// key this stays well inside [`crate::message::GOSSIP_CONTENT_LIMIT`].
pub const MAX_DELTA_KEYS: usize = 2048;

/// Upper bound on the delta bytes of one [`ReplayReply`]; with the
/// per-delta overhead the signed frame stays under
/// [`crate::message::MAX_FRAME`].
pub const REPLAY_PAGE_BUDGET: usize = 48 * 1024;
/// Hard cap on the replay pages of one repair; a larger backlog falls
/// back to a range session.
pub const MAX_REPLAY_PAGES: usize = 64;
/// Wire and memory overhead charged per logged delta besides its key
/// bytes (two fingerprints, seq, incarnation, vector headers).
const DELTA_LOG_OVERHEAD: usize = 96;

/// Largest per-set shard fan-out (2^16 shards). At 64 keys per shard
/// that is 4M keys before shards start to grow past their target.
const MAX_SHARD_BITS: u8 = 16;
/// Target mean keys per shard; resize when it drifts past 2x / below
/// 1/4 of this (hysteresis so a set hovering at a boundary does not
/// rebuild every tick).
const SHARD_TARGET: usize = 64;

/// Conservative wire-size estimate of one [`Answer::Children`].
const CHILDREN_ANSWER_BYTES: usize = 1 + 11 + 1 + 16 * (16 + 10);
/// Fixed overhead of one [`Answer::Items`] besides its key bytes.
const ITEMS_ANSWER_OVERHEAD: usize = 1 + 11 + 5;
/// Estimate of one key in [`encode_keys`] form when deciding whether to
/// send items before encoding them (varint of a 64-bit gap ≤ 10 bytes).
const KEY_BYTES_ESTIMATE: usize = 10;

/// Reconciliation key of a 32-byte chunk hash.
pub fn key_of(hash: &[u8; 32]) -> Key {
    u64::from_be_bytes(hash[0..8].try_into().unwrap())
}

/// splitmix64 finalizer: a bijective mix so the second sum is not a
/// linear function of the first.
fn mix(k: u64) -> u64 {
    let mut z = k.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Additive accumulator of a key set: the fingerprint's preimage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Acc {
    pub count: u64,
    s1: u64,
    s2: u64,
}

impl Acc {
    fn add(&mut self, k: Key) {
        self.count += 1;
        self.s1 = self.s1.wrapping_add(k);
        self.s2 = self.s2.wrapping_add(mix(k));
    }

    fn sub(&mut self, k: Key) {
        self.count -= 1;
        self.s1 = self.s1.wrapping_sub(k);
        self.s2 = self.s2.wrapping_sub(mix(k));
    }

    fn merge(&mut self, o: &Acc) {
        self.count += o.count;
        self.s1 = self.s1.wrapping_add(o.s1);
        self.s2 = self.s2.wrapping_add(o.s2);
    }

    fn unmerge(&mut self, o: &Acc) {
        self.count -= o.count;
        self.s1 = self.s1.wrapping_sub(o.s1);
        self.s2 = self.s2.wrapping_sub(o.s2);
    }

    fn of(keys: &[Key]) -> Self {
        let mut a = Acc::default();
        for &k in keys {
            a.add(k);
        }
        a
    }

    pub fn fingerprint(&self) -> Fingerprint {
        let mut pre = [0u8; 24];
        pre[0..8].copy_from_slice(&self.count.to_le_bytes());
        pre[8..16].copy_from_slice(&self.s1.to_le_bytes());
        pre[16..24].copy_from_slice(&self.s2.to_le_bytes());
        let h = blake3::hash(&pre);
        h.as_bytes()[0..16].try_into().unwrap()
    }
}

/// An aligned key prefix: every key whose top `bits` bits are `path`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Range {
    pub bits: u8,
    pub path: u64,
}

fn low_mask(n: u32) -> u64 {
    if n >= 64 {
        u64::MAX
    } else {
        (1u64 << n) - 1
    }
}

impl Range {
    pub const ROOT: Range = Range { bits: 0, path: 0 };

    /// Rejects ranges a peer could send that do not name a prefix.
    pub fn is_valid(&self) -> bool {
        match self.bits {
            0 => self.path == 0,
            64 => true,
            b if b < 64 => self.path < (1u64 << b),
            _ => false,
        }
    }

    /// Smallest key in the range.
    pub fn lo(&self) -> Key {
        if self.bits == 0 {
            0
        } else {
            self.path << (64 - u32::from(self.bits))
        }
    }

    /// Largest key in the range (inclusive).
    pub fn hi(&self) -> Key {
        self.lo() | low_mask(64 - u32::from(self.bits))
    }

    pub fn contains(&self, k: Key) -> bool {
        (self.lo()..=self.hi()).contains(&k)
    }

    pub fn can_split(&self) -> bool {
        self.bits < 64
    }

    /// The (up to) 16 child prefixes, in key order. Empty at 64 bits.
    pub fn children(&self) -> Vec<Range> {
        if !self.can_split() {
            return Vec::new();
        }
        let step = FANOUT_BITS.min(64 - self.bits);
        (0..(1u64 << step))
            .map(|i| Range {
                bits: self.bits + step,
                path: (self.path << step) | i,
            })
            .collect()
    }

    /// Every range at depth `bits` (a multiple of 4, ≤ 16), in order.
    pub fn level(bits: u8) -> Vec<Range> {
        let bits = bits.min(16);
        (0..(1u64 << bits))
            .map(|path| Range { bits, path })
            .collect()
    }
}

#[derive(Debug, Clone, Default)]
struct Shard {
    keys: Vec<Key>,
    acc: Acc,
}

/// A set of keys with O(1)-maintained range accumulators.
///
/// Keys are split by their top `shard_bits` bits into sorted shards of
/// ~[`SHARD_TARGET`] keys, each with a cached [`Acc`]. A range at most
/// `shard_bits` deep sums whole shards' cached accumulators; a deeper
/// range lives inside one shard and is summed by a binary-searched
/// scan of ≤ ~128 keys. Insert/remove is a binary search plus a
/// memmove of one small shard. Memory is ~8–12 bytes per key plus 48
/// bytes per shard.
#[derive(Debug, Clone)]
pub struct KeySet {
    shard_bits: u8,
    shards: Vec<Shard>,
    total: Acc,
}

impl Default for KeySet {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for KeySet {
    fn eq(&self, other: &Self) -> bool {
        self.total == other.total && self.iter().eq(other.iter())
    }
}

impl KeySet {
    pub fn new() -> Self {
        Self {
            shard_bits: 0,
            shards: vec![Shard::default()],
            total: Acc::default(),
        }
    }

    /// Build from keys in any order; duplicates collapse.
    pub fn from_keys(keys: impl IntoIterator<Item = Key>) -> Self {
        let mut v: Vec<Key> = keys.into_iter().collect();
        v.sort_unstable();
        v.dedup();
        Self::from_sorted(v)
    }

    fn from_sorted(keys: Vec<Key>) -> Self {
        let bits = ideal_bits(keys.len());
        let mut s = Self {
            shard_bits: bits,
            shards: vec![Shard::default(); 1usize << bits],
            total: Acc::default(),
        };
        for k in keys {
            let i = s.shard_of(k);
            let sh = &mut s.shards[i];
            sh.keys.push(k);
            sh.acc.add(k);
            s.total.add(k);
        }
        s
    }

    pub fn len(&self) -> usize {
        self.total.count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.total.count == 0
    }

    /// Accumulator of the whole set, maintained incrementally.
    pub fn root(&self) -> Acc {
        self.total
    }

    pub fn root_fingerprint(&self) -> Fingerprint {
        self.total.fingerprint()
    }

    /// Rough resident bytes (keys + shard headers), for status output.
    pub fn approx_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.keys.capacity() * 8 + std::mem::size_of::<Shard>())
            .sum()
    }

    fn shard_of(&self, k: Key) -> usize {
        if self.shard_bits == 0 {
            0
        } else {
            (k >> (64 - u32::from(self.shard_bits))) as usize
        }
    }

    pub fn contains(&self, k: Key) -> bool {
        self.shards[self.shard_of(k)].keys.binary_search(&k).is_ok()
    }

    /// Returns whether `k` was newly inserted.
    pub fn insert(&mut self, k: Key) -> bool {
        let i = self.shard_of(k);
        let sh = &mut self.shards[i];
        match sh.keys.binary_search(&k) {
            Ok(_) => false,
            Err(pos) => {
                sh.keys.insert(pos, k);
                sh.acc.add(k);
                self.total.add(k);
                self.maybe_resize();
                true
            }
        }
    }

    /// Returns whether `k` was present.
    pub fn remove(&mut self, k: Key) -> bool {
        let i = self.shard_of(k);
        let sh = &mut self.shards[i];
        match sh.keys.binary_search(&k) {
            Ok(pos) => {
                sh.keys.remove(pos);
                sh.acc.sub(k);
                self.total.sub(k);
                self.maybe_resize();
                true
            }
            Err(_) => false,
        }
    }

    /// Shards overlapping `r`, as an inclusive index range.
    fn shard_span(&self, r: &Range) -> (usize, usize) {
        (self.shard_of(r.lo()), self.shard_of(r.hi()))
    }

    /// Accumulator of the keys in `r`.
    pub fn acc(&self, r: &Range) -> Acc {
        if r.bits == 0 {
            return self.total;
        }
        let (a, b) = self.shard_span(r);
        if r.bits <= self.shard_bits {
            let mut acc = Acc::default();
            for sh in &self.shards[a..=b] {
                acc.merge(&sh.acc);
            }
            acc
        } else {
            // Deeper than the shard prefix: the range is inside shard `a`.
            let keys = &self.shards[a].keys;
            let s = keys.partition_point(|&k| k < r.lo());
            let e = keys.partition_point(|&k| k <= r.hi());
            Acc::of(&keys[s..e])
        }
    }

    /// Sorted keys in `r`.
    pub fn keys_in(&self, r: &Range) -> Vec<Key> {
        let (a, b) = self.shard_span(r);
        let mut out = Vec::new();
        for sh in &self.shards[a..=b] {
            let s = sh.keys.partition_point(|&k| k < r.lo());
            let e = sh.keys.partition_point(|&k| k <= r.hi());
            out.extend_from_slice(&sh.keys[s..e]);
        }
        out
    }

    /// Make the set's intersection with `r` exactly `keys` (which the
    /// caller has checked lie in `r`). This is how an
    /// [`Answer::Items`] lands: adds and removals in one step.
    pub fn replace_range(&mut self, r: &Range, keys: &[Key]) {
        let (a, b) = self.shard_span(r);
        let (lo, hi) = (r.lo(), r.hi());
        let mut dropped = Acc::default();
        for sh in &mut self.shards[a..=b] {
            let s = sh.keys.partition_point(|&k| k < lo);
            let e = sh.keys.partition_point(|&k| k <= hi);
            if s == e {
                continue;
            }
            let removed = Acc::of(&sh.keys[s..e]);
            sh.keys.drain(s..e);
            sh.acc.unmerge(&removed);
            dropped.merge(&removed);
        }
        self.total.unmerge(&dropped);
        for &k in keys {
            debug_assert!(r.contains(k));
            let i = self.shard_of(k);
            let sh = &mut self.shards[i];
            if let Err(pos) = sh.keys.binary_search(&k) {
                sh.keys.insert(pos, k);
                sh.acc.add(k);
                self.total.add(k);
            }
        }
        self.maybe_resize();
    }

    pub fn clear_range(&mut self, r: &Range) {
        self.replace_range(r, &[]);
    }

    /// All keys in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = Key> + '_ {
        self.shards.iter().flat_map(|s| s.keys.iter().copied())
    }

    fn maybe_resize(&mut self) {
        let n = self.len();
        let shards = self.shards.len();
        let grow = n > shards * SHARD_TARGET * 2 && self.shard_bits < MAX_SHARD_BITS;
        let shrink = self.shard_bits > 0 && n < shards * SHARD_TARGET / 4;
        if grow || shrink {
            let keys: Vec<Key> = self.iter().collect();
            *self = Self::from_sorted(keys);
        }
    }
}

fn ideal_bits(n: usize) -> u8 {
    let mut b = 0u8;
    while b < MAX_SHARD_BITS && (1usize << b) * SHARD_TARGET < n {
        b += 1;
    }
    b
}

// ---- compact key lists -------------------------------------------------

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn get_varint(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let mut v: u64 = 0;
    for shift in (0..70).step_by(7) {
        let b = *buf.get(*pos)?;
        *pos += 1;
        if shift == 63 && b > 1 {
            return None;
        }
        v |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            // Reject a non-minimal (overlong) encoding: a terminating zero
            // byte after at least one continuation byte carries no value
            // bits, so the same gap had a shorter canonical form. `encode_keys`
            // only ever emits minimal varints, so nothing legitimate produces
            // this; accepting it would let two distinct byte strings decode to
            // the same key set (wire malleability). Found by the
            // `net_reconcile_keys` fuzz target's round-trip invariant.
            if shift > 0 && b == 0 {
                return None;
            }
            return Some(v);
        }
    }
    None
}

/// Sorted, deduplicated keys as varint gaps from `base` (a range's
/// `lo`, or 0). Random 64-bit keys are ~10-byte varints raw; gaps in a
/// deep range are a few bytes shorter.
pub fn encode_keys(base: Key, keys: &[Key]) -> Vec<u8> {
    let mut out = Vec::with_capacity(keys.len() * 8);
    let mut prev = base;
    for (i, &k) in keys.iter().enumerate() {
        debug_assert!(k >= prev && (i == 0 || k > prev));
        put_varint(&mut out, k - prev);
        prev = k;
    }
    out
}

/// Inverse of [`encode_keys`]. Rejects anything that is not strictly
/// increasing from `base`, overflows, or has trailing garbage, so a
/// peer cannot inject duplicates or out-of-range keys.
pub fn decode_keys(base: Key, buf: &[u8]) -> Option<Vec<Key>> {
    let mut out = Vec::new();
    let mut pos = 0;
    let mut prev = base;
    while pos < buf.len() {
        let gap = get_varint(buf, &mut pos)?;
        if !out.is_empty() && gap == 0 {
            return None;
        }
        let k = prev.checked_add(gap)?;
        out.push(k);
        prev = k;
    }
    Some(out)
}

// ---- messages ----------------------------------------------------------

/// The owner's set at a point in its publish history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    /// Random per daemon start: a restart resets `seq`.
    pub incarnation: u64,
    /// Publish sequence: bumped once per [`Delta`] (or silent change).
    pub seq: u64,
    pub root: Fingerprint,
    pub count: u64,
}

/// One publish tick's net change to the owner's set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Delta {
    pub incarnation: u64,
    pub seq: u64,
    /// Root fingerprint before / after this delta.
    pub base: Fingerprint,
    pub root: Fingerprint,
    /// [`encode_keys`] (base 0) of added / removed keys.
    pub adds: Vec<u8>,
    pub removes: Vec<u8>,
}

/// "My mirror of your set, restricted to `range`, has this fingerprint
/// and count."
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Query {
    pub range: Range,
    pub fp: Fingerprint,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Answer {
    /// The owner's exact keys in `range` ([`encode_keys`] from
    /// `range.lo()`); they replace the mirror's.
    Items { range: Range, keys: Vec<u8> },
    /// `(fingerprint, count)` of each of `range.children()`, in order.
    Children {
        range: Range,
        children: Vec<(Fingerprint, u64)>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reply {
    /// The owner's set these answers were computed from.
    pub summary: Summary,
    /// How many leading queries were handled; the rest must be re-sent.
    /// Handled queries without an answer are settled (equal).
    pub processed: u32,
    pub answers: Vec<Answer>,
}

/// Answer `queries` against the owner's live `set`, within `budget`
/// answer bytes. Always processes at least one query when given any,
/// so every round makes progress.
pub fn respond(set: &KeySet, summary: Summary, queries: &[Query], budget: usize) -> Reply {
    let mut answers = Vec::new();
    let mut used = 0usize;
    let mut processed = 0u32;
    for q in queries.iter().take(MAX_QUERIES_PER_REQUEST) {
        if !q.range.is_valid() {
            // A malformed range from a buggy peer: stop here rather than
            // "settle" it; the initiator aborts on a non-progressing reply.
            break;
        }
        let acc = set.acc(&q.range);
        if acc.count == q.count && acc.fingerprint() == q.fp {
            processed += 1;
            continue;
        }
        let items = acc.count <= ITEMS_THRESHOLD
            || !q.range.can_split()
            || (q.count == 0
                && (acc.count as usize).saturating_mul(KEY_BYTES_ESTIMATE) + ITEMS_ANSWER_OVERHEAD
                    <= budget);
        let (answer, size) = if items {
            let keys = encode_keys(q.range.lo(), &set.keys_in(&q.range));
            let size = keys.len() + ITEMS_ANSWER_OVERHEAD;
            (
                Answer::Items {
                    range: q.range,
                    keys,
                },
                size,
            )
        } else {
            let children = q
                .range
                .children()
                .iter()
                .map(|c| {
                    let a = set.acc(c);
                    (a.fingerprint(), a.count)
                })
                .collect();
            (
                Answer::Children {
                    range: q.range,
                    children,
                },
                CHILDREN_ANSWER_BYTES,
            )
        };
        if !answers.is_empty() && used + size > budget {
            break;
        }
        used += size;
        answers.push(answer);
        processed += 1;
    }
    Reply {
        summary,
        processed,
        answers,
    }
}

/// Why an initiator abandoned a session. Every variant means "the peer
/// sent something inconsistent"; the mirror keeps whatever it applied
/// (each applied answer is individually exact) and a later session
/// retries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionError {
    NoProgress,
    UnexpectedRange,
    BadKeys,
    BadChildren,
}

/// Initiator side of one reconciliation session over a mirror.
///
/// The pending ranges are walked **depth first**: the next request takes
/// the most recently queued ranges (the children of the last answers)
/// before the older, shallower ones. A breadth-first walk (the first
/// implementation) had to finish a whole level before it asked about
/// the next, and a level of a large peer is far more than one session's
/// rounds: with 8 Mi keys and a gap of tens of thousands of changes
/// tens of thousands of level-16 ranges are disputed, each answered
/// with 16 child fingerprints (~436 bytes, ~110 per reply), so the 256
/// rounds went to `Children` frontiers, no `Items` answer was ever
/// reached, nothing was applied, and the next session repeated it
/// exactly. Depth first reaches the first leaves after a handful of
/// rounds and every `Items` answer is applied to the mirror at once, so
/// a session that hits [`MAX_ROUNDS`] has still moved the mirror toward
/// the owner and the next one only re-walks what is left (an equal range
/// settles with no answer). Each applied answer is exact on its own, so
/// stopping anywhere is safe.
#[derive(Debug)]
pub struct Session {
    /// Pending ranges; the last element is the next to ask.
    stack: Vec<Range>,
    in_flight: usize,
    pub rounds: usize,
}

impl Session {
    /// Start with every range at `start_bits` (0 = just the root). A
    /// deeper start saves rounds when a difference is already known and
    /// the set is large; see [`start_bits_for`].
    pub fn new(start_bits: u8) -> Self {
        let mut stack = Range::level(start_bits);
        stack.reverse();
        Self {
            stack,
            in_flight: 0,
            rounds: 0,
        }
    }

    pub fn is_done(&self) -> bool {
        self.stack.is_empty()
    }

    /// Queries for the next round, or `None` when settled or out of
    /// rounds.
    pub fn next_request(&mut self, mirror: &KeySet) -> Option<Vec<Query>> {
        if self.stack.is_empty() || self.rounds >= MAX_ROUNDS {
            return None;
        }
        let n = self.stack.len().min(MAX_QUERIES_PER_REQUEST);
        self.in_flight = n;
        Some(
            self.stack
                .iter()
                .rev()
                .take(n)
                .map(|r| {
                    let a = mirror.acc(r);
                    Query {
                        range: *r,
                        fp: a.fingerprint(),
                        count: a.count,
                    }
                })
                .collect(),
        )
    }

    /// Apply the owner's reply to the mirror and queue the next level.
    pub fn apply(&mut self, mirror: &mut KeySet, reply: &Reply) -> Result<(), SessionError> {
        self.rounds += 1;
        let processed = (reply.processed as usize).min(self.in_flight);
        self.in_flight = 0;
        if processed == 0 {
            return Err(SessionError::NoProgress);
        }
        // The queries were the top `processed` of the stack, in pop order.
        let mut expected: std::collections::HashSet<Range> = self
            .stack
            .split_off(self.stack.len() - processed)
            .into_iter()
            .collect();
        let mut next: Vec<Range> = Vec::new();
        for answer in &reply.answers {
            match answer {
                Answer::Items { range, keys } => {
                    if !expected.remove(range) {
                        return Err(SessionError::UnexpectedRange);
                    }
                    let keys = decode_keys(range.lo(), keys).ok_or(SessionError::BadKeys)?;
                    if keys.last().is_some_and(|&k| !range.contains(k)) {
                        return Err(SessionError::BadKeys);
                    }
                    mirror.replace_range(range, &keys);
                }
                Answer::Children { range, children } => {
                    if !expected.remove(range) {
                        return Err(SessionError::UnexpectedRange);
                    }
                    let kids = range.children();
                    if kids.len() != children.len() || kids.is_empty() {
                        return Err(SessionError::BadChildren);
                    }
                    for (child, (fp, count)) in kids.iter().zip(children) {
                        if *count == 0 {
                            mirror.clear_range(child);
                            continue;
                        }
                        let mine = mirror.acc(child);
                        if mine.count != *count || mine.fingerprint() != *fp {
                            next.push(*child);
                        }
                    }
                }
            }
        }
        // Pushed in reverse so the lowest path pops first.
        self.stack.extend(next.into_iter().rev());
        Ok(())
    }
}

/// Where a session starts when a difference is already known: deeper
/// for larger sets so the first round lands near the differing leaves
/// (≤ 256 queries, ~9.5 KiB). An "are we in sync?" probe uses 0.
pub fn start_bits_for(n: usize) -> u8 {
    if n < 2048 {
        0
    } else if n < 32_768 {
        4
    } else {
        8
    }
}

// ---- delta-log replay (plan 47 P3) ---------------------------------------

/// The owner's answer to "send me the deltas after `after_seq`".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayReply {
    /// The owner's set when the reply was computed.
    pub summary: Summary,
    /// `false` when the log cannot serve the request (another
    /// incarnation, truncated, a silent change, or a `seq` ahead of the
    /// owner's): the asker falls back to a range session.
    pub available: bool,
    /// Consecutive deltas starting at `after_seq + 1`, at most
    /// [`REPLAY_PAGE_BUDGET`] bytes; fewer than are missing when paged.
    pub deltas: Vec<Delta>,
}

impl ReplayReply {
    /// The reply of a node that cannot or will not replay.
    pub fn unavailable() -> Self {
        Self {
            summary: Summary {
                incarnation: 0,
                seq: 0,
                root: [0; 16],
                count: 0,
            },
            available: false,
            deltas: Vec::new(),
        }
    }
}

fn delta_cost(d: &Delta) -> usize {
    d.adds.len() + d.removes.len() + DELTA_LOG_OVERHEAD
}

/// The owner's bounded memory of the deltas it published: at most
/// `max_bytes` of them (oldest dropped first) and none older than
/// `max_age`. Always a contiguous run of `seq`: a change that was not
/// published as a delta ([`DeltaLog::clear`]) starts a new run.
#[derive(Debug, Clone)]
pub struct DeltaLog {
    entries: std::collections::VecDeque<(std::time::Instant, Delta)>,
    bytes: usize,
    max_bytes: usize,
    max_age: std::time::Duration,
}

impl DeltaLog {
    /// `max_bytes == 0` keeps nothing (replay disabled).
    pub fn new(max_bytes: usize, max_age: std::time::Duration) -> Self {
        Self {
            entries: std::collections::VecDeque::new(),
            bytes: 0,
            max_bytes,
            max_age,
        }
    }

    /// Bytes held (key bytes plus per-delta overhead).
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Forget everything: the owner's `seq` moved without a delta.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
    }

    /// Record a published delta (call in `seq` order).
    pub fn push(&mut self, d: Delta, now: std::time::Instant) {
        if self.max_bytes == 0 {
            return;
        }
        if self
            .entries
            .back()
            .is_some_and(|(_, last)| last.incarnation != d.incarnation || last.seq + 1 != d.seq)
        {
            self.clear();
        }
        self.bytes += delta_cost(&d);
        self.entries.push_back((now, d));
        self.prune(now);
    }

    /// Drop what is over the size or age bound. The newest delta is
    /// always kept, even when alone it exceeds the size bound.
    pub fn prune(&mut self, now: std::time::Instant) {
        while self.entries.len() > 1 && self.bytes > self.max_bytes {
            self.pop_front();
        }
        while let Some((at, _)) = self.entries.front() {
            if now.duration_since(*at) <= self.max_age {
                break;
            }
            self.pop_front();
        }
    }

    fn pop_front(&mut self) {
        if let Some((_, d)) = self.entries.pop_front() {
            self.bytes -= delta_cost(&d);
        }
    }
}

/// Answer a replay request from `log` against the owner's current
/// `summary`: the deltas after `after_seq` of `incarnation`, within
/// `budget` bytes (always at least one when any are missing).
pub fn respond_replay(
    log: &DeltaLog,
    summary: Summary,
    incarnation: u64,
    after_seq: u64,
    budget: usize,
) -> ReplayReply {
    let unavailable = ReplayReply {
        summary,
        available: false,
        deltas: Vec::new(),
    };
    if incarnation != summary.incarnation || after_seq > summary.seq {
        return unavailable;
    }
    if after_seq == summary.seq {
        return ReplayReply {
            summary,
            available: true,
            deltas: Vec::new(),
        };
    }
    // The log must run from `after_seq + 1` to the owner's current seq.
    let (Some((_, first)), Some((_, last))) = (log.entries.front(), log.entries.back()) else {
        return unavailable;
    };
    if first.incarnation != incarnation || first.seq > after_seq + 1 || last.seq != summary.seq {
        return unavailable;
    }
    let skip = (after_seq + 1 - first.seq) as usize;
    let mut used = 0usize;
    let mut deltas = Vec::new();
    for (_, d) in log.entries.iter().skip(skip) {
        let cost = delta_cost(d);
        if !deltas.is_empty() && used + cost > budget {
            break;
        }
        used += cost;
        deltas.push(d.clone());
    }
    ReplayReply {
        summary,
        available: true,
        deltas,
    }
}

/// Why a replayed page was refused (nothing was applied).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayError {
    /// The mirror has no exact point of this incarnation to replay from.
    NotExact,
    /// A delta of another incarnation, out of order, a gap, or a delta
    /// with undecodable or too many keys.
    Malformed,
}

// ---- mirror of one peer's set -------------------------------------------

/// What happened to a pushed [`Delta`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaOutcome {
    /// Applied on the exact base: the mirror equals the owner at `seq`.
    Exact,
    /// Applied, but the mirror was not on the delta's base (a missed
    /// delta or a session that straddled owner changes). Every applied
    /// key is at least as fresh as before; the caller should resync.
    Diverged,
    /// Older than something already applied (reordered / duplicate).
    Stale,
    /// Undecodable key lists.
    Malformed,
}

/// One peer's set as this node knows it.
#[derive(Debug, Default, Clone)]
pub struct Mirror {
    pub keys: KeySet,
    pub incarnation: Option<u64>,
    /// Highest owner `seq` whose delta (or summary) this mirror has seen.
    pub last_seq: u64,
    /// The owner `seq` at which this mirror was last known to equal the
    /// owner's set exactly (an exact delta, a matching summary, a
    /// completed session or replay); `None` after a restart. Replay
    /// starts here: `last_seq` may be ahead of it when a delta was
    /// applied off its base.
    pub exact_seq: Option<u64>,
}

impl Mirror {
    fn adopt_incarnation(&mut self, incarnation: u64) {
        if self.incarnation != Some(incarnation) {
            // The owner restarted: its seq restarted too. Keep the keys —
            // a restarted cache is mostly the same set — and let the next
            // session repair the difference.
            self.incarnation = Some(incarnation);
            self.last_seq = 0;
            self.exact_seq = None;
        }
    }

    /// Where a replay would start: the owner's incarnation this mirror
    /// follows and the `seq` it was last exact at.
    pub fn replay_point(&self) -> Option<(u64, u64)> {
        Some((self.incarnation?, self.exact_seq?))
    }

    /// Apply replayed deltas (those after `after`, in order, as
    /// [`respond_replay`] returns them) and return the last `seq`
    /// applied, or `after` for an empty page. All-or-nothing on
    /// malformed input; otherwise the keys are applied even where the
    /// mirror was off the chain (a delta already applied by gossip is
    /// applied again, which is idempotent: a delta only says which keys
    /// are present or absent). The caller decides whether the result is
    /// exact by comparing it with the owner's summary
    /// ([`Mirror::matches`]); until then `exact_seq` does not move.
    pub fn apply_replay(&mut self, deltas: &[Delta], after: u64) -> Result<u64, ReplayError> {
        let inc = self.incarnation.ok_or(ReplayError::NotExact)?;
        let mut decoded = Vec::with_capacity(deltas.len());
        let mut expect = after;
        for d in deltas {
            if d.incarnation != inc || d.seq != expect + 1 {
                return Err(ReplayError::Malformed);
            }
            let (Some(adds), Some(removes)) = (decode_keys(0, &d.adds), decode_keys(0, &d.removes))
            else {
                return Err(ReplayError::Malformed);
            };
            if adds.len() + removes.len() > MAX_DELTA_KEYS {
                return Err(ReplayError::Malformed);
            }
            expect = d.seq;
            decoded.push((adds, removes));
        }
        for (adds, removes) in decoded {
            for k in removes {
                self.keys.remove(k);
            }
            for k in adds {
                self.keys.insert(k);
            }
        }
        self.last_seq = self.last_seq.max(expect);
        Ok(expect)
    }

    pub fn apply_delta(&mut self, d: &Delta) -> DeltaOutcome {
        self.adopt_incarnation(d.incarnation);
        if d.seq <= self.last_seq {
            return DeltaOutcome::Stale;
        }
        let (Some(adds), Some(removes)) = (decode_keys(0, &d.adds), decode_keys(0, &d.removes))
        else {
            return DeltaOutcome::Malformed;
        };
        // The sender caps a delta at `MAX_DELTA_KEYS` before encoding, so
        // a decoded delta larger than that was forged: refuse it rather
        // than let a peer push an unbounded key list into the mirror.
        if adds.len() + removes.len() > MAX_DELTA_KEYS {
            return DeltaOutcome::Malformed;
        }
        let on_base = self.keys.root_fingerprint() == d.base;
        // Removes before adds: a key both removed and re-added in one
        // tick is published as an add only, so order only matters for
        // malformed input, where "present" is the safer outcome.
        for k in removes {
            self.keys.remove(k);
        }
        for k in adds {
            self.keys.insert(k);
        }
        self.last_seq = d.seq;
        if on_base && self.keys.root_fingerprint() == d.root {
            self.exact_seq = Some(d.seq);
            DeltaOutcome::Exact
        } else {
            DeltaOutcome::Diverged
        }
    }

    /// Does the mirror match this owner summary exactly? Also advances
    /// `last_seq` when it does, so older deltas are recognized as stale.
    pub fn matches(&mut self, s: &Summary) -> bool {
        let same = self.keys.len() as u64 == s.count && self.keys.root_fingerprint() == s.root;
        if same {
            self.adopt_incarnation(s.incarnation);
            self.last_seq = self.last_seq.max(s.seq);
            self.exact_seq = Some(self.exact_seq.map_or(s.seq, |e| e.max(s.seq)));
        }
        same
    }
}

/// Run a whole session in memory. Test and bench helper; the daemon
/// drives [`Session`] over the network instead.
pub fn sync_in_memory(mirror: &mut KeySet, owner: &KeySet, start_bits: u8) -> (usize, usize) {
    let summary = Summary {
        incarnation: 1,
        seq: 1,
        root: owner.root_fingerprint(),
        count: owner.len() as u64,
    };
    let mut s = Session::new(start_bits);
    let mut bytes = 0usize;
    while let Some(q) = s.next_request(mirror) {
        let reply = respond(owner, summary, &q, REPLY_BUDGET);
        bytes += postcard::to_allocvec(&q).map(|v| v.len()).unwrap_or(0);
        bytes += postcard::to_allocvec(&reply).map(|v| v.len()).unwrap_or(0);
        if s.apply(mirror, &reply).is_err() {
            break;
        }
    }
    (s.rounds, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic PRNG (splitmix64) so failures reproduce by seed
    /// without a `rand` dependency in this crate.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            mix(self.0)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n.max(1)
        }
    }

    fn random_set(rng: &mut Rng, n: usize) -> Vec<Key> {
        (0..n).map(|_| rng.next()).collect()
    }

    /// Owner = base with `d` random edits (half adds, half removes) —
    /// the mirror is `base`.
    fn perturb(rng: &mut Rng, base: &KeySet, d: usize) -> KeySet {
        let mut owner = base.clone();
        let keys: Vec<Key> = base.iter().collect();
        for i in 0..d {
            if i % 2 == 0 || keys.is_empty() {
                owner.insert(rng.next());
            } else {
                let k = keys[rng.below(keys.len() as u64) as usize];
                owner.remove(k);
            }
        }
        owner
    }

    #[test]
    fn range_bounds_and_children() {
        assert_eq!(Range::ROOT.lo(), 0);
        assert_eq!(Range::ROOT.hi(), u64::MAX);
        let kids = Range::ROOT.children();
        assert_eq!(kids.len(), 16);
        assert_eq!(kids[0].lo(), 0);
        assert_eq!(kids[15].hi(), u64::MAX);
        for w in kids.windows(2) {
            assert_eq!(w[0].hi() + 1, w[1].lo(), "children must tile the parent");
        }
        let deep = Range { bits: 64, path: 7 };
        assert!(deep.is_valid() && !deep.can_split());
        assert_eq!((deep.lo(), deep.hi()), (7, 7));
        assert!(!Range { bits: 4, path: 16 }.is_valid());
        assert!(!Range { bits: 65, path: 0 }.is_valid());
        assert!(!Range { bits: 0, path: 1 }.is_valid());
        // The last split before 64 bits still tiles exactly.
        let r = Range { bits: 60, path: 3 };
        let kids = r.children();
        assert_eq!(kids.len(), 16);
        assert_eq!(kids[0].lo(), r.lo());
        assert_eq!(kids[15].hi(), r.hi());
    }

    #[test]
    fn keyset_accumulators_match_a_scan_at_every_depth() {
        let mut rng = Rng(1);
        for n in [0usize, 1, 100, 5_000, 40_000] {
            let keys = random_set(&mut rng, n);
            let set = KeySet::from_keys(keys.iter().copied());
            for bits in [0u8, 4, 8, 12, 16, 20, 32] {
                for _ in 0..8 {
                    let path = if bits == 0 {
                        0
                    } else {
                        rng.below(1u64 << bits)
                    };
                    let r = Range { bits, path };
                    let mut want: Vec<Key> =
                        keys.iter().copied().filter(|&k| r.contains(k)).collect();
                    want.sort_unstable();
                    want.dedup();
                    assert_eq!(set.keys_in(&r), want, "n={n} range={r:?}");
                    assert_eq!(set.acc(&r), Acc::of(&want), "n={n} range={r:?}");
                }
            }
        }
    }

    /// Incremental maintenance (including shard resizes both ways) must
    /// land on the same accumulators as building from scratch.
    #[test]
    fn incremental_updates_equal_a_rebuild() {
        let mut rng = Rng(2);
        let mut set = KeySet::new();
        let mut present: Vec<Key> = Vec::new();
        let mut member = std::collections::HashSet::new();
        for step in 0..60_000 {
            // Grow for the first half, then shrink hard (net -3/4 per
            // step): exercises both resize directions.
            let grow = step < 30_000 || rng.below(8) == 0;
            if grow || present.is_empty() {
                let k = rng.next();
                assert_eq!(set.insert(k), member.insert(k));
                if member.len() > present.len() {
                    present.push(k);
                }
            } else {
                let i = rng.below(present.len() as u64) as usize;
                let k = present.swap_remove(i);
                member.remove(&k);
                assert!(set.remove(k));
                assert!(!set.remove(k), "a removed key was still present");
            }
            if step % 5_000 == 0 {
                let rebuilt = KeySet::from_keys(present.iter().copied());
                assert_eq!(set.root(), rebuilt.root());
                assert!(set == rebuilt);
            }
        }
        let rebuilt = KeySet::from_keys(present.iter().copied());
        assert!(set == rebuilt);
        assert_eq!(set.len(), present.len());
        for r in Range::level(8) {
            assert_eq!(set.acc(&r), rebuilt.acc(&r));
        }
    }

    #[test]
    fn replace_range_is_exact() {
        let mut rng = Rng(3);
        let mut set = KeySet::from_keys(random_set(&mut rng, 10_000));
        let r = Range { bits: 4, path: 9 };
        let fresh: Vec<Key> = {
            let mut v: Vec<Key> = (0..50).map(|_| r.lo() | (rng.next() >> 4)).collect();
            v.sort_unstable();
            v.dedup();
            v
        };
        let outside: Vec<Key> = set.iter().filter(|&k| !r.contains(k)).collect();
        set.replace_range(&r, &fresh);
        assert_eq!(set.keys_in(&r), fresh);
        let outside_after: Vec<Key> = set.iter().filter(|&k| !r.contains(k)).collect();
        assert_eq!(
            outside, outside_after,
            "keys outside the range were touched"
        );
        assert!(set == KeySet::from_keys(set.iter().collect::<Vec<_>>()));
    }

    #[test]
    fn key_codec_roundtrips_and_rejects_garbage() {
        let mut rng = Rng(4);
        for n in [0usize, 1, 2, 100, 3000] {
            let mut keys = random_set(&mut rng, n);
            keys.sort_unstable();
            keys.dedup();
            let enc = encode_keys(0, &keys);
            assert_eq!(decode_keys(0, &enc).unwrap(), keys);
        }
        // Edge values.
        let keys = vec![0, 1, u64::MAX];
        assert_eq!(decode_keys(0, &encode_keys(0, &keys)).unwrap(), keys);
        let r = Range {
            bits: 8,
            path: 0xab,
        };
        let keys = vec![r.lo(), r.lo() + 5, r.hi()];
        assert_eq!(
            decode_keys(r.lo(), &encode_keys(r.lo(), &keys)).unwrap(),
            keys
        );
        // Duplicates (gap 0 after the first key) are refused.
        assert!(decode_keys(0, &[5, 0]).is_none());
        // Overflow past u64::MAX is refused.
        let mut bad = encode_keys(0, &[u64::MAX]);
        bad.push(1);
        assert!(decode_keys(0, &bad).is_none());
        // A truncated varint is refused.
        assert!(decode_keys(0, &[0x80]).is_none());
        // An 11-byte varint is refused.
        assert!(decode_keys(0, &[0xff; 11]).is_none());
        // A non-minimal (overlong) varint is refused: `0x80 0x00` and
        // `0x81 0x00` both terminate with a redundant zero byte after a
        // continuation, so a peer cannot smuggle a second byte string that
        // decodes to the same key set. (Regression: net_reconcile_keys fuzz.)
        assert!(decode_keys(0, &[0x80, 0x00]).is_none());
        assert!(decode_keys(0, &[0x81, 0x00]).is_none());
        // The exact fuzz-found case: a 7-continuation varint padded with a
        // trailing zero byte.
        assert!(decode_keys(
            u64::from_le_bytes([0xcc; 8]),
            &[0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0x00, 0x01]
        )
        .is_none());
        // The minimal form of the same leading value is still accepted.
        assert!(decode_keys(0, &[0x00]).is_some());
        assert!(decode_keys(0, &[0x81, 0x01]).is_some());
    }

    #[test]
    fn identical_sets_settle_in_one_round_with_no_answers() {
        let mut rng = Rng(5);
        let owner = KeySet::from_keys(random_set(&mut rng, 50_000));
        let mut mirror = owner.clone();
        let (rounds, _) = sync_in_memory(&mut mirror, &owner, 0);
        assert_eq!(rounds, 1);
        assert!(mirror == owner);
    }

    /// The milestone's property test: sets of 0..100k keys with 0..1000
    /// differences converge to exact equality within a bounded number of
    /// rounds, from both a root start and a size-based start.
    #[test]
    fn random_sets_converge_exactly_in_bounded_rounds() {
        let mut rng = Rng(6);
        let sizes = [0usize, 1, 31, 33, 500, 5_000, 20_000, 100_000];
        let diffs = [0usize, 1, 2, 10, 100, 1000];
        for &n in &sizes {
            let base = KeySet::from_keys(random_set(&mut rng, n));
            for &d in &diffs {
                let owner = perturb(&mut rng, &base, d);
                for start in [0u8, start_bits_for(n)] {
                    let mut mirror = base.clone();
                    let (rounds, bytes) = sync_in_memory(&mut mirror, &owner, start);
                    assert!(
                        mirror == owner,
                        "n={n} d={d} start={start}: mirror did not converge"
                    );
                    // Depth to the leaves plus paging of differing
                    // ranges; comfortably under this for d ≤ 1000.
                    assert!(rounds <= 16, "n={n} d={d} start={start}: {rounds} rounds");
                    if d == 0 && start == 0 {
                        assert_eq!(rounds, 1);
                    }
                    // Cost scales with the difference, not the set:
                    // generous per-difference allowance plus a constant.
                    assert!(
                        bytes <= 256 * 1024 + d * 1024,
                        "n={n} d={d} start={start}: {bytes} bytes"
                    );
                }
            }
        }
    }

    /// Keys in exactly one of the two sets.
    fn symmetric_difference(a: &KeySet, b: &KeySet) -> usize {
        let (mut x, mut y) = (a.iter().peekable(), b.iter().peekable());
        let mut d = 0;
        loop {
            match (x.peek().copied(), y.peek().copied()) {
                (None, None) => return d,
                (Some(_), None) => {
                    x.next();
                    d += 1;
                }
                (None, Some(_)) => {
                    y.next();
                    d += 1;
                }
                (Some(p), Some(q)) if p == q => {
                    x.next();
                    y.next();
                }
                (Some(p), Some(q)) if p < q => {
                    x.next();
                    d += 1;
                }
                _ => {
                    y.next();
                    d += 1;
                }
            }
        }
    }

    /// Plan 47 P3: the livelock. On a set whose level-16 ranges hold more
    /// than [`ITEMS_THRESHOLD`] keys, a gap with tens of thousands of
    /// differences makes the owner answer `Children` for every disputed
    /// level-16 range. A breadth-first walk spent all `MAX_ROUNDS` rounds
    /// on those frontiers, reached no `Items` answer, applied nothing, and
    /// the next session repeated it exactly (on 8 Mi keys: 50 sessions,
    /// 517 MiB, nothing applied). The set here is the smallest the CI can
    /// afford that has the same shape (3 M keys: ~46 keys per level-16
    /// range, ~35 k disputed level-16 ranges for 60 k differences, ~310
    /// rounds of `Children` alone); the constants are the product's.
    #[test]
    fn a_large_gap_on_a_large_set_converges_because_sessions_keep_progress() {
        let mut rng = Rng(47);
        let base = KeySet::from_keys(random_set(&mut rng, 3_000_000));
        let owner = perturb(&mut rng, &base, 60_000);
        let before = symmetric_difference(&base, &owner);
        assert!(before > 50_000, "{before}");
        let summary = Summary {
            incarnation: 1,
            seq: 1,
            root: owner.root_fingerprint(),
            count: owner.len() as u64,
        };
        let mut mirror = base.clone();
        let mut left = before;
        let mut sessions = 0;
        while mirror != owner {
            sessions += 1;
            assert!(
                sessions <= 8,
                "no convergence after {} sessions: {left} of {before} differences left",
                sessions - 1
            );
            let mut s = Session::new(start_bits_for(mirror.len()));
            while let Some(q) = s.next_request(&mirror) {
                let reply = respond(&owner, summary, &q, REPLY_BUDGET);
                s.apply(&mut mirror, &reply).unwrap();
            }
            let now = symmetric_difference(&mirror, &owner);
            assert!(
                now < left,
                "session {sessions} used {} rounds and applied nothing ({now} differences left)",
                s.rounds
            );
            left = now;
        }
    }

    #[test]
    fn a_small_delta_on_a_large_set_is_cheap() {
        let mut rng = Rng(7);
        let base = KeySet::from_keys(random_set(&mut rng, 100_000));
        let owner = perturb(&mut rng, &base, 4);
        let mut mirror = base.clone();
        let (rounds, bytes) = sync_in_memory(&mut mirror, &owner, 0);
        assert!(mirror == owner);
        // Root, then three levels of children, then items: ≤ 6 rounds
        // and a few KiB, against ~800 KB for the keys themselves.
        assert!(rounds <= 6, "{rounds} rounds");
        assert!(bytes < 16 * 1024, "{bytes} bytes for 4 differences");
    }

    /// Initial sync from an empty mirror pages the owner's keys at
    /// REPLY_BUDGET per round.
    #[test]
    fn initial_sync_from_empty_is_paged_and_bounded() {
        let mut rng = Rng(8);
        for n in [1usize, 1000, 100_000] {
            let owner = KeySet::from_keys(random_set(&mut rng, n));
            let mut mirror = KeySet::new();
            let (rounds, bytes) = sync_in_memory(&mut mirror, &owner, start_bits_for(n));
            assert!(mirror == owner, "n={n}");
            let pages = (n * KEY_BYTES_ESTIMATE).div_ceil(REPLY_BUDGET);
            assert!(
                rounds <= pages + 8,
                "n={n}: {rounds} rounds for {pages} pages"
            );
            assert!(bytes <= n * 12 + 64 * 1024, "n={n}: {bytes} bytes");
        }
    }

    /// Mirror has entries the owner dropped entirely (owner emptied).
    #[test]
    fn owner_emptied_clears_the_mirror() {
        let mut rng = Rng(9);
        let mut mirror = KeySet::from_keys(random_set(&mut rng, 20_000));
        let owner = KeySet::new();
        let (rounds, _) = sync_in_memory(&mut mirror, &owner, 0);
        assert!(mirror.is_empty());
        assert!(rounds <= 2, "{rounds}");
    }

    #[test]
    fn every_reply_respects_the_budget_and_makes_progress() {
        let mut rng = Rng(10);
        let owner = KeySet::from_keys(random_set(&mut rng, 100_000));
        let empty = KeySet::new();
        let queries: Vec<Query> = Range::level(8)
            .into_iter()
            .map(|r| {
                let a = empty.acc(&r);
                Query {
                    range: r,
                    fp: a.fingerprint(),
                    count: 0,
                }
            })
            .collect();
        let summary = Summary {
            incarnation: 1,
            seq: 1,
            root: owner.root_fingerprint(),
            count: owner.len() as u64,
        };
        let reply = respond(&owner, summary, &queries, REPLY_BUDGET);
        assert!(reply.processed >= 1);
        let size = postcard::to_allocvec(&reply).unwrap().len();
        assert!(size <= REPLY_BUDGET + 256, "reply is {size} bytes");
        // A tiny budget still processes one query.
        let reply = respond(&owner, summary, &queries, 1);
        assert_eq!(reply.processed, 1);
    }

    #[test]
    fn a_non_progressing_or_inconsistent_reply_aborts_the_session() {
        let owner = KeySet::from_keys([1u64, 2, 3]);
        let mut mirror = KeySet::new();
        let summary = Summary {
            incarnation: 1,
            seq: 1,
            root: owner.root_fingerprint(),
            count: 3,
        };
        let mut s = Session::new(0);
        s.next_request(&mirror).unwrap();
        let empty = Reply {
            summary,
            processed: 0,
            answers: vec![],
        };
        assert_eq!(s.apply(&mut mirror, &empty), Err(SessionError::NoProgress));

        let mut s = Session::new(0);
        s.next_request(&mirror).unwrap();
        let wrong = Reply {
            summary,
            processed: 1,
            answers: vec![Answer::Items {
                range: Range { bits: 4, path: 1 },
                keys: vec![],
            }],
        };
        assert_eq!(
            s.apply(&mut mirror, &wrong),
            Err(SessionError::UnexpectedRange)
        );

        // Keys outside the answered range are refused, not inserted.
        let mut s = Session::new(4);
        s.next_request(&mirror).unwrap();
        let r = Range { bits: 4, path: 0 };
        let outside = Reply {
            summary,
            processed: 16,
            answers: vec![Answer::Items {
                range: r,
                keys: encode_keys(r.lo(), &[r.hi() + 1]),
            }],
        };
        assert_eq!(s.apply(&mut mirror, &outside), Err(SessionError::BadKeys));
        assert!(mirror.is_empty());
    }

    fn delta(owner_before: &KeySet, owner_after: &KeySet, inc: u64, seq: u64) -> Delta {
        let before: std::collections::BTreeSet<Key> = owner_before.iter().collect();
        let after: std::collections::BTreeSet<Key> = owner_after.iter().collect();
        let adds: Vec<Key> = after.difference(&before).copied().collect();
        let removes: Vec<Key> = before.difference(&after).copied().collect();
        Delta {
            incarnation: inc,
            seq,
            base: owner_before.root_fingerprint(),
            root: owner_after.root_fingerprint(),
            adds: encode_keys(0, &adds),
            removes: encode_keys(0, &removes),
        }
    }

    // ---- delta-log replay (plan 47 P3) ----

    /// An owner publishing `ticks` deltas of `per_tick` random edits each
    /// into `log`; returns the owner's set after every tick (index 0 is
    /// the starting set) and its summary.
    fn publish(
        rng: &mut Rng,
        start: &KeySet,
        ticks: usize,
        per_tick: usize,
        inc: u64,
        log: &mut DeltaLog,
        now: std::time::Instant,
    ) -> (Vec<KeySet>, Summary) {
        let mut sets = vec![start.clone()];
        for t in 0..ticks {
            let next = perturb(rng, &sets[t], per_tick);
            log.push(delta(&sets[t], &next, inc, t as u64 + 1), now);
            sets.push(next);
        }
        let last = sets.last().unwrap();
        let summary = Summary {
            incarnation: inc,
            seq: ticks as u64,
            root: last.root_fingerprint(),
            count: last.len() as u64,
        };
        (sets, summary)
    }

    /// Drive a replay the way the daemon does: request, apply, page.
    /// Returns the pages used, or `None` when the owner could not serve
    /// it or the result did not match its summary.
    fn replay_all(
        mirror: &mut Mirror,
        log: &DeltaLog,
        summary: Summary,
        budget: usize,
    ) -> Option<usize> {
        let (inc, mut after) = mirror.replay_point()?;
        for page in 1..=MAX_REPLAY_PAGES {
            let reply = respond_replay(log, summary, inc, after, budget);
            if !reply.available {
                return None;
            }
            after = mirror.apply_replay(&reply.deltas, after).ok()?;
            if after >= reply.summary.seq {
                return mirror.matches(&reply.summary).then_some(page);
            }
            if reply.deltas.is_empty() {
                return None;
            }
        }
        None
    }

    fn far() -> std::time::Instant {
        std::time::Instant::now()
    }

    fn big_log() -> DeltaLog {
        DeltaLog::new(1 << 30, std::time::Duration::from_secs(3600))
    }

    #[test]
    fn replay_repairs_a_small_gap_exactly() {
        let mut rng = Rng(21);
        let s0 = KeySet::from_keys(random_set(&mut rng, 5_000));
        let mut log = big_log();
        let (sets, summary) = publish(&mut rng, &s0, 6, 40, 9, &mut log, far());
        // The mirror followed seq 1 and 2 exactly, then missed 3..6.
        let mut m = Mirror {
            keys: sets[2].clone(),
            incarnation: Some(9),
            last_seq: 2,
            exact_seq: Some(2),
        };
        assert_eq!(
            replay_all(&mut m, &log, summary, REPLAY_PAGE_BUDGET),
            Some(1)
        );
        assert!(m.keys == sets[6]);
        assert_eq!(m.exact_seq, Some(6));
        assert_eq!(m.last_seq, 6);
        // Already current: nothing to replay, still verified.
        assert_eq!(
            replay_all(&mut m, &log, summary, REPLAY_PAGE_BUDGET),
            Some(1)
        );
    }

    #[test]
    fn replay_pages_a_large_backlog_within_the_budget() {
        let mut rng = Rng(22);
        let s0 = KeySet::from_keys(random_set(&mut rng, 20_000));
        let mut log = big_log();
        let (sets, summary) = publish(&mut rng, &s0, 40, 2_000, 5, &mut log, far());
        let mut m = Mirror {
            keys: s0.clone(),
            incarnation: Some(5),
            last_seq: 0,
            exact_seq: Some(0),
        };
        // A small budget forces several pages; every page respects it.
        let budget = 20 * 1024;
        let first = respond_replay(&log, summary, 5, 0, budget);
        let used: usize = first
            .deltas
            .iter()
            .map(|d| d.adds.len() + d.removes.len())
            .sum();
        assert!(first.deltas.len() < 40 && used <= budget);
        let pages = replay_all(&mut m, &log, summary, budget).expect("replay");
        assert!(pages > 1, "{pages}");
        assert!(m.keys == sets[40]);
        // Even a budget smaller than one delta makes progress.
        let mut m2 = Mirror {
            keys: s0,
            incarnation: Some(5),
            last_seq: 0,
            exact_seq: Some(0),
        };
        assert_eq!(replay_all(&mut m2, &log, summary, 1), Some(40));
        assert!(m2.keys == sets[40]);
    }

    /// The log bounds: size drops the oldest deltas, age drops the old
    /// ones, and a mirror older than what is left is refused (the
    /// daemon then runs a session), never served a gap.
    #[test]
    fn a_truncated_log_refuses_and_a_session_still_converges() {
        let mut rng = Rng(23);
        let s0 = KeySet::from_keys(random_set(&mut rng, 5_000));
        let t0 = far();
        let mut log = DeltaLog::new(3_000, std::time::Duration::from_secs(60));
        let (sets, summary) = publish(&mut rng, &s0, 20, 40, 3, &mut log, t0);
        assert!(
            log.len() < 20 && log.bytes() <= 3_000 + 600,
            "{} entries {} bytes",
            log.len(),
            log.bytes()
        );
        let mut m = Mirror {
            keys: s0.clone(),
            incarnation: Some(3),
            last_seq: 0,
            exact_seq: Some(0),
        };
        let reply = respond_replay(&log, summary, 3, 0, REPLAY_PAGE_BUDGET);
        assert!(!reply.available && reply.deltas.is_empty());
        assert_eq!(replay_all(&mut m, &log, summary, REPLAY_PAGE_BUDGET), None);
        // The fallback repairs it exactly.
        sync_in_memory(&mut m.keys, &sets[20], 0);
        assert!(m.keys == sets[20]);
        assert!(m.matches(&summary));
        assert_eq!(m.exact_seq, Some(20));
        // A mirror inside what the log still holds is served.
        let keep_from = 20 - log.len() as u64;
        let mut m = Mirror {
            keys: sets[keep_from as usize].clone(),
            incarnation: Some(3),
            last_seq: keep_from,
            exact_seq: Some(keep_from),
        };
        assert_eq!(
            replay_all(&mut m, &log, summary, REPLAY_PAGE_BUDGET),
            Some(1)
        );
        assert!(m.keys == sets[20]);

        // Age: everything older than the bound goes, the newest stays
        // only while it is young enough.
        let mut log = DeltaLog::new(1 << 30, std::time::Duration::from_secs(60));
        publish(&mut rng, &s0, 5, 10, 3, &mut log, t0);
        log.prune(t0 + std::time::Duration::from_secs(61));
        assert!(log.is_empty() && log.bytes() == 0);
    }

    #[test]
    fn a_restart_or_a_silent_change_leaves_no_replay() {
        let mut rng = Rng(24);
        let s0 = KeySet::from_keys(random_set(&mut rng, 2_000));
        let mut log = big_log();
        let (sets, summary) = publish(&mut rng, &s0, 4, 20, 11, &mut log, far());
        // Asking about the previous incarnation (the owner restarted:
        // new incarnation, empty log).
        let fresh = DeltaLog::new(1 << 20, std::time::Duration::from_secs(60));
        let restarted = Summary {
            incarnation: 12,
            seq: 0,
            ..summary
        };
        assert!(!respond_replay(&fresh, restarted, 11, 2, REPLAY_PAGE_BUDGET).available);
        assert!(!respond_replay(&log, summary, 10, 2, REPLAY_PAGE_BUDGET).available);
        // A mirror claiming to be ahead of the owner is refused.
        assert!(!respond_replay(&log, summary, 11, 99, REPLAY_PAGE_BUDGET).available);
        // A silent change (seq moved with no delta): the log is cleared
        // by the owner; a later delta starts a new run that does not
        // reach back to the old mirror.
        log.clear();
        let next = perturb(&mut rng, &sets[4], 30);
        let silent_seq = 5;
        let after_silent = perturb(&mut rng, &next, 10);
        log.push(delta(&next, &after_silent, 11, silent_seq + 1), far());
        let now = Summary {
            incarnation: 11,
            seq: silent_seq + 1,
            root: after_silent.root_fingerprint(),
            count: after_silent.len() as u64,
        };
        assert!(!respond_replay(&log, now, 11, 4, REPLAY_PAGE_BUDGET).available);
        // A mirror exact at the silent seq can use the new run.
        assert!(respond_replay(&log, now, 11, silent_seq, REPLAY_PAGE_BUDGET).available);
        // A discontinuous push restarts the log by itself.
        let mut gappy = big_log();
        gappy.push(delta(&sets[0], &sets[1], 11, 1), far());
        gappy.push(delta(&sets[2], &sets[3], 11, 3), far());
        assert_eq!(gappy.len(), 1);
    }

    /// Reordered gossip: delta 3 arrives before 2 and is applied off its
    /// base (the mirror stays at `exact_seq` 1), 2 is dropped as stale.
    /// Replay from the last exact point still ends exact, because a
    /// delta only says which keys are present or absent.
    #[test]
    fn replay_repairs_reordered_deltas() {
        let mut rng = Rng(25);
        let s0 = KeySet::from_keys(random_set(&mut rng, 3_000));
        let mut log = big_log();
        let (sets, summary) = publish(&mut rng, &s0, 4, 60, 8, &mut log, far());
        let mut m = Mirror {
            keys: sets[0].clone(),
            incarnation: Some(8),
            last_seq: 0,
            exact_seq: Some(0),
        };
        let d: Vec<Delta> = (1..=4)
            .map(|i| delta(&sets[i - 1], &sets[i], 8, i as u64))
            .collect();
        assert_eq!(m.apply_delta(&d[0]), DeltaOutcome::Exact);
        assert_eq!(m.apply_delta(&d[2]), DeltaOutcome::Diverged);
        assert_eq!(m.apply_delta(&d[1]), DeltaOutcome::Stale);
        assert_eq!(m.exact_seq, Some(1));
        assert_eq!(m.last_seq, 3);
        assert_eq!(
            replay_all(&mut m, &log, summary, REPLAY_PAGE_BUDGET),
            Some(1)
        );
        assert!(m.keys == sets[4]);
        assert_eq!((m.exact_seq, m.last_seq), (Some(4), 4));
    }

    /// A mirror that is wrong in a way the log cannot explain (a key
    /// removed locally after a declined fetch, say) must not be passed
    /// as exact: the fingerprint check fails and the caller falls back.
    #[test]
    fn replay_never_accepts_a_mirror_that_differs_from_the_owner() {
        let mut rng = Rng(26);
        let s0 = KeySet::from_keys(random_set(&mut rng, 3_000));
        let mut log = big_log();
        let (sets, summary) = publish(&mut rng, &s0, 3, 30, 4, &mut log, far());
        let victim = sets[0].iter().nth(7).unwrap();
        let mut dirty = sets[1].clone();
        dirty.remove(victim);
        let mut m = Mirror {
            keys: dirty,
            incarnation: Some(4),
            last_seq: 1,
            exact_seq: Some(1),
        };
        assert_eq!(replay_all(&mut m, &log, summary, REPLAY_PAGE_BUDGET), None);
        assert_eq!(m.exact_seq, Some(1), "exact point must not advance");
        sync_in_memory(&mut m.keys, &sets[3], 0);
        assert!(m.keys == sets[3]);
    }

    #[test]
    fn a_malformed_replay_page_is_refused_without_side_effects() {
        let mut rng = Rng(27);
        let s0 = KeySet::from_keys(random_set(&mut rng, 500));
        let mut log = big_log();
        let (sets, summary) = publish(&mut rng, &s0, 3, 10, 6, &mut log, far());
        let page = respond_replay(&log, summary, 6, 0, REPLAY_PAGE_BUDGET).deltas;
        let mut m = Mirror {
            keys: sets[0].clone(),
            incarnation: Some(6),
            last_seq: 0,
            exact_seq: Some(0),
        };
        // Wrong start (a hole), another incarnation, undecodable keys.
        assert_eq!(m.apply_replay(&page[1..], 0), Err(ReplayError::Malformed));
        let mut foreign = page.clone();
        foreign[1].incarnation = 7;
        assert_eq!(m.apply_replay(&foreign, 0), Err(ReplayError::Malformed));
        let mut junk = page.clone();
        junk[2].adds = vec![0xFF; 12];
        assert_eq!(m.apply_replay(&junk, 0), Err(ReplayError::Malformed));
        let mut huge = page;
        huge[0].adds = encode_keys(0, &(0..MAX_DELTA_KEYS as u64 + 1).collect::<Vec<_>>());
        assert_eq!(m.apply_replay(&huge, 0), Err(ReplayError::Malformed));
        assert!(m.keys == sets[0] && m.last_seq == 0);
        // A mirror with no known incarnation cannot replay at all.
        let mut blank = Mirror::default();
        assert_eq!(blank.apply_replay(&[], 0), Err(ReplayError::NotExact));
        assert_eq!(blank.replay_point(), None);
    }

    /// A new incarnation drops the exact point: the old keys stay (a
    /// restarted cache is mostly the same set) but replay is off until a
    /// session or a matching summary makes the mirror exact again.
    #[test]
    fn a_restart_clears_the_exact_point() {
        let mut rng = Rng(28);
        let s0 = KeySet::from_keys(random_set(&mut rng, 800));
        let s1 = perturb(&mut rng, &s0, 10);
        let mut m = Mirror {
            keys: s0.clone(),
            incarnation: Some(1),
            last_seq: 5,
            exact_seq: Some(5),
        };
        let d = delta(&s1, &s1, 2, 1);
        m.apply_delta(&d);
        assert_eq!(m.incarnation, Some(2));
        assert_eq!(m.replay_point(), None);
        let sum = Summary {
            incarnation: 2,
            seq: 9,
            root: m.keys.root_fingerprint(),
            count: m.keys.len() as u64,
        };
        assert!(m.matches(&sum));
        assert_eq!(m.replay_point(), Some((2, 9)));
    }

    #[test]
    fn chained_deltas_keep_the_mirror_exact_and_gaps_are_detected() {
        let mut rng = Rng(11);
        let s0 = KeySet::from_keys(random_set(&mut rng, 1000));
        let s1 = perturb(&mut rng, &s0, 10);
        let s2 = perturb(&mut rng, &s1, 10);
        let s3 = perturb(&mut rng, &s2, 10);
        let mut m = Mirror {
            keys: s0.clone(),
            incarnation: Some(7),
            last_seq: 0,
            exact_seq: Some(0),
        };
        assert_eq!(m.apply_delta(&delta(&s0, &s1, 7, 1)), DeltaOutcome::Exact);
        assert!(m.keys == s1);
        // Duplicate delivery is stale.
        assert_eq!(m.apply_delta(&delta(&s0, &s1, 7, 1)), DeltaOutcome::Stale);
        // Skip seq 2: seq 3 applies optimistically but reports divergence.
        assert_eq!(
            m.apply_delta(&delta(&s2, &s3, 7, 3)),
            DeltaOutcome::Diverged
        );
        // Late seq 2 is stale; a session repairs the gap.
        assert_eq!(m.apply_delta(&delta(&s1, &s2, 7, 2)), DeltaOutcome::Stale);
        sync_in_memory(&mut m.keys, &s3, 0);
        assert!(m.keys == s3);
        let sum = Summary {
            incarnation: 7,
            seq: 3,
            root: s3.root_fingerprint(),
            count: s3.len() as u64,
        };
        assert!(m.matches(&sum));
        // An owner restart resets seq but keeps the keys.
        let s4 = perturb(&mut rng, &s3, 3);
        assert_eq!(m.apply_delta(&delta(&s3, &s4, 8, 1)), DeltaOutcome::Exact);
        assert!(m.keys == s4);
        assert_eq!(m.incarnation, Some(8));
    }

    #[test]
    fn a_malformed_delta_is_refused_without_side_effects() {
        let mut m = Mirror::default();
        let d = Delta {
            incarnation: 1,
            seq: 1,
            base: KeySet::new().root_fingerprint(),
            root: [0; 16],
            adds: vec![0x80],
            removes: vec![],
        };
        assert_eq!(m.apply_delta(&d), DeltaOutcome::Malformed);
        assert!(m.keys.is_empty());
    }

    #[test]
    fn a_delta_over_the_key_cap_is_refused_without_side_effects() {
        let mut m = Mirror::default();
        // One key past the cap in the adds list alone: decodable, but
        // larger than any honest sender would ship.
        let adds: Vec<Key> = (1..=(MAX_DELTA_KEYS as u64 + 1)).collect();
        let d = Delta {
            incarnation: 1,
            seq: 1,
            base: KeySet::new().root_fingerprint(),
            root: [0; 16],
            adds: encode_keys(0, &adds),
            removes: vec![],
        };
        assert_eq!(m.apply_delta(&d), DeltaOutcome::Malformed);
        assert!(m.keys.is_empty(), "an oversized delta must not be applied");
        // Split across both lists so their sum trips the cap even though
        // neither alone would look outsized.
        let half = MAX_DELTA_KEYS as u64 / 2 + 1;
        let adds: Vec<Key> = (1..=half).collect();
        let removes: Vec<Key> = (half + 1..=half + half).collect();
        assert!(adds.len() + removes.len() > MAX_DELTA_KEYS);
        let d = Delta {
            incarnation: 1,
            seq: 1,
            base: KeySet::new().root_fingerprint(),
            root: [0; 16],
            adds: encode_keys(0, &adds),
            removes: encode_keys(0, &removes),
        };
        assert_eq!(m.apply_delta(&d), DeltaOutcome::Malformed);
        assert!(m.keys.is_empty());
    }

    /// Churn while a session is in flight: the owner changes between
    /// rounds. The session still terminates, and one more session on a
    /// quiescent owner makes the mirror exact.
    #[test]
    fn owner_churn_mid_session_converges_on_the_next_session() {
        let mut rng = Rng(12);
        let mut owner = KeySet::from_keys(random_set(&mut rng, 30_000));
        let mut mirror = perturb(&mut rng, &owner, 500);
        let summary = |o: &KeySet| Summary {
            incarnation: 1,
            seq: 1,
            root: o.root_fingerprint(),
            count: o.len() as u64,
        };
        let mut s = Session::new(start_bits_for(owner.len()));
        while let Some(q) = s.next_request(&mirror) {
            let reply = respond(&owner, summary(&owner), &q, REPLY_BUDGET);
            s.apply(&mut mirror, &reply).unwrap();
            owner = perturb(&mut rng, &owner, 20);
        }
        assert!(s.rounds <= MAX_ROUNDS);
        let (rounds, _) = sync_in_memory(&mut mirror, &owner, 0);
        assert!(mirror == owner);
        assert!(rounds <= 16, "{rounds}");
    }

    #[test]
    fn fingerprints_distinguish_near_identical_sets() {
        let a = KeySet::from_keys([1u64, 2, 3]);
        let b = KeySet::from_keys([1u64, 2, 4]);
        let c = KeySet::from_keys([0u64, 3]);
        assert_ne!(a.root_fingerprint(), b.root_fingerprint());
        assert_ne!(a.root_fingerprint(), c.root_fingerprint());
        // Same Σk, different Σmix(k): {1, 4} vs {2, 3}.
        let d = KeySet::from_keys([1u64, 4]);
        let e = KeySet::from_keys([2u64, 3]);
        assert_ne!(d.root_fingerprint(), e.root_fingerprint());
        assert_eq!(
            KeySet::new().root_fingerprint(),
            KeySet::from_keys(Vec::<Key>::new()).root_fingerprint()
        );
    }

    #[test]
    fn key_of_is_the_big_endian_hash_prefix() {
        let mut h = [0u8; 32];
        h[0] = 0x12;
        h[7] = 0x34;
        h[8] = 0xff;
        assert_eq!(key_of(&h), 0x1200_0000_0000_0034);
    }
}
