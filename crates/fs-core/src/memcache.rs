//! In-memory tier of the chunk cache: verified chunk contents, shared as
//! [`Bytes`], over [`crate::cache::DiskCache`] (follow-up to plan 31 C7b).
//!
//! # Why
//!
//! A disk-cache read loads the whole chunk file and blake3-verifies it on
//! every call. A kernel read is 4–128 KiB, a chunk 1–64 MiB, so a cached
//! sequential read re-read and re-hashed each chunk tens of times (C7b
//! measured ~100 MiB/s at 4 MiB chunks, 215–246 at 1 MiB). Here a chunk
//! is verified once, on the disk read that admits it, and later reads
//! share that copy: zero-copy `Bytes` slices, no disk I/O, no hash.
//!
//! # What an entry is
//!
//! Content-addressed bytes that passed the disk cache's hash check, so an
//! entry never goes stale. It is still dropped whenever the disk entry
//! goes (removed, evicted, pruned, found corrupt, forgotten): memory
//! entries are always a subset of the disk cache's entries, and the
//! owner ([`crate::cache::DiskCache`]) keeps that invariant under its
//! state lock, so the memory never holds bytes the disk accounting has
//! let go of.
//!
//! # Eviction: 2Q with a correlated-reference filter
//!
//! Two segments share one byte budget, as in 2Q (Johnson & Shasha, VLDB
//! '94) and segmented LRU:
//!
//! * **probation** — a FIFO every new entry enters (target: a quarter of
//!   the budget once the protected segment is full);
//! * **protected** — CLOCK (a hit sets a reference bit; the hand gives a
//!   referenced entry a second chance) for entries that showed reuse.
//!
//! An entry is promoted when it is re-referenced *uncorrelated* while in
//! probation, or when it is admitted again shortly after probation
//! evicted it (a bounded **ghost** list of recently evicted hashes, half
//! the budget's worth of bytes, keys only). The victim is taken from
//! probation while probation is over its target, otherwise from
//! protected.
//!
//! The filter is what makes this fit chunk-granular reads: a sequential
//! reader touches the same chunk 8–1024 times in a burst (one hit per
//! kernel read), which a plain frequency or LRU-promotion policy would
//! take for heavy reuse and so let one large `cat` flush the hot set.
//! A hit counts as reuse only if at least `budget / 8` bytes have been
//! admitted since the entry was admitted — the chunk has aged a good way
//! through probation, rather than being read through in one go. A
//! sequential scan therefore streams through probation (and the ghost
//! list) and never displaces protected entries; a hot random working
//! set gets promoted by either path and stays. Why not pure LRU: one
//! scan larger than the budget empties it. Why not S3-FIFO/TinyLFU as
//! is: their frequency counters see the burst as frequency.
//!
//! # Concurrency
//!
//! Lookups take a read lock on one of [`SHARDS`] hash-sharded maps and
//! touch only atomics (reference/reuse bits, the hit counter); a hit
//! never takes the policy lock. Admissions and removals — at most one per
//! disk read — serialise on one policy mutex, which also owns the
//! segment queues (hashes plus a generation; entries removed out of band
//! leave stale queue items that are skipped and periodically compacted).
//! Lock order: the owner's state lock, then the policy mutex, then a
//! shard lock.

use crate::chunk::ChunkHash;
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering::Relaxed};
use std::sync::{Mutex, RwLock};

/// Lookup shards (by the hash's first byte: content hashes are uniform).
pub const SHARDS: usize = 16;

const PROBATION: u8 = 0;
const PROTECTED: u8 = 1;

/// Stale queue items tolerated before a compaction pass.
const STALE_SLACK: usize = 64;

struct Slot {
    bytes: Bytes,
    /// Distinguishes this residency from an earlier one of the same hash
    /// in the segment queues.
    gen: u64,
    /// [`MemCache::admitted`] just after this entry's admission.
    admitted_at: u64,
    segment: AtomicU8,
    /// CLOCK bit, set by a hit (the protected segment's second chance).
    referenced: AtomicBool,
    /// An uncorrelated re-reference was seen while in probation.
    reused: AtomicBool,
}

/// One lookup shard, on its own cache line.
#[repr(align(64))]
#[derive(Default)]
struct Shard {
    map: RwLock<HashMap<ChunkHash, Slot>>,
    hits: AtomicU64,
}

#[derive(Default)]
struct Policy {
    probation: VecDeque<(ChunkHash, u64)>,
    protected: VecDeque<(ChunkHash, u64)>,
    probation_bytes: u64,
    protected_bytes: u64,
    entries: usize,
    /// Queue items whose entry was removed out of band.
    stale: usize,
    ghost: VecDeque<(ChunkHash, u64)>,
    /// hash -> (generation, size) of the live ghost record.
    ghost_index: HashMap<ChunkHash, (u64, u64)>,
    ghost_bytes: u64,
    next_gen: u64,
}

impl Policy {
    fn gen(&mut self) -> u64 {
        self.next_gen += 1;
        self.next_gen
    }

    fn used(&self) -> u64 {
        self.probation_bytes + self.protected_bytes
    }
}

/// Counters and occupancy of a [`MemCache`] (see `node.status`'s cache
/// section and `/metrics`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemCacheStats {
    pub budget_bytes: u64,
    pub used_bytes: u64,
    /// Of `used_bytes`, what the protected (reused) segment holds.
    pub protected_bytes: u64,
    pub entries: u64,
    /// Reads served from memory.
    pub hits: u64,
    /// Reads that had to load (and verify) the disk copy.
    pub misses: u64,
    /// Reads that waited for another reader's load of the same chunk
    /// instead of loading it again (single-flight).
    pub coalesced: u64,
    pub admissions: u64,
    pub evictions: u64,
    /// Entries moved to the protected segment (reuse in probation, or a
    /// re-admission the ghost list remembered).
    pub promotions: u64,
}

/// See the module doc.
pub struct MemCache {
    budget: u64,
    probation_target: u64,
    ghost_budget: u64,
    reuse_window: u64,
    max_entry: u64,
    shards: Box<[Shard]>,
    policy: Mutex<Policy>,
    /// Bytes admitted so far (monotonic): the clock the correlated-reference
    /// filter measures an entry's age in.
    admitted: AtomicU64,
    misses: AtomicU64,
    coalesced: AtomicU64,
    admissions: AtomicU64,
    evictions: AtomicU64,
    promotions: AtomicU64,
}

impl MemCache {
    /// A cache of at most `budget` bytes of chunk contents. Chunks larger
    /// than a quarter of it are never admitted (a budget smaller than a
    /// few chunks would only churn).
    pub fn new(budget: u64) -> MemCache {
        MemCache {
            budget,
            probation_target: budget / 4,
            ghost_budget: budget / 2,
            reuse_window: budget / 8,
            max_entry: budget / 4,
            shards: (0..SHARDS).map(|_| Shard::default()).collect(),
            policy: Mutex::new(Policy::default()),
            admitted: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            coalesced: AtomicU64::new(0),
            admissions: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            promotions: AtomicU64::new(0),
        }
    }

    pub fn budget(&self) -> u64 {
        self.budget
    }

    /// Whether a chunk of `size` bytes would be admitted at all.
    pub fn admits(&self, size: u64) -> bool {
        size > 0 && size <= self.max_entry
    }

    fn shard(&self, hash: &ChunkHash) -> &Shard {
        &self.shards[hash.0[0] as usize % SHARDS]
    }

    /// The resident copy of `hash`, counted as a hit. Never blocks on the
    /// policy lock.
    pub fn get(&self, hash: &ChunkHash) -> Option<Bytes> {
        let shard = self.shard(hash);
        let bytes = {
            let map = shard.map.read().unwrap();
            let slot = map.get(hash)?;
            if !slot.referenced.load(Relaxed) {
                slot.referenced.store(true, Relaxed);
            }
            if slot.segment.load(Relaxed) == PROBATION
                && !slot.reused.load(Relaxed)
                && self.admitted.load(Relaxed).wrapping_sub(slot.admitted_at) >= self.reuse_window
            {
                slot.reused.store(true, Relaxed);
            }
            slot.bytes.clone()
        };
        shard.hits.fetch_add(1, Relaxed);
        Some(bytes)
    }

    /// Residency, without counting or touching anything.
    pub fn contains(&self, hash: &ChunkHash) -> bool {
        self.shard(hash).map.read().unwrap().contains_key(hash)
    }

    /// A read had to load the disk copy.
    pub fn note_miss(&self) {
        self.misses.fetch_add(1, Relaxed);
    }

    /// A read waited for another reader's load of the same chunk.
    pub fn note_coalesced(&self) {
        self.coalesced.fetch_add(1, Relaxed);
    }

    /// Admit verified `bytes` for `hash` (the caller has checked them
    /// against the hash). Returns the evicted contents, for the caller to
    /// drop once it holds no lock (freeing a multi-MiB buffer is not
    /// free). A no-op if already resident or too large.
    #[must_use = "drop the evicted bytes outside any lock"]
    pub fn insert(&self, hash: ChunkHash, bytes: Bytes) -> Vec<Bytes> {
        let size = bytes.len() as u64;
        let mut dropped = Vec::new();
        if !self.admits(size) {
            return dropped;
        }
        let mut p = self.policy.lock().unwrap();
        if self.contains(&hash) {
            return dropped;
        }
        // Taken before making room: the evictions below may add to, and
        // trim, the ghost list. Its queue item goes stale (compaction
        // drops it).
        let ghost = p.ghost_index.remove(&hash);
        if let Some((_, ghost_size)) = ghost {
            p.ghost_bytes -= ghost_size;
        }
        let segment = if ghost.is_some() {
            PROTECTED
        } else {
            PROBATION
        };
        if !self.make_room(&mut p, size, segment, &mut dropped) {
            return dropped;
        }
        if ghost.is_some() {
            self.promotions.fetch_add(1, Relaxed);
        }
        let gen = p.gen();
        let admitted_at = self.admitted.fetch_add(size, Relaxed) + size;
        self.shard(&hash).map.write().unwrap().insert(
            hash,
            Slot {
                bytes,
                gen,
                admitted_at,
                segment: AtomicU8::new(segment),
                referenced: AtomicBool::new(false),
                reused: AtomicBool::new(false),
            },
        );
        if segment == PROTECTED {
            p.protected.push_back((hash, gen));
            p.protected_bytes += size;
        } else {
            p.probation.push_back((hash, gen));
            p.probation_bytes += size;
        }
        p.entries += 1;
        self.admissions.fetch_add(1, Relaxed);
        self.compact(&mut p);
        dropped
    }

    /// Drop `hash` (its disk entry went). Returns the bytes, for the
    /// caller to drop outside its locks.
    #[must_use = "drop the removed bytes outside any lock"]
    pub fn remove(&self, hash: &ChunkHash) -> Option<Bytes> {
        let mut p = self.policy.lock().unwrap();
        let slot = self.shard(hash).map.write().unwrap().remove(hash)?;
        let size = slot.bytes.len() as u64;
        if slot.segment.load(Relaxed) == PROTECTED {
            p.protected_bytes -= size;
        } else {
            p.probation_bytes -= size;
        }
        p.entries -= 1;
        p.stale += 1;
        self.compact(&mut p);
        Some(slot.bytes)
    }

    pub fn stats(&self) -> MemCacheStats {
        let (used, protected, entries) = {
            let p = self.policy.lock().unwrap();
            (p.used(), p.protected_bytes, p.entries as u64)
        };
        MemCacheStats {
            budget_bytes: self.budget,
            used_bytes: used,
            protected_bytes: protected,
            entries,
            hits: self.shards.iter().map(|s| s.hits.load(Relaxed)).sum(),
            misses: self.misses.load(Relaxed),
            coalesced: self.coalesced.load(Relaxed),
            admissions: self.admissions.load(Relaxed),
            evictions: self.evictions.load(Relaxed),
            promotions: self.promotions.load(Relaxed),
        }
    }

    /// Evict until `size` more bytes fit into `segment`. False (and
    /// nothing admitted) only if the accounting cannot make room, which
    /// a correct accounting never hits for an admissible size.
    fn make_room(&self, p: &mut Policy, size: u64, segment: u8, dropped: &mut Vec<Bytes>) -> bool {
        let protected_cap = self.budget - self.probation_target;
        let incoming_protected = if segment == PROTECTED { size } else { 0 };
        loop {
            let over_total = p.used() + size > self.budget;
            let over_protected = p.protected_bytes + incoming_protected > protected_cap;
            if !over_total && !over_protected {
                return true;
            }
            // Protected over its cap: from protected. Otherwise probation
            // while it is over its target (or protected is empty), else
            // protected; the other segment if the first has nothing.
            let first = if !over_protected
                && (p.probation_bytes > self.probation_target || p.protected.is_empty())
            {
                PROBATION
            } else {
                PROTECTED
            };
            let progressed = self.evict_from(p, first, dropped)
                || (!over_protected && self.evict_from(p, first ^ 1, dropped));
            if !progressed {
                return false;
            }
        }
    }

    fn evict_from(&self, p: &mut Policy, segment: u8, dropped: &mut Vec<Bytes>) -> bool {
        if segment == PROBATION {
            self.evict_probation(p, dropped)
        } else {
            self.evict_protected(p, dropped)
        }
    }

    /// Take probation's oldest entry: promote it if it showed reuse,
    /// otherwise evict it into the ghost list. False if probation is
    /// empty.
    fn evict_probation(&self, p: &mut Policy, dropped: &mut Vec<Bytes>) -> bool {
        while let Some((hash, gen)) = p.probation.pop_front() {
            let shard = self.shard(&hash);
            let (size, reused) = {
                let map = shard.map.read().unwrap();
                match map.get(&hash) {
                    Some(slot) if slot.gen == gen => {
                        (slot.bytes.len() as u64, slot.reused.load(Relaxed))
                    }
                    _ => {
                        p.stale = p.stale.saturating_sub(1);
                        continue;
                    }
                }
            };
            p.probation_bytes -= size;
            if reused {
                // Every mutation holds the policy lock: the slot seen
                // above is still the one in the map.
                if let Some(slot) = shard.map.read().unwrap().get(&hash) {
                    slot.segment.store(PROTECTED, Relaxed);
                    slot.referenced.store(false, Relaxed);
                }
                p.protected.push_back((hash, gen));
                p.protected_bytes += size;
                self.promotions.fetch_add(1, Relaxed);
            } else {
                let slot = shard.map.write().unwrap().remove(&hash).expect("live slot");
                p.entries -= 1;
                self.evictions.fetch_add(1, Relaxed);
                dropped.push(slot.bytes);
                self.remember(p, hash, size);
            }
            return true;
        }
        false
    }

    /// CLOCK over the protected segment: a referenced entry has its bit
    /// cleared and goes round again; the first unreferenced one is
    /// evicted (not remembered: it already had its chance). False if the
    /// segment is empty.
    fn evict_protected(&self, p: &mut Policy, dropped: &mut Vec<Bytes>) -> bool {
        // Bound the sweep: hits racing the hand could otherwise keep
        // re-setting bits forever. Past it, the entry under the hand goes.
        let mut chances = p.protected.len();
        while let Some((hash, gen)) = p.protected.pop_front() {
            let shard = self.shard(&hash);
            let referenced = {
                let map = shard.map.read().unwrap();
                match map.get(&hash) {
                    Some(slot) if slot.gen == gen => slot.referenced.swap(false, Relaxed),
                    _ => {
                        p.stale = p.stale.saturating_sub(1);
                        continue;
                    }
                }
            };
            if referenced && chances > 0 {
                chances -= 1;
                p.protected.push_back((hash, gen));
                continue;
            }
            let slot = shard.map.write().unwrap().remove(&hash).expect("live slot");
            p.protected_bytes -= slot.bytes.len() as u64;
            p.entries -= 1;
            self.evictions.fetch_add(1, Relaxed);
            dropped.push(slot.bytes);
            return true;
        }
        false
    }

    /// Record an eviction from probation in the ghost list.
    fn remember(&self, p: &mut Policy, hash: ChunkHash, size: u64) {
        let gen = p.gen();
        if let Some((_, old)) = p.ghost_index.insert(hash, (gen, size)) {
            p.ghost_bytes -= old;
        }
        p.ghost.push_back((hash, gen));
        p.ghost_bytes += size;
        while p.ghost_bytes > self.ghost_budget {
            let Some((old, old_gen)) = p.ghost.pop_front() else {
                break;
            };
            if let Some(&(live_gen, old_size)) = p.ghost_index.get(&old) {
                if live_gen == old_gen {
                    p.ghost_index.remove(&old);
                    p.ghost_bytes -= old_size;
                }
            }
        }
    }

    /// Drop stale queue items once they outnumber the live ones.
    fn compact(&self, p: &mut Policy) {
        if p.stale > STALE_SLACK && p.stale > p.entries {
            let live = |(hash, gen): &(ChunkHash, u64)| {
                self.shard(hash)
                    .map
                    .read()
                    .unwrap()
                    .get(hash)
                    .is_some_and(|slot| slot.gen == *gen)
            };
            p.probation.retain(live);
            p.protected.retain(live);
            p.stale = 0;
        }
        if p.ghost.len() > 2 * p.ghost_index.len() + STALE_SLACK {
            let Policy {
                ghost, ghost_index, ..
            } = p;
            ghost.retain(|(hash, gen)| ghost_index.get(hash).is_some_and(|(g, _)| g == gen));
        }
    }

    /// Internal consistency, for tests: the policy's byte and entry
    /// accounting matches the maps.
    #[cfg(test)]
    fn check(&self) {
        let p = self.policy.lock().unwrap();
        let (mut probation, mut protected, mut entries) = (0, 0, 0);
        for shard in self.shards.iter() {
            for slot in shard.map.read().unwrap().values() {
                entries += 1;
                if slot.segment.load(Relaxed) == PROTECTED {
                    protected += slot.bytes.len() as u64;
                } else {
                    probation += slot.bytes.len() as u64;
                }
            }
        }
        assert_eq!(p.entries, entries, "entry count");
        assert_eq!(p.probation_bytes, probation, "probation bytes");
        assert_eq!(p.protected_bytes, protected, "protected bytes");
        assert!(p.used() <= self.budget, "over budget");
        let ghost: u64 = p.ghost_index.values().map(|(_, size)| size).sum();
        assert_eq!(p.ghost_bytes, ghost, "ghost bytes");
        assert!(p.ghost_bytes <= self.ghost_budget, "ghost over budget");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KIB: u64 = 1024;

    fn chunk(i: u32, size: u64) -> (ChunkHash, Bytes) {
        let mut data = i.to_le_bytes().to_vec();
        data.resize(size as usize, 0);
        (ChunkHash::of(&data), Bytes::from(data))
    }

    fn admit(c: &MemCache, i: u32, size: u64) -> ChunkHash {
        let (hash, bytes) = chunk(i, size);
        drop(c.insert(hash, bytes));
        hash
    }

    #[test]
    fn a_hit_shares_the_admitted_bytes() {
        let c = MemCache::new(64 * KIB);
        let (hash, bytes) = chunk(1, 4 * KIB);
        let ptr = bytes.as_ptr();
        drop(c.insert(hash, bytes));
        let got = c.get(&hash).unwrap();
        assert_eq!(got.as_ptr(), ptr, "a hit is the resident copy, not a copy");
        let stats = c.stats();
        assert_eq!(
            (stats.hits, stats.entries, stats.used_bytes),
            (1, 1, 4 * KIB)
        );
        c.check();
    }

    #[test]
    fn the_budget_is_never_exceeded() {
        let c = MemCache::new(64 * KIB);
        for i in 0..1000 {
            admit(&c, i, (1 + u64::from(i) % 16) * KIB);
            assert!(c.stats().used_bytes <= 64 * KIB);
        }
        c.check();
        let stats = c.stats();
        assert!(stats.evictions > 0);
        assert_eq!(stats.admissions, 1000);
    }

    #[test]
    fn oversized_and_empty_chunks_are_not_admitted() {
        let c = MemCache::new(64 * KIB);
        let big = admit(&c, 1, 16 * KIB + 1);
        let empty = admit(&c, 2, 0);
        assert!(!c.contains(&big) && !c.contains(&empty));
        assert!(c.contains(&admit(&c, 3, 16 * KIB)));
        let off = MemCache::new(0);
        assert!(!off.contains(&admit(&off, 4, 1)));
        c.check();
    }

    #[test]
    fn removal_releases_the_bytes() {
        let c = MemCache::new(64 * KIB);
        let hash = admit(&c, 1, 8 * KIB);
        assert!(c.remove(&hash).is_some());
        assert!(c.get(&hash).is_none());
        assert!(c.remove(&hash).is_none());
        assert_eq!(c.stats().used_bytes, 0);
        c.check();
    }

    #[test]
    fn probation_is_fifo_when_nothing_is_reused() {
        // Eight 8 KiB entries fill 64 KiB; a ninth evicts the first.
        let c = MemCache::new(64 * KIB);
        let hashes: Vec<_> = (0..9).map(|i| admit(&c, i, 8 * KIB)).collect();
        assert!(!c.contains(&hashes[0]));
        assert!(hashes[1..].iter().all(|h| c.contains(h)));
        c.check();
    }

    /// The point of the policy: a hot set that showed reuse survives a
    /// sequential scan many times the budget, each chunk of which is read
    /// many times in a burst (one hit per kernel read).
    #[test]
    fn a_bursty_sequential_scan_does_not_flush_the_hot_set() {
        let chunk_size = 4 * KIB;
        let c = MemCache::new(256 * KIB); // 64 chunks
                                          // Hot set: 24 chunks (96 KiB), touched round-robin while other
                                          // chunks come and go, until each has been promoted.
        let hot: Vec<_> = (0..24).map(|i| admit(&c, i, chunk_size)).collect();
        for round in 0..8u32 {
            for i in 0..8 {
                admit(&c, 10_000 + round * 8 + i, chunk_size);
            }
            for h in &hot {
                if c.get(h).is_none() {
                    // Evicted to the ghost list; re-admission promotes.
                    let i = hot.iter().position(|x| x == h).unwrap() as u32;
                    admit(&c, i, chunk_size);
                }
            }
        }
        assert!(hot.iter().all(|h| c.contains(h)), "hot set resident");
        assert!(
            c.stats().protected_bytes >= 24 * chunk_size,
            "hot set promoted"
        );

        // The scan: 1024 chunks (16x the budget), each read 32 times back
        // to back, the way 128 KiB kernel reads walk a 4 MiB chunk.
        for i in 0..1024u32 {
            let h = admit(&c, 100_000 + i, chunk_size);
            for _ in 0..32 {
                assert!(c.get(&h).is_some(), "a chunk survives its own burst");
            }
            // The hot set is still being used, occasionally.
            if i % 64 == 0 {
                for h in &hot {
                    let _ = c.get(h);
                }
            }
        }
        let resident = hot.iter().filter(|h| c.contains(h)).count();
        assert_eq!(resident, hot.len(), "the scan flushed the hot set");
        c.check();
    }

    /// Plain LRU would lose the hot set here; the same workload through a
    /// cache whose hot set never showed uncorrelated reuse loses it too —
    /// the filter, not luck, is what protects it above.
    #[test]
    fn without_demonstrated_reuse_a_scan_does_evict() {
        let c = MemCache::new(256 * KIB);
        let cold: Vec<_> = (0..24).map(|i| admit(&c, i, 4 * KIB)).collect();
        for i in 0..1024u32 {
            let h = admit(&c, 100_000 + i, 4 * KIB);
            for _ in 0..32 {
                let _ = c.get(&h);
            }
        }
        assert!(cold.iter().all(|h| !c.contains(h)));
        assert_eq!(c.stats().protected_bytes, 0, "no scan chunk was promoted");
        c.check();
    }

    #[test]
    fn readmission_from_the_ghost_list_promotes() {
        let c = MemCache::new(64 * KIB);
        let first = admit(&c, 0, 8 * KIB);
        for i in 1..9 {
            admit(&c, i, 8 * KIB);
        }
        assert!(!c.contains(&first), "evicted from probation");
        admit(&c, 0, 8 * KIB);
        assert!(c.contains(&first));
        let stats = c.stats();
        assert_eq!(stats.promotions, 1);
        assert_eq!(stats.protected_bytes, 8 * KIB);
        c.check();
    }

    #[test]
    fn stale_queue_items_are_compacted() {
        let c = MemCache::new(1 << 20);
        for round in 0..20u32 {
            let hashes: Vec<_> = (0..50).map(|i| admit(&c, round * 50 + i, KIB)).collect();
            for h in &hashes {
                drop(c.remove(h));
            }
        }
        let p = c.policy.lock().unwrap();
        assert!(
            p.probation.len() <= STALE_SLACK + 1,
            "{}",
            p.probation.len()
        );
        drop(p);
        c.check();
    }

    #[test]
    fn concurrent_hits_admissions_and_removals_keep_the_accounting() {
        let c = std::sync::Arc::new(MemCache::new(256 * KIB));
        let threads: Vec<_> = (0..8u32)
            .map(|t| {
                let c = c.clone();
                std::thread::spawn(move || {
                    for i in 0..2000u32 {
                        let key = (t * 7 + i) % 300;
                        let (hash, bytes) = chunk(key, (1 + u64::from(key % 8)) * KIB);
                        match i % 5 {
                            0 => drop(c.remove(&hash)),
                            1 | 2 => drop(c.insert(hash, bytes)),
                            _ => {
                                if let Some(got) = c.get(&hash) {
                                    assert_eq!(got, bytes, "a hit returns the admitted bytes");
                                }
                            }
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        c.check();
    }
}
