//! The index against brute force over materialized snapshot multisets.

use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};

/// splitmix64: seeded, dependency-free, good enough to shuffle a model.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

fn hash(chunk: u32) -> ChunkHash {
    ChunkHash(*blake3::hash(&chunk.to_le_bytes()).as_bytes())
}

type Content = BTreeMap<u32, u64>;

const PARAMS: SnapAcctParams = SnapAcctParams {
    gc_horizon_ms: 1_000,
    gc_interval_ms: 500,
    cache_bytes: 4 << 20,
};

fn open_temp() -> (SnapAcct, tempfile::TempDir, Arc<AtomicU64>) {
    let dir = tempfile::tempdir().unwrap();
    let (ix, opened) = SnapAcct::open(&dir.path().join("snapacct"), "fs-test", PARAMS).unwrap();
    assert_eq!(opened, Opened::Fresh);
    let now = Arc::new(AtomicU64::new(1_000_000));
    let clock = now.clone();
    let ix = ix.with_clock(move || clock.load(Ordering::Relaxed));
    (ix, dir, now)
}

struct ModelSnap {
    id: String,
    ord: u32,
    content: Content,
    lsize: u64,
}

struct ModelChain {
    chain: u32,
    snaps: Vec<ModelSnap>,
    /// The chain directory's live content, which snapshots freeze.
    work: Content,
    work_lsize: u64,
    /// Chunks this chain once held: re-adding one is an afterlife.
    dropped: Vec<u32>,
}

struct World {
    ix: SnapAcct,
    _dir: tempfile::TempDir,
    now: Arc<AtomicU64>,
    rng: Rng,
    sizes: Vec<u64>,
    chains: Vec<ModelChain>,
    live: BTreeSet<u32>,
    /// chunk → (size, since_ms)
    tombs: BTreeMap<u32, (u64, u64)>,
    next_id: u64,
    /// The commit sequence of the last mutation, which every mutation
    /// commits as `accounted_seq`.
    seq: u64,
    log: Vec<String>,
    stats: BTreeMap<&'static str, u64>,
}

fn deltas(sizes: &[u64], from: &Content, to: &Content) -> Vec<Delta> {
    let mut out = Vec::new();
    for (&chunk, &occ) in to {
        let was = from.get(&chunk).copied().unwrap_or(0);
        if occ != was {
            out.push(Delta {
                hash: hash(chunk),
                delta: occ as i64 - was as i64,
                size_bytes: sizes[chunk as usize],
            });
        }
    }
    for (&chunk, &was) in from {
        if !to.contains_key(&chunk) {
            out.push(Delta {
                hash: hash(chunk),
                delta: -(was as i64),
                size_bytes: sizes[chunk as usize],
            });
        }
    }
    out
}

/// Noise the index must see through: the same per-hash net change
/// spelled as several entries for one hash — a delta split in two (e.g.
/// `[(h, −2), (h, +1)]` for −1), or a `[(h, +1), (h, −1)]` pair for a
/// chunk that does not change — then shuffled.
fn noisy(
    rng: &mut Rng,
    stats: &mut BTreeMap<&'static str, u64>,
    sizes: &[u64],
    mut ds: Vec<Delta>,
) -> Vec<Delta> {
    if !rng.chance(30) {
        return ds;
    }
    *stats.entry("duplicate-hash deltas").or_default() += 1;
    if !ds.is_empty() && rng.chance(70) {
        let pick = rng.below(ds.len() as u64) as usize;
        let x = rng.below(7) as i64 - 3;
        let x = if x == 0 { 1 } else { x };
        let mut tail = ds[pick];
        ds[pick].delta -= x;
        tail.delta = x;
        ds.push(tail);
    } else {
        let chunk = rng.below(sizes.len() as u64) as u32;
        for d in [1, -1] {
            ds.push(Delta {
                hash: hash(chunk),
                delta: d,
                size_bytes: sizes[chunk as usize],
            });
        }
    }
    for i in (1..ds.len()).rev() {
        ds.swap(i, rng.below(i as u64 + 1) as usize);
    }
    ds
}

impl World {
    fn new(seed: u64, chunks: u32) -> World {
        let (ix, dir, now) = open_temp();
        let mut rng = Rng(seed);
        let sizes = (0..chunks).map(|_| 1 + rng.below(5000)).collect();
        let mut world = World {
            ix,
            _dir: dir,
            now,
            rng,
            sizes,
            chains: Vec::new(),
            live: BTreeSet::new(),
            tombs: BTreeMap::new(),
            next_id: 0,
            seq: 0,
            log: Vec::new(),
            stats: BTreeMap::new(),
        };
        for _ in 0..3 {
            world.add_chain();
        }
        world
    }

    fn next_seq(&mut self) -> Option<u64> {
        self.seq += 1 + self.rng.below(3);
        Some(self.seq)
    }

    fn add_chain(&mut self) {
        let ino = 1000 + self.chains.len() as u64;
        let chain = self.ix.register_chain(ino).unwrap();
        assert_eq!(self.ix.register_chain(ino).unwrap(), chain);
        assert_eq!(self.ix.chain_of(ino).unwrap(), Some(chain));
        self.chains.push(ModelChain {
            chain,
            snaps: Vec::new(),
            work: Content::new(),
            work_lsize: 0,
            dropped: Vec::new(),
        });
    }

    fn referenced(&self) -> BTreeSet<u32> {
        self.chains
            .iter()
            .flat_map(|c| c.snaps.iter().flat_map(|s| s.content.keys().copied()))
            .collect()
    }

    fn live_oracle(&self) -> impl FnMut(&ChunkHash) -> bool + 'static {
        let live: HashSet<ChunkHash> = self.live.iter().map(|&c| hash(c)).collect();
        move |h| live.contains(h)
    }

    /// Mirror the index's tombstone rule across one operation.
    fn settle(&mut self, before: &BTreeSet<u32>) {
        let after = self.referenced();
        let now = self.now.load(Ordering::Relaxed);
        for &chunk in before.difference(&after) {
            if !self.live.contains(&chunk) {
                let size = self.sizes[chunk as usize];
                self.tombs.entry(chunk).or_insert((size, now));
            }
        }
        for chunk in after.difference(before) {
            self.tombs.remove(chunk);
        }
    }

    fn fresh_id(&mut self) -> String {
        self.next_id += 1;
        format!("snap-{}", self.next_id)
    }

    /// Random edits to a multiset: new chunks from the global pool
    /// (shared across chains: dedup, nested roots), removals, and
    /// afterlife re-adds.
    fn mutate(&mut self, mut content: Content, dropped: &mut Vec<u32>) -> Content {
        let edits = self.rng.below(6);
        for _ in 0..edits {
            match self.rng.below(10) {
                0..=3 => {
                    let chunk = self.rng.below(self.sizes.len() as u64) as u32;
                    *content.entry(chunk).or_default() += 1 + self.rng.below(2);
                }
                4..=7 if !content.is_empty() => {
                    let pick = self.rng.below(content.len() as u64) as usize;
                    let (&chunk, &occ) = content.iter().nth(pick).unwrap();
                    let take = 1 + self.rng.below(occ);
                    if take == occ {
                        content.remove(&chunk);
                        dropped.push(chunk);
                    } else {
                        content.insert(chunk, occ - take);
                    }
                }
                _ if !dropped.is_empty() => {
                    let chunk = dropped[self.rng.below(dropped.len() as u64) as usize];
                    if !content.contains_key(&chunk) {
                        *self.stats.entry("afterlife re-add").or_default() += 1;
                    }
                    *content.entry(chunk).or_default() += 1;
                }
                _ => {}
            }
        }
        content
    }

    fn create(&mut self, c: usize) {
        let before = self.referenced();
        let mut dropped = std::mem::take(&mut self.chains[c].dropped);
        let work = self.mutate(self.chains[c].work.clone(), &mut dropped);
        let lsize =
            (self.chains[c].work_lsize as i64 + self.rng.below(2001) as i64 - 1000).max(0) as u64;
        let id = self.fresh_id();
        let mut live = self.live_oracle();
        let seq = self.next_seq();
        let ds = {
            let empty = Content::new();
            let from = self.chains[c].snaps.last().map_or(&empty, |h| &h.content);
            deltas(&self.sizes, from, &work)
        };
        let ds = noisy(&mut self.rng, &mut self.stats, &self.sizes, ds);
        let chain = &mut self.chains[c];
        chain.dropped = dropped;
        chain.work = work.clone();
        chain.work_lsize = lsize;
        let ord = match chain.snaps.last() {
            None => self
                .ix
                .apply_first(chain.chain, &id, "root", lsize, ds, &mut live, seq)
                .unwrap(),
            Some(head) => self
                .ix
                .apply_created(
                    chain.chain,
                    &id,
                    "root",
                    lsize as i64 - head.lsize as i64,
                    ds,
                    &mut live,
                    seq,
                )
                .unwrap(),
        };
        assert!(chain.snaps.last().is_none_or(|h| h.ord < ord));
        *self.stats.entry("create").or_default() += 1;
        let shared = self.chains.iter().enumerate().any(|(i, other)| {
            i != c
                && other
                    .snaps
                    .iter()
                    .any(|s| s.content.keys().any(|k| work.contains_key(k)))
        });
        if shared {
            *self
                .stats
                .entry("create sharing across chains")
                .or_default() += 1;
        }
        let chain = &mut self.chains[c];
        self.log.push(format!("create {c} -> {ord} {work:?}"));
        chain.snaps.push(ModelSnap {
            id,
            ord,
            content: work,
            lsize,
        });
        self.settle(&before);
    }

    fn delete(&mut self, c: usize, pos: usize) {
        let before = self.referenced();
        let seq = self.next_seq();
        let chain = &mut self.chains[c];
        let ord = chain.snaps[pos].ord;
        let head_step = (pos + 1 == chain.snaps.len() && pos > 0).then(|| {
            deltas(
                &self.sizes,
                &chain.snaps[pos - 1].content,
                &chain.snaps[pos].content,
            )
        });
        let located = self.ix.locate(&chain.snaps[pos].id).unwrap();
        assert_eq!(located, Some((chain.chain, ord)));
        self.ix
            .apply_deleted(chain.chain, ord, head_step, seq)
            .unwrap();
        let kind = match (pos, chain.snaps.len()) {
            (_, 1) => "delete only",
            (0, _) => "delete tail",
            (p, n) if p + 1 == n => "delete head",
            _ => "delete middle",
        };
        *self.stats.entry(kind).or_default() += 1;
        self.log.push(format!("delete {c} pos {pos} ord {ord}"));
        chain.snaps.remove(pos);
        self.settle(&before);
    }

    /// Insert a snapshot before position `pos` (a late-applied row).
    fn insert(&mut self, c: usize, pos: usize) {
        let before = self.referenced();
        let mut dropped = std::mem::take(&mut self.chains[c].dropped);
        let base = self.chains[c].snaps[pos].content.clone();
        let content = self.mutate(base, &mut dropped);
        let lsize = self.rng.below(50_000);
        let id = self.fresh_id();
        let mut live = self.live_oracle();
        let seq = self.next_seq();
        let sizes = self.sizes.clone();
        let chain = &mut self.chains[c];
        chain.dropped = dropped;
        let empty = Content::new();
        let after = pos.checked_sub(1).map(|p| chain.snaps[p].ord);
        let head = chain.snaps.last().unwrap();
        let rewind = match pos.checked_sub(1) {
            Some(p) => noisy(
                &mut self.rng,
                &mut self.stats,
                &sizes,
                deltas(&sizes, &chain.snaps[p].content, &head.content),
            ),
            None => Vec::new(),
        };
        chain.snaps.insert(
            pos,
            ModelSnap {
                id,
                ord: 0,
                content,
                lsize,
            },
        );
        let mut suffix = Vec::new();
        for i in pos..chain.snaps.len() {
            let (prev, prev_lsize) = match i.checked_sub(1) {
                Some(p) => (&chain.snaps[p].content, chain.snaps[p].lsize),
                None => (&empty, 0),
            };
            let s = &chain.snaps[i];
            suffix.push(NewSnapshot {
                id: s.id.clone(),
                root: "root".into(),
                lsize_delta: s.lsize as i64 - prev_lsize as i64,
                deltas: noisy(
                    &mut self.rng,
                    &mut self.stats,
                    &sizes,
                    deltas(&sizes, prev, &s.content),
                ),
            });
        }
        let ords = self
            .ix
            .rederive_suffix(chain.chain, after, rewind, suffix, &mut live, seq)
            .unwrap();
        for (s, ord) in chain.snaps[pos..].iter_mut().zip(&ords) {
            s.ord = *ord;
        }
        *self.stats.entry("out-of-order insert").or_default() += 1;
        self.log
            .push(format!("insert {c} before pos {pos} -> {ords:?}"));
        self.settle(&before);
    }

    fn toggle_live(&mut self) {
        let chunk = self.rng.below(self.sizes.len() as u64) as u32;
        let live = !self.live.contains(&chunk);
        if live {
            self.live.insert(chunk);
            self.tombs.remove(&chunk);
        } else {
            self.live.remove(&chunk);
        }
        let seq = self.next_seq();
        self.ix.set_live(hash(chunk), live, seq).unwrap();
        *self.stats.entry("live toggle").or_default() += 1;
        self.log.push(format!("live {chunk} = {live}"));
    }

    fn tick(&mut self) {
        let step = self.rng.below(1500);
        let now = self.now.fetch_add(step, Ordering::Relaxed) + step;
        let cutoff = now.saturating_sub(PARAMS.gc_horizon_ms + PARAMS.gc_interval_ms);
        let before = self.tombs.len();
        self.tombs.retain(|_, (_, since)| *since > cutoff);
        let dropped = self.ix.expire_tombstones().unwrap();
        assert_eq!(dropped as usize, before - self.tombs.len());
        *self.stats.entry("tombstones expired").or_default() += dropped;
        self.log.push(format!("tick +{step}"));
    }

    fn step(&mut self) {
        let c = self.rng.below(self.chains.len() as u64) as usize;
        let n = self.chains[c].snaps.len();
        match self.rng.below(100) {
            0..=44 => self.create(c),
            45..=59 if n > 0 => {
                // Middle, head, tail, in roughly equal measure.
                let pos = match self.rng.below(3) {
                    0 => n - 1,
                    1 => 0,
                    _ => self.rng.below(n as u64) as usize,
                };
                self.delete(c, pos);
            }
            60..=74 => self.toggle_live(),
            75..=84 if n > 0 => {
                let pos = self.rng.below(n as u64) as usize;
                self.insert(c, pos);
            }
            85..=92 => self.tick(),
            93 if self.chains.len() < 5 => self.add_chain(),
            _ => self.create(c),
        }
    }

    // ----------------------------------------------------- brute force

    fn referrers(&self) -> BTreeMap<u32, BTreeSet<(u32, u32)>> {
        let mut out: BTreeMap<u32, BTreeSet<(u32, u32)>> = BTreeMap::new();
        for chain in &self.chains {
            for s in &chain.snaps {
                for &chunk in s.content.keys() {
                    out.entry(chunk).or_default().insert((chain.chain, s.ord));
                }
            }
        }
        out
    }

    fn brute_reclaim(&self, d: &BTreeSet<(u32, u32)>) -> Amount {
        let mut out = Amount::default();
        for (chunk, refs) in self.referrers() {
            if !self.live.contains(&chunk) && refs.is_subset(d) {
                out.bytes += self.sizes[chunk as usize];
                out.chunks += 1;
            }
        }
        out
    }

    fn check(&mut self) {
        let log = self.log[self.log.len().saturating_sub(12)..].join("\n  ");
        let ctx = || log.clone();
        if let Err(e) = self.ix.check_structure() {
            panic!("structure: {e}\nlast ops:\n  {}", ctx());
        }
        assert_eq!(self.ix.accounted_seq().unwrap(), self.seq, "{}", ctx());
        let referrers = self.referrers();
        let sizes = self.sizes.clone();
        let size = |chunk: &u32| sizes[*chunk as usize];
        let mut unique = Amount::default();
        for chain in &self.chains {
            let mut used_sum = 0;
            let mut written_sum = 0;
            for (p, s) in chain.snaps.iter().enumerate() {
                let refer: u64 = s.content.keys().map(size).sum();
                let written: u64 = match p.checked_sub(1) {
                    Some(q) => s
                        .content
                        .keys()
                        .filter(|k| !chain.snaps[q].content.contains_key(k))
                        .map(size)
                        .sum(),
                    None => refer,
                };
                let owned: Vec<u32> = s
                    .content
                    .keys()
                    .copied()
                    .filter(|k| !self.live.contains(k) && referrers[k].len() == 1)
                    .collect();
                let used: u64 = owned.iter().map(size).sum();
                unique.bytes += used;
                unique.chunks += owned.len() as u64;
                used_sum += used;
                written_sum += written;
                let got = self.ix.snap_numbers(chain.chain, s.ord).unwrap().unwrap();
                let want = SnapNumbers {
                    chain: chain.chain,
                    ord: s.ord,
                    id: s.id.clone(),
                    root: "root".into(),
                    used,
                    written,
                    refer,
                    lsize: s.lsize,
                };
                assert_eq!(
                    got,
                    want,
                    "snapshot {}/{}; last ops:\n  {}",
                    chain.chain,
                    s.ord,
                    ctx()
                );
            }
            let numbers = self.ix.chain_numbers(chain.chain).unwrap().unwrap();
            assert_eq!(
                (numbers.snapshots as usize, numbers.used, numbers.written),
                (chain.snaps.len(), used_sum, written_sum),
                "chain {}",
                chain.chain
            );
            assert_eq!(numbers.head, chain.snaps.last().map(|s| s.ord));
            assert_eq!(numbers.first, chain.snaps.first().map(|s| s.ord));
            let listed: Vec<u32> = self
                .ix
                .chain_snapshots(chain.chain)
                .unwrap()
                .iter()
                .map(|n| n.ord)
                .collect();
            assert_eq!(
                listed,
                chain.snaps.iter().map(|s| s.ord).collect::<Vec<_>>()
            );
        }

        // The run invariant, chunk by chunk.
        for chunk in 0..self.sizes.len() as u32 {
            let entry = self.ix.chunk_entry(&hash(chunk)).unwrap();
            let Some(refs) = referrers.get(&chunk) else {
                assert!(entry.is_none(), "chunk {chunk} unreferenced but indexed");
                continue;
            };
            let entry = entry.unwrap_or_else(|| panic!("chunk {chunk} referenced, not indexed"));
            assert_eq!(entry.live, self.live.contains(&chunk));
            assert_eq!(entry.size, self.sizes[chunk as usize]);
            for chain in &self.chains {
                let head = chain.snaps.last().map(|s| s.ord);
                let mut covered = BTreeSet::new();
                for run in entry.runs.iter().filter(|r| r.chain == chain.chain) {
                    let last = if run.is_open() {
                        head.unwrap()
                    } else {
                        run.last
                    };
                    covered.extend(
                        chain
                            .snaps
                            .iter()
                            .map(|s| s.ord)
                            .filter(|&o| o >= run.first && o <= last),
                    );
                    if run.is_open() {
                        assert_eq!(
                            Some(run.occ),
                            chain
                                .snaps
                                .last()
                                .and_then(|s| s.content.get(&chunk).copied()),
                            "chunk {chunk}: open run occ"
                        );
                    }
                }
                let present: BTreeSet<u32> = refs
                    .iter()
                    .filter(|(c, _)| *c == chain.chain)
                    .map(|&(_, o)| o)
                    .collect();
                assert_eq!(
                    covered,
                    present,
                    "chunk {chunk} in chain {}: runs {:?}; last ops:\n  {}",
                    chain.chain,
                    entry.runs,
                    ctx()
                );
            }
        }

        // The filesystem breakdown.
        let all_snaps: BTreeSet<(u32, u32)> = self
            .chains
            .iter()
            .flat_map(|c| c.snaps.iter().map(|s| (c.chain, s.ord)))
            .collect();
        let total = self.brute_reclaim(&all_snaps);
        let mut with_live = Amount::default();
        for chunk in referrers.keys().filter(|c| self.live.contains(c)) {
            with_live.bytes += size(chunk);
            with_live.chunks += 1;
        }
        let awaiting = Amount {
            bytes: self.tombs.values().map(|t| t.0).sum(),
            chunks: self.tombs.len() as u64,
        };
        let want = FsBreakdown {
            snapshots_total: total,
            unique,
            shared_only: Amount {
                bytes: total.bytes - unique.bytes,
                chunks: total.chunks - unique.chunks,
            },
            shared_with_live: with_live,
            awaiting_gc: awaiting,
        };
        assert_eq!(
            self.ix.fs_breakdown().unwrap(),
            want,
            "last ops:\n  {}",
            ctx()
        );
        assert_eq!(self.ix.awaiting_gc().unwrap(), awaiting);

        // reclaim(D): everything, each chain, a range, random sets.
        let mut sets: Vec<BTreeSet<(u32, u32)>> = vec![all_snaps.clone()];
        let chains: Vec<Vec<(u32, u32)>> = self
            .chains
            .iter()
            .map(|c| c.snaps.iter().map(|s| (c.chain, s.ord)).collect())
            .collect();
        for snaps in chains {
            let n = snaps.len() as u64;
            if n > 0 {
                let a = self.rng.below(n) as usize;
                let b = a + self.rng.below(n - a as u64) as usize;
                sets.push(snaps[a..=b].iter().copied().collect());
            }
            sets.push(snaps.into_iter().collect());
        }
        let pool: Vec<(u32, u32)> = all_snaps.iter().copied().collect();
        for _ in 0..3 {
            let mut d = BTreeSet::new();
            for &s in &pool {
                if self.rng.chance(40) {
                    d.insert(s);
                }
            }
            sets.push(d);
        }
        for d in sets {
            if d.is_empty() {
                continue;
            }
            let list: Vec<(u32, u32)> = d.iter().copied().collect();
            assert_eq!(
                self.ix.reclaim(&list).unwrap(),
                self.brute_reclaim(&d),
                "reclaim({list:?}); last ops:\n  {}",
                ctx()
            );
        }
    }
}

fn run_model(seeds: std::ops::Range<u64>, steps: usize) {
    let mut stats: BTreeMap<&'static str, u64> = BTreeMap::new();
    for seed in seeds {
        let mut world = World::new(seed, 40);
        for _ in 0..steps {
            world.step();
            world.check();
        }
        for (k, v) in world.stats {
            *stats.entry(k).or_default() += v;
        }
    }
    // The histories must have exercised everything they are meant to.
    println!("snapacct model: {stats:?}");
    for kind in [
        "create",
        "create sharing across chains",
        "afterlife re-add",
        "delete head",
        "delete tail",
        "delete middle",
        "delete only",
        "out-of-order insert",
        "duplicate-hash deltas",
        "live toggle",
        "tombstones expired",
    ] {
        assert!(
            stats.get(kind).copied().unwrap_or(0) > 0,
            "no {kind}: {stats:?}"
        );
    }
}

/// Seeded random histories (create with adds/removes/afterlife,
/// cross-chain sharing from a global chunk pool, deletes at the head,
/// tail and middle, live toggles, out-of-order inserts, tombstone
/// expiry); every number checked against brute force after every step.
#[test]
fn model_agrees_with_brute_force() {
    run_model(0..16, 150);
}

#[test]
#[ignore = "heavy: cargo test -p constellation-engine --release -- --ignored snapacct"]
fn model_agrees_with_brute_force_heavy() {
    run_model(1000..1200, 400);
}

fn content(pairs: &[(u32, u64)]) -> Content {
    pairs.iter().copied().collect()
}

/// ZFS's documented behaviour: deleting S moves the chunks S shared with
/// exactly one neighbour into that neighbour's `USED`; S's own unique
/// chunks are freed (awaiting GC).
#[test]
fn deleting_a_snapshot_moves_shared_with_one_neighbour_into_its_used() {
    let (ix, _dir, _now) = open_temp();
    let sizes = vec![100, 20, 3, 4000];
    let chain = ix.register_chain(7).unwrap();
    let (x, y, z, w) = (0u32, 1u32, 2u32, 3u32);
    let a = content(&[(x, 1), (w, 1)]);
    let b = content(&[(x, 1), (y, 2), (z, 1), (w, 1)]);
    let c = content(&[(y, 1), (w, 3)]);
    let mut not_live = |_: &ChunkHash| false;
    let oa = ix
        .apply_first(
            chain,
            "a",
            "ra",
            10,
            deltas(&sizes, &Content::new(), &a),
            &mut not_live,
            None,
        )
        .unwrap();
    let ob = ix
        .apply_created(
            chain,
            "b",
            "rb",
            5,
            deltas(&sizes, &a, &b),
            &mut not_live,
            None,
        )
        .unwrap();
    let oc = ix
        .apply_created(
            chain,
            "c",
            "rc",
            -1,
            deltas(&sizes, &b, &c),
            &mut not_live,
            None,
        )
        .unwrap();
    let used = |ord| ix.snap_numbers(chain, ord).unwrap().unwrap().used;
    assert_eq!((used(oa), used(ob), used(oc)), (0, 3, 0));
    assert_eq!(
        ix.reclaim(&[(chain, ob)]).unwrap(),
        Amount {
            bytes: 3,
            chunks: 1
        }
    );
    assert_eq!(
        ix.reclaim(&[(chain, oa), (chain, ob)]).unwrap(),
        Amount {
            bytes: 103,
            chunks: 2
        }
    );
    ix.apply_deleted(chain, ob, None, None).unwrap();
    assert_eq!(used(oa), 100, "x, shared only with b, is now a's alone");
    assert_eq!(used(oc), 20, "y, shared only with b, is now c's alone");
    assert_eq!(
        ix.awaiting_gc().unwrap(),
        Amount {
            bytes: 3,
            chunks: 1
        }
    );
    let c_numbers = ix.snap_numbers(chain, oc).unwrap().unwrap();
    assert_eq!(
        (c_numbers.written, c_numbers.refer, c_numbers.lsize),
        (20, 4020, 14)
    );
    ix.check_structure().unwrap();

    // Deleting the head needs the step that led to it.
    let err = ix.apply_deleted(chain, oc, None, None).unwrap_err();
    assert!(matches!(err, SnapAcctError::Invalid(_)), "{err}");
    ix.apply_deleted(chain, oc, Some(deltas(&sizes, &a, &c)), None)
        .unwrap();
    assert_eq!(used(oa), 4100, "a is the only snapshot left");
    ix.check_structure().unwrap();
    // A wrong step is refused, and changes nothing.
    let ob = ix
        .apply_created(
            chain,
            "b2",
            "rb",
            0,
            deltas(&sizes, &a, &b),
            &mut not_live,
            None,
        )
        .unwrap();
    let err = ix
        .apply_deleted(chain, ob, Some(deltas(&sizes, &c, &b)), None)
        .unwrap_err();
    assert!(matches!(err, SnapAcctError::Invalid(_)), "{err}");
    ix.check_structure().unwrap();
    assert_eq!(ix.locate("b2").unwrap(), Some((chain, ob)));
}

#[test]
fn an_afterlife_is_a_second_run_and_written_again() {
    let (ix, _dir, _now) = open_temp();
    let sizes = vec![7, 9];
    let chain = ix.register_chain(1).unwrap();
    let s0 = content(&[(0, 1), (1, 1)]);
    let s1 = content(&[(1, 1)]);
    let mut live = |_: &ChunkHash| false;
    ix.apply_first(
        chain,
        "0",
        "",
        0,
        deltas(&sizes, &Content::new(), &s0),
        &mut live,
        None,
    )
    .unwrap();
    ix.apply_created(chain, "1", "", 0, deltas(&sizes, &s0, &s1), &mut live, None)
        .unwrap();
    let o2 = ix
        .apply_created(chain, "2", "", 0, deltas(&sizes, &s1, &s0), &mut live, None)
        .unwrap();
    let entry = ix.chunk_entry(&hash(0)).unwrap().unwrap();
    assert_eq!(entry.runs.len(), 2, "{:?}", entry.runs);
    assert_eq!(ix.snap_numbers(chain, o2).unwrap().unwrap().written, 7);
    // Deleting the gap merges the runs back into one.
    ix.apply_deleted(chain, 1, None, None).unwrap();
    assert_eq!(ix.chunk_entry(&hash(0)).unwrap().unwrap().runs.len(), 1);
    assert_eq!(ix.snap_numbers(chain, o2).unwrap().unwrap().written, 0);
    ix.check_structure().unwrap();
}

#[test]
fn a_foreign_or_outdated_index_is_wiped() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ix");
    {
        let (ix, opened) = SnapAcct::open(&path, "fs-a", PARAMS).unwrap();
        assert_eq!(opened, Opened::Fresh);
        ix.register_chain(5).unwrap();
        ix.set_accounted_seq(42).unwrap();
        assert!(path.join(MARKER).exists());
    }
    {
        let (ix, opened) = SnapAcct::open(&path, "fs-a", PARAMS).unwrap();
        assert_eq!(opened, Opened::Reused);
        assert_eq!(ix.accounted_seq().unwrap(), 42);
        assert_eq!(ix.chain_of(5).unwrap(), Some(0));
    }
    let (ix, opened) = SnapAcct::open(&path, "fs-b", PARAMS).unwrap();
    assert_eq!(opened, Opened::Rebuilt);
    assert_eq!(ix.accounted_seq().unwrap(), 0);
    assert_eq!(ix.chain_of(5).unwrap(), None);
}

/// Several entries for one hash in a delta list are summed before they
/// are applied: the index built from noisy lists matches the one built
/// from netted lists (the model's deltas) entry for entry.
#[test]
fn duplicate_hashes_in_one_delta_list_are_summed() {
    let sizes = vec![10, 200, 3000];
    let dup = |pairs: &[(u32, i64)]| -> Vec<Delta> {
        pairs
            .iter()
            .map(|&(c, d)| Delta {
                hash: hash(c),
                delta: d,
                size_bytes: sizes[c as usize],
            })
            .collect()
    };
    let s0 = content(&[(0, 1), (1, 2)]);
    let s1 = s0.clone();
    let s2 = content(&[(0, 1), (1, 1)]);
    let mut not_live = |_: &ChunkHash| false;
    let (noisy_ix, _d1, _n1) = open_temp();
    let (model_ix, _d2, _n2) = open_temp();
    for ix in [&noisy_ix, &model_ix] {
        let chain = ix.register_chain(9).unwrap();
        ix.apply_first(
            chain,
            "s0",
            "",
            0,
            deltas(&sizes, &Content::new(), &s0),
            &mut not_live,
            None,
        )
        .unwrap();
    }
    // A chunk the chain never held, added and removed in one list.
    noisy_ix
        .apply_created(0, "s1", "", 0, dup(&[(2, 1), (2, -1)]), &mut not_live, None)
        .unwrap();
    model_ix
        .apply_created(
            0,
            "s1",
            "",
            0,
            deltas(&sizes, &s0, &s1),
            &mut not_live,
            None,
        )
        .unwrap();
    // An open run with occ 2 taken to 1 as −2 then +1.
    noisy_ix
        .apply_created(0, "s2", "", 0, dup(&[(1, -2), (1, 1)]), &mut not_live, None)
        .unwrap();
    model_ix
        .apply_created(
            0,
            "s2",
            "",
            0,
            deltas(&sizes, &s1, &s2),
            &mut not_live,
            None,
        )
        .unwrap();
    noisy_ix.check_structure().unwrap();
    assert_eq!(noisy_ix.chunk_entry(&hash(2)).unwrap(), None);
    let one = noisy_ix.chunk_entry(&hash(1)).unwrap().unwrap();
    assert_eq!(
        one.runs.as_slice(),
        &[Run {
            chain: 0,
            first: 0,
            last: OPEN,
            occ: 1
        }]
    );
    assert_eq!(
        noisy_ix.chain_snapshots(0).unwrap(),
        model_ix.chain_snapshots(0).unwrap()
    );
    for c in 0..3 {
        assert_eq!(
            noisy_ix.chunk_entry(&hash(c)).unwrap(),
            model_ix.chunk_entry(&hash(c)).unwrap()
        );
    }
    assert_eq!(
        noisy_ix.fs_breakdown().unwrap(),
        model_ix.fs_breakdown().unwrap()
    );
    let written: Vec<u64> = noisy_ix
        .chain_snapshots(0)
        .unwrap()
        .iter()
        .map(|s| s.written)
        .collect();
    assert_eq!(written, [210, 0, 0]);
}

/// `accounted_seq` commits with the change it accounts for: a refused
/// change leaves it where it was, and it never moves backwards.
#[test]
fn accounted_seq_commits_with_the_change() {
    let (ix, _dir, _now) = open_temp();
    let sizes = vec![5, 6];
    let chain = ix.register_chain(3).unwrap();
    let s0 = content(&[(0, 1)]);
    let s1 = content(&[(0, 1), (1, 1)]);
    let mut not_live = |_: &ChunkHash| false;
    let o0 = ix
        .apply_first(
            chain,
            "s0",
            "",
            0,
            deltas(&sizes, &Content::new(), &s0),
            &mut not_live,
            Some(10),
        )
        .unwrap();
    assert_eq!(ix.accounted_seq().unwrap(), 10);
    // None leaves it alone.
    ix.set_live(hash(0), true, None).unwrap();
    assert_eq!(ix.accounted_seq().unwrap(), 10);
    // A refused change (deltas against the wrong base) commits neither.
    let err = ix
        .apply_created(
            chain,
            "bad",
            "",
            0,
            deltas(&sizes, &s1, &s0),
            &mut not_live,
            Some(11),
        )
        .unwrap_err();
    assert!(matches!(err, SnapAcctError::Invalid(_)), "{err}");
    assert_eq!(ix.accounted_seq().unwrap(), 10);
    assert_eq!(ix.locate("bad").unwrap(), None);
    // Backwards is refused, and the change with it.
    let err = ix
        .apply_created(
            chain,
            "s1",
            "",
            0,
            deltas(&sizes, &s0, &s1),
            &mut not_live,
            Some(9),
        )
        .unwrap_err();
    assert!(matches!(err, SnapAcctError::Invalid(_)), "{err}");
    assert_eq!(ix.locate("s1").unwrap(), None);
    let o1 = ix
        .apply_created(
            chain,
            "s1",
            "",
            0,
            deltas(&sizes, &s0, &s1),
            &mut not_live,
            Some(12),
        )
        .unwrap();
    assert_eq!(ix.accounted_seq().unwrap(), 12);
    ix.apply_deleted(chain, o0, None, Some(12)).unwrap();
    ix.set_live_many([(hash(1), true)], Some(13)).unwrap();
    assert_eq!(ix.accounted_seq().unwrap(), 13);
    ix.apply_deleted(chain, o1, None, Some(14)).unwrap();
    ix.set_accounted_seq(20).unwrap();
    assert!(ix.set_accounted_seq(19).is_err());
    assert_eq!(ix.accounted_seq().unwrap(), 20);
    ix.check_structure().unwrap();
}

/// A header that does not decode (damage, or a future format of another
/// shape) is a mismatch: wiped and rebuilt, never a permanent error.
#[test]
fn an_undecodable_header_is_wiped() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ix");
    {
        let (ix, _) = SnapAcct::open(&path, "fs-a", PARAMS).unwrap();
        ix.register_chain(5).unwrap();
        let mut tx = ix.db.write_tx();
        tx.insert(&ix.meta, META_HEADER, [0xff; 3]);
        tx.commit().unwrap();
    }
    let (ix, opened) = SnapAcct::open(&path, "fs-a", PARAMS).unwrap();
    assert_eq!(opened, Opened::Rebuilt);
    assert_eq!(ix.chain_of(5).unwrap(), None);
    assert_eq!(ix.accounted_seq().unwrap(), 0);
}

/// `open` wipes only what it created: a non-empty directory without the
/// marker is refused and left as it was.
#[test]
fn a_foreign_directory_is_refused_not_wiped() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("precious"), b"keep me").unwrap();
    let err = SnapAcct::open(dir.path(), "fs-a", PARAMS).err().unwrap();
    assert!(matches!(err, SnapAcctError::Invalid(_)), "{err}");
    assert_eq!(
        std::fs::read(dir.path().join("precious")).unwrap(),
        b"keep me"
    );
    // An existing but empty directory is fine.
    let empty = dir.path().join("empty");
    std::fs::create_dir(&empty).unwrap();
    let (_, opened) = SnapAcct::open(&empty, "fs-a", PARAMS).unwrap();
    assert_eq!(opened, Opened::Fresh);
    assert!(empty.join(MARKER).exists());
}

/// Footprint per indexed chunk and the cost of applying a 10k-delta
/// snapshot. Report-only numbers (run in release):
/// `cargo test -p constellation-engine --release snapacct::tests::measure -- --ignored --nocapture`
#[test]
#[ignore = "measurement"]
fn measure_footprint_and_apply_cost() {
    let (ix, _dir, _now) = open_temp();
    let chain = ix.register_chain(1).unwrap();
    let n: u32 = 200_000;
    let size = |c: u32| 1 + u64::from(c % (4 << 20));
    let mk = |c: u32, d: i64| Delta {
        hash: hash(c),
        delta: d,
        size_bytes: size(c),
    };
    let mut live = |_: &ChunkHash| true;
    let started = std::time::Instant::now();
    ix.apply_first(
        chain,
        "s0",
        "",
        0,
        (0..n).map(|c| mk(c, 1)),
        &mut live,
        None,
    )
    .unwrap();
    let first = started.elapsed();
    // 20 snapshots, each replacing 5k chunks with 5k new ones: 10k deltas.
    let mut next = n;
    let mut oldest = 0;
    let mut costs = Vec::new();
    for s in 1..=20 {
        let ds: Vec<Delta> = (oldest..oldest + 5000)
            .map(|c| mk(c, -1))
            .chain((next..next + 5000).map(|c| mk(c, 1)))
            .collect();
        oldest += 5000;
        next += 5000;
        let started = std::time::Instant::now();
        ix.apply_created(chain, &format!("s{s}"), "", 0, ds, &mut live, None)
            .unwrap();
        costs.push(started.elapsed());
    }
    let r = ix.db.read_tx();
    let mut logical = 0u64;
    let mut chunks = 0u64;
    for ks in [
        &ix.chunk, &ix.birth, &ix.death, &ix.snap, &ix.meta, &ix.tomb,
    ] {
        for guard in r.iter(ks) {
            let (k, v) = guard.into_inner().unwrap();
            logical += (k.len() + v.len()) as u64;
        }
    }
    for _ in r.iter(&ix.chunk) {
        chunks += 1;
    }
    drop(r);
    // Flushed and compacted tables (lz4 blocks, prefix-truncated keys);
    // what `footprint_bytes` reports once everything is flushed.
    let mut tables = 0u64;
    for ks in [
        &ix.chunk, &ix.birth, &ix.death, &ix.snap, &ix.meta, &ix.tomb,
    ] {
        ks.inner().rotate_memtable_and_wait().unwrap();
        ks.inner().major_compact().unwrap();
        tables += ks.inner().disk_space();
    }
    let disk = ix.footprint_bytes().unwrap();
    assert_eq!(disk, tables);
    costs.sort();
    println!(
        "snapacct: {chunks} indexed chunks; logical {logical} B ({:.1} B/chunk); \
         compacted tables {tables} B ({:.1} B/chunk); footprint_bytes {disk} B; \
         first snapshot ({n} chunks) {first:?}; 10k-delta apply median {:?}, max {:?}",
        logical as f64 / chunks as f64,
        tables as f64 / chunks as f64,
        costs[costs.len() / 2],
        costs[costs.len() - 1],
    );
    ix.check_structure().unwrap();
}
