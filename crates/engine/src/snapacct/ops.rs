//! The index's mutations, in one write transaction each.
//!
//! An operation loads the chunk entries, snapshot records and chain rows
//! it touches into a [`Ctx`], edits them in memory, and [`Ctx::flush`]
//! derives everything else from the before/after pair of each: the
//! birth/death keys a run edit implies, the owner transition (`USED`
//! and the filesystem buckets), tombstones, and the chain sums. That
//! keeps the run surgery in each operation free of bookkeeping, and makes
//! the bookkeeping one piece of code the model test exercises from every
//! operation. A multi-step operation (rewind, then re-append) flushes
//! between steps inside the same transaction: reads see the
//! transaction's own writes.

use super::encoding::*;
use super::{Result, Run, SnapAcct, SnapAcctError, OPEN};
use crate::snapwalk::Delta;
use constellation_fs_core::ChunkHash;
use fjall::{Readable, SingleWriterTxKeyspace, SingleWriterWriteTx};
use smallvec::SmallVec;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// One snapshot to append, for [`SnapAcct::rederive_suffix`].
#[derive(Clone, Debug)]
pub struct NewSnapshot {
    pub id: String,
    pub root: String,
    /// `LSIZE` minus its predecessor's (its own `LSIZE` for a chain's
    /// first snapshot).
    pub lsize_delta: i64,
    /// `ChainWalk::step(predecessor, this)`, or the full occurrences
    /// ([`super::first_deltas`]) for a chain's first snapshot.
    pub deltas: Vec<Delta>,
}

/// Where a chunk's size is counted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Class {
    /// Not indexed (no runs).
    Absent,
    /// The sole owner's `USED`, and the `unique` bucket.
    Unique(u32, u32),
    /// Not live, in ≥2 snapshots.
    Shared,
    /// In a snapshot and in the live tree.
    Live,
}

/// The class of an entry, given the chain heads (an open run born at its
/// chain's head covers exactly one snapshot).
pub(super) fn class_of(
    entry: Option<&ChunkEntry>,
    head: &mut dyn FnMut(u32) -> Result<Option<u32>>,
) -> Result<Class> {
    let Some(entry) = entry.filter(|e| !e.runs.is_empty()) else {
        return Ok(Class::Absent);
    };
    if entry.live {
        return Ok(Class::Live);
    }
    // Unique when every run (one per size) covers the same one snapshot.
    let mut only = None;
    for run in &entry.runs {
        let last = if run.is_open() {
            head(run.chain)?
        } else {
            Some(run.last)
        };
        if last != Some(run.first) || only.is_some_and(|at| at != (run.chain, run.first)) {
            return Ok(Class::Shared);
        }
        only = Some((run.chain, run.first));
    }
    let (chain, ord) = only.expect("runs are not empty");
    Ok(Class::Unique(chain, ord))
}

struct Slot {
    orig: Option<ChunkEntry>,
    entry: Option<ChunkEntry>,
    before: Class,
}

struct SnapSlot {
    orig: Option<SnapRec>,
    now: Option<SnapRec>,
}

fn add(a: (u64, u64), size: u64, sign: i64, what: &str) -> Result<(u64, u64)> {
    let moved = if sign > 0 {
        a.0.checked_add(size).zip(a.1.checked_add(1))
    } else {
        a.0.checked_sub(size).zip(a.1.checked_sub(1))
    };
    moved.ok_or_else(|| SnapAcctError::Corrupt(format!("{what} counter out of range")))
}

fn shift(value: u64, delta: i64, what: &str) -> Result<u64> {
    value
        .checked_add_signed(delta)
        .ok_or_else(|| SnapAcctError::Corrupt(format!("{what} out of range")))
}

/// A delta list summed per hash and size, net-zero entries dropped. The
/// lists are per-(chunk, size) changes between two trees (snapwalk's are
/// already one per pair), but nothing in the public signatures promises
/// that, and applying `[(h, +1), (h, −1)]` one by one would open a run and
/// close it before it began.
type NetDeltas = HashMap<ChunkHash, BTreeMap<u64, i128>>;

fn net_deltas(deltas: impl IntoIterator<Item = Delta>) -> NetDeltas {
    let mut net: NetDeltas = HashMap::new();
    for delta in deltas {
        *net.entry(delta.hash)
            .or_default()
            .entry(delta.size_bytes)
            .or_default() += i128::from(delta.delta);
    }
    for sizes in net.values_mut() {
        sizes.retain(|_, d| *d != 0);
    }
    net.retain(|_, sizes| !sizes.is_empty());
    net
}

/// Whether `entry` is in snapshot `ord` of `chain`: some run of any size
/// covers it (an open run reaches the head).
fn present(entry: &ChunkEntry, chain: u32, ord: u32) -> bool {
    entry
        .runs
        .iter()
        .any(|r| r.chain == chain && r.first <= ord && (r.is_open() || r.last >= ord))
}

/// The size an entry counts at: its runs' largest (plan 32 §6.1).
fn canonical_size(entry: &ChunkEntry) -> u64 {
    entry
        .runs
        .iter()
        .map(|r| r.size)
        .max()
        .unwrap_or(entry.size)
}

pub(super) struct Ctx<'a> {
    ix: &'a SnapAcct,
    tx: SingleWriterWriteTx<'a>,
    chunks: HashMap<ChunkHash, Slot>,
    snaps: HashMap<(u32, u32), SnapSlot>,
    chains: HashMap<u32, (ChainRec, ChainRec)>,
    /// Chain heads as the transaction stands at the start of this step.
    heads: HashMap<u32, Option<u32>>,
    /// Heads this step moves.
    head_after: HashMap<u32, Option<u32>>,
    fs: FsCounters,
    fs_orig: FsCounters,
    now_ms: u64,
    /// The commit sequence this change brings the index to, written into
    /// the header by [`Ctx::commit`] in the same transaction.
    seq: Option<u64>,
}

impl<'a> Ctx<'a> {
    pub(super) fn new(ix: &'a SnapAcct, seq: Option<u64>) -> Result<Ctx<'a>> {
        let tx = ix.db.write_tx();
        let fs = super::fs_counters(&tx, &ix.meta)?;
        Ok(Ctx {
            ix,
            tx,
            chunks: HashMap::new(),
            snaps: HashMap::new(),
            chains: HashMap::new(),
            heads: HashMap::new(),
            head_after: HashMap::new(),
            fs,
            fs_orig: fs,
            now_ms: (ix.clock)(),
            seq,
        })
    }

    pub(super) fn commit(mut self) -> Result<()> {
        self.flush()?;
        if let Some(seq) = self.seq {
            self.ix.advance_seq(&mut self.tx, seq)?;
        }
        self.tx.commit()?;
        Ok(())
    }

    // ---------------------------------------------------------- reading

    pub(super) fn head(&mut self, chain: u32) -> Result<Option<u32>> {
        if let Some(&head) = self.heads.get(&chain) {
            return Ok(head);
        }
        let head = match self
            .tx
            .prefix(&self.ix.snap, chain.to_be_bytes())
            .next_back()
        {
            Some(g) => Some(split_chain_ord(&g.key()?)?.1),
            None => None,
        };
        self.heads.insert(chain, head);
        Ok(head)
    }

    fn head_now(&mut self, chain: u32) -> Result<Option<u32>> {
        match self.head_after.get(&chain) {
            Some(&head) => Ok(head),
            None => self.head(chain),
        }
    }

    /// Existing ordinals of `chain` in `[from, to]`, as of this step's
    /// start.
    fn snap_ords(&self, chain: u32, from: u32, to: u32) -> Result<Vec<u32>> {
        let mut out = Vec::new();
        for guard in self
            .tx
            .range(&self.ix.snap, snap_key(chain, from)..=snap_key(chain, to))
        {
            out.push(split_chain_ord(&guard.key()?)?.1);
        }
        Ok(out)
    }

    fn prev_ord(&self, chain: u32, ord: u32) -> Result<Option<u32>> {
        match self
            .tx
            .range(&self.ix.snap, snap_key(chain, 0)..snap_key(chain, ord))
            .next_back()
        {
            Some(g) => Ok(Some(split_chain_ord(&g.key()?)?.1)),
            None => Ok(None),
        }
    }

    fn next_ord(&self, chain: u32, ord: u32) -> Result<Option<u32>> {
        if ord == u32::MAX {
            return Ok(None);
        }
        match self
            .tx
            .range(
                &self.ix.snap,
                snap_key(chain, ord + 1)..=snap_key(chain, u32::MAX),
            )
            .next()
        {
            Some(g) => Ok(Some(split_chain_ord(&g.key()?)?.1)),
            None => Ok(None),
        }
    }

    /// The hashes of `ks` (birth or death) keyed in `chain|[from, to]`.
    fn scan(
        &self,
        ks: &SingleWriterTxKeyspace,
        chain: u32,
        from: u32,
        to: u32,
    ) -> Result<Vec<ChunkHash>> {
        let mut out = Vec::new();
        for guard in self.tx.range(
            ks,
            ord_key(chain, from, &ChunkHash([0; 32]))..=ord_key(chain, to, &ChunkHash([0xff; 32])),
        ) {
            out.push(ord_key_hash(&guard.key()?)?);
        }
        Ok(out)
    }

    fn load(&mut self, hash: ChunkHash) -> Result<&mut Slot> {
        if !self.chunks.contains_key(&hash) {
            let orig = self
                .tx
                .get(&self.ix.chunk, hash.0)?
                .map(|v| decode_chunk(&v))
                .transpose()?;
            let before = class_of(orig.as_ref(), &mut |chain| self.head(chain))?;
            self.chunks.insert(
                hash,
                Slot {
                    entry: orig.clone(),
                    orig,
                    before,
                },
            );
        }
        Ok(self.chunks.get_mut(&hash).expect("just loaded"))
    }

    /// An indexed chunk's entry; its absence is corruption.
    fn entry(&mut self, hash: ChunkHash, why: &str) -> Result<&mut ChunkEntry> {
        self.load(hash)?
            .entry
            .as_mut()
            .ok_or_else(|| SnapAcctError::Corrupt(format!("{why}: no entry for {}", hash.to_hex())))
    }

    fn snap_slot(&mut self, chain: u32, ord: u32) -> Result<&mut SnapSlot> {
        if !self.snaps.contains_key(&(chain, ord)) {
            let orig: Option<SnapRec> = self
                .tx
                .get(&self.ix.snap, snap_key(chain, ord))?
                .map(|v| from_postcard(&v, "snapshot record"))
                .transpose()?;
            self.snaps.insert(
                (chain, ord),
                SnapSlot {
                    now: orig.clone(),
                    orig,
                },
            );
        }
        Ok(self.snaps.get_mut(&(chain, ord)).expect("just loaded"))
    }

    fn chain_rec(&mut self, chain: u32) -> Result<&mut ChainRec> {
        if !self.chains.contains_key(&chain) {
            let rec: ChainRec = match self.tx.get(&self.ix.meta, chain_key(chain))? {
                Some(v) => from_postcard(&v, "chain record")?,
                None => {
                    return Err(SnapAcctError::Invalid(format!(
                        "chain {chain} is not registered"
                    )));
                }
            };
            self.chains.insert(chain, (rec.clone(), rec));
        }
        Ok(&mut self.chains.get_mut(&chain).expect("just loaded").1)
    }

    // -------------------------------------------------------- mutations

    /// Append a snapshot after the chain's head (or as its first).
    pub(super) fn append(
        &mut self,
        chain: u32,
        id: &str,
        root: &str,
        lsize_delta: i64,
        deltas: impl IntoIterator<Item = Delta>,
        live: &mut dyn FnMut(&ChunkHash) -> bool,
    ) -> Result<u32> {
        if self.tx.contains_key(&self.ix.meta, id_key(id))? {
            return Err(SnapAcctError::Invalid(format!(
                "snapshot {id} is already indexed"
            )));
        }
        let head = self.head(chain)?;
        let rec = self.chain_rec(chain)?;
        let new = rec.next_ord;
        if new == OPEN {
            return Err(SnapAcctError::Invalid(format!(
                "chain {chain} ran out of ordinals"
            )));
        }
        rec.next_ord += 1;
        debug_assert!(head.is_none_or(|h| h < new));
        self.head_after.insert(chain, Some(new));
        let (prev_refer, prev_lsize) = match head {
            Some(h) => {
                let rec = self.snap_slot(chain, h)?.now.as_ref().ok_or_else(|| {
                    SnapAcctError::Corrupt(format!("chain {chain} head {h} has no record"))
                })?;
                (rec.refer, rec.lsize)
            }
            None => (0, 0),
        };
        let (mut opened, mut closed) = (0u64, 0u64);
        for (hash, sizes) in net_deltas(deltas) {
            let slot = self.load(hash)?;
            // A new entry counts at the largest size it enters with; the
            // flush moves an existing one's size to its runs' largest.
            let entry = slot.entry.get_or_insert_with(|| ChunkEntry {
                size: sizes
                    .iter()
                    .filter(|(_, d)| **d > 0)
                    .map(|(size, _)| *size)
                    .max()
                    .unwrap_or(0),
                live: live(&hash),
                runs: SmallVec::new(),
            });
            let was_present = entry.runs.iter().any(|r| r.chain == chain && r.is_open());
            for (size, delta) in sizes {
                let open = entry
                    .runs
                    .iter()
                    .position(|r| r.chain == chain && r.is_open() && r.size == size);
                let was = open.map_or(0, |i| entry.runs[i].occ);
                let now = (was as i128) + delta;
                if now < 0 || now > i128::from(u64::MAX) {
                    return Err(SnapAcctError::Invalid(format!(
                        "chunk {} would occur {now} times at {size} bytes in chain {chain} \
                         (delta {delta} against the head)",
                        hash.to_hex(),
                    )));
                }
                let now = now as u64;
                match (open, now) {
                    (None, _) => entry.runs.push(Run {
                        chain,
                        first: new,
                        last: OPEN,
                        occ: now,
                        size,
                    }),
                    (Some(i), 0) => {
                        let h = head.ok_or_else(|| {
                            SnapAcctError::Corrupt(format!("open run in empty chain {chain}"))
                        })?;
                        entry.runs[i].last = h;
                        entry.runs[i].occ = 0;
                    }
                    (Some(i), _) => entry.runs[i].occ = now,
                }
            }
            // `REFER`/`WRITTEN` count chunks, whatever their sizes: only
            // entering or leaving the chain's head counts.
            let is_present = entry.runs.iter().any(|r| r.chain == chain && r.is_open());
            match (was_present, is_present) {
                (false, true) => opened += entry.size,
                (true, false) => closed += entry.size,
                _ => {}
            }
        }
        // A run open and born at the old head covered one snapshot and now
        // covers two: re-examine its owner.
        if let Some(h) = head {
            for hash in self.scan(&self.ix.birth, chain, h, h)? {
                self.load(hash)?;
            }
        }
        let refer = (prev_refer + opened)
            .checked_sub(closed)
            .ok_or_else(|| SnapAcctError::Corrupt(format!("REFER of chain {chain} below zero")))?;
        let lsize = prev_lsize
            .checked_add_signed(lsize_delta)
            .ok_or_else(|| SnapAcctError::Invalid(format!("LSIZE of {id} out of range")))?;
        self.snap_slot(chain, new)?.now = Some(SnapRec {
            id: id.to_string(),
            root: root.to_string(),
            used: 0,
            written: opened,
            refer,
            lsize,
        });
        Ok(new)
    }

    /// Delete snapshot `k` of `chain`.
    pub(super) fn delete(
        &mut self,
        chain: u32,
        k: u32,
        head_step: Option<Vec<Delta>>,
    ) -> Result<()> {
        if self.snap_slot(chain, k)?.now.is_none() {
            return Err(SnapAcctError::Invalid(format!(
                "no snapshot {k} in chain {chain}"
            )));
        }
        let prev = self.prev_ord(chain, k)?;
        let Some(next) = self.next_ord(chain, k)? else {
            return match prev {
                None => self.truncate(chain, None, Vec::new()),
                Some(p) => {
                    let step = head_step.ok_or_else(|| {
                        SnapAcctError::Invalid(format!(
                            "deleting head {k} of chain {chain} needs step({p}, {k})"
                        ))
                    })?;
                    self.truncate(chain, Some(p), step)
                }
            };
        };
        // Every chunk whose runs begin or end at k, or begin at next(k):
        // the only ones whose runs or `WRITTEN[next]` share change.
        let mut candidates = self.scan(&self.ix.birth, chain, k, k)?;
        candidates.extend(self.scan(&self.ix.death, chain, k, k)?);
        candidates.extend(self.scan(&self.ix.birth, chain, next, next)?);
        let mut seen = HashSet::new();
        let mut written_next: i64 = 0;
        for hash in candidates {
            if !seen.insert(hash) {
                continue;
            }
            let entry = self.entry(hash, "birth or death at a deleted snapshot")?;
            // next(k) wrote it if k did not have it; from now on, if
            // prev(k) does not.
            let in_prev = prev.is_some_and(|p| present(entry, chain, p));
            let (in_k, in_next) = (present(entry, chain, k), present(entry, chain, next));
            let mut runs = SmallVec::<[Run; 1]>::new();
            for mut run in entry.runs.drain(..) {
                if run.chain == chain && run.first == k {
                    // [k,k] goes; [k,j] starts at next(k) instead.
                    if !run.is_open() && run.last == k {
                        continue;
                    }
                    run.first = next;
                } else if run.chain == chain && !run.is_open() && run.last == k {
                    // Runs ending at k (and starting before it) end at
                    // prev(k).
                    run.last = prev.ok_or_else(|| {
                        SnapAcctError::Corrupt(format!(
                            "run ending at the tail {k} began before it"
                        ))
                    })?;
                }
                runs.push(run);
            }
            // A size absent only from k now has adjacent runs [i, prev]
            // and [next, j]: merge them.
            if let Some(p) = prev {
                let adjacent = |runs: &[Run]| {
                    runs.iter().enumerate().find_map(|(i, r)| {
                        if r.chain != chain || r.is_open() || r.last != p {
                            return None;
                        }
                        let j = runs.iter().position(|n| {
                            n.chain == chain && n.size == r.size && n.first == next
                        })?;
                        Some((i, j))
                    })
                };
                while let Some((i, j)) = adjacent(&runs) {
                    let later = runs.remove(j);
                    let i = if j < i { i - 1 } else { i };
                    runs[i].last = later.last;
                    runs[i].occ = later.occ;
                }
            }
            entry.runs = runs;
            let wrote = |by_prev: bool| i64::from(in_next && !by_prev);
            written_next += entry.size as i64 * (wrote(in_prev) - wrote(in_k));
        }
        self.snap_slot(chain, k)?.now = None;
        if written_next != 0 {
            let rec = self.snap_slot(chain, next)?.now.as_mut().ok_or_else(|| {
                SnapAcctError::Corrupt(format!("next snapshot {next} has no record"))
            })?;
            rec.written = shift(rec.written, written_next, "WRITTEN")?;
        }
        Ok(())
    }

    /// Make `after` the chain's head (or empty the chain), dropping every
    /// later snapshot. `rewind` is `step(after, head)`: the only way to
    /// learn `after`'s occurrence counts, which become the open runs'.
    pub(super) fn truncate(
        &mut self,
        chain: u32,
        after: Option<u32>,
        rewind: impl IntoIterator<Item = Delta>,
    ) -> Result<()> {
        let head = self
            .head(chain)?
            .ok_or_else(|| SnapAcctError::Invalid(format!("chain {chain} is empty")))?;
        let Some(p) = after else {
            for hash in self.scan(&self.ix.birth, chain, 0, u32::MAX)? {
                self.entry(hash, "birth in a cleared chain")?
                    .runs
                    .retain(|r| r.chain != chain);
            }
            for ord in self.snap_ords(chain, 0, u32::MAX)? {
                self.snap_slot(chain, ord)?.now = None;
            }
            self.head_after.insert(chain, None);
            return Ok(());
        };
        if p >= head || self.snap_slot(chain, p)?.now.is_none() {
            return Err(SnapAcctError::Invalid(format!(
                "cannot rewind chain {chain} (head {head}) to {p}"
            )));
        }
        // A closed run never ends at the head (it would be open). A run
        // that ends there would "re-open" when the head goes; that cannot
        // be represented, so it cannot have been built.
        let dead_head = self.scan(&self.ix.death, chain, head, head)?;
        if !dead_head.is_empty() {
            debug_assert!(false, "chain {chain}: closed run ending at head {head}");
            return Err(SnapAcctError::Corrupt(format!(
                "chain {chain}: {} closed run(s) end at head {head}",
                dead_head.len()
            )));
        }
        // `rewind` is head − p per chunk; a chunk it does not name has the
        // same count at both (possibly in two runs: [.., p] and [q, OPEN]).
        let rewind_by = net_deltas(rewind);
        let mut candidates: Vec<ChunkHash> = self.scan(&self.ix.birth, chain, p, head)?;
        candidates.extend(self.scan(&self.ix.death, chain, p, head)?);
        candidates.extend(rewind_by.keys().copied());
        let mut seen = HashSet::new();
        let none = BTreeMap::new();
        for hash in candidates {
            if !seen.insert(hash) {
                continue;
            }
            let by_size = rewind_by.get(&hash).unwrap_or(&none);
            let Some(entry) = self.load(hash)?.entry.as_mut() else {
                return Err(SnapAcctError::Invalid(format!(
                    "rewind names {}, which no snapshot holds",
                    hash.to_hex()
                )));
            };
            // Per size: occurrences at p = at the head − (head − p).
            let mut sizes: BTreeSet<u64> = by_size.keys().copied().collect();
            sizes.extend(
                entry
                    .runs
                    .iter()
                    .filter(|r| r.chain == chain)
                    .map(|r| r.size),
            );
            let mut at_p = Vec::with_capacity(sizes.len());
            for size in sizes {
                let delta = by_size.get(&size).copied().unwrap_or(0);
                let at_head = entry
                    .runs
                    .iter()
                    .find(|r| r.chain == chain && r.is_open() && r.size == size)
                    .map_or(0, |r| r.occ);
                let n = i128::from(at_head) - delta;
                if n < 0 || n > i128::from(u64::MAX) {
                    return Err(SnapAcctError::Invalid(format!(
                        "rewind: chunk {} would occur {n} times at {size} bytes at {p}",
                        hash.to_hex()
                    )));
                }
                at_p.push((size, n as u64));
            }
            entry.runs.retain(|r| !(r.chain == chain && r.first > p));
            for (size, n) in at_p {
                let covering = entry.runs.iter_mut().find(|r| {
                    r.chain == chain
                        && r.size == size
                        && r.first <= p
                        && (r.is_open() || r.last >= p)
                });
                match (covering, n) {
                    (Some(run), n) if n > 0 => {
                        run.last = OPEN;
                        run.occ = n;
                    }
                    (None, 0) => {}
                    (covering, n) => {
                        return Err(SnapAcctError::Invalid(format!(
                            "rewind of chain {chain} to {p} disagrees with the runs of {} \
                             at {size} bytes: run covering {p}: {covering:?}, occurrences \
                             there per rewind: {n}",
                            hash.to_hex()
                        )));
                    }
                }
            }
        }
        for ord in self.snap_ords(chain, p + 1, head)? {
            self.snap_slot(chain, ord)?.now = None;
        }
        self.head_after.insert(chain, Some(p));
        Ok(())
    }

    pub(super) fn set_live(&mut self, hash: ChunkHash, live: bool) -> Result<()> {
        match self.load(hash)?.entry.as_mut() {
            Some(entry) => entry.live = live,
            None if live => self.revive(&hash)?,
            None => {}
        }
        Ok(())
    }

    pub(super) fn expire_tombstones(&mut self) -> Result<u64> {
        let p = self.ix.params;
        let cutoff = self
            .now_ms
            .saturating_sub(p.gc_horizon_ms.saturating_add(p.gc_interval_ms));
        let mut expired = Vec::new();
        for guard in self.tx.range(
            &self.ix.tomb,
            tomb_time_key(0, &ChunkHash([0; 32]))..=tomb_time_key(cutoff, &ChunkHash([0xff; 32])),
        ) {
            let key = guard.key()?;
            let hash: [u8; 32] = key
                .get(9..41)
                .and_then(|b| b.try_into().ok())
                .ok_or_else(|| SnapAcctError::Corrupt("short tombstone key".into()))?;
            expired.push(ChunkHash(hash));
        }
        for hash in &expired {
            self.revive(hash)?;
        }
        Ok(expired.len() as u64)
    }

    fn tombstone(&mut self, hash: &ChunkHash, size: u64) -> Result<()> {
        if self.tx.contains_key(&self.ix.tomb, tomb_hash_key(hash))? {
            return Ok(());
        }
        self.tx.insert(
            &self.ix.tomb,
            tomb_hash_key(hash),
            encode_tomb(size, self.now_ms),
        );
        self.tx
            .insert(&self.ix.tomb, tomb_time_key(self.now_ms, hash), []);
        self.fs.tomb = add(self.fs.tomb, size, 1, "tombstone")?;
        Ok(())
    }

    /// Drop `hash`'s tombstone, if any.
    fn revive(&mut self, hash: &ChunkHash) -> Result<()> {
        let Some(v) = self.tx.get(&self.ix.tomb, tomb_hash_key(hash))? else {
            return Ok(());
        };
        let (size, since) = decode_tomb(&v)?;
        self.tx.remove(&self.ix.tomb, tomb_hash_key(hash));
        self.tx.remove(&self.ix.tomb, tomb_time_key(since, hash));
        self.fs.tomb = add(self.fs.tomb, size, -1, "tombstone")?;
        Ok(())
    }

    // ------------------------------------------------------------ flush

    /// `chain`'s ordinals as this step leaves them.
    fn ords_now(&self, chain: u32) -> Result<Vec<u32>> {
        let mut ords: BTreeSet<u32> = self.snap_ords(chain, 0, u32::MAX)?.into_iter().collect();
        for (&(c, ord), slot) in &self.snaps {
            if c == chain {
                if slot.now.is_some() {
                    ords.insert(ord);
                } else {
                    ords.remove(&ord);
                }
            }
        }
        Ok(ords.into_iter().collect())
    }

    /// `entry`'s size changes by `delta`: `REFER` of every snapshot
    /// holding it, and `WRITTEN` of each one holding it whose
    /// predecessor does not. (The owner's `USED` and the buckets move
    /// with the class.) Cost: the chains' ordinals from the chunk's first
    /// run on; a size changes only for a chunk met at a new largest size,
    /// or losing the run that had it.
    fn resize(
        &mut self,
        entry: &ChunkEntry,
        delta: i64,
        ords: &mut HashMap<u32, Vec<u32>>,
    ) -> Result<()> {
        let chains: BTreeSet<u32> = entry.runs.iter().map(|r| r.chain).collect();
        for chain in chains {
            let chain_ords = match ords.entry(chain) {
                std::collections::hash_map::Entry::Occupied(o) => o.into_mut(),
                std::collections::hash_map::Entry::Vacant(v) => v.insert(self.ords_now(chain)?),
            };
            let from = entry
                .runs
                .iter()
                .filter(|r| r.chain == chain)
                .map(|r| r.first)
                .min()
                .unwrap_or(0);
            let mut held_before = false;
            for &ord in chain_ords.iter().filter(|&&o| o >= from) {
                let held = present(entry, chain, ord);
                if held {
                    let rec = self.snap_slot(chain, ord)?.now.as_mut().ok_or_else(|| {
                        SnapAcctError::Corrupt(format!("snapshot {chain}/{ord} has no record"))
                    })?;
                    rec.refer = shift(rec.refer, delta, "REFER")?;
                    if !held_before {
                        rec.written = shift(rec.written, delta, "WRITTEN")?;
                    }
                }
                held_before = held;
            }
        }
        Ok(())
    }

    fn move_class(&mut self, class: Class, size: u64, sign: i64) -> Result<()> {
        match class {
            Class::Absent => {}
            Class::Live => self.fs.live = add(self.fs.live, size, sign, "live")?,
            Class::Shared => self.fs.shared = add(self.fs.shared, size, sign, "shared")?,
            Class::Unique(chain, ord) => {
                self.fs.unique = add(self.fs.unique, size, sign, "unique")?;
                // An owner deleted in this step takes its USED with it.
                if let Some(rec) = self.snap_slot(chain, ord)?.now.as_mut() {
                    rec.used = shift(rec.used, size as i64 * sign, "USED")?;
                }
            }
        }
        Ok(())
    }

    /// Write this step's changes into the transaction and derive the
    /// bookkeeping (see the module docs). Later steps read what it wrote.
    pub(super) fn flush(&mut self) -> Result<()> {
        let chunks = std::mem::take(&mut self.chunks);
        let mut ords: HashMap<u32, Vec<u32>> = HashMap::new();
        for (hash, slot) in chunks {
            let Slot {
                orig,
                mut entry,
                before,
            } = slot;
            let mut dropped_live = None;
            if entry.as_ref().is_some_and(|e| e.runs.is_empty()) {
                dropped_live = entry.take().map(|e| e.live);
            }
            // The step counted the chunk at its size so far; if its runs'
            // largest moved, every snapshot holding it moves with it.
            if let Some(e) = entry.as_mut() {
                let canonical = canonical_size(e);
                if canonical != e.size {
                    self.resize(e, canonical as i64 - e.size as i64, &mut ords)?;
                    e.size = canonical;
                }
            }
            let after = class_of(entry.as_ref(), &mut |chain| self.head_now(chain))?;
            let size = orig.as_ref().map_or(0, |e| e.size);
            let size_after = entry.as_ref().map_or(0, |e| e.size);
            if before != after || size != size_after {
                self.move_class(before, size, -1)?;
                self.move_class(after, size_after, 1)?;
            }
            let runs = |e: &Option<ChunkEntry>| -> SmallVec<[Run; 2]> {
                e.as_ref()
                    .map(|e| e.runs.iter().copied().collect())
                    .unwrap_or_default()
            };
            let (old, new) = (runs(&orig), runs(&entry));
            let birth = |r: &Run| (r.chain, r.first);
            let death = |r: &Run| (!r.is_open()).then_some((r.chain, r.last));
            for r in &old {
                if !new.iter().any(|n| birth(n) == birth(r)) {
                    self.tx
                        .remove(&self.ix.birth, ord_key(r.chain, r.first, &hash));
                }
                if let Some((c, l)) = death(r) {
                    if !new.iter().any(|n| death(n) == Some((c, l))) {
                        self.tx.remove(&self.ix.death, ord_key(c, l, &hash));
                    }
                }
            }
            for r in &new {
                if !old.iter().any(|o| birth(o) == birth(r)) {
                    self.tx
                        .insert(&self.ix.birth, ord_key(r.chain, r.first, &hash), []);
                }
                if let Some((c, l)) = death(r) {
                    if !old.iter().any(|o| death(o) == Some((c, l))) {
                        self.tx.insert(&self.ix.death, ord_key(c, l, &hash), []);
                    }
                }
            }
            match (&orig, &entry) {
                (_, Some(e)) if orig.as_ref() != Some(e) => {
                    self.tx.insert(&self.ix.chunk, hash.0, encode_chunk(e));
                }
                (Some(_), None) => self.tx.remove(&self.ix.chunk, hash.0),
                _ => {}
            }
            match (&orig, &entry) {
                (Some(_), None) if dropped_live == Some(false) => self.tombstone(&hash, size)?,
                (None, Some(_)) => self.revive(&hash)?,
                _ => {}
            }
        }

        let snaps = std::mem::take(&mut self.snaps);
        for ((chain, ord), SnapSlot { orig, now }) in snaps {
            if orig == now {
                continue;
            }
            let key = snap_key(chain, ord);
            match (&orig, &now) {
                (None, Some(n)) => {
                    let rec = self.chain_rec(chain)?;
                    rec.snapshots += 1;
                    rec.used = shift(rec.used, n.used as i64, "chain USED")?;
                    rec.written = shift(rec.written, n.written as i64, "chain WRITTEN")?;
                    self.tx
                        .insert(&self.ix.meta, id_key(&n.id), to_postcard(&(chain, ord)));
                }
                (Some(o), None) => {
                    let rec = self.chain_rec(chain)?;
                    rec.snapshots = rec.snapshots.checked_sub(1).ok_or_else(|| {
                        SnapAcctError::Corrupt(format!("chain {chain} snapshot count"))
                    })?;
                    rec.used = shift(rec.used, -(o.used as i64), "chain USED")?;
                    rec.written = shift(rec.written, -(o.written as i64), "chain WRITTEN")?;
                    self.tx.remove(&self.ix.meta, id_key(&o.id));
                }
                (Some(o), Some(n)) => {
                    let rec = self.chain_rec(chain)?;
                    rec.used = shift(rec.used, n.used as i64 - o.used as i64, "chain USED")?;
                    rec.written = shift(
                        rec.written,
                        n.written as i64 - o.written as i64,
                        "chain WRITTEN",
                    )?;
                }
                (None, None) => {}
            }
            match now {
                Some(n) => self.tx.insert(&self.ix.snap, key, to_postcard(&n)),
                None => self.tx.remove(&self.ix.snap, key),
            }
        }

        for (chain, (orig, now)) in std::mem::take(&mut self.chains) {
            if orig != now {
                self.tx
                    .insert(&self.ix.meta, chain_key(chain), to_postcard(&now));
            }
        }
        if self.fs != self.fs_orig {
            self.tx
                .insert(&self.ix.meta, META_FS, to_postcard(&self.fs));
            self.fs_orig = self.fs;
        }
        self.heads.clear();
        self.head_after.clear();
        Ok(())
    }
}

/// [`SnapAcct::check_structure`]: recompute everything from the chunk
/// entries and compare.
pub(super) fn check_structure(ix: &SnapAcct) -> Result<()> {
    let bad = |s: String| Err(SnapAcctError::Corrupt(s));
    let r = ix.db.read_tx();
    let mut snaps: BTreeMap<(u32, u32), SnapRec> = BTreeMap::new();
    for guard in r.iter(&ix.snap) {
        let (k, v) = guard.into_inner()?;
        snaps.insert(split_chain_ord(&k)?, from_postcard(&v, "snapshot record")?);
    }
    let mut ords: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    for &(chain, ord) in snaps.keys() {
        ords.entry(chain).or_default().push(ord);
    }
    let exists = |chain: u32, ord: u32| snaps.contains_key(&(chain, ord));
    let mut used: HashMap<(u32, u32), u64> = HashMap::new();
    let mut written: HashMap<(u32, u32), u64> = HashMap::new();
    let mut refer: HashMap<(u32, u32), u64> = HashMap::new();
    let mut fs = FsCounters::default();
    let (mut births, mut deaths) = (0u64, 0u64);
    for guard in r.iter(&ix.chunk) {
        let (k, v) = guard.into_inner()?;
        let hash = ChunkHash(
            k.as_ref()
                .try_into()
                .map_err(|_| SnapAcctError::Corrupt("chunk key length".into()))?,
        );
        let hex = hash.to_hex();
        let entry = decode_chunk(&v)?;
        if entry.runs.is_empty() {
            return bad(format!("{hex}: entry without runs"));
        }
        if r.contains_key(&ix.tomb, tomb_hash_key(&hash))? {
            return bad(format!("{hex}: indexed and tombstoned"));
        }
        if entry.size != canonical_size(&entry) {
            return bad(format!(
                "{hex}: size {} but its runs' largest is {}",
                entry.size,
                canonical_size(&entry)
            ));
        }
        // Per chain and size: maximal runs, keyed.
        let mut by_size: BTreeMap<(u32, u64), Vec<Run>> = BTreeMap::new();
        for run in &entry.runs {
            by_size.entry((run.chain, run.size)).or_default().push(*run);
        }
        let mut keys_birth = BTreeSet::new();
        let mut keys_death = BTreeSet::new();
        for ((chain, _), mut runs) in by_size {
            runs.sort_by_key(|r| r.first);
            let chain_ords = ords.get(&chain).map(Vec::as_slice).unwrap_or_default();
            let head = chain_ords.last().copied();
            for (i, run) in runs.iter().enumerate() {
                if !exists(chain, run.first) {
                    return bad(format!("{hex}: run {run:?} starts at no snapshot"));
                }
                if !r.contains_key(&ix.birth, ord_key(chain, run.first, &hash))? {
                    return bad(format!("{hex}: run {run:?} has no birth key"));
                }
                keys_birth.insert((chain, run.first));
                let last = if run.is_open() {
                    if i + 1 != runs.len() || run.occ == 0 {
                        return bad(format!("{hex}: open run {run:?} not last or occ 0"));
                    }
                    head.expect("a run starts at a snapshot")
                } else {
                    if !exists(chain, run.last) || run.last < run.first || run.occ != 0 {
                        return bad(format!("{hex}: closed run {run:?} malformed"));
                    }
                    if Some(run.last) == head {
                        return bad(format!("{hex}: closed run {run:?} ends at the head"));
                    }
                    if !r.contains_key(&ix.death, ord_key(chain, run.last, &hash))? {
                        return bad(format!("{hex}: run {run:?} has no death key"));
                    }
                    keys_death.insert((chain, run.last));
                    run.last
                };
                if let Some(next) = runs.get(i + 1) {
                    let gap = chain_ords.iter().any(|&o| o > last && o < next.first);
                    if run.is_open() || next.first <= last || !gap {
                        return bad(format!("{hex}: runs {run:?} and {next:?} not maximal"));
                    }
                }
            }
        }
        births += keys_birth.len() as u64;
        deaths += keys_death.len() as u64;
        // The chunk is in a snapshot when any run covers it.
        let chains: BTreeSet<u32> = entry.runs.iter().map(|r| r.chain).collect();
        for chain in chains {
            let chain_ords = ords.get(&chain).map(Vec::as_slice).unwrap_or_default();
            let mut held_before = false;
            for &o in chain_ords {
                let held = present(&entry, chain, o);
                if held {
                    *refer.entry((chain, o)).or_default() += entry.size;
                    if !held_before {
                        *written.entry((chain, o)).or_default() += entry.size;
                    }
                }
                held_before = held;
            }
        }
        let class = class_of(Some(&entry), &mut |chain| {
            Ok(ords.get(&chain).and_then(|o| o.last().copied()))
        })?;
        match class {
            Class::Absent => unreachable!("runs are not empty"),
            Class::Live => fs.live = add(fs.live, entry.size, 1, "live")?,
            Class::Shared => fs.shared = add(fs.shared, entry.size, 1, "shared")?,
            Class::Unique(chain, ord) => {
                fs.unique = add(fs.unique, entry.size, 1, "unique")?;
                *used.entry((chain, ord)).or_default() += entry.size;
            }
        }
    }
    if r.iter(&ix.birth).count() as u64 != births {
        return bad(format!(
            "{births} runs but a different number of birth keys"
        ));
    }
    if r.iter(&ix.death).count() as u64 != deaths {
        return bad(format!(
            "{deaths} closed runs but a different number of death keys"
        ));
    }
    let mut chains: BTreeMap<u32, (u32, u64, u64)> = BTreeMap::new();
    for (&(chain, ord), rec) in &snaps {
        let key = (chain, ord);
        let want = (
            used.get(&key).copied().unwrap_or(0),
            written.get(&key).copied().unwrap_or(0),
            refer.get(&key).copied().unwrap_or(0),
        );
        if (rec.used, rec.written, rec.refer) != want {
            return bad(format!(
                "snapshot {chain}/{ord}: (used, written, refer) = {:?}, recomputed {want:?}",
                (rec.used, rec.written, rec.refer)
            ));
        }
        let at: Option<(u32, u32)> = r
            .get(&ix.meta, id_key(&rec.id))?
            .map(|v| from_postcard(&v, "snapshot location"))
            .transpose()?;
        if at != Some(key) {
            return bad(format!(
                "snapshot {chain}/{ord}: id {} maps to {at:?}",
                rec.id
            ));
        }
        let c = chains.entry(chain).or_default();
        c.0 += 1;
        c.1 += rec.used;
        c.2 += rec.written;
    }
    for guard in r.prefix(&ix.meta, b"chain/") {
        let (k, v) = guard.into_inner()?;
        let chain = u32::from_be_bytes(
            k[6..]
                .try_into()
                .map_err(|_| SnapAcctError::Corrupt("chain key".into()))?,
        );
        let rec: ChainRec = from_postcard(&v, "chain record")?;
        let want = chains.remove(&chain).unwrap_or_default();
        if (rec.snapshots, rec.used, rec.written) != want {
            return bad(format!(
                "chain {chain}: (snapshots, used, written) = {:?}, recomputed {want:?}",
                (rec.snapshots, rec.used, rec.written)
            ));
        }
        if ords
            .get(&chain)
            .and_then(|o| o.last())
            .is_some_and(|&h| h >= rec.next_ord)
        {
            return bad(format!("chain {chain}: ordinal counter behind its head"));
        }
    }
    if let Some(chain) = chains.keys().next() {
        return bad(format!("snapshots of unregistered chain {chain}"));
    }
    let (mut tomb_keys, mut time_keys) = (0u64, 0u64);
    for guard in r.iter(&ix.tomb) {
        let (k, v) = guard.into_inner()?;
        match k.first() {
            Some(b'h') => {
                let (size, since) = decode_tomb(&v)?;
                let hash = ChunkHash(
                    k[1..]
                        .try_into()
                        .map_err(|_| SnapAcctError::Corrupt("tombstone key".into()))?,
                );
                if !r.contains_key(&ix.tomb, tomb_time_key(since, &hash))? {
                    return bad(format!("tombstone {} has no time key", hash.to_hex()));
                }
                fs.tomb = add(fs.tomb, size, 1, "tombstone")?;
                tomb_keys += 1;
            }
            Some(b't') => time_keys += 1,
            _ => return bad("stray tombstone key".into()),
        }
    }
    if tomb_keys != time_keys {
        return bad("tombstone keys out of step".into());
    }
    let stored = super::fs_counters(&r, &ix.meta)?;
    if stored != fs {
        return bad(format!("fs counters {stored:?}, recomputed {fs:?}"));
    }
    Ok(())
}
