//! Bounded-memory write staging (plan 07, DESIGN.md §7/§9).
//!
//! One file per open, dirty inode at
//! `<state_dir>/staging/<ino>.<gen>`. `<gen>` is a per-mount monotonic
//! counter so a stale file from a previous life is never confused with
//! a live one — mount-time GC (`gc`) simply deletes everything under
//! the staging root before the FUSE loop starts, no locking required.
//!
//! Per-inode RAM is `file_len`, the optional base manifest, the slots
//! of the admitted chunks (bounded by the staging budget), and a
//! dirty chunk-index set stored as merged runs (`DirtyRuns`): ~8 bytes
//! per *run*, not per chunk. Sequential append — the shape `rsync`/`cp`
//! actually produce — collapses to a handful of runs; a genuinely
//! fragmenting random-write workload pays one run per disjoint region,
//! which is still bounded in practice and never O(chunks).
//!
//! **mmap is rejected, deliberately.** A `SIGBUS` from ENOSPC or from
//! truncation of a mapped region kills the daemon with no errno path,
//! and there is no way to apply backpressure to a writer that is just
//! touching memory. `pwrite`/`pread` return errors as values, which the
//! FUSE boundary can map through `errno()`. Keep it that way.
//!
//! Staging bytes are reserve-before-accept, mirroring
//! `DiskCache::insert`'s discipline: the budget tracks each admitted
//! chunk's *logical* length (the same "reserve the uncompressed size"
//! choice the chunk cache makes), not the bytes actually written into
//! its slot, plus the released slots a session keeps for reuse (blocks
//! the file still holds; at most two per session, punched to make room
//! when a reservation would not fit beside them) — conservative, but
//! simple, deterministic, and exact ENOSPC-with-no-partial-state. A
//! reservation failure never touches the file or the in-memory length.
//! A slot's space is released only when it is punched or the file
//! deleted, so the budget bounds the staging files' disk use.

use constellation_fs_core::Ino;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

/// `pwrite`, portable: unix's `write_at` directly; Windows has the same
/// contract under a different name (`seek_write`, `std::os::windows::fs`).
fn file_write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::write_at(file, buf, offset)
    }
    #[cfg(windows)]
    {
        std::os::windows::fs::FileExt::seek_write(file, buf, offset)
    }
}

/// `pread` into a full buffer, portable (unix `read_at`, Windows
/// `seek_read`), retrying `Interrupted`; what lies past the file's end
/// reads as zeros (the file covers its slots, so only a read of a
/// chunk past them would).
fn file_read_at_or_zero(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        #[cfg(unix)]
        let read = std::os::unix::fs::FileExt::read_at(file, buf, offset);
        #[cfg(windows)]
        let read = std::os::windows::fs::FileExt::seek_read(file, buf, offset);
        match read {
            Ok(0) => {
                buf.fill(0);
                break;
            }
            Ok(n) => {
                buf = &mut buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum StagingError {
    #[error("staging budget exhausted: need {needed} bytes, {available} available")]
    Full { needed: u64, available: u64 },
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Shared budget across every open dirty inode's staging file on this
/// mount. `CONSTELLATION_STAGING_BUDGET` (bytes) overrides the default
/// of a quarter of `--cache-size` — decoupled from the chunk cache
/// because staging must hold a whole in-flight write until flush (07
/// adds no streaming; that is 08), while the chunk cache only holds
/// each sealed chunk briefly before eager upload demotes it.
///
/// `used` counts everything the staging files may hold on disk,
/// including the released slots sessions keep for their next chunks
/// (`kept`, [`Staging`]): the bound on disk use. Those are reclaimable —
/// a reservation that would not fit punches them first — so they are
/// left out of [`Self::active`], which pressure and throttling look at:
/// to every writer they are free space.
pub struct StagingBudget {
    budget: u64,
    used: Mutex<Use>,
    /// A view's own count ([`StagingBudget::child`]): every reservation
    /// is also the node's, which alone refuses (`Full`).
    parent: Option<Arc<StagingBudget>>,
    /// Signalled on every release of a child, for a view's staging
    /// admission ([`StagingBudget::wait_below`]). `None` on the node's
    /// budget: its releases stay a lock and a subtraction.
    released: Option<Condvar>,
    /// The node's budget: every session's kept slots, for reclaiming.
    keepers: Mutex<Vec<std::sync::Weak<Kept>>>,
    /// The node's budget: stamps kept slots in the order they were kept,
    /// so reclaiming takes the oldest first.
    kept_seq: AtomicU64,
}

#[derive(Default)]
struct Use {
    used: u64,
    /// Of `used`, what sessions' kept slots hold.
    kept: u64,
}

impl StagingBudget {
    pub fn new(budget: u64) -> Arc<Self> {
        Arc::new(Self {
            budget,
            used: Mutex::new(Use::default()),
            parent: None,
            released: None,
            keepers: Mutex::new(Vec::new()),
            kept_seq: AtomicU64::new(0),
        })
    }

    /// One view's share of `parent` (plan 31 §9.10 `ViewQos::
    /// max_staging_bytes`): it counts what the view's write sessions
    /// stage, reserving each byte against `parent` too, but never refuses
    /// on its own — `limit` is enforced before a write starts, as a wait
    /// ([`Self::wait_below`]), not as `ENOSPC` in the middle of one.
    pub fn child(parent: &Arc<StagingBudget>, limit: u64) -> Arc<Self> {
        Arc::new(Self {
            budget: limit,
            used: Mutex::new(Use::default()),
            parent: Some(parent.clone()),
            released: Some(Condvar::new()),
            keepers: Mutex::new(Vec::new()),
            kept_seq: AtomicU64::new(0),
        })
    }

    pub fn reserve(&self, bytes: u64) -> Result<(), StagingError> {
        if bytes == 0 {
            return Ok(());
        }
        if let Some(parent) = &self.parent {
            parent.reserve(bytes)?;
            self.used.lock().unwrap().used += bytes;
            return Ok(());
        }
        loop {
            let mut used = self.used.lock().unwrap();
            if used.used + bytes <= self.budget {
                used.used += bytes;
                return Ok(());
            }
            // Kept slots are free space to every writer: give back the
            // oldest one's blocks, then look again — no more of them than
            // it takes (this runs in a write, under its inode's locks).
            let refused = StagingError::Full {
                needed: bytes,
                available: self.budget.saturating_sub(used.used),
            };
            if used.kept == 0 {
                return Err(refused);
            }
            drop(used);
            if !self.reclaim_oldest() {
                return Err(refused);
            }
        }
    }

    pub fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut used = self.used.lock().unwrap();
        used.used = used.used.saturating_sub(bytes);
        debug_assert!(used.kept <= used.used, "kept slots beyond what is used");
        drop(used);
        if let Some(parent) = &self.parent {
            parent.release(bytes);
        }
        if let Some(released) = &self.released {
            released.notify_all();
        }
    }

    /// `bytes` of what is reserved now sit in a kept slot (or, `false`,
    /// no longer do).
    fn keep(&self, bytes: u64, kept: bool) {
        if bytes == 0 {
            return;
        }
        let mut used = self.used.lock().unwrap();
        if kept {
            used.kept += bytes;
        } else {
            used.kept = used.kept.saturating_sub(bytes);
        }
        debug_assert!(used.kept <= used.used, "kept slots beyond what is used");
        drop(used);
        if let Some(parent) = &self.parent {
            parent.keep(bytes, kept);
        }
        if let (Some(released), true) = (&self.released, kept) {
            released.notify_all();
        }
    }

    /// The node's budget, where kept slots register.
    fn root(&self) -> &StagingBudget {
        match &self.parent {
            Some(parent) => parent.root(),
            None => self,
        }
    }

    fn register(&self, kept: &Arc<Kept>) {
        let mut keepers = self.root().keepers.lock().unwrap();
        keepers.retain(|k| k.strong_count() > 0);
        keepers.push(Arc::downgrade(kept));
    }

    /// The next stamp for a slot being kept ([`Self::reclaim_oldest`]).
    fn next_kept_seq(&self) -> u64 {
        self.root().kept_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Punch the oldest kept slot of any session and release its
    /// reservation; `false` if no session keeps one. (Kept the longest,
    /// its pages are the likeliest to be written back, its punch the
    /// cheapest.)
    fn reclaim_oldest(&self) -> bool {
        let keepers: Vec<Arc<Kept>> = {
            let mut keepers = self.keepers.lock().unwrap();
            keepers.retain(|k| k.strong_count() > 0);
            keepers
                .iter()
                .filter_map(std::sync::Weak::upgrade)
                .collect()
        };
        loop {
            let oldest = keepers
                .iter()
                .filter_map(|kept| kept.oldest().map(|seq| (seq, kept)))
                .min_by_key(|(seq, _)| *seq);
            let Some((seq, kept)) = oldest else {
                return false;
            };
            // Taken for a write since, or reclaimed by another: look again.
            if kept.reclaim(seq) {
                return true;
            }
        }
    }

    /// Of `used`, what sessions' kept slots hold.
    #[cfg(test)]
    pub(crate) fn kept(&self) -> u64 {
        self.used.lock().unwrap().kept
    }

    /// Everything the staging files may hold on disk (the bound
    /// `reserve` enforces), kept slots included.
    pub fn used(&self) -> u64 {
        self.used.lock().unwrap().used
    }

    /// What write sessions hold, leaving out the slots they keep for
    /// reuse (reclaimable on demand): what staging pressure and
    /// throttling look at.
    pub fn active(&self) -> u64 {
        let used = self.used.lock().unwrap();
        used.used.saturating_sub(used.kept)
    }

    pub fn budget(&self) -> u64 {
        self.budget
    }

    /// Wait until `bytes` more fit under this budget, or nothing is
    /// staged at all (a single write larger than the whole limit must
    /// still be able to proceed alone), or `deadline` passes (`false`).
    /// Only a [`Self::child`] is waited on; the node's budget answers at
    /// once. Kept slots do not count ([`Self::active`]).
    pub fn wait_below(&self, bytes: u64, deadline: std::time::Instant) -> bool {
        let Some(released) = &self.released else {
            return true;
        };
        let mut used = self.used.lock().unwrap();
        loop {
            let active = used.used.saturating_sub(used.kept);
            if active == 0 || active.saturating_add(bytes) <= self.budget {
                return true;
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return false;
            }
            used = released.wait_timeout(used, deadline - now).unwrap().0;
        }
    }
}

/// A dirty chunk-index set stored as merged, sorted, half-open runs
/// (`[start, end)`), so a sequential write of N chunks costs a few dozen
/// bytes rather than N * 8.
#[derive(Debug, Default, Clone)]
pub struct DirtyRuns {
    runs: Vec<(u64, u64)>,
}

impl DirtyRuns {
    pub fn mark(&mut self, idx: u64) {
        self.mark_range(idx, idx + 1);
    }

    /// Mark `[start, end)` dirty, merging with any overlapping or
    /// adjacent existing run.
    pub fn mark_range(&mut self, start: u64, end: u64) {
        if start >= end {
            return;
        }
        let mut new_start = start;
        let mut new_end = end;
        let mut merged = Vec::with_capacity(self.runs.len() + 1);
        for &(s, e) in &self.runs {
            if e < new_start || s > new_end {
                merged.push((s, e));
            } else {
                new_start = new_start.min(s);
                new_end = new_end.max(e);
            }
        }
        let pos = merged.partition_point(|&(s, _)| s < new_start);
        merged.insert(pos, (new_start, new_end));
        self.runs = merged;
    }

    pub fn contains(&self, idx: u64) -> bool {
        let pos = self.runs.partition_point(|&(s, _)| s <= idx);
        pos > 0 && self.runs[pos - 1].1 > idx
    }

    pub fn clear(&mut self, idx: u64) {
        let mut next = Vec::with_capacity(self.runs.len() + 1);
        for &(start, end) in &self.runs {
            if idx < start || idx >= end {
                next.push((start, end));
                continue;
            }
            if start < idx {
                next.push((start, idx));
            }
            if idx + 1 < end {
                next.push((idx + 1, end));
            }
        }
        self.runs = next;
    }

    pub fn clear_range(&mut self, start: u64, end: u64) {
        if start >= end {
            return;
        }
        let mut next = Vec::with_capacity(self.runs.len() + 1);
        for &(run_start, run_end) in &self.runs {
            if run_end <= start || run_start >= end {
                next.push((run_start, run_end));
                continue;
            }
            if run_start < start {
                next.push((run_start, start));
            }
            if run_end > end {
                next.push((end, run_end));
            }
        }
        self.runs = next;
    }

    /// Drop every dirty chunk at or after `keep` (truncate-down).
    pub fn retain_below(&mut self, keep: u64) {
        let pos = self.runs.partition_point(|&(s, _)| s < keep);
        self.runs.truncate(pos);
        if let Some(last) = self.runs.last_mut() {
            if last.1 > keep {
                last.1 = keep;
            }
        }
    }

    #[allow(dead_code)] // part of the public shape (plan 07); exercised by tests
    pub fn run_count(&self) -> usize {
        self.runs.len()
    }

    #[allow(dead_code)] // part of the public shape (plan 07); used via Staging::dirty_indices
    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.runs.iter().flat_map(|&(s, e)| s..e)
    }
}

#[cfg(test)]
thread_local! {
    /// Tests: `(skip, fail)` — let `skip` more [`Staging::write_at`]s on
    /// this thread through, then fail the next `fail` with `EIO`, as the
    /// staging file's own write would (not the budget's refusal).
    pub(crate) static FAIL_WRITES: std::cell::Cell<(u32, u32)> =
        const { std::cell::Cell::new((0, 0)) };
}

/// One open, dirty inode's staged bytes. Each chunk the session admits
/// ([`Staging::prepare_chunk`]) gets a chunk-sized *slot* of the file,
/// and its bytes live there at their offset inside the chunk; a released
/// chunk's slot goes to the next chunk admitted. A dirty chunk is read
/// back only where the session put bytes into it since it was given its
/// slot: the ranges its writes delivered (`view::WriteState::written`),
/// and the base a read staged in the gaps between them
/// (`view::WriteState::staged_base`; both go when the chunk's slot
/// does). Its other bytes come from the committed manifest, not from
/// here, so a reused slot's earlier bytes are never read, and nothing
/// has to be punched or zeroed to give a slot to another chunk.
///
/// The file used to mirror the real file 1:1 (byte offsets equal), and
/// every chunk a seal released was punched out (`FALLOC_FL_PUNCH_HOLE`)
/// so a long sequential write did not grow it to the file's size. A
/// punch first writes the range's dirty pages back and waits for them:
/// bytes written moments before, and about to be discarded, went to the
/// disk synchronously on the writer's path — ~170 ms per 4 MiB chunk on
/// btrfs with `compress=zstd` (a 23 MiB/s ceiling on sequential writes),
/// 3–8 ms on uncompressed btrfs and xfs. With slots a sequential writer
/// cycles through two of them and its staged bytes rarely leave the page
/// cache at all; the file is at most as large as the most chunks the
/// session had admitted at once, or two.
pub struct Staging {
    path: PathBuf,
    /// The file, and the released slots kept for reuse: shared with the
    /// node's budget, which reclaims kept slots when it runs out.
    kept: Arc<Kept>,
    budget: Arc<StagingBudget>,
    chunk_size: u64,
    /// The real file's length as this session sees it: an admitted
    /// chunk's reservation is its length below it. Logical only — the
    /// staging file's own length is the slots', whatever this is.
    file_len: u64,
    /// What the admitted chunks' slots hold reserved.
    reserved: u64,
    dirty: DirtyRuns,
    /// Admitted chunks and their slots.
    slots: std::collections::HashMap<u64, Slot>,
    /// Slots the file has had: it is at most `next_slot` chunks long.
    next_slot: u64,
}

/// How many slots a session holds (admitted or kept) before it punches
/// what it releases: a sequential writer needs two (the chunk it writes,
/// and the one it releases while the next is not admitted yet, or as a
/// write straddling the boundary admits it). So what a session keeps
/// beyond its admitted chunks is at most this many chunks, however many
/// it released.
const KEEP_SLOTS: usize = 2;

/// A chunk-sized region of the staging file (`at` × chunk size), and the
/// budget reserved for the bytes it may hold: the admitted chunk's
/// length, or what an earlier tenant wrote there if that was more.
#[derive(Clone, Copy, Debug)]
struct Slot {
    at: u64,
    held: u64,
    /// When it was kept ([`StagingBudget::next_kept_seq`]); meaningless
    /// while a chunk has it.
    kept_seq: u64,
}

/// A staging file and its free slots. Behind the session (its shard's
/// lock) and, for reclaiming kept slots, the node's budget: `free` is a
/// leaf lock — nothing else is locked under it but the budgets'.
struct Kept {
    file: File,
    chunk_size: u64,
    /// The host's hole punching (a slot given up gives its blocks back).
    fs: Arc<dyn constellation_platform::FsPrimitives>,
    /// The session's budget: a kept slot's reservation is released
    /// through it.
    budget: Arc<StagingBudget>,
    free: Mutex<Free>,
}

#[derive(Default)]
struct Free {
    /// Released slots, blocks and reservation kept for the next chunks
    /// admitted.
    kept: Vec<Slot>,
    /// Slot numbers given up (punched, their reservation released),
    /// reused before the file grows a new one.
    empty: Vec<u64>,
}

impl Kept {
    /// Punch `slot` out and release its reservation (`kept`: it was a
    /// kept slot). A host that cannot punch holes still gets the release
    /// (as before slots): the slot's blocks stay, and the next chunk
    /// admitted reuses them, so the file still never outgrows the most
    /// slots the session had at once.
    fn give_up(&self, free: &mut Free, slot: Slot, kept: bool) {
        if slot.held > 0 {
            let _ = self
                .fs
                .punch_hole(&self.file, slot.at * self.chunk_size, self.chunk_size);
            if kept {
                self.budget.keep(slot.held, false);
            }
            self.budget.release(slot.held);
        }
        free.empty.push(slot.at);
    }

    /// Keep `slot` (its reservation counted as kept), the newest.
    fn keep(&self, free: &mut Free, mut slot: Slot) {
        slot.kept_seq = self.budget.next_kept_seq();
        self.budget.keep(slot.held, true);
        free.kept.push(slot);
    }

    /// The stamp of the oldest slot kept here.
    fn oldest(&self) -> Option<u64> {
        self.free
            .lock()
            .unwrap()
            .kept
            .first()
            .map(|slot| slot.kept_seq)
    }

    /// The node's budget ran out: give up the kept slot stamped `seq`
    /// if it is still the oldest kept here.
    fn reclaim(&self, seq: u64) -> bool {
        let mut free = self.free.lock().unwrap();
        if free.kept.first().map(|slot| slot.kept_seq) != Some(seq) {
            return false;
        }
        let slot = free.kept.remove(0);
        self.give_up(&mut free, slot, true);
        true
    }
}

#[cfg(test)]
thread_local! {
    /// Tests: fill each slot with junk when a chunk is given it, so a read
    /// of anything but the bytes written into it since shows.
    static POISON_SLOTS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Tests: make the slots staging hands out on this thread read as junk
/// until written ([`POISON_SLOTS`]).
#[cfg(test)]
pub fn poison_slots(on: bool) {
    POISON_SLOTS.with(|p| p.set(on));
}

impl Staging {
    /// `dir` is the staging root (`<state_dir>/staging`); the file is
    /// created at `dir/<ino>.<gen>`.
    pub fn create(
        dir: &Path,
        ino: Ino,
        gen: u64,
        chunk_size: u32,
        budget: Arc<StagingBudget>,
        fs: Arc<dyn constellation_platform::FsPrimitives>,
    ) -> Result<Self, StagingError> {
        fs::create_dir_all(dir)?;
        let path = dir.join(format!("{ino}.{gen}"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        let chunk_size = u64::from(chunk_size.max(1));
        let kept = Arc::new(Kept {
            file,
            chunk_size,
            fs,
            budget: budget.clone(),
            free: Mutex::new(Free::default()),
        });
        budget.register(&kept);
        Ok(Self {
            path,
            kept,
            budget,
            chunk_size,
            file_len: 0,
            reserved: 0,
            dirty: DirtyRuns::default(),
            slots: std::collections::HashMap::new(),
            next_slot: 0,
        })
    }

    /// Chunk `idx`'s length below `file_len`.
    fn chunk_len(&self, idx: u64) -> u64 {
        self.chunk_size.min(
            self.file_len
                .saturating_sub(idx.saturating_mul(self.chunk_size)),
        )
    }

    /// Admit chunks `[first, end)` — reserve each one's length (an
    /// admitted chunk: its growth since) and give each new one a slot:
    /// a kept one, an empty one, or a new one at the file's end — all or
    /// nothing: a reservation the budget refuses changes nothing.
    fn admit(&mut self, first: u64, end: u64) -> Result<(), StagingError> {
        let new = (first..end)
            .filter(|idx| !self.slots.contains_key(idx))
            .count();
        // Take the slots out first: the reservation below may reclaim
        // kept slots, this session's included.
        let (taken_kept, taken_empty) = {
            let mut free = self.kept.free.lock().unwrap();
            let free = &mut *free;
            let k = new.min(free.kept.len());
            let taken_kept = free.kept.split_off(free.kept.len() - k);
            let e = (new - k).min(free.empty.len());
            let taken_empty = free.empty.split_off(free.empty.len() - e);
            (taken_kept, taken_empty)
        };
        let mut from_kept = taken_kept.iter().rev().copied();
        let mut from_empty = taken_empty.iter().rev().copied();
        let mut fresh = 0u64;
        let mut need = 0u64;
        let mut plan: Vec<(u64, Option<Slot>, u64)> = Vec::new();
        for idx in first..end {
            let len = self.chunk_len(idx);
            match self.slots.get(&idx) {
                Some(slot) => {
                    let growth = len.saturating_sub(slot.held);
                    if growth > 0 {
                        need += growth;
                        plan.push((idx, None, growth));
                    }
                }
                None => {
                    let slot = from_kept
                        .next()
                        .or_else(|| {
                            from_empty.next().map(|at| Slot {
                                at,
                                held: 0,
                                kept_seq: 0,
                            })
                        })
                        .unwrap_or_else(|| {
                            fresh += 1;
                            Slot {
                                at: self.next_slot + fresh - 1,
                                held: 0,
                                kept_seq: 0,
                            }
                        });
                    let growth = len.saturating_sub(slot.held);
                    need += growth;
                    plan.push((idx, Some(slot), growth));
                }
            }
        }
        let give_back = |kept: &Kept, taken_kept: Vec<Slot>, taken_empty: Vec<u64>| {
            let mut free = kept.free.lock().unwrap();
            free.kept.extend(taken_kept);
            free.empty.extend(taken_empty);
        };
        if let Err(error) = self.budget.reserve(need) {
            give_back(&self.kept, taken_kept, taken_empty);
            return Err(error);
        }
        // New slots extend the file over them first, so the writes land
        // inside it: a write past the end can make the file system
        // preallocate beyond it (xfs, speculatively), blocks the budget
        // does not count. A file growing by its writes alone did, ~4 MiB.
        if fresh > 0 {
            let len = (self.next_slot + fresh) * self.chunk_size;
            if let Err(error) = self.kept.file.set_len(len) {
                self.budget.release(need);
                give_back(&self.kept, taken_kept, taken_empty);
                return Err(error.into());
            }
        }
        // The kept slots' reservations are the admitted chunks' now.
        let was_kept: u64 = taken_kept.iter().map(|slot| slot.held).sum();
        self.budget.keep(was_kept, false);
        self.reserved += need + was_kept;
        self.next_slot += fresh;
        for (idx, slot, growth) in plan {
            match slot {
                None => self.slots.get_mut(&idx).expect("admitted").held += growth,
                Some(mut slot) => {
                    slot.held += growth;
                    #[cfg(test)]
                    if POISON_SLOTS.with(|p| p.get()) {
                        let junk = vec![0xa5u8; self.chunk_size as usize];
                        let _ = file_write_at(&self.kept.file, &junk, slot.at * self.chunk_size);
                    }
                    self.slots.insert(idx, slot);
                }
            }
        }
        Ok(())
    }

    /// Give up an admitted chunk's slot: kept for the next admission,
    /// its reservation with it, while the session holds fewer than
    /// [`KEEP_SLOTS`]; else a slot is punched — the oldest kept one
    /// rather than this one if there is one (its pages are the likelier
    /// to have been written back already, which makes the punch cheap).
    fn free_slot(&mut self, slot: Slot) {
        self.reserved -= slot.held.min(self.reserved);
        let mut free = self.kept.free.lock().unwrap();
        if self.slots.len() + free.kept.len() < KEEP_SLOTS {
            self.kept.keep(&mut free, slot);
            return;
        }
        if free.kept.is_empty() {
            self.kept.give_up(&mut free, slot, false);
        } else {
            self.kept.keep(&mut free, slot);
            let oldest = free.kept.remove(0);
            self.kept.give_up(&mut free, oldest, true);
        }
    }

    /// `pwrite` at the real file's `offset`, into the slots of the chunks
    /// it falls in. A chunk not admitted yet is admitted first, and a
    /// write past `file_len` extends it; a budget failure leaves no
    /// partial state. (The write path admits each chunk itself first —
    /// [`Self::prepare_chunk`] — and this is then only the copy.)
    pub fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), StagingError> {
        if buf.is_empty() {
            return Ok(());
        }
        let end = offset + buf.len() as u64;
        let old_len = self.file_len;
        self.file_len = self.file_len.max(end);
        let first = offset / self.chunk_size;
        let last = (end - 1) / self.chunk_size;
        if let Err(error) = self.admit(first, last + 1) {
            self.file_len = old_len;
            return Err(error);
        }
        #[cfg(test)]
        if FAIL_WRITES.with(|f| match f.get() {
            (0, 0) => false,
            (0, fail) => {
                f.set((0, fail - 1));
                true
            }
            (skip, fail) => {
                f.set((skip - 1, fail));
                false
            }
        }) {
            return Err(io::Error::from_raw_os_error(5).into());
        }
        let mut at = offset;
        let mut rest = buf;
        while !rest.is_empty() {
            let idx = at / self.chunk_size;
            let inside = at - idx * self.chunk_size;
            let n = ((self.chunk_size - inside) as usize).min(rest.len());
            let slot = self.slots[&idx];
            let mut piece = &rest[..n];
            let mut to = slot.at * self.chunk_size + inside;
            while !piece.is_empty() {
                let wrote = file_write_at(&self.kept.file, piece, to)?;
                if wrote == 0 {
                    return Err(io::Error::from(io::ErrorKind::WriteZero).into());
                }
                piece = &piece[wrote..];
                to += wrote as u64;
            }
            rest = &rest[n..];
            at += n as u64;
        }
        Ok(())
    }

    /// `pread` at the real file's `offset`, from the slots of the chunks
    /// it falls in. Callers read only what they wrote since the chunk was
    /// admitted (`view::WriteState::written`, `staged_base`): a slot's
    /// other bytes are an earlier tenant's, or zeros. A chunk without a
    /// slot reads as zeros.
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), StagingError> {
        let mut at = offset;
        let mut rest = &mut buf[..];
        while !rest.is_empty() {
            let idx = at / self.chunk_size;
            let inside = at - idx * self.chunk_size;
            let n = ((self.chunk_size - inside) as usize).min(rest.len());
            let (piece, tail) = rest.split_at_mut(n);
            match self.slots.get(&idx) {
                Some(slot) => file_read_at_or_zero(
                    &self.kept.file,
                    piece,
                    slot.at * self.chunk_size + inside,
                )?,
                None => piece.fill(0),
            }
            rest = tail;
            at += n as u64;
        }
        Ok(())
    }

    /// Change the real file's length as this session sees it. Nothing
    /// is charged or released (a chunk is charged when
    /// [`prepare_chunk`](Self::prepare_chunk) admits a write into it, and
    /// released by [`release_chunk`](Self::release_chunk) /
    /// [`release_chunks`](Self::release_chunks)), and the staging file is
    /// not touched: its length is its slots'.
    pub fn set_len_sparse(&mut self, new_len: u64) -> Result<(), StagingError> {
        self.file_len = new_len;
        Ok(())
    }

    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    /// Bytes currently reserved against the shared budget for this
    /// staging file: its admitted chunks' and the slots it keeps for
    /// reuse.
    #[allow(dead_code)] // part of the public shape (plan 07)
    pub fn reserved_bytes(&self) -> u64 {
        let kept: u64 = self
            .kept
            .free
            .lock()
            .unwrap()
            .kept
            .iter()
            .map(|slot| slot.held)
            .sum();
        self.reserved + kept
    }

    pub fn mark_dirty(&mut self, idx: u64) {
        self.dirty.mark(idx);
    }

    /// Admit chunk `idx` before writing into it: reserve its length (or
    /// its growth, admitted already) and give it a slot. A refusal
    /// changes nothing.
    pub fn prepare_chunk(&mut self, idx: u64) -> Result<(), StagingError> {
        self.admit(idx, idx + 1)
    }

    /// A sealed chunk no longer needs staging residency: it is no longer
    /// dirty, and its slot goes to the next chunk admitted (its
    /// reservation with it), or is punched if the session holds enough
    /// slots ([`Self::free_slot`]). The bytes are durable in the chunk
    /// cache before this is called. Releasing a chunk twice is a no-op.
    pub fn release_chunk(&mut self, idx: u64) {
        self.dirty.clear(idx);
        if let Some(slot) = self.slots.remove(&idx) {
            self.free_slot(slot);
        }
    }

    pub fn is_dirty(&self, idx: u64) -> bool {
        self.dirty.contains(idx)
    }

    pub fn retain_dirty_below(&mut self, keep: u64) {
        self.dirty.retain_below(keep);
    }

    /// Chunks `[start, end)` are discarded whole (truncated away,
    /// punched): no longer dirty, and their slots given up — kept for
    /// reuse or punched, as [`Self::release_chunk`].
    pub fn release_chunks(&mut self, start: u64, end: u64) {
        if start >= end {
            return;
        }
        let mut admitted: Vec<u64> = self
            .slots
            .keys()
            .copied()
            .filter(|index| *index >= start && *index < end)
            .collect();
        admitted.sort_unstable();
        for index in admitted {
            self.release_chunk(index);
        }
        self.dirty.clear_range(start, end);
    }

    #[allow(dead_code)] // part of the public shape (plan 07); exercised by tests
    pub fn dirty_indices(&self) -> impl Iterator<Item = u64> + '_ {
        self.dirty.iter()
    }

    #[cfg(test)]
    pub fn dirty_run_count(&self) -> usize {
        self.dirty.run_count()
    }

    /// Release this file's reservation and delete it. Called once the
    /// content has been sealed into the chunk cache (flush) and is no
    /// longer needed here. Unlinking drops its pages unwritten: nothing
    /// staged has to reach the disk. (Dropping a session does the same.)
    pub fn discard(self) {
        drop(self);
    }
}

/// A session dropped without [`Staging::discard`] (a flush that failed
/// composing gives its session up) still gives back its reservation —
/// kept slots too, or `active` would understate what sessions hold for
/// good — and its file.
impl Drop for Staging {
    fn drop(&mut self) {
        let kept = std::mem::take(&mut self.kept.free.lock().unwrap().kept);
        for slot in kept {
            self.budget.keep(slot.held, false);
            self.budget.release(slot.held);
        }
        self.budget.release(self.reserved);
        self.reserved = 0;
        let _ = fs::remove_file(&self.path);
    }
}

/// Mount-time GC (plan 07 step 6): nothing under the staging root can
/// be live at mount start — the generation counter guarantees a fresh
/// mount never reuses a name, and staging holds only bytes POSIX
/// permits losing on a crash. Returns the reclaimed byte count so the
/// caller can log it (a large number after a crash is the operator's
/// signal that a big write was in flight).
pub fn gc(dir: &Path) -> std::io::Result<u64> {
    if !dir.exists() {
        return Ok(0);
    }
    let mut reclaimed = 0u64;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if let Ok(meta) = entry.metadata() {
            reclaimed += meta.len();
        }
        let _ = fs::remove_file(entry.path());
    }
    Ok(reclaimed)
}

/// Per-mount monotonic generation counter for staging file names.
#[derive(Default)]
pub struct GenCounter(AtomicU64);

impl GenCounter {
    pub fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_fs_core::ChunkLayout;
    use tempfile::TempDir;

    fn budget(n: u64) -> Arc<StagingBudget> {
        StagingBudget::new(n)
    }

    fn host_fs() -> Arc<dyn constellation_platform::FsPrimitives> {
        constellation_platform::HostServices::native().fs
    }

    /// A view's child budget counts its own staging and the node's, never
    /// refuses by itself, and wakes a staging admission wait on release.
    #[test]
    fn a_child_budget_counts_into_its_parent_and_wakes_waiters() {
        let node = budget(100);
        let view = StagingBudget::child(&node, 10);
        view.reserve(8).unwrap();
        assert_eq!((view.used(), node.used()), (8, 8));
        // Over the view's limit: counted, not refused (the wait is the
        // admission's); the node's limit still refuses.
        view.reserve(8).unwrap();
        assert_eq!((view.used(), node.used()), (16, 16));
        assert!(matches!(view.reserve(90), Err(StagingError::Full { .. })));
        assert_eq!(view.used(), 16, "a refused reservation is not counted");
        let soon = std::time::Instant::now() + std::time::Duration::from_millis(20);
        assert!(!view.wait_below(1, soon), "over the limit until released");
        let waiter = {
            let view = view.clone();
            std::thread::spawn(move || {
                view.wait_below(
                    4,
                    std::time::Instant::now() + std::time::Duration::from_secs(10),
                )
            })
        };
        std::thread::sleep(std::time::Duration::from_millis(20));
        view.release(12);
        assert!(waiter.join().unwrap(), "woken by the release");
        assert_eq!((view.used(), node.used()), (4, 4));
        // The node's own budget never waits.
        assert!(node.wait_below(1 << 40, std::time::Instant::now()));
    }

    /// The host's file primitives, counting hole punches.
    #[derive(Default)]
    struct CountingFs {
        punches: AtomicU64,
    }

    impl constellation_platform::FsPrimitives for CountingFs {
        fn punch_hole(&self, file: &File, offset: u64, len: u64) -> io::Result<()> {
            self.punches.fetch_add(1, Ordering::Relaxed);
            host_fs().punch_hole(file, offset, len)
        }
        fn preallocate(&self, file: &File, offset: u64, len: u64, keep: bool) -> io::Result<()> {
            host_fs().preallocate(file, offset, len, keep)
        }
        fn full_fsync(&self, file: &File) -> io::Result<()> {
            host_fs().full_fsync(file)
        }
        fn drop_cache(&self, file: &File, offset: u64, len: u64) -> io::Result<()> {
            host_fs().drop_cache(file, offset, len)
        }
    }

    #[test]
    fn round_trip_across_chunk_boundaries() {
        let dir = TempDir::new().unwrap();
        let mut s = Staging::create(dir.path(), 1, 0, 64, budget(1 << 20), host_fs()).unwrap();
        let layout = ChunkLayout::new(64);
        // A write spanning two chunks lands in their two slots; it reads
        // back at the real file's offsets.
        let data: Vec<u8> = (0..100u8).collect();
        s.write_at(30, &data).unwrap();
        for slice in layout.slices(30, data.len() as u64) {
            s.mark_dirty(slice.index);
        }
        let mut back = vec![0u8; 100];
        s.read_at(30, &mut back).unwrap();
        assert_eq!(back, data);

        // Never written: zeros (a fresh slot's hole, a slot's tail past
        // the file's end, a chunk with no slot).
        let mut hole = vec![9u8; 30];
        s.read_at(0, &mut hole).unwrap();
        assert_eq!(hole, vec![0u8; 30]);
        let mut tail = vec![9u8; 40];
        s.read_at(130, &mut tail).unwrap();
        assert_eq!(tail, vec![0u8; 40]);
        let mut unadmitted = vec![9u8; 64];
        s.read_at(640, &mut unadmitted).unwrap();
        assert_eq!(unadmitted, vec![0u8; 64]);
    }

    #[test]
    fn truncate_down_drops_dirty_runs_and_slots() {
        let dir = TempDir::new().unwrap();
        let b = budget(1 << 20);
        let mut s = Staging::create(dir.path(), 1, 0, 16, b.clone(), host_fs()).unwrap();
        let layout = ChunkLayout::new(16);
        s.write_at(0, &[1u8; 40]).unwrap(); // chunks 0,1,2
        for i in 0..layout.chunk_count(40) {
            s.mark_dirty(i);
        }
        assert_eq!(b.used(), 40);
        assert!(s.is_dirty(2));
        // Keep chunks 0,1 (as `View::truncate_locked`).
        let keep = layout.chunk_count(20);
        s.retain_dirty_below(keep);
        s.release_chunks(keep, layout.chunk_count(40));
        s.set_len_sparse(20).unwrap();
        assert!(s.is_dirty(0));
        assert!(s.is_dirty(1));
        assert!(!s.is_dirty(2));
        // Chunk 2's slot is punched (the session holds two more).
        assert_eq!(b.used(), 32);
        let mut back = vec![0u8; 4];
        s.read_at(16, &mut back).unwrap();
        assert_eq!(back, vec![1, 1, 1, 1]);
        // Released with the session holding fewer, a slot is kept,
        // reservation and all, for the next chunk admitted.
        s.release_chunk(1);
        assert_eq!(b.used(), 32);
        s.set_len_sparse(80).unwrap();
        s.prepare_chunk(4).unwrap();
        assert_eq!(b.used(), 32);
    }

    #[test]
    fn extending_the_length_charges_nothing_until_a_chunk_is_written() {
        let dir = TempDir::new().unwrap();
        let b = budget(1 << 20);
        let mut s = Staging::create(dir.path(), 1, 0, 64, b.clone(), host_fs()).unwrap();
        s.write_at(0, &[5u8; 10]).unwrap();
        s.set_len_sparse(100).unwrap();
        assert_eq!(b.used(), 10);
        s.write_at(150, &[6u8; 10]).unwrap(); // chunk 2: 128..160
        assert_eq!(b.used(), 10 + 32);
        let mut mid = vec![9u8; 54];
        s.read_at(10, &mut mid).unwrap();
        assert_eq!(mid, vec![0u8; 54], "never written: zeros");
        let mut back = vec![0u8; 10];
        s.read_at(150, &mut back).unwrap();
        assert_eq!(back, vec![6u8; 10]);
    }

    #[test]
    fn budget_reserve_before_accept_leaves_no_partial_state() {
        let dir = TempDir::new().unwrap();
        let b = budget(50);
        let mut s = Staging::create(dir.path(), 1, 0, 64, b.clone(), host_fs()).unwrap();
        s.write_at(0, &[1u8; 40]).unwrap();
        assert_eq!(b.used(), 40);
        let err = s.write_at(40, &[2u8; 20]).unwrap_err();
        assert!(matches!(err, StagingError::Full { .. }));
        // No partial state: file_len and budget usage unchanged by the
        // failed write.
        assert_eq!(s.file_len(), 40);
        assert_eq!(b.used(), 40);
        let mut back = vec![0u8; 10];
        s.read_at(0, &mut back).unwrap();
        assert_eq!(back, vec![1u8; 10]);
        // A write over several chunks is admitted whole or not at all.
        let err = s.write_at(64, &[3u8; 100]).unwrap_err();
        assert!(matches!(err, StagingError::Full { .. }));
        assert_eq!((s.file_len(), b.used()), (40, 40));
        assert_eq!(s.slots.len(), 1);
    }

    #[test]
    fn sequential_append_dirty_runs_stay_flat() {
        let dir = TempDir::new().unwrap();
        let mut s = Staging::create(dir.path(), 1, 0, 64, budget(1 << 30), host_fs()).unwrap();
        for i in 0..2000u64 {
            s.mark_dirty(i);
        }
        assert!(
            s.dirty_run_count() <= 2,
            "sequential append must collapse to a handful of runs, got {}",
            s.dirty_run_count()
        );
    }

    #[test]
    fn fragmenting_random_writes_bound_run_count() {
        let dir = TempDir::new().unwrap();
        let mut s = Staging::create(dir.path(), 1, 0, 64, budget(1 << 30), host_fs()).unwrap();
        // Every other chunk index: maximally fragmenting.
        let n = 500u64;
        for i in 0..n {
            s.mark_dirty(i * 2);
        }
        // No merging is possible for this pattern; the bound is the
        // (higher) ceiling of "one run per marked chunk", not O(1).
        assert_eq!(s.dirty_run_count() as u64, n);
    }

    #[test]
    fn gc_removes_orphans_and_leaves_a_live_generation_untouched_after_recreate() {
        let dir = TempDir::new().unwrap();
        let staging_dir = dir.path().join("staging");
        fs::create_dir_all(&staging_dir).unwrap();
        fs::write(staging_dir.join("5.0"), vec![0u8; 100]).unwrap();
        fs::write(staging_dir.join("6.3"), vec![0u8; 50]).unwrap();
        let reclaimed = gc(&staging_dir).unwrap();
        assert_eq!(reclaimed, 150);
        assert_eq!(fs::read_dir(&staging_dir).unwrap().count(), 0);
        // GC on an already-empty (or missing) dir is a clean no-op.
        assert_eq!(gc(&staging_dir).unwrap(), 0);
        assert_eq!(gc(&dir.path().join("never-existed")).unwrap(), 0);
    }

    #[test]
    fn discard_releases_budget_and_deletes_file() {
        let dir = TempDir::new().unwrap();
        let b = budget(1000);
        let mut s = Staging::create(dir.path(), 9, 1, 64, b.clone(), host_fs()).unwrap();
        s.write_at(0, &[1u8; 100]).unwrap();
        s.mark_dirty(0);
        assert_eq!(b.used(), 100);
        // The released slot kept for reuse is released with the rest.
        s.release_chunk(0);
        assert_eq!(b.used(), 100);
        let path = dir.path().join("9.1");
        assert!(path.exists());
        s.discard();
        assert_eq!(b.used(), 0);
        assert!(!path.exists());
    }

    /// A released chunk's slot, reservation included, goes to the next
    /// chunk admitted: nothing is punched. Only a slot released while the
    /// session holds `KEEP_SLOTS` is punched, and its reservation
    /// released.
    #[test]
    fn a_released_slot_is_kept_for_the_next_chunk_and_released_once() {
        let dir = TempDir::new().unwrap();
        let b = budget(256);
        let counting = Arc::new(CountingFs::default());
        let mut s = Staging::create(dir.path(), 9, 1, 64, b.clone(), counting.clone()).unwrap();
        s.write_at(0, &[1u8; 64]).unwrap();
        s.mark_dirty(0);
        s.release_chunk(0);
        assert!(!s.is_dirty(0));
        assert_eq!(b.used(), 64, "the kept slot's blocks stay reserved");
        s.release_chunk(0);
        assert_eq!(b.used(), 64, "one boundary seals only once");
        s.prepare_chunk(0).unwrap();
        s.write_at(0, &[2u8; 64]).unwrap();
        s.mark_dirty(0);
        assert_eq!(
            b.used(),
            64,
            "writing a sealed chunk re-admits it into the kept slot"
        );
        // Two chunks admitted, both released: one slot kept, one punched.
        s.write_at(64, &[3u8; 128]).unwrap();
        s.mark_dirty(1);
        s.mark_dirty(2);
        assert_eq!(b.used(), 192);
        assert_eq!(counting.punches.load(Ordering::Relaxed), 0);
        // Three chunks admitted, all released: two slots kept, one
        // punched (the first, released while the session held three).
        s.release_chunk(0);
        s.release_chunk(1);
        s.release_chunk(2);
        assert_eq!(counting.punches.load(Ordering::Relaxed), 1);
        assert_eq!(b.used(), 128);
        // The kept slots and then the punched one are reused before the
        // file grows a fourth.
        s.write_at(192, &[4u8; 192]).unwrap();
        assert_eq!(s.next_slot, 3);
        assert_eq!(b.used(), 192);
        let mut back = vec![0u8; 64];
        s.read_at(320, &mut back).unwrap();
        assert_eq!(back, vec![4u8; 64]);
        s.discard();
        assert_eq!(b.used(), 0);
    }

    /// Kept slots count toward the disk bound (`used`) but are free space
    /// to every writer: not `active` (what pressure, throttling and a
    /// view's admission look at), and a reservation that would not fit
    /// otherwise reclaims them — punched, their reservation released —
    /// whichever session keeps them.
    #[test]
    fn kept_slots_are_reclaimed_for_a_reservation_that_does_not_fit() {
        let dir = TempDir::new().unwrap();
        let node = budget(192);
        let view = StagingBudget::child(&node, 64);
        let counting = Arc::new(CountingFs::default());
        let mut a = Staging::create(dir.path(), 1, 0, 64, view.clone(), counting.clone()).unwrap();
        let mut b = Staging::create(dir.path(), 2, 0, 64, node.clone(), counting.clone()).unwrap();
        a.write_at(0, &[1u8; 128]).unwrap();
        a.mark_dirty(0);
        a.mark_dirty(1);
        a.release_chunks(0, 2);
        assert_eq!(counting.punches.load(Ordering::Relaxed), 0);
        assert_eq!((node.used(), node.active()), (128, 0));
        assert_eq!((view.used(), view.active()), (128, 0));
        // The view's limit is 64: its kept slots do not hold a write back.
        assert!(view.wait_below(64, std::time::Instant::now()));
        // 128 more do not fit beside a's kept slots: as many are
        // reclaimed as it takes — one, the oldest.
        b.write_at(0, &[2u8; 128]).unwrap();
        assert_eq!(counting.punches.load(Ordering::Relaxed), 1);
        assert_eq!((node.used(), node.active(), node.kept()), (192, 128, 64));
        assert_eq!((view.used(), view.active()), (64, 0));
        assert_eq!(a.reserved_bytes(), 64);
        // a's next chunks take its kept slot, then the reclaimed one:
        // the file does not grow.
        a.write_at(128, &[3u8; 64]).unwrap();
        assert_eq!((node.used(), node.kept()), (192, 0));
        assert!(matches!(
            a.write_at(192, &[5u8; 64]),
            Err(StagingError::Full { .. })
        ));
        assert_eq!(a.next_slot, 2);
        let mut back = vec![0u8; 64];
        a.read_at(128, &mut back).unwrap();
        assert_eq!(back, vec![3u8; 64]);
        // Nothing to reclaim: refused.
        assert!(matches!(
            b.write_at(128, &[4u8; 64]),
            Err(StagingError::Full { .. })
        ));
        // A refusal gives the slots it had taken back: the reclaimed slot
        // is still a's to reuse, and the file has not grown.
        assert_eq!(a.kept.free.lock().unwrap().empty, vec![0]);
        assert_eq!((a.next_slot, a.slots.len()), (2, 1));
        b.discard();
        assert_eq!((node.used(), view.used()), (64, 64));
        a.write_at(192, &[5u8; 64]).unwrap();
        assert_eq!(a.next_slot, 2);
        assert_eq!(a.kept.free.lock().unwrap().empty, Vec::<u64>::new());
        a.read_at(192, &mut back).unwrap();
        assert_eq!(back, vec![5u8; 64]);
        a.discard();
        assert_eq!((node.used(), view.used()), (0, 0));
    }

    /// A session dropped without `discard` (a flush failing to compose
    /// drops it) gives back its reservation, kept slots included, and its
    /// file.
    #[test]
    fn dropping_a_session_releases_everything_and_deletes_its_file() {
        let dir = TempDir::new().unwrap();
        let b = budget(1 << 20);
        let mut s = Staging::create(dir.path(), 9, 1, 64, b.clone(), host_fs()).unwrap();
        s.write_at(0, &[1u8; 128]).unwrap();
        s.mark_dirty(0);
        s.release_chunk(0);
        assert_eq!((b.used(), b.kept()), (128, 64));
        let path = dir.path().join("9.1");
        assert!(path.exists());
        drop(s);
        assert_eq!((b.used(), b.kept(), b.active()), (0, 0, 0));
        assert!(!path.exists());
    }

    /// The staging file covers its slots exactly: a new slot extends it
    /// first (no write lands past its end, where a file system may
    /// preallocate blocks the budget does not count), a reused one does
    /// not.
    #[test]
    fn the_staging_file_is_as_long_as_its_slots() {
        let dir = TempDir::new().unwrap();
        let mut s = Staging::create(dir.path(), 9, 1, 64, budget(1 << 20), host_fs()).unwrap();
        let len = || fs::metadata(dir.path().join("9.1")).unwrap().len();
        assert_eq!(len(), 0);
        s.set_len_sparse(1000).unwrap();
        s.prepare_chunk(7).unwrap();
        assert_eq!(len(), 64);
        s.write_at(7 * 64 + 3, &[1u8; 5]).unwrap();
        s.write_at(2 * 64, &[2u8; 128]).unwrap(); // chunks 2, 3
        assert_eq!((s.next_slot, len()), (3, 192));
        s.mark_dirty(7);
        s.release_chunk(7);
        s.prepare_chunk(9).unwrap(); // the slot chunk 7 gave up
        assert_eq!((s.next_slot, len()), (3, 192));
    }

    /// Two sessions on two threads under a budget of a few chunks: each
    /// writes, reads back, and releases chunks at random, so slots are
    /// kept, taken back, reclaimed by the other session's reservations
    /// and refused. Kept never exceeds used, used never the budget, and
    /// every admitted chunk reads back what was written into it.
    #[test]
    fn concurrent_sessions_reclaim_and_reuse_slots_consistently() {
        const CS: u64 = 4096;
        let dir = TempDir::new().unwrap();
        let node = budget(5 * CS);
        let view = StagingBudget::child(&node, 2 * CS);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let watcher = {
            let (node, stop) = (node.clone(), stop.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let (used, kept) = {
                        let u = node.used.lock().unwrap();
                        (u.used, u.kept)
                    };
                    assert!(kept <= used && used <= 5 * CS, "kept {kept} used {used}");
                }
            })
        };
        let workers: Vec<_> = [(1u64, node.clone()), (2, view.clone())]
            .into_iter()
            .map(|(ino, b)| {
                let path = dir.path().to_path_buf();
                std::thread::spawn(move || {
                    let mut s = Staging::create(&path, ino, 0, CS as u32, b, host_fs()).unwrap();
                    s.set_len_sparse(64 * CS).unwrap();
                    let mut rng = ino.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
                    let mut next = || {
                        rng ^= rng << 13;
                        rng ^= rng >> 7;
                        rng ^= rng << 17;
                        rng
                    };
                    let mut model: std::collections::HashMap<u64, Vec<u8>> = Default::default();
                    let mut refused = 0;
                    for step in 0..4000u64 {
                        let idx = next() % 64;
                        match next() % 3 {
                            0 | 1 => {
                                let fill = vec![(step % 251) as u8 + 1; CS as usize];
                                match s.write_at(idx * CS, &fill) {
                                    Ok(()) => {
                                        s.mark_dirty(idx);
                                        model.insert(idx, fill);
                                    }
                                    Err(StagingError::Full { .. }) => refused += 1,
                                    Err(e) => panic!("{e}"),
                                }
                            }
                            _ => {
                                s.release_chunk(idx);
                                model.remove(&idx);
                            }
                        }
                        for (idx, want) in &model {
                            let mut got = vec![0u8; CS as usize];
                            s.read_at(idx * CS, &mut got).unwrap();
                            assert_eq!(&got, want, "session {ino} chunk {idx} step {step}");
                        }
                    }
                    s.discard();
                    refused
                })
            })
            .collect();
        let refused: u64 = workers.into_iter().map(|w| w.join().unwrap()).sum();
        stop.store(true, Ordering::Relaxed);
        watcher.join().unwrap();
        assert!(refused > 0, "the budget was tight enough to refuse");
        assert_eq!((node.used(), node.kept(), view.used()), (0, 0, 0));
    }

    /// A short chunk given a slot a full one used keeps the slot's whole
    /// reservation: those blocks are still the file's.
    #[test]
    fn a_reused_slot_keeps_what_its_blocks_hold_reserved() {
        let dir = TempDir::new().unwrap();
        let b = budget(1 << 20);
        let mut s = Staging::create(dir.path(), 9, 1, 64, b.clone(), host_fs()).unwrap();
        s.write_at(0, &[1u8; 64]).unwrap();
        s.mark_dirty(0);
        s.release_chunk(0);
        s.write_at(64, &[2u8; 10]).unwrap();
        assert_eq!(b.used(), 64);
        assert_eq!(s.reserved_bytes(), 64);
    }

    /// A sequential writer's staging is two slots, whatever the length
    /// it writes, and nothing is punched on its path (it used to punch
    /// every chunk it sealed, which on btrfs with compression waits for
    /// the chunk's writeback: ~170 ms per 4 MiB).
    #[test]
    fn a_sequential_writer_cycles_two_slots_without_punching() {
        let dir = TempDir::new().unwrap();
        let b = budget(1 << 30);
        let counting = Arc::new(CountingFs::default());
        let mut s = Staging::create(dir.path(), 9, 1, 64, b.clone(), counting.clone()).unwrap();
        for i in 0..100u64 {
            // An unaligned run: each write straddles a chunk boundary.
            let at = i * 64 + 32;
            s.set_len_sparse(at + 64).unwrap();
            for idx in [i, i + 1] {
                s.prepare_chunk(idx).unwrap();
            }
            s.write_at(at, &[i as u8; 64]).unwrap();
            s.mark_dirty(i);
            s.mark_dirty(i + 1);
            s.release_chunk(i);
            assert!(b.used() <= 2 * 64);
        }
        assert_eq!(counting.punches.load(Ordering::Relaxed), 0);
        assert_eq!(s.next_slot, 2);
        assert!(fs::metadata(dir.path().join("9.1")).unwrap().len() <= 2 * 64);
    }

    /// A host that cannot punch holes still gets the release; the slot
    /// is reused, so the file stays as large as the slots ever in use.
    #[test]
    fn a_failed_punch_still_releases_and_the_slot_is_reused() {
        struct NoPunch;
        impl constellation_platform::FsPrimitives for NoPunch {
            fn punch_hole(&self, _: &File, _: u64, _: u64) -> io::Result<()> {
                Err(io::ErrorKind::Unsupported.into())
            }
            fn preallocate(&self, _: &File, _: u64, _: u64, _: bool) -> io::Result<()> {
                Ok(())
            }
            fn full_fsync(&self, _: &File) -> io::Result<()> {
                Ok(())
            }
            fn drop_cache(&self, _: &File, _: u64, _: u64) -> io::Result<()> {
                Ok(())
            }
        }
        let dir = TempDir::new().unwrap();
        let b = budget(1 << 20);
        let mut s = Staging::create(dir.path(), 9, 1, 64, b.clone(), Arc::new(NoPunch)).unwrap();
        for round in 0..10u64 {
            for idx in 0..4u64 {
                s.write_at((round * 4 + idx) * 64, &[1u8; 64]).unwrap();
                s.mark_dirty(round * 4 + idx);
            }
            s.release_chunks(round * 4, round * 4 + 4);
            assert_eq!(b.used(), 128, "the kept slots only");
        }
        assert_eq!(s.next_slot, 4);
    }

    #[test]
    fn gen_counter_is_monotonic() {
        let g = GenCounter::default();
        let a = g.next();
        let b = g.next();
        assert!(b > a);
    }
}
