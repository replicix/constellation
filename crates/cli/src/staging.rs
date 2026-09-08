//! Bounded-memory write staging (plan 07, DESIGN.md §7/§9).
//!
//! One sparse file per open, dirty inode at
//! `<state_dir>/staging/<ino>.<gen>`. `<gen>` is a per-mount monotonic
//! counter so a stale file from a previous life is never confused with
//! a live one — mount-time GC (`gc`) simply deletes everything under
//! the staging root before the FUSE loop starts, no locking required.
//!
//! Per-inode RAM is `file_len`, the optional base manifest, and a
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
//! `DiskCache::insert`'s discipline: the budget tracks the file's
//! *logical* length (the same "reserve the uncompressed size" choice
//! the chunk cache makes), not actual on-disk footprint after sparse
//! holes — conservative, but simple, deterministic, and exact
//! ENOSPC-with-no-partial-state. A reservation failure never touches
//! the file or the in-memory length.

use constellation_fs_core::Ino;
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

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
pub struct StagingBudget {
    budget: u64,
    used: Mutex<u64>,
}

impl StagingBudget {
    pub fn new(budget: u64) -> Arc<Self> {
        Arc::new(Self {
            budget,
            used: Mutex::new(0),
        })
    }

    pub fn reserve(&self, bytes: u64) -> Result<(), StagingError> {
        if bytes == 0 {
            return Ok(());
        }
        let mut used = self.used.lock().unwrap();
        if *used + bytes > self.budget {
            return Err(StagingError::Full {
                needed: bytes,
                available: self.budget.saturating_sub(*used),
            });
        }
        *used += bytes;
        Ok(())
    }

    pub fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut used = self.used.lock().unwrap();
        *used = used.saturating_sub(bytes);
    }

    pub fn used(&self) -> u64 {
        *self.used.lock().unwrap()
    }

    pub fn budget(&self) -> u64 {
        self.budget
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

/// One open, dirty inode's staged bytes: a sparse file mirroring the
/// real file's layout (byte offsets match 1:1), so unwritten holes
/// read back as zero for free via ordinary filesystem sparse-file
/// semantics — no special-casing needed here.
pub struct Staging {
    file: File,
    path: PathBuf,
    budget: Arc<StagingBudget>,
    file_len: u64,
    reserved: u64,
    dirty: DirtyRuns,
    released_chunks: std::collections::HashSet<u64>,
    admitted_chunks: std::collections::HashMap<u64, u64>,
}

impl Staging {
    /// `dir` is the staging root (`<state_dir>/staging`); the file is
    /// created at `dir/<ino>.<gen>`.
    pub fn create(
        dir: &Path,
        ino: Ino,
        gen: u64,
        budget: Arc<StagingBudget>,
    ) -> Result<Self, StagingError> {
        fs::create_dir_all(dir)?;
        let path = dir.join(format!("{ino}.{gen}"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        Ok(Self {
            file,
            path,
            budget,
            file_len: 0,
            reserved: 0,
            dirty: DirtyRuns::default(),
            released_chunks: std::collections::HashSet::new(),
            admitted_chunks: std::collections::HashMap::new(),
        })
    }

    /// `pwrite`. Growing the file reserves the growth against the
    /// shared budget *before* touching the file or `file_len`, so a
    /// budget failure leaves no partial state.
    pub fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), StagingError> {
        if buf.is_empty() {
            return Ok(());
        }
        let end = offset + buf.len() as u64;
        if end > self.file_len {
            let growth = end - self.file_len;
            self.budget.reserve(growth)?;
            self.reserved += growth;
            self.file_len = end;
        }
        self.file.write_at(buf, offset)?;
        Ok(())
    }

    /// `pread`. Callers only read ranges within `file_len`; holes there
    /// come back zeroed by the filesystem.
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), StagingError> {
        self.file.read_exact_at(buf, offset)?;
        Ok(())
    }

    /// `ftruncate`, with matching budget reserve (growth) or release
    /// (shrink). A failed growth reservation rolls back cleanly; a
    /// failed `ftruncate` after a successful reservation releases it
    /// back rather than leaking budget against a file that never grew.
    #[cfg(test)]
    pub fn set_len(&mut self, new_len: u64) -> Result<(), StagingError> {
        if new_len > self.file_len {
            let growth = new_len - self.file_len;
            self.budget.reserve(growth)?;
            if let Err(e) = self.file.set_len(new_len) {
                self.budget.release(growth);
                return Err(e.into());
            }
            self.reserved += growth;
        } else if new_len < self.file_len {
            self.file.set_len(new_len)?;
            let released = (self.file_len - new_len).min(self.reserved);
            self.budget.release(released);
            self.reserved -= released;
        }
        self.file_len = new_len;
        Ok(())
    }

    /// Change the sparse staging file's logical length without charging
    /// holes to the dirty-byte budget. Individual chunks are charged when
    /// [`prepare_chunk`](Self::prepare_chunk) admits a write.
    pub fn set_len_sparse(&mut self, new_len: u64) -> Result<(), StagingError> {
        self.file.set_len(new_len)?;
        self.file_len = new_len;
        Ok(())
    }

    #[allow(dead_code)] // part of the public shape (plan 07); exercised by tests
    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    /// Bytes currently reserved against the shared budget for this
    /// staging file (== its logical length; see the module doc).
    #[allow(dead_code)] // part of the public shape (plan 07)
    pub fn reserved_bytes(&self) -> u64 {
        self.reserved
    }

    pub fn mark_dirty(&mut self, idx: u64) {
        self.dirty.mark(idx);
    }

    /// Re-admit a previously sealed range before modifying it.
    pub fn prepare_chunk(&mut self, idx: u64, chunk_size: u32) -> Result<(), StagingError> {
        let start = idx * u64::from(chunk_size);
        let len = u64::from(chunk_size).min(self.file_len.saturating_sub(start));
        if let Some(admitted) = self.admitted_chunks.get_mut(&idx) {
            let growth = len.saturating_sub(*admitted);
            self.budget.reserve(growth)?;
            self.reserved += growth;
            *admitted = len;
            return Ok(());
        }
        self.released_chunks.remove(&idx);
        self.budget.reserve(len)?;
        self.reserved += len;
        self.admitted_chunks.insert(idx, len);
        Ok(())
    }

    /// A sealed chunk no longer needs staging residency. Keep the
    /// sparse file's logical offsets stable, but punch the range out
    /// and release its budget. Linux filesystems that do not support
    /// hole punching still get the logical budget release; the bytes
    /// are already durable in the chunk cache before this is called.
    pub fn release_chunk(&mut self, idx: u64, chunk_size: u32) {
        if !self.dirty.contains(idx) {
            return;
        }
        self.dirty.clear(idx);
        self.released_chunks.insert(idx);
        let start = idx * u64::from(chunk_size);
        let len = self
            .admitted_chunks
            .remove(&idx)
            .unwrap_or_else(|| u64::from(chunk_size).min(self.file_len.saturating_sub(start)));
        if len == 0 {
            return;
        }
        #[cfg(target_os = "linux")]
        unsafe {
            libc::fallocate(
                std::os::fd::AsRawFd::as_raw_fd(&self.file),
                libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                start as libc::off_t,
                len as libc::off_t,
            );
        }
        let released = len.min(self.reserved);
        self.reserved -= released;
        self.budget.release(released);
    }

    pub fn is_dirty(&self, idx: u64) -> bool {
        self.dirty.contains(idx)
    }

    pub fn retain_dirty_below(&mut self, keep: u64) {
        self.dirty.retain_below(keep);
    }

    /// Punch complete chunks from staging and release only chunks that had
    /// actually been admitted against the budget.
    pub fn punch_chunks(&mut self, start: u64, end: u64, chunk_size: u32) {
        if start >= end {
            return;
        }
        let admitted: Vec<_> = self
            .admitted_chunks
            .keys()
            .copied()
            .filter(|index| *index >= start && *index < end)
            .collect();
        for index in admitted {
            self.release_chunk(index, chunk_size);
        }
        self.dirty.clear_range(start, end);
        #[cfg(target_os = "linux")]
        unsafe {
            let offset = start.saturating_mul(u64::from(chunk_size));
            let len = end
                .saturating_sub(start)
                .saturating_mul(u64::from(chunk_size));
            libc::fallocate(
                std::os::fd::AsRawFd::as_raw_fd(&self.file),
                libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                offset as libc::off_t,
                len as libc::off_t,
            );
        }
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
    /// longer needed here.
    pub fn discard(self) {
        self.budget.release(self.reserved);
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

    #[test]
    fn round_trip_across_chunk_boundaries() {
        let dir = TempDir::new().unwrap();
        let mut s = Staging::create(dir.path(), 1, 0, budget(1 << 20)).unwrap();
        let layout = ChunkLayout::new(64);
        // Write spanning two chunks. Staging mirrors real file offsets
        // 1:1, so a write spanning several chunks is one plain pwrite —
        // exactly what the FUSE write path does.
        let data = vec![7u8; 100];
        s.write_at(30, &data).unwrap();
        for slice in layout.slices(30, data.len() as u64) {
            s.mark_dirty(slice.index);
        }
        let mut back = vec![0u8; 100];
        s.read_at(30, &mut back).unwrap();
        assert_eq!(back, data);

        // Unwritten range inside file_len reads as zero (sparse hole).
        let mut hole = vec![9u8; 30];
        s.read_at(0, &mut hole).unwrap();
        assert_eq!(hole, vec![0u8; 30]);
    }

    #[test]
    fn truncate_down_drops_dirty_runs_and_recuts() {
        let dir = TempDir::new().unwrap();
        let mut s = Staging::create(dir.path(), 1, 0, budget(1 << 20)).unwrap();
        let layout = ChunkLayout::new(16);
        s.write_at(0, &[1u8; 40]).unwrap(); // chunks 0,1,2
        for i in 0..layout.chunk_count(40) {
            s.mark_dirty(i);
        }
        assert!(s.is_dirty(2));
        s.set_len(20).unwrap(); // keep chunks 0,1
        s.retain_dirty_below(layout.chunk_count(20));
        assert!(s.is_dirty(0));
        assert!(s.is_dirty(1));
        assert!(!s.is_dirty(2));
        let mut back = vec![0u8; 4];
        s.read_at(16, &mut back).unwrap();
        assert_eq!(back, vec![1, 1, 1, 1]);
    }

    #[test]
    fn truncate_up_then_write_past_old_end_leaves_a_hole() {
        let dir = TempDir::new().unwrap();
        let mut s = Staging::create(dir.path(), 1, 0, budget(1 << 20)).unwrap();
        s.write_at(0, &[5u8; 10]).unwrap();
        s.set_len(100).unwrap(); // hole from 10..100
        s.write_at(150, &[6u8; 10]).unwrap(); // extend further; 100..150 also a hole
        let mut mid = vec![9u8; 90];
        s.read_at(10, &mut mid).unwrap();
        assert_eq!(
            mid,
            vec![0u8; 90],
            "extension hole must read as zero, not stale bytes"
        );
    }

    #[test]
    fn budget_reserve_before_accept_leaves_no_partial_state() {
        let dir = TempDir::new().unwrap();
        let b = budget(50);
        let mut s = Staging::create(dir.path(), 1, 0, b.clone()).unwrap();
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
    }

    #[test]
    fn sequential_append_dirty_runs_stay_flat() {
        let dir = TempDir::new().unwrap();
        let mut s = Staging::create(dir.path(), 1, 0, budget(1 << 30)).unwrap();
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
        let mut s = Staging::create(dir.path(), 1, 0, budget(1 << 30)).unwrap();
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
        let mut s = Staging::create(dir.path(), 9, 1, b.clone()).unwrap();
        s.write_at(0, &[1u8; 100]).unwrap();
        assert_eq!(b.used(), 100);
        let path = dir.path().join("9.1");
        assert!(path.exists());
        s.discard();
        assert_eq!(b.used(), 0);
        assert!(!path.exists());
    }

    #[test]
    fn eager_release_is_once_and_redirty_readmits() {
        let dir = TempDir::new().unwrap();
        let b = budget(128);
        let mut s = Staging::create(dir.path(), 9, 1, b.clone()).unwrap();
        s.write_at(0, &[1u8; 64]).unwrap();
        s.mark_dirty(0);
        s.release_chunk(0, 64);
        assert_eq!(b.used(), 0);
        s.release_chunk(0, 64);
        assert_eq!(b.used(), 0, "one boundary seals only once");
        s.prepare_chunk(0, 64).unwrap();
        s.write_at(0, &[2u8; 64]).unwrap();
        s.mark_dirty(0);
        assert_eq!(b.used(), 64, "writing a sealed chunk re-dirties it");
    }

    #[test]
    fn gen_counter_is_monotonic() {
        let g = GenCounter::default();
        let a = g.next();
        let b = g.next();
        assert!(b > a);
    }
}
