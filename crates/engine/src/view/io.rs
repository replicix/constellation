//! A write session's data path: read, write, truncate, `fallocate`,
//! `SEEK_DATA`/`SEEK_HOLE`.
//!
//! # Cold reads defer (plan 31 §6.3, C7b)
//!
//! A read is first tried on the frontend's thread with [`cold_probe`]
//! armed: served entirely from local state (the disk cache, the write
//! session) it completes there, exactly as before. The moment it would
//! have to *wait* for a chunk — a prefetch in flight, a forwarded chunk,
//! a peer or S3 — the fetch refuses instead, the attempt unwinds (its
//! inode lock released, its session reattached), and the whole read runs
//! again on the engine's completion pool (`crate::completion`), which
//! answers the responder. The frontend thread is free at once. What the
//! inline attempt did before it stopped is idempotent (readahead's stream
//! cursor, the atime bump), so the second run repeats it harmlessly.
//!
//! The same holds for the inode's op lock (below): an inline read that
//! would wait for it refuses and defers the same way. The holder is
//! another op on the same file — a flush publishing it (which may wait as
//! long as the store does), a write, or simply **another read**: reads
//! take the lock exclusively, and kernel readahead routinely has several
//! READs of one file in flight. So on every frontend that defers, on
//! `/dev/fuse` as on a ring, a *resident* read that contends with another
//! op on its file also hops to the completion pool, not only a cold one.
//! That costs one thread hop, and fio showed no regression (plan 38 Z2a);
//! it is needed on the ring, where a read is served on the ring thread
//! itself and a deferred cold read holds this lock for its whole fetch.
//!
//! Only a frontend that can answer from another thread
//! (`FrontendCaps::deferrable` holds `Read`) defers; for any other, and
//! with `CONSTELLATION_DEFER_COLD_READS=0`, the read waits on the calling
//! thread as it always did. The two runs are separate critical sections
//! of the inode's op lock: a write that arrives in between is ordered
//! before the read, which is a valid order for two requests the kernel had
//! in flight at once (a single thread's next request waits for this one's
//! answer).

use super::*;

/// See the module doc.
pub(super) mod cold_probe {
    use std::cell::Cell;

    thread_local! {
        /// A read is being tried inline, with deferral allowed.
        static ARMED: Cell<bool> = const { Cell::new(false) };
        /// ... and it reached a fetch that would wait.
        static TRIPPED: Cell<bool> = const { Cell::new(false) };
    }

    /// Run `read` with fetches that would wait refused. `None`: it tripped
    /// (defer it); `Some`: it completed from local state.
    pub(in crate::view) fn inline<T>(read: impl FnOnce() -> T) -> Option<T> {
        /// Disarms even if `read` panics (the thread serves other ops).
        struct Disarm;
        impl Drop for Disarm {
            fn drop(&mut self) {
                ARMED.with(|a| a.set(false));
            }
        }
        ARMED.with(|a| a.set(true));
        TRIPPED.with(|t| t.set(false));
        let out = {
            let _disarm = Disarm;
            read()
        };
        if TRIPPED.with(|t| t.replace(false)) {
            None
        } else {
            Some(out)
        }
    }

    /// At a fetch that would wait: refuse (and remember it) if a read is
    /// being tried inline on this thread.
    pub(in crate::view) fn refuse() -> bool {
        if ARMED.with(Cell::get) {
            TRIPPED.with(|t| t.set(true));
            true
        } else {
            false
        }
    }
}

/// The share of the chunk cache that dirty (not yet uploaded) chunks may
/// take before writers are throttled and then refused: all of it, less
/// `CONSTELLATION_CACHE_READ_RESERVE_PCT` percent (default 0, clamped to
/// 90) kept for clean chunks — reads and readahead.
///
/// Plan 31 C7b measured the case this is for (Mountpoint's reserved
/// prunable share): with a 128 MiB cache, 1 MiB chunks and uploads capped
/// at 4 MB/s, a writer holds the cache at its dirty limit and a
/// concurrent cold sequential reader falls from ~110 MiB/s to ~12 MiB/s —
/// every prefetched and demand-fetched chunk finds no room and is fetched
/// again for the next 128 KiB kernel read (with a 2 GiB cache the same
/// writer costs the reader ~2x, not ~9x). A reserve lets the reader keep
/// its chunks, at the price of throttling writers earlier; it is off by
/// default because it moves where a small cache refuses writes
/// (`ENOSPC` backpressure), which existing deployments tune against.
pub(super) fn dirty_budget(budget: u64) -> u64 {
    static PCT: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let pct = *PCT.get_or_init(|| {
        std::env::var("CONSTELLATION_CACHE_READ_RESERVE_PCT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
            .min(90)
    });
    budget - budget / 100 * pct
}

/// `CONSTELLATION_DEFER_COLD_READS` (default on): `0`/`false`/`off` keeps
/// every read on the frontend's thread.
pub(super) fn defer_cold_reads() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("CONSTELLATION_DEFER_COLD_READS").as_deref(),
            Ok("0") | Ok("false") | Ok("off")
        )
    })
}

/// How many of `chunk`'s bytes (the chunk at `chunk_start`) lie below
/// `floor`: the rest is dead (read as zeros). [`clip_at`]'s cut, without
/// writing to shared bytes.
fn valid_below(floor: u64, chunk_start: u64, chunk: &[u8]) -> usize {
    floor.saturating_sub(chunk_start).min(chunk.len() as u64) as usize
}

/// Zeros, from a static page rather than an allocation.
fn push_zeros(out: &mut ReadData, mut n: usize) {
    static ZEROS: [u8; 64 * 1024] = [0; 64 * 1024];
    while n > 0 {
        let k = n.min(ZEROS.len());
        out.push(Bytes::from_static(&ZEROS[..k]));
        n -= k;
    }
}

impl View {
    pub(super) fn do_read(&self, ino: Ino, offset: u64, size: u64) -> Result<ReadData, Code> {
        // Serve pending (unflushed) state when present so read-after-write
        // within an open handle is coherent. The inode's operation lock
        // orders the read against writes and flushes of the same file for
        // its whole duration; the session is detached from its shard so
        // a chunk fetch (which may wait on S3) holds no shard lock
        // (EC2 finding 1).
        //
        // Waiting for that lock is a wait like any other: tried inline, a
        // read that finds another op holding it (a flush publishing the
        // file, which may take as long as the store does) defers instead
        // (the module doc). Over FUSE-over-io_uring this is what keeps a
        // ring thread — which reads stay on, plan 38 §3(b) — from stalling
        // every other request of its queues behind someone else's flush.
        let _op = match self.inode_ops.try_lock(ino) {
            Some(op) => op,
            None if cold_probe::refuse() => return Err(Code::Again),
            None => self.inode_ops.lock(ino),
        };
        let ws = self.writes.detach(ino);
        let result = self.do_read_detached(ino, ws.as_ref(), offset, size);
        if let Some(ws) = ws {
            self.writes.reattach(ino, ws);
        }
        result
    }

    /// This view's `Arc`, when a read may defer to the completion pool:
    /// the frontend allows it, the view was opened by an engine, and
    /// deferral is not turned off.
    pub(super) fn read_deferral(&self) -> Option<Arc<View>> {
        if !self
            .caps
            .deferrable
            .contains(constellation_vfs::OpKind::Read)
            || !defer_cold_reads()
        {
            return None;
        }
        self.this.get().and_then(std::sync::Weak::upgrade)
    }

    /// Drop `ino`'s committed chunks from the local disk cache, once they
    /// are durable in the store, so the next read of them is cold (the
    /// conformance kit's `evict` hook, and tests). Returns how many were
    /// dropped. Chunks not yet uploaded are left alone and counted as an
    /// error: evicting them would lose data.
    pub fn evict_cached(&self, ino: Ino) -> Result<usize, String> {
        let ino = self.real_ino(ino);
        self.drain_inode(ino)
            .map_err(|code| format!("draining {ino}: {code:?}"))?;
        let manifest = self.load_manifest(ino).map_err(|c| format!("{c:?}"))?;
        let hashes = self.chunk_list(&manifest).map_err(|c| format!("{c:?}"))?;
        let mut dropped = 0;
        for hash in hashes.values() {
            match self.cache.state_of(hash) {
                Some(ChunkState::Clean) => {
                    self.cache.remove(hash).map_err(|e| e.to_string())?;
                    dropped += 1;
                }
                None | Some(ChunkState::Pinned) => {}
                Some(state) => return Err(format!("chunk {} is {state:?}", hash.to_hex())),
            }
        }
        Ok(dropped)
    }

    /// The bytes of `[offset, offset + size)`, as shared slices of the
    /// chunks they come from (the memory cache's verified copies, when
    /// resident: no copy, no disk read, no hash) plus zeros for holes,
    /// EOF padding and truncated-away base bytes.
    pub(super) fn do_read_detached(
        &self,
        ino: Ino,
        ws: Option<&WriteState>,
        offset: u64,
        size: u64,
    ) -> Result<ReadData, Code> {
        let manifest = self.load_manifest(ino)?;
        let attr = self.meta.getattr(ino).map_err(|e| e.code())?;
        let committed_len = attr.as_ref().map(|a| a.size).unwrap_or(manifest.file_len);
        let file_len = ws.as_ref().map(|w| w.file_len).unwrap_or(committed_len);
        if offset >= file_len {
            return Ok(ReadData::default());
        }
        // Read-time atime (plan 20): best-effort, policy-gated, and a
        // no-op unless the operator opted in. Records into a separate
        // accumulator shard (never the write shard held here), so it
        // adds a lock only in the rare bump case and cannot deadlock the
        // read. A missing attr row simply skips the bump. Placed after
        // the EOF early return: a zero-byte read need not move atime.
        if let Some(attr) = &attr {
            self.atime
                .on_read(attr, constellation_fs_core::types::now_ns());
        }
        let len = size.min(file_len - offset);
        let hashes = self.chunk_list(&manifest)?;
        if offset == 0 {
            let files = self.scan.note_read(ino);
            self.prefetch.enqueue_scan(files);
        }
        // Kick sequential readahead for upcoming committed chunks.
        self.prefetch
            .on_read(ino, offset, len, self.chunk_size, &hashes);
        let layout = constellation_fs_core::ChunkLayout::new(self.chunk_size);
        let mut out = ReadData::default();
        for slice in layout.slices(offset, len) {
            let chunk_start = slice.index * self.chunk_size as u64;
            let start = slice.offset as usize;
            let end = (slice.offset + slice.len) as usize;
            // The chunk's bytes and how many of them are valid: past
            // `valid` (a short chunk, a hole, a truncation point) it reads
            // as zeros. Nothing here copies a shared chunk.
            let (chunk, valid): (Bytes, usize) = match &ws {
                Some(w) if w.sealed.contains_key(&slice.index) => {
                    let chunk = self
                        .cache
                        .get_shared(w.sealed.get(&slice.index).unwrap())
                        .map_err(|_| Code::Io)?
                        .ok_or(Code::Io)?;
                    let valid = chunk.len();
                    (chunk, valid)
                }
                Some(w) if w.staging.is_dirty(slice.index) => {
                    // Staged bytes: just the range asked for.
                    let mut buf = vec![0u8; end - start];
                    w.staging
                        .read_at(chunk_start + start as u64, &mut buf)
                        .map_err(|e| staging_code(&e))?;
                    out.push(Bytes::from(buf));
                    continue;
                }
                // Punched whole by this session: zeros.
                Some(w) if w.zeroed.contains(slice.index) => (Bytes::new(), 0),
                Some(w) => {
                    // Untouched by this session: the base's bytes, dead
                    // past a truncation (`WriteState::floor`).
                    let chunk = self.read_committed_chunk(ino, &hashes, slice.index)?;
                    let valid = valid_below(w.base_floor(manifest.file_len), chunk_start, &chunk);
                    (chunk, valid)
                }
                None => {
                    // No session: the committed manifest, valid only
                    // below its `file_len` (a truncate lowered it; the
                    // chunk straddling it keeps dead bytes past it, and
                    // the inode's size may have grown past it since).
                    let chunk = self.read_committed_chunk(ino, &hashes, slice.index)?;
                    let valid = valid_below(manifest.file_len, chunk_start, &chunk);
                    (chunk, valid)
                }
            };
            let have = valid.min(end).saturating_sub(start);
            if have > 0 {
                out.push(chunk.slice(start..start + have));
            }
            push_zeros(&mut out, end - start - have);
        }
        Ok(out)
    }

    pub(super) fn read_committed_chunk(
        &self,
        ino: Ino,
        hashes: &constellation_fs_core::manifest::SparseChunks,
        idx: u64,
    ) -> Result<Bytes, Code> {
        match hashes.get(&idx) {
            Some(h) => self.fetch_chunk_for_inode(Some(ino), h),
            None => Ok(Bytes::new()),
        }
    }

    pub(super) fn do_write(&self, ino: Ino, offset: u64, data: &[u8]) -> Result<u32, Code> {
        if data.is_empty() {
            return Ok(0);
        }
        let dirty = self.cache.dirty_bytes();
        let budget = dirty_budget(self.cache.usage().budget);
        match crate::writeback::throttle_delay(dirty, budget) {
            Ok(delay) if !delay.is_zero() => {
                constellation_vfs::watch::stage("writeback throttle (dirty cache)");
                std::thread::sleep(delay)
            }
            Ok(_) => {}
            Err(()) => {
                // Sealed chunks grow the dirty-cache budget in
                // chunk-sized steps (plan 29 M6), exactly like staging
                // below: a small `--cache-size` relative to the chunk
                // size can cross the whole 75%-99% soft-pressure band in
                // one write and hit the hard limit with no prior
                // `throttle_delay` ever having slept. Without this grace
                // sleep, whichever of the two budgets (this one or
                // staging's, right below) happened to be the one that
                // ran out first was timing-dependent, which is what made
                // `writeback-backpressure` fail intermittently ("ENOSPC
                // arrived without observable throttling") rather than
                // consistently either way.
                std::thread::sleep(Duration::from_millis(100));
                return Err(Code::NoSpace);
            }
        }
        match crate::writeback::throttle_delay(
            self.staging_budget.used(),
            self.staging_budget.budget(),
        ) {
            Ok(delay) if !delay.is_zero() => {
                constellation_vfs::watch::stage("writeback throttle (staging)");
                std::thread::sleep(delay)
            }
            Ok(_) => {}
            Err(()) => {
                // Staging reservations grow in chunk-sized steps, so a
                // tiny budget can cross the soft-pressure band in one
                // write. Preserve observable backpressure before ENOSPC.
                std::thread::sleep(Duration::from_millis(100));
                return Err(Code::NoSpace);
            }
        }
        let _op = self.inode_ops.lock(ino);
        let manifest = self.load_manifest(ino)?;
        let hashes = self.chunk_list(&manifest)?;
        let layout = constellation_fs_core::ChunkLayout::new(self.chunk_size);
        let mut writes = self.writes.lock(ino);
        let ws = self.write_state(&mut writes, ino, &manifest)?;
        // Guard the range arithmetic: a huge offset near u64::MAX would
        // otherwise overflow and panic here while holding the write-shard
        // lock (poisoning it — see WriteShards::lock). Refuse with EFBIG,
        // as `do_fallocate` already does for the same overflow.
        let write_end = offset
            .checked_add(data.len() as u64)
            .ok_or(Code::FileTooBig)?;
        let new_file_len = ws.file_len.max(write_end);
        self.quota_check(ino, new_file_len)?;
        ws.staging
            .set_len_sparse(new_file_len)
            .map_err(|e| staging_code(&e))?;
        let mut consumed = 0usize;
        for slice in layout.slices(offset, data.len() as u64) {
            let full_len = layout.chunk_len(new_file_len, slice.index);
            let is_whole_chunk = slice.offset == 0 && slice.len == full_len;
            let chunk_start = slice.index * self.chunk_size as u64;
            let sealed = self.unseal(ws, ino, slice.index)?;
            // A chunk this session punched (or zeroed) whole has no base
            // content any more: seeded from zeros, never the base's
            // bytes (a sealed copy is this session's own, and valid).
            let punched = ws.zeroed.contains(slice.index);
            ws.holes.clear(slice.index);
            ws.staging
                .prepare_chunk(slice.index, self.chunk_size)
                .map_err(|e| staging_code(&e))?;
            // A partial (not-whole-chunk) write into a chunk this open
            // handle has not touched yet must first seed the untouched
            // bytes from the committed content — otherwise they would
            // read back as a spurious hole (zero) instead of their real
            // pre-write value.
            if !is_whole_chunk && !ws.staging.is_dirty(slice.index) {
                let seed = match sealed {
                    Some(hash) => {
                        let mut data = self
                            .cache
                            .get(&hash)
                            .map_err(|_| Code::Io)?
                            .ok_or(Code::Io)?;
                        data.resize(full_len as usize, 0);
                        data
                    }
                    None if punched => vec![0u8; full_len as usize],
                    None => {
                        let mut seed =
                            self.committed_chunk_padded(&hashes, slice.index, full_len)?;
                        ws.clip_base(&mut seed, chunk_start, manifest.file_len);
                        seed
                    }
                };
                ws.staging
                    .write_at(chunk_start, &seed)
                    .map_err(|e| staging_code(&e))?;
            }
            let write_start = chunk_start + u64::from(slice.offset);
            let write_end = write_start + u64::from(slice.len);
            ws.staging
                .write_at(write_start, &data[consumed..consumed + slice.len as usize])
                .map_err(|e| staging_code(&e))?;
            ws.staging.mark_dirty(slice.index);
            ws.written.push((write_start, write_end));
            consumed += slice.len as usize;
        }
        ws.file_len = new_file_len;
        if offset <= ws.high_water && write_end > ws.high_water {
            ws.high_water = write_end;
        }
        self.seal_crossed_chunks(ino, ws)?;
        Ok(data.len() as u32)
    }

    pub(super) fn truncate(&self, ino: Ino, new_size: u64) -> Result<(), Code> {
        let _op = self.inode_ops.lock(ino);
        let manifest = self.load_manifest(ino)?;
        let mut writes = self.writes.lock(ino);
        self.truncate_locked(&mut writes, ino, new_size, &manifest)
    }

    pub(super) fn truncate_locked(
        &self,
        writes: &mut HashMap<Ino, WriteState>,
        ino: Ino,
        new_size: u64,
        manifest: &Manifest,
    ) -> Result<(), Code> {
        let layout = constellation_fs_core::ChunkLayout::new(self.chunk_size);
        let cs = u64::from(self.chunk_size);
        let ws = self.write_state(writes, ino, manifest)?;
        self.quota_check(ino, new_size)?;
        if new_size < ws.file_len {
            // Everything at or past `new_size` is dead, whatever holds it
            // — the base (`floor`: zeroed wherever base content is read
            // for this session), this session's sealed chunks, its
            // staged bytes, its recorded writes — so that a later
            // extension reads zeros there.
            ws.floor = Some(ws.floor.map_or(new_size, |f| f.min(new_size)));
            let keep = layout.chunk_count(new_size);
            let old_chunks = layout.chunk_count(ws.file_len);
            // Sealed chunks: wholly past the point, dropped; the one the
            // point falls inside goes back to staging, to be cut below.
            let sealed: Vec<u64> = ws
                .sealed
                .keys()
                .copied()
                .filter(|i| *i * cs < ws.file_len)
                .collect();
            for idx in sealed {
                if idx * cs >= new_size {
                    self.unseal(ws, ino, idx)?;
                } else if (idx + 1) * cs > new_size {
                    let hash = self.unseal(ws, ino, idx)?.expect("sealed");
                    let data = self
                        .cache
                        .get(&hash)
                        .map_err(|_| Code::Io)?
                        .ok_or(Code::Io)?;
                    ws.staging
                        .prepare_chunk(idx, self.chunk_size)
                        .map_err(|e| staging_code(&e))?;
                    ws.staging
                        .write_at(idx * cs, &data)
                        .map_err(|e| staging_code(&e))?;
                    // (Its written ranges are already recorded; the rest
                    // of it was seeded from the base, which the
                    // composition re-reads, clipped.)
                    ws.staging.mark_dirty(idx);
                }
            }
            // Drop dirty runs past the new end; `Staging::set_len`
            // (ftruncate) cuts the boundary chunk's staged bytes.
            ws.staging.retain_dirty_below(keep);
            ws.staging.punch_chunks(keep, old_chunks, self.chunk_size);
            ws.written.retain_mut(|(start, end)| {
                *end = (*end).min(new_size);
                *start < *end
            });
            ws.high_water = ws.high_water.min(new_size);
        }
        ws.staging
            .set_len_sparse(new_size)
            .map_err(|e| staging_code(&e))?;
        ws.file_len = new_size;
        Ok(())
    }

    pub(super) fn do_fallocate(
        &self,
        ino: Ino,
        offset: u64,
        length: u64,
        mode: FallocateMode,
    ) -> Result<(), Code> {
        let keep_size = mode.contains(FallocateMode::KEEP_SIZE);
        let punch = mode.contains(FallocateMode::PUNCH_HOLE);
        let zero = mode.contains(FallocateMode::ZERO_RANGE);
        if mode.contains(FallocateMode::UNSUPPORTED) || (punch && !keep_size) || (punch && zero) {
            return Err(Code::NotSupported);
        }
        let end = offset.checked_add(length).ok_or(Code::FileTooBig)?;
        // Held across the truncate and the boundary writes below (the
        // lock is re-entrant on this thread).
        let _op = self.inode_ops.lock(ino);
        let manifest = self.load_manifest(ino)?;
        let old_size = self
            .writes
            .lock(ino)
            .get(&ino)
            .map(|state| state.file_len)
            .unwrap_or(manifest.file_len);
        let new_size = if keep_size {
            old_size
        } else {
            old_size.max(end)
        };
        // Gate growth before any staging mutation: the zero-range branch
        // below extends the file itself and never reaches `truncate`.
        if new_size > old_size {
            self.quota_check(ino, new_size)?;
        }
        if !punch && !zero {
            if new_size != old_size {
                self.truncate(ino, new_size)?;
            }
            return Ok(());
        }

        let effective_end = end.min(new_size);
        if offset >= effective_end {
            return Ok(());
        }
        let chunk_size = u64::from(self.chunk_size);
        let full_start = offset.div_ceil(chunk_size);
        let full_end = effective_end / chunk_size;
        {
            let mut writes = self.writes.lock(ino);
            let ws = self.write_state(&mut writes, ino, &manifest)?;
            if new_size > ws.file_len {
                ws.staging
                    .set_len_sparse(new_size)
                    .map_err(|error| staging_code(&error))?;
                ws.file_len = new_size;
            }
            if full_start < full_end {
                ws.holes.mark_range(full_start, full_end);
                ws.zeroed.mark_range(full_start, full_end);
                let sealed: Vec<_> = ws
                    .sealed
                    .keys()
                    .copied()
                    .filter(|index| *index >= full_start && *index < full_end)
                    .collect();
                for index in sealed {
                    self.unseal(ws, ino, index)?;
                }
                ws.staging
                    .punch_chunks(full_start, full_end, self.chunk_size);
            }
        }

        // Boundary chunks remain ordinary data chunks after zeroing the
        // selected bytes. At most two chunk-sized buffers are materialized.
        if full_start >= full_end {
            self.do_write(ino, offset, &vec![0; (effective_end - offset) as usize])?;
            return Ok(());
        }
        let first_boundary_end = effective_end.min(full_start * chunk_size);
        if offset < first_boundary_end {
            self.do_write(
                ino,
                offset,
                &vec![0; (first_boundary_end - offset) as usize],
            )?;
        }
        let last_boundary_start = offset.max(full_end * chunk_size);
        if last_boundary_start < effective_end {
            self.do_write(
                ino,
                last_boundary_start,
                &vec![0; (effective_end - last_boundary_start) as usize],
            )?;
        }
        Ok(())
    }

    pub(super) fn seek_sparse(
        &self,
        ino: Ino,
        offset: u64,
        whence: SeekWhence,
    ) -> Result<i64, Code> {
        let _op = self.inode_ops.lock(ino);
        let manifest = self.load_manifest(ino)?;
        let mut chunks = self.chunk_list(&manifest)?;
        let writes = self.writes.lock(ino);
        let file_len = if let Some(state) = writes.get(&ino) {
            let floor = state.base_floor(manifest.file_len);
            let cs = u64::from(self.chunk_size);
            chunks.retain(|index, _| !state.zeroed.contains(*index) && *index * cs < floor);
            chunks.extend(state.sealed.iter().map(|(index, hash)| (*index, *hash)));
            for index in state.staging.dirty_indices() {
                chunks.insert(index, ChunkHash([1; 32]));
            }
            state.file_len
        } else {
            manifest.file_len
        };
        if offset >= file_len {
            return Err(Code::NoDeviceOrAddress);
        }
        let chunk_size = u64::from(self.chunk_size);
        let start_index = offset / chunk_size;
        match whence {
            SeekWhence::Data => {
                if chunks.contains_key(&start_index) {
                    return Ok(offset as i64);
                }
                chunks
                    .range(start_index + 1..)
                    .next()
                    .map(|(&index, _)| (index * chunk_size) as i64)
                    .filter(|position| *position < file_len as i64)
                    .ok_or(Code::NoDeviceOrAddress)
            }
            SeekWhence::Hole => {
                if !chunks.contains_key(&start_index) {
                    return Ok(offset as i64);
                }
                let mut index = start_index + 1;
                while chunks.contains_key(&index) {
                    index += 1;
                }
                Ok((index * chunk_size).min(file_len) as i64)
            }
            _ => Err(Code::Invalid),
        }
    }
}
