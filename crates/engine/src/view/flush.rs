//! Chunks and publication: fetching committed chunks, sealing a
//! sequential writer's crossed chunks, and the flush that composes and
//! commits a write session's manifest (locally, or forwarded with rebase).

use super::*;
use crate::recovery::conflict_name_for_path;

/// Sequential runs a write session follows at once ([`View::note_write`]).
const MAX_RUNS: usize = 8;
/// Whole dirty chunks a write seals under staging pressure, at most
/// ([`View::note_write`]).
const PRESSURE_SEALS: usize = 8;

/// How many times a forwarded whole-file manifest commit may rebase
/// onto a concurrent update before giving up with `EAGAIN`. Each pass
/// costs one round trip to the holder and loses only to a writer that
/// committed in between, so a handful of attempts absorbs a conflict
/// storm across a realistic writer set without spinning forever.
pub(super) const MANIFEST_COMMIT_ATTEMPTS: u32 = 8;

/// `CONSTELLATION_COMPOSE_CONCURRENCY`: how many chunks one flush composes
/// at once, its own thread included (default `min(CPUs, 8)`, like
/// `CONSTELLATION_ENCODE_CONCURRENCY`; `1` composes one at a time).
fn compose_concurrency() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("CONSTELLATION_COMPOSE_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1)
                    .min(8)
            })
            .max(1)
    })
}

/// `CONSTELLATION_COMPOSE_MEMORY` (bytes, default 64 MiB): what the
/// compose workers beyond each flush's own thread may hold at once,
/// node-wide. A worker holds up to two chunks (the image it builds and,
/// transiently, the base chunk read from disk), so it is charged twice
/// the chunk size: at 4 MiB chunks the default admits eight, at 64 MiB
/// none (such a flush composes one chunk at a time, as before).
fn compose_memory() -> u64 {
    static N: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("CONSTELLATION_COMPOSE_MEMORY")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(64 << 20)
    })
}

/// The bytes compose workers hold node-wide ([`compose_memory`]).
static COMPOSE_MEMORY_USED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Memory reserved in `used` (out of `limit`) for up to `want` extra
/// compose workers of `per` bytes each, as many as fit: the count
/// granted, released on drop.
pub(super) struct ComposeWorkers<'a> {
    used: &'a std::sync::atomic::AtomicU64,
    pub(super) granted: usize,
    per: u64,
}

impl<'a> ComposeWorkers<'a> {
    pub(super) fn reserve(
        used: &'a std::sync::atomic::AtomicU64,
        limit: u64,
        want: usize,
        per: u64,
    ) -> Self {
        use std::sync::atomic::Ordering::{AcqRel, Acquire};
        let mut granted = 0;
        while granted < want
            && used
                .fetch_update(AcqRel, Acquire, |now| {
                    now.checked_add(per).filter(|next| *next <= limit)
                })
                .is_ok()
        {
            granted += 1;
        }
        Self { used, granted, per }
    }
}

impl Drop for ComposeWorkers<'_> {
    fn drop(&mut self) {
        self.used.fetch_sub(
            self.per * self.granted as u64,
            std::sync::atomic::Ordering::AcqRel,
        );
    }
}

#[cfg(test)]
thread_local! {
    /// Tests: the compose concurrency of flushes on this thread, in
    /// place of [`compose_concurrency`]'s.
    pub(super) static COMPOSE_CONCURRENCY: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
    /// Tests: how long each chunk's compose is held, so the workers of a
    /// flush on this thread are seen to overlap.
    pub(super) static COMPOSE_HOLD: std::cell::Cell<Duration> =
        const { std::cell::Cell::new(Duration::ZERO) };
    /// Tests: the most chunks the last compose on this thread had in
    /// progress at once.
    pub(super) static COMPOSE_PEAK: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Run `compose` on every chunk index in `jobs` — on the calling thread
/// and on as many scoped workers as [`compose_concurrency`] and
/// [`compose_memory`] admit — and return the outcomes in `jobs`' order.
/// The first failure (in `jobs`' order, among the chunks attempted) is
/// the result; after one, no worker starts another chunk. A panic in a
/// worker is resumed on the calling thread.
fn compose_each<T: Send>(
    jobs: &[u64],
    chunk_size: u32,
    compose: impl Fn(u64) -> Result<T, Code> + Sync,
) -> Result<Vec<T>, Code> {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
    #[cfg(not(test))]
    let concurrency = compose_concurrency();
    #[cfg(test)]
    let concurrency = COMPOSE_CONCURRENCY
        .with(|c| c.get())
        .unwrap_or_else(compose_concurrency);
    #[cfg(test)]
    let hold = COMPOSE_HOLD.with(|h| h.get());
    let want = concurrency.min(jobs.len()).saturating_sub(1);
    let workers = ComposeWorkers::reserve(
        &COMPOSE_MEMORY_USED,
        compose_memory(),
        want,
        2 * u64::from(chunk_size),
    );
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let (running, peak) = (AtomicUsize::new(0), AtomicUsize::new(0));
    /// A panicking compose stops the others from starting another chunk,
    /// as a failing one does (the panic is resumed after the joins).
    struct FailOnPanic<'a>(&'a AtomicBool);
    impl Drop for FailOnPanic<'_> {
        fn drop(&mut self) {
            if std::thread::panicking() {
                self.0.store(true, Relaxed);
            }
        }
    }
    let work = || {
        let _fail_on_panic = FailOnPanic(&failed);
        let mut done = Vec::new();
        while !failed.load(Relaxed) {
            let i = next.fetch_add(1, Relaxed);
            let Some(idx) = jobs.get(i) else {
                break;
            };
            peak.fetch_max(running.fetch_add(1, Relaxed) + 1, Relaxed);
            #[cfg(test)]
            std::thread::sleep(hold);
            let outcome = compose(*idx);
            running.fetch_sub(1, Relaxed);
            if outcome.is_err() {
                failed.store(true, Relaxed);
            }
            done.push((i, outcome));
        }
        done
    };
    if workers.granted == 0 {
        let done = work();
        #[cfg(test)]
        COMPOSE_PEAK.with(|p| p.set(peak.load(Relaxed)));
        return done.into_iter().map(|(_, outcome)| outcome).collect();
    }
    let mut done = std::thread::scope(|scope| {
        // Each worker works for the flush's op: the stall watchdog names
        // the stage a worker waits in (a chunk fetch from S3, say), not
        // only the flush's own.
        let spawned: Vec<_> = (0..workers.granted)
            .filter_map(|_| {
                let op = constellation_vfs::watch::current();
                std::thread::Builder::new()
                    .name("compose".into())
                    .spawn_scoped(scope, move || {
                        if let Some(op) = op {
                            op.adopt();
                        }
                        work()
                    })
                    .ok()
            })
            .collect();
        let mut done = work();
        for worker in spawned {
            match worker.join() {
                Ok(theirs) => done.extend(theirs),
                Err(panic) => std::panic::resume_unwind(panic),
            }
        }
        done
    });
    drop(workers);
    #[cfg(test)]
    COMPOSE_PEAK.with(|p| p.set(peak.load(Relaxed)));
    done.sort_unstable_by_key(|(i, _)| *i);
    let mut out = Vec::with_capacity(jobs.len());
    for (_, outcome) in done {
        out.push(outcome?);
    }
    // Every job ran unless one failed, and then the loop above returned.
    debug_assert_eq!(out.len(), jobs.len());
    Ok(out)
}

impl View {
    pub(super) fn load_manifest(&self, ino: Ino) -> Result<Manifest, Code> {
        match self.meta.manifest(ino) {
            Ok(Some(bytes)) => Manifest::decode(&bytes).map_err(|_| Code::Io),
            Ok(None) => match self.meta.scratch_manifest(ino) {
                Ok(Some(bytes)) => Manifest::decode(&bytes).map_err(|_| Code::Io),
                Ok(None) => Ok(Manifest::empty(self.chunk_size)),
                Err(e) => Err(e.code()),
            },
            Err(e) => Err(e.code()),
        }
    }

    /// Resolve the sparse data-chunk map (following manifest spill).
    pub(super) fn chunk_list(&self, m: &Manifest) -> Result<SparseChunks, Code> {
        match &m.chunks {
            ChunkInfo::Inline(v) => Ok(v.clone()),
            ChunkInfo::Spilled(h) => {
                let blob = self.fetch_chunk(h)?;
                decode_chunk_list(&blob).map_err(|_| Code::Io)
            }
        }
    }

    /// Get one chunk: cache first (memory, then the verified disk copy),
    /// then object store (inserted clean). An in-flight prefetch for the
    /// same chunk is awaited rather than duplicated. Shared bytes: a
    /// caller that edits them takes its own copy (`Vec::from`).
    pub(super) fn fetch_chunk(&self, hash: &ChunkHash) -> Result<Bytes, Code> {
        self.fetch_chunk_for_inode(None, hash)
    }

    /// A chunk another node forwarded in a manifest while it was still
    /// uploading there (`meta::store::remote`: only a sequencer has such
    /// manifests before the chunk is up) is in no store yet and no peer
    /// serves it: wait for its report rather than fail the read, up to
    /// `CONSTELLATION_REMOTE_CHUNK_WAIT_S`.
    pub(super) fn wait_forwarded_chunk(&self, hash: &ChunkHash) {
        if !self.meta.awaits_remote_chunk(hash).unwrap_or(false) {
            return;
        }
        let started = std::time::Instant::now();
        let limit = crate::upload::remote_chunk_wait();
        while started.elapsed() < limit && self.meta.awaits_remote_chunk(hash).unwrap_or(false) {
            std::thread::sleep(Duration::from_millis(10));
        }
        tracing::debug!(
            hash = %hash.to_hex(),
            waited_ms = started.elapsed().as_millis() as u64,
            "read waited for a chunk another node forwarded as pending"
        );
    }

    pub(super) fn fetch_chunk_for_inode(
        &self,
        ino: Option<Ino>,
        hash: &ChunkHash,
    ) -> Result<Bytes, Code> {
        if let Ok(Some(data)) = self.cache.get_shared(hash) {
            return Ok(data);
        }
        // Everything past here may wait (a prefetch in flight, a
        // forwarded chunk, a peer, S3): a read tried inline with deferral
        // allowed stops here and goes to the completion pool instead.
        if super::io::cold_probe::refuse() {
            return Err(Code::Again);
        }
        if self.prefetch.claim_for_demand(hash) {
            if let Some(ino) = ino {
                self.prefetch.note_stall(ino);
                self.scan.note_stall(ino);
            }
        }
        if self.prefetch.is_inflight(hash) {
            constellation_vfs::watch::stage("chunk fetch: prefetch in flight");
            while self.prefetch.is_inflight(hash) {
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        }
        if let Ok(Some(data)) = self.cache.get_shared(hash) {
            return Ok(data);
        }
        if let Some(ino) = ino {
            self.prefetch.note_stall(ino);
            self.scan.note_stall(ino);
        }
        constellation_vfs::watch::stage("chunk fetch: forwarded chunk wait");
        self.wait_forwarded_chunk(hash);
        if let Some(coop) = &self.coop {
            constellation_vfs::watch::stage("chunk fetch: coop (peers/S3)");
            return self
                .rt
                .block_on(coop.fetch(hash))
                .map(|data| {
                    // Verified in flight (a peer fetch hashes as it
                    // decodes, an S3 spill before it is committed): the
                    // memory tier can have them without a disk read
                    // (plan 38 §2.3).
                    let data = Bytes::from(data);
                    self.cache.admit_verified(hash, data.clone());
                    data
                })
                .map_err(|error| {
                    tracing::warn!(
                        hash = %hash.to_hex(),
                        error = %error,
                        "coop fetch failed"
                    );
                    Code::Io
                });
        }
        let mut data = None;
        let mut last_error = None;
        constellation_vfs::watch::stage("chunk fetch: S3");
        for attempt in 0..3 {
            let fetched = (|| {
                let mut spill = self
                    .cache
                    .begin_spill()
                    .map_err(|e| format!("begin_spill (plain): {e}"))?;
                let result = if self.store.is_e2e() {
                    let mut cipher = self
                        .cache
                        .begin_spill()
                        .map_err(|e| format!("begin_spill (cipher): {e}"))?;
                    self.rt.block_on(self.store.get_chunk_to_writer_e2e(
                        hash,
                        &mut cipher,
                        &mut spill,
                        DecodePriority::Demand,
                    ))
                } else {
                    self.rt
                        .block_on(self.store.get_chunk_to_writer(hash, &mut spill))
                }
                .map_err(|e| format!("get_chunk_to_writer: {e}"))?;
                let _ = result;
                spill.rewind().map_err(|e| format!("spill rewind: {e}"))?;
                let mut bytes = Vec::new();
                spill
                    .read_to_end(&mut bytes)
                    .map_err(|e| format!("spill read_to_end: {e}"))?;
                let _ = self.cache.commit_spill(hash, spill, ChunkState::Clean);
                // Hashed in flight by `get_chunk_to_writer` on their way
                // into the spill: the first read of this chunk is a
                // memory hit rather than a whole-file read and a second
                // hash (plan 38 §2.3).
                let bytes = Bytes::from(bytes);
                self.cache.admit_verified(hash, bytes.clone());
                Ok::<_, String>(bytes)
            })();
            match fetched {
                Ok(bytes) => {
                    data = Some(bytes);
                    break;
                }
                Err(error) => last_error = Some(error),
            }
            if attempt < 2 {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
        let data = data.ok_or_else(|| {
            tracing::warn!(
                hash = %hash.to_hex(),
                attempts = 3,
                error = last_error.as_deref().unwrap_or("unknown"),
                "direct S3 chunk fetch failed after retries"
            );
            Code::Io
        })?;
        Ok(data)
    }

    /// Chunk `idx` (`len` bytes) as write session `ws` leaves it, into
    /// `data`: the bytes its writes delivered (`ws.written`, read from
    /// staging) laid over `base`'s content for the chunk — zeros where
    /// `base` has no chunk, past its length or the session's truncation
    /// floor (`WriteState::clip_base`), and in a chunk the session
    /// punched or zeroed whole (`zeroed`). The base's chunk is fetched
    /// only when the writes leave part of the chunk to it (plan 30 §M9 ×
    /// §M4: the base's chunk may be one only a departed holder had — an
    /// adopted, held manifest — and an overwrite must not depend on it).
    ///
    /// The one definition of a dirty chunk's content: the flush composes
    /// it over the base it commits on (rebasing re-composes it over the
    /// manifest that is current, 124cc95), a seal over the manifest
    /// current at the write that seals it (a sealed chunk's content is
    /// then fixed: the flush names its hash as is, and a rebase does not
    /// merge another writer's bytes into it — as before, when the seal
    /// froze the copy made at the first write), and a read of a dirty chunk
    /// over the manifest current at the read (`View::read_dirty`, the
    /// same rule piecewise). So a session reads back what its flush would
    /// publish were it to commit then; the staging file holds only the
    /// written bytes (finding 09; [`WriteState::written`]).
    pub(super) fn chunk_image(
        &self,
        ws: &WriteState,
        idx: u64,
        len: usize,
        base: &ImageBase<'_>,
        data: &mut Vec<u8>,
    ) -> Result<Option<StagedBase>, Code> {
        let start = idx * u64::from(self.chunk_size);
        let end = start + len as u64;
        data.clear();
        data.reserve(len);
        let floor = ws.base_floor(base.file_len);
        // The base chunk the image holds, and how much of it: what a
        // seal records for the chunk's later re-staging (`sealed_base`).
        let used = match base.chunks.get(&idx) {
            Some(hash)
                if start < floor && !ws.zeroed.contains(idx) && !ws.written.covers(start, end) =>
            {
                let base_len =
                    if let Some(valid) = ws.staged_base_valid(idx, hash, start, floor, end) {
                        // Already in staging (`WriteState::staged_base`).
                        let valid = (valid as usize).min(len);
                        data.resize(len, 0);
                        ws.staging
                            .read_at(start, &mut data[..valid])
                            .map_err(|e| staging_code(&e))?;
                        ws.staged_base[&idx].len
                    } else {
                        let fetched = self.fetch_chunk(hash)?;
                        data.extend_from_slice(&fetched);
                        data.resize(len, 0);
                        ws.clip_base(data, start, base.file_len);
                        fetched.len() as u64
                    };
                let valid = base_len.min(floor - start).min(len as u64);
                Some(StagedBase {
                    hash: *hash,
                    len: base_len,
                    until: start + valid,
                })
            }
            _ => {
                data.resize(len, 0);
                None
            }
        };
        for (from, to) in ws.written.overlapping(start, end) {
            let rel = (from - start) as usize;
            ws.staging
                .read_at(from, &mut data[rel..rel + (to - from) as usize])
                .map_err(|e| staging_code(&e))?;
        }
        Ok(used)
    }

    /// Get-or-create the pending write state for `ino`, allocating a
    /// fresh staging file (bounded-RAM, plan 07) the first time this
    /// inode is touched since its last flush.
    pub(super) fn write_state<'a>(
        &self,
        writes: &'a mut HashMap<Ino, WriteState>,
        ino: Ino,
        manifest: &Manifest,
    ) -> Result<&'a mut WriteState, Code> {
        if let std::collections::hash_map::Entry::Vacant(e) = writes.entry(ino) {
            // The inode's size is authoritative: a committed
            // `setattr(size)` (a truncate on this or another node whose
            // manifest commit has not happened, or never will) leaves the
            // manifest's `file_len` and chunks as they were. Below it the
            // manifest's content is valid; past it, it is dead.
            let size = self
                .meta
                .getattr(ino)
                .ok()
                .flatten()
                .map(|a| a.size)
                .unwrap_or(manifest.file_len);
            let gen = self.staging_gen.next();
            let mut staging = Staging::create(
                &self.staging_dir,
                ino,
                gen,
                self.chunk_size,
                self.session_staging_budget(),
                self.host.fs.clone(),
            )
            .map_err(|e| staging_code(&e))?;
            staging.set_len_sparse(size).map_err(|e| staging_code(&e))?;
            e.insert(WriteState {
                staging,
                file_len: size,
                base: Some(manifest.clone()),
                sealed: HashMap::new(),
                enrolled: HashSet::new(),
                holes: crate::staging::DirtyRuns::default(),
                zeroed: crate::staging::DirtyRuns::default(),
                seal_buffer: Vec::new(),
                runs: Vec::new(),
                mtime_ns: None,
                ctime_ns: 0,
                written: Written::default(),
                staged_base: HashMap::new(),
                sealed_base: HashMap::new(),
                floor: (size < manifest.file_len).then_some(size),
            });
        }
        Ok(writes.get_mut(&ino).unwrap())
    }

    pub(super) fn drain_inode(&self, ino: Ino) -> Result<(), Code> {
        let Some(handle) = &self.sync else {
            return Ok(());
        };
        constellation_vfs::watch::stage("chunk drain (upload, core reply)");
        let (reply, receive) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(SyncRequest::DrainInode {
                ino,
                fsync: crate::fsync_wait::in_scope(),
                reply,
            })
            .map_err(|_| Code::Io)?;
        // On the `fsync` path (plan 39) the wait also ends on an interrupt
        // or the soft timeout, and the failure is classified for its
        // retry loop (`crate::fsync_wait`).
        match crate::fsync_wait::recv(&self.rt, receive) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(failure)) => {
                if crate::fsync_wait::in_scope() {
                    tracing::debug!(error = %failure, class = failure.class.as_str(), ino, "fsync upload attempt failed");
                } else {
                    tracing::warn!(error = %failure, ino, "write-through upload failed");
                }
                crate::fsync_wait::note_sync_failure(&failure);
                Err(Code::Io)
            }
            Err(()) => Err(Code::Io),
        }
    }

    /// Note that `[offset, end)` was written, and seal the dirty chunks
    /// that made a sequential run move past (eager upload: a file larger
    /// than the cache streams through it). Byte continuity of a run is
    /// used instead of borrowing the read prefetcher's signal: it
    /// directly proves a sequential writer has crossed the boundary. Up
    /// to [`MAX_RUNS`] runs are followed at once (a multi-stream
    /// download writes at several fronts), and only what this write made
    /// a run cross is sealed: a rewrite inside a run (a header patched on
    /// every append) seals nothing. A later write into a sealed chunk
    /// re-admits and re-dirties that chunk.
    ///
    /// A random writer's chunks stay dirty until the flush, or until the
    /// node's staging is half full: then each write also seals up to
    /// [`PRESSURE_SEALS`] of its session's whole dirty chunks — well
    /// before `throttle_delay` slows writes (three quarters) or refuses
    /// them (full), and few at a time, as this runs under the write
    /// shard's lock.
    ///
    /// (A session on an existing file used to count the whole file as
    /// crossed, so each small overwrite re-sealed its whole chunk — read
    /// back, hashed, cached and uploaded: 4 KiB random writes into a
    /// 64 MiB file ran at 19-88 IOPS and left 11 GB of superseded chunks
    /// in the cache.)
    pub(super) fn note_write(
        &self,
        ino: Ino,
        ws: &mut WriteState,
        offset: u64,
        end: u64,
        base: &ImageBase<'_>,
    ) -> Result<(), Code> {
        let chunk = u64::from(self.chunk_size);
        let (start, old_end) = match ws
            .runs
            .iter()
            .position(|(s, e)| (*s..=*e).contains(&offset))
        {
            Some(i) => ws.runs.remove(i),
            None => {
                if ws.runs.len() == MAX_RUNS {
                    ws.runs.remove(0);
                }
                (offset, offset)
            }
        };
        let new_end = old_end.max(end);
        ws.runs.push((start, new_end));
        self.seal_chunks(ino, ws, old_end / chunk..new_end / chunk, base)?;
        if self.staging_budget.active().saturating_mul(2) >= self.staging_budget.budget() {
            let whole = ws.file_len / chunk;
            let victims: Vec<u64> = ws
                .staging
                .dirty_indices()
                .filter(|idx| *idx < whole)
                .take(PRESSURE_SEALS)
                .collect();
            self.seal_chunks(ino, ws, victims, base)?;
        }
        Ok(())
    }

    /// Seal `chunks` (whole ones) that are dirty: their content
    /// ([`Self::chunk_image`] over `base`, the manifest current at the
    /// write) goes into the cache as a pending upload, and their staged
    /// bytes are released.
    fn seal_chunks(
        &self,
        ino: Ino,
        ws: &mut WriteState,
        chunks: impl IntoIterator<Item = u64>,
        base: &ImageBase<'_>,
    ) -> Result<(), Code> {
        let mut sealed_any = false;
        for idx in chunks {
            if !ws.staging.is_dirty(idx) || ws.sealed.contains_key(&idx) {
                continue;
            }
            let mut data = std::mem::take(&mut ws.seal_buffer);
            let used = match self.chunk_image(ws, idx, self.chunk_size as usize, base, &mut data) {
                Ok(used) => used,
                Err(code) => {
                    data.clear();
                    ws.seal_buffer = data;
                    return Err(code);
                }
            };
            let hash = self.store.hash(&data);
            if data.iter().all(|byte| *byte == 0) {
                ws.staging.release_chunk(idx);
                ws.staged_base.remove(&idx);
                ws.holes.mark(idx);
                ws.zeroed.mark(idx);
                data.clear();
                ws.seal_buffer = data;
                sealed_any = true;
                continue;
            }
            let known_durable = self.cache.contains(&hash)
                && !self
                    .meta
                    .upload_pending_for_hash(&hash)
                    .map_err(|error| error.code())?;
            if !known_durable {
                self.cache
                    .insert(&hash, &data, ChunkState::Dirty)
                    .map_err(|_| Code::NoSpace)?;
                self.meta
                    .add_pending_upload(&hash, ino)
                    .map_err(|error| error.code())?;
                ws.enrolled.insert(idx);
            }
            ws.staging.release_chunk(idx);
            ws.staged_base.remove(&idx);
            if let Some(used) = used {
                ws.sealed_base.insert(idx, used);
            }
            ws.sealed.insert(idx, hash);
            data.clear();
            ws.seal_buffer = data;
            sealed_any = true;
        }
        if sealed_any {
            self.nudge_sync();
        }
        Ok(())
    }

    /// Drop sealed chunk `idx` from the write session (it is being
    /// overwritten or punched). Its pending-upload claim stays: the copy
    /// uploads like any other, becomes Clean — durable, and evictable —
    /// and is garbage GC collects. Withdrawing the claim instead left a
    /// Dirty cache entry nothing would ever upload or evict: every chunk
    /// sealed and then written again leaked its cache space for good (a
    /// database's cache filled with them, and writes failed). Marking it
    /// Clean is no way out either: Clean means "in S3" to every check
    /// that skips an upload, so identical content written later went
    /// unuploaded, and its cache copy could be evicted — lost.
    pub(super) fn unseal(
        &self,
        ws: &mut WriteState,
        _ino: Ino,
        idx: u64,
    ) -> Result<Option<ChunkHash>, Code> {
        ws.enrolled.remove(&idx);
        ws.sealed_base.remove(&idx);
        Ok(ws.sealed.remove(&idx))
    }

    /// Insert content as Dirty unless the local durable-set rung proves
    /// S3 already has it. Returns whether this inode must enrol a
    /// pending row.
    pub(super) fn cache_for_upload(&self, hash: &ChunkHash, data: &[u8]) -> Result<bool, Code> {
        let known_durable = self
            .cache
            .state_of(hash)
            .is_some_and(|state| matches!(state, ChunkState::Clean | ChunkState::Pinned))
            && !self
                .meta
                .upload_pending_for_hash(hash)
                .map_err(|error| error.code())?;
        if known_durable {
            return Ok(false);
        }
        self.cache
            .insert(hash, data, ChunkState::Dirty)
            .map_err(|_| Code::NoSpace)?;
        Ok(true)
    }

    /// Flush an inode's pending writes: hash + upload chunks, write the
    /// new manifest. No-op when there is no pending state.
    ///
    /// The manifest commit is a namespace mutation, so it needs the
    /// lease — but the chunk uploads do not (content is immutable and
    /// content-addressed), which is why the gate sits just before the
    /// commit rather than at the top.
    ///
    /// Eager chunk upload is best-effort. The dirty cache entry is
    /// reserve-accounted and the sync task uploads all dirty chunks
    /// before publishing this manifest, so a transient reset on a
    /// healed S3 connection does not become application-visible EIO in
    /// local-fsync mode.
    ///
    /// [`Self::publish_inode`], then the write-through drain it leaves.
    pub(super) fn flush_inode(&self, ino: Ino, force_through: bool) -> Result<(), Code> {
        if self.publish_inode(ino, force_through)? {
            self.drain_inode(ino)?;
        }
        Ok(())
    }

    /// The publication half of [`Self::flush_inode`]: the inode's pending
    /// writes composed and their manifest committed at the sequencer.
    /// `Ok(true)`: the close acts as `through` and the caller still owes
    /// the inode's drain ([`Self::drain_inode`]: its chunks into S3)
    /// before it answers.
    ///
    /// The inode's operation lock is held from taking the session until
    /// the commit is answered, and no longer: that is what orders the
    /// file's publications (one commit at a time, each over the one
    /// before) and an mtime set against them. The drain only waits for
    /// what is already published to reach S3 — a `back` close leaves that
    /// to the background passes, and nothing waits for it — and every
    /// path that needs it (`fsync`, a lock release's flush, the next
    /// `through` close) drains the inode's pending chunks itself. Held
    /// across the drain, the lock made every read, write and close of the
    /// file wait one S3 round trip per close (xcheckfs
    /// `lock_traffic_concurrent_with_io`: four threads opening, writing
    /// and closing one file ran one close's upload at a time).
    pub(super) fn publish_inode(&self, ino: Ino, force_through: bool) -> Result<bool, Code> {
        // The inode's operation lock, not its shard lock, is what keeps
        // per-inode request order across the publication: the session is
        // detached from its shard (its size stays visible to `getattr`
        // and `lookup`) so the compose and the commit — a forward — run
        // with no shard held (EC2 finding 1).
        let _op = self.inode_ops.lock(ino);
        let Some(ws) = self.writes.detach(ino) else {
            return Ok(false);
        };
        // Nothing to publish: no manifest commit, which would stamp both
        // times with its own (whatever the write that failed staged is
        // gone with it, as its error said).
        if ws.changed_nothing() {
            ws.staging.discard();
            self.retire_writes(ino, Retired::NothingChanged);
            return Ok(false);
        }
        // Written, then unlinked while still open (here or on another
        // node): there is no name to publish the content under, and the
        // sequencer would refuse the manifest (`NotFound`). POSIX has the
        // descriptor's `write`/`fsync`/`close` succeed, and its reads see
        // the data: keep the session attached — this node's reads overlay
        // it — until the last close drops it with the inode
        // (`View::release`, [`Self::drop_writes`]).
        //
        // Only for an orphan this replica holds. An inode it merely lacks
        // is not known to be unlinked — speculation that installed it may
        // be rolled back and redone, the entry not in the log yet — so the
        // sequencer decides, below: taken for unlinked, the close of a
        // file another node had just created answered success, published
        // nothing, and the last close dropped the bytes (harness
        // `concurrent-create-no-excl`).
        if self.orphaned(ino) {
            self.writes.reattach(ino, ws);
            return Ok(false);
        }
        constellation_vfs::watch::stage("flush: compose and commit");
        crate::locks::take_lapsed();
        REFUSED_COMMIT.with(|r| r.borrow_mut().take());
        // Plan 42 K1's fault point: a session dropped without becoming
        // the row (F13).
        let flushed = match crate::fault::commit_fault(|| self.path_below_view_root(ino)) {
            Some(crate::fault::CommitFault::Fail) => Err(FlushFail {
                errno: Code::Io,
                ws: None,
            }),
            Some(crate::fault::CommitFault::Lapse) => {
                crate::locks::note_lapsed();
                Err(FlushFail {
                    errno: Code::Io,
                    ws: Some(Box::new(ws)),
                })
            }
            None => self.flush_detached(ino, ws, force_through),
        };
        match flushed {
            Ok(drain) => {
                self.retire_writes(ino, Retired::Committed);
                Ok(drain)
            }
            Err(FlushFail {
                errno,
                ws: Some(ws),
            }) if crate::locks::take_lapsed() => {
                // Plan 30 §M14 phase 2: the sequencer refused the commit —
                // the lock grant these writes were made under had ended
                // (the fencing token; a grant this node still honoured
                // was re-sent, `mutate_op_rebasable`). They are never
                // published into the file, which the next lock holder may
                // have written since: they become a conflict copy beside
                // it (`.constellation-conflict/`, the replay drain's), and
                // every open description reports `EIO` once.
                let refused = REFUSED_COMMIT.with(|r| r.borrow_mut().take());
                let mut ws = *ws;
                let kept = match refused {
                    Some(c) if c.ino == ino => self.keep_refused_commit(c),
                    _ => false,
                };
                if !kept {
                    let enrolled: Vec<u64> = ws.enrolled.iter().copied().collect();
                    for idx in enrolled {
                        if let Err(error) = self.unseal(&mut ws, ino, idx) {
                            tracing::warn!(ino, idx, %error, "withdrawing a discarded chunk's upload claim failed");
                        }
                    }
                }
                ws.staging.discard();
                self.retire_writes(ino, Retired::Uncommitted);
                if kept {
                    tracing::warn!(
                        target: "constellation::locks",
                        ino,
                        "writes whose commit was refused (the lock grant they were made under has ended) \
                         are kept as a conflict copy, not published into the file (EIO)"
                    );
                } else {
                    tracing::error!(
                        target: "constellation::locks",
                        ino,
                        "discarded writes whose commit was refused: the lock grant they were made under \
                         has ended, and no conflict copy could be queued (EIO)"
                    );
                }
                if let Some(l) = self.cluster_locks() {
                    l.note_discard(ino);
                }
                Err(errno)
            }
            Err(FlushFail {
                errno,
                ws: Some(ws),
            }) => {
                self.writes.reattach(ino, *ws);
                if errno == Code::NotFound {
                    // Unlinked by another node between the check above
                    // and the sequencer's answer: as above, once this
                    // replica has applied the unlink — the refusal raised
                    // the session watermark to the state it was answered
                    // from, and a replayed unlink of an inode the replica
                    // holds leaves the orphan record.
                    self.session_wait(&[ReadKey::Ino(ino)]);
                    if self.orphaned(ino) {
                        return Ok(false);
                    }
                    // An inode this replica never held, or held only
                    // through speculation that was rolled back (a
                    // takeover stranded the `Exists` hint's create): the
                    // sequencer has no such file, so nothing was written.
                    // Never a success — the session stays attached for a
                    // later close to retry, and the last close drops it
                    // only after answering this error (`View::release`).
                    return Err(Code::Stale);
                }
                Err(errno)
            }
            Err(FlushFail { errno, ws: None }) => {
                self.retire_writes(ino, Retired::Uncommitted);
                Err(errno)
            }
        }
    }

    /// Whether `ino` (a shared-namespace inode, not a scratch one) is an
    /// orphan record on this replica: unlinked (`nlink == 0`) and kept
    /// for its open descriptors. Unlike [`Self::unlinked`], an inode the
    /// replica lacks altogether is not one.
    pub(super) fn orphaned(&self, ino: Ino) -> bool {
        if self.meta.scratch_getattr(ino).ok().flatten().is_some() {
            return false;
        }
        matches!(self.meta.getattr(ino), Ok(Some(attr)) if attr.nlink == 0)
    }

    /// Whether `ino` (a shared-namespace inode, not a scratch one) has no
    /// name left on this replica: an orphan record (`nlink == 0`, kept
    /// for its open descriptors), or gone from the replica altogether.
    pub(super) fn unlinked(&self, ino: Ino) -> bool {
        if self.meta.scratch_getattr(ino).ok().flatten().is_some() {
            return false;
        }
        match self.meta.getattr(ino) {
            Ok(Some(attr)) => attr.nlink == 0,
            Ok(None) => true,
            Err(_) => false,
        }
    }

    /// Drop `ino`'s unpublished write session: its staged bytes, and the
    /// pending-upload claims of the chunks it sealed. `true` if there was
    /// one. For content that must never be published (a lapsed lock
    /// grant's, an unlinked file's at its last close).
    pub(super) fn drop_writes(&self, ino: Ino) -> bool {
        // After any flush of the file in flight (it holds the session
        // detached from its shard).
        let _op = self.inode_ops.lock(ino);
        let ws = self.writes.lock(ino).remove(&ino);
        let Some(mut ws) = ws else {
            return false;
        };
        // Plan 42 §5.1.5: the kernel's pages hold what this session wrote,
        // which the view will never answer now.
        if !ws.changed_nothing() {
            self.inflight.invalidate_uncommitted(ino);
        }
        let enrolled: Vec<u64> = ws.enrolled.iter().copied().collect();
        for idx in enrolled {
            if let Err(error) = self.unseal(&mut ws, ino, idx) {
                tracing::warn!(ino, idx, %error, "withdrawing a dropped chunk's upload claim failed");
            }
        }
        ws.staging.discard();
        true
    }

    /// [`Self::flush_inode`]'s body, on a detached session: `Ok(true)`
    /// when the published chunks still have to be drained to S3 before
    /// the close returns (write-through).
    pub(super) fn flush_detached(
        &self,
        ino: Ino,
        ws: WriteState,
        force_through: bool,
    ) -> Result<bool, FlushFail> {
        let dropped = |errno| FlushFail { errno, ws: None };
        let epoch_active = self.sync.as_ref().is_some_and(|h| {
            h.epoch_active
                .as_ref()
                .is_some_and(|active| active.load(std::sync::atomic::Ordering::Relaxed))
        });
        let base = match &ws.base {
            Some(m) => m.clone(),
            None => self.load_manifest(ino).map_err(dropped)?,
        };
        let (manifest_bytes, dirty_hashes) = self
            .compose_manifest(&ws, &base, ws.file_len)
            .map_err(dropped)?;
        if self.meta.scratch_getattr(ino).ok().flatten().is_some() {
            for hash in &dirty_hashes {
                self.meta
                    .add_pending_upload(hash, ino)
                    .map_err(|e| dropped(e.code()))?;
            }
            self.meta
                .scratch_set_manifest(
                    ino,
                    &manifest_bytes,
                    ws.file_len,
                    ws.mtime_ns,
                    ws.commit_ctime(),
                )
                .map_err(|e| dropped(e.code()))?;
            ws.staging.discard();
            return Ok(force_through);
        }
        self.finish_flush(
            ino,
            ws,
            base,
            manifest_bytes,
            dirty_hashes,
            force_through,
            epoch_active,
        )
    }

    /// Compose the whole-file manifest this flush publishes: `base`'s
    /// chunks, minus holes and anything past `file_len`, with this
    /// flush's sealed and staged chunks laid over the top. Also returns
    /// the chunks newly sealed into the cache as dirty, which the
    /// manifest commit enrols as pending uploads.
    ///
    /// Separate from [`Self::flush_inode`] so that a commit the holder
    /// rejects for a stale base can be recomposed against the manifest
    /// that *is* current, without a second copy of this logic.
    pub(super) fn compose_manifest(
        &self,
        ws: &WriteState,
        base: &Manifest,
        file_len: u64,
    ) -> Result<(Vec<u8>, Vec<ChunkHash>), Code> {
        let cs = u64::from(self.chunk_size);
        // A manifest's content is valid only below its `file_len`
        // (`replay::clip_manifest`). A clipped *spilled* manifest keeps
        // its chunk list (the clip cannot rewrite the spill blob), so
        // the entries at or past `file_len` are dead: dropped here, or
        // the re-cut below measured one against a base it lies outside
        // of (`chunk_len`'s debug assertion; the truncate/fallocate fuzz
        // seed 34, a truncate to 32 of a ten-chunk file).
        let mut old_hashes = self.chunk_list(base)?;
        old_hashes.retain(|index, _| *index * cs < base.file_len);
        let layout = constellation_fs_core::ChunkLayout::new(self.chunk_size);
        let n_chunks = layout.chunk_count(file_len);
        let floor = ws.base_floor(base.file_len);
        let mut new_hashes: SparseChunks = old_hashes
            .iter()
            .filter(|(index, _)| **index < n_chunks && !ws.holes.contains(**index))
            // Wholly past a truncation: dead, a hole now.
            .filter(|(index, _)| **index * cs < floor)
            .map(|(index, hash)| (*index, *hash))
            .collect();
        // Every chunk this flush seals into the cache as Dirty: the
        // durable pending-upload set this manifest commit journals
        // (plan 07 step 1). Each chunk is sealed with only its own
        // image in memory (read its written ranges, hash, insert, drop
        // the buffer), so peak RSS stays O(chunk_size) per compose
        // worker — never O(file_len), 07's whole point — and the workers
        // are bounded node-wide ([`compose_each`]).
        let mut dirty_hashes: Vec<ChunkHash> = Vec::new();
        for (&idx, hash) in &ws.sealed {
            if idx >= n_chunks {
                continue;
            }
            if self
                .meta
                .upload_pending_for_hash(hash)
                .map_err(|error| error.code())?
            {
                dirty_hashes.push(*hash);
            }
            new_hashes.insert(idx, *hash);
        }
        // A dirty chunk starts from `base`'s committed content — not
        // what was current when the session first wrote it — with just
        // the byte ranges this flush's own `write()` calls delivered
        // replayed on top (`chunk_image`). Otherwise a chunk two nodes
        // disjointly patch converges on whichever writer commits last,
        // each writer's copy of the base silently reproducing the
        // *other* writer's bytes as they were before its patch landed
        // (124cc95).
        //
        // The chunks are independent — content-addressed, each its own
        // cache file — so they are composed in parallel (finding 09):
        // with frequent `fsync`s a random writer dirties most of a
        // file's chunks per flush, and one at a time the flush waited
        // for each chunk's hash and its cache insert's `fdatasync` in
        // turn. Each insert is still durable before this returns, so
        // the order the commit relies on — every chunk in the cache
        // (durable) before the manifest that names it is journaled and
        // its pending rows inserted (plan 07) — is unchanged, and so is
        // the result: the outcomes are applied in chunk order, the
        // manifest and the pending list are the sequential compose's,
        // byte for byte. A failure fails the compose as before (the
        // workers start no further chunk); chunks other workers had
        // already cached stay Dirty entries no row names, as those
        // before a failing chunk always did.
        let jobs: Vec<u64> = ws
            .staging
            .dirty_indices()
            .filter(|idx| *idx < n_chunks)
            .collect();
        let image_base = ImageBase {
            chunks: &old_hashes,
            file_len: base.file_len,
        };
        let outcomes = compose_each(&jobs, self.chunk_size, |idx| {
            let mut data = Vec::new();
            let len = layout.chunk_len(file_len, idx) as usize;
            self.chunk_image(ws, idx, len, &image_base, &mut data)?;
            if data.iter().all(|byte| *byte == 0) {
                return Ok(None);
            }
            let hash = self.store.hash(&data);
            let dirty = self.cache_for_upload(&hash, &data)?;
            Ok(Some((hash, dirty)))
        })?;
        for (idx, outcome) in jobs.into_iter().zip(outcomes) {
            match outcome {
                None => {
                    new_hashes.remove(&idx);
                }
                Some((hash, dirty)) => {
                    if dirty {
                        dirty_hashes.push(hash);
                    }
                    new_hashes.insert(idx, hash);
                }
            }
        }
        // An untouched old chunk needs re-cutting when its length changed
        // (the tail after a truncate-down) or a truncation point falls
        // inside it (its bytes past the point are dead: zeroed, even when
        // the file was extended past them again).
        let straddle = (floor < file_len && !floor.is_multiple_of(cs) && floor / cs < n_chunks)
            .then_some(floor / cs);
        let recut: Vec<u64> = n_chunks
            .checked_sub(1)
            .into_iter()
            .chain(straddle)
            .collect::<std::collections::BTreeSet<u64>>()
            .into_iter()
            .collect();
        for idx in recut {
            if !ws.staging.is_dirty(idx) && !ws.sealed.contains_key(&idx) && !ws.holes.contains(idx)
            {
                if let Some(h) = old_hashes.get(&idx) {
                    let expect_len = layout.chunk_len(file_len, idx) as usize;
                    let old_len = base.layout.chunk_len(base.file_len.max(1), idx) as usize;
                    if old_len != expect_len || Some(idx) == straddle {
                        let mut data = Vec::from(self.fetch_chunk(h)?);
                        data.resize(expect_len, 0);
                        ws.clip_base(&mut data, idx * cs, base.file_len);
                        if data.iter().all(|byte| *byte == 0) {
                            new_hashes.remove(&idx);
                        } else {
                            let hash = self.store.hash(&data);
                            if self.cache_for_upload(&hash, &data)? {
                                dirty_hashes.push(hash);
                            }
                            new_hashes.insert(idx, hash);
                        }
                    }
                }
            }
        }
        let (manifest, spill) = Manifest::from_sparse_chunks(
            self.chunk_size,
            file_len,
            new_hashes,
            INLINE_CHUNKS_MAX,
            // Must match the identity the blob is stored under below, which
            // on an E2E mount is the keyed addressing hash, not a plain one.
            |blob| self.store.hash(blob),
        );
        if let Some(blob) = spill {
            let bh = self.store.hash(&blob);
            if self.cache_for_upload(&bh, &blob)? {
                dirty_hashes.push(bh);
            }
        }
        Ok((manifest.encode(), dirty_hashes))
    }

    /// Publish a composed manifest and retire the write state.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn finish_flush(
        &self,
        ino: Ino,
        ws: WriteState,
        base: Manifest,
        manifest_bytes: Vec<u8>,
        dirty_hashes: Vec<ChunkHash>,
        force_through: bool,
        epoch_active: bool,
    ) -> Result<bool, FlushFail> {
        // Plan 30 §M3b: a local commit is admitted through the lease view
        // (counted in flight until the commit returns) so a release's final
        // flush cannot miss it — see `lease.rs`'s module doc, "The
        // releasing flag". The guard is dropped right after the commit:
        // the chunk drain below can take long and must not hold a release.
        let view = self.sync.as_ref().map(|handle| handle.lease.clone());
        let mut admitted = view.as_ref().and_then(|view| view.admit());
        // Plan 30 §M12: the sequencer's own commit runs here only for a
        // file the root owns; one a range's (or a subtree's) delegate owns
        // goes through the core, like the file's create did — before,
        // the root committed the manifest of a file it had created
        // through a range delegate against a replica that lacked the
        // inode, the close's error was swallowed and the content never
        // published (harness `shared-dir-multi-writer`, the root's own
        // files in the split phase read back empty for the whole run).
        // The admission holds the delegation gate shared across the
        // commit (`Meta::root_fast_path`).
        let owned = admitted.as_ref().and_then(|_| {
            self.meta
                .root_fast_path(&constellation_meta::MutateOp::SetManifest {
                    ino,
                    base_manifest: None,
                    manifest: Vec::new(),
                    size: 0,
                    mtime_ns: None,
                    ctime_ns: None,
                })
        });
        if admitted.is_some() && owned.is_none() {
            admitted = None;
            if let Some(h) = &self.sync {
                h.delegates.note_routed();
            }
        }
        let holds_lease = self.sync.is_none() || admitted.is_some();
        if holds_lease {
            // Plan 30 §M9: under a durability gate the commit is
            // acknowledged (the close returns) only once its row is
            // durable, as `execute_mutate`'s fast path does; an in-doubt
            // outcome re-commits through the core, so keep the inputs.
            //
            // Plan 43 C1-2: whether to wait is the committed row's own class
            // (as `execute_mutate`'s fast path); `gated` only says whether
            // the inputs are kept for the in-doubt re-commit. A gate that
            // came on between the two leaves a gated row without them: it
            // is waited for all the same, and an in-doubt outcome there
            // fails the close (the staged state is kept; the caller retries)
            // rather than acknowledging a row nothing holds.
            let gated = admitted.is_some()
                && self.sync.as_ref().is_some_and(|h| {
                    h.lease.ack_gated() || self.meta.session().published_class().gated()
                });
            let retry = gated.then(|| (base.clone(), manifest_bytes.clone(), dirty_hashes.clone()));
            constellation_meta::clear_last_journaled();
            let committed =
                self.commit_manifest_local(ino, &ws, base, manifest_bytes, dirty_hashes);
            let target = match (&committed, &self.sync) {
                (Ok(()), Some(h)) => self.fast_ack_target(h),
                _ => None,
            };
            drop(owned);
            drop(admitted);
            if let Err(errno) = committed {
                return Err(FlushFail {
                    errno,
                    ws: Some(Box::new(ws)),
                });
            }
            if let (Some(h), Some(target)) = (&self.sync, target) {
                let acked = match self.local_ack_durable(h, target) {
                    Ok(acked) => acked,
                    // Plan 43 C1-2 (D4): the gate went off and the row
                    // could not be re-tagged `Lone`: not acknowledged (the
                    // staged state is kept; the caller retries).
                    Err(_) => {
                        return Err(FlushFail {
                            errno: Code::Io,
                            ws: Some(Box::new(ws)),
                        });
                    }
                };
                if !acked {
                    let Some((base, bytes, dirty)) = retry else {
                        return Err(FlushFail {
                            errno: Code::Io,
                            ws: Some(Box::new(ws)),
                        });
                    };
                    // In doubt (the lease was lost, or the backups never
                    // answered): the row may be rolled back. Commit
                    // again through the core, which resolves it — the
                    // base check refuses a duplicate and the rebase then
                    // lays this flush over whatever survived.
                    if let Err(errno) =
                        self.commit_manifest_forwarded(ino, &ws, base, bytes, dirty, false)
                    {
                        return Err(FlushFail {
                            errno,
                            ws: Some(Box::new(ws)),
                        });
                    }
                }
            }
            // Plan 30 §M8: the sequencer's own manifest commit bypasses
            // `execute_mutate`; it recalls read delegations on the file
            // before the close returns, like every other local write.
            if let Some(h) = &self.sync {
                self.recall_after_local_inos(
                    h,
                    vec![ino],
                    constellation_platform::clock::protocol().wait(),
                );
            }
        } else if let Err(errno) = self.commit_manifest_forwarded(
            ino,
            &ws,
            base,
            manifest_bytes,
            dirty_hashes,
            self.defers_upload(ino, force_through, epoch_active),
        ) {
            return Err(FlushFail {
                errno,
                ws: Some(Box::new(ws)),
            });
        }
        // The staged bytes now live in the durable chunk cache (and are
        // enrolled in `pending_upload`); the staging file is scratch
        // and safe to drop.
        ws.staging.discard();
        let through = self.sync.as_ref().is_none_or(|handle| {
            handle
                .write_mode
                .effective(force_through, false, handle.fsync_s3)
                == crate::writeback::WriteMode::Through
        });
        Ok(through && !epoch_active)
    }

    /// Commit a whole-file manifest as the lease holder, rebasing if our
    /// base turns out to be stale.
    ///
    /// A flush composes its image while it may not hold the lease; by
    /// the time an acquisition lets this commit through, a segment
    /// tailed from another node — or another flush forwarded through us
    /// while we *did* hold it — can have moved the manifest on. Without
    /// a check here, that lease thrash reproduces the disjoint-write
    /// lost update purely locally, with no forwarding involved: this
    /// node would overwrite whatever the tailed record wrote for the
    /// same inode with an image that never saw it.
    pub(super) fn commit_manifest_local(
        &self,
        ino: Ino,
        ws: &WriteState,
        base: Manifest,
        manifest_bytes: Vec<u8>,
        dirty_hashes: Vec<ChunkHash>,
    ) -> Result<(), Code> {
        self.commit_manifest_with_rebase(
            ino,
            ws,
            base,
            manifest_bytes,
            dirty_hashes,
            |base, manifest_bytes, file_len, dirty_hashes| {
                // Plan 30 §M14 phase 2: the holder's own commit carries the
                // fencing token too (checked here, recorded for a replay).
                let tag = crate::locks::current_tag().map_err(|code| match code {
                    Code::Intr => MutateFail::Errno(Code::Intr),
                    _ => MutateFail::LockLapsed,
                })?;
                self.meta
                    .with_lock_tag(&tag, crate::locks::now(), || {
                        self.meta.set_manifest_dirty(
                            ino,
                            Some(&base.encode()),
                            manifest_bytes,
                            file_len,
                            ws.mtime_ns,
                            ws.commit_ctime(),
                            dirty_hashes,
                        )
                    })
                    .map_err(|e| {
                        if matches!(e, MetaError::LockLapsed) {
                            self.meta.locks().note_owner_fenced_op();
                        }
                        mutate_fail(e)
                    })
            },
        )
    }

    /// Whether a non-owner's close of `ino` forwards its manifest before
    /// its chunks are up (`--write-mode back`; see
    /// [`Self::commit_manifest_forwarded`]): not when the close must act
    /// as `through` (`fsync`, `O_SYNC`, `--fsync-mode s3`, a lock's
    /// flush), not in a continuation epoch (which never drains at close
    /// anyway), and only for a file the root sequences — a delegate's
    /// stream and backup feed hold such a manifest back until its chunks
    /// are up (`Meta::delegate_txs_from`), which under a delegate's
    /// backup would make the close wait for them anyway.
    pub(super) fn defers_upload(&self, ino: Ino, force_through: bool, epoch_active: bool) -> bool {
        let Some(h) = &self.sync else {
            return false;
        };
        !epoch_active
            && h.write_mode.effective(force_through, false, h.fsync_s3)
                == crate::writeback::WriteMode::Back
            && self
                .meta
                .root_fast_path(&constellation_meta::MutateOp::SetManifest {
                    ino,
                    base_manifest: None,
                    manifest: Vec::new(),
                    size: 0,
                    mtime_ns: None,
                    ctime_ns: None,
                })
                .is_some()
    }

    /// Forward a whole-file manifest commit to the lease holder,
    /// rebasing if our base turns out to be stale.
    ///
    /// The holder rejects an image composed on a superseded manifest
    /// instead of installing it, because a whole-file image built on an
    /// old base drops whatever chunks landed in between — that is how
    /// concurrent disjoint `WriteAt`s from several nodes used to lose
    /// every patch but the last. On rejection, lay this flush's own
    /// chunks over the manifest that is current and try again.
    ///
    /// `defer_upload` (a `--write-mode back` close, see
    /// [`Self::defers_upload`]): forward at once instead of uploading
    /// first. The chunks stay enrolled here (durable on local disk, as a
    /// sequencer's own `back` close leaves them); the forward names the
    /// ones still pending (`crate::forwarded_pending_chunks`, attached by
    /// the sync task as it sends), and the sequencer awaits them before
    /// anything naming them leaves it — its ship, its pre-S3 stream —
    /// until this node reports them up (`meta::store::remote`). So the
    /// log still never names a chunk S3 lacks, and the close costs one
    /// forward instead of an S3 round trip and a forward. Everything else
    /// is the forward's as before: the base check and rebase, the
    /// exactly-once rid, the shadow that gives this node read-your-writes
    /// (the bytes are in its cache), stranding and replay by rid (the
    /// queued op is re-sent with its pending list).
    pub(super) fn commit_manifest_forwarded(
        &self,
        ino: Ino,
        ws: &WriteState,
        base: Manifest,
        manifest_bytes: Vec<u8>,
        dirty_hashes: Vec<ChunkHash>,
        defer_upload: bool,
    ) -> Result<(), Code> {
        self.commit_manifest_with_rebase(
            ino,
            ws,
            base,
            manifest_bytes,
            dirty_hashes,
            |base, manifest_bytes, file_len, dirty_hashes| {
                // Content-addressed chunks can be uploaded before
                // authority is obtained. Enrol them without journaling
                // the manifest, then drain, so the manifest this pass
                // forwards never names a hash S3 does not have yet —
                // including a hash a rebase just introduced by merging
                // this flush's bytes onto a fresher base, which is not
                // the same chunk the very first attempt (if any)
                // already drained.
                for hash in dirty_hashes {
                    // Sealed into the cache and no longer pending: a
                    // sync round's drain already uploaded it (the round
                    // races this flush); enrolling it again would upload
                    // it twice (plan 30 §M11's `delegated-subtrees`
                    // measured 1.7 chunk PUTs per file from the rounds a
                    // delegate's writes trigger).
                    let known_durable = matches!(
                        self.cache.state_of(hash),
                        Some(ChunkState::Clean) | Some(ChunkState::Pinned)
                    ) && !self
                        .meta
                        .upload_pending_for_hash(hash)
                        .map_err(mutate_fail)?;
                    if known_durable {
                        continue;
                    }
                    self.meta
                        .add_pending_upload(hash, ino)
                        .map_err(mutate_fail)?;
                }
                // Plan 30 §M10: in an active continuation epoch S3 is away,
                // and the epoch's hold owner journals the manifest locally
                // like its own writes; the chunks stay enrolled here and
                // go up with this node's first round once S3 returns (a
                // reader before then fetches them from this node over
                // P2P). The epoch's writes are as durable as their nodes,
                // as the holder's own are. (A handoff of the hold would
                // carry the chunks' manifest to this node instead, but a
                // hold with an unshipped journal is no longer handed over.)
                let epoch_active = self.sync.as_ref().is_some_and(|h| {
                    h.epoch_active
                        .as_ref()
                        .is_some_and(|a| a.load(std::sync::atomic::Ordering::Relaxed))
                });
                if !epoch_active && !defer_upload {
                    self.drain_inode(ino).map_err(MutateFail::Errno)?;
                }
                let result = self.mutate_op_rebasable(
                    ino,
                    constellation_meta::MutateOp::SetManifest {
                        ino,
                        base_manifest: Some(base.encode()),
                        manifest: manifest_bytes.to_vec(),
                        size: file_len,
                        mtime_ns: ws.mtime_ns,
                        ctime_ns: ws.commit_ctime(),
                    },
                );
                if defer_upload {
                    // The chunks go up in the next round, which then
                    // reports them to the sequencer.
                    if let Some(h) = &self.sync {
                        let _ = h.tx.send(SyncRequest::Nudge);
                    }
                }
                result
            },
        )
    }

    /// Shared rebase-and-retry loop for a whole-file manifest commit.
    /// `attempt_commit` tries to install `manifest_bytes` (composed on
    /// `base` for length `file_len`, with `dirty_hashes` freshly sealed
    /// by this flush) and reports [`MutateFail::Conflict`] when that
    /// base has been superseded. On conflict this recomposes from the
    /// manifest that *is* current — the holder's reply, or this node's
    /// own replica when `manifest` comes back `None` because this node
    /// executed the mutation itself — and tries again, up to
    /// `MANIFEST_COMMIT_ATTEMPTS` times.
    pub(super) fn commit_manifest_with_rebase(
        &self,
        ino: Ino,
        ws: &WriteState,
        mut base: Manifest,
        mut manifest_bytes: Vec<u8>,
        mut dirty_hashes: Vec<ChunkHash>,
        mut attempt_commit: impl FnMut(&Manifest, &[u8], u64, &[ChunkHash]) -> Result<(), MutateFail>,
    ) -> Result<(), Code> {
        // A flush that shortened the file relative to its own base is a
        // truncate, and must not be silently re-extended by a peer's
        // length. Any other flush adopts the longer of the two so a
        // concurrent extension survives the rebase.
        let truncates = ws.base.as_ref().is_some_and(|b| ws.file_len < b.file_len);
        let mut file_len = ws.file_len;
        for attempt in 1..=MANIFEST_COMMIT_ATTEMPTS {
            let current = match attempt_commit(&base, &manifest_bytes, file_len, &dirty_hashes) {
                Ok(()) => return Ok(()),
                Err(MutateFail::Errno(e)) => return Err(e),
                Err(MutateFail::LockLapsed) => {
                    // Plan 30 §M14 phase 2: written under a lock grant
                    // that ended before the commit landed — `flush_inode`
                    // keeps it as a conflict copy rather than ever
                    // publishing it into the file.
                    crate::locks::note_lapsed();
                    REFUSED_COMMIT.with(|r| {
                        *r.borrow_mut() = Some(RefusedCommit {
                            ino,
                            manifest: manifest_bytes.clone(),
                            size: file_len,
                            dirty: dirty_hashes.clone(),
                        })
                    });
                    return Err(Code::Io);
                }
                Err(MutateFail::Conflict { manifest }) => manifest,
            };
            // `None` means whichever node executed the mutation was us,
            // so our own replica already holds the authoritative image.
            base = match current {
                Some(bytes) => Manifest::decode(&bytes).map_err(|_| Code::Io)?,
                None => self.load_manifest(ino)?,
            };
            file_len = if truncates {
                ws.file_len
            } else {
                ws.file_len.max(base.file_len)
            };
            // The chunks are already cached and enrolled for upload, so
            // recomposing only rebuilds the manifest; a rebased dirty
            // list that comes back shorter just means some are already
            // durable (already-uploaded content is not re-enrolled).
            let (rebased, rebased_dirty) = self.compose_manifest(ws, &base, file_len)?;
            manifest_bytes = rebased;
            dirty_hashes = rebased_dirty;
            tracing::debug!(
                ino,
                attempt,
                file_len,
                "manifest commit rebased onto a concurrent update"
            );
        }
        tracing::warn!(
            ino,
            attempts = MANIFEST_COMMIT_ATTEMPTS,
            "manifest commit kept losing its base; giving up"
        );
        Err(Code::Again)
    }
}

/// Plan 30 §M14 phase 2: the manifest commit the sequencer refused for a
/// lock grant that had ended, as `commit_manifest_with_rebase` last tried
/// it (this thread's; `flush_inode` takes it).
struct RefusedCommit {
    ino: Ino,
    manifest: Vec<u8>,
    size: u64,
    dirty: Vec<ChunkHash>,
}

thread_local! {
    static REFUSED_COMMIT: std::cell::RefCell<Option<RefusedCommit>> =
        const { std::cell::RefCell::new(None) };
}

impl View {
    /// Keep a refused commit's content as a conflict copy: its chunks stay
    /// enrolled for upload (the copy's manifest names them), and the
    /// commit is queued as a replay already refused, which the replay
    /// drain materializes under the view's root (`recovery::materialize_remote`)
    /// and never executes. `false`: nothing could be queued.
    fn keep_refused_commit(&self, c: RefusedCommit) -> bool {
        let Some(h) = &self.sync else {
            return false;
        };
        for hash in &c.dirty {
            let durable = matches!(
                self.cache.state_of(hash),
                Some(ChunkState::Clean) | Some(ChunkState::Pinned)
            ) && !self.meta.upload_pending_for_hash(hash).unwrap_or(true);
            if !durable {
                if let Err(error) = self.meta.add_pending_upload(hash, c.ino) {
                    tracing::warn!(ino = c.ino, %error, "enrolling a refused commit's chunk failed");
                    return false;
                }
            }
        }
        let rid = constellation_meta::Rid {
            node: h.node_id,
            incarnation: h.incarnation,
            seq: h
                .next_rid_seq
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        };
        // The copy goes under the conflict directory of this view's root
        // (the filesystem root, or a subtree mount's root: the copy stays
        // where the writer can see it and confined to it), named after
        // the file's path below that root, not beside the file: a lock
        // guards files whose directories mean something to their
        // application (beside `.git/refs/heads/master.lock` it is a ref
        // with a bad name, and `git fsck` fails). It is the file's owner's
        // with the owner's permission bits only (`recovery::copy_op`):
        // the file's ancestors no longer guard it. Only the copy is made
        // of this op (it is queued refused, never executed).
        let (uid, gid, mode) = self
            .meta
            .getattr(c.ino)
            .ok()
            .flatten()
            .map(|a| (a.uid, a.gid, a.mode & 0o700))
            .unwrap_or((0, 0, 0o600));
        let op = constellation_meta::MutateOp::Publish {
            ino: c.ino,
            parent: self.view_root,
            name: conflict_name_for_path(&self.path_below_view_root(c.ino)),
            mode,
            uid,
            gid,
            mtime_ns: 0,
            manifest: c.manifest,
            size: c.size,
            xattrs: Vec::new(),
            noreplace: false,
        };
        let refusal = constellation_meta::Refusal {
            reason: "the lock grant it was written under had ended (fencing token)".into(),
            ts_unix: (constellation_fs_core::types::now_ns() / 1_000_000_000) as i64,
        };
        match self.meta.queue_refused_replay(rid, &op, refusal) {
            Ok(()) => {
                let _ = h.tx.send(SyncRequest::Nudge);
                true
            }
            Err(error) => {
                tracing::warn!(ino = c.ino, %error, "queueing a refused commit's conflict copy failed");
                false
            }
        }
    }
}

impl View {
    /// `ino`'s path below this view's root (`ino-<ino>` if it is not
    /// below it, or its path cannot be read).
    fn path_below_view_root(&self, ino: Ino) -> String {
        let fallback = || format!("ino-{ino}");
        let Ok(path) = self.meta.path_of(ino) else {
            return fallback();
        };
        if self.view_root == constellation_fs_core::types::ROOT_INO {
            return path;
        }
        let Ok(root) = self.meta.path_of(self.view_root) else {
            return fallback();
        };
        match path.strip_prefix(&root) {
            Some(rest) if rest.starts_with('/') => rest.to_string(),
            _ => fallback(),
        }
    }
}

#[cfg(test)]
mod conflict_name_tests {
    #[test]
    fn a_path_becomes_one_name() {
        assert_eq!(
            super::conflict_name_for_path("/r/.git/refs/heads/master.lock"),
            "r%2F.git%2Frefs%2Fheads%2Fmaster.lock"
        );
        assert_eq!(super::conflict_name_for_path("/a%b"), "a%25b");
        let long = format!("/{}", "x/".repeat(300));
        assert!(super::conflict_name_for_path(&long).len() <= 200);
    }
}
