//! Chunks and publication: fetching committed chunks, sealing a
//! sequential writer's crossed chunks, and the flush that composes and
//! commits a write session's manifest (locally, or forwarded with rebase).

use super::*;
use crate::recovery::conflict_name_for_path;

/// How many times a forwarded whole-file manifest commit may rebase
/// onto a concurrent update before giving up with `EAGAIN`. Each pass
/// costs one round trip to the holder and loses only to a writer that
/// committed in between, so a handful of attempts absorbs a conflict
/// storm across a realistic writer set without spinning forever.
pub(super) const MANIFEST_COMMIT_ATTEMPTS: u32 = 8;

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

    /// Full content of committed chunk `idx`, zero-padded to `len`
    /// (cache/store fetch, or zero-fill for a hole/beyond-EOF chunk that
    /// was never written). Used to seed a staging chunk's untouched
    /// bytes before a partial (non-whole-chunk) write lands on top.
    pub(super) fn committed_chunk_padded(
        &self,
        hashes: &SparseChunks,
        idx: u64,
        len: u32,
    ) -> Result<Vec<u8>, Code> {
        let mut data = match hashes.get(&idx) {
            Some(h) => Vec::from(self.fetch_chunk(h)?),
            None => Vec::new(),
        };
        data.resize(len as usize, 0);
        Ok(data)
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
            let staging = Staging::create(
                &self.staging_dir,
                ino,
                gen,
                self.session_staging_budget(),
                self.host.fs.clone(),
            )
            .map_err(|e| staging_code(&e))?;
            let mut staging = staging;
            if size != manifest.file_len {
                staging.set_len_sparse(size).map_err(|e| staging_code(&e))?;
            }
            e.insert(WriteState {
                staging,
                file_len: size,
                base: Some(manifest.clone()),
                sealed: HashMap::new(),
                enrolled: HashSet::new(),
                holes: crate::staging::DirtyRuns::default(),
                zeroed: crate::staging::DirtyRuns::default(),
                seal_buffer: Vec::new(),
                high_water: size.min(manifest.file_len),
                written: Vec::new(),
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

    /// Seal dirty full chunks below the contiguous write high-water
    /// mark. Byte-prefix continuity is used instead of borrowing the
    /// read prefetcher's signal: it directly proves a sequential
    /// writer has crossed the boundary. A later write into a sealed
    /// chunk re-admits and re-dirties that chunk.
    pub(super) fn seal_crossed_chunks(&self, ino: Ino, ws: &mut WriteState) -> Result<(), Code> {
        let complete = ws.high_water / u64::from(self.chunk_size);
        let mut sealed_any = false;
        for idx in 0..complete {
            if !ws.staging.is_dirty(idx) || ws.sealed.contains_key(&idx) {
                continue;
            }
            let mut data = std::mem::take(&mut ws.seal_buffer);
            data.resize(self.chunk_size as usize, 0);
            ws.staging
                .read_at(idx * u64::from(self.chunk_size), &mut data)
                .map_err(|error| staging_code(&error))?;
            let hash = self.store.hash(&data);
            if data.iter().all(|byte| *byte == 0) {
                ws.staging.release_chunk(idx, self.chunk_size);
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
            ws.staging.release_chunk(idx, self.chunk_size);
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
    /// overwritten or punched) and withdraw the one pending-upload claim
    /// its seal enrolled, if it enrolled one. Other claims on the same
    /// content — another index with identical bytes, an earlier manifest
    /// of this inode not yet uploaded — are untouched, so that content
    /// still uploads (`Meta::cancel_pending_upload`).
    pub(super) fn unseal(
        &self,
        ws: &mut WriteState,
        ino: Ino,
        idx: u64,
    ) -> Result<Option<ChunkHash>, Code> {
        let hash = ws.sealed.remove(&idx);
        if let Some(hash) = &hash {
            if ws.enrolled.remove(&idx) {
                self.meta
                    .cancel_pending_upload(hash, ino)
                    .map_err(|error| error.code())?;
            }
        }
        Ok(hash)
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
    pub(super) fn flush_inode(&self, ino: Ino, force_through: bool) -> Result<(), Code> {
        // The inode's operation lock, not its shard lock, is what keeps
        // per-inode request order across the publication: the session is
        // detached from its shard (its size stays visible to `getattr`
        // and `lookup`) so the compose, the commit — a forward — and the
        // drain run with no shard held (EC2 finding 1).
        let _op = self.inode_ops.lock(ino);
        let Some(ws) = self.writes.detach(ino) else {
            return Ok(());
        };
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
            return Ok(());
        }
        constellation_vfs::watch::stage("flush: compose and commit");
        crate::locks::take_lapsed();
        REFUSED_COMMIT.with(|r| r.borrow_mut().take());
        match self.flush_detached(ino, ws, force_through) {
            Ok(drain) => {
                self.writes.retire(ino);
                if drain {
                    self.drain_inode(ino)?;
                }
                Ok(())
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
                self.writes.retire(ino);
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
                    l.invalidate(ino);
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
                        return Ok(());
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
                self.writes.retire(ino);
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
                .scratch_set_manifest(ino, &manifest_bytes, ws.file_len)
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
        // (plan 07 step 1). Sealing one chunk at a time (read its
        // staged range, hash, insert, drop the buffer) keeps peak RSS
        // O(chunk_size), never O(file_len) — 07's whole point.
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
        let dirty_indices: Vec<_> = ws.staging.dirty_indices().collect();
        for idx in dirty_indices {
            if idx >= n_chunks {
                continue;
            }
            let expect_len = layout.chunk_len(file_len, idx) as usize;
            let chunk_start = idx * self.chunk_size as u64;
            let chunk_end = chunk_start + expect_len as u64;
            // Start from `base`'s committed content for this chunk —
            // not whatever `do_write` seeded the untouched neighborhood
            // with at write time, which is only as fresh as the
            // manifest at that moment — and replay just the byte
            // ranges this flush's own `write()` calls actually
            // delivered on top. Otherwise a chunk two nodes disjointly
            // patch converges on whichever writer commits last, since
            // that writer's seed silently reproduces the *other*
            // writer's bytes as zero: the earlier content it copied in
            // predates the sibling's patch landing.
            // A chunk this flush's writes cover completely needs nothing
            // of the base: not fetched (plan 30 §M9 × §M4: the base's
            // chunk may be one only a departed holder had — an adopted,
            // held manifest — and an overwrite must not depend on it).
            let covered = covers(&ws.written, chunk_start, chunk_end);
            let mut data = match old_hashes.get(&idx) {
                Some(hash) if !covered && !ws.zeroed.contains(idx) => {
                    let mut fetched = Vec::from(self.fetch_chunk(hash)?);
                    fetched.resize(expect_len, 0);
                    ws.clip_base(&mut fetched, chunk_start, base.file_len);
                    fetched
                }
                _ => vec![0u8; expect_len],
            };
            for &(start, end) in &ws.written {
                let start = start.max(chunk_start);
                let end = end.min(chunk_end);
                if end <= start {
                    continue;
                }
                let len = (end - start) as usize;
                let mut buf = vec![0u8; len];
                ws.staging
                    .read_at(start, &mut buf)
                    .map_err(|e| staging_code(&e))?;
                let rel = (start - chunk_start) as usize;
                data[rel..rel + len].copy_from_slice(&buf);
            }
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
            let gated =
                admitted.is_some() && self.sync.as_ref().is_some_and(|h| h.lease.ack_gated());
            let retry = gated.then(|| (base.clone(), manifest_bytes.clone(), dirty_hashes.clone()));
            let committed =
                self.commit_manifest_local(ino, &ws, base, manifest_bytes, dirty_hashes);
            let jseq =
                (gated && committed.is_ok()).then(|| self.meta.journal_tip().unwrap_or(u64::MAX));
            drop(owned);
            drop(admitted);
            if let Err(errno) = committed {
                return Err(FlushFail {
                    errno,
                    ws: Some(Box::new(ws)),
                });
            }
            if let (Some(h), Some(jseq), Some((base, bytes, dirty))) = (&self.sync, jseq, retry) {
                if !self.local_ack_durable(h, jseq) {
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
                self.recall_after_local_inos(h, vec![ino]);
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
                let tag = crate::locks::current_tag().map_err(|_| MutateFail::LockLapsed)?;
                self.meta
                    .with_lock_tag(&tag, crate::locks::now_ms(), || {
                        self.meta.set_manifest_dirty(
                            ino,
                            Some(&base.encode()),
                            manifest_bytes,
                            file_len,
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
