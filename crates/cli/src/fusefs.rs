//! FUSE filesystem: bridges the metadata store, chunk store, and disk
//! cache (Phase 1: single node, close/fsync-time flush).

use anyhow::Result;
use constellation_fs_core::cache::{ChunkState, DiskCache};
use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest};
use constellation_fs_core::{ChunkHash, FileAttr, Ino, InodeKind, INLINE_CHUNKS_MAX};
use constellation_meta::{MetaError, MetaStore, SqliteMeta};
use constellation_store_s3::{ChunkStore, CompressionSetting};
use fuser::{
    FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyWrite, Request, TimeOrNow,
};
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::Handle;

const TTL: Duration = Duration::from_secs(1);

/// In-flight write state for one inode: materialized modified chunks and
/// the pending file length (flushed at fsync/close).
#[derive(Default)]
struct WriteState {
    chunks: BTreeMap<u64, Vec<u8>>,
    file_len: u64,
    base: Option<Manifest>,
}

/// A request to the daemon's sync task.
pub enum SyncRequest {
    /// Run a sync round soon; the sender does not wait.
    Nudge,
    /// Run a sync round and report its outcome (fsync barrier).
    Barrier(tokio::sync::oneshot::Sender<Result<(), String>>),
    /// Take the lease for `part` if it is free. `Ok(false)` means a live
    /// foreign holder still owns it.
    Acquire {
        part: String,
        reply: tokio::sync::oneshot::Sender<Result<bool, String>>,
    },
    /// A peer asked us to hand `part`'s lease over (M3.3 fast path):
    /// flush that partition's journal to S3 and release the lease.
    /// Replies with the epoch we held, or `None` if we do not hold it or
    /// the flush failed — in which case the requester falls back to
    /// waiting the lease out through S3, which is always correct.
    HandOff {
        part: String,
        reply: tokio::sync::oneshot::Sender<Option<u64>>,
    },
    /// Reintegrate a stranded branch, either from the control API or
    /// automatically after mounting a persisted deposed state dir.
    Reintegrate(tokio::sync::oneshot::Sender<Result<String, String>>),
    /// Permanently leave the cluster (self). Flushes, tombstones the
    /// registry record, marks the state dir spent, then the caller
    /// unmounts.
    Leave {
        force: bool,
        reply: tokio::sync::oneshot::Sender<Result<String, String>>,
    },
}

/// FUSE-side handle to the metadata sync task.
pub struct SyncHandle {
    pub tx: tokio::sync::mpsc::UnboundedSender<SyncRequest>,
    /// `--fsync-mode s3`: fsync() returns only once the journal is in S3.
    pub fsync_s3: bool,
    /// Lock-free lease state keyed by partition; the write gate reads
    /// the relevant view per mutating op.
    pub leases:
        Arc<std::sync::Mutex<std::collections::HashMap<String, Arc<crate::lease::LeaseView>>>>,
    /// Bound on how long a mutation waits for a foreign holder.
    pub acquire_deadline: Duration,
    /// Offline designation (DESIGN.md §5.2). `None` when no designations
    /// exist for this mount — the write gate then behaves exactly as
    /// before phase 4a.
    pub designations: Option<Arc<crate::designation::DesignationManager>>,
    /// Continuation epoch (DESIGN.md §5.3): frozen ⇒ EROFS; active ⇒
    /// epoch is the authority root (writes without S3 CAS).
    pub epoch_frozen: Option<Arc<std::sync::atomic::AtomicBool>>,
    pub epoch_active: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// Set after a successful self-leave, or when our registry record is
    /// retired/vanished under us: mutations fail with EIO.
    pub departed: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// A read-only registry member never enters the write gate.
    pub read_only_member: bool,
}

pub struct ConstellationFs {
    meta: Arc<SqliteMeta>,
    store: Arc<ChunkStore>,
    cache: Arc<DiskCache>,
    rt: Handle,
    chunk_size: u32,
    compression: CompressionSetting,
    writes: Mutex<HashMap<Ino, WriteState>>,
    /// Open handle counts per inode, for orphan reaping on last close.
    opens: Mutex<HashMap<Ino, u32>>,
    /// Sequential readahead.
    pub(crate) prefetch: crate::prefetch::Prefetcher,
    /// Publication path to the sync task (None in tests).
    sync: Option<SyncHandle>,
}

fn errno(e: &MetaError) -> i32 {
    match e {
        MetaError::NoEnt(_) | MetaError::NoEntry => libc::ENOENT,
        MetaError::Exists => libc::EEXIST,
        MetaError::NotDir => libc::ENOTDIR,
        MetaError::IsDir => libc::EISDIR,
        MetaError::NotEmpty => libc::ENOTEMPTY,
        MetaError::Invalid(_) => libc::EINVAL,
        MetaError::Sqlite(_) | MetaError::Json(_) => libc::EIO,
    }
}

fn to_fuse_attr(a: &FileAttr) -> fuser::FileAttr {
    let kind = match a.kind {
        InodeKind::File => FileType::RegularFile,
        InodeKind::Dir => FileType::Directory,
        InodeKind::Symlink => FileType::Symlink,
        InodeKind::Fifo => FileType::NamedPipe,
        InodeKind::Socket => FileType::Socket,
        InodeKind::BlockDev => FileType::BlockDevice,
        InodeKind::CharDev => FileType::CharDevice,
    };
    let ts = |ns: i64| {
        if ns >= 0 {
            UNIX_EPOCH + Duration::from_nanos(ns as u64)
        } else {
            UNIX_EPOCH
        }
    };
    fuser::FileAttr {
        ino: a.ino,
        size: a.size,
        blocks: a.size.div_ceil(512),
        atime: ts(a.atime_ns),
        mtime: ts(a.mtime_ns),
        ctime: ts(a.ctime_ns),
        crtime: ts(a.ctime_ns),
        kind,
        perm: a.mode as u16,
        nlink: a.nlink,
        uid: a.uid,
        gid: a.gid,
        rdev: a.rdev as u32,
        blksize: 131072,
        flags: 0,
    }
}

fn time_or_now_ns(t: TimeOrNow) -> i64 {
    let st = match t {
        TimeOrNow::SpecificTime(st) => st,
        TimeOrNow::Now => SystemTime::now(),
    };
    st.duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

impl ConstellationFs {
    pub fn new(
        meta: Arc<SqliteMeta>,
        store: Arc<ChunkStore>,
        cache: Arc<DiskCache>,
        rt: Handle,
        chunk_size: u32,
        compression: CompressionSetting,
        sync: Option<SyncHandle>,
    ) -> Self {
        let prefetch = crate::prefetch::Prefetcher::new(rt.clone(), store.clone(), cache.clone());
        Self {
            meta,
            store,
            cache,
            rt,
            chunk_size,
            compression,
            writes: Mutex::new(HashMap::new()),
            opens: Mutex::new(HashMap::new()),
            prefetch,
            sync,
        }
    }

    /// Ask the sync task for an immediate round without waiting:
    /// close() is the close-to-open publication point, and the sooner
    /// the record ships, the sooner other nodes tail it.
    pub(crate) fn nudge_sync(&self) {
        if let Some(h) = &self.sync {
            let _ = h.tx.send(SyncRequest::Nudge);
        }
    }

    /// The write gate (DESIGN.md §5): every mutating op passes through
    /// here, reads never do. The fast path is a few atomic loads; only
    /// when the lease is not currently usable does this hand off to the
    /// sync task, and only the first mutation after an idle release
    /// pays a CAS round trip.
    #[allow(dead_code)]
    pub(crate) fn require_lease(&self) -> Result<(), i32> {
        self.require_lease_for(constellation_fs_core::types::ROOT_INO)
    }

    pub(crate) fn require_lease_for(&self, ino: Ino) -> Result<(), i32> {
        let Some(h) = &self.sync else { return Ok(()) };
        if h.read_only_member {
            return Err(libc::EROFS);
        }
        if let Some(departed) = &h.departed {
            if departed.load(std::sync::atomic::Ordering::Relaxed) {
                tracing::error!("refusing mutation: this node has left the cluster");
                return Err(libc::EIO);
            }
        }
        if let Some(frozen) = &h.epoch_frozen {
            if frozen.load(std::sync::atomic::Ordering::Relaxed) {
                tracing::error!("refusing mutation: continuation epoch frozen (lost a member)");
                return Err(libc::EROFS);
            }
        }
        // Offline designation gate (DESIGN.md §5.2), checked before the
        // ordinary lease: a designated path's write authority does not
        // come from the partition lease at all while the designee is
        // reachable — see the module doc on `designation::GateDecision`.
        if let Some(designations) = &h.designations {
            let path = self.meta.path_of(ino).unwrap_or_else(|_| "/".into());
            match self.rt.block_on(designations.check(&path)) {
                crate::designation::GateDecision::NoDesignation => {}
                crate::designation::GateDecision::Proceed => return Ok(()),
                crate::designation::GateDecision::ReadOnly { designee, path } => {
                    tracing::error!(
                        path,
                        designee,
                        "refusing mutation: path is offline-designated to another node \
                         and no delegation is available"
                    );
                    return Err(libc::EROFS);
                }
            }
        }
        if h.epoch_active
            .as_ref()
            .is_some_and(|active| active.load(std::sync::atomic::Ordering::Relaxed))
        {
            let part = self.meta.partition_of(ino).unwrap_or_else(|_| "p0".into());
            let map = h.leases.lock().unwrap();
            if let Some(view) = map.get(&part) {
                if view.usable() {
                    view.touch();
                    return Ok(());
                }
            }
            // The sync task performs a P2P-only handoff in epoch mode.
        }
        let part = self.meta.partition_of(ino).unwrap_or_else(|_| "p0".into());
        {
            let map = h.leases.lock().unwrap();
            if let Some(view) = map.get(&part) {
                if view.usable() {
                    view.touch();
                    return Ok(());
                }
                if view.is_lost() {
                    tracing::error!(
                        part,
                        "refusing mutation: this node lost the partition lease"
                    );
                    return Err(libc::EIO);
                }
            }
        }
        let start = std::time::Instant::now();
        loop {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if h.tx
                .send(SyncRequest::Acquire {
                    part: part.clone(),
                    reply: tx,
                })
                .is_err()
            {
                return Err(libc::EIO);
            }
            match rx.blocking_recv() {
                Ok(Ok(true)) => {
                    if let Some(view) = h.leases.lock().unwrap().get(&part) {
                        view.touch();
                    }
                    return Ok(());
                }
                Ok(Ok(false)) => {}
                Ok(Err(e)) => {
                    tracing::error!(error = %e, "lease acquisition failed");
                    return Err(libc::EIO);
                }
                Err(_) => return Err(libc::EIO),
            }
            if start.elapsed() >= h.acquire_deadline {
                tracing::error!(
                    waited = ?start.elapsed(),
                    part,
                    "another node holds the partition lease; failing the write with EIO"
                );
                return Err(libc::EIO);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// fsync() barrier. In `--fsync-mode s3`, block until the journal
    /// (up to now) is durable in the shared log; otherwise just nudge.
    pub(crate) fn sync_barrier(&self) -> Result<(), i32> {
        let Some(h) = &self.sync else { return Ok(()) };
        if !h.fsync_s3 {
            let _ = h.tx.send(SyncRequest::Nudge);
            return Ok(());
        }
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        if h.tx.send(SyncRequest::Barrier(reply_tx)).is_err() {
            return Err(libc::EIO);
        }
        match reply_rx.blocking_recv() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "fsync barrier: sync failed");
                Err(libc::EIO)
            }
            Err(_) => Err(libc::EIO),
        }
    }

    fn load_manifest(&self, ino: Ino) -> Result<Manifest, i32> {
        match self.meta.manifest(ino) {
            Ok(Some(bytes)) => Manifest::decode(&bytes).map_err(|_| libc::EIO),
            Ok(None) => Ok(Manifest::empty(self.chunk_size)),
            Err(e) => Err(errno(&e)),
        }
    }

    /// Resolve the full chunk hash list (following manifest spill).
    fn chunk_list(&self, m: &Manifest) -> Result<Vec<ChunkHash>, i32> {
        match &m.chunks {
            ChunkInfo::Inline(v) => Ok(v.clone()),
            ChunkInfo::Spilled(h) => {
                let blob = self.fetch_chunk(h)?;
                decode_chunk_list(&blob).map_err(|_| libc::EIO)
            }
        }
    }

    /// Get one chunk: cache first, then object store (inserted clean).
    /// An in-flight prefetch for the same chunk is awaited rather than
    /// duplicated.
    fn fetch_chunk(&self, hash: &ChunkHash) -> Result<Vec<u8>, i32> {
        if let Ok(Some(data)) = self.cache.get(hash) {
            return Ok(data);
        }
        while self.prefetch.is_inflight(hash) {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        if let Ok(Some(data)) = self.cache.get(hash) {
            return Ok(data);
        }
        let mut data = None;
        for attempt in 0..3 {
            if let Ok(bytes) = self.rt.block_on(self.store.get_chunk(hash)) {
                data = Some(bytes);
                break;
            }
            if attempt < 2 {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
        let data = data.ok_or(libc::EIO)?;
        // Best effort: cache full just means we stream through.
        let _ = self.cache.insert(hash, &data, ChunkState::Clean);
        Ok(data)
    }

    /// Materialize chunk `idx` of a file for modification: pending write
    /// buffer > cache/store > zero-fill (holes / beyond-EOF extension).
    fn materialize_chunk(
        &self,
        ws: &WriteState,
        hashes: &[ChunkHash],
        idx: u64,
    ) -> Result<Vec<u8>, i32> {
        if let Some(buf) = ws.chunks.get(&idx) {
            return Ok(buf.clone());
        }
        if let Some(h) = hashes.get(idx as usize) {
            let mut data = self.fetch_chunk(h)?;
            data.resize(data.len(), 0);
            return Ok(data);
        }
        Ok(Vec::new())
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
    fn try_upload_dirty(&self, hash: &ChunkHash, data: &[u8]) {
        let mut last_error = None;
        for attempt in 0..3 {
            match self
                .rt
                .block_on(self.store.put_chunk(hash, data, self.compression))
            {
                Ok(()) => {
                    self.cache.set_state(hash, ChunkState::Clean);
                    return;
                }
                Err(error) => last_error = Some(error),
            }
            if attempt < 2 {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
        if let Some(error) = last_error {
            tracing::warn!(%error, %hash, "deferring dirty chunk upload to sync task");
        }
    }

    fn flush_inode(&self, ino: Ino) -> Result<(), i32> {
        let ws = {
            let mut writes = self.writes.lock().unwrap();
            match writes.remove(&ino) {
                Some(ws) => ws,
                None => return Ok(()),
            }
        };
        if let Err(e) = self.require_lease_for(ino) {
            // Put the pending state back: the data is not lost, the
            // caller sees the error and can retry.
            self.writes.lock().unwrap().insert(ino, ws);
            return Err(e);
        }
        let epoch_active = self.sync.as_ref().is_some_and(|h| {
            h.epoch_active
                .as_ref()
                .is_some_and(|active| active.load(std::sync::atomic::Ordering::Relaxed))
        });
        let base = match &ws.base {
            Some(m) => m.clone(),
            None => self.load_manifest(ino)?,
        };
        let old_hashes = self.chunk_list(&base)?;
        let layout = constellation_fs_core::ChunkLayout::new(self.chunk_size);
        let n_chunks = layout.chunk_count(ws.file_len);
        let mut new_hashes: Vec<ChunkHash> = Vec::with_capacity(n_chunks as usize);
        for idx in 0..n_chunks {
            let expect_len = layout.chunk_len(ws.file_len, idx) as usize;
            if let Some(buf) = ws.chunks.get(&idx) {
                let mut data = buf.clone();
                data.resize(expect_len, 0);
                let hash = ChunkHash::of(&data);
                // Dirty until uploaded, then demoted to clean.
                self.cache
                    .insert(&hash, &data, ChunkState::Dirty)
                    .map_err(|_| libc::ENOSPC)?;
                if !epoch_active {
                    self.try_upload_dirty(&hash, &data);
                }
                new_hashes.push(hash);
            } else if let Some(h) = old_hashes.get(idx as usize) {
                // Untouched chunk: reuse. The final (possibly shortened)
                // chunk is re-cut if the file shrank into it.
                if idx == n_chunks - 1 {
                    let old_len = base.layout.chunk_len(base.file_len.max(1), idx) as usize;
                    if old_len != expect_len {
                        let mut data = self.fetch_chunk(h)?;
                        data.resize(expect_len, 0);
                        let hash = ChunkHash::of(&data);
                        self.cache
                            .insert(&hash, &data, ChunkState::Dirty)
                            .map_err(|_| libc::ENOSPC)?;
                        if !epoch_active {
                            self.try_upload_dirty(&hash, &data);
                        }
                        new_hashes.push(hash);
                        continue;
                    }
                }
                new_hashes.push(*h);
            } else {
                // Hole created by extension without data: a zero chunk.
                let data = vec![0u8; expect_len];
                let hash = ChunkHash::of(&data);
                self.cache
                    .insert(&hash, &data, ChunkState::Dirty)
                    .map_err(|_| libc::ENOSPC)?;
                if !epoch_active {
                    self.try_upload_dirty(&hash, &data);
                }
                new_hashes.push(hash);
            }
        }
        let (manifest, spill) =
            Manifest::from_chunks(self.chunk_size, ws.file_len, new_hashes, INLINE_CHUNKS_MAX);
        if let Some(blob) = spill {
            let bh = ChunkHash::of(&blob);
            self.cache
                .insert(&bh, &blob, ChunkState::Dirty)
                .map_err(|_| libc::ENOSPC)?;
            if !epoch_active {
                self.try_upload_dirty(&bh, &blob);
            }
        }
        self.meta
            .set_manifest(ino, &manifest.encode(), ws.file_len)
            .map_err(|e| errno(&e))?;
        Ok(())
    }
}

// (Filesystem impl in fusefs_ops.rs include)
include!("fusefs_ops.rs");
