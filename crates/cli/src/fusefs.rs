//! FUSE filesystem: bridges the metadata store, chunk store, and disk
//! cache (Phase 1: single node, close/fsync-time flush).

use anyhow::Result;
use constellation_fs_core::cache::{ChunkState, DiskCache};
use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest};
use constellation_fs_core::{ChunkHash, FileAttr, Ino, InodeKind, INLINE_CHUNKS_MAX};
use constellation_meta::{MetaError, MetaStore};
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

pub struct ConstellationFs {
    meta: Arc<dyn MetaStore>,
    store: ChunkStore,
    cache: DiskCache,
    rt: Handle,
    chunk_size: u32,
    compression: CompressionSetting,
    writes: Mutex<HashMap<Ino, WriteState>>,
    /// Open handle counts per inode, for orphan reaping on last close.
    opens: Mutex<HashMap<Ino, u32>>,
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
        meta: Arc<dyn MetaStore>,
        store: ChunkStore,
        cache: DiskCache,
        rt: Handle,
        chunk_size: u32,
        compression: CompressionSetting,
    ) -> Self {
        Self {
            meta,
            store,
            cache,
            rt,
            chunk_size,
            compression,
            writes: Mutex::new(HashMap::new()),
            opens: Mutex::new(HashMap::new()),
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
    fn fetch_chunk(&self, hash: &ChunkHash) -> Result<Vec<u8>, i32> {
        if let Ok(Some(data)) = self.cache.get(hash) {
            return Ok(data);
        }
        let data = self
            .rt
            .block_on(self.store.get_chunk(hash))
            .map_err(|_| libc::EIO)?;
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
    fn flush_inode(&self, ino: Ino) -> Result<(), i32> {
        let ws = {
            let mut writes = self.writes.lock().unwrap();
            match writes.remove(&ino) {
                Some(ws) => ws,
                None => return Ok(()),
            }
        };
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
                self.rt
                    .block_on(self.store.put_chunk(&hash, &data, self.compression))
                    .map_err(|_| libc::EIO)?;
                self.cache.set_state(&hash, ChunkState::Clean);
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
                        self.rt
                            .block_on(self.store.put_chunk(&hash, &data, self.compression))
                            .map_err(|_| libc::EIO)?;
                        let _ = self.cache.insert(&hash, &data, ChunkState::Clean);
                        new_hashes.push(hash);
                        continue;
                    }
                }
                new_hashes.push(*h);
            } else {
                // Hole created by extension without data: a zero chunk.
                let data = vec![0u8; expect_len];
                let hash = ChunkHash::of(&data);
                self.rt
                    .block_on(self.store.put_chunk(&hash, &data, self.compression))
                    .map_err(|_| libc::EIO)?;
                new_hashes.push(hash);
            }
        }
        let (manifest, spill) =
            Manifest::from_chunks(self.chunk_size, ws.file_len, new_hashes, INLINE_CHUNKS_MAX);
        if let Some(blob) = spill {
            let bh = ChunkHash::of(&blob);
            self.rt
                .block_on(self.store.put_chunk(&bh, &blob, self.compression))
                .map_err(|_| libc::EIO)?;
        }
        self.meta
            .set_manifest(ino, &manifest.encode(), ws.file_len)
            .map_err(|e| errno(&e))?;
        Ok(())
    }
}

// (Filesystem impl in fusefs_ops.rs include)
include!("fusefs_ops.rs");
