//! Zero-copy reads (plan 38 §3(d), Z4b): a read that lies within one
//! verified chunk file of the local disk cache is answered with that file
//! and a range of it ([`ReadData::zero_copy`]), and the frontend's kernel
//! reads it straight into the reader's pages — over Linux FUSE, one
//! `IORING_OP_READ_FIXED` on a zero-copy io_uring queue (kernel 7.3,
//! `CAP_SYS_ADMIN`, opt-in). No byte of it passes through this process.
//!
//! # Two decisions: at open, then per read
//!
//! **At open** ([`View::zero_copy_open`], [`View::zero_copy_open_frozen`])
//! the view decides whether the handle is marked
//! [`constellation_vfs::Opened::zero_copy`]: the frontend negotiated
//! zero-copy ([`FrontendCaps::zero_copy`]), `--cache-verify admit`, a
//! read-only open, a regular file of at least the frontend's threshold
//! ([`FrontendCaps::zero_copy_min_read`]; a smaller file has no read that
//! could qualify), and — for a live file — the passthrough rule's
//! write-intent checks: no handle of the inode open for writing, no write
//! session. A frozen snapshot file has nothing that writes it. An open
//! answered with a passthrough backing file is not marked: its reads never
//! reach the daemon. Passthrough's single-chunk bound and its residency
//! check are left out: a multi-chunk file is what zero-copy is for, and
//! residency is per chunk, so it is a per-read question. The mark matters
//! because the kernel then hands *every* read of the handle over as
//! registered pages; a read the view answers with bytes instead is copied
//! into them one more time (`ReplyData::zero_copy`'s doc in the vendored
//! fuser: a bounce through a memfd and a `READ_FIXED` from it).
//!
//! **Per read** (`do_read_detached` / `read_frozen` →
//! [`View::zero_copy_read`]) a marked handle's read goes zero-copy exactly
//! when — the inode has no write session (no staged bytes, sealed chunks,
//! punched holes or pending truncate `floor` to overlay), the read's slice
//! list has **one** slice of at least the threshold, every byte of it lies
//! below the committed manifest's `file_len` and the inode's size (nothing
//! to zero-fill), the chunk exists (not a hole), and its file is resident and verified by this process at a
//! length covering the slice. Anything else — a read crossing a chunk
//! boundary above all — takes the ordinary path on the same session: the
//! memory tier, or a fetch into it. There is never a second `READ_FIXED`
//! for one request, and no whole-file object to fall back to (plan 38
//! §3(d)): chunk files are the only on-disk form a file has.
//!
//! The threshold is the coordinator's rule for Z4b's fix round, from the
//! read-cost gate: a zero-copy read of a chunk file costs more CPU than
//! the memory hit it replaces below 512 KiB, so a small read is served
//! with bytes. A chunk the memory tier holds is *not* a reason to answer
//! with bytes: on a marked handle the kernel delivers every read as
//! registered pages, and bytes are bounced into them one copy more, which
//! costs more than a `READ_FIXED` from the chunk file (PROGRESS "Plan 38
//! Z4" has the numbers).
//!
//! A cold chunk is not fetched for zero-copy: the read that finds it
//! missing takes the ordinary path, whose fetch verifies it and makes it
//! resident, and the handle's next read in that chunk qualifies.
//!
//! # The chunks under the reads are pinned
//!
//! Each marked handle keeps the last [`ZERO_COPY_SOURCES`] chunk files it
//! read open ([`ZeroCopyHandle`]): each opened once, and held with the disk
//! cache's open pin ([`DiskCache::pin_open`]) — the same guard a
//! passthrough open holds — so eviction never picks it. Random reads over
//! a few chunks reuse them instead of opening, pinning and closing a file
//! per read. A pin does not stop [`DiskCache::remove`] (a copy found
//! corrupt, an explicit eviction): every read checks that the cache still
//! has the chunk, verified, at the length the file was opened at, and that
//! the open file is still linked, and opens the current one otherwise. The
//! read hands the frontend a shared reference to the file and its pin, and
//! the frontend keeps it until the kernel's read is done, so a handle
//! moving on (or closing) while an earlier read of it is still in flight
//! leaves that read's chunk pinned until it completes. What zero-copy can
//! hold un-evictable is bounded by [`ZERO_COPY_SOURCES`] per open marked
//! handle; an idle open handle keeps its chunks so, as a passthrough open
//! keeps its one. The handle's entry goes at its `release`, and every entry
//! when the frontend goes away ([`View::drop_all_zero_copy`]); a zero-copy
//! session is never handed over (it runs on io_uring, which `detach`
//! refuses), so nothing here crosses a handover.
//!
//! # `--cache-verify always`
//!
//! Never zero-copy: that mode promises every byte served is hashed on the
//! read that serves it, and these bytes are never seen here. The FUSE
//! host does not even set up zero-copy queues under it; the check here
//! holds for any frontend.

use super::*;
use constellation_vfs::{Fh, ZeroCopySource};

/// How many chunk files a marked handle keeps open and pinned (module
/// doc): enough that random reads over a few chunks do not reopen one per
/// read, few enough that what zero-copy holds un-evictable stays a small
/// multiple of the open marked handles.
pub(super) const ZERO_COPY_SOURCES: usize = 4;

/// One chunk file a marked handle has open (module doc).
struct CachedSource {
    hash: ChunkHash,
    /// The file's length as opened (= the cache's accounting then).
    len: u64,
    source: Arc<ZeroCopySource>,
}

/// A marked handle's open chunk files, least recently read first (module
/// doc).
#[derive(Default)]
pub(super) struct ZeroCopyHandle {
    sources: Mutex<Vec<CachedSource>>,
}

/// What a [`ZeroCopySource`] holds: the pin that keeps its chunk where it is.
struct ZeroCopyHold(#[allow(dead_code)] OpenPin);

impl View {
    /// Whether an open of a regular file of `size` bytes with `flags` is
    /// marked zero-copy, before the per-inode write checks (the module
    /// doc's open rule): the frontend takes zero-copy reads, `--cache-verify
    /// admit`, a read-only open, and a file with room for a read of the
    /// threshold's size.
    fn zero_copy_open_allowed(&self, flags: OpenFlags, size: u64) -> bool {
        self.zero_copy_offered()
            && !flags.intersects(OpenFlags::WRITE | OpenFlags::TRUNC | OpenFlags::APPEND)
            && size > 0
            && size >= u64::from(self.zero_copy_min_read())
    }

    /// Whether an open of `ino` with `flags` is marked zero-copy (the
    /// module doc's open rule); on `true` the caller registers `fh` with
    /// [`Self::note_zero_copy_open`].
    pub(super) fn zero_copy_open(&self, ino: Ino, flags: OpenFlags, attr: &FileAttr) -> bool {
        if attr.kind != InodeKind::File || !self.zero_copy_open_allowed(flags, attr.size) {
            return false;
        }
        if self.writers.lock().unwrap().get(&ino).copied().unwrap_or(0) > 0 {
            return false;
        }
        let shard = self.writes.lock(ino);
        !(shard.contains_key(&ino) || self.writes.pending_len(&shard, ino).is_some())
    }

    /// The same for a frozen snapshot file: nothing can write it, so only
    /// the mount-wide refusals, the flags and its size decide.
    pub(super) fn zero_copy_open_frozen(&self, flags: OpenFlags, node: &SyntheticNode) -> bool {
        match node {
            SyntheticNode::Frozen {
                kind: InodeKind::File,
                size,
                object: Some(_),
                ..
            } => self.zero_copy_open_allowed(flags, *size),
            _ => false,
        }
    }

    /// `fh` was marked zero-copy at its open.
    pub(super) fn note_zero_copy_open(&self, fh: Fh) {
        self.zero_copy
            .lock()
            .unwrap()
            .insert(fh.0, Arc::new(ZeroCopyHandle::default()));
    }

    /// `fh`'s zero-copy state, if it was marked and the frontend still
    /// takes zero-copy reads.
    pub(super) fn zero_copy_handle(&self, fh: Fh) -> Option<Arc<ZeroCopyHandle>> {
        if !self.zero_copy_on.load(std::sync::atomic::Ordering::Relaxed) {
            return None;
        }
        self.zero_copy.lock().unwrap().get(&fh.0).cloned()
    }

    /// `fh` was released. Its chunks stay pinned until the last read in
    /// flight on each is done (module doc). Nothing to do — not even the
    /// map's lock — on a view whose frontend takes no zero-copy reads:
    /// turning them off drops every entry ([`Self::set_zero_copy_on`]).
    pub(super) fn drop_zero_copy(&self, fh: Fh) {
        if !self.zero_copy_on.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        let dropped = self.zero_copy.lock().unwrap().remove(&fh.0);
        drop(dropped);
    }

    /// Every marked handle at once: the frontend is gone (as
    /// [`Self::drop_all_passthrough`]).
    pub(crate) fn drop_all_zero_copy(&self) {
        let dropped = std::mem::take(&mut *self.zero_copy.lock().unwrap());
        drop(dropped);
    }

    /// The frontend negotiated zero-copy reads of at least `min_read`
    /// bytes, or stopped taking them (then no handle is zero-copy any
    /// more, and none holds a chunk).
    pub(super) fn set_zero_copy_on(&self, on: bool, min_read: u32) {
        self.zero_copy_min_read
            .store(min_read, std::sync::atomic::Ordering::Relaxed);
        self.zero_copy_on
            .store(on, std::sync::atomic::Ordering::Relaxed);
        if !on {
            self.drop_all_zero_copy();
        }
    }

    fn zero_copy_min_read(&self) -> u32 {
        self.zero_copy_min_read
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The refusals that hold for every open of the view: a frontend that
    /// cannot take zero-copy reads, or `--cache-verify always`.
    fn zero_copy_offered(&self) -> bool {
        self.zero_copy_on.load(std::sync::atomic::Ordering::Relaxed)
            && self.cache.verify_mode() == CacheVerify::Admit
    }

    /// The zero-copy answer to a read of `ino` that its caller has cut
    /// into exactly one `slice` of `chunk_size`-byte chunks with nothing
    /// to overlay (module doc), or `None` for the ordinary path.
    /// `file_len`: the inode's size.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn zero_copy_read(
        &self,
        zc: &ZeroCopyHandle,
        ino: Ino,
        manifest: &Manifest,
        hashes: &constellation_fs_core::manifest::SparseChunks,
        chunk_size: u32,
        file_len: u64,
        slice: &constellation_fs_core::chunk::ChunkSlice,
    ) -> Option<ReadData> {
        if self.cache.verify_mode() != CacheVerify::Admit
            || slice.len == 0
            || slice.len < self.zero_copy_min_read()
        {
            return None;
        }
        let chunk_start = slice.index * u64::from(chunk_size);
        let end = chunk_start + u64::from(slice.offset) + u64::from(slice.len);
        // Every byte is the chunk's: none past the manifest's content (a
        // committed truncate leaves dead bytes there, read as zeros) or
        // past the inode's size.
        if end > manifest.file_len || end > file_len {
            return None;
        }
        let hash = *hashes.get(&slice.index)?;
        let need = u64::from(slice.offset) + u64::from(slice.len);
        let source = self.zero_copy_source(zc, ino, hash, need)?;
        Some(ReadData::zero_copy(
            source,
            u64::from(slice.offset),
            slice.len as usize,
        ))
    }

    /// `hash`'s chunk file, pinned, from `zc`'s open files or opened now;
    /// `None` unless it is resident, verified, and at least `need` bytes
    /// long.
    fn zero_copy_source(
        &self,
        zc: &ZeroCopyHandle,
        ino: Ino,
        hash: ChunkHash,
        need: u64,
    ) -> Option<Arc<ZeroCopySource>> {
        let resident = self.cache.resident(&hash)?;
        // Not verified: a file a restart found on disk that nobody here
        // has hashed (plan 38 §2.3). The ordinary read hashes it, and the
        // next read qualifies.
        if !resident.verified || resident.len < need {
            return None;
        }
        let mut sources = zc.sources.lock().unwrap();
        if let Some(at) = sources.iter().position(|c| c.hash == hash) {
            // A pin does not stop `DiskCache::remove` (a copy found
            // corrupt, an explicit eviction, a lost chunk): the entry the
            // check above found may be a new file at the same path, and
            // the one held here unlinked. Served only while it is still
            // the cache's file — same length, still linked.
            let cached = sources.remove(at);
            // `nlink` is a unix-only notion (zero-copy is Linux-FUSE-only
            // to begin with, module doc); elsewhere, never trust a cached
            // handle and fall through to reopen it below.
            #[cfg(unix)]
            let still_linked = cached
                .source
                .file()
                .metadata()
                .is_ok_and(|m| std::os::unix::fs::MetadataExt::nlink(&m) > 0);
            #[cfg(not(unix))]
            let still_linked = false;
            let current = cached.len == resident.len && still_linked;
            if current {
                let source = Arc::clone(&cached.source);
                sources.push(cached);
                return Some(source);
            }
        }
        // The pin before the open, as for passthrough: a pinned chunk is
        // not evicted, so its path cannot be unlinked between the two.
        let pin = self.cache.pin_open(&hash).ok()?;
        let file = match std::fs::File::open(self.cache.chunk_path(&hash)) {
            Ok(file) => file,
            Err(error) => {
                tracing::warn!(
                    ino,
                    hash = %hash.to_hex(),
                    %error,
                    "zero-copy: the chunk file could not be opened; serving the read instead"
                );
                return None;
            }
        };
        let len = file.metadata().map(|m| m.len()).ok()?;
        if len != resident.len {
            tracing::warn!(
                ino,
                hash = %hash.to_hex(),
                "zero-copy: the chunk file is not the length the cache accounts for"
            );
            return None;
        }
        let source = Arc::new(ZeroCopySource::new(file, ZeroCopyHold(pin)));
        if sources.len() >= ZERO_COPY_SOURCES {
            sources.remove(0);
        }
        sources.push(CachedSource {
            hash,
            len,
            source: Arc::clone(&source),
        });
        Some(source)
    }

    /// How many handles are marked zero-copy (tests).
    #[cfg(test)]
    pub(crate) fn zero_copy_handles(&self) -> usize {
        self.zero_copy.lock().unwrap().len()
    }
}
