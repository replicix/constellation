//! Passthrough-eligible opens (plan 38 §3(c)): when a read-only `open`
//! lands on a file whose whole content is one verified chunk already in
//! the local disk cache, the view hands the frontend that chunk file
//! itself ([`PassthroughChunk`] in [`constellation_vfs::Opened`]) instead
//! of promising to serve its reads.
//!
//! Nothing here talks to a kernel — the Linux FUSE adapter turns a
//! backing file into a `FOPEN_PASSTHROUGH` reply (plan 38 Z3b,
//! `constellation-frontend-fuse`'s `passthrough` module, which also owns
//! the kernel's per-inode rules this module cannot see). What this module
//! owns is the part that is the engine's either way:
//! deciding eligibility once, at open; keeping the chunk file alive and
//! un-evicted for exactly as long as the handle; and firing the two
//! read-path hooks that a passthrough handle's reads would otherwise
//! never reach.
//!
//! # Why the chunk is pinned, not merely opened
//!
//! Linux would keep the bytes readable through an open descriptor after
//! the file is unlinked, so eviction-while-open "works" without any of
//! this. What it would break is the disk cache's accounting: `used` would
//! stop covering bytes that are still on the disk, for as long as any
//! passthrough open outlives its eviction. [`DiskCache::pin_open`] is the
//! other choice — the chunk is simply not evictable while a holder exists
//! — which keeps "`used` is what is on disk" true and costs one more
//! non-evictable state, bounded by the open-file table (plan 38 §3(c)
//! argues this trade at length).
//!
//! # What one open handle does not see
//!
//! A passthrough handle reads the chunk it was opened on for its whole
//! life: a *remote* write landing a new manifest leaves it where it is,
//! which is the close-to-open consistency this mount already promises
//! (plan 38 §3(c)). A *local* write — another descriptor on this same
//! mount writing the file — is not covered by that promise: today such a
//! write is visible to a concurrent read through the `WriteState` overlay
//! (`io.rs`), and a passthrough handle, whose reads never reach that
//! overlay, keeps reading the committed chunk instead. That is a real
//! read-after-write break within one mount, and it is not fixable from
//! here (eligibility is decided once, at open, and there is no way to
//! revoke a backing file mid-open). Plan 38 Z3b closes most of it at the
//! open instead: passthrough is refused while any handle on the inode was
//! opened for writing (`writers`, counted from the `OpenFlags` that `open`
//! and `release` both carry) or while it has a write session. What is
//! left is the order the other way round — a passthrough handle opened
//! *first*, a writer after it — and that is close-to-open by design: the
//! passthrough handle keeps the bytes it was opened on until it is closed
//! and reopened, as it would for a writer on another node (plan 38
//! §3(c), and the FUSE adapter's module doc for what the kernel lets a
//! later open of such an inode be).
//!
//! # Frozen snapshot files (plan 38 Z3c)
//!
//! A snapshot view's files are synthetic `Frozen` nodes, which `open`
//! answers before the live rule could run; they get their own entry,
//! [`View::frozen_passthrough_backing`], with the live rule's shape and
//! residency checks and its pin, and without the write-intent ones an
//! immutable file cannot need. Their pins are trimmed at `release` from
//! the handle table rather than from `opens` ([`View::release_frozen`]).
//! Their manifests come from the view's cache of frozen manifests
//! (`frozen_manifests`), which `read_frozen` shares: a snapshot manifest is
//! immutable, so the open's load is the reads' too.
//!
//! # Not for a chunk the memory tier holds
//!
//! Passthrough only wins where the daemon would have gone to the disk
//! cache anyway. A chunk held in the memory tier is a memory hit for the
//! daemon, and the kernel reading the chunk file instead is slower: after
//! `drop_caches` it reads the disk, and with the page cache warm it is
//! no faster (plan 38 Z3c's measurement, in PROGRESS). So
//! [`View::pin_backing`] — the residency check both rules share — refuses
//! a chunk that is in memory ([`DiskCache::in_memory`], a peek that moves
//! nothing). Since the first verifying read of a chunk admits it to
//! memory, passthrough engages for chunks the memory tier has evicted, or
//! with the tier off.
//!
//! # Scan-ahead and atime move to the open
//!
//! `do_read_detached` fires cross-file scan-ahead at offset 0 and the
//! read-time atime bump on every read. A passthrough handle's reads never
//! come back to the daemon, and for a single-chunk file "offset 0 was
//! read" and "it was opened for reading" are the same event, so both fire
//! once from here. They are idempotent by construction (a cold read
//! already runs them twice — `io`'s module doc), so a unit test that
//! calls `read` on a passthrough-opened handle double-firing them changes
//! nothing but a counter.

use super::*;

/// What one passthrough open holds until its `release`. The pin and the
/// file exist only to be dropped at the right moment: the pin keeps the
/// disk cache from evicting the chunk under the open descriptor, and the
/// `Arc` of the file keeps that open file description alive even if the
/// frontend drops its own clone of it without telling the engine.
///
/// `hash` names the chunk the handle sits on: a handover carries it, so
/// the resumed view can re-pin the chunks the kernel still serves the
/// handed-over descriptors from (`handoff.rs`'s module doc).
pub(super) struct PassthroughHandle {
    hash: ChunkHash,
    _pin: OpenPin,
    /// `None` only for a handle a handover brought in whose chunk file
    /// could not be reopened: the kernel holds its own reference to the
    /// file it serves from, so the pin is what matters here.
    _fd: Option<Arc<std::fs::File>>,
}

/// Whether an open with `flags` can write the file: the access mode, as
/// the kernel's `f_flags` keeps it until `release` (`O_TRUNC` and
/// `O_CREAT` are gone from it by then, so they cannot be what a count
/// that `release` decrements is keyed on).
pub(super) fn writes(flags: OpenFlags) -> bool {
    flags.contains(OpenFlags::WRITE)
}

/// A view's passthrough, as [`View::passthrough_status`] reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassthroughStatus {
    /// Eligible opens are offered a backing file.
    pub enabled: bool,
    /// Opens holding one now.
    pub opens: u64,
    /// Why `enabled` is false.
    pub unavailable_reason: Option<&'static str>,
}

impl View {
    /// The chunk file `ino` may be read from directly for an `open` with
    /// `flags`, or `None` — which is every open that is not exactly the
    /// case plan 38 §3(c) names. On `Some`, a [`PassthroughHandle`] is
    /// registered for `ino` and must be dropped by [`Self::drop_passthrough`].
    ///
    /// Every condition is evaluated here, once; none is re-checked while
    /// the handle lives. That is close-to-open consistency, which is what
    /// this mount already promises (plan 30's `cto=strict`) and the only
    /// granularity the kernel's backing-fd model offers — there is no
    /// mid-open hook to revoke a backing file.
    pub(super) fn passthrough_backing(
        &self,
        ino: Ino,
        flags: OpenFlags,
        attr: &FileAttr,
    ) -> Option<PassthroughChunk> {
        if !self.passthrough_offered() {
            return None;
        }
        // Any write intent at all: the handle would have to be able to
        // change the file, and a backing file is read-only here.
        if flags.intersects(OpenFlags::WRITE | OpenFlags::TRUNC | OpenFlags::APPEND) {
            return None;
        }
        // Another handle on this inode can write it (plan 38 Z3b): what it
        // writes would be visible to an ordinary read through the
        // `WriteState` overlay and never to a passthrough handle, so the
        // inode is served by the daemon until every writer has closed.
        if self.writers.lock().unwrap().get(&ino).copied().unwrap_or(0) > 0 {
            return None;
        }
        // One chunk holds the whole file, and an empty file has no chunk
        // to hand over.
        if attr.kind != InodeKind::File || attr.size == 0 {
            return None;
        }
        // A write session on the inode — attached, or detached by a flush
        // in flight — means a read of this file is not the committed
        // manifest's bytes: staged writes, sealed chunks, punched holes,
        // and the pending truncate whose `floor` lives in that same
        // session (`WriteState::floor`) all overlay it.
        {
            let shard = self.writes.lock(ino);
            if shard.contains_key(&ino) || self.writes.pending_len(&shard, ino).is_some() {
                return None;
            }
        }
        let manifest = self.load_manifest(ino).ok()?;
        if attr.size > u64::from(manifest.layout.chunk_size) {
            return None;
        }
        // The manifest's content is valid only below its own `file_len`
        // (a committed truncate the manifest predates leaves dead bytes
        // past it, which a read zeroes and the kernel would not), so the
        // chunk file is the file only when the two lengths agree.
        if manifest.file_len != attr.size {
            return None;
        }
        // Inline only: a one-chunk file never spills its chunk list, and
        // fetching a spilled list here would put a store round trip on
        // the open path for a case that cannot arise.
        let ChunkInfo::Inline(chunks) = &manifest.chunks else {
            return None;
        };
        if chunks.len() != 1 {
            return None;
        }
        let hash = *chunks.get(&0)?;
        let fd = self.pin_backing(ino, hash, attr.size)?;
        // The read-path hooks this handle's reads will never reach (see
        // the module doc). Observable change: an eligible open bumps
        // atime even if the application never reads a byte, where before
        // this plan only a read did. Plan 38 §3(c) sanctions it — for a
        // one-chunk file "it was opened for reading" is the same event as
        // "offset 0 was read" — and `relatime`'s own granularity (a day)
        // makes the difference unobservable in all but a test.
        let files = self.scan.note_read(ino);
        self.prefetch.enqueue_scan(files);
        self.atime
            .on_read(attr, constellation_fs_core::types::now_ns());
        Some(PassthroughChunk {
            fd,
            len: attr.size,
            hash: hash.0,
        })
    }

    /// The two refusals that hold for every open on the view, whatever
    /// the file: no frontend to hand a backing file to, or
    /// `--cache-verify always`.
    fn passthrough_offered(&self) -> bool {
        // Nobody to hand it to: a frontend that does not consume
        // `Opened::backing` would drop it, leaving the engine holding an
        // open descriptor and an un-evictable chunk for a reader that
        // never existed — and a cache whose budget filled with such
        // chunks refuses inserts (`CacheFull`) on the *write* path. Only
        // a frontend that declares it gets offered one (plan 31 §6.6's
        // `FrontendCaps`; Linux FUSE declares it in plan 38 Z3b, with the
        // `FOPEN_PASSTHROUGH` reply that consumes it).
        //
        // `--cache-verify always` promises that every byte served is
        // hashed on the read that serves it. A backing file is read by
        // the kernel, with the daemon never seeing the bytes, so the two
        // cannot both hold: under `Always` passthrough is never offered
        // (plan 38 §2.3).
        self.passthrough_on
            .load(std::sync::atomic::Ordering::Relaxed)
            && self.cache.verify_mode() == CacheVerify::Admit
    }

    /// The frozen-snapshot counterpart of [`Self::passthrough_backing`]
    /// (plan 38 Z3c): the file `ino` of a snapshot (`node`, a
    /// [`SyntheticNode::Frozen`]) may be read from its chunk file directly.
    /// On `Some`, a [`PassthroughHandle`] is registered for `ino`, and
    /// [`Self::release_frozen`] drops it.
    ///
    /// The rule is the live one with what immutability makes moot left
    /// out: a snapshot's file has no write session, no writer and no
    /// pending truncate, and its open for writing is `EROFS` before this
    /// runs. What remains is the shape (one inline chunk holding exactly
    /// `size` bytes) and the residency (cached **and verified**), checked
    /// the same way and pinned the same way, because what keeps the bytes
    /// under a backing descriptor is the disk cache either way.
    ///
    /// Neither read-path hook fires: a frozen read feeds neither the
    /// scan-ahead (whose directory walk is the live tree's) nor atime (a
    /// snapshot has none to move), so a passthrough open has nothing to
    /// make up for.
    pub(super) fn frozen_passthrough_backing(
        &self,
        ino: Ino,
        flags: OpenFlags,
        node: &SyntheticNode,
    ) -> Option<PassthroughChunk> {
        if !self.passthrough_offered() {
            return None;
        }
        if flags.intersects(OpenFlags::WRITE | OpenFlags::TRUNC | OpenFlags::APPEND) {
            return None;
        }
        let SyntheticNode::Frozen {
            kind: InodeKind::File,
            size,
            object: Some(object),
            ..
        } = node
        else {
            return None;
        };
        let size = *size;
        // Before the manifest load: a file larger than a chunk is never
        // eligible, and its open should not pay for finding that out.
        if size == 0 || size > u64::from(self.chunk_size) {
            return None;
        }
        // A frozen manifest is not in the replica's tables: it is read
        // from the snapshot's tree, through the view's cache of them,
        // which `read_frozen` shares — an open that is not passthrough
        // leaves its manifest there for the reads that follow, so no
        // frozen file loads it twice (`frozen_manifests`).
        let manifest = self
            .frozen_manifest(object, "snapshot manifest load (passthrough)")
            .inspect_err(|error| {
                tracing::debug!(ino, %error, "frozen passthrough: manifest load failed");
            })
            .ok()?;
        if size > u64::from(manifest.layout.chunk_size) || manifest.file_len != size {
            return None;
        }
        let ChunkInfo::Inline(chunks) = &manifest.chunks else {
            return None;
        };
        if chunks.len() != 1 {
            return None;
        }
        let hash = *chunks.get(&0)?;
        let fd = self.pin_backing(ino, hash, size)?;
        Some(PassthroughChunk {
            fd,
            len: size,
            hash: hash.0,
        })
    }

    /// A handle of the frozen file `ino` was released (the handle table no
    /// longer lists it): trim its passthrough pins to the handles of it
    /// still open. A synthetic inode is not counted in `opens` — that
    /// table names live inodes to the open-orphan hold writer — so the
    /// count comes from the handle table, which a handover carries too.
    pub(super) fn release_frozen(&self, ino: Ino) {
        if !self.passthrough.lock().unwrap().contains_key(&ino) {
            return;
        }
        self.drop_passthrough(ino, self.handles.count(ino));
    }

    /// Pin `hash` (the whole of `ino`'s `size` bytes), open its chunk file
    /// and register the pair as one of `ino`'s passthrough handles; `None`,
    /// with nothing held, if the chunk is held in the memory tier, or is
    /// not resident and verified at exactly that length.
    fn pin_backing(&self, ino: Ino, hash: ChunkHash, size: u64) -> Option<Arc<std::fs::File>> {
        // Held in the memory tier: the daemon serves it from RAM, and the
        // kernel would read the chunk file — from its page cache if it is
        // there, from the disk if it is not. Measured on a snapshot view
        // (PROGRESS, plan 38 Z3c), 4096 small files after `drop_caches`:
        // passthrough took ~3x the wall time and ~1.8x the daemon CPU of
        // the memory hits it replaced, and was no faster with the page
        // cache warm; with the chunk not in memory (tier off or evicted)
        // it was at parity or better. So a chunk the memory tier holds is
        // never offered — a peek, which moves neither the tier's counters
        // nor its recency. Not re-checked while the handle lives: a chunk
        // admitted later leaves an open passthrough handle where it is.
        if self.cache.in_memory(&hash) {
            return None;
        }
        let resident = self.cache.resident(&hash)?;
        // Not verified: a chunk file this process found on disk at
        // startup and has not hashed. Handing it to the kernel would
        // serve bytes nobody checked, so it is not offered — the first
        // ordinary read hashes it and marks it (plan 38 §2.3), and the
        // next open of the file qualifies.
        if !resident.verified || resident.len != size {
            return None;
        }
        // The pin before the open, never the other way round: a pinned
        // entry is not evicted, so the path cannot be unlinked between
        // the two. Every `return None` below drops it again — a pin whose
        // open never happened would hold a chunk in the cache with
        // nothing to show for it.
        let pin = self.cache.pin_open(&hash).ok()?;
        let fd = match std::fs::File::open(self.cache.chunk_path(&hash)) {
            Ok(file) => Arc::new(file),
            Err(error) => {
                tracing::warn!(
                    ino,
                    hash = %hash.to_hex(),
                    %error,
                    "passthrough: the chunk file could not be opened; serving reads instead"
                );
                return None;
            }
        };
        // The accounting said this length; the file itself has the last
        // word, because the kernel will read it without asking again.
        if fd.metadata().map(|m| m.len()).ok() != Some(size) {
            tracing::warn!(
                ino,
                hash = %hash.to_hex(),
                "passthrough: the chunk file is not the length the cache accounts for"
            );
            return None;
        }
        self.passthrough
            .lock()
            .unwrap()
            .entry(ino)
            .or_default()
            .push(PassthroughHandle {
                hash,
                _pin: pin,
                _fd: Some(Arc::clone(&fd)),
            });
        Some(fd)
    }

    /// An `open`/`create` of `ino` with `flags` was answered: count it if
    /// it can write.
    pub(super) fn note_writer_open(&self, ino: Ino, flags: OpenFlags) {
        if writes(flags) {
            *self.writers.lock().unwrap().entry(ino).or_insert(0) += 1;
        }
    }

    /// A handle opened with `flags` was released.
    pub(super) fn note_writer_release(&self, ino: Ino, flags: OpenFlags) {
        if !writes(flags) {
            return;
        }
        let mut writers = self.writers.lock().unwrap();
        if let std::collections::hash_map::Entry::Occupied(mut slot) = writers.entry(ino) {
            if *slot.get() <= 1 {
                slot.remove();
            } else {
                *slot.get_mut() -= 1;
            }
        }
    }

    /// The frontend learned whether it can consume a backing file
    /// ([`constellation_vfs::Vfs::frontend_negotiated`]). Turning it off
    /// leaves the handles already open alone: their pins go at their
    /// `release`, as always.
    pub(super) fn set_passthrough_on(&self, on: bool) {
        self.passthrough_on
            .store(on, std::sync::atomic::Ordering::Relaxed);
    }

    /// The passthrough handles a handover carries, per inode: the chunk
    /// each one sits on, oldest first ([`HandleTableSnapshot`]).
    pub(super) fn export_passthrough(&self) -> Vec<(Ino, Vec<[u8; 32]>)> {
        let mut out: Vec<(Ino, Vec<[u8; 32]>)> = self
            .passthrough
            .lock()
            .unwrap()
            .iter()
            .map(|(ino, hs)| (*ino, hs.iter().map(|h| h.hash.0).collect()))
            .collect();
        out.sort_unstable_by_key(|(ino, _)| *ino);
        out
    }

    /// Re-pin the chunks a handed-over view's passthrough handles sit on
    /// (plan 38 Z3b). The kernel keeps serving those handles from the
    /// previous process's backing files across the handover — it holds its
    /// own reference to each — so what has to be rebuilt here is only the
    /// pin that keeps the disk cache from evicting the chunk under them,
    /// and the trim at `release` then works on the imported entries
    /// exactly as on the view's own.
    ///
    /// A chunk that is no longer resident cannot be pinned, and is logged:
    /// the handle still reads its bytes (the kernel's reference keeps the
    /// unlinked file), but the cache's `used` no longer covers them until
    /// it closes — the "deferred free" this plan otherwise avoids. The old
    /// process keeps its own pins until it `exec`s
    /// (`Engine::close_view_for_handover`), so the only window for that is
    /// the new image's start-up before this view reopens.
    pub(super) fn import_passthrough(&self, handles: &[(Ino, Vec<[u8; 32]>)]) {
        let mut map = self.passthrough.lock().unwrap();
        for (ino, hashes) in handles {
            for raw in hashes {
                let hash = ChunkHash(*raw);
                let pin = match self.cache.pin_open(&hash) {
                    Ok(pin) => pin,
                    Err(error) => {
                        tracing::warn!(
                            ino,
                            hash = %hash.to_hex(),
                            %error,
                            "passthrough: a handed-over handle's chunk is no longer cached; \
                             it stays readable but unaccounted until the handle closes"
                        );
                        continue;
                    }
                };
                let fd = std::fs::File::open(self.cache.chunk_path(&hash))
                    .ok()
                    .map(Arc::new);
                map.entry(*ino).or_default().push(PassthroughHandle {
                    hash,
                    _pin: pin,
                    _fd: fd,
                });
            }
        }
    }

    /// One of `ino`'s passthrough handles is gone (its `release`), and
    /// `remaining` handles of any kind are still open on it.
    ///
    /// The table is kept per inode, not per handle (it predates plan 39's
    /// one-handle-per-open numbering, which it does not consult), so it
    /// cannot know which of several concurrent opens ended. What it can know is that the
    /// pins must never outnumber the live handles, and that their count
    /// must never fall below the number of live *passthrough* handles: a
    /// pin dropped while the handle it belongs to is still being served
    /// makes the chunk evictable, and unlinking it under a live backing
    /// descriptor is exactly the "deferred free" plan 38 §3(c) rejects.
    /// Trimming to `remaining` satisfies both, because an eligible open
    /// pushes a handle and every open counts: pins ≤ handles always, and
    /// the last close trims to zero, so nothing leaks either.
    ///
    /// What stays imprecise is *which* pin a trim drops when the inode
    /// has pins on two chunks (a write landed between two opens): the
    /// oldest pins are kept. With Linux FUSE that is almost always the
    /// right one, because the kernel serves every passthrough handle of an
    /// inode from the backing file of the *first* passthrough open of it
    /// (the FUSE adapter's per-inode rule) and the oldest pin is that
    /// open's. The exception is two opens racing a manifest change whose
    /// replies land out of order, where the kept pin can be the other
    /// chunk's; the kernel's own reference keeps the bytes readable
    /// regardless (`remove`'s doc), so the cost is the cache's `used`
    /// briefly not covering a file an fd still holds.
    pub(super) fn drop_passthrough(&self, ino: Ino, remaining: u32) {
        // Dropped outside the map's lock: releasing a pin takes the disk
        // cache's state lock, which other threads hold while taking this
        // one is not something to make possible.
        let dropped = {
            let mut map = self.passthrough.lock().unwrap();
            let std::collections::hash_map::Entry::Occupied(mut slot) = map.entry(ino) else {
                return;
            };
            let keep = (remaining as usize).min(slot.get().len());
            let dropped = slot.get_mut().split_off(keep);
            if slot.get().is_empty() {
                slot.remove();
            }
            dropped
        };
        for handle in &dropped {
            tracing::trace!(ino, hash = %handle.hash.to_hex(), "passthrough: pin released");
        }
    }

    /// Every passthrough handle of this view at once: the frontend
    /// serving it is gone (unmount, or a handover to another process), so
    /// no `release` will ever arrive for the handles it had open.
    ///
    /// Dropping the `View` does this too — the table owns the pins — but
    /// a frontend's death does not always drop the `View` promptly (the
    /// host may still hold an `Arc` while it tears the mount down), and a
    /// pin held past the handle it belongs to is a chunk the cache cannot
    /// evict for no reason anybody can see. A handover does *not* come
    /// through here (`Engine::close_view_for_handover`): the kernel goes
    /// on serving the handed-over handles from this process's backing
    /// files, so the pins stay until the process `exec`s, and the resumed
    /// view re-pins the same chunks from the snapshot
    /// ([`Self::import_passthrough`]).
    pub(crate) fn drop_all_passthrough(&self) {
        let dropped = std::mem::take(&mut *self.passthrough.lock().unwrap());
        for (ino, handles) in &dropped {
            for handle in handles {
                tracing::debug!(
                    ino,
                    hash = %handle.hash.to_hex(),
                    "passthrough: pin dropped with no release (teardown or handover)"
                );
            }
        }
    }

    /// Whether this view offers passthrough at all, how many opens it
    /// backs right now, and if it offers none, why — what `node.status`'s
    /// `fuse.mounts[].passthrough` reports for a view whose host has no
    /// better answer. A Linux FUSE host does: its session knows what the
    /// kernel agreed and how many handles the kernel serves from a backing
    /// file (plan 38 Z3b), and its answer wins. The reasons are the two of
    /// [`Self::passthrough_backing`]'s refusals that hold for every open
    /// on the view; the per-open ones (write intent, size, residency) are
    /// not a property of the mount.
    pub fn passthrough_status(&self) -> PassthroughStatus {
        let opens = self
            .passthrough
            .lock()
            .unwrap()
            .values()
            .map(|handles| handles.len() as u64)
            .sum();
        // The FUSE session's reason names (`cache_verify_always` first, as
        // there: it is the operator's choice and holds whatever else does).
        let unavailable_reason = if self.cache.verify_mode() != CacheVerify::Admit {
            Some("cache_verify_always")
        } else if !self
            .passthrough_on
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            Some("frontend")
        } else {
            None
        };
        PassthroughStatus {
            enabled: unavailable_reason.is_none(),
            opens,
            unavailable_reason,
        }
    }

    /// How many passthrough handles `ino` has open (tests; the operator's
    /// count is the FUSE session's, which knows what the kernel was told).
    #[cfg(test)]
    pub(crate) fn passthrough_handles(&self, ino: Ino) -> usize {
        self.passthrough_hashes(ino).len()
    }

    /// The chunks `ino`'s passthrough handles sit on, oldest open first
    /// (what a handover snapshot carries).
    #[cfg(test)]
    pub(crate) fn passthrough_hashes(&self, ino: Ino) -> Vec<ChunkHash> {
        self.passthrough
            .lock()
            .unwrap()
            .get(&ino)
            .map_or_else(Vec::new, |hs| hs.iter().map(|h| h.hash).collect())
    }
}
