//! `copy_file_range(2)`, copied by the view itself (DESIGN §12,
//! "`copy_file_range`").
//!
//! # Why the daemon copies
//!
//! Phase 1 designed `copy_file_range` as pure metadata — a `copy_manifest`
//! record sharing the source's chunks (DESIGN §12) — and never built it,
//! so the FUSE adapter left fuser's `ENOSYS` in place (plan 31 lists the
//! op among those "relying on fuser's ENOSYS-ish defaults"). On `ENOSYS`
//! Linux falls back to a splice copy for good (`fuse_copy_file_range`:
//! `fc->no_copy_file_range`, then `splice_copy_file_range`), and that copy
//! reads the source **through the kernel's page cache**. Without the
//! writeback cache and without `FOPEN_KEEP_CACHE` (neither is set here)
//! that cache can hold a stale page: a buffered write copies into an
//! uptodate folio and unlocks it before its `WRITE` reaches us
//! (`fuse_fill_write_pages`); an `open(2)` meanwhile (no
//! `FOPEN_KEEP_CACHE`) drops the file's pages
//! (`invalidate_inode_pages2`), and a read or readahead of that page
//! fetches it from us before the `WRITE` is applied — the page keeps the
//! old bytes although the write was acknowledged. A read then returns
//! stale bytes, which a later open forgets; a splice copy writes them
//! into the destination, where they are persisted: corruption at rest
//! (xcheckfs, write-back with an ext4 primary: the destination's
//! `O_DIRECT` re-read was wrong as well). Copying here reads what a read
//! of the view answers — the committed manifest and the source's write
//! session on this node — never the kernel's pages, and closes that path.
//!
//! # A byte copy, not shared chunks
//!
//! The bytes go through the destination's write session exactly as a
//! `write` of them would (`do_write`): the lock fence, the owner fence
//! and fencing token (the op's), quota, the throttles, the staging
//! admission, sealing and eager upload, the times, the size, and the
//! publication at close, `fsync` or (for an `O_SYNC` destination) before
//! the answer — forwarded to the file's sequencer from any node, as a
//! write's manifest is. Nothing new can go wrong there.
//!
//! Sharing the source's chunk hashes instead (the `copy_manifest` idea)
//! would skip the bytes but bypass all of it: a hash put into a session's
//! sealed set is referenced by no manifest and enrolled in no pending
//! upload until the destination commits, so GC's protection of it, the
//! dedup-vs-condemn handshake (§14), the upload claims a sealed chunk
//! carries, and the per-file chunk size (a committed chunk past a
//! truncation keeps dead bytes; the last one is short) would all need new
//! rules, and a mistake there loses data. What sharing would buy at rest
//! happens anyway: a copied chunk that lands whole and aligned hashes to
//! the source's (content addressing, keyed under E2E with the same key),
//! is found durable in the cache when it seals (`seal_chunks`), and is
//! neither cached again nor uploaded again. A zero chunk seals as a hole.
//! What remains is the bytes' trip through staging and one hash per
//! chunk: the fast path stays the `copy_manifest` record of §12.
//!
//! # Steps
//!
//! One call copies at most [`COPY_CHUNKS`] chunks (64 MiB at the default
//! 4 MiB, never more than [`COPY_CALL_MAX`]) and answers a short count
//! past that, which callers loop on (`cp`, rsync, the kernel's own
//! users): Linux holds the destination's `i_rwsem` across the whole
//! request, not killably, and the op holds a completion-pool thread, an
//! admission slot and a session handover's drain for as long as it runs.
//! A copy runs in steps that end on the destination's chunk boundaries,
//! so an aligned step is a whole-chunk write (no seed read) and memory
//! stays one chunk. Each step is a read of the source (its inode op lock,
//! released) and then a write of the destination (its own) — never both
//! locks at once, so two copies in opposite directions cannot deadlock,
//! and a copy within one file (the ranges never overlap: refused below)
//! reads nothing it wrote. Each step checks what a `read` and a `write`
//! check at their start — the lock fence of both files, the caller's owner
//! fence, the staging admission — so a long copy is, literally, a run of
//! writes. An error after a step answered is a short copy whose count is
//! exactly the bytes the destination holds (the next call reports the
//! error), as a local filesystem's splice copy answers; a caller that is
//! dying (`cancel`) ends it the same way.
//!
//! `ENOSYS`, `EOPNOTSUPP` and `EXDEV` are never answered (whatever code a
//! step fails with beneath): the first makes Linux stop asking for the
//! mount's lifetime (`fc->no_copy_file_range`), all three send it to the
//! splice copy above. They are answered `EIO`.
//!
//! # Coherence with the kernel
//!
//! Linux holds the destination's `i_rwsem` across the request, writes
//! back both files' dirty pages first (a shared `mmap`'s stores: without
//! the writeback cache, every `write(2)` has reached us before it
//! returned), and drops the destination's pages of the copied range
//! afterwards (`truncate_inode_pages_range`), then updates its size and
//! times from the answer (`fuse_write_update_attr`) — what it does for a
//! write it sent itself. Nothing here notifies the kernel of its own
//! node's copy (`crate::kernel_inval` reports foreign changes only), so
//! nothing fights that truncation; the in-flight registration holds back
//! a foreign change's notification for either file until the answer.
//!
//! Two kernel caveats no daemon can help (any FUSE file system has them):
//! the VFS clamps the length to the source's size *as the kernel caches
//! it*, without revalidating (`generic_copy_file_checks`), so a source
//! another node just grew copies short — up to its old end — until the
//! push invalidation (`crate::kernel_inval`) or the attribute TTL lands;
//! and an `mmap` store into the partial first or last page of the
//! destination range made between the kernel's writeback of the range and
//! its truncation of it after the answer is lost (its comment in
//! `__fuse_copy_file_range` says so: a `write(2)` gives more).

use super::*;
use constellation_vfs::{CancelToken, COPY_MAX};

/// The most chunks one call copies (the module doc).
pub(super) const COPY_CHUNKS: u64 = 16;

/// ... and never more bytes than this, whatever the chunk size.
pub(super) const COPY_CALL_MAX: u64 = 256 << 20;

/// What a copy answers instead of a code that would make Linux copy the
/// request itself through its page cache (the module doc): `ENOSYS` (and
/// for the mount's lifetime), `EOPNOTSUPP`, `EXDEV`.
pub(super) fn never_a_fallback(code: Code) -> Code {
    match code {
        Code::NotImplemented | Code::NotSupported | Code::CrossDevice => {
            tracing::warn!(%code, "copy_file_range failed with a code that would make the kernel copy itself; answering EIO");
            Code::Io
        }
        code => code,
    }
}

/// Who asked, for the owner fence a copy re-checks at every step.
#[derive(Clone, Copy)]
pub(super) struct CopyCaller {
    pub(super) pid: Option<u32>,
    pub(super) lock_owner: Option<u64>,
}

/// A test's hook between two steps of a copy ([`AFTER_STEP`]).
#[cfg(test)]
pub(super) type StepHook = Box<dyn FnMut(u64)>;

#[cfg(test)]
thread_local! {
    /// Called on the copying thread after each step that copied, with the
    /// bytes copied so far: what tests do between two steps.
    pub(super) static AFTER_STEP: std::cell::RefCell<Option<StepHook>> =
        const { std::cell::RefCell::new(None) };
}

impl View {
    /// Copy up to `len` bytes of `src` at `off_in` to `dst` at `off_out`
    /// (the module doc), answering how many were copied, or the error with
    /// the file it concerns. The op's checks (handles, flags, a frozen
    /// destination) are the caller's (`ops`).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn copy_range(
        &self,
        src: Ino,
        off_in: u64,
        dst: Ino,
        off_out: u64,
        len: u64,
        caller: CopyCaller,
        cancel: Option<&CancelToken>,
        deadline: Option<std::time::Instant>,
    ) -> Result<u64, (Ino, Code)> {
        let chunk = u64::from(self.chunk_size);
        let len = len
            .min(COPY_MAX)
            .min((COPY_CHUNKS * chunk).min(COPY_CALL_MAX).max(chunk));
        let src_len = self.copy_source_len(src).map_err(|code| (src, code))?;
        self.copy_dest_kind(dst).map_err(|code| (dst, code))?;
        // What the VFS computes before it asks (`generic_copy_file_checks`),
        // on this node's view of the source: the kernel's cached size may
        // be another node's older one.
        let count = len.min(src_len.saturating_sub(off_in));
        if count == 0 {
            return Ok(0);
        }
        if src == dst && off_out < off_in + count && off_in < off_out + count {
            return Err((dst, Code::Invalid));
        }
        if off_out
            .checked_add(count)
            .is_none_or(|end| end > i64::MAX as u64)
        {
            return Err((dst, Code::FileTooBig));
        }
        let mut buf = Vec::new();
        let mut done = 0u64;
        while done < count {
            let pos_out = off_out + done;
            let want = (count - done).min(chunk - pos_out % chunk);
            let step = self.copy_step(
                src,
                off_in + done,
                dst,
                pos_out,
                want,
                &mut buf,
                caller,
                cancel,
                deadline,
            );
            match step {
                Ok(n) => {
                    done += n;
                    if n < want {
                        // The source ended sooner (it shrank meanwhile).
                        break;
                    }
                    #[cfg(test)]
                    AFTER_STEP.with(|hook| {
                        if let Some(hook) = hook.borrow_mut().as_mut() {
                            hook(done)
                        }
                    });
                }
                Err(error) if done == 0 => return Err(error),
                Err((ino, code)) => {
                    tracing::debug!(src, dst, done, ino, %code, "copy_file_range ended short");
                    break;
                }
            }
        }
        Ok(done)
    }

    /// One step: what a `read` of the source answers, written as a
    /// `write` of the destination would be.
    #[allow(clippy::too_many_arguments)]
    fn copy_step(
        &self,
        src: Ino,
        pos_in: u64,
        dst: Ino,
        pos_out: u64,
        want: u64,
        buf: &mut Vec<u8>,
        caller: CopyCaller,
        cancel: Option<&CancelToken>,
        deadline: Option<std::time::Instant>,
    ) -> Result<u64, (Ino, Code)> {
        if cancel.is_some_and(CancelToken::is_cancelled) {
            return Err((dst, Code::Intr));
        }
        // What `read` and `write` check at their start (`admit!`, the lock
        // fence), per step: a long copy is a run of them, and a grant can
        // lapse between two.
        if self
            .cluster_locks()
            .is_some_and(|l| self.lock_owner_fenced_for(l, caller.pid, caller.lock_owner, None))
        {
            return Err((dst, Code::Io));
        }
        if self.lock_fenced(src) {
            return Err((src, Code::Io));
        }
        if self.lock_fenced(dst) {
            return Err((dst, Code::Io));
        }
        self.admission
            .admit_staging_until(want, deadline)
            .map_err(|code| (dst, code))?;
        let data = if View::is_synthetic(src) {
            self.read_frozen(src, None, pos_in, want)
        } else {
            self.do_read_zc(src, None, pos_in, want)
        }
        .map_err(|code| (src, code))?;
        let bytes: &[u8] = match data.segments() {
            [] => return Ok(0),
            [one] => one,
            many => {
                buf.clear();
                for segment in many {
                    buf.extend_from_slice(segment);
                }
                buf
            }
        };
        self.lock_discard_tainted_for_write(dst);
        let n = self
            .do_write(dst, pos_out, bytes)
            .map_err(|code| (dst, code))?;
        Ok(u64::from(n))
    }

    /// The source's length as a read of it sees it (its write session's,
    /// over the committed size), refusing what is no regular file.
    fn copy_source_len(&self, src: Ino) -> Result<u64, Code> {
        if View::is_synthetic(src) {
            let node = self.synthetic_node(src).ok_or(Code::Stale)?;
            if !self.synthetic_active(&node) {
                return Err(Code::Stale);
            }
            return match node {
                SyntheticNode::Frozen {
                    kind: InodeKind::File,
                    size,
                    ..
                } => Ok(size),
                SyntheticNode::Frozen {
                    kind: InodeKind::Dir,
                    ..
                }
                | SyntheticNode::ConstellationOf { .. }
                | SyntheticNode::SnapshotsOf { .. } => Err(Code::IsDir),
                SyntheticNode::Frozen { .. } => Err(Code::Invalid),
            };
        }
        let attr = self.current_attr(src)?.ok_or(Code::NotFound)?;
        regular(attr.kind)?;
        Ok(attr.size)
    }

    /// The destination must be a regular file.
    fn copy_dest_kind(&self, dst: Ino) -> Result<(), Code> {
        let attr = self.current_attr(dst)?.ok_or(Code::NotFound)?;
        regular(attr.kind)
    }
}

/// `copy_file_range` copies between regular files (`EISDIR` for a
/// directory, `EINVAL` for anything else, as `generic_file_rw_checks`).
fn regular(kind: InodeKind) -> Result<(), Code> {
    match kind {
        InodeKind::File => Ok(()),
        InodeKind::Dir => Err(Code::IsDir),
        _ => Err(Code::Invalid),
    }
}
