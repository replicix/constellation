//! Plan 30 §M14: the lock fence at I/O and publication points, and a
//! recalled grant's flush.

use super::*;
use constellation_vfs::{OpCtx, OpKind};

/// Plan 30 §M14: the ops the owner fence refuses — every op that reads
/// or changes file data or the namespace. Lookups, `getattr`, `readdir`
/// and the like are not (git's failure path and the shell around it must
/// still see the tree), nor are `flush`, `release` and the lock ops: the
/// close and the unlock are how the fence lifts. An open for writing is
/// checked in `open` itself.
fn owner_fence_applies(kind: OpKind) -> bool {
    matches!(
        kind,
        OpKind::Setattr
            | OpKind::Mknod
            | OpKind::Mkdir
            | OpKind::Symlink
            | OpKind::Link
            | OpKind::Unlink
            | OpKind::Rmdir
            | OpKind::Rename
            | OpKind::Create
            | OpKind::Read
            | OpKind::Write
            | OpKind::Fsync
            | OpKind::Fallocate
            | OpKind::Setxattr
            | OpKind::Removexattr
    )
}

impl View {
    /// Plan 30 §M14, the owner fence: whether `cx` is an op the fence
    /// refuses ([`owner_fence_applies`]) issued by a lock owner whose
    /// grant lapsed — by its lock owner id, or by its process or one that
    /// process started. One relaxed atomic load while this node holds no
    /// local lock; two while every grant under one is still honoured.
    pub(crate) fn lock_owner_fenced(&self, cx: &OpCtx<'_>) -> bool {
        owner_fence_applies(cx.kind) && self.lock_owner_fenced_any(cx)
    }

    /// [`Self::lock_owner_fenced`] whatever the op.
    pub(crate) fn lock_owner_fenced_any(&self, cx: &OpCtx<'_>) -> bool {
        let Some(l) = self.cluster_locks() else {
            return false;
        };
        let fenced = l.owner_fenced(cx.caller.pid, cx.lock_owner);
        if fenced {
            tracing::debug!(
                target: "constellation::locks",
                op = cx.kind.name(),
                pid = ?cx.caller.pid,
                lock_owner = ?cx.lock_owner,
                "refused: the caller's lock grant lapsed (EIO until its locks are gone)"
            );
        }
        fenced
    }

    /// Plan 30 §M14: I/O on `ino` is fenced (`EIO`) — this node holds
    /// local locks on it under a grant that lapsed. One relaxed atomic
    /// load while no local lock exists anywhere (and nothing at all under
    /// `--locks local`).
    pub(crate) fn lock_fenced(&self, ino: Ino) -> bool {
        self.cluster_locks().is_some_and(|l| l.fenced(ino))
    }

    /// Plan 30 §M14: the fence at a point that publishes `ino`'s dirty
    /// data (close, release, `fsync`; checked *before* the closing
    /// owner's locks are dropped, which would lift the fence). Data
    /// written under a grant this node no longer honours — lapsed, lost,
    /// or unlocked under the fence (`LockTables::take_discard`) — is
    /// discarded, never published: a later publication would overwrite
    /// what the next holder wrote under its own grant. `Err(EIO)` then,
    /// if anything was discarded or local locks are still fenced.
    ///
    /// `Ok(true)`: `fh`, the description publishing, was open when such a
    /// discard happened earlier and has not reported it yet (plan 39
    /// §3.7, errseq-style: [`super::durable`]'s module doc); the caller
    /// reports `EIO` after its own flush. A discard found here is an error
    /// event too: `fh` reports it now, every other description open on the
    /// inode at its next close or `fsync`. One or two relaxed loads when
    /// this node holds no grant, lock or taint and never discarded.
    pub(crate) fn lock_publish_gate(
        &self,
        ino: Ino,
        fh: constellation_vfs::Fh,
    ) -> Result<bool, Code> {
        if View::is_synthetic(ino) {
            return Ok(false);
        }
        if let Some(l) = self.cluster_locks() {
            if let Some(fenced) = l.take_discard(ino) {
                let dirty = self.discard_lock_writes(ino);
                if dirty {
                    let seq = l.note_discard(ino);
                    self.handles.mark_seen(fh, seq);
                }
                if fenced || dirty {
                    return Err(Code::Io);
                }
            }
        }
        let current = self.meta.locks().error_seq(ino);
        Ok(current != 0 && self.handles.take_error(fh, ino, current))
    }

    /// `fh` reports an `EIO` for another reason (the owner fence): it
    /// counts as the report of every discard on `ino` it has not reported
    /// yet.
    pub(crate) fn lock_errors_reported(&self, ino: Ino, fh: constellation_vfs::Fh) {
        let current = self.meta.locks().error_seq(ino);
        if current != 0 {
            self.handles.take_error(fh, ino, current);
        }
    }

    /// Plan 30 §M14: before a new local lock or a write on `ino` — dirty
    /// data written under an earlier grant that ended without its flush
    /// must not ride along with what comes next (under a fresh grant, or
    /// unlocked). Discarded; every description open now reports `EIO` at
    /// its next close or `fsync`.
    pub(crate) fn lock_discard_tainted(&self, ino: Ino) {
        let Some(l) = self.cluster_locks() else {
            return;
        };
        if l.take_discard(ino).is_some() && self.discard_lock_writes(ino) {
            l.note_discard(ino);
        }
    }

    /// Plan 30 §M14: drop `ino`'s unpublished writes (staged bytes, sealed
    /// chunks and their pending-upload claims) and the kernel's pages of
    /// the file, which hold the same bytes. `true` if there were any.
    pub(super) fn discard_lock_writes(&self, ino: Ino) -> bool {
        // After any flush of the file in flight: what it publishes was its
        // to publish, as when the flush held the shard throughout.
        if !self.drop_writes(ino) {
            return false;
        }
        tracing::warn!(
            target: "constellation::locks",
            ino,
            "discarded writes made under a lock grant this node no longer holds (EIO)"
        );
        if let Some(l) = self.cluster_locks() {
            l.invalidate(ino);
        }
        true
    }
}

/// Plan 30 §M14: a recalled grant's flush — what `fsync` guarantees,
/// for one inode: the write state flushed and its manifest committed at
/// the sequencer, the inode's chunks uploaded (write-back included: the
/// next holder must be able to fetch them), and the local journal synced
/// (the log too under `--fsync-mode s3`).
impl crate::locks::LockFlush for View {
    fn flush_for_lock(&self, ino: Ino) -> bool {
        // The release's flush publishes only what its grant still covers:
        // one that lapsed meanwhile (a partition, a stalled node) has
        // been outwaited by its owner, who may have granted the file
        // since — its data is discarded (reported at the next close).
        if let Some(l) = self.cluster_locks() {
            if l.take_discard(ino).is_some() && self.discard_lock_writes(ino) {
                l.note_discard(ino);
            }
        }
        let r = self
            .flush_inode(ino, true)
            .and_then(|()| self.drain_inode(ino))
            .and_then(|()| self.sync_barrier(ino));
        if let Err(code) = r {
            tracing::warn!(target: "constellation::locks", ino, %code, "flush before a lock release failed");
        }
        r.is_ok()
    }
}
