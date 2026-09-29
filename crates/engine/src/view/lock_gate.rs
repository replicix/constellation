//! Plan 30 §M14: the lock fence at I/O and publication points, and a
//! recalled grant's flush.

use super::*;

impl View {
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
    /// `Ok(true)`: such a discard happened earlier where nobody could be
    /// told (a new lock, a recalled grant's flush); the caller reports
    /// `EIO` after its own flush. One or two relaxed loads when this node
    /// holds no grant, lock or taint.
    pub(crate) fn lock_publish_gate(&self, ino: Ino) -> Result<bool, Code> {
        let Some(l) = self.cluster_locks() else {
            return Ok(false);
        };
        if View::is_synthetic(ino) {
            return Ok(false);
        }
        if let Some(fenced) = l.take_discard(ino) {
            let dirty = self.discard_lock_writes(ino);
            if fenced || dirty {
                return Err(Code::Io);
            }
        }
        Ok(l.take_owed(ino))
    }

    /// Plan 30 §M14: before a new local lock or a write on `ino` — dirty
    /// data written under an earlier grant that ended without its flush
    /// must not ride along with what comes next (under a fresh grant, or
    /// unlocked). Discarded; the next close or `fsync` reports `EIO`.
    pub(crate) fn lock_discard_tainted(&self, ino: Ino) {
        let Some(l) = self.cluster_locks() else {
            return;
        };
        if l.take_discard(ino).is_some() && self.discard_lock_writes(ino) {
            l.owe(ino);
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
                l.owe(ino);
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
