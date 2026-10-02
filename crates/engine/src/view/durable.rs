//! Plan 39: the `fsync` family's durability waits, and the open file
//! descriptions they report errors to.
//!
//! **Waits.** [`View::fsync_durable`] is what an `fsync` (and `fsyncdir`,
//! which is the same barrier applied to a directory's committed entries)
//! does once the lock fence let it through: publish the write session,
//! drain every chunk of the file still pending upload on this node,
//! whoever queued it (plan 39b), and the barrier of the mount's
//! `--fsync-mode` — retried through `crate::fsync_wait` while S3 is
//! transiently away. A frontend that answers from another thread runs it on
//! the `fsync` pool (`crate::fsync_wait::pool`), never on its own worker.
//!
//! **Descriptions.** A view hands out one [`Fh`] per open (§6.12 still
//! holds: a handle addresses only the inode it was opened on). What a
//! handle remembers is the last discard error event on its inode it has
//! seen (`LockTables::note_discard`, errseq-style): sampled at open, so a
//! description opened after a discard never reports it, and compared at
//! each publication point (close, `fsync`), so every description open when
//! it happened reports `EIO` exactly once. Before, the inode carried one
//! "owed" flag the first publication point consumed: a second descriptor's
//! `fsync` then returned 0 for data that had been thrown away — the shape
//! of the 2018 "fsyncgate" (PostgreSQL's checkpointer `fsync`ed through a
//! descriptor other than the one the writeback error was charged to).

use super::*;
use constellation_vfs::{Durability, Fh};

/// The open file descriptions of a view (see the module doc).
#[derive(Default)]
pub(super) struct Handles {
    next: std::sync::atomic::AtomicU64,
    open: Mutex<HashMap<u64, Handle>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Handle {
    pub ino: Ino,
    /// The newest discard error event on `ino` this description has seen.
    pub seen: u64,
}

impl Handles {
    /// A new description of `ino`, having seen everything up to `seen`.
    pub(super) fn open(&self, ino: Ino, seen: u64) -> Fh {
        // Never 0: a frontend passes `Fh(0)` for "no handle".
        let fh = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        self.open.lock().unwrap().insert(fh, Handle { ino, seen });
        Fh(fh)
    }

    pub(super) fn ino(&self, fh: Fh) -> Option<Ino> {
        self.open.lock().unwrap().get(&fh.0).map(|h| h.ino)
    }

    pub(super) fn close(&self, fh: Fh) {
        self.open.lock().unwrap().remove(&fh.0);
    }

    /// Closes `fh` when dropped (`release`, whatever path it answers by).
    pub(super) fn closing(&self, fh: Fh) -> Closing<'_> {
        Closing(self, fh)
    }

    /// Whether `fh` (a description of `ino`) has an error event newer than
    /// `current`'s it has not reported; marks it reported. A handle this
    /// view never gave out (a directory's `Fh(0)`) is owed nothing.
    pub(super) fn take_error(&self, fh: Fh, ino: Ino, current: u64) -> bool {
        let mut open = self.open.lock().unwrap();
        match open.get_mut(&fh.0) {
            Some(h) if h.ino == ino && h.seen < current => {
                h.seen = current;
                true
            }
            _ => false,
        }
    }

    /// `fh` has just been told about `seq` (it reported the discard itself).
    pub(super) fn mark_seen(&self, fh: Fh, seq: u64) {
        if let Some(h) = self.open.lock().unwrap().get_mut(&fh.0) {
            h.seen = h.seen.max(seq);
        }
    }

    /// For a handover: every description and the numbering.
    pub(super) fn export(&self) -> (Vec<(u64, Handle)>, u64) {
        let mut all: Vec<(u64, Handle)> = self
            .open
            .lock()
            .unwrap()
            .iter()
            .map(|(fh, h)| (*fh, *h))
            .collect();
        all.sort_unstable_by_key(|(fh, _)| *fh);
        (all, self.next.load(std::sync::atomic::Ordering::Relaxed))
    }

    pub(super) fn import(&self, handles: &[(u64, Handle)], next: u64) {
        let mut open = self.open.lock().unwrap();
        for (fh, h) in handles {
            open.insert(*fh, *h);
        }
        self.next
            .fetch_max(next, std::sync::atomic::Ordering::Relaxed);
    }
}

/// See [`Handles::closing`].
pub(super) struct Closing<'a>(&'a Handles, Fh);

impl Drop for Closing<'_> {
    fn drop(&mut self) {
        self.0.close(self.1);
    }
}

impl View {
    /// A new open file description of `ino` (see the module doc).
    pub(super) fn open_handle(&self, ino: Ino) -> Fh {
        self.handles.open(ino, self.meta.locks().error_seq(ino))
    }

    /// This view's last description of `ino` closed: forget the inode's
    /// discard error events once no view on the node has it open either
    /// (the events are the node's, `LockTables`; a description of another
    /// mount opened before the event still owes its `EIO`). A description
    /// opened afterwards has seen the event, so forgetting it later loses
    /// nothing.
    pub(super) fn forget_discard_errors(&self, ino: Ino) {
        let locks = self.meta.locks();
        if locks.error_seq(ino) == 0 {
            return;
        }
        let open_elsewhere = self
            .holds
            .as_ref()
            .is_some_and(|h| h.sources().is_open(ino));
        if !open_elsewhere && !self.opens.lock().unwrap().contains_key(&ino) {
            locks.forget_errors(ino);
        }
    }

    /// The inode `fh` was opened on, if this view gave it out.
    pub(super) fn handle_ino(&self, fh: Fh) -> Option<Ino> {
        self.handles.ino(fh)
    }

    /// A handle on `ino` for a test that reads without caring which:
    /// one already open, else a bare one (no open counted).
    #[cfg(test)]
    pub(super) fn any_handle(&self, ino: Ino) -> Fh {
        let open = self.handles.open.lock().unwrap();
        if let Some((fh, _)) = open.iter().find(|(_, h)| h.ino == ino) {
            return Fh(*fh);
        }
        drop(open);
        self.handles.open(ino, 0)
    }

    /// The node's `fsync` policy and waits.
    pub(crate) fn fsync_waits(&self) -> Arc<crate::fsync_wait::FsyncWaits> {
        self.sync
            .as_ref()
            .map(|h| h.fsync.clone())
            .unwrap_or_else(crate::fsync_wait::default_waits)
    }

    /// This view's `Arc`, when an `fsync` (`kind` `Fsync`) or an
    /// `O_SYNC` write's publication (`Write`) may finish on the `fsync`
    /// pool: the frontend answers that op from another thread, the view
    /// was opened by an engine, and there is a sync task to wait for at
    /// all.
    pub(super) fn fsync_deferral(&self, kind: constellation_vfs::OpKind) -> Option<Arc<View>> {
        if self.sync.is_none() || !self.caps.deferrable.contains(kind) {
            return None;
        }
        self.this.get().and_then(std::sync::Weak::upgrade)
    }

    /// The `fsync` barrier for `ino` once the lock fence let it through
    /// (see the module doc), waiting out transient S3 failures. `cancel`:
    /// the caller is gone (for FUSE: its thread has a fatal signal
    /// pending), `EINTR`.
    pub(super) fn fsync_durable(
        &self,
        ino: Ino,
        level: Durability,
        cancel: Option<constellation_vfs::CancelToken>,
    ) -> Result<(), Code> {
        self.durable_run(ino, cancel, || self.sync_barrier_at(ino, level))
    }

    /// The retry loop shared by `fsync` and an `O_SYNC` write: publish
    /// `ino` write-through, then drain every chunk of `ino` still in the
    /// durable `pending_upload` table, then `then`.
    ///
    /// Plan 39b: the drain is the file's, not the call's. Linux `fsync(2)`
    /// covers "all modified in-core data of the file", whoever wrote it —
    /// a PostgreSQL checkpointer `fsync`s files backends wrote and closed
    /// — so in both `--fsync-mode`s this waits for the chunks an earlier
    /// `close()` under `--write-mode back` left queued, a failed
    /// write-through close's, a failed earlier `fsync`'s or `O_SYNC`
    /// write's (what the per-view, in-memory `fsync_owed` set used to
    /// track; the table is the source of truth and survives a restart),
    /// and a retry's own. And it waits for them in a continuation epoch
    /// too (plan 30 §M10), hard-mount style: the epoch exempts a
    /// `close()`, never a barrier. An inode with no pending row costs one
    /// seek in the table's by-inode mirror and no sync-task round trip.
    /// The drain itself (`SyncRequest::DrainInode { fsync: true }`) never
    /// answers success with a row of the inode left: a lost chunk is a
    /// permanent failure, rows another node still has to upload a
    /// transient one this loop waits out.
    fn durable_run(
        &self,
        ino: Ino,
        cancel: Option<constellation_vfs::CancelToken>,
        then: impl Fn() -> Result<(), Code>,
    ) -> Result<(), Code> {
        self.fsync_waits().run(ino, cancel, || {
            self.flush_inode(ino, true)?;
            if self
                .meta
                .upload_pending_for_ino(ino)
                .map_err(|error| error.code())?
            {
                self.drain_inode(ino)?;
            }
            then()
        })
    }

    /// An `O_SYNC`/`O_DSYNC` write's publication: [`Self::flush_inode`]
    /// write-through and the inode's drain, waiting out transient S3
    /// failures like an `fsync`. A write that failed here leaves its
    /// chunks pending, so the next `fsync` drains them.
    ///
    /// Ended early (the caller killed, `cancel`; the soft timeout or the
    /// kernel cap) it answers `EIO`, never `EINTR`: the bytes are written,
    /// but they are not durable, and an `O_SYNC` write that returns
    /// claims they are. The data stays pending either way.
    pub(super) fn flush_sync_write(
        &self,
        ino: Ino,
        cancel: Option<constellation_vfs::CancelToken>,
    ) -> Result<(), Code> {
        self.durable_run(ino, cancel, || Ok(()))
            .map_err(|code| match code {
                Code::Intr => Code::Io,
                code => code,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_are_unique_and_address_their_inode() {
        let h = Handles::default();
        let a = h.open(7, 0);
        let b = h.open(7, 0);
        assert_ne!(a, b);
        assert_ne!(a, Fh(0));
        assert_eq!(h.ino(a), Some(7));
        h.close(a);
        assert_eq!(h.ino(a), None);
        assert_eq!(h.ino(b), Some(7));
    }

    /// errseq: each description open at an event reports it once; one
    /// opened after it never does.
    #[test]
    fn an_error_event_is_reported_once_per_description_open_at_the_time() {
        let h = Handles::default();
        let a = h.open(7, 0);
        let b = h.open(7, 0);
        let event = 3;
        let c = h.open(7, event);
        assert!(h.take_error(a, 7, event));
        assert!(!h.take_error(a, 7, event), "reported once");
        assert!(h.take_error(b, 7, event));
        assert!(!h.take_error(c, 7, event), "opened after the event");
        assert!(!h.take_error(Fh(0), 7, event), "no description");
        assert!(h.take_error(c, 7, event + 1), "a newer event");
    }
}
