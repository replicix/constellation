//! [`FuseNotifySink`]: the engine's cache invalidations, as
//! `FUSE_NOTIFY_INVAL_*` writes on `/dev/fuse`.
//!
//! A 1:1 rename of the adapter's former `NotifySink` impl on
//! `fuser::Notifier` (plan 31 §6.5). The delivery rules — one dedicated
//! thread, never while an engine lock is held, held back while an op on
//! the inode is in flight, dropped past the TTL — are the engine's
//! (`constellation_engine::kernel_inval`, whose module doc tells the
//! zombie-daemon incident behind them); this only performs the writes.
//!
//! # The gate (plan 31 §6.11, session handover)
//!
//! A notification is a synchronous `write(2)` the kernel may park behind
//! a request in flight on the same inode (`kernel_inval`'s module doc).
//! Once a session stops reading `/dev/fuse` for a handover, a request the
//! kernel queued meanwhile is answered only by the *next* server — so a
//! notification written in that gap could wait for as long as the gap
//! lasts, and one written by a process that is about to `exec` would wedge
//! the `exec` itself (it waits for every other thread to leave the
//! kernel). The sink therefore has a gate: a detach closes it *while the
//! session is still serving*, waits for the writes already under way (they
//! can complete, since their requests are still being answered), and from
//! then on drops every invalidation until the session is resumed. What it
//! drops is at most a TTL of staleness for entries and attributes; the
//! resuming host re-invalidates the data of every open file
//! (`constellation-frontend-fuse`'s caller does, from the handle table).
//!
//! # What the page-cache table is told (plan 42 §5.1.3)
//!
//! Every data invalidation the sink writes marks the inode's pages as not
//! current (S1, *before* the write: an open answered while the kernel is
//! dropping them must not keep them), and every one it cannot deliver —
//! refused by the closed gate, failed with anything but `ENOENT`, or
//! dropped by the engine (`FrontendEvents::undelivered`) — marks them too
//! (S2: pages have no lifetime, so a lost notification leaves them stale
//! until an open drops them). The table is the session's
//! ([`crate::PageCacheTable`]), handed to the gate with its notifier: a
//! resumed session's gate marks the resumed session's table.
//!
//! # Only what the kernel can hold
//!
//! An entry invalidation (and a delete) takes the parent directory's
//! `i_rwsem` in the kernel whether or not the name is cached, so it is
//! written only for a name the kernel can hold a valid dentry for
//! ([`KernelEntries`], `dentries`'s module doc): the others drop nothing,
//! and skipping them keeps this thread off the directory locks a `kill -9`
//! could otherwise leave it parked on for good.

use crate::dentries::KernelEntries;
use crate::pagecache::{Mark, PageCacheTable};
use constellation_vfs::{FrontendEvents, Invalidation};
use fuser::{INodeNo, Notifier};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

struct GateState {
    notifier: Option<Notifier>,
    /// What the kernel of the session written to can hold.
    entries: Arc<KernelEntries>,
    /// What that session knows of its kernel's page cache.
    pc: Arc<PageCacheTable>,
    open: bool,
    /// Writes under way right now.
    writing: u32,
}

/// The gate between the engine's invalidation thread and one session's
/// `/dev/fuse` (see the module doc).
pub(crate) struct NotifyGate {
    state: Mutex<GateState>,
    idle: Condvar,
}

impl NotifyGate {
    pub(crate) fn new(
        notifier: Notifier,
        entries: Arc<KernelEntries>,
        pc: Arc<PageCacheTable>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(GateState {
                notifier: Some(notifier),
                entries,
                pc,
                open: true,
                writing: 0,
            }),
            idle: Condvar::new(),
        })
    }

    /// The notifier to write with, and what its kernel can hold, counted
    /// as a write under way; `Err` with the session's table while the
    /// gate is closed (`None` once it is retired).
    #[allow(clippy::type_complexity)]
    pub(crate) fn enter(
        &self,
    ) -> Result<(Notifier, Arc<KernelEntries>, Arc<PageCacheTable>), Option<Arc<PageCacheTable>>>
    {
        let mut st = self.state.lock().unwrap();
        let pc = st.pc.clone();
        if !st.open {
            return Err(Some(pc));
        }
        let Some(notifier) = st.notifier.clone() else {
            return Err(None);
        };
        st.writing += 1;
        Ok((notifier, st.entries.clone(), pc))
    }

    /// The session's page-cache table (whatever the gate's state).
    pub(crate) fn page_cache(&self) -> Arc<PageCacheTable> {
        self.state.lock().unwrap().pc.clone()
    }

    pub(crate) fn exit(&self) {
        let mut st = self.state.lock().unwrap();
        st.writing -= 1;
        if st.writing == 0 {
            self.idle.notify_all();
        }
    }

    /// Close the gate and wait up to `within` for the writes under way.
    /// `false`: one is still in the kernel (the gate stays closed; the
    /// caller reopens it if it gives up).
    pub(crate) fn close_and_wait(&self, within: Duration) -> bool {
        let deadline = Instant::now() + within;
        let mut st = self.state.lock().unwrap();
        st.open = false;
        while st.writing > 0 {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return false;
            }
            st = self.idle.wait_timeout(st, left).unwrap().0;
        }
        true
    }

    /// Close the gate for good, after waiting up to `within` for the
    /// writes under way, and drop the notifier: an ended session's
    /// connection must close, and the notifier's channel is one of the
    /// descriptors holding it open.
    pub(crate) fn retire(&self, within: Duration) {
        let _ = self.close_and_wait(within);
        self.state.lock().unwrap().notifier = None;
    }

    /// Open the gate again, writing through `notifier` from now on when
    /// one is given (a resumed session's channel, with that session's
    /// `entries` and page-cache table).
    #[allow(clippy::type_complexity)]
    pub(crate) fn reopen(
        &self,
        session: Option<(Notifier, Arc<KernelEntries>, Arc<PageCacheTable>)>,
    ) {
        let mut st = self.state.lock().unwrap();
        if let Some((notifier, entries, pc)) = session {
            st.notifier = Some(notifier);
            st.entries = entries;
            st.pc = pc;
        }
        st.open = true;
    }
}

/// One mount's kernel cache, as the engine's invalidations reach it.
#[derive(Clone)]
pub struct FuseNotifySink {
    gate: Arc<NotifyGate>,
}

impl FuseNotifySink {
    /// A sink writing through `notifier`, never gated (a session that is
    /// not handed over), to the kernel whose dentries `entries` tracks
    /// ([`crate::FuseFs::kernel_entries`] of the session's filesystem).
    pub fn new(notifier: Notifier, entries: Arc<KernelEntries>) -> Self {
        Self {
            gate: NotifyGate::new(notifier, entries, PageCacheTable::new(Default::default())),
        }
    }

    /// A sink writing through `notifier` to the kernel of `link`'s
    /// session ([`crate::FuseFs::kernel_link`]): what it writes and fails
    /// to write is marked in that session's page-cache table.
    pub fn linked(notifier: Notifier, link: &crate::KernelLink) -> Self {
        let gate = NotifyGate::new(notifier, link.entries.clone(), link.pc.clone());
        // The session's repair lane writes through it too.
        let _ = link.gate.set(gate.clone());
        Self { gate }
    }

    pub(crate) fn gated(gate: Arc<NotifyGate>) -> Self {
        Self { gate }
    }

    pub(crate) fn gate(&self) -> &Arc<NotifyGate> {
        &self.gate
    }
}

/// The inode whose pages `inv` drops, if it drops pages.
fn data_ino(inv: &Invalidation) -> Option<u64> {
    match inv {
        Invalidation::Data { ino, .. } | Invalidation::Fenced { ino } => Some(*ino),
        _ => None,
    }
}

impl FrontendEvents for FuseNotifySink {
    fn invalidate(&self, batch: &[Invalidation]) {
        let (notifier, entries, pc) = match self.gate.enter() {
            Ok(entered) => entered,
            Err(pc) => {
                // Closed for a handover (or retired): nothing reaches the
                // kernel, and its pages stay as they are (S2).
                if let Some(pc) = pc {
                    for ino in batch.iter().filter_map(data_ino) {
                        pc.mark(ino, Mark::S2);
                    }
                }
                return;
            }
        };
        for inv in batch {
            // Before the write: an open answered while the kernel drops
            // the pages must not keep them (S1), and the drop is armed
            // for the write-race bookkeeping until the write returns
            // (A2: the kernel drops inside it).
            let data = data_ino(inv);
            let whole = matches!(
                inv,
                Invalidation::Data { range: None, .. } | Invalidation::Fenced { .. }
            );
            let arm = data.map(|ino| {
                pc.mark(ino, Mark::S1);
                (ino, pc.arm_drop(ino, false, whole))
            });
            // ENOENT (nothing cached) is the common answer; any other
            // error leaves pages the kernel did not drop (S2), and for
            // the rest only means there was nothing to drop.
            let result = match inv {
                Invalidation::Entry { parent, name }
                | Invalidation::Deleted { parent, name, .. }
                    if !entries.may_be_cached(*parent, name.as_bytes()) =>
                {
                    Ok(())
                }
                Invalidation::Entry { parent, name } => {
                    notifier.inval_entry(INodeNo(*parent), OsStr::from_bytes(name.as_bytes()))
                }
                // Offset -1: the attributes only.
                Invalidation::Attr { ino } => notifier.inval_inode(INodeNo(*ino), -1, 0),
                // Offset 0, length 0: the attributes and every page.
                Invalidation::Data { ino, range: None } | Invalidation::Fenced { ino } => {
                    notifier.inval_inode(INodeNo(*ino), 0, 0)
                }
                Invalidation::Data {
                    ino,
                    range: Some((offset, len)),
                } => notifier.inval_inode(INodeNo(*ino), *offset as i64, *len as i64),
                Invalidation::Deleted { parent, name, ino } => notifier.delete(
                    INodeNo(*parent),
                    INodeNo(*ino),
                    OsStr::from_bytes(name.as_bytes()),
                ),
                _ => Ok(()),
            };
            if let (Some(ino), Err(e)) = (data, &result) {
                if e.raw_os_error() != Some(libc::ENOENT) {
                    pc.mark(ino, Mark::S2);
                }
            }
            if let Some((ino, seq)) = arm {
                pc.drop_written(ino, seq);
            }
        }
        self.gate.exit();
    }

    /// Plan 42 §5.1.3 (S2): the engine dropped `batch`; its pages were not
    /// dropped either.
    fn undelivered(&self, batch: &[Invalidation]) {
        let pc = self.gate.page_cache();
        for ino in batch.iter().filter_map(data_ino) {
            pc.mark(ino, Mark::S2);
        }
    }
}
