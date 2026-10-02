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

use constellation_vfs::{FrontendEvents, Invalidation};
use fuser::{INodeNo, Notifier};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

struct GateState {
    notifier: Option<Notifier>,
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
    pub(crate) fn new(notifier: Notifier) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(GateState {
                notifier: Some(notifier),
                open: true,
                writing: 0,
            }),
            idle: Condvar::new(),
        })
    }

    /// The notifier to write with, counted as a write under way; `None`
    /// while the gate is closed.
    fn enter(&self) -> Option<Notifier> {
        let mut st = self.state.lock().unwrap();
        if !st.open {
            return None;
        }
        let notifier = st.notifier.clone()?;
        st.writing += 1;
        Some(notifier)
    }

    fn exit(&self) {
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
    /// one is given (a resumed session's channel).
    pub(crate) fn reopen(&self, notifier: Option<Notifier>) {
        let mut st = self.state.lock().unwrap();
        if let Some(notifier) = notifier {
            st.notifier = Some(notifier);
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
    /// not handed over).
    pub fn new(notifier: Notifier) -> Self {
        Self {
            gate: NotifyGate::new(notifier),
        }
    }

    pub(crate) fn gated(gate: Arc<NotifyGate>) -> Self {
        Self { gate }
    }

    pub(crate) fn gate(&self) -> &Arc<NotifyGate> {
        &self.gate
    }
}

impl FrontendEvents for FuseNotifySink {
    fn invalidate(&self, batch: &[Invalidation]) {
        let Some(notifier) = self.gate.enter() else {
            return;
        };
        for inv in batch {
            // ENOENT (nothing cached) is the common answer; every error
            // only means there was nothing to drop.
            let _ = match inv {
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
        }
        self.gate.exit();
    }
}
