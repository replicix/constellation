//! Plan 30 §M14: which open file descriptions locked for which lock
//! owners, so that the release of a description drops its own (OFD and
//! `flock`) locks — and the wait that makes that release look synchronous
//! to a lock request that follows the `close(2)`.
//!
//! # Why the release is where it is
//!
//! With `FUSE_POSIX_LOCKS`/`FUSE_FLOCK_LOCKS` the kernel keeps no lock of
//! its own for the file (`fuse_file_lock` and `fuse_file_flock` forward
//! everything), so `locks_remove_posix`/`locks_remove_file` find no lock
//! context at close and send nothing. What the daemon gets is:
//!
//! - `FLUSH`, on every `close(2)` of a descriptor, synchronous (the
//!   `close` waits for its answer), naming the closing process's POSIX
//!   lock owner: the view drops that owner's locks there (`flush`), as
//!   POSIX says any close does;
//! - `RELEASE`, after the last descriptor of the description is gone,
//!   naming the description's `flock` owner (`FUSE_RELEASE_FLOCK_UNLOCK`)
//!   and no OFD owner unless the description also used `flock` (the same
//!   owner id): the view drops both there (`release`, through this table,
//!   since 8a6f96c — they leaked before).
//!
//! `FLUSH` cannot drop OFD or `flock` locks: it comes for every descriptor
//! (a `dup`, a forked child's copy, an `exec`'s `O_CLOEXEC` close), and
//! the description and its locks outlive all but the last. Only
//! `RELEASE` says the description is gone.
//!
//! # The release comes after `close(2)` returned
//!
//! `fuse_file_release` sends `RELEASE` in the background
//! (`fuse_file_put(ff, false)`, `fuse_simple_background`): the kernel
//! queues it before `close(2)` returns (the last `fput` runs as task work
//! on the way out of the syscall) but does not wait for the answer, and
//! later still when the description has asynchronous reads or writes in
//! flight or the background queue is full. Another worker (another ring)
//! can therefore serve a lock request the process issues next — or
//! another process told of the close — before the `RELEASE`: a
//! non-blocking lock or a `F_GETLK` then still sees the closed
//! description's lock, `EAGAIN` where every local file system grants it
//! (xcheckfs, Linux 7.3: 40–80 % of tries, 0.1–5 ms of lag).
//!
//! So a `FLUSH` marks each description with locks as closing, and a
//! non-blocking request (or a test) whose only conflicts are locks of
//! closing descriptions waits for their `RELEASE`
//! ([`LockHandles::await_closing`]) before it answers; one whose blocker
//! a release dropped between the refusal and the look is asked once
//! more. The wait is bounded twice by `crate::locks::close_wait`:
//!
//! - from the `FLUSH`: a `FLUSH` that was not the last close (a `dup`'s,
//!   a child's) is answered by no `RELEASE`, and a request that conflicts
//!   with the still-open description's lock gets its `EAGAIN` once the
//!   window is over — late, never wrong;
//! - from the request: a description whose descriptors keep being closed
//!   (a shell holding `flock -n 9` runs a child per command, and each
//!   child's exit closes its copy of descriptor 9) renews its window with
//!   every `FLUSH`, and a request in its shadow must still be refused in
//!   time, not wait as long as the closes go on.
//!
//! A lock request, unlock or test through the description proves it open
//! and ends its window at once. Nothing is released early: the locks
//! still go only with `RELEASE`, with an explicit unlock or with the
//! owner's close, as before.
//!
//! A mount served by a single worker waits for no closing description
//! (`View::set_single_worker`, from the frontend's
//! `FrontendCaps::single_worker`): its requests are served in queue
//! order (strictly so over `/dev/fuse` and on a one-CPU host; over the
//! ring as long as the closing CPU's queue has a free entry), so the
//! `RELEASE` queued before `close(2)` returned is served before the
//! process's next request, and a wait would only hold the one worker
//! that could serve it.
//!
//! The `RELEASE` also publishes what was written under the locks, tagged
//! with the grant (f57db3c, the fencing token), after it dropped them; a
//! new turn on the node waits for an earlier turn's ops in flight under
//! the grant (`LocalOutcome::Predecessor`). A non-blocking request waits
//! for those too, until the same deadline (`ClusterLocks::try_lock_by`);
//! they finish without a FUSE worker, so this wait is kept on a single
//! worker.

use constellation_fs_core::Ino;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// What [`LockHandles::await_closing`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CloseWait {
    /// It waited for closing descriptions: ask again.
    Waited,
    /// No local lock is in the way any more, and a release dropped locks
    /// since the request began: ask again once.
    Cleared,
    /// A lock of a description that is open, or closed longer ago than the
    /// window, is in the way, or the request's deadline has passed: the
    /// refusal stands.
    Refused,
}

/// The lock owners each open handle locked for (see the module doc).
#[derive(Default)]
pub(crate) struct LockHandles {
    by_fh: Mutex<HashMap<u64, Handle>>,
    /// Signalled when a handle is released or proves itself open.
    changed: Condvar,
    /// Counts the releases that dropped locks ([`Self::releases`]).
    releases: AtomicU64,
}

#[derive(Default)]
struct Handle {
    owners: HashSet<(Ino, u64)>,
    /// When a `close(2)` of one of the description's descriptors was
    /// last answered, with no lock op through it since.
    closed: Option<Instant>,
}

impl Handle {
    /// A lock op came through the handle: its description is open.
    /// Whether it was marked closing (and a waiter must look again).
    fn open(&mut self) -> bool {
        self.closed.take().is_some()
    }
}

impl LockHandles {
    /// `owner` locked `ino` through handle `fh`. An owner is bound to the
    /// handle it locked through last: the kernel names an OFD owner after
    /// its `struct file`, which a new open can reuse while the old one's
    /// `RELEASE` is still queued, and that release must not drop the new
    /// description's locks. The request also proves the description open
    /// ([`Self::opened`]).
    ///
    /// Before it moves the owner, a request waits for the `RELEASE` of
    /// the handle the owner is bound to when that handle's close was
    /// answered within `window` and the owner still `holds` locks on
    /// `ino` (finding 12): with the same owner id through another
    /// handle, that description is gone and the new one is at its
    /// address, so those locks are the closed description's. Bound to
    /// the new handle, they would outlive the old `RELEASE` (which would
    /// find nothing of the owner's to drop) as the new description's,
    /// merged with its own. A POSIX owner can lock through several
    /// descriptions of one file, and one of them can be closed by
    /// another process (a child's copy) with no `RELEASE` to follow: its
    /// request goes on once the window is over, its locks kept, as
    /// before. Never past `deadline`, the request's own bound.
    pub(crate) fn note(
        &self,
        fh: u64,
        ino: Ino,
        owner: u64,
        window: Duration,
        deadline: Instant,
        holds: impl Fn() -> bool,
    ) {
        let mut handles = self.by_fh.lock().unwrap();
        loop {
            let now = Instant::now();
            let until = handles
                .iter()
                .find(|(k, h)| **k != fh && h.owners.contains(&(ino, owner)))
                .and_then(|(_, h)| h.closed)
                .map(|closed| (closed + window).min(deadline))
                .filter(|until| *until > now);
            match until {
                // Read under this table's mutex, which is taken before the
                // lock table's.
                Some(until) if holds() => {
                    handles = self.changed.wait_timeout(handles, until - now).unwrap().0;
                }
                _ => break,
            }
        }
        if let Some(h) = handles.get_mut(&fh) {
            if h.open() {
                self.changed.notify_all();
            }
            if h.owners.contains(&(ino, owner)) {
                return;
            }
        }
        for h in handles.values_mut() {
            h.owners.remove(&(ino, owner));
        }
        handles.retain(|k, h| *k == fh || !h.owners.is_empty());
        handles.entry(fh).or_default().owners.insert((ino, owner));
    }

    /// A lock op other than a lock request (an unlock, a test) came
    /// through `fh`: its description is open after all, and whoever waits
    /// for it stops waiting.
    pub(crate) fn opened(&self, fh: u64) {
        if let Some(h) = self.by_fh.lock().unwrap().get_mut(&fh) {
            if h.open() {
                self.changed.notify_all();
            }
        }
    }

    /// A descriptor of `fh` was closed (`flush`): its description may be
    /// on its way to `RELEASE`.
    pub(crate) fn closed(&self, fh: u64) {
        if let Some(h) = self.by_fh.lock().unwrap().get_mut(&fh) {
            h.closed = Some(Instant::now());
        }
    }

    /// `fh` is released: `drop_owner` drops `flock` (the owner the kernel
    /// names in `RELEASE`, if any, unless another handle binds it) and
    /// each owner that locked through `fh`
    /// (under this table's mutex, so a waiter never sees the handle gone
    /// with its locks still there), returning what the caller wants back
    /// and whether it dropped any lock. When one did, every waiter asks
    /// again.
    pub(crate) fn release<T>(
        &self,
        fh: u64,
        flock: Option<(Ino, u64)>,
        mut drop_owner: impl FnMut(Ino, u64) -> (T, bool),
    ) -> Vec<(Ino, T)> {
        let mut handles = self.by_fh.lock().unwrap();
        let owners = handles.remove(&fh).map(|h| h.owners).unwrap_or_default();
        // The owner the kernel names is the description's own (its
        // `struct file`): bound to another handle still open, it is a new
        // description at the old one's address, which locked before this
        // late `RELEASE` arrived (`flock` after the old one unlocked, so
        // no wait held it back: [`Self::note`]). Its locks are not this
        // description's.
        let flock = flock
            .filter(|(ino, owner)| !handles.values().any(|h| h.owners.contains(&(*ino, *owner))));
        let mut any = false;
        let dropped: Vec<(Ino, T)> = flock
            .into_iter()
            .chain(owners)
            .map(|(ino, owner)| {
                let (t, dropped) = drop_owner(ino, owner);
                any |= dropped;
                (ino, t)
            })
            .collect();
        if any {
            self.releases.fetch_add(1, Ordering::SeqCst);
            drop(handles);
            self.changed.notify_all();
        }
        dropped
    }

    /// The owners of locks on `ino` bound to a description closed within
    /// `window`, its `RELEASE` on its way: Linux dropped their locks
    /// already, so a test names one of them only when nothing else is in
    /// the way (`View::lock_test`). A snapshot, taken before the lock
    /// table's mutex: this table's mutex is taken before that one, never
    /// inside it.
    pub(crate) fn closing_owners(&self, ino: Ino, window: Duration) -> HashSet<u64> {
        if window.is_zero() {
            return HashSet::new();
        }
        let now = Instant::now();
        self.by_fh
            .lock()
            .unwrap()
            .values()
            .filter(|h| h.closed.is_some_and(|t| now < t + window))
            .flat_map(|h| h.owners.iter())
            .filter(|(i, _)| *i == ino)
            .map(|(_, owner)| *owner)
            .collect()
    }

    /// How many releases dropped locks so far: read before a request, it
    /// tells [`Self::await_closing`] whether one may have dropped what
    /// refused it.
    pub(crate) fn releases(&self) -> u64 {
        self.releases.load(Ordering::SeqCst)
    }

    /// A non-blocking request on `ino` was refused: if every lock in its
    /// way (`conflicts`: their owners, read under this table's mutex)
    /// belongs to a description closed within `window`, wait until those
    /// descriptions are released, prove open, or the window ends — and
    /// never past `deadline`, the request's own bound. `since`:
    /// [`Self::releases`] before the request.
    pub(crate) fn await_closing(
        &self,
        ino: Ino,
        window: Duration,
        deadline: Instant,
        since: u64,
        conflicts: impl Fn() -> Vec<u64>,
    ) -> CloseWait {
        let mut handles = self.by_fh.lock().unwrap();
        let mut waited = false;
        loop {
            let owners = conflicts();
            if owners.is_empty() {
                return if waited {
                    CloseWait::Waited
                } else if self.releases() != since {
                    CloseWait::Cleared
                } else {
                    // Nothing local was in the way: refused by another
                    // node's grant, or an earlier turn's ops.
                    CloseWait::Refused
                };
            }
            let now = Instant::now();
            if window.is_zero() || now >= deadline {
                return CloseWait::Refused;
            }
            let mut until = now;
            for owner in owners {
                let bound = handles
                    .iter()
                    .find(|(_, h)| h.owners.contains(&(ino, owner)));
                let closed = bound.and_then(|(_, h)| h.closed);
                match closed.map(|t| t + window).filter(|end| *end > now) {
                    Some(end) => until = until.max(end),
                    // A lock of a description that is open (or closed
                    // longer ago than the window): refused.
                    None => {
                        tracing::debug!(
                            target: "constellation::locks",
                            ino,
                            owner,
                            fh = ?bound.map(|(fh, _)| *fh),
                            closed_ago = ?closed.map(|t| now - t),
                            "a non-blocking lock request is refused by a lock of a description \
                             that is open, or whose release the close wait did not see"
                        );
                        return if waited {
                            CloseWait::Waited
                        } else {
                            CloseWait::Refused
                        };
                    }
                }
            }
            // Closes that keep coming renew the windows: the request's
            // deadline caps them.
            let until = until.min(deadline);
            handles = self.changed.wait_timeout(handles, until - now).unwrap().0;
            waited = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// A deadline far enough away not to matter.
    fn later() -> Instant {
        Instant::now() + Duration::from_secs(60)
    }

    #[test]
    fn a_waiter_wakes_when_the_closing_description_is_released() {
        let t = Arc::new(LockHandles::default());
        let held = Arc::new(Mutex::new(vec![0xa_u64]));
        t.note(1, 7, 0xa, Duration::ZERO, later(), || false);
        t.closed(1);
        let releaser = std::thread::spawn({
            let (t, held) = (t.clone(), held.clone());
            move || {
                std::thread::sleep(Duration::from_millis(20));
                t.release(1, None, |_, owner| {
                    held.lock().unwrap().retain(|o| *o != owner);
                    ((), true)
                });
            }
        });
        let started = Instant::now();
        let waited = t.await_closing(7, Duration::from_secs(10), later(), 0, || {
            held.lock().unwrap().clone()
        });
        releaser.join().unwrap();
        assert_eq!(waited, CloseWait::Waited);
        assert!(held.lock().unwrap().is_empty());
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "woken, not timed out"
        );
    }

    #[test]
    fn an_open_description_or_a_spent_window_is_not_waited_for() {
        let t = LockHandles::default();
        let window = Duration::from_secs(10);
        t.note(1, 7, 0xa, Duration::ZERO, later(), || false);
        // Nothing local in the way: refused elsewhere, the refusal stands;
        // unless a release dropped locks since the request: ask again.
        assert_eq!(
            t.await_closing(7, window, later(), t.releases(), Vec::new),
            CloseWait::Refused
        );
        let since = t.releases();
        t.note(9, 7, 0xc, Duration::ZERO, later(), || false);
        t.release(9, None, |_, _| ((), true));
        assert_eq!(
            t.await_closing(7, window, later(), since, Vec::new),
            CloseWait::Cleared
        );
        // A release that dropped nothing (the `flock` owner the kernel
        // names for a description that took no lock) is no reason to ask
        // again.
        let since = t.releases();
        t.release(10, Some((7, 0xd)), |_, _| ((), false));
        assert_eq!(t.releases(), since);
        // Never closed.
        assert_eq!(
            t.await_closing(7, window, later(), t.releases(), || vec![0xa]),
            CloseWait::Refused
        );
        // Closed, then locked through again: open after all.
        t.closed(1);
        t.note(1, 7, 0xa, Duration::ZERO, later(), || false);
        assert_eq!(
            t.await_closing(7, window, later(), t.releases(), || vec![0xa]),
            CloseWait::Refused
        );
        // Closed, then unlocked or tested through: open too.
        t.closed(1);
        t.opened(1);
        assert_eq!(
            t.await_closing(7, window, later(), t.releases(), || vec![0xa]),
            CloseWait::Refused
        );
        // One conflict closing, another open: refused at once.
        t.note(2, 7, 0xb, Duration::ZERO, later(), || false);
        t.closed(1);
        assert_eq!(
            t.await_closing(7, window, later(), t.releases(), || vec![0xa, 0xb]),
            CloseWait::Refused
        );
        // A close whose window is over (a `dup`'s): waited for no more.
        let short = Duration::from_millis(30);
        let started = Instant::now();
        assert_eq!(
            t.await_closing(7, short, later(), t.releases(), || vec![0xa]),
            CloseWait::Waited
        );
        assert!(started.elapsed() >= Duration::from_millis(20));
        assert_eq!(
            t.await_closing(7, short, later(), t.releases(), || vec![0xa]),
            CloseWait::Refused
        );
        // Off (a mount with a single worker).
        t.closed(1);
        assert_eq!(
            t.await_closing(7, Duration::ZERO, later(), t.releases(), || vec![0xa]),
            CloseWait::Refused
        );
    }

    /// A late `RELEASE` names its description's `flock` owner, which a
    /// new description at the same address, bound to its own handle, may
    /// hold now: that owner's locks are left alone. Unbound, it is
    /// dropped as before.
    #[test]
    fn a_late_release_leaves_a_reused_flock_owner_alone() {
        let t = LockHandles::default();
        t.note(2, 7, 0xa, Duration::ZERO, later(), || false);
        let mut dropped = Vec::new();
        t.release(1, Some((7, 0xa)), |ino, owner| {
            dropped.push((ino, owner));
            ((), true)
        });
        assert!(dropped.is_empty(), "{dropped:?}");
        t.release(3, Some((7, 0xb)), |ino, owner| {
            dropped.push((ino, owner));
            ((), true)
        });
        assert_eq!(dropped, [(7, 0xb)]);
    }

    /// No wait for a closing handle's `RELEASE` when the owner holds no
    /// lock on the file any more (it unlocked before the close): nothing
    /// of the closed description's could pass to the new one.
    #[test]
    fn a_request_holding_nothing_does_not_wait_for_the_old_release() {
        let t = LockHandles::default();
        t.note(1, 7, 0xa, Duration::ZERO, later(), || false);
        t.closed(1);
        let started = Instant::now();
        t.note(2, 7, 0xa, Duration::from_secs(10), later(), || false);
        assert!(started.elapsed() < Duration::from_secs(1));
        // Holding locks, it waits (here, out the deadline).
        t.note(3, 7, 0xb, Duration::ZERO, later(), || false);
        t.closed(3);
        let started = Instant::now();
        t.note(
            4,
            7,
            0xb,
            Duration::from_secs(10),
            Instant::now() + Duration::from_millis(50),
            || true,
        );
        assert!(started.elapsed() >= Duration::from_millis(40));
    }

    /// The reviewer's case: a description whose descriptors keep being
    /// closed (`exec 9>lock; flock -n 9 || exit; while :; do cmd; done` —
    /// every child's exit flushes descriptor 9) renews its window with
    /// each `FLUSH`. A request in its shadow is refused by its own
    /// deadline, not after the closes stop.
    #[test]
    fn repeated_closes_do_not_hold_a_request_past_its_deadline() {
        let t = Arc::new(LockHandles::default());
        t.note(1, 7, 0xa, Duration::ZERO, later(), || false);
        t.closed(1);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let closer = std::thread::spawn({
            let (t, stop) = (t.clone(), stop.clone());
            move || {
                while !stop.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(10));
                    t.closed(1);
                }
            }
        });
        let window = Duration::from_millis(100);
        let started = Instant::now();
        let deadline = started + window;
        let mut answer = CloseWait::Waited;
        // As `View::past_closing_locks` does: ask again after each wait,
        // until refused (or, without the deadline, given up on here).
        while answer != CloseWait::Refused && started.elapsed() < Duration::from_secs(3) {
            answer = t.await_closing(7, window, deadline, t.releases(), || vec![0xa]);
        }
        let took = started.elapsed();
        stop.store(true, Ordering::SeqCst);
        closer.join().unwrap();
        assert!(took >= Duration::from_millis(90), "{took:?}");
        assert!(
            took < window + Duration::from_millis(500),
            "bounded by the request's deadline, not the closes: {took:?}"
        );
    }
}
