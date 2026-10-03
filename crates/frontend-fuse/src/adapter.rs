//! `impl fuser::Filesystem for FuseFs<V>`: each callback decoded into an
//! [`OpCtx`](constellation_vfs::OpCtx) and a [`Vfs`] call, its reply wrapped
//! as the op's responder.
//!
//! Each op is begun by the session's [`Observer`] (plan 31 §6.10): it gets
//! its id and a `vfs.op` span, entered for the call, and its reply is
//! wrapped to count the op — outcome and latency — when it completes,
//! wherever that happens.
//!
//! No filesystem policy lives here — that is the view's, beneath the
//! trait. What does: the FUSE protocol. The kernel's flag words (`O_*`,
//! `FALLOC_FL_*`, `SEEK_*`, `XATTR_*`, `RENAME_*`, `F_*LCK`) become the
//! contract's decoded types, the kernel's device-number encoding becomes
//! [`Rdev`], a `SystemTime` becomes nanoseconds, and the `FUSE_INIT`
//! negotiation (the kernel queue sizing, parallel directory ops, and the
//! lock capabilities `--locks cluster` needs) happens here.
//!
//! Every op answers inline on the calling fuser worker, as it always did,
//! except a blocking lock, which the view completes from its `lock-wait`
//! thread, and a cold read (a chunk in no local cache), which the engine
//! completes from its completion pool (`FrontendCaps::linux_fuse` declares
//! every op deferrable; locks and cold reads defer so far — plan 31 §6.3,
//! C7b), and an `fsync`, which the engine finishes on its `fsync` pool
//! (plan 39 §3.3). All are counted ([`Deferred`]) for a session handover.
//!
//! `FUSE_INTERRUPT` (plan 39 §3.3, vendored fuser patch `interrupt`): an
//! `fsync`/`fsyncdir` and an `O_SYNC`/`O_DSYNC` write register their
//! request with [`Interrupts`] and carry the resulting [`CancelToken`] in
//! their context. The policy is NFS `hard`'s: **killable, not
//! interruptible**. An interrupt alone cancels nothing — the kernel sends
//! one for any signal, a timer's or a handled `SIGINT`'s, and an `fsync`
//! that answered `EINTR` to those would fail applications that never saw
//! an outage. It marks the request, and the token is cancelled once the
//! calling thread has a fatal signal pending (checked at once and then
//! every 100 ms while the request waits): the engine's wait ends, the
//! caller — which the kernel keeps waiting for this answer even after
//! `SIGKILL`, the request being already in userspace — is released to
//! die, and the data stays pending.
//!
//! A blocking lock (`F_SETLKW`, `F_OFD_SETLKW`, `flock` without `LOCK_NB`)
//! registers too, under the other policy: **interruptible**. POSIX ends
//! such a wait with `EINTR` on any signal, a handled one included, and
//! programs rely on it — an `alarm(2)` bounding a lock wait, a timer that
//! ends a worker (stress-ng's `lockf`/`lockmix`, whose processes otherwise
//! wait on each other for ever and stay unkillable, the request being in
//! userspace). Its interrupt cancels it at once; the wait answers `EINTR`
//! and holds nothing. Other ops ignore interrupts, as before.
//!
//! The interrupt can also never come. FUSE-over-io_uring (seen on Linux
//! 7.3) loses it when the signal arrives after the request is queued and
//! before the ring hands it to the daemon: the kernel marks the request
//! interrupted (its caller then waits killably, in `D`, with the signal
//! still pending) but sends no `FUSE_INTERRUPT` for it, then or later. So
//! a request that came over a ring does not rely on the interrupt alone:
//! once it has waited [`FATAL_POLL`] it is checked against its caller's
//! `/proc` state too — a lock wait whose caller has a deliverable signal
//! pending answers `EINTR`, and a killable request whose caller is dying
//! is released, both as if the interrupt had come. Over `/dev/fuse` the
//! kernel's interrupt is reliable and alone decides: the caller's pending
//! set includes process-directed signals (`ShdPnd`), which another thread
//! of the caller may be the one to take, and must not end this wait.

use crate::dentries::{KernelEntries, RenameGuard};
use crate::passthrough::{reason, BackingOps, PassthroughState, PassthroughWish, PreOpen};
use crate::reply::{
    AttrReply, BytesReply, CreateReply, DirReply, EmptyReply, EntryReply, LockReply, LseekReply,
    OpenCtx, OpenReply, ReadReply, RenameReply, StatfsReply, Undo, WriteReply, XattrListReply,
    XattrReply, F_RDLCK, F_UNLCK, F_WRLCK,
};
use constellation_types::{Code, Rdev};
use constellation_vfs::{
    Caller, CancelToken, Durability, FallocateMode, Fh, FrontendCaps, LockKind, LockOwner,
    LockRange, LockSpec, Name, Observer, OpKind, OpenFlags, OpenOwner, RenameFlags, Responder,
    SeekWhence, SetAttr, SetXattrFlags, TimeSet, Vfs, WriteData, XattrName,
};
use fuser::{
    BsdFileFlags, FileHandle, Filesystem, INodeNo, InitFlags, KernelConfig, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyLseek, ReplyOpen, ReplyWrite, ReplyXattr, Request,
    TimeOrNow, WriteFlags,
};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// The `frontend` label of this crate's op metrics and spans.
pub const FRONTEND: &str = "fuse";

/// What `FUSE_INIT` negotiates besides the capabilities: the kernel's
/// request queue, sized for the dispatcher's workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KernelTuning {
    pub max_background: u16,
    pub congestion_threshold: u16,
    /// `FUSE_PARALLEL_DIROPS`: more than one worker.
    pub parallel_dirops: bool,
}

impl KernelTuning {
    /// The tuning for a dispatcher of `workers` threads
    /// ([`crate::threads`]).
    pub fn for_workers(workers: usize) -> Self {
        Self {
            max_background: crate::threads::max_background(workers),
            congestion_threshold: crate::threads::congestion_threshold(workers),
            parallel_dirops: workers > 1,
        }
    }
}

/// The FUSE session's filesystem: a shared [`Vfs`] (the daemon keeps its
/// own handle to the view, for the lock path's flushes and the hold
/// writer), the frontend's capabilities, and the `FUSE_INIT` tuning.
pub struct FuseFs<V: Vfs> {
    vfs: Arc<V>,
    caps: FrontendCaps,
    tuning: KernelTuning,
    /// Op ids, the `vfs.op` span and the op metrics (plan 31 §6.10), for
    /// this session's view, labelled with the transport the session
    /// negotiated (plan 38 §5). That is only known once the handshake has
    /// run, after this filesystem was handed to fuser, so the session
    /// fills the slot right after `FUSE_INIT` ([`Self::observer_slot`]),
    /// before any request is served.
    obs: Arc<OnceLock<Observer>>,
    /// Requests answered from another thread and not answered yet (plan
    /// 31 §6.11: a handover drains them, or refuses).
    deferred: Arc<Deferred>,
    /// Plan 39 §3.3: the requests `FUSE_INTERRUPT` may cancel.
    interrupts: Arc<Interrupts>,
    /// Plan 38 Z3b: whether this session serves opens with
    /// `FOPEN_PASSTHROUGH`, and the per-inode state that keeps its
    /// replies within the kernel's rules (`passthrough`'s module doc).
    passthrough: Arc<PassthroughState>,
    /// The names the kernel can hold a valid dentry for: every reply that
    /// hands it one is recorded, so the notification sink writes entry
    /// invalidations only where they can drop something (`dentries`).
    entries: Arc<KernelEntries>,
}

impl<V: Vfs> FuseFs<V> {
    /// An adapter that does not ask for passthrough (a mount asks with
    /// [`Self::with_passthrough`]).
    pub fn new(vfs: Arc<V>, caps: FrontendCaps, tuning: KernelTuning) -> Self {
        Self {
            vfs,
            caps,
            tuning,
            obs: Arc::default(),
            deferred: Arc::default(),
            interrupts: Arc::default(),
            passthrough: PassthroughState::new(PassthroughWish::Off(reason::DISABLED)),
            entries: KernelEntries::new(),
        }
    }

    /// The names this connection's kernel can hold a valid dentry for, as
    /// the notification sink ([`crate::FuseNotifySink::new`]) consults
    /// them.
    pub fn kernel_entries(&self) -> &Arc<KernelEntries> {
        &self.entries
    }

    /// Ask for passthrough at `FUSE_INIT` (or not, and why).
    pub fn with_passthrough(self, wish: PassthroughWish) -> Self {
        self.passthrough.set_wish(wish);
        self
    }

    /// Passthrough with `ops` standing in for the backing-id ioctls and
    /// `CAP_SYS_ADMIN` assumed: what the wire tests drive, since a socket
    /// pair standing in for `/dev/fuse` has neither.
    #[doc(hidden)]
    pub fn with_passthrough_ops(mut self, ops: Box<dyn BackingOps>) -> Self {
        self.passthrough = PassthroughState::with_ops(PassthroughWish::On, ops, true);
        self
    }

    /// This session's passthrough state (`node.status`, the handover).
    pub fn passthrough(&self) -> &Arc<PassthroughState> {
        &self.passthrough
    }

    pub(crate) fn deferred(&self) -> &Arc<Deferred> {
        &self.deferred
    }

    /// Where the session puts this filesystem's [`Observer`] once it knows
    /// the negotiated transport.
    pub(crate) fn observer_slot(&self) -> Arc<OnceLock<Observer>> {
        self.obs.clone()
    }

    /// Replace how a caller's fatal (and any pending) signal is detected
    /// (tests; see [`fatal_signal_pending`], [`signal_pending`]).
    pub fn with_fatal_signal_probe(mut self, probe: fn(u32) -> bool) -> Self {
        // A dying thread has a signal pending; the test's other threads
        // have none (they may be anybody's real pids here).
        self.interrupts = Arc::new(Interrupts::with_probes(probe, probe));
        self
    }

    /// Whether this session's requests come over a ring, which may lose
    /// their interrupts (module doc).
    fn lossy(&self) -> bool {
        self.obs().metrics().transport() != fuser::Transport::DevFuse.name()
    }

    /// `fsync` and `fsyncdir` (see there).
    fn sync_barrier(&self, req: &Request, ino: INodeNo, fh: Fh, reply: ReplyEmpty) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Fsync, ino.0);
        let _in = op.enter();
        let unique = req.unique().0;
        let token = self.interrupts.register(unique, req.pid(), self.lossy());
        // Counted as a bounded deferral, like a cold read: a detach waits
        // for it (bounded) and refuses if it is still waiting for S3.
        let reply = op.responder(
            self.deferred
                .track_bounded(self.interrupts.guard(unique, EmptyReply(reply))),
        );
        self.vfs.fsync(
            &op.ctx(&caller).with_cancel(&token),
            ino.0,
            fh,
            Durability::Configured,
            reply,
        );
    }

    /// The session's observer. A filesystem served without a
    /// [`crate::FuseSession`] (the wire tests drive one over a socket
    /// pair) never has its slot filled and counts as `/dev/fuse`, which
    /// is what such a connection is.
    #[inline]
    fn obs(&self) -> &Observer {
        self.obs.get_or_init(|| {
            Observer::new(
                FRONTEND,
                &self.vfs.identity(),
                fuser::Transport::DevFuse.name(),
            )
        })
    }

    /// The view's side of a refused open, given back (`reply::Undo`).
    fn undo(&self, req: &Request, flags: OpenFlags) -> Undo {
        let vfs = self.vfs.clone();
        let caller = caller(req);
        Box::new(move |ino, fh: Fh| {
            struct Discard;
            impl constellation_vfs::Responder<()> for Discard {
                fn done(self, _: constellation_vfs::VfsResult<()>) {}
            }
            let cx = constellation_vfs::OpCtx::new(OpKind::Release, &caller);
            vfs.release(&cx, ino, fh, flags, None, Discard);
        })
    }
}

/// Whether an open with `flags` intends to change the file: what keeps a
/// passthrough open from starting while it is under way, and what the
/// engine's eligibility rule refuses (plan 38 §3(c)).
pub(crate) fn open_flags_write_intent(flags: OpenFlags) -> bool {
    flags.intersects(OpenFlags::WRITE | OpenFlags::TRUNC | OpenFlags::APPEND)
}

/// The requests whose reply may come from another thread after the fuser
/// worker that read them moved on, in two counts:
///
/// - **blocking**: the blocking lock waits (`FUSE_SETLKW`/`flock`), which
///   may wait forever. A detach refuses while any is in flight: a reply
///   must be written on the descriptor its request was read from, by a
///   process that still serves it.
/// - **bounded**: reads, which the engine may answer from its completion
///   pool when they are cold (plan 31 C7b), and `fsync`s, which it answers
///   from its `fsync` pool (plan 39). A read's wait ends on its own (a
///   fetch succeeds or fails); an `fsync` normally answers within its
///   barrier but waits out an S3 outage. A detach drains them for a
///   bounded time instead of refusing up front, and refuses if one is
///   still unanswered then (an `fsync` waiting for S3 — before plan 39 the
///   same `fsync` held its worker, and the detach waited for the worker).
///
/// Every other op answers inline, on the worker, before it reads again, so
/// stopping the workers drains those by itself.
#[derive(Default)]
pub(crate) struct Deferred {
    blocking: std::sync::atomic::AtomicUsize,
    bounded: std::sync::atomic::AtomicUsize,
}

impl Deferred {
    /// Blocking lock waits in flight.
    pub(crate) fn count(&self) -> usize {
        self.blocking.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Reads that may still be answered from the engine's pool.
    pub(crate) fn bounded(&self) -> usize {
        self.bounded.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn counter(&self, bounded: bool) -> &std::sync::atomic::AtomicUsize {
        if bounded {
            &self.bounded
        } else {
            &self.blocking
        }
    }

    fn track<R>(self: &Arc<Self>, reply: R) -> Tracked<R> {
        self.track_as(reply, false)
    }

    fn track_bounded<R>(self: &Arc<Self>, reply: R) -> Tracked<R> {
        self.track_as(reply, true)
    }

    fn track_as<R>(self: &Arc<Self>, reply: R, bounded: bool) -> Tracked<R> {
        self.counter(bounded)
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Tracked {
            reply: Some(reply),
            deferred: self.clone(),
            bounded,
        }
    }
}

#[cfg(test)]
impl Deferred {
    pub(crate) fn track_raw(&self) {
        self.blocking
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn untrack_raw(&self) {
        self.blocking
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn track_bounded_raw(&self) {
        self.bounded
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn untrack_bounded_raw(&self) {
        self.bounded
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
pub(crate) fn track_for_test<R: constellation_vfs::Responder<()>>(
    deferred: &Arc<Deferred>,
    reply: R,
) -> impl constellation_vfs::Responder<()> {
    deferred.track(reply)
}

/// A responder counted in [`Deferred`] until it has answered (or was
/// dropped, which answers `EIO`).
struct Tracked<R> {
    reply: Option<R>,
    deferred: Arc<Deferred>,
    bounded: bool,
}

impl<T, R: constellation_vfs::Responder<T>> constellation_vfs::Responder<T> for Tracked<R> {
    fn done(mut self, result: constellation_vfs::VfsResult<T>) {
        if let Some(reply) = self.reply.take() {
            reply.done(result);
        }
    }
}

impl<R> Drop for Tracked<R> {
    fn drop(&mut self) {
        // The reply (if `done` never ran) answers from its own drop first.
        drop(self.reply.take());
        self.deferred
            .counter(self.bounded)
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// How long an interrupt that names a request no handler has registered
/// yet is remembered. The kernel only interrupts a request it already
/// handed to the daemon, so the race is the time between a reader taking
/// the request and its handler registering it: microseconds on a
/// `/dev/fuse` worker, but over io_uring the request may queue for an
/// offload thread first (fuser's `dispatch_on_ring`), and the kernel sends
/// a request's interrupt only once. Linux request uniques are never reused
/// (`fuse_get_unique` only ever increments the counter), so a remembered
/// interrupt can only match the request it names; the time and count
/// bounds just bound the memory of interrupts for ops that never register.
const EARLY_INTERRUPT_KEPT: std::time::Duration = std::time::Duration::from_secs(60);
const EARLY_INTERRUPTS_MAX: usize = 4096;

/// How often an interrupted request's caller is checked for a fatal signal.
const FATAL_POLL: std::time::Duration = std::time::Duration::from_millis(100);

/// Whether the thread `tid` has a fatal signal pending — the kernel's own
/// `fatal_signal_pending()`, read from `/proc/<tid>/status`.
///
/// A FUSE request's `pid` is the caller's *thread* id (the kernel fills it
/// from `task_pid(current)`), and `/proc/<tid>/status` is that thread's
/// view: `SigPnd` its private pending set, `ShdPnd` the process's. When a
/// signal is going to kill the process — `SIGKILL`, or any signal whose
/// action is the default fatal one (an unhandled `SIGTERM`/`SIGINT`, …),
/// or another thread's `exit_group` — the kernel adds `SIGKILL` to every
/// thread's private set (`complete_signal`, `zap_other_threads`); that bit
/// is exactly what `fatal_signal_pending()` tests. A handled or blocked
/// signal never sets it, so those keep waiting. `ShdPnd`'s `SIGKILL` is
/// read too (a group kill not yet distributed).
///
/// Unknowable is "no": `pid` 0 (a request the kernel makes itself, or a
/// caller outside the mount's pid namespace) and a thread `/proc` does not
/// show (another pid namespace, `hidepid`) keep the request waiting as
/// `hard` does — the soft timeout and the kernel cap still apply.
pub fn fatal_signal_pending(tid: u32) -> bool {
    if tid == 0 {
        return false;
    }
    std::fs::read_to_string(format!("/proc/{tid}/status"))
        .is_ok_and(|status| status_has_sigkill_pending(&status))
}

fn status_has_sigkill_pending(status: &str) -> bool {
    let sigkill: u64 = 1 << (libc::SIGKILL - 1);
    status
        .lines()
        .filter_map(|l| {
            l.strip_prefix("SigPnd:")
                .or_else(|| l.strip_prefix("ShdPnd:"))
        })
        .filter_map(|mask| u64::from_str_radix(mask.trim(), 16).ok())
        .any(|mask| mask & sigkill != 0)
}

/// Whether the thread `tid` has a signal pending that it does not block —
/// what makes the kernel's `signal_pending()` true and ends an
/// interruptible wait: `(SigPnd | ShdPnd) & !SigBlk` in
/// `/proc/<tid>/status`. An ignored signal is never queued, so it never
/// shows. Unknowable (`tid` 0, a thread `/proc` does not show) is "no".
pub fn signal_pending(tid: u32) -> bool {
    if tid == 0 {
        return false;
    }
    std::fs::read_to_string(format!("/proc/{tid}/status"))
        .is_ok_and(|status| status_has_deliverable_signal(&status))
}

fn status_has_deliverable_signal(status: &str) -> bool {
    let mask = |key: &str| {
        status
            .lines()
            .find_map(|l| l.strip_prefix(key))
            .and_then(|m| u64::from_str_radix(m.trim(), 16).ok())
            .unwrap_or(0)
    };
    (mask("SigPnd:") | mask("ShdPnd:")) & !mask("SigBlk:") != 0
}

/// The requests that honour `FUSE_INTERRUPT`, by `unique` (see the module
/// doc).
pub(crate) struct Interrupts {
    state: std::sync::Mutex<InterruptState>,
    /// [`fatal_signal_pending`], or a test's stand-in.
    fatal: fn(u32) -> bool,
    /// [`signal_pending`], or a test's stand-in.
    pending: fn(u32) -> bool,
}

impl Default for Interrupts {
    fn default() -> Self {
        Self::with_probes(fatal_signal_pending, signal_pending)
    }
}

struct Waiting {
    token: CancelToken,
    /// The caller's thread.
    pid: u32,
    /// The kernel interrupted it: watched for a fatal signal.
    interrupted: bool,
    /// Any interrupt cancels it, a handled signal's too (a blocking
    /// lock wait; see the module doc).
    any_signal: bool,
    /// It came over a ring, which may lose its interrupt: past
    /// [`FATAL_POLL`] its caller is checked without one.
    lossy: bool,
    /// Registered at: past [`FATAL_POLL`], the caller's own state is
    /// checked even without an interrupt (see the module doc).
    since: std::time::Instant,
}

#[derive(Default)]
struct InterruptState {
    waiting: std::collections::HashMap<u64, Waiting>,
    /// Interrupts that overtook their request, oldest first.
    early: std::collections::VecDeque<(u64, std::time::Instant)>,
    /// A watcher thread is running.
    watcher: bool,
}

impl InterruptState {
    fn prune_early(&mut self) {
        let now = std::time::Instant::now();
        self.early
            .retain(|(_, at)| now.duration_since(*at) < EARLY_INTERRUPT_KEPT);
    }

    /// Interrupted, or over a ring, and not cancelled yet.
    fn watched(&self) -> bool {
        self.waiting
            .values()
            .any(|w| (w.interrupted || w.lossy) && !w.token.is_cancelled())
    }

    /// What [`Interrupts::probe`] checks now: each waiting request that is
    /// not cancelled, with whether its caller's death (interrupted, or
    /// over a ring and overdue) or any pending signal (a lock wait over a
    /// ring, overdue) cancels it.
    fn checks(&self) -> Vec<Check> {
        let now = std::time::Instant::now();
        self.waiting
            .values()
            .filter(|w| !w.token.is_cancelled())
            .filter_map(|w| {
                let overdue = w.lossy && now.duration_since(w.since) >= FATAL_POLL;
                let check = Check {
                    token: w.token.clone(),
                    pid: w.pid,
                    fatal: w.interrupted || overdue,
                    pending: w.any_signal && overdue,
                };
                (check.fatal || check.pending).then_some(check)
            })
            .collect()
    }
}

/// A waiting request to check against its caller's `/proc` state, read
/// outside the [`Interrupts`] lock.
struct Check {
    token: CancelToken,
    pid: u32,
    fatal: bool,
    pending: bool,
}

impl Interrupts {
    fn with_probes(fatal: fn(u32) -> bool, pending: fn(u32) -> bool) -> Self {
        Self {
            state: std::sync::Mutex::default(),
            fatal,
            pending,
        }
    }

    /// `unique`, from the thread `pid`, honours interrupts until its reply
    /// — killable: cancelled once its interrupted caller is dying. `lossy`:
    /// it came over a ring, which may lose the interrupt (module doc). The
    /// token to wait on.
    pub(crate) fn register(self: &Arc<Self>, unique: u64, pid: u32, lossy: bool) -> CancelToken {
        self.register_as(unique, pid, false, lossy)
    }

    /// [`Self::register`], interruptible: any interrupt of `unique`
    /// cancels it at once (a blocking lock wait).
    pub(crate) fn register_interruptible(
        self: &Arc<Self>,
        unique: u64,
        pid: u32,
        lossy: bool,
    ) -> CancelToken {
        self.register_as(unique, pid, true, lossy)
    }

    fn register_as(
        self: &Arc<Self>,
        unique: u64,
        pid: u32,
        any_signal: bool,
        lossy: bool,
    ) -> CancelToken {
        let token = CancelToken::new();
        let mut st = self.state.lock().unwrap();
        st.prune_early();
        let interrupted = match st.early.iter().position(|(u, _)| *u == unique) {
            Some(i) => {
                st.early.remove(i);
                true
            }
            None => false,
        };
        st.waiting.insert(
            unique,
            Waiting {
                token: token.clone(),
                pid,
                interrupted,
                any_signal,
                lossy,
                since: std::time::Instant::now(),
            },
        );
        if interrupted && any_signal {
            token.cancel();
        } else {
            self.watch(st);
        }
        token
    }

    pub(crate) fn unregister(&self, unique: u64) {
        self.state.lock().unwrap().waiting.remove(&unique);
    }

    /// The kernel interrupts `unique`.
    pub(crate) fn interrupt(self: &Arc<Self>, unique: u64) {
        let mut st = self.state.lock().unwrap();
        if let Some(w) = st.waiting.get_mut(&unique) {
            w.interrupted = true;
            if w.any_signal {
                w.token.cancel();
            } else {
                self.watch(st);
            }
            return;
        }
        st.prune_early();
        // Interrupts of ops that do not honour them land here too; the
        // bound keeps a signal storm from growing the list.
        if st.early.len() >= EARLY_INTERRUPTS_MAX {
            st.early.pop_front();
        }
        st.early.push_back((unique, std::time::Instant::now()));
    }

    /// Cancel every checked request whose caller is dying, or, for a
    /// lock wait over a ring, has a signal pending ([`InterruptState::checks`]).
    /// Reads `/proc`: never called under the lock.
    fn probe(&self, checks: Vec<Check>) {
        for c in checks {
            if c.fatal && (self.fatal)(c.pid) || c.pending && (self.pending)(c.pid) {
                c.token.cancel();
            }
        }
    }

    /// Check the watched requests now, and from a watcher thread every
    /// [`FATAL_POLL`] for as long as one of them is still waiting (the
    /// kernel interrupts a request once, or, over a ring, maybe never: the
    /// fatal signal may come later, the interrupt not at all).
    fn watch(self: &Arc<Self>, mut st: std::sync::MutexGuard<'_, InterruptState>) {
        let checks = st.checks();
        let spawn = !st.watcher && st.watched();
        st.watcher |= spawn;
        drop(st);
        self.probe(checks);
        if !spawn {
            return;
        }
        let this = Arc::downgrade(self);
        let spawned = std::thread::Builder::new()
            .name("fuse-interrupts".into())
            .spawn(move || loop {
                std::thread::sleep(FATAL_POLL);
                let Some(this) = this.upgrade() else {
                    return;
                };
                let mut st = this.state.lock().unwrap();
                let checks = st.checks();
                let done = !st.watched();
                st.watcher = !done;
                drop(st);
                this.probe(checks);
                if done {
                    return;
                }
            });
        if let Err(error) = spawned {
            tracing::warn!(%error, "could not start the FUSE interrupt watcher");
            self.state.lock().unwrap().watcher = false;
        }
    }

    fn guard<R>(self: &Arc<Self>, unique: u64, reply: R) -> Interruptible<R> {
        Interruptible {
            reply: Some(reply),
            interrupts: self.clone(),
            unique,
        }
    }
}

/// A responder whose request stops honouring interrupts once answered (or
/// dropped, which answers `EIO`).
struct Interruptible<R> {
    reply: Option<R>,
    interrupts: Arc<Interrupts>,
    unique: u64,
}

impl<T, R: constellation_vfs::Responder<T>> constellation_vfs::Responder<T> for Interruptible<R> {
    fn done(mut self, result: constellation_vfs::VfsResult<T>) {
        self.interrupts.unregister(self.unique);
        if let Some(reply) = self.reply.take() {
            reply.done(result);
        }
    }
}

impl<R> Drop for Interruptible<R> {
    fn drop(&mut self) {
        self.interrupts.unregister(self.unique);
    }
}

/// Who is asking: FUSE sends uid, gid and pid (0 for a request the
/// kernel makes on its own); the supplementary groups are read from the
/// host only if a check needs them (`constellation_vfs::Caller`).
fn caller(req: &Request) -> Caller {
    let pid = req.pid();
    Caller::new(req.uid(), req.gid(), (pid != 0).then_some(pid))
}

fn name(name: &OsStr) -> &Name {
    Name::new(name.as_bytes())
}

fn xattr_name(name: &OsStr) -> &XattrName {
    XattrName::new(name.as_bytes())
}

/// An open's flag word, decoded. The access mode reads as the kernel's
/// `O_ACCMODE` check does: read-only, write-only, or anything else as
/// both.
pub(crate) fn open_flags(raw: i32) -> OpenFlags {
    let mut flags = match raw & libc::O_ACCMODE {
        libc::O_RDONLY => OpenFlags::READ,
        libc::O_WRONLY => OpenFlags::WRITE,
        _ => OpenFlags::READ | OpenFlags::WRITE,
    };
    for (bit, flag) in [
        (libc::O_CREAT, OpenFlags::CREATE),
        (libc::O_EXCL, OpenFlags::EXCL),
        (libc::O_TRUNC, OpenFlags::TRUNC),
        (libc::O_APPEND, OpenFlags::APPEND),
        // `O_SYNC` includes `O_DSYNC`'s bit: either one publishes each
        // write before it returns.
        (libc::O_SYNC | libc::O_DSYNC, OpenFlags::SYNC),
    ] {
        if raw & bit != 0 {
            flags |= flag;
        }
    }
    flags
}

/// `fallocate`'s mode word, decoded; any bit the contract does not name
/// is kept as `UNSUPPORTED` (the view refuses it where it always did).
#[cfg(target_os = "linux")]
pub(crate) fn fallocate_mode(raw: i32) -> FallocateMode {
    let mut mode = FallocateMode::empty();
    let mut rest = raw;
    for (bit, flag) in [
        (libc::FALLOC_FL_KEEP_SIZE, FallocateMode::KEEP_SIZE),
        (libc::FALLOC_FL_PUNCH_HOLE, FallocateMode::PUNCH_HOLE),
        (libc::FALLOC_FL_ZERO_RANGE, FallocateMode::ZERO_RANGE),
    ] {
        if raw & bit != 0 {
            mode |= flag;
            rest &= !bit;
        }
    }
    if rest != 0 {
        mode |= FallocateMode::UNSUPPORTED;
    }
    mode
}

/// Without Linux's `FALLOC_FL_*`: only a plain allocation is nameable.
#[cfg(not(target_os = "linux"))]
pub(crate) fn fallocate_mode(raw: i32) -> FallocateMode {
    if raw == 0 {
        FallocateMode::empty()
    } else {
        FallocateMode::UNSUPPORTED
    }
}

/// `setxattr`'s flag word, decoded (both bits at once, or any other, is
/// the view's `EINVAL`).
pub(crate) fn setxattr_flags(raw: i32) -> SetXattrFlags {
    let mut flags = SetXattrFlags::empty();
    let mut rest = raw;
    for (bit, flag) in [
        (libc::XATTR_CREATE, SetXattrFlags::CREATE),
        (libc::XATTR_REPLACE, SetXattrFlags::REPLACE),
    ] {
        if raw & bit != 0 {
            flags |= flag;
            rest &= !bit;
        }
    }
    if rest != 0 {
        flags |= SetXattrFlags::UNSUPPORTED;
    }
    flags
}

/// `renameat2`'s flags, decoded (the view accepts and ignores them, as
/// the adapter always did).
pub(crate) fn rename_flags(raw: u32) -> RenameFlags {
    let mut flags = RenameFlags::empty();
    let mut rest = raw;
    #[cfg(target_os = "linux")]
    for (bit, flag) in [
        (libc::RENAME_NOREPLACE, RenameFlags::NOREPLACE),
        (libc::RENAME_EXCHANGE, RenameFlags::EXCHANGE),
        (libc::RENAME_WHITEOUT, RenameFlags::WHITEOUT),
    ] {
        if raw & bit != 0 {
            flags |= flag;
            rest &= !bit;
        }
    }
    if rest != 0 {
        flags |= RenameFlags::UNSUPPORTED;
    }
    flags
}

/// `lseek`'s whence. The kernel forwards only `SEEK_DATA`/`SEEK_HOLE`
/// (it answers the rest itself, and refuses a whence past `SEEK_MAX`).
pub(crate) fn seek_whence(raw: i32) -> Option<SeekWhence> {
    match raw {
        libc::SEEK_SET => Some(SeekWhence::Set),
        libc::SEEK_CUR => Some(SeekWhence::Cur),
        libc::SEEK_END => Some(SeekWhence::End),
        libc::SEEK_DATA => Some(SeekWhence::Data),
        libc::SEEK_HOLE => Some(SeekWhence::Hole),
        _ => None,
    }
}

/// A `setattr` time.
fn time_set(t: TimeOrNow) -> TimeSet {
    match t {
        TimeOrNow::Now => TimeSet::Now,
        TimeOrNow::SpecificTime(st) => TimeSet::At(
            st.duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0),
        ),
    }
}

fn lock_spec(owner: fuser::LockOwner, start: u64, end: u64, kind: LockKind, pid: u32) -> LockSpec {
    LockSpec {
        owner: LockOwner(owner.0),
        range: LockRange { start, end },
        kind,
        pid,
    }
}

impl<V: Vfs> Filesystem for FuseFs<V> {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        let _ = config.set_max_background(self.tuning.max_background);
        let _ = config.set_congestion_threshold(self.tuning.congestion_threshold);
        if self.tuning.parallel_dirops {
            // Older kernels may not advertise this capability. Multi-reader
            // dispatch still works; only directory operations remain ordered.
            let _ = config.add_capabilities(InitFlags::FUSE_PARALLEL_DIROPS);
        }
        // Plan 30 §M14: under `--locks cluster` the kernel hands POSIX and
        // `flock` locks to `getlk`/`setlk`; without the capabilities (and
        // on a frozen snapshot view, where nothing can be written — the
        // daemon declares no `cluster_locks` for one) it keeps them
        // node-local, as it always did.
        if self.caps.cluster_locks {
            if let Err(missing) =
                config.add_capabilities(InitFlags::FUSE_POSIX_LOCKS | InitFlags::FUSE_FLOCK_LOCKS)
            {
                tracing::warn!(
                    ?missing,
                    "the kernel does not offer FUSE lock forwarding; locks on this mount stay node-local"
                );
            }
        }
        // Plan 38 Z3b: passthrough only for a process that can register
        // backing files and a kernel that offers it; `max_stack_depth`
        // is what turns it on for the connection, so it stays 0
        // otherwise (and with it, the kernel's passthrough code is never
        // entered). The outcome is logged once and told to the view, which
        // offers backing files only when this said yes.
        let outcome = self.passthrough.want_from_kernel().and_then(|()| {
            config
                .add_capabilities(InitFlags::FUSE_PASSTHROUGH)
                .map_err(|_| reason::KERNEL)?;
            let _ = config.set_max_stack_depth(1);
            Ok(())
        });
        self.passthrough.settle(outcome);
        self.caps.passthrough = self.passthrough.enabled();
        self.vfs.frontend_negotiated(&self.caps);
        Ok(())
    }

    fn lookup(&self, req: &Request, parent: INodeNo, n: &OsStr, reply: ReplyEntry) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Lookup, parent.0);
        let _in = op.enter();
        let name_guard = self.entries.begin(parent.0, n.as_bytes());
        self.vfs.lookup(
            &op.ctx(&caller),
            parent.0,
            name(n),
            op.responder(EntryReply {
                reply,
                name: name_guard,
            }),
        );
    }

    fn getattr(&self, req: &Request, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Getattr, ino.0);
        let _in = op.enter();
        self.vfs.getattr(
            &op.ctx(&caller),
            ino.0,
            fh.map(|fh| Fh(fh.0)),
            op.responder(AttrReply(reply)),
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &self,
        req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let caller = caller(req);
        let set = SetAttr {
            mode,
            uid,
            gid,
            size,
            atime: atime.map(time_set),
            mtime: mtime.map(time_set),
        };
        let op = self.obs().begin(OpKind::Setattr, ino.0);
        let _in = op.enter();
        self.vfs.setattr(
            &op.ctx(&caller),
            ino.0,
            fh.map(|fh| Fh(fh.0)),
            &set,
            op.responder(AttrReply(reply)),
        );
    }

    fn readlink(&self, req: &Request, ino: INodeNo, reply: ReplyData) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Readlink, ino.0);
        let _in = op.enter();
        self.vfs
            .readlink(&op.ctx(&caller), ino.0, op.responder(BytesReply(reply)));
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        n: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Mkdir, parent.0);
        let _in = op.enter();
        let name_guard = self.entries.begin(parent.0, n.as_bytes());
        self.vfs.mkdir(
            &op.ctx(&caller),
            parent.0,
            name(n),
            mode,
            op.responder(EntryReply {
                reply,
                name: name_guard,
            }),
        );
    }

    fn mknod(
        &self,
        req: &Request,
        parent: INodeNo,
        n: &OsStr,
        mode: u32,
        _umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let caller = caller(req);
        // FUSE carries the kernel's 32-bit `new_encode_dev`; the contract
        // (and the journal) carry the portable pair (plan 31 §7).
        let rdev: Rdev = constellation_platform::from_linux_fuse_rdev(rdev);
        let op = self.obs().begin(OpKind::Mknod, parent.0);
        let _in = op.enter();
        let name_guard = self.entries.begin(parent.0, n.as_bytes());
        self.vfs.mknod(
            &op.ctx(&caller),
            parent.0,
            name(n),
            mode,
            rdev,
            op.responder(EntryReply {
                reply,
                name: name_guard,
            }),
        );
    }

    fn link(
        &self,
        req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Link, ino.0);
        let _in = op.enter();
        let name_guard = self.entries.begin(newparent.0, newname.as_bytes());
        self.vfs.link(
            &op.ctx(&caller),
            ino.0,
            newparent.0,
            name(newname),
            op.responder(EntryReply {
                reply,
                name: name_guard,
            }),
        );
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        n: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let caller = caller(req);
        let flags = open_flags(flags);
        let op = self.obs().begin(OpKind::Create, parent.0);
        let _in = op.enter();
        let cx = OpenCtx {
            ino: 0,
            flags,
            pre: PreOpen::Unregistered,
            pt: self.passthrough.clone(),
            undo: self.undo(req, flags),
        };
        let name_guard = self.entries.begin(parent.0, n.as_bytes());
        self.vfs.create(
            &op.ctx(&caller),
            parent.0,
            name(n),
            mode,
            flags,
            OpenOwner::NONE,
            op.responder(CreateReply {
                reply,
                cx,
                name: name_guard,
            }),
        );
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Symlink, parent.0);
        let _in = op.enter();
        let name_guard = self.entries.begin(parent.0, link_name.as_bytes());
        self.vfs.symlink(
            &op.ctx(&caller),
            parent.0,
            name(link_name),
            target.as_os_str().as_bytes(),
            op.responder(EntryReply {
                reply,
                name: name_guard,
            }),
        );
    }

    fn unlink(&self, req: &Request, parent: INodeNo, n: &OsStr, reply: ReplyEmpty) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Unlink, parent.0);
        let _in = op.enter();
        self.vfs.unlink(
            &op.ctx(&caller),
            parent.0,
            name(n),
            op.responder(EmptyReply(reply)),
        );
    }

    fn rmdir(&self, req: &Request, parent: INodeNo, n: &OsStr, reply: ReplyEmpty) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Rmdir, parent.0);
        let _in = op.enter();
        self.vfs.rmdir(
            &op.ctx(&caller),
            parent.0,
            name(n),
            op.responder(EmptyReply(reply)),
        );
    }

    fn rename(
        &self,
        req: &Request,
        parent: INodeNo,
        n: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: fuser::RenameFlags,
        reply: ReplyEmpty,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Rename, parent.0);
        let _in = op.enter();
        let flags = rename_flags(flags.bits());
        let names = RenameGuard::new(
            &self.entries,
            (parent.0, n.as_bytes()),
            (newparent.0, newname.as_bytes()),
        );
        self.vfs.rename(
            &op.ctx(&caller),
            parent.0,
            name(n),
            newparent.0,
            name(newname),
            flags,
            op.responder(RenameReply {
                reply,
                names,
                exchange: flags.contains(RenameFlags::EXCHANGE),
            }),
        );
    }

    fn open(&self, req: &Request, ino: INodeNo, flags: fuser::OpenFlags, reply: ReplyOpen) {
        let caller = caller(req);
        let flags = open_flags(flags.0);
        let op = self.obs().begin(OpKind::Open, ino.0);
        let _in = op.enter();
        // A read-write open of an inode open in passthrough mode is
        // refused (`ETXTBSY`) at the reply, after the view answered, so
        // the view's own refusals win (`passthrough`'s module doc).
        let pre = self.passthrough.before_open(ino.0, flags);
        let cx = OpenCtx {
            ino: ino.0,
            flags,
            pre,
            pt: self.passthrough.clone(),
            undo: self.undo(req, flags),
        };
        self.vfs.open(
            &op.ctx(&caller),
            ino.0,
            flags,
            OpenOwner::NONE,
            op.responder(OpenReply { reply, cx }),
        );
    }

    fn read(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: fuser::OpenFlags,
        lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Read, ino.0);
        let _in = op.enter();
        self.vfs.read(
            &op.ctx(&caller).with_lock_owner(lock_owner.map(|o| o.0)),
            ino.0,
            Fh(fh.0),
            offset,
            size,
            // A cold read may be answered from the engine's completion
            // pool: counted until it is, for a detach to drain.
            op.responder(self.deferred.track_bounded(ReadReply { reply, size })),
        );
    }

    fn write(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        flags: fuser::OpenFlags,
        lock_owner: Option<fuser::LockOwner>,
        reply: ReplyWrite,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Write, ino.0);
        let _in = op.enter();
        // Borrowed from wherever fuser received the request: the worker's
        // `/dev/fuse` buffer, or, over io_uring, the ring entry's own payload
        // buffer the kernel copied the bytes into — no copy into a worker
        // buffer either way (plan 38 §3(b)). The borrow ends when this call
        // returns, before the entry can be re-armed: fuser keeps the entry
        // fetched until the dispatch that received `data` is over
        // (`HeldRequest` in `vendor/fuser/src/uring/ring.rs`), and a view
        // that keeps the bytes past the call copies them into
        // `WriteData::Shared` itself.
        let flags = open_flags(flags.0);
        if !flags.contains(OpenFlags::SYNC) {
            self.vfs.write(
                &op.ctx(&caller).with_lock_owner(lock_owner.map(|o| o.0)),
                ino.0,
                Fh(fh.0),
                offset,
                WriteData::Borrowed(data),
                flags,
                op.responder(WriteReply(reply)),
            );
            return;
        }
        // Plan 39 §3.3: an `O_SYNC`/`O_DSYNC` write waits for durability
        // like an `fsync` — on the engine's `fsync` pool, so a bounded
        // deferral, and ended by its caller's death.
        let unique = req.unique().0;
        let token = self.interrupts.register(unique, req.pid(), self.lossy());
        let reply = op.responder(
            self.deferred
                .track_bounded(self.interrupts.guard(unique, WriteReply(reply))),
        );
        self.vfs.write(
            &op.ctx(&caller)
                .with_cancel(&token)
                .with_lock_owner(lock_owner.map(|o| o.0)),
            ino.0,
            Fh(fh.0),
            offset,
            WriteData::Borrowed(data),
            flags,
            reply,
        );
    }

    fn flush(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: fuser::LockOwner,
        reply: ReplyEmpty,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Flush, ino.0);
        let _in = op.enter();
        self.vfs.flush(
            &op.ctx(&caller),
            ino.0,
            Fh(fh.0),
            LockOwner(lock_owner.0),
            op.responder(EmptyReply(reply)),
        );
    }

    /// `fsync`/`fdatasync` (the engine has no cheaper data-only barrier:
    /// `datasync` is ignored). Plan 39: the engine may wait out an S3
    /// outage and answer from its `fsync` pool, so the reply is counted as
    /// a deferral, and an interrupt ends the wait with `EINTR`.
    fn fsync(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        self.sync_barrier(req, ino, Fh(fh.0), reply);
    }

    /// Plan 39 §3.6: the same barrier for a directory — its entries'
    /// mutations committed, the local store synced, and under
    /// `--fsync-mode s3` the journal shipped — so "fsync the directory
    /// after a create or rename" means what applications rely on it to
    /// mean. Before, fuser's default answered `ENOSYS`, and the kernel then
    /// answered every later directory `fsync` on the mount 0 without
    /// asking. `fh` is the `opendir` handle (always 0: the view keeps
    /// none for directories).
    fn fsyncdir(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        self.sync_barrier(req, ino, Fh(fh.0), reply);
    }

    /// CONSTELLATION PATCH (interrupt) in vendored fuser: see [`Interrupts`].
    fn interrupt(&self, _req: &Request, unique: fuser::RequestId) {
        // An `Arc` method: the first interrupt of a wait starts the
        // watcher, which holds the table weakly.
        self.interrupts.interrupt(unique.0);
    }

    fn release(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        flags: fuser::OpenFlags,
        lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Release, ino.0);
        let _in = op.enter();
        // The kernel has let go of the handle — its backing file
        // included — before it sends `RELEASE`; the inode's backing id
        // goes with its last passthrough handle.
        self.passthrough.release(ino.0);
        self.vfs.release(
            &op.ctx(&caller),
            ino.0,
            Fh(fh.0),
            open_flags(flags.0),
            lock_owner.map(|owner| LockOwner(owner.0)),
            op.responder(EmptyReply(reply)),
        );
    }

    fn readdir(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        reply: ReplyDirectory,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Readdir, ino.0);
        let _in = op.enter();
        self.vfs.readdir(
            &op.ctx(&caller),
            ino.0,
            Fh(fh.0),
            offset,
            false,
            op.responder(DirReply(reply)),
        );
    }

    fn setxattr(
        &self,
        req: &Request,
        ino: INodeNo,
        n: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        let op = self.obs().begin(OpKind::Setxattr, ino.0);
        if position != 0 {
            // macOS's resource-fork offset; a Linux kernel always sends 0.
            // Refused here, and counted like any other refusal.
            op.responder(EmptyReply(reply))
                .done(Err(Code::Invalid.into()));
            return;
        }
        let caller = caller(req);
        let _in = op.enter();
        self.vfs.setxattr(
            &op.ctx(&caller),
            ino.0,
            xattr_name(n),
            value,
            setxattr_flags(flags),
            op.responder(EmptyReply(reply)),
        );
    }

    fn getxattr(&self, req: &Request, ino: INodeNo, n: &OsStr, size: u32, reply: ReplyXattr) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Getxattr, ino.0);
        let _in = op.enter();
        self.vfs.getxattr(
            &op.ctx(&caller),
            ino.0,
            xattr_name(n),
            op.responder(XattrReply { reply, size }),
        );
    }

    fn listxattr(&self, req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Listxattr, ino.0);
        let _in = op.enter();
        self.vfs.listxattr(
            &op.ctx(&caller),
            ino.0,
            op.responder(XattrListReply { reply, size }),
        );
    }

    fn removexattr(&self, req: &Request, ino: INodeNo, n: &OsStr, reply: ReplyEmpty) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Removexattr, ino.0);
        let _in = op.enter();
        self.vfs.removexattr(
            &op.ctx(&caller),
            ino.0,
            xattr_name(n),
            op.responder(EmptyReply(reply)),
        );
    }

    fn statfs(&self, req: &Request, ino: INodeNo, reply: fuser::ReplyStatfs) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Statfs, ino.0);
        let _in = op.enter();
        self.vfs
            .statfs(&op.ctx(&caller), ino.0, op.responder(StatfsReply(reply)));
    }

    fn fallocate(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Fallocate, ino.0);
        let _in = op.enter();
        self.vfs.fallocate(
            &op.ctx(&caller),
            ino.0,
            Fh(fh.0),
            offset,
            length,
            fallocate_mode(mode),
            op.responder(EmptyReply(reply)),
        );
    }

    fn lseek(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: i64,
        whence: i32,
        reply: ReplyLseek,
    ) {
        // Refused here or not, the op is counted.
        let op = self.obs().begin(OpKind::Seek, ino.0);
        if offset < 0 {
            op.responder(LseekReply(reply))
                .done(Err(Code::NoDeviceOrAddress.into()));
            return;
        }
        let Some(whence) = seek_whence(whence) else {
            op.responder(LseekReply(reply))
                .done(Err(Code::Invalid.into()));
            return;
        };
        let caller = caller(req);
        let _in = op.enter();
        self.vfs.seek(
            &op.ctx(&caller),
            ino.0,
            Fh(fh.0),
            offset as u64,
            whence,
            op.responder(LseekReply(reply)),
        );
    }

    /// Plan 30 §M14: `F_GETLK` under `--locks cluster`. Under `--locks
    /// local` the kernel never asks.
    fn getlk(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: fuser::LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        reply: fuser::ReplyLock,
    ) {
        let caller = caller(req);
        // A write test asks about any lock; anything else (a read test,
        // `F_UNLCK`) about write locks only.
        let kind = if typ == F_WRLCK {
            LockKind::Write
        } else {
            LockKind::Read
        };
        let op = self.obs().begin(OpKind::LockTest, ino.0);
        let _in = op.enter();
        self.vfs.lock_test(
            &op.ctx(&caller),
            ino.0,
            Fh(fh.0),
            lock_spec(lock_owner, start, end, kind, pid),
            op.responder(LockReply(reply)),
        );
    }

    /// Plan 30 §M14: `F_SETLK`/`F_SETLKW`/`flock` under `--locks
    /// cluster`. Non-blocking requests are answered on this worker; the
    /// view answers a blocking one (`sleep`) from a thread of its own, so
    /// a contended lock never pins a FUSE worker. Interrupts are wired to
    /// `fsync` only ([`Interrupts`]): a blocked lock wait cannot be
    /// cancelled by a signal.
    fn setlk(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: fuser::LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        let caller = caller(req);
        let kind = match typ {
            F_UNLCK => {
                let op = self.obs().begin(OpKind::LockRelease, ino.0);
                let _in = op.enter();
                self.vfs.lock_release(
                    &op.ctx(&caller),
                    ino.0,
                    Fh(fh.0),
                    LockOwner(lock_owner.0),
                    LockRange { start, end },
                    op.responder(EmptyReply(reply)),
                );
                return;
            }
            F_RDLCK => LockKind::Read,
            F_WRLCK => LockKind::Write,
            // Not reachable from the kernel, which validates `l_type`
            // (and builds `flock`'s itself) before asking.
            _ => {
                self.obs()
                    .begin(OpKind::LockAcquire, ino.0)
                    .responder(EmptyReply(reply))
                    .done(Err(Code::Invalid.into()));
                return;
            }
        };
        let op = self.obs().begin(OpKind::LockAcquire, ino.0);
        let _in = op.enter();
        let spec = lock_spec(lock_owner, start, end, kind, pid);
        if sleep {
            // May answer from the view's `lock-wait` thread: counted, and
            // counted as an op when that thread answers. Interruptible
            // (the module doc): a signal ends the wait with `EINTR`.
            let unique = req.unique().0;
            let token = self
                .interrupts
                .register_interruptible(unique, req.pid(), self.lossy());
            let reply = op.responder(
                self.deferred
                    .track(self.interrupts.guard(unique, EmptyReply(reply))),
            );
            let cx = op.ctx(&caller).with_cancel(&token);
            self.vfs
                .lock_acquire(&cx, ino.0, Fh(fh.0), spec, true, reply);
        } else {
            let cx = op.ctx(&caller);
            self.vfs.lock_acquire(
                &cx,
                ino.0,
                Fh(fh.0),
                spec,
                false,
                op.responder(EmptyReply(reply)),
            );
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use constellation_vfs::types::mode;

    /// The stand-in caller: thread 66 is dying (from the moment
    /// [`DYING`] is set), every other thread lives.
    static DYING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    fn probe(pid: u32) -> bool {
        pid == 66 && DYING.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Killable, not interruptible (plan 39 §3.3): an interrupt cancels
    /// what it names only once its caller is dying — at once, or when the
    /// fatal signal comes later; early or late; nothing else.
    #[test]
    fn interrupts_cancel_only_what_they_name_and_only_for_a_dying_caller() {
        let i = Arc::new(Interrupts::with_probes(probe, probe));
        let living = i.register(10, 55, false);
        i.interrupt(10);
        std::thread::sleep(FATAL_POLL * 3);
        assert!(!living.is_cancelled(), "a handled signal keeps waiting");
        i.unregister(10);
        let doomed = i.register(12, 66, false);
        i.interrupt(11);
        assert!(!doomed.is_cancelled(), "another request's interrupt");
        i.interrupt(12);
        assert!(!doomed.is_cancelled(), "not dying yet");
        DYING.store(true, std::sync::atomic::Ordering::SeqCst);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !doomed.is_cancelled() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(doomed.is_cancelled(), "the fatal signal came later");
        i.unregister(12);
        // Overtaken: the interrupt came before its request registered.
        i.interrupt(13);
        assert!(
            i.register(13, 66, false).is_cancelled(),
            "an early interrupt"
        );
        i.unregister(13);
        assert!(
            !i.register(14, 66, false).is_cancelled(),
            "never interrupted"
        );
        i.unregister(14);
        i.interrupt(15);
        assert!(
            !i.register(15, 55, false).is_cancelled(),
            "early, the caller lives"
        );
        i.unregister(15);
        assert!(!i.state.lock().unwrap().watched());
    }

    /// A blocking lock wait is interruptible: any interrupt cancels it at
    /// once (POSIX's `EINTR` from `F_SETLKW`), early or late, a living
    /// caller's too; it never needs the watcher.
    #[test]
    fn an_interruptible_request_is_cancelled_by_any_interrupt() {
        let i = Arc::new(Interrupts::with_probes(probe, probe));
        let lock = i.register_interruptible(20, 55, false);
        let fsync = i.register(21, 55, false);
        assert!(!lock.is_cancelled());
        i.interrupt(21);
        assert!(!lock.is_cancelled(), "another request's interrupt");
        i.interrupt(20);
        assert!(lock.is_cancelled(), "a handled signal ends a lock wait");
        assert!(!fsync.is_cancelled(), "but not an fsync");
        i.unregister(20);
        i.unregister(21);
        i.interrupt(22);
        assert!(
            i.register_interruptible(22, 55, false).is_cancelled(),
            "an early interrupt"
        );
        i.unregister(22);
        assert!(!i.state.lock().unwrap().watched());
    }

    /// This test's own dying thread, 88 (tests run in parallel: [`DYING`]
    /// is the other test's), and thread 77, which has a handled signal
    /// pending and is not dying.
    static DYING_88: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    fn fatal_probe(pid: u32) -> bool {
        pid == 88 && DYING_88.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn pending_probe(pid: u32) -> bool {
        pid == 77 || fatal_probe(pid)
    }

    /// The interrupt may never come (FUSE-over-io_uring loses it for a
    /// request interrupted before the ring delivered it): past
    /// [`FATAL_POLL`] a ring's lock wait whose caller has a signal pending is
    /// cancelled anyway, and so is a killable request whose caller is dying
    /// — but not one whose caller merely has a handled signal pending.
    #[test]
    fn a_lost_interrupt_is_made_up_for_from_the_callers_state() {
        let i = Arc::new(Interrupts::with_probes(fatal_probe, pending_probe));
        let lock = i.register_interruptible(30, 77, true);
        let calm_lock = i.register_interruptible(31, 55, true);
        let fsync = i.register(32, 77, true);
        assert!(!lock.is_cancelled(), "not before FATAL_POLL");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !lock.is_cancelled() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(lock.is_cancelled(), "a signal pending, no interrupt");
        std::thread::sleep(FATAL_POLL * 3);
        assert!(!calm_lock.is_cancelled(), "no signal pending");
        assert!(
            !fsync.is_cancelled(),
            "a handled signal does not end an fsync"
        );
        DYING_88.store(true, std::sync::atomic::Ordering::SeqCst);
        let dying = i.register(33, 88, true);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !dying.is_cancelled() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(dying.is_cancelled(), "a dying caller, no interrupt");
        for u in 30..=33 {
            i.unregister(u);
        }
        assert!(!i.state.lock().unwrap().watched());
    }

    /// Over `/dev/fuse` the kernel's interrupt alone decides: a lock wait
    /// whose caller shows a signal pending — a process-directed one,
    /// say, that another of its threads handles — keeps waiting, and so
    /// does a killable request of a dying caller the kernel has not
    /// interrupted; neither needs the watcher.
    #[test]
    fn over_dev_fuse_only_the_kernels_interrupt_cancels() {
        let i = Arc::new(Interrupts::with_probes(fatal_probe, pending_probe));
        DYING_88.store(true, std::sync::atomic::Ordering::SeqCst);
        let lock = i.register_interruptible(40, 77, false);
        let fsync = i.register(41, 88, false);
        assert!(!i.state.lock().unwrap().watched(), "nothing to poll");
        std::thread::sleep(FATAL_POLL * 3);
        assert!(!lock.is_cancelled(), "another thread's signal");
        assert!(!fsync.is_cancelled(), "no interrupt yet");
        i.interrupt(41);
        assert!(fsync.is_cancelled(), "interrupted, and dying");
        i.interrupt(40);
        assert!(lock.is_cancelled(), "its own interrupt");
        i.unregister(40);
        i.unregister(41);
    }

    #[test]
    fn a_deliverable_signal_is_pending_and_not_blocked() {
        let status = |sig: &str, shd: &str, blk: &str| {
            format!("Name:\tpg\nSigPnd:\t{sig}\nShdPnd:\t{shd}\nSigBlk:\t{blk}\nSigIgn:\t0\n")
        };
        let none = "0000000000000000";
        assert!(!status_has_deliverable_signal(&status(none, none, none)));
        // SIGALRM (14) pending, privately or for the process.
        assert!(status_has_deliverable_signal(&status(
            "0000000000002000",
            none,
            none
        )));
        assert!(status_has_deliverable_signal(&status(
            none,
            "0000000000002000",
            none
        )));
        // ... but blocked: not deliverable.
        assert!(!status_has_deliverable_signal(&status(
            none,
            "0000000000002000",
            "0000000000002000"
        )));
        assert!(!signal_pending(0));
    }

    #[test]
    fn a_pending_sigkill_is_read_from_the_threads_status() {
        let status = |sig: &str, shd: &str| {
            format!("Name:\tpg\nSigQ:\t0/1\nSigPnd:\t{sig}\nShdPnd:\t{shd}\nSigBlk:\t0000000000000000\n")
        };
        assert!(!status_has_sigkill_pending(&status(
            "0000000000000000",
            "0000000000000000"
        )));
        // SIGINT (2) and SIGALRM (14) pending: not fatal by themselves.
        assert!(!status_has_sigkill_pending(&status(
            "0000000000002002",
            "0000000000000000"
        )));
        assert!(status_has_sigkill_pending(&status(
            "0000000000000100",
            "0000000000000000"
        )));
        assert!(status_has_sigkill_pending(&status(
            "0000000000000000",
            "0000000000000100"
        )));
        // This thread is alive and has nothing pending; thread 0 is unknowable.
        assert!(!fatal_signal_pending(std::process::id()));
        assert!(!fatal_signal_pending(0));
    }

    #[test]
    fn open_flags_decode_the_access_mode_as_the_kernel_does() {
        let rw = OpenFlags::READ | OpenFlags::WRITE;
        assert_eq!(open_flags(libc::O_RDONLY), OpenFlags::READ);
        assert_eq!(open_flags(libc::O_WRONLY), OpenFlags::WRITE);
        assert_eq!(open_flags(libc::O_RDWR), rw);
        assert_eq!(
            open_flags(libc::O_ACCMODE),
            rw,
            "an invalid mode reads as both"
        );
        assert_eq!(
            open_flags(libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC),
            rw | OpenFlags::CREATE | OpenFlags::EXCL | OpenFlags::TRUNC
        );
        for sync in [libc::O_SYNC, libc::O_DSYNC] {
            assert!(open_flags(libc::O_WRONLY | sync).contains(OpenFlags::SYNC));
        }
        assert!(!open_flags(libc::O_WRONLY | libc::O_APPEND).contains(OpenFlags::SYNC));
        assert!(open_flags(libc::O_WRONLY | libc::O_APPEND).contains(OpenFlags::APPEND));
    }

    #[test]
    fn fallocate_setxattr_and_rename_flags_keep_what_they_cannot_name() {
        assert_eq!(fallocate_mode(0), FallocateMode::empty());
        assert_eq!(
            fallocate_mode(libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE),
            FallocateMode::PUNCH_HOLE | FallocateMode::KEEP_SIZE
        );
        assert_eq!(
            fallocate_mode(libc::FALLOC_FL_ZERO_RANGE),
            FallocateMode::ZERO_RANGE
        );
        assert!(fallocate_mode(libc::FALLOC_FL_COLLAPSE_RANGE).contains(FallocateMode::UNSUPPORTED));
        assert_eq!(setxattr_flags(0), SetXattrFlags::empty());
        assert_eq!(setxattr_flags(libc::XATTR_CREATE), SetXattrFlags::CREATE);
        assert_eq!(setxattr_flags(libc::XATTR_REPLACE), SetXattrFlags::REPLACE);
        assert!(setxattr_flags(libc::XATTR_CREATE | libc::XATTR_REPLACE)
            .mode()
            .is_err());
        assert!(setxattr_flags(8).contains(SetXattrFlags::UNSUPPORTED));
        assert!(setxattr_flags(8).mode().is_err());
        assert_eq!(rename_flags(0), RenameFlags::empty());
        assert_eq!(rename_flags(libc::RENAME_NOREPLACE), RenameFlags::NOREPLACE);
        assert!(rename_flags(1 << 20).contains(RenameFlags::UNSUPPORTED));
    }

    #[test]
    fn whence_and_lock_types_and_mode_bits_match_the_kernels() {
        assert_eq!(seek_whence(libc::SEEK_DATA), Some(SeekWhence::Data));
        assert_eq!(seek_whence(libc::SEEK_HOLE), Some(SeekWhence::Hole));
        assert_eq!(seek_whence(libc::SEEK_SET), Some(SeekWhence::Set));
        assert_eq!(seek_whence(99), None);
        assert_ne!(F_RDLCK, F_WRLCK);
        assert_ne!(F_WRLCK, F_UNLCK);
        // The contract's POSIX type bits are the kernel's.
        assert_eq!(mode::S_IFMT, libc::S_IFMT);
        assert_eq!(mode::S_IFREG, libc::S_IFREG);
        assert_eq!(mode::S_IFDIR, libc::S_IFDIR);
        assert_eq!(mode::S_IFLNK, libc::S_IFLNK);
        assert_eq!(mode::S_IFIFO, libc::S_IFIFO);
        assert_eq!(mode::S_IFSOCK, libc::S_IFSOCK);
        assert_eq!(mode::S_IFBLK, libc::S_IFBLK);
        assert_eq!(mode::S_IFCHR, libc::S_IFCHR);
    }

    #[test]
    fn setattr_times_are_nanoseconds_or_the_engines_now() {
        assert_eq!(time_set(TimeOrNow::Now), TimeSet::Now);
        let t = UNIX_EPOCH + std::time::Duration::from_nanos(1_234_567_891);
        assert_eq!(
            time_set(TimeOrNow::SpecificTime(t)),
            TimeSet::At(1_234_567_891)
        );
        let before = UNIX_EPOCH - std::time::Duration::from_secs(1);
        assert_eq!(time_set(TimeOrNow::SpecificTime(before)), TimeSet::At(0));
    }

    #[test]
    fn kernel_tuning_scales_with_the_workers() {
        let one = KernelTuning::for_workers(1);
        assert_eq!(
            (
                one.max_background,
                one.congestion_threshold,
                one.parallel_dirops
            ),
            (16, 12, false)
        );
        let twelve = KernelTuning::for_workers(12);
        assert_eq!((twelve.max_background, twelve.parallel_dirops), (96, true));
    }
}
