//! Plan 30 §M14: `--locks local|cluster` — cross-node `flock`/`fcntl`.
//!
//! The protocol lives in `constellation_authority::core` (its `locks`
//! module doc: grants leased from the owning sequencer, recalls, renewals,
//! fencing) and the tables in `constellation_meta::locks` (shared by the
//! core and the FUSE threads). This module is the daemon's side of it:
//! the mount option, the FUSE-facing request loop ([`ClusterLocks`]), the
//! flush the core asks for before a recalled grant is released
//! ([`LockFlushers`]), and the conversions between the core's messages
//! and their P2P wire form.
//!
//! Plan 31: the FUSE reply plumbing (`fuser::ReplyEmpty`/`ReplyLock`,
//! the `F_RDLCK`/`F_WRLCK`/`F_UNLCK` lock types) is the frontend's
//! (`constellation-frontend-fuse`); the view's `lock_test`/`lock_acquire`/
//! `lock_release` ops (`view::ops`) call in here. [`ClusterLocks::lock`]
//! completes through a callback that completes the op's
//! `constellation_vfs::Responder`, and [`ClusterLocks::test`] speaks
//! `write: bool`.
//!
//! - `--locks local`: today's behaviour. The FUSE mount does not
//!   negotiate `FUSE_POSIX_LOCKS`/`FUSE_FLOCK_LOCKS`, so the kernel keeps
//!   every lock node-local and never asks the daemon. Two nodes can both
//!   "hold" an exclusive lock on the same file.
//! - `--locks cluster` (the default with P2P): a lock taken on one node
//!   excludes conflicting locks on every other node. The first lock on a
//!   file costs one round trip to its sequencer; the grant is then cached
//!   (an uncontended re-lock costs no message) until another node wants a
//!   conflicting one. Data written under a lock is flushed through before
//!   the grant moves, and the next holder waits for it and drops its
//!   kernel cache of the file, so lock-protected read-modify-write works
//!   across nodes. A node whose grant lapsed (partitioned from the
//!   sequencer past the ttl) fails I/O on the files it holds locks on
//!   with `EIO` until they are unlocked (NFSv4's rule), and the lock's
//!   owner — its process and the processes it started — gets `EIO` from
//!   every write and namespace op on the mount meanwhile
//!   ([`ClusterLocks::owner_fenced`]). Every mutation an owner issues
//!   carries the fencing token of its grants ([`current_tag`]), so an op
//!   already on its way when the grant lapsed is refused where it lands.
//! - Without P2P (`CONSTELLATION_P2P=off`, or an endpoint that could not
//!   start) the effective mode is `local`: the inbox is not a lock path.
//!   An explicit `--locks cluster` then fails the mount.
//! - `CONSTELLATION_LOCKS` supplies the default when the flag is absent.
//! - `CONSTELLATION_LOCK_TTL_MS` (default 20000): a grant's lifetime,
//!   renewed in the background while held.
//! - `CONSTELLATION_LOCK_CACHE_IDLE_MS` (default 30000): how long an
//!   unused cached grant is kept before it is released.
//! - `CONSTELLATION_LOCK_CLOSE_WAIT_MS` (default 100): how long a
//!   non-blocking request waits for what a just-closed description left
//!   behind ([`close_wait`]).
//!
//! A blocked lock wait ends with `EINTR` when the frontend cancels it (the
//! FUSE adapter does on any signal, as POSIX has `F_SETLKW` do), and with
//! `ETIMEDOUT` when the op's deadline (`OpCtx::deadline`, set by a
//! frontend that bounds its ops: none in the tree today, the conformance
//! kit does) passes first:
//! both are checked between the polls of a wait on a conflicting lock of
//! this node, before each request to the sequencer, and while the core's
//! answer to one is awaited (in slices of 50 ms: the core may spend seconds
//! on an owner that does not answer). A request the
//! owner has parked (another node holds a conflicting grant) is not
//! withdrawn: that wait still returns only once the grant comes.
//!
//! A non-blocking request on a lock nothing conflicts with, which its owner
//! may not grant yet (a grace after a takeover or a restart, the tenure's
//! floor, a barrier, a delegate's short authority; plan 43 F-b), waits
//! instead of failing with `EAGAIN`: the core answers `Wait { retry_ms }`
//! and the retries run on a bounded pool of `lock-wait` threads
//! ([`ClusterLocks::lock_nb_deferred`], [`WaitPool`]: never a frontend
//! worker, nor an offload thread of an io_uring ring, which a wait of tens
//! of seconds would stall for every other request offloaded to it). A
//! frontend that cannot answer from another thread waits inline. The
//! frontend is asked first (`Responder::begin_wait`): it registers the wait
//! for the caller's signal and, over a ring, reserves one of its queue's
//! `depth - 1` lock-wait entries, since a waiting request **holds its ring
//! entry** until it is answered; with none to give (or no pool thread to
//! start) the request is answered now as before F-b, `EAGAIN`, counted
//! (`grace_wait_refused_ring`, `grace_wait_refused_pool`). The wait ends
//! with the grant, with `EAGAIN` when the table then shows a conflict or
//! once [`ClusterLocks::owner_wait_max`] has passed since the request
//! (counted: `grace_wait_gave_up`), and with `EINTR` when the frontend
//! cancels it.
//!
//! Not supported: POSIX deadlock detection (`EDEADLK`) — two owners
//! waiting on each other wait until a signal ends one wait, as they do
//! with `flock`.

use crate::sync::SyncRequest;
use anyhow::{bail, Result};
use constellation_authority::{
    LockAnswer, LockOutcome, LockRenewEntry, LockRenewResult, LockTestAnswer, LockTestOutcome, Now,
};
use constellation_fs_core::Ino;
use constellation_meta::locks::{
    Discard, Fence, GrantId, GrantWire, LocalLock, LocalOutcome, LockMode, LockTag, OwnerFenced,
};
use constellation_meta::{JournalPos, Meta, Position, ReadKey};
use constellation_net::{LockOutcomeWire, LockRenewResultWire, LockRenewWire, LockTestOutcomeWire};
use constellation_types::Code;
use constellation_vfs::CancelToken;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

/// The error an explicit `--locks cluster` gets without P2P.
pub const NEEDS_P2P: &str = "cluster locks need P2P; use --locks local";

/// `--locks` (or `CONSTELLATION_LOCKS`): `Some(true)` cluster,
/// `Some(false)` local, `None` when neither says (cluster with P2P).
pub fn cluster_flag(flag: Option<&str>) -> Result<Option<bool>> {
    let env = std::env::var("CONSTELLATION_LOCKS").ok();
    let Some(raw) = flag.or(env.as_deref()) else {
        return Ok(None);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "" => Ok(None),
        "local" => Ok(Some(false)),
        "cluster" => Ok(Some(true)),
        other => bail!("invalid --locks {other:?} (expected local or cluster)"),
    }
}

/// The effective mode: cluster unless asked for local or P2P is off; an
/// explicit cluster without P2P is an error.
pub fn cluster_effective(flag: Option<bool>, p2p: bool) -> Result<bool> {
    match flag {
        Some(true) if !p2p => bail!(NEEDS_P2P),
        Some(v) => Ok(v),
        None => Ok(p2p),
    }
}

fn env_ms(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

/// `CONSTELLATION_LOCK_TTL_MS` (default 20000).
pub fn lock_ttl_ms(default: u64) -> u64 {
    env_ms("CONSTELLATION_LOCK_TTL_MS", default)
}

/// `CONSTELLATION_LOCK_CACHE_IDLE_MS` (default 30000).
pub fn lock_cache_idle_ms(default: u64) -> u64 {
    env_ms("CONSTELLATION_LOCK_CACHE_IDLE_MS", default)
}

/// `CONSTELLATION_LOCK_CLOSE_WAIT_MS` (default 100; `0`: no wait): how
/// long a non-blocking request (or a test) refused by what a just-closed
/// open file description leaves behind may wait for it to go — the
/// description's own locks, which the kernel's `RELEASE` drops after
/// `close(2)` returned, at most this long from the close (the view's
/// `lock_handles`), and a close's publication of the file still in flight
/// under the grant or an op of the earlier turn left in doubt
/// (`LocalOutcome::Predecessor`; a pin can outlast the wait, which then
/// answers `EAGAIN`) — and never more than this long from the request
/// itself, however many closes follow. Far above
/// the lag measured on a loaded host (p99 2–4 ms), and short enough that
/// a request behind a lock whose description stays open (a `dup`'s
/// close) is not held up for long.
pub fn close_wait() -> Duration {
    static WAIT: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *WAIT.get_or_init(|| {
        let ms = std::env::var("CONSTELLATION_LOCK_CLOSE_WAIT_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(100);
        Duration::from_millis(ms)
    })
}

/// The protocol clock's three readings of one instant (plan 43 C7-H1):
/// grants are honoured on its honour clock, fencing tokens are minted from
/// it on the wall clock and the wait for them to run out is on its wait
/// clock. Tests move it per thread ([`advance_test_clock`],
/// [`step_test_wall`]) instead of sleeping past a grant.
pub(crate) fn now() -> Now {
    let now = constellation_platform::clock::protocol().now();
    #[cfg(test)]
    let now = TEST_CLOCK.with(|c| c.get().apply(now));
    now
}

/// The wait reading alone: one host clock, where [`now`] reads four (the
/// park gates' hot path, [`constellation_meta::locks::LockTables::may_park_est`]).
pub(crate) fn wait_now() -> constellation_authority::WaitMs {
    let wait = constellation_platform::clock::protocol().wait();
    #[cfg(test)]
    let wait = constellation_authority::WaitMs(wait.0 + TEST_CLOCK.with(|c| c.get().wait));
    wait
}

/// Per-thread test offsets of the three readings.
#[cfg(test)]
#[derive(Clone, Copy, Default)]
struct TestClock {
    wall: i64,
    honour: i64,
    wait: i64,
}

#[cfg(test)]
impl TestClock {
    fn apply(self, now: Now) -> Now {
        Now {
            wall: constellation_authority::Ms(now.wall.0 + self.wall),
            honour: constellation_authority::HonourMs(now.honour.0 + self.honour),
            wait: constellation_authority::WaitMs(now.wait.0 + self.wait),
            boot: now.boot,
        }
    }
}

#[cfg(test)]
thread_local! {
    static TEST_CLOCK: std::cell::Cell<TestClock> = std::cell::Cell::new(TestClock::default());
}

/// Time passes on this thread's [`now`]: all three readings move forward
/// (tests only; not a step).
#[cfg(test)]
pub(crate) fn advance_test_clock(ms: i64) {
    TEST_CLOCK.with(|c| {
        let mut t = c.get();
        t.wall += ms;
        t.honour += ms;
        t.wait += ms;
        c.set(t);
    });
}

/// A wall-clock step on this thread's [`now`] (tests only): the wall reading
/// moves by `ms`; the honour reading follows a forward step and ignores a
/// backward one (P4), the wait reading never moves.
#[cfg(test)]
pub(crate) fn step_test_wall(ms: i64) {
    TEST_CLOCK.with(|c| {
        let mut t = c.get();
        t.wall += ms;
        if ms > 0 {
            t.honour += ms;
        }
        c.set(t);
    });
}

/// The process of task `pid` (FUSE names a request by its thread),
/// named across pid reuse: `(thread group id, its identity)`, the
/// platform's `Lineage::start` (never a read that waits for an `execve(2)`
/// the caller is in, where the host has pidfs: a `FLUSH` can come from
/// there) — `(pid, 0)` where `/proc` cannot tell (gone, or no `/proc`).
pub fn process_of(pid: u32) -> (u32, u64) {
    if pid == 0 {
        return (0, 0);
    }
    let process = &constellation_platform::native().process;
    match process.lineage(pid) {
        Ok(l) if l.tgid == pid => (pid, l.start),
        Ok(l) => (l.tgid, process.lineage(l.tgid).map_or(0, |p| p.start)),
        Err(_) => (pid, 0),
    }
}

/// Whether two `(pid, start time)` name the same process (an unknown
/// start time matches by pid alone).
fn same_process(a: (u32, u64), b: (u32, u64)) -> bool {
    a.0 == b.0 && (a.1 == 0 || b.1 == 0 || a.1 == b.1)
}

/// The process of task `pid` (whose own `/proc` entry is `task`) and
/// its ancestors, nearest first, as `(pid, start time)`: up the parent
/// chain through `/proc`, at most [`LINEAGE_DEPTH`] levels. Without
/// `/proc` (or the task gone) just `(pid, 0)`, which matches by pid alone.
fn ancestry_of(
    pid: u32,
    task: Option<constellation_platform::process::Lineage>,
) -> Vec<(u32, u64)> {
    let process = &constellation_platform::native().process;
    let Some(t) = task else {
        return vec![(pid, 0)];
    };
    let mut chain = Vec::new();
    let mut proc = if t.tgid == pid {
        (pid, t.start)
    } else {
        (t.tgid, process.lineage(t.tgid).map_or(0, |p| p.start))
    };
    let mut ppid = t.ppid;
    for _ in 0..LINEAGE_DEPTH {
        chain.push(proc);
        if ppid <= 1 {
            break;
        }
        let Ok(l) = process.lineage(ppid) else {
            break;
        };
        proc = (ppid, l.start);
        ppid = l.ppid;
    }
    chain
}

/// The largest offset the kernel accepts in a lock reply (`OFFSET_MAX`);
/// anything past it is answered `EIO` by `convert_fuse_file_lock`.
const OFFSET_MAX: u64 = i64::MAX as u64;

/// How often a blocked request re-checks a conflicting local lock (local
/// conflicts wake nobody: the other owner's unlock is a plain table
/// update).
const LOCAL_POLL: Duration = Duration::from_millis(15);

/// How often a non-blocking request behind an earlier turn's close
/// publication or op in doubt re-checks (see [`close_wait`]).
const PREDECESSOR_POLL: Duration = Duration::from_micros(200);

/// How long a granted lock waits for the kernel to drop its cache of the
/// file before it is used anyway (the invalidation thread can be held up
/// by a notification waiting on an unrelated FUSE request).
const INVAL_WAIT: Duration = Duration::from_secs(1);

/// How far up the process tree the owner fence looks for a fenced
/// process (`flock(1) sh -c 'git …'`: git is two levels below the lock).
const LINEAGE_DEPTH: usize = 64;

/// How long after its install a recalled grant still waits for the local
/// lock it was asked for: what [`ClusterLocks::granted`] may wait (the
/// session budget, then the kernel invalidation) plus the lease margin's
/// ceiling (1 s). Past it the grant is released rather than renewed.
pub fn first_use_budget_ms(meta: &Meta) -> i64 {
    (meta.session().budget() + INVAL_WAIT).as_millis() as i64 + 1_000
}

/// Non-blocking requests give up with `EAGAIN` after this many grant
/// round trips that did not end in a lock (each one lost to a recall that
/// overtook it: another node wants the file).
const TRY_ROUNDS: u32 = 4;

/// How a [`ClusterLocks::set_inner`] pass ended without an error.
enum Step {
    /// The lock is taken.
    Done,
    /// The owner may not grant yet and nothing conflicts: ask again after
    /// this long (a non-blocking request only).
    Wait(Duration),
}

/// One attempt of a non-blocking request ([`ClusterLocks::try_lock_step`]).
#[derive(Debug, PartialEq, Eq)]
pub enum NbStep {
    /// Answered: the lock is taken, or the error.
    Done(Result<(), Code>),
    /// Nothing conflicts but the owner may not grant yet: ask again after
    /// this long (the request waits, see the module doc).
    Wait(Duration),
}

/// How often a wait on an owner's grace looks at its cancellation.
const WAIT_SLICE: Duration = Duration::from_millis(100);

/// The longest interval a request polls an owner at that cannot say when it
/// may grant ([`ClusterLocks::next_wait`]).
const POLL_BACKOFF_MAX: Duration = Duration::from_secs(1);

/// What ends a lock wait before the lock comes: the op's cancellation
/// (`Intr`) and its deadline (`TimedOut`), both from its `OpCtx`. Checked
/// where the wait loops, so a wait that the frontend gave up on holds
/// nothing and leaves no waiter behind. Plan 43 C7-F F-b N2: also while the
/// core's answer to a request is awaited ([`ClusterLocks::await_core`]), in
/// slices of [`ANSWER_SLICE`], so a signal ends a request in flight against
/// an owner that does not answer for seconds within a slice, not after its
/// round trip.
#[derive(Clone, Copy)]
struct Interrupt<'a> {
    cancel: Option<&'a CancelToken>,
    until: Option<std::time::Instant>,
    /// A request that carries no cancellation (a non-blocking lock request
    /// registers for signals only once it waits) asks its frontend for one
    /// when the core's answer is slow.
    late: Option<&'a dyn Late>,
}

/// The cancellation a request asks its frontend for late (see
/// [`Interrupt::late`]): `begin` registers the request for its caller's
/// signal (`Responder::begin_wait`) and returns the token, once.
pub struct LateToken<F> {
    begin: std::cell::RefCell<F>,
    got: std::cell::OnceCell<Option<CancelToken>>,
}

/// What [`Interrupt`] asks of a [`LateToken`].
pub trait Late {
    /// Register now (once).
    fn arm(&self);

    /// The token, once registered.
    fn token(&self) -> Option<&CancelToken>;
}

impl<F: FnMut() -> Option<CancelToken>> LateToken<F> {
    /// Nothing is allocated or called until [`Late::arm`]: a request
    /// answered at once pays for the two cells only.
    pub fn new(begin: F) -> Self {
        LateToken {
            begin: std::cell::RefCell::new(begin),
            got: std::cell::OnceCell::new(),
        }
    }
}

impl<F: FnMut() -> Option<CancelToken>> Late for LateToken<F> {
    fn arm(&self) {
        self.got.get_or_init(|| (self.begin.borrow_mut())());
    }

    fn token(&self) -> Option<&CancelToken> {
        self.got.get().and_then(Option::as_ref)
    }
}

impl Interrupt<'_> {
    const NONE: Interrupt<'static> = Interrupt {
        cancel: None,
        until: None,
        late: None,
    };

    /// `Err` when the wait must end now. A cancellation wins over a
    /// deadline that passed at the same time: the caller asked.
    fn check(&self) -> Result<(), Code> {
        if self.cancel.is_some_and(CancelToken::is_cancelled)
            || self
                .late
                .and_then(|l| l.token())
                .is_some_and(CancelToken::is_cancelled)
        {
            Err(Code::Intr)
        } else if self.until.is_some_and(|t| std::time::Instant::now() >= t) {
            Err(Code::TimedOut)
        } else {
            Ok(())
        }
    }

    /// The request has waited a slice for the core: if nothing can cancel
    /// it yet, ask the frontend for something that can.
    fn arm_late(&self) {
        if self.cancel.is_none() {
            if let Some(late) = self.late {
                late.arm();
            }
        }
    }
}

/// How often the wait for the core's answer to a request looks at the
/// request's cancellation (plan 43 C7-F F-b N2): a signal ends a lock
/// request within this, whatever the core is doing for it.
const ANSWER_SLICE: Duration = Duration::from_millis(50);

struct Unpark(std::thread::Thread);

impl std::task::Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// The FUSE side of cluster locks, shared by a view's FUSE threads and
/// the threads that wait for `F_SETLKW`/`flock` (`None` in
/// `SyncHandle::locks` is `--locks local`).
pub struct ClusterLocks {
    pub meta: Arc<Meta>,
    pub tx: tokio::sync::mpsc::UnboundedSender<SyncRequest>,
    /// The frontends' kernel caches of a file, dropped after a grant
    /// (`crate::kernel_inval`); `None` with kernel invalidation off.
    pub inval: Option<crate::kernel_inval::InodeInvalidator>,
    /// Each task's process ancestry, for the owner fence and the fencing
    /// token (see [`Self::ancestry`]).
    pub lineage: Mutex<LineageCache>,
    /// How long a non-blocking request waits for an owner that cannot
    /// grant yet before it fails `EAGAIN`
    /// (`constellation_authority::Config::lock_owner_wait_max_ms`): one
    /// bound, from the request's start, for every wait on an owner (a
    /// grace, a floor, a stale or an unreachable owner).
    pub owner_wait_max: Duration,
    /// The retry interval an owner hints for a wait it cannot time (a
    /// floor, a barrier, a stale or an unreachable owner:
    /// `Config::lock_poll_ms`). A request told so again and again asks
    /// less and less often (twice the last interval, up to a second): a
    /// hundred requests waiting forty seconds for a stale owner would
    /// otherwise ask it two thousand times a second. A hint longer than
    /// this is a grace's remainder, exact, and is followed.
    pub poll_hint: Duration,
    /// The threads that wait out non-blocking requests an owner cannot
    /// grant yet ([`Self::lock_nb_deferred`]).
    pub waits: WaitPool,
}

/// Most threads of a [`WaitPool`]: a waiting request is a timer and a
/// short step every so often, not a thread.
pub const WAIT_POOL_THREADS: usize = 16;

/// How long a pool thread with nothing to serve stays.
const WAIT_POOL_IDLE: Duration = Duration::from_secs(5);

/// The bounded pool of `lock-wait` threads behind
/// [`ClusterLocks::lock_nb_deferred`].
#[derive(Default)]
pub struct WaitPool {
    state: Mutex<PoolState>,
    wake: std::sync::Condvar,
    /// The park threads' own ([`Role::Park`]).
    wake_parks: std::sync::Condvar,
    next_id: std::sync::atomic::AtomicU64,
    /// Tests: starting a thread fails.
    #[cfg(test)]
    fail_spawn: std::sync::atomic::AtomicBool,
}

#[derive(Default)]
struct PoolState {
    tasks: Vec<WaitTask>,
    /// Plan 43 C7-F F-c: ops parked behind a lapsing grant.
    parks: Vec<ParkTask>,
    /// Threads serving `tasks` ([`Role::Task`]) and how many of them wait.
    workers: usize,
    idle: usize,
    /// Threads serving `parks` ([`Role::Park`]) and how many of them wait.
    park_workers: usize,
    park_idle: usize,
}

/// What a `lock-wait` thread serves. Plan 43 C7-F lock-wait follow-ups
/// (W3): a request waiting for an owner asks it again after each wait, and
/// that attempt may take seconds (an owner that does not answer, a few
/// times over) while it holds its thread; a parked op, which holds a
/// process in `read` or `write`, waits for the next look at its grant's
/// verdict — 20 ms apart — and an application resumes as late as the
/// first free thread. The two are served by threads of their own, so
/// neither waits for the other.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Task,
    Park,
}

/// Most threads serving parked ops (a look at a verdict is a table read;
/// the op runs again elsewhere, [`redo_pool`]).
pub const PARK_POOL_THREADS: usize = 4;

/// The threads the parked ops run again on: plan 43 C7-F lock-wait
/// follow-ups (W3). A parked op asked again from the start is any op — a
/// close or an `fsync` whose publication waits on the store for as long as
/// it is down — and it ran on the `lock-wait` thread that saw the verdict,
/// so enough of them, resolving together after a failover, held every
/// thread the lock waits need. A pool of its own, like the `fsync` one's:
/// its jobs wait as long as the store makes them.
pub fn redo_pool() -> &'static crate::completion::CompletionPool {
    static POOL: std::sync::OnceLock<crate::completion::CompletionPool> =
        std::sync::OnceLock::new();
    POOL.get_or_init(|| crate::completion::CompletionPool::named("lock-redo", 256))
}

/// How a parked op's wait ended ([`ClusterLocks::park_deferred`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkEnd {
    /// The verdict is in (or the park's bound passed): the op asks again
    /// from the start, and what it finds then is final.
    Resolved,
    /// The caller's signal ended the wait: `EINTR`.
    Interrupted,
    /// No `lock-wait` thread could be started: `EIO`, counted
    /// (`lapse_park_refused_pool`).
    NoThread,
}

/// An op parked behind a lapsing grant: polled until `ready`.
struct ParkTask {
    id: u64,
    started: std::time::Instant,
    due: std::time::Instant,
    cancel: Option<CancelToken>,
    op: Option<constellation_vfs::watch::Current>,
    ready: Box<dyn Fn() -> bool + Send>,
    done: Box<dyn FnOnce(ParkEnd) + Send>,
}

/// A parked op whose look at the verdict is due, or whose caller's signal
/// came.
fn park_ready(p: &ParkTask, now: std::time::Instant) -> bool {
    p.due <= now || p.cancel.as_ref().is_some_and(CancelToken::is_cancelled)
}

/// How often a parked op looks at its grant's verdict.
const PARK_POLL: Duration = Duration::from_millis(20);

/// How long past the lapse's own bound a parked op waits for the table to
/// give up before it asks again regardless.
const PARK_BOUND_SLACK: Duration = Duration::from_secs(5);

impl WaitPool {
    /// Threads running now.
    #[cfg(test)]
    pub fn threads(&self) -> usize {
        self.state.lock().unwrap().workers
    }

    /// Make starting a pool thread fail (tests).
    #[cfg(test)]
    pub fn fail_spawn(&self, fail: bool) {
        self.fail_spawn
            .store(fail, std::sync::atomic::Ordering::SeqCst);
    }
}

/// A non-blocking request waiting for an owner's grace.
struct WaitTask {
    id: u64,
    ino: Ino,
    lock: LocalLock,
    started: std::time::Instant,
    /// The last interval of its run of polling hints ([`ClusterLocks::next_wait`]).
    poll: Option<Duration>,
    /// When it asks again.
    due: std::time::Instant,
    cancel: Option<CancelToken>,
    op: Option<constellation_vfs::watch::Current>,
    done: Box<dyn FnOnce(Result<(), Code>) + Send>,
}

/// A process and its ancestors, nearest first: `(pid, start time)`.
type Ancestry = Vec<(u32, u64)>;

/// The tasks' process ancestries ([`ancestry_of`]), valid for one set of
/// lock-holding processes: a lock that stays (or a fence that does)
/// costs every other task one `/proc` walk and then one `/proc` read per
/// op (its start time, against pid reuse). Reset when the set changes,
/// which also drops chains a re-parented process no longer has.
#[derive(Default)]
pub struct LineageCache {
    lockers: Vec<(u32, u64)>,
    /// By `(task id, task start time)`.
    chains: std::collections::HashMap<(u32, u64), Arc<Ancestry>>,
}

/// Past this many pids the cache starts over.
const LINEAGE_CACHE_MAX: usize = 4096;

impl ClusterLocks {
    /// Whether I/O on `ino` must be refused (`EIO`): local locks under a
    /// lapsed grant. One relaxed atomic load when no local lock exists.
    ///
    /// Plan 43 C7-F F-c: [`Fence::Parked`] while the grant is lapsing: the
    /// op waits for the owner's verdict ([`View::lock_fenced`] and the park
    /// gate, `view::lock_gate`).
    pub fn fenced(&self, ino: Ino) -> Fence {
        let locks = self.meta.locks();
        // The table's state before the clock: with no local lock on the
        // node nothing is fenced, and the common case reads no clock at all
        // (`view::lock_gate` says so; every read, write, setattr, create,
        // close and fsync asks).
        if locks.lockers_none() {
            return Fence::Ok;
        }
        locks.fenced(ino, now())
    }

    /// Plan 30 §M14, the owner fence: whether an op by `pid` (FUSE: the
    /// calling thread) or by the kernel lock owner `lock_owner` must be
    /// refused (`EIO`) because it comes from a lock owner whose grant
    /// lapsed with its local lock still held — any thread of the process
    /// that took the lock, or a process it started (a `flock(1)`
    /// wrapper's git), matched by thread group. The lock owner only
    /// matches `fcntl` locks on direct I/O (the kernel sends one with
    /// direct-I/O reads and writes only, and a `flock`'s owner is its open
    /// file, never a write's). Other processes on the node are never
    /// fenced. One or two relaxed atomic loads while no owner can be
    /// fenced.
    ///
    /// Plan 43 C7-F F-c: an owner whose locks are under a grant that is
    /// *lapsing* (not captured) is [`Fence::Parked`]: its op waits for the
    /// owner's verdict on the late renewal; an owner already fenced stays
    /// fenced.
    pub fn owner_fenced(&self, pid: Option<u32>, lock_owner: Option<u64>) -> Fence {
        let locks = self.meta.locks();
        // The table's state before the clock: `local_inos` first, so the
        // no-lock fast path reads no clock at all.
        if locks.lockers_none() {
            return Fence::Ok;
        }
        let now = now();
        if !locks.owner_fence_armed(now) {
            return Fence::Ok;
        }
        let matches = |list: &[constellation_meta::locks::FencedOwner]| {
            lock_owner.is_some_and(|o| list.iter().any(|f| f.owner == o))
                || pid.is_some_and(|pid| {
                    let owners: Vec<(u32, u64)> = list
                        .iter()
                        .filter(|f| f.pid > 1)
                        .map(|f| (f.pid, f.pid_start))
                        .collect();
                    !owners.is_empty()
                        && self
                            .ancestry(pid, &self.locker_procs())
                            .iter()
                            .any(|p| owners.iter().any(|o| same_process(*o, *p)))
                })
        };
        let fenced = locks.fenced_owners(now);
        if !fenced.is_empty() && matches(&fenced) {
            locks.note_owner_fenced_op();
            return Fence::Fenced;
        }
        let parked = locks.parked_owners(now);
        if !parked.is_empty() && matches(&parked) {
            return Fence::Parked;
        }
        Fence::Ok
    }

    /// The processes holding local locks, for [`Self::ancestry`]'s cache.
    fn locker_procs(&self) -> Vec<(u32, u64)> {
        let mut v: Vec<(u32, u64)> = self
            .meta
            .locks()
            .lockers()
            .into_iter()
            .map(|(_, pid, start)| (pid, start))
            .collect();
        v.sort_unstable();
        v.dedup();
        v
    }

    /// Task `pid`'s process and its ancestors (by thread group, so a
    /// sibling thread of the locking thread and a process the locker
    /// started match whichever thread took the lock). Cached per task,
    /// keyed by its start time too, so a recycled pid is judged afresh;
    /// the `/proc` walk runs outside the cache's mutex.
    fn ancestry(&self, pid: u32, lockers: &[(u32, u64)]) -> Arc<Ancestry> {
        let process = &constellation_platform::native().process;
        let task = process.lineage(pid).ok();
        let key = (pid, task.map_or(0, |t| t.start));
        {
            let mut cache = self.lineage.lock().unwrap_or_else(|e| e.into_inner());
            if cache.lockers != lockers || cache.chains.len() >= LINEAGE_CACHE_MAX {
                cache.lockers = lockers.to_vec();
                cache.chains.clear();
            }
            if let Some(c) = cache.chains.get(&key) {
                return c.clone();
            }
        }
        let chain = Arc::new(ancestry_of(pid, task));
        let mut cache = self.lineage.lock().unwrap_or_else(|e| e.into_inner());
        if cache.lockers == lockers {
            cache.chains.insert(key, chain.clone());
        }
        chain
    }

    /// Plan 30 §M14 phase 2, the fencing token: the tag a mutation by
    /// `pid` / `lock_owner` carries — the grants the local locks of every
    /// owner it matches (as the owner fence matches them) are under
    /// (`LockTables::owner_tag`). Empty when it holds no lock (one relaxed
    /// load while no local lock exists); `Err` when one of its locks is
    /// under no honoured grant (it is fenced: the op is refused, `EIO`).
    pub fn caller_tag(
        &self,
        pid: Option<u32>,
        lock_owner: Option<u64>,
    ) -> Result<LockTag, OwnerFenced> {
        let locks = self.meta.locks();
        let lockers = locks.lockers();
        if lockers.is_empty() {
            return Ok(LockTag::NONE);
        }
        let mut procs: Vec<(u32, u64)> = lockers.iter().map(|l| (l.1, l.2)).collect();
        procs.sort_unstable();
        procs.dedup();
        let chain = pid.map(|pid| self.ancestry(pid, &procs));
        let owners: Vec<u64> = lockers
            .iter()
            .filter(|(owner, lpid, lstart)| {
                lock_owner == Some(*owner)
                    || (*lpid > 1
                        && chain
                            .as_ref()
                            .is_some_and(|c| c.iter().any(|p| same_process((*lpid, *lstart), *p))))
            })
            .map(|l| l.0)
            .collect();
        locks.owner_tag(&owners, now())
    }

    /// Whether `ino`'s dirty data must be discarded rather than
    /// published (`Some(fenced)`; see `LockTables::take_discard`).
    pub fn take_discard(&self, ino: Ino, write: bool) -> Discard {
        let locks = self.meta.locks();
        // Nothing held, locked or tainted: nothing to discard, and no clock
        // read (as `fenced`).
        if locks.nothing_to_discard() {
            return Discard::Publish;
        }
        locks.take_discard(ino, now(), write)
    }

    /// Dirty data of `ino` was discarded: every open file description of
    /// it reports `EIO` once, at its next close or `fsync`
    /// (`LockTables::note_discard`). Returns the event's sequence number.
    pub fn note_discard(&self, ino: Ino) -> u64 {
        self.meta.locks().note_discard(ino)
    }

    /// Drop the kernel's pages and attributes of `ino` (not waited for:
    /// callers are FUSE request handlers, which the notification may be
    /// queued behind).
    pub fn invalidate(&self, ino: Ino) {
        if let Some(inval) = &self.inval {
            inval.invalidate(ino);
        }
    }

    /// `getlk`: the conflicting lock, as `Some((start, end, write,
    /// pid))`; `None` when there is none. `write: false` asks about any
    /// lock (a read test, or `F_UNLCK`). Another node's grant answers as a
    /// whole-file lock with pid 0.
    ///
    /// `going`: owners whose locks go with a description on its way out
    /// (the view's `lock_handles`), named only when no other lock is in
    /// the way, as Linux, which dropped them already, names that one.
    pub fn test(
        &self,
        ino: Ino,
        owner: u64,
        start: u64,
        end: u64,
        write: bool,
        going: impl Fn(u64) -> bool,
    ) -> Option<(u64, u64, bool, u32)> {
        match self
            .meta
            .locks()
            .local_test_going_last(ino, owner, write, start, end, now(), going)
        {
            LocalOutcome::Conflict(l) => Some((l.start, l.end.min(OFFSET_MAX), l.write, l.pid)),
            // (`local_test` never answers `Predecessor`: only a lock
            // waits for an earlier turn.)
            LocalOutcome::Done | LocalOutcome::Predecessor | LocalOutcome::Parked => None,
            LocalOutcome::NeedGrant(mode) => {
                constellation_vfs::watch::stage("lock test (core reply)");
                let (reply, answer) = tokio::sync::oneshot::channel();
                if self
                    .tx
                    .send(SyncRequest::LockTest { ino, mode, reply })
                    .is_err()
                {
                    return None;
                }
                match answer.blocking_recv() {
                    Ok(LockTestAnswer::Held { mode, .. }) => {
                        Some((0, OFFSET_MAX, mode == LockMode::Exclusive, 0))
                    }
                    _ => None,
                }
            }
        }
    }

    /// `setlk` with `F_UNLCK`: drop `owner`'s locks in the range.
    pub fn unlock(&self, ino: Ino, owner: u64, start: u64, end: u64) {
        if self
            .meta
            .locks()
            .local_unlock(ino, owner, start, end, now())
        {
            self.idle(ino);
        }
    }

    /// A close (`flush`) or a `flock` release: drop every lock of `owner`
    /// on `ino` (POSIX: closing any descriptor drops the process's locks
    /// on the file). Returns whether no local lock is left on `ino`; the
    /// caller then calls [`Self::idle`] once the file's data is flushed.
    pub fn drop_owner(&self, ino: Ino, owner: u64) -> bool {
        self.meta.locks().local_release_owner(ino, owner, now())
    }

    /// [`Self::drop_owner`], also saying whether it dropped any lock:
    /// `(no local lock left, dropped)`.
    pub fn drop_owner_dropped(&self, ino: Ino, owner: u64) -> (bool, bool) {
        self.meta
            .locks()
            .local_release_owner_dropped(ino, owner, now())
    }

    /// No local lock is left on `ino`: if its grant was recalled, the
    /// core releases it now (flushing the file first). Nobody waits.
    pub fn idle(&self, ino: Ino) {
        if self.meta.locks().held(ino).is_some_and(|h| h.recalled) {
            let _ = self.tx.send(SyncRequest::LockIdle { ino });
        }
    }

    /// `setlk` with `F_RDLCK`/`F_WRLCK`. A blocking request (`sleep`)
    /// waits on a thread of its own, never on the FUSE worker: a
    /// contended `flock` may wait for as long as another node holds the
    /// file, and a worker pinned per waiter would starve the mount. Not
    /// the runtime's blocking pool either: it is small (4 threads on one
    /// CPU) and the release a waiter waits for may itself need it (the
    /// driver's `Action::LockFlush`), so waiters must never be able to
    /// fill it. `cancel` (the op's, set by the frontend on a signal) ends
    /// the wait with `Intr`, and `until` (the op's deadline) with
    /// `TimedOut` (see the module doc for what they reach).
    ///
    /// `done` answers the request (the view's completes the op's
    /// responder), on this thread or on the waiter's.
    pub fn lock<F>(
        self: &Arc<Self>,
        ino: Ino,
        lock: LocalLock,
        sleep: bool,
        cancel: Option<CancelToken>,
        until: Option<std::time::Instant>,
        done: F,
    ) where
        F: FnOnce(Result<(), Code>) + Send + 'static,
    {
        if !sleep {
            done(self.set(ino, lock, false, Interrupt::NONE, None));
            return;
        }
        let this = self.clone();
        // The request's watchdog entry moves with the reply: the waiter
        // thread's stages name it (`constellation_vfs::watch::current`,
        // `Current::adopt`).
        let op = constellation_vfs::watch::current();
        let spawned = std::thread::Builder::new()
            .name("lock-wait".into())
            .spawn(move || {
                if let Some(op) = op {
                    op.adopt();
                }
                let interrupt = Interrupt {
                    cancel: cancel.as_ref(),
                    until,
                    late: None,
                };
                done(this.set(ino, lock, true, interrupt, None))
            });
        if let Err(error) = spawned {
            // The reply went with the closure; dropping it answers EIO.
            tracing::warn!(%error, ino, "could not start a lock-wait thread");
        }
    }

    /// [`Self::lock`] on the calling thread, blocking or not: for a
    /// frontend that cannot answer an op from another thread
    /// (`constellation_vfs::FrontendCaps::deferrable`).
    pub fn lock_here(
        &self,
        ino: Ino,
        lock: LocalLock,
        sleep: bool,
        cancel: Option<&CancelToken>,
        until: Option<std::time::Instant>,
    ) -> Result<(), Code> {
        self.set(
            ino,
            lock,
            sleep,
            Interrupt {
                cancel,
                until,
                late: None,
            },
            None,
        )
    }

    /// A non-blocking [`Self::lock_here`] that waits for an earlier turn's
    /// close publication or op in doubt (`LocalOutcome::Predecessor`) until
    /// `deadline` at
    /// most: the request's own bound, shared with the view's wait for
    /// closing descriptions (`View::past_closing_locks`), so the two
    /// together never hold it longer than [`close_wait`].
    pub fn try_lock_by(
        &self,
        ino: Ino,
        lock: LocalLock,
        deadline: std::time::Instant,
        cancel: Option<&CancelToken>,
    ) -> Result<(), Code> {
        self.set(
            ino,
            lock,
            false,
            Interrupt {
                cancel,
                until: None,
                late: None,
            },
            Some(deadline),
        )
    }

    /// `deadline`: how long a non-blocking request waits for an earlier
    /// turn's close publication or op in doubt ([`Self::try_lock_by`]);
    /// [`close_wait`] from its first
    /// `Predecessor` when `None`.
    ///
    /// A non-blocking request the owner cannot grant yet waits here, on
    /// the calling thread (a frontend that cannot answer from another
    /// one): see [`Self::lock_nb_deferred`] for the bound.
    fn set(
        &self,
        ino: Ino,
        lock: LocalLock,
        sleep: bool,
        interrupt: Interrupt<'_>,
        deadline: Option<std::time::Instant>,
    ) -> Result<(), Code> {
        let lock = Self::prepared(lock);
        if sleep {
            let mut asked = false;
            let r = self
                .set_inner(ino, lock, true, interrupt, deadline, &mut asked)
                .map(|_| ());
            return self.settled(ino, r, asked);
        }
        let interrupt = Self::nonblocking(interrupt);
        let started = std::time::Instant::now();
        let mut deadline = deadline;
        let mut waited = false;
        let mut poll = None;
        let result = loop {
            match self.nb_step(ino, lock, interrupt, deadline) {
                NbStep::Done(r) => break r,
                NbStep::Wait(d) => {
                    let d = self.next_wait(&mut poll, d);
                    if !std::mem::replace(&mut waited, true) {
                        self.meta.locks().with_stats(|s| s.grace_waits += 1);
                    }
                    // (The close wait of the next attempt starts afresh.)
                    deadline = None;
                    if let Err(code) = self.wait_out(d, started, interrupt) {
                        break Err(code);
                    }
                }
            }
        };
        if waited {
            self.note_grace_waited(started);
        }
        result
    }

    /// A request that waited for a grace is over (granted or not).
    fn note_grace_waited(&self, started: std::time::Instant) {
        let ms = started.elapsed().as_millis() as u64;
        self.meta
            .locks()
            .with_stats(|s| s.grace_wait_ms_total += ms);
    }

    /// `FUSE` names the locking *thread*; the owner fence (and `getlk`)
    /// need its process, named across pid reuse.
    fn prepared(mut lock: LocalLock) -> LocalLock {
        (lock.pid, lock.pid_start) = process_of(lock.pid);
        lock
    }

    /// A non-blocking request never waits for the lock, so a deadline
    /// cannot time it out: it is refused (`Again`) or granted. The
    /// cancellation still ends one that is about to ask the sequencer.
    fn nonblocking(interrupt: Interrupt<'_>) -> Interrupt<'_> {
        Interrupt {
            until: None,
            ..interrupt
        }
    }

    /// The end of a request: on an error after the sequencer was asked, a
    /// grant may have come for it (or its answer was lost after the core
    /// installed it) that it will now never use: the grant must not wait
    /// for that first lock — recalled, it would be renewed for ever and
    /// its waiters would wait for ever.
    fn settled(&self, ino: Ino, r: Result<(), Code>, asked: bool) -> Result<(), Code> {
        if r.is_err() && asked && self.meta.locks().abandon_first_use(ino, now()) {
            self.idle(ino);
        }
        r
    }

    /// One attempt of a non-blocking request for a `lock` already
    /// [`prepared`](Self::prepared) and an `interrupt` already
    /// [`nonblocking`](Self::nonblocking).
    fn nb_step(
        &self,
        ino: Ino,
        lock: LocalLock,
        interrupt: Interrupt<'_>,
        deadline: Option<std::time::Instant>,
    ) -> NbStep {
        let mut asked = false;
        match self.set_inner(ino, lock, false, interrupt, deadline, &mut asked) {
            Ok(Step::Done) => NbStep::Done(Ok(())),
            Ok(Step::Wait(d)) => NbStep::Wait(d),
            Err(e) => NbStep::Done(self.settled(ino, Err(e), asked)),
        }
    }

    /// How long a request told to wait `hint` waits: the hint if it is a
    /// grace's remainder, which is exact; a hint that is the owner's polling
    /// interval (`poll_hint`, for a wait it cannot time) backs off, twice the
    /// last interval up to a second (`poll`: the last of the run of such
    /// hints).
    pub(crate) fn next_wait(&self, poll: &mut Option<Duration>, hint: Duration) -> Duration {
        if hint > self.poll_hint {
            *poll = None;
            return hint;
        }
        let wait = match *poll {
            Some(last) => (last * 2).min(POLL_BACKOFF_MAX).max(hint),
            None => hint,
        };
        *poll = Some(wait);
        wait
    }

    /// Sleep `d` (in slices, looking at the cancellation), unless the
    /// request is past [`Self::owner_wait_max`] from `started` (its
    /// start): then `EAGAIN`, counted.
    fn wait_out(
        &self,
        d: Duration,
        started: std::time::Instant,
        interrupt: Interrupt<'_>,
    ) -> Result<(), Code> {
        let end = std::time::Instant::now() + d;
        loop {
            interrupt.check()?;
            let now = std::time::Instant::now();
            if now.duration_since(started) >= self.owner_wait_max {
                self.meta.locks().with_stats(|s| s.grace_wait_gave_up += 1);
                return Err(Code::Again);
            }
            if now >= end {
                return Ok(());
            }
            std::thread::sleep((end - now).min(WAIT_SLICE));
        }
    }

    /// One attempt of a non-blocking request, for the view to hand to
    /// [`Self::lock_nb_deferred`] when it has to wait. As
    /// [`Self::try_lock_by`], which waits instead.
    pub fn try_lock_step(
        &self,
        ino: Ino,
        lock: LocalLock,
        deadline: std::time::Instant,
        cancel: Option<&CancelToken>,
        late: Option<&dyn Late>,
    ) -> NbStep {
        let interrupt = Interrupt {
            cancel,
            until: None,
            late,
        };
        self.nb_step(ino, Self::prepared(lock), interrupt, Some(deadline))
    }

    /// A non-blocking request the owner cannot grant yet (its first
    /// attempt answered [`NbStep::Wait`] `first_wait`; `started` is when
    /// the request began), waited out by the [`WaitPool`]: never the
    /// calling frontend worker, never the tokio blocking pool (see
    /// [`Self::lock`]), and above all never the offload thread of an
    /// io_uring ring, which a wait of up to [`Self::owner_wait_max`] would
    /// hold for every other request offloaded to it. A pool thread asks
    /// again after each wait until the lock is taken, refused (`EAGAIN`:
    /// the table then shows a conflict, or the bound passed, counted in
    /// `grace_wait_gave_up`) or cancelled (`EINTR`), and answers through
    /// `done`.
    ///
    /// The pool has at most [`WAIT_POOL_THREADS`] threads, however many
    /// requests wait (a wait is a timer and a short step, not a thread).
    /// When it cannot start even one (the process is out of threads) the
    /// request is answered now, as it was before it could wait: `EAGAIN`,
    /// counted (`grace_wait_refused_pool`), never `EIO`.
    pub fn lock_nb_deferred<F>(
        self: &Arc<Self>,
        ino: Ino,
        lock: LocalLock,
        first_wait: Duration,
        started: std::time::Instant,
        cancel: Option<CancelToken>,
        done: F,
    ) where
        F: FnOnce(Result<(), Code>) + Send + 'static,
    {
        let id = self
            .waits
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut poll = None;
        let first_wait = self.next_wait(&mut poll, first_wait);
        let task = WaitTask {
            id,
            ino,
            lock: Self::prepared(lock),
            started,
            poll,
            due: std::time::Instant::now() + first_wait,
            cancel,
            // The request's watchdog entry moves with the reply: the pool
            // thread's stages name it (`constellation_vfs::watch::current`).
            op: constellation_vfs::watch::current(),
            done: Box::new(done),
        };
        // Counted once the wait is in hand (a request the pool cannot take
        // is counted as refused, not as having waited).
        let counted = || self.meta.locks().with_stats(|s| s.grace_waits += 1);
        let mut st = self.waits.state.lock().unwrap();
        st.tasks.push(task);
        if st.idle > 0 {
            counted();
            self.waits.wake.notify_one();
            return;
        }
        if st.workers >= WAIT_POOL_THREADS {
            // Every thread is at work; one of them comes to it.
            counted();
            return;
        }
        st.workers += 1;
        drop(st);
        let this = self.clone();
        #[cfg(test)]
        let fail = self
            .waits
            .fail_spawn
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fail = false;
        let spawned = if fail {
            Err(std::io::Error::other("no thread (test)"))
        } else {
            std::thread::Builder::new()
                .name("lock-wait".into())
                .spawn(move || this.pool_worker(Role::Task))
        };
        if let Err(error) = spawned {
            tracing::warn!(%error, ino, "could not start a lock-wait thread");
            let mut st = self.waits.state.lock().unwrap();
            st.workers -= 1;
            if st.workers > 0 {
                // The threads that run serve it, later.
                counted();
                return;
            }
            let task = st
                .tasks
                .iter()
                .position(|t| t.id == id)
                .map(|i| st.tasks.swap_remove(i));
            drop(st);
            if let Some(task) = task {
                self.meta
                    .locks()
                    .with_stats(|s| s.grace_wait_refused_pool += 1);
                self.note_grace_waited(task.started);
                (task.done)(Err(Code::Again));
            }
        } else {
            counted();
        }
    }

    /// A waiting request whose turn has come: its wait is over, its caller
    /// cancelled it, or its bound passed.
    fn task_ready(&self, t: &WaitTask, now: std::time::Instant) -> bool {
        t.due <= now
            || t.cancel.as_ref().is_some_and(CancelToken::is_cancelled)
            || now.duration_since(t.started) >= self.owner_wait_max
    }

    /// Start one more pool thread of `role` to watch the work that waits
    /// while every thread is at a step (up to the role's bound). The pool
    /// grew only when a request was submitted and no thread was idle: a
    /// burst of requests woke one thread, whose step then held the others'
    /// turns, as they came due, for as long as it lasted.
    fn grow(self: &Arc<Self>, st: &mut PoolState, role: Role) {
        let (workers, idle, cap) = match role {
            Role::Task => (&mut st.workers, st.idle, WAIT_POOL_THREADS),
            Role::Park => (&mut st.park_workers, st.park_idle, PARK_POOL_THREADS),
        };
        if idle > 0 || *workers >= cap {
            return;
        }
        *workers += 1;
        let this = self.clone();
        let spawned = std::thread::Builder::new()
            .name("lock-wait".into())
            .spawn(move || this.pool_worker(role));
        if let Err(error) = spawned {
            tracing::warn!(%error, "could not start a lock-wait thread");
            match role {
                Role::Task => st.workers -= 1,
                Role::Park => st.park_workers -= 1,
            }
        }
    }

    /// A [`WaitPool`] thread of `role`: serves the waiting requests that
    /// are due (their wait is over), cancelled or past their bound
    /// ([`Role::Task`]), or the parked ops whose look at the verdict is due
    /// or whose caller's signal came ([`Role::Park`]), until none is left
    /// for [`WAIT_POOL_IDLE`].
    fn pool_worker(self: Arc<Self>, role: Role) {
        let mut st = self.waits.state.lock().unwrap();
        loop {
            let now = std::time::Instant::now();
            let mut next = None::<std::time::Instant>;
            if role == Role::Park {
                let mut ready_park = None;
                for (i, p) in st.parks.iter().enumerate() {
                    if park_ready(p, now) {
                        ready_park = Some(i);
                        break;
                    }
                    next = Some(next.map_or(p.due, |n| n.min(p.due)));
                }
                if let Some(i) = ready_park {
                    let task = st.parks.swap_remove(i);
                    // Others wait: one thread is to watch for them while
                    // this one is at its step.
                    if !st.parks.is_empty() {
                        self.grow(&mut st, Role::Park);
                    }
                    drop(st);
                    let again = self.advance_park(task);
                    st = self.waits.state.lock().unwrap();
                    if let Some(task) = again {
                        st.parks.push(task);
                    }
                    continue;
                }
            } else {
                let mut ready = None;
                for (i, t) in st.tasks.iter().enumerate() {
                    if self.task_ready(t, now) {
                        ready = Some(i);
                        break;
                    }
                    next = Some(next.map_or(t.due, |n| n.min(t.due)));
                }
                if let Some(i) = ready {
                    let task = st.tasks.swap_remove(i);
                    // Others wait: one thread is to watch for their turns
                    // while this one is at its step, which may hold it for a
                    // round trip to an owner that does not answer.
                    if !st.tasks.is_empty() {
                        self.grow(&mut st, Role::Task);
                    }
                    drop(st);
                    let again = self.advance(task);
                    st = self.waits.state.lock().unwrap();
                    if let Some(task) = again {
                        st.tasks.push(task);
                    }
                    continue;
                }
            }
            // Nothing due: look again when the first is, at the latest a
            // slice from now (the cancellations), or end when idle.
            let none = match role {
                Role::Task => st.tasks.is_empty(),
                Role::Park => st.parks.is_empty(),
            };
            let wait = match next {
                Some(next) => (next - now).min(WAIT_SLICE),
                None => WAIT_POOL_IDLE,
            };
            let wake = match role {
                Role::Task => {
                    st.idle += 1;
                    &self.waits.wake
                }
                Role::Park => {
                    st.park_idle += 1;
                    &self.waits.wake_parks
                }
            };
            let (guard, timeout) = wake.wait_timeout(st, wait).unwrap();
            st = guard;
            match role {
                Role::Task => st.idle -= 1,
                Role::Park => st.park_idle -= 1,
            }
            let none_now = match role {
                Role::Task => st.tasks.is_empty(),
                Role::Park => st.parks.is_empty(),
            };
            if none && timeout.timed_out() && none_now {
                match role {
                    Role::Task => st.workers -= 1,
                    Role::Park => st.park_workers -= 1,
                }
                return;
            }
        }
    }

    /// One step of a waiting request that is due, cancelled or past its
    /// bound: its end (`None`, answered), or the request to wait again.
    fn advance(&self, mut task: WaitTask) -> Option<WaitTask> {
        if let Some(op) = &task.op {
            op.adopt();
        }
        let interrupt = Interrupt {
            cancel: task.cancel.as_ref(),
            until: None,
            late: None,
        };
        let result = if let Err(code) = interrupt.check() {
            Err(code)
        } else if task.started.elapsed() >= self.owner_wait_max {
            self.meta.locks().with_stats(|s| s.grace_wait_gave_up += 1);
            Err(Code::Again)
        } else {
            constellation_vfs::watch::stage("lock grace (owner)");
            match self.nb_step(task.ino, task.lock, interrupt, None) {
                NbStep::Done(r) => r,
                NbStep::Wait(d) => {
                    let d = self.next_wait(&mut task.poll, d);
                    task.due = std::time::Instant::now() + d;
                    if let Some(op) = &task.op {
                        op.release();
                    }
                    return Some(task);
                }
            }
        };
        self.note_grace_waited(task.started);
        (task.done)(result);
        if let Some(op) = &task.op {
            op.release();
        }
        None
    }

    /// Plan 43 C7-F F-c: park an op behind a lapsing grant, on the
    /// [`WaitPool`] (never a frontend worker nor an offload thread): `ready`
    /// is polled until it says the verdict is in, the caller's `cancel`
    /// ends the wait (`ParkEnd::Interrupted`), or the bound passes — the
    /// lapse's own (`lock_owner_wait_max_ms`, which the table gives up
    /// at) plus a little: `ParkEnd::Resolved` either way, and `done` asks
    /// again, finding the grant resolved or fenced. `done` runs on a pool
    /// thread (or, with none to start, on this one with
    /// `ParkEnd::NoThread`).
    pub fn park_deferred(
        self: &Arc<Self>,
        cancel: Option<CancelToken>,
        ready: impl Fn() -> bool + Send + 'static,
        done: impl FnOnce(ParkEnd) + Send + 'static,
    ) {
        let id = self
            .waits
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let task = ParkTask {
            id,
            started: std::time::Instant::now(),
            due: std::time::Instant::now() + PARK_POLL,
            cancel,
            op: constellation_vfs::watch::current(),
            ready: Box::new(ready),
            done: Box::new(done),
        };
        let mut st = self.waits.state.lock().unwrap();
        st.parks.push(task);
        if st.park_idle > 0 {
            self.waits.wake_parks.notify_one();
            return;
        }
        if st.park_workers >= PARK_POOL_THREADS {
            // Every thread is at work; one of them comes to it.
            return;
        }
        st.park_workers += 1;
        drop(st);
        let this = self.clone();
        #[cfg(test)]
        let fail = self
            .waits
            .fail_spawn
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fail = false;
        let spawned = if fail {
            Err(std::io::Error::other("no thread (test)"))
        } else {
            std::thread::Builder::new()
                .name("lock-wait".into())
                .spawn(move || this.pool_worker(Role::Park))
        };
        if let Err(error) = spawned {
            tracing::warn!(%error, "could not start a lock-wait thread");
            let mut st = self.waits.state.lock().unwrap();
            st.park_workers -= 1;
            if st.park_workers > 0 {
                return;
            }
            let task = st
                .parks
                .iter()
                .position(|t| t.id == id)
                .map(|i| st.parks.swap_remove(i));
            drop(st);
            if let Some(task) = task {
                self.meta
                    .locks()
                    .with_stats(|s| s.lapse_park_refused_pool += 1);
                (task.done)(ParkEnd::NoThread);
            }
        }
    }

    /// One step of a parked op that is due or cancelled: its end (`None`),
    /// or the task to wait again.
    fn advance_park(&self, mut task: ParkTask) -> Option<ParkTask> {
        if let Some(op) = &task.op {
            op.adopt();
        }
        let end = if task.cancel.as_ref().is_some_and(CancelToken::is_cancelled) {
            self.meta
                .locks()
                .with_stats(|s| s.lapse_park_interrupted += 1);
            Some(ParkEnd::Interrupted)
        } else if task.started.elapsed() >= self.owner_wait_max + PARK_BOUND_SLACK || (task.ready)()
        {
            Some(ParkEnd::Resolved)
        } else {
            None
        };
        match end {
            Some(end) => {
                // The op asks again (or is answered `EINTR`) on a thread of
                // the redo pool, which may wait for as long as the store
                // makes it: this thread looks at the other parked ops.
                let (op, done) = (task.op.take(), task.done);
                if let Some(op) = &op {
                    op.release();
                }
                redo_pool().submit(move || {
                    if let Some(op) = &op {
                        op.adopt();
                    }
                    done(end);
                    if let Some(op) = &op {
                        op.release();
                    }
                });
                None
            }
            None => {
                task.due = std::time::Instant::now() + PARK_POLL;
                if let Some(op) = &task.op {
                    op.release();
                }
                Some(task)
            }
        }
    }

    /// The core's answer to a request, awaited in slices of
    /// [`ANSWER_SLICE`] that look at the request's cancellation and
    /// deadline (plan 43 C7-F F-b N2): the core may take seconds to answer
    /// (it asks an owner that is gone, a few times, before it says so),
    /// and a signal is not to wait for it. A request ended here before its
    /// answer leaves the core to answer nobody; a grant that comes for it
    /// after that is the first use no request will make, which
    /// [`Self::settled`] and the first-use budget account for (as for an
    /// answer that was lost).
    fn await_core<T>(
        answer: tokio::sync::oneshot::Receiver<T>,
        interrupt: Interrupt<'_>,
    ) -> Result<T, Code> {
        use std::future::Future;
        let waker = std::task::Waker::from(Arc::new(Unpark(std::thread::current())));
        let mut cx = std::task::Context::from_waker(&waker);
        let mut answer = std::pin::pin!(answer);
        let mut slices = 0u32;
        loop {
            match answer.as_mut().poll(&mut cx) {
                std::task::Poll::Ready(Ok(answer)) => return Ok(answer),
                std::task::Poll::Ready(Err(_)) => return Err(Code::Io),
                std::task::Poll::Pending => {}
            }
            interrupt.check()?;
            if slices == 1 {
                // Not an answer that comes at once: something that can
                // cancel the request is wanted from here.
                interrupt.arm_late();
            }
            slices = slices.saturating_add(1);
            std::thread::park_timeout(ANSWER_SLICE);
        }
    }

    fn set_inner(
        &self,
        ino: Ino,
        lock: LocalLock,
        sleep: bool,
        interrupt: Interrupt<'_>,
        mut deadline: Option<std::time::Instant>,
        asked: &mut bool,
    ) -> Result<Step, Code> {
        let mut rounds = 0u32;
        // The last round brought a grant (the core said `Granted`); if the
        // next look finds another client of this node holding the lock
        // under it, the round was lost to that client, not to a recall.
        let mut granted_round = false;
        let mut predecessor_counted = false;
        // When the application began to wait: every request of this call
        // carries how long that is, so that a call that lost a round to a
        // recall (or to another client of this node) keeps its place in
        // the owner's queue instead of joining it again at the back.
        let started = std::time::Instant::now();
        loop {
            let outcome = self.meta.locks().local_set(ino, lock, now());
            if outcome == LocalOutcome::Predecessor && !predecessor_counted {
                // Once per request, however often it looks again.
                predecessor_counted = true;
                self.meta.locks().with_stats(|s| s.predecessor_waits += 1);
            }
            match outcome {
                LocalOutcome::Done => return Ok(Step::Done),
                // Plan 43 C7-F F-c: the grant held is lapsing and its
                // owner has not said yet: wait for the verdict (a
                // non-blocking request from the wait pool, like one that
                // waits for a grace), never ask for a fresh grant that
                // would replace it and fence its owners.
                LocalOutcome::Parked => {
                    if !sleep {
                        return Ok(Step::Wait(PARK_POLL));
                    }
                    interrupt.check()?;
                    std::thread::sleep(LOCAL_POLL);
                }
                LocalOutcome::Predecessor
                    if !sleep
                        && *deadline
                            .get_or_insert_with(|| std::time::Instant::now() + close_wait())
                            > std::time::Instant::now() =>
                {
                    // Nothing is in the way but what an earlier turn counts
                    // as done still on its way under the grant: a close's
                    // publication of the file (a `RELEASE` the kernel sent
                    // after `close(2)` returned, a `FLUSH` that dropped the
                    // closer's locks first), or an op left in doubt. A
                    // local file system would grant now: wait for it a
                    // little rather than refuse.
                    std::thread::sleep(PREDECESSOR_POLL);
                }
                outcome @ (LocalOutcome::Conflict(_) | LocalOutcome::Predecessor) => {
                    if !sleep {
                        if matches!(outcome, LocalOutcome::Predecessor) {
                            tracing::debug!(
                                target: "constellation::locks",
                                ino,
                                "a non-blocking lock request is refused: an earlier turn's close \
                                 is still publishing, or its op in doubt, after the close wait"
                            );
                        }
                        return Err(Code::Again);
                    }
                    interrupt.check()?;
                    // A round that got its grant and lost the local race
                    // to another client of this node does not count: its
                    // place in the owner's queue is kept (`waited_ms`), so
                    // it is no reason to wait longer than the others.
                    // (Only after that: a `Granted` that `local_set` still
                    // answers `NeedGrant` to keeps its back-off, or a core
                    // granting what the tables refuse would be spun on.)
                    if std::mem::take(&mut granted_round) {
                        rounds = 0;
                    }
                    std::thread::sleep(LOCAL_POLL);
                }
                LocalOutcome::NeedGrant(mode) => {
                    granted_round = false;
                    if !sleep && rounds >= TRY_ROUNDS {
                        return Err(Code::Again);
                    }
                    interrupt.check()?;
                    if rounds >= 2 {
                        // A request that keeps being answered without a
                        // grant (a recall overtook it), or with one the
                        // local tables do not take: back off a little
                        // instead of spinning on the sequencer. A round
                        // that did get a grant, and lost the local race to
                        // another client, does not count (`granted_round`).
                        std::thread::sleep(Duration::from_millis(10 * u64::from(rounds.min(20))));
                    }
                    rounds += 1;
                    constellation_vfs::watch::stage("lock grant (core reply)");
                    let (reply, answer) = tokio::sync::oneshot::channel();
                    *asked = true;
                    self.tx
                        .send(SyncRequest::Lock {
                            ino,
                            mode,
                            blocking: sleep,
                            waited_ms: if sleep {
                                started.elapsed().as_millis() as u64
                            } else {
                                0
                            },
                            reply,
                        })
                        .map_err(|_| Code::Io)?;
                    match Self::await_core(answer, interrupt)? {
                        LockAnswer::Granted { position } => {
                            self.granted(ino, &position);
                            granted_round = sleep;
                        }
                        LockAnswer::WouldBlock => {
                            if !sleep {
                                return Err(Code::Again);
                            }
                            // The owner parks blocking requests, so this
                            // is a race with a recall; ask again shortly.
                            std::thread::sleep(LOCAL_POLL);
                        }
                        LockAnswer::Wait { retry_ms } => {
                            // Nothing conflicts, the owner may not grant
                            // yet. Not a round that was lost to a recall.
                            rounds -= 1;
                            let d = Duration::from_millis(retry_ms);
                            if !sleep {
                                return Ok(Step::Wait(d));
                            }
                            // The owner parks blocking requests, so this
                            // is a race too: wait it out.
                            interrupt.check()?;
                            std::thread::sleep(d);
                        }
                        LockAnswer::Unavailable => return Err(Code::NoLock),
                    }
                }
            }
        }
    }

    /// A grant arrived: what the previous holder wrote under its lock is
    /// at or before `position` — to this file and to every other one
    /// (the owner joins the releaser's frontier in). Wait for the replica
    /// to reach it, then drop the kernel's pages and attributes of the
    /// file, which may predate it.
    ///
    /// `position` also becomes this session's `observed` watermark, so a
    /// read of any *other* file waits for it too, however the wait for
    /// the locked file itself ended (a speculation that covers it, or the
    /// session budget): the application reads the refs next to the lock
    /// file, not only the lock file (EC2 campaign 4 B-1).
    fn granted(&self, ino: Ino, position: &Position) {
        self.meta.session().raise_observed(*position);
        constellation_vfs::watch::stage("session wait after a lock grant");
        let waited = self.meta.session_wait_at(&[ReadKey::Ino(ino)], position);
        tracing::debug!(target: "constellation::locks", ino, ?position, ?waited, "lock granted");
        match waited {
            constellation_meta::SessionWait::Waited(d) => {
                self.meta.locks().with_stats(|s| {
                    s.grants_waited += 1;
                    s.grant_wait_ms_total += d.as_millis() as u64;
                });
            }
            constellation_meta::SessionWait::TimedOut(d) => {
                // The guarantee the grant carries — the holder reads what
                // the previous holders wrote — does not hold for this
                // turn: the replica has not reached the floor, and the
                // reads under the lock answer from what it has. Counted
                // (`locks.grants_degraded`) and said out loud, every time.
                self.meta.locks().with_stats(|s| {
                    s.grants_waited += 1;
                    s.grant_wait_ms_total += d.as_millis() as u64;
                    s.grants_degraded += 1;
                });
                tracing::warn!(
                    target: "constellation::locks",
                    ino,
                    floor = ?position,
                    applied = ?self.meta.session().applied(),
                    waited = ?d,
                    "a lock grant's floor was not reached within the session budget: the reads \
                     under this grant are degraded (they may not see what the previous holder wrote)"
                );
            }
            _ => {}
        }
        if let Some(inval) = &self.inval {
            constellation_vfs::watch::stage("kernel invalidation after a lock grant");
            if !inval.invalidate_and_wait(ino, INVAL_WAIT) {
                tracing::debug!(
                    target: "constellation::locks",
                    ino,
                    "kernel cache invalidation after a lock grant timed out"
                );
            }
        }
    }
}

// ------------------------------------------------------- the fencing token

/// Plan 30 §M14 phase 2: the FUSE op this thread is serving, for the
/// fencing token its mutations carry. Entered when the op starts (the
/// view's `admit!`, `flush`, `release`), and only while this node holds a
/// local lock at all; which grants the op is under is worked out at its
/// first mutation ([`current_tag`]; `flush` and `release` ask at once,
/// before the close drops the caller's locks), which also counts the op
/// in flight for them — none of them is released before the op ends
/// (`LockTables::tag_begin`/`tag_end`, the release ordering). Each
/// mutation's token then carries the grants' windows as they are when it
/// is sent (`LockTables::refresh_tag`).
struct ScopeState {
    locks: Arc<ClusterLocks>,
    pid: Option<u32>,
    lock_owner: Option<u64>,
    /// The op's cancellation, which ends the wait for a grant that is
    /// lapsing (plan 43 C7-F lock-wait follow-ups, W1): `EINTR`.
    cancel: Option<CancelToken>,
    /// The wait for the grant ended with `cancel`.
    interrupted: bool,
    /// Worked out (and announced in flight) at the first mutation; a
    /// scope entered with a fixed tag has it from the start.
    tag: Option<Result<LockTag, OwnerFenced>>,
    /// The token last handed to a mutation (the same grants, windows as
    /// of then).
    sent: LockTag,
    /// The token of the mutation that ended in doubt (the latest such):
    /// how long it may still execute at an executor that checks the
    /// window — what its grants' release waits for. Not `sent`, which a
    /// later mutation of the op may have refreshed up to the grant's end
    /// (pinned that long, a recalled grant lapsed before its release).
    doubt: LockTag,
    /// A mutation of this op ended in doubt: it may still execute, so
    /// its grants wait for its tokens' windows before a release.
    in_doubt: bool,
    /// The file whose turn this scope's op ends: a close (`FLUSH`,
    /// `RELEASE`) that drops its caller's locks there and publishes it
    /// ([`begin_close`]), or the flush before a grant's release
    /// ([`ClusterLocks::fixed_tag_scope`]). A mutation of it that ends in
    /// doubt pins that file's grant ([`note_tag_in_doubt`]).
    closes: Option<Ino>,
}

thread_local! {
    static SCOPE: std::cell::RefCell<Option<ScopeState>> = const { std::cell::RefCell::new(None) };
}

/// See [`ScopeState`]. Ends the scope (and the op's in-flight count) when
/// dropped; restores an enclosing scope.
#[must_use]
pub struct TagScope {
    prev: Option<Option<ScopeState>>,
}

impl TagScope {
    fn inert() -> TagScope {
        TagScope { prev: None }
    }

    fn enter(state: ScopeState) -> TagScope {
        let prev = SCOPE.with(|s| s.borrow_mut().replace(state));
        TagScope { prev: Some(prev) }
    }
}

impl Drop for TagScope {
    fn drop(&mut self) {
        let Some(prev) = self.prev.take() else {
            return;
        };
        let ended = SCOPE.with(|s| std::mem::replace(&mut *s.borrow_mut(), prev));
        if let Some(ScopeState {
            locks,
            tag: Some(Ok(tag)),
            sent,
            doubt,
            in_doubt,
            ..
        }) = ended
        {
            let last = if in_doubt && !doubt.is_empty() {
                doubt
            } else if sent.is_empty() {
                tag
            } else {
                sent
            };
            locks.meta.locks().tag_end(&last, in_doubt, now());
        }
    }
}

/// Who an op deferred to another thread was issued by, to re-enter its
/// scope there ([`ScopeCaller::enter`]).
pub struct ScopeCaller {
    locks: Arc<ClusterLocks>,
    pid: Option<u32>,
    lock_owner: Option<u64>,
    cancel: Option<CancelToken>,
}

impl ScopeCaller {
    pub fn enter(&self) -> TagScope {
        self.locks
            .tag_scope(self.pid, self.lock_owner, self.cancel.as_ref())
    }
}

impl ClusterLocks {
    /// Enter the fencing-token scope of an op by `pid` / `lock_owner` on
    /// this thread (see [`ScopeState`]). One relaxed load, and nothing
    /// else, while no local lock exists on this node.
    pub fn tag_scope(
        self: &Arc<Self>,
        pid: Option<u32>,
        lock_owner: Option<u64>,
        cancel: Option<&CancelToken>,
    ) -> TagScope {
        if self.meta.locks().lockers_none() {
            return TagScope::inert();
        }
        TagScope::enter(ScopeState {
            locks: self.clone(),
            pid,
            lock_owner,
            cancel: cancel.cloned(),
            interrupted: false,
            tag: None,
            sent: LockTag::NONE,
            doubt: LockTag::NONE,
            in_doubt: false,
            closes: None,
        })
    }

    /// A scope whose mutations carry `tag` whoever runs them: a recalled
    /// grant's flush (`Action::LockFlush`), on the driver's thread,
    /// publishes the locked file `ino` under the grant it is about to
    /// release.
    pub fn fixed_tag_scope(self: &Arc<Self>, tag: LockTag, ino: Ino) -> TagScope {
        if tag.is_empty() {
            return TagScope::inert();
        }
        self.meta.locks().tag_begin(&tag, now());
        TagScope::enter(ScopeState {
            locks: self.clone(),
            pid: None,
            lock_owner: None,
            cancel: None,
            interrupted: false,
            tag: Some(Ok(tag)),
            sent: LockTag::NONE,
            doubt: LockTag::NONE,
            in_doubt: false,
            closes: Some(ino),
        })
    }
}

/// The caller of the op this thread serves, if it is in a scope.
pub fn scope_caller() -> Option<ScopeCaller> {
    SCOPE.with(|s| {
        s.borrow().as_ref().map(|st| ScopeCaller {
            locks: st.locks.clone(),
            pid: st.pid,
            lock_owner: st.lock_owner,
            cancel: st.cancel.clone(),
        })
    })
}

/// The fencing token for a mutation of the op this thread serves (empty
/// outside a scope, or for a caller that holds no lock). Which grants it
/// names is worked out once per op, at its first call, which counts the
/// op in flight for them; their windows are taken now, at every call
/// (the token of the mutation about to be sent: a renewal since the op
/// began moved them on). `Err(EIO)`: the caller holds a lock under no
/// honoured grant — it is fenced (counted as an owner-fenced op).
pub fn current_tag() -> Result<LockTag, Code> {
    SCOPE.with(|s| {
        let mut s = s.borrow_mut();
        let Some(st) = s.as_mut() else {
            return Ok(LockTag::NONE);
        };
        let tag = st
            .tag
            .get_or_insert_with(|| {
                let mut tag = st.locks.caller_tag(st.pid, st.lock_owner);
                // Plan 43 C7-F F-c: a caller whose grant is lapsing (not
                // fenced yet) waits, here, for its owner's verdict: an op
                // the park gate could not hold (it passed the gate before
                // the grant lapsed) is then neither refused nor lost.
                if tag.is_err() {
                    let started = std::time::Instant::now();
                    let bound = st.locks.owner_wait_max + PARK_BOUND_SLACK;
                    let mut waited = false;
                    while tag.is_err()
                        && started.elapsed() < bound
                        && st.locks.owner_fenced(st.pid, st.lock_owner) == Fence::Parked
                    {
                        if st.cancel.as_ref().is_some_and(CancelToken::is_cancelled) {
                            st.interrupted = true;
                            break;
                        }
                        if !std::mem::replace(&mut waited, true) {
                            st.locks
                                .meta
                                .locks()
                                .with_stats(|s| s.lapse_inline_waits += 1);
                        }
                        std::thread::sleep(Duration::from_millis(10));
                        tag = st.locks.caller_tag(st.pid, st.lock_owner);
                    }
                    if waited {
                        let ms = started.elapsed().as_millis() as u64;
                        st.locks.meta.locks().with_stats(|s| {
                            s.lapse_inline_wait_ms_total += ms;
                            s.lapse_inline_interrupted += u64::from(st.interrupted);
                        });
                    }
                }
                if let Ok(t) = &tag {
                    st.locks.meta.locks().tag_begin(t, now());
                }
                tag
            })
            .clone();
        let tag = tag.map(|captured| {
            let base = if st.sent.is_empty() {
                &captured
            } else {
                &st.sent
            };
            let fresh = st.locks.meta.locks().refresh_tag(base, now());
            st.sent = fresh.clone();
            fresh
        });
        tag.map_err(|OwnerFenced| {
            // Its caller's signal ended the wait for a grant that is
            // lapsing: nothing is decided about the grant (the op ends
            // `EINTR`, as a parked one does).
            if st.interrupted {
                return Code::Intr;
            }
            // What it publishes now was written under that lock: never
            // published (see `take_lapsed`).
            note_lapsed();
            st.locks.meta.locks().note_owner_fenced_op();
            tracing::debug!(
                target: "constellation::locks",
                pid = ?st.pid,
                "refused a mutation: the caller holds a lock under no honoured grant (EIO)"
            );
            Code::Io
        })
    })
}

/// A mutation of this op was refused for a lapsed grant (`LockLapsed`):
/// whether to send it again under a fresh rid and token — every grant it
/// named is still held and honoured here, under a window that moved on
/// since the refused token was taken (a renewal). The executor judged a
/// window this node has since extended (a delegate's clock check, or a
/// replay that waited), not an ended grant: an ended grant is renewed no
/// more, so this is `false` again soon and the op is not re-sent for
/// ever.
pub fn reissue_after_lapse() -> Reissue {
    SCOPE.with(|s| {
        let s = s.borrow();
        let Some(st) = s.as_ref() else {
            return Reissue::No;
        };
        if st.sent.is_empty() {
            return Reissue::No;
        }
        let locks = st.locks.meta.locks();
        // Plan 43 C7-F F-c: a grant this mutation was issued under that is
        // lapsing is waited for (its owner admits or refuses a late
        // renewal): the op sent under its old window, which the executor
        // refused as lapsed, goes again under the renewed one. Lock-wait
        // follow-ups (W1): its caller's signal ends the wait (`EINTR`), and
        // the wait is counted with the other inline waits.
        let started = std::time::Instant::now();
        let bound = st.locks.owner_wait_max + PARK_BOUND_SLACK;
        let mut waited = false;
        let mut interrupted = false;
        while locks.tag_lapsing(&st.sent, now()) && started.elapsed() < bound {
            if st.cancel.as_ref().is_some_and(CancelToken::is_cancelled) {
                interrupted = true;
                break;
            }
            if !std::mem::replace(&mut waited, true) {
                locks.with_stats(|s| s.lapse_inline_waits += 1);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        if waited {
            let ms = started.elapsed().as_millis() as u64;
            locks.with_stats(|s| {
                s.lapse_inline_wait_ms_total += ms;
                s.lapse_inline_interrupted += u64::from(interrupted);
            });
        }
        if interrupted {
            return Reissue::Interrupted;
        }
        let now = now();
        let fresh = locks.refresh_tag(&st.sent, now);
        if fresh != st.sent && locks.tag_honoured(&fresh, now) {
            Reissue::Yes
        } else {
            Reissue::No
        }
    })
}

/// What [`reissue_after_lapse`] decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reissue {
    /// Send the mutation again under a fresh rid and token.
    Yes,
    /// It stays refused (`EIO`).
    No,
    /// Its caller's signal came while it waited (`EINTR`).
    Interrupted,
}

/// A tagged mutation of this op ended in doubt (it may still execute
/// somewhere): its grants are not released before its tokens' windows
/// are over, and no new local turn starts under them before then either
/// (`LocalOutcome::Predecessor`) — pinned now, before the op answers.
/// A close that ends its caller's turn on a file pins that file's grant
/// too ([`begin_close`]).
pub fn note_tag_in_doubt(rid: Option<constellation_meta::Rid>) {
    SCOPE.with(|s| {
        if let Some(st) = s.borrow_mut().as_mut() {
            if st
                .tag
                .as_ref()
                .is_some_and(|t| t.as_ref().is_ok_and(|t| !t.is_empty()))
            {
                st.in_doubt = true;
                st.doubt = st.sent.clone();
                // Pinned now, before the op answers: the answer lets its
                // caller unlock, and the next turn must find the pin
                // (`LocalOutcome::Predecessor`), not wait for the scope's
                // end (`TagScope::drop` pins it again, harmlessly).
                let table = st.locks.meta.locks();
                table.pin_in_doubt(&st.doubt, rid);
                // A close ending its caller's turn on a file
                // ([`begin_close`]) pins that file's grant too, whatever
                // its token names (one grant per minter, the one whose
                // window ends first: maybe another file's), until the
                // grant's window as held now.
                if let Some(h) = st.closes.and_then(|ino| table.held(ino)) {
                    // Minted as a token is: the honour window on the wall
                    // clock, now (plan 43 C7-H1, R8).
                    table.pin_in_doubt(
                        &LockTag(vec![constellation_meta::locks::LockToken {
                            grant: h.id,
                            until_ms: now().wall_of_honour(h.until).0,
                        }]),
                        rid,
                    );
                }
            }
        }
    });
}

/// `current_tag` for a close (`FLUSH`, `RELEASE`) of `ino`, called before
/// the close drops its caller's locks: the scope's grants are worked out
/// (and the op counted in flight under them) now. `ends_turn`: the close
/// drops locks of its caller on `ino` and publishes the file — what that
/// turn wrote, which its application counts as done once its `close(2)`
/// returned (or, for a `RELEASE`, before the kernel even sent it), and
/// which must land before what the next turn writes. Then a new local
/// turn on the file waits (`LockTables::publish_begin`,
/// `LocalOutcome::Predecessor`) from now — before the drop, so no turn
/// starts between the drop and the commit — until the returned guard is
/// dropped, after the publication; and the file's grant is pinned if one
/// of the close's mutations ends in doubt ([`note_tag_in_doubt`]). Other
/// closes (no lock of the caller on the file: an unrelated commit), and
/// other publications (an `fsync`, an `O_SYNC` write, answered when done,
/// so concurrent with any unlock until then), hold no new turn. A fenced
/// caller's `Err` is not lost: `current_tag` noted the lapse, and the
/// close's publish gate discards what it wrote.
pub fn begin_close(ino: Ino, ends_turn: bool) -> Publishing {
    let _ = current_tag();
    if !ends_turn {
        return Publishing { held: None };
    }
    let held = SCOPE.with(|s| {
        let mut s = s.borrow_mut();
        let st = s.as_mut()?;
        // A fenced caller's writes are discarded, never published.
        if matches!(st.tag, Some(Err(_))) {
            return None;
        }
        // Counted under the grant held on the file now, whatever the tag
        // names: it names, per minting sequencer, only the grant whose
        // window ends first, which may be another file's (a process
        // holding locks on A and B closes A under B's token) — or none,
        // when the closer's process is not told apart from its lock's
        // (`ends_turn` is the caller's own reading of the lock table).
        st.closes = Some(ino);
        let grant = st.locks.meta.locks().held(ino).map(|h| h.id)?;
        st.locks.meta.locks().publish_begin(grant);
        Some((st.locks.clone(), grant))
    });
    Publishing { held }
}

/// A publication a new local turn on its file waits for, under `grant`
/// (see [`begin_close`]); [`ClusterLocks::publishing`] for a grant
/// release's flush. Ends when dropped.
#[must_use]
pub struct Publishing {
    held: Option<(Arc<ClusterLocks>, GrantId)>,
}

impl Drop for Publishing {
    fn drop(&mut self) {
        if let Some((locks, grant)) = self.held.take() {
            locks.meta.locks().publish_end(grant, now());
        }
    }
}

impl ClusterLocks {
    /// The flush before `grant`'s release publishes its file under it
    /// (`LockFlush`): counted as [`begin_close`] counts a close's.
    pub fn publishing(self: &Arc<Self>, grant: GrantId) -> Publishing {
        self.meta.locks().publish_begin(grant);
        Publishing {
            held: Some((self.clone(), grant)),
        }
    }
}

thread_local! {
    static LAPSED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// A mutation on this thread was refused for a lapsed lock grant (the
/// fencing token): the publication it was part of is discarded
/// ([`take_lapsed`]).
pub fn note_lapsed() {
    LAPSED.with(|l| l.set(true));
}

/// Whether a mutation on this thread was refused for a lapsed lock grant
/// since the last call (clears it).
pub fn take_lapsed() -> bool {
    LAPSED.with(|l| l.replace(false))
}

/// One view's flush for a recalled lock grant (`Action::LockFlush`):
/// what `fsync` guarantees, for one inode.
pub trait LockFlush: Send + Sync {
    /// `grant`: the grant about to be released (phase 2: the flush's
    /// commit carries it as its fencing token).
    fn flush_for_lock(&self, ino: Ino, grant: GrantId) -> bool;
    /// The view's root directory: a refused replay of an op issued under
    /// a lock keeps its conflict copy under the deepest mounted view root
    /// above its file (`recovery::materialize_remote`).
    fn view_root(&self) -> Ino;
}

/// Every mounted view's flush, for the driver's `Action::LockFlush`: a
/// file's dirty data lives in the write state of whichever view wrote it.
/// Weak: a view's filesystem goes away with its FUSE session.
#[derive(Default)]
pub struct LockFlushers {
    views: Mutex<Vec<(u64, Weak<dyn LockFlush>)>>,
}

impl LockFlushers {
    pub fn register(&self, id: u64, view: Weak<dyn LockFlush>) {
        let mut v = self.views.lock().unwrap();
        v.retain(|(_, w)| w.strong_count() > 0);
        v.push((id, view));
    }

    pub fn unregister(&self, id: u64) {
        self.views.lock().unwrap().retain(|(i, _)| *i != id);
    }

    /// Flush `ino` in every live view (blocking; run it off the runtime).
    /// No live view: nothing in memory to flush.
    pub fn flush(&self, ino: Ino, grant: GrantId) -> bool {
        let views: Vec<Arc<dyn LockFlush>> = self
            .views
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(_, w)| w.upgrade())
            .collect();
        views.iter().all(|v| v.flush_for_lock(ino, grant))
    }

    /// Every live view's root directory.
    pub fn view_roots(&self) -> Vec<Ino> {
        self.views
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(_, w)| w.upgrade())
            .map(|v| v.view_root())
            .collect()
    }
}

/// The driver's hook for `Action::LockFlush` (blocking).
pub type LockFlushHook = Arc<dyn Fn(Ino, GrantId) -> bool + Send + Sync>;

/// The driver's view of the mounted views' roots
/// ([`LockFlushers::view_roots`]), for `Action::ConflictCopy`.
pub type ViewRootsHook = Arc<dyn Fn() -> Vec<Ino> + Send + Sync>;

// ---------------------------------------------------------------- wire

pub fn exclusive(mode: LockMode) -> bool {
    mode == LockMode::Exclusive
}

pub fn mode_of(exclusive: bool) -> LockMode {
    if exclusive {
        LockMode::Exclusive
    } else {
        LockMode::Shared
    }
}

pub fn grant_wire(id: GrantId) -> (u64, u64) {
    (id.node, id.seq)
}

pub fn grant_of((node, seq): (u64, u64)) -> GrantId {
    GrantId { node, seq }
}

pub fn outcome_wire(o: &LockOutcome) -> LockOutcomeWire {
    match o {
        LockOutcome::Granted {
            id,
            mode,
            ttl_ms,
            position,
        } => LockOutcomeWire::Granted {
            grant: grant_wire(*id),
            exclusive: exclusive(*mode),
            ttl_ms: *ttl_ms,
            position_seq: position.seq,
            position_pending: position.pending.map(|p| (p.epoch, p.jseq)),
            position_streams: position.streams_wire(),
        },
        LockOutcome::Waiting { retry_ms, grace } => LockOutcomeWire::Waiting {
            retry_ms: *retry_ms,
            grace: *grace,
        },
        LockOutcome::WouldBlock => LockOutcomeWire::WouldBlock,
        LockOutcome::NotOwner { owner } => LockOutcomeWire::NotOwner { owner: *owner },
        LockOutcome::Busy => LockOutcomeWire::Busy,
    }
}

pub fn outcome_of(w: LockOutcomeWire) -> LockOutcome {
    match w {
        LockOutcomeWire::Granted {
            grant,
            exclusive,
            ttl_ms,
            position_seq,
            position_pending,
            position_streams,
        } => LockOutcome::Granted {
            id: grant_of(grant),
            mode: mode_of(exclusive),
            ttl_ms,
            position: Position {
                seq: position_seq,
                pending: position_pending.map(|(epoch, jseq)| JournalPos { epoch, jseq }),
                streams: Default::default(),
            }
            .with_streams_wire(&position_streams),
        },
        LockOutcomeWire::Waiting { retry_ms, grace } => LockOutcome::Waiting { retry_ms, grace },
        LockOutcomeWire::WouldBlock => LockOutcome::WouldBlock,
        LockOutcomeWire::NotOwner { owner } => LockOutcome::NotOwner { owner },
        LockOutcomeWire::Busy => LockOutcome::Busy,
    }
}

pub fn renew_entries_wire(entries: &[LockRenewEntry]) -> Vec<LockRenewWire> {
    entries
        .iter()
        .map(|e| LockRenewWire {
            ino: e.ino,
            grant: grant_wire(e.grant),
            exclusive: exclusive(e.mode),
            late: e.late,
        })
        .collect()
}

pub fn renew_entries_of(entries: Vec<LockRenewWire>) -> Vec<LockRenewEntry> {
    entries
        .into_iter()
        .map(|e| LockRenewEntry {
            ino: e.ino,
            grant: grant_of(e.grant),
            mode: mode_of(e.exclusive),
            late: e.late,
        })
        .collect()
}

pub fn renew_results_wire(
    results: &[(Ino, GrantId, LockRenewResult)],
) -> Vec<(u64, (u64, u64), LockRenewResultWire)> {
    results
        .iter()
        .map(|(ino, id, r)| {
            let r = match *r {
                LockRenewResult::Ok {
                    ttl_ms,
                    recalled,
                    id,
                    mode,
                } => LockRenewResultWire::Ok {
                    ttl_ms,
                    recalled,
                    id: grant_wire(id),
                    exclusive: exclusive(mode),
                },
                LockRenewResult::Lost => LockRenewResultWire::Lost,
                LockRenewResult::NotOwner { owner } => LockRenewResultWire::NotOwner { owner },
            };
            (*ino, grant_wire(*id), r)
        })
        .collect()
}

pub fn renew_results_of(
    results: Vec<(u64, (u64, u64), LockRenewResultWire)>,
) -> Vec<(Ino, GrantId, LockRenewResult)> {
    results
        .into_iter()
        .map(|(ino, id, r)| {
            let r = match r {
                LockRenewResultWire::Ok {
                    ttl_ms,
                    recalled,
                    id,
                    exclusive,
                } => LockRenewResult::Ok {
                    ttl_ms,
                    recalled,
                    id: grant_of(id),
                    mode: mode_of(exclusive),
                },
                LockRenewResultWire::Lost => LockRenewResult::Lost,
                LockRenewResultWire::NotOwner { owner } => LockRenewResult::NotOwner { owner },
            };
            (ino, grant_of(id), r)
        })
        .collect()
}

pub fn test_outcome_wire(o: LockTestOutcome) -> LockTestOutcomeWire {
    match o {
        LockTestOutcome::Free => LockTestOutcomeWire::Free,
        LockTestOutcome::Held { node, mode } => LockTestOutcomeWire::Held {
            node,
            exclusive: exclusive(mode),
        },
        LockTestOutcome::NotOwner { owner } => LockTestOutcomeWire::NotOwner { owner },
    }
}

pub fn test_outcome_of(w: LockTestOutcomeWire) -> LockTestOutcome {
    match w {
        LockTestOutcomeWire::Free => LockTestOutcome::Free,
        LockTestOutcomeWire::Held { node, exclusive } => LockTestOutcome::Held {
            node,
            mode: mode_of(exclusive),
        },
        LockTestOutcomeWire::NotOwner { owner } => LockTestOutcome::NotOwner { owner },
    }
}

/// Grants handed over with a delegation or mirrored to a backup
/// (postcard; an undecodable batch is empty — the receiver's grace and
/// the holders' reclaims cover what it would have carried).
pub fn grants_wire(grants: &[GrantWire]) -> Vec<u8> {
    if grants.is_empty() {
        return Vec::new();
    }
    postcard::to_allocvec(grants).unwrap_or_default()
}

/// What is left of each carried grant's record on the wire
/// (`PeerMsg::DelegRenewed::lock_carry`, `LockHandback::carry`;
/// postcard, empty: none). An undecodable one is empty: the receiver
/// restamps the grants, which is never sooner.
pub fn carry_wire(carry: &[(constellation_meta::locks::GrantId, u64)]) -> Vec<u8> {
    if carry.is_empty() {
        return Vec::new();
    }
    postcard::to_allocvec(carry).unwrap_or_default()
}

pub fn carry_of(bytes: &[u8]) -> Vec<(constellation_meta::locks::GrantId, u64)> {
    if bytes.is_empty() {
        return Vec::new();
    }
    postcard::from_bytes(bytes).unwrap_or_default()
}

/// A lock floor on the wire (empty: none).
pub fn floor_wire(p: &constellation_meta::Position) -> Vec<u8> {
    if *p == constellation_meta::Position::ZERO {
        return Vec::new();
    }
    p.to_postcard()
}

pub fn floor_of(bytes: &[u8]) -> constellation_meta::Position {
    if bytes.is_empty() {
        return constellation_meta::Position::ZERO;
    }
    constellation_meta::Position::from_postcard(bytes)
}

pub fn grants_of(bytes: &[u8]) -> Vec<GrantWire> {
    if bytes.is_empty() {
        return Vec::new();
    }
    postcard::from_bytes(bytes).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_parse_and_need_p2p() {
        assert_eq!(cluster_flag(Some("local")).unwrap(), Some(false));
        assert_eq!(cluster_flag(Some("Cluster")).unwrap(), Some(true));
        assert!(cluster_flag(Some("global")).is_err());
        assert!(cluster_effective(None, true).unwrap());
        assert!(!cluster_effective(None, false).unwrap());
        assert!(!cluster_effective(Some(false), true).unwrap());
        let e = cluster_effective(Some(true), false).unwrap_err();
        assert!(e.to_string().contains("use --locks local"));
    }

    #[test]
    fn wire_forms_round_trip() {
        let position = Position {
            seq: 9,
            pending: Some(JournalPos { epoch: 3, jseq: 4 }),
            streams: Default::default(),
        };
        let granted = LockOutcome::Granted {
            id: GrantId { node: 2, seq: 7 },
            mode: LockMode::Exclusive,
            ttl_ms: 5000,
            position,
        };
        assert_eq!(outcome_of(outcome_wire(&granted)), granted);
        for o in [
            LockOutcome::Waiting {
                retry_ms: 3,
                grace: true,
            },
            LockOutcome::Waiting {
                retry_ms: 3,
                grace: false,
            },
            LockOutcome::WouldBlock,
            LockOutcome::NotOwner { owner: 4 },
            LockOutcome::Busy,
        ] {
            assert_eq!(outcome_of(outcome_wire(&o)), o);
        }
        let entries = vec![LockRenewEntry {
            ino: 5,
            grant: GrantId { node: 1, seq: 2 },
            mode: LockMode::Shared,
            late: true,
        }];
        assert_eq!(renew_entries_of(renew_entries_wire(&entries)), entries);
        let results = vec![
            (
                5,
                GrantId { node: 1, seq: 2 },
                LockRenewResult::Ok {
                    ttl_ms: 10,
                    recalled: true,
                    id: GrantId { node: 1, seq: 9 },
                    mode: LockMode::Exclusive,
                },
            ),
            (6, GrantId { node: 1, seq: 3 }, LockRenewResult::Lost),
            (
                7,
                GrantId { node: 1, seq: 4 },
                LockRenewResult::NotOwner { owner: 8 },
            ),
        ];
        assert_eq!(renew_results_of(renew_results_wire(&results)), results);
        for o in [
            LockTestOutcome::Free,
            LockTestOutcome::Held {
                node: 3,
                mode: LockMode::Shared,
            },
            LockTestOutcome::NotOwner { owner: 1 },
        ] {
            assert_eq!(test_outcome_of(test_outcome_wire(o)), o);
        }
        let grants = vec![GrantWire {
            id: GrantId { node: 1, seq: 2 },
            node: 3,
            ino: 4,
            mode: LockMode::Exclusive,
            until_ms: 99,
            recalled: false,
            gen: 0,
            confirmed_ms: constellation_meta::locks::Grant::UNCONFIRMED,
            settled: true,
            lapsed: true,
        }];
        assert_eq!(grants_of(&grants_wire(&grants)), grants);
        assert!(grants_of(&[]).is_empty());
        let carry = vec![
            (GrantId { node: 1, seq: 2 }, 0),
            (GrantId { node: 7, seq: 9 }, 20_999),
        ];
        assert_eq!(carry_of(&carry_wire(&carry)), carry);
        assert!(carry_wire(&[]).is_empty());
        assert!(carry_of(&[]).is_empty());
        // Undecodable: nothing carried (the receiver restamps).
        assert!(carry_of(&[0xff, 0xff, 0xff]).is_empty());
    }
}
