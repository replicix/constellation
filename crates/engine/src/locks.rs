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
//!
//! A blocked lock wait ends with `EINTR` when the frontend cancels it (the
//! FUSE adapter does on any signal, as POSIX has `F_SETLKW` do): checked
//! between the polls of a wait on a conflicting lock of this node and
//! before each request to the sequencer. A request the owner has parked
//! (another node holds a conflicting grant) is not withdrawn: that wait
//! still returns only once the grant comes.
//!
//! Not supported: POSIX deadlock detection (`EDEADLK`) — two owners
//! waiting on each other wait until a signal ends one wait, as they do
//! with `flock`.

use crate::sync::SyncRequest;
use anyhow::{bail, Result};
use constellation_authority::{
    LockAnswer, LockOutcome, LockRenewEntry, LockRenewResult, LockTestAnswer, LockTestOutcome,
};
use constellation_fs_core::Ino;
use constellation_meta::locks::{
    Grant, GrantId, LocalLock, LocalOutcome, LockMode, LockTag, OwnerFenced,
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

/// The clock grants are honoured by and fencing tokens are checked at
/// (unix ms). Tests move it per thread ([`advance_test_clock`]) instead
/// of sleeping past a grant.
pub(crate) fn now_ms() -> i64 {
    let now = constellation_store_s3::lease::now_unix_ms();
    #[cfg(test)]
    let now = now + TEST_CLOCK_OFFSET_MS.with(|c| c.get());
    now
}

#[cfg(test)]
thread_local! {
    static TEST_CLOCK_OFFSET_MS: std::cell::Cell<i64> = const { std::cell::Cell::new(0) };
}

/// Move this thread's [`now_ms`] forward (tests only).
#[cfg(test)]
pub(crate) fn advance_test_clock(ms: i64) {
    TEST_CLOCK_OFFSET_MS.with(|c| c.set(c.get() + ms));
}

/// The process of task `pid` (FUSE names a request by its thread),
/// named across pid reuse: `(thread group id, start time)` — `(pid, 0)`
/// where `/proc` cannot tell (gone, or no `/proc`).
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
    pub fn fenced(&self, ino: Ino) -> bool {
        self.meta.locks().fenced(ino, now_ms())
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
    pub fn owner_fenced(&self, pid: Option<u32>, lock_owner: Option<u64>) -> bool {
        let locks = self.meta.locks();
        // The table's state before the clock: `local_inos` first, so the
        // no-lock fast path reads no clock at all.
        if locks.lockers_none() {
            return false;
        }
        let now = now_ms();
        if !locks.owner_fence_armed(now) {
            return false;
        }
        let fenced = locks.fenced_owners(now);
        if fenced.is_empty() {
            return false;
        }
        let hit = lock_owner.is_some_and(|o| fenced.iter().any(|f| f.owner == o))
            || pid.is_some_and(|pid| {
                let owners: Vec<(u32, u64)> = fenced
                    .iter()
                    .filter(|f| f.pid > 1)
                    .map(|f| (f.pid, f.pid_start))
                    .collect();
                !owners.is_empty()
                    && self
                        .ancestry(pid, &self.locker_procs())
                        .iter()
                        .any(|p| owners.iter().any(|o| same_process(*o, *p)))
            });
        if hit {
            locks.note_owner_fenced_op();
        }
        hit
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
        locks.owner_tag(&owners, now_ms())
    }

    /// Whether `ino`'s dirty data must be discarded rather than
    /// published (`Some(fenced)`; see `LockTables::take_discard`).
    pub fn take_discard(&self, ino: Ino) -> Option<bool> {
        self.meta.locks().take_discard(ino, now_ms())
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
    pub fn test(
        &self,
        ino: Ino,
        owner: u64,
        start: u64,
        end: u64,
        write: bool,
    ) -> Option<(u64, u64, bool, u32)> {
        match self
            .meta
            .locks()
            .local_test(ino, owner, write, start, end, now_ms())
        {
            LocalOutcome::Conflict(l) => Some((l.start, l.end.min(OFFSET_MAX), l.write, l.pid)),
            // (`local_test` never answers `Predecessor`: only a lock
            // waits for an earlier turn.)
            LocalOutcome::Done | LocalOutcome::Predecessor => None,
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
            .local_unlock(ino, owner, start, end, now_ms())
        {
            self.idle(ino);
        }
    }

    /// A close (`flush`) or a `flock` release: drop every lock of `owner`
    /// on `ino` (POSIX: closing any descriptor drops the process's locks
    /// on the file). Returns whether no local lock is left on `ino`; the
    /// caller then calls [`Self::idle`] once the file's data is flushed.
    pub fn drop_owner(&self, ino: Ino, owner: u64) -> bool {
        self.meta.locks().local_release_owner(ino, owner, now_ms())
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
    /// the wait with `Intr` (see the module doc for what it reaches).
    ///
    /// `done` answers the request (the view's completes the op's
    /// responder), on this thread or on the waiter's.
    pub fn lock<F>(
        self: &Arc<Self>,
        ino: Ino,
        lock: LocalLock,
        sleep: bool,
        cancel: Option<CancelToken>,
        done: F,
    ) where
        F: FnOnce(Result<(), Code>) + Send + 'static,
    {
        if !sleep {
            done(self.set(ino, lock, false, None));
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
                done(this.set(ino, lock, true, cancel.as_ref()))
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
    ) -> Result<(), Code> {
        self.set(ino, lock, sleep, cancel)
    }

    fn set(
        &self,
        ino: Ino,
        mut lock: LocalLock,
        sleep: bool,
        cancel: Option<&CancelToken>,
    ) -> Result<(), Code> {
        // FUSE names the locking *thread*; the owner fence (and `getlk`)
        // need its process, named across pid reuse.
        (lock.pid, lock.pid_start) = process_of(lock.pid);
        let mut asked = false;
        let r = self.set_inner(ino, lock, sleep, cancel, &mut asked);
        if r.is_err() && asked {
            // A grant may have come for this request (or its answer was
            // lost after the core installed it) that it will now never
            // use: the grant must not wait for that first lock — recalled,
            // it would be renewed for ever and its waiters would wait for
            // ever.
            if self.meta.locks().abandon_first_use(ino, now_ms()) {
                self.idle(ino);
            }
        }
        r
    }

    fn set_inner(
        &self,
        ino: Ino,
        lock: LocalLock,
        sleep: bool,
        cancel: Option<&CancelToken>,
        asked: &mut bool,
    ) -> Result<(), Code> {
        let cancelled = || cancel.is_some_and(CancelToken::is_cancelled);
        let mut rounds = 0u32;
        loop {
            match self.meta.locks().local_set(ino, lock, now_ms()) {
                LocalOutcome::Done => return Ok(()),
                LocalOutcome::Conflict(_) | LocalOutcome::Predecessor => {
                    if !sleep {
                        return Err(Code::Again);
                    }
                    if cancelled() {
                        return Err(Code::Intr);
                    }
                    std::thread::sleep(LOCAL_POLL);
                }
                LocalOutcome::NeedGrant(mode) => {
                    if !sleep && rounds >= TRY_ROUNDS {
                        return Err(Code::Again);
                    }
                    if cancelled() {
                        return Err(Code::Intr);
                    }
                    if rounds >= 2 {
                        // A grant that keeps being overtaken by recalls:
                        // back off a little instead of spinning on the
                        // sequencer.
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
                            reply,
                        })
                        .map_err(|_| Code::Io)?;
                    match answer.blocking_recv().map_err(|_| Code::Io)? {
                        LockAnswer::Granted { position } => self.granted(ino, &position),
                        LockAnswer::WouldBlock => {
                            if !sleep {
                                return Err(Code::Again);
                            }
                            // The owner parks blocking requests, so this
                            // is a race with a recall; ask again shortly.
                            std::thread::sleep(LOCAL_POLL);
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
            locks.meta.locks().tag_end(&last, in_doubt, now_ms());
        }
    }
}

/// Who an op deferred to another thread was issued by, to re-enter its
/// scope there ([`ScopeCaller::enter`]).
pub struct ScopeCaller {
    locks: Arc<ClusterLocks>,
    pid: Option<u32>,
    lock_owner: Option<u64>,
}

impl ScopeCaller {
    pub fn enter(&self) -> TagScope {
        self.locks.tag_scope(self.pid, self.lock_owner)
    }
}

impl ClusterLocks {
    /// Enter the fencing-token scope of an op by `pid` / `lock_owner` on
    /// this thread (see [`ScopeState`]). One relaxed load, and nothing
    /// else, while no local lock exists on this node.
    pub fn tag_scope(self: &Arc<Self>, pid: Option<u32>, lock_owner: Option<u64>) -> TagScope {
        if self.meta.locks().lockers_none() {
            return TagScope::inert();
        }
        TagScope::enter(ScopeState {
            locks: self.clone(),
            pid,
            lock_owner,
            tag: None,
            sent: LockTag::NONE,
            doubt: LockTag::NONE,
            in_doubt: false,
        })
    }

    /// A scope whose mutations carry `tag` whoever runs them: a recalled
    /// grant's flush (`Action::LockFlush`), on the driver's thread,
    /// publishes the locked file under the grant it is about to release.
    pub fn fixed_tag_scope(self: &Arc<Self>, tag: LockTag) -> TagScope {
        if tag.is_empty() {
            return TagScope::inert();
        }
        self.meta.locks().tag_begin(&tag, now_ms());
        TagScope::enter(ScopeState {
            locks: self.clone(),
            pid: None,
            lock_owner: None,
            tag: Some(Ok(tag)),
            sent: LockTag::NONE,
            doubt: LockTag::NONE,
            in_doubt: false,
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
                let tag = st.locks.caller_tag(st.pid, st.lock_owner);
                if let Ok(t) = &tag {
                    st.locks.meta.locks().tag_begin(t, now_ms());
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
            let fresh = st.locks.meta.locks().refresh_tag(base, now_ms());
            st.sent = fresh.clone();
            fresh
        });
        tag.map_err(|OwnerFenced| {
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
pub fn reissue_after_lapse() -> bool {
    SCOPE.with(|s| {
        let s = s.borrow();
        let Some(st) = s.as_ref() else {
            return false;
        };
        if st.sent.is_empty() {
            return false;
        }
        let now = now_ms();
        let locks = st.locks.meta.locks();
        let fresh = locks.refresh_tag(&st.sent, now);
        fresh != st.sent && locks.tag_honoured(&fresh, now)
    })
}

/// A tagged mutation of this op ended in doubt (it may still execute
/// somewhere): its grants are not released before its tokens' windows
/// are over.
pub fn note_tag_in_doubt() {
    SCOPE.with(|s| {
        if let Some(st) = s.borrow_mut().as_mut() {
            if st
                .tag
                .as_ref()
                .is_some_and(|t| t.as_ref().is_ok_and(|t| !t.is_empty()))
            {
                st.in_doubt = true;
                st.doubt = st.sent.clone();
            }
        }
    });
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
        LockOutcome::Waiting { retry_ms } => LockOutcomeWire::Waiting {
            retry_ms: *retry_ms,
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
        LockOutcomeWire::Waiting { retry_ms } => LockOutcome::Waiting { retry_ms },
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
pub fn grants_wire(grants: &[Grant]) -> Vec<u8> {
    if grants.is_empty() {
        return Vec::new();
    }
    postcard::to_allocvec(grants).unwrap_or_default()
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

pub fn grants_of(bytes: &[u8]) -> Vec<Grant> {
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
            LockOutcome::Waiting { retry_ms: 3 },
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
        let grants = vec![Grant {
            id: GrantId { node: 1, seq: 2 },
            node: 3,
            ino: 4,
            mode: LockMode::Exclusive,
            until_ms: 99,
            recalled: false,
            gen: 0,
            confirmed_ms: Grant::UNCONFIRMED,
        }];
        assert_eq!(grants_of(&grants_wire(&grants)), grants);
        assert!(grants_of(&[]).is_empty());
    }
}
