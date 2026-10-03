//! Plan 39: an `fsync` that behaves like an NFS `hard` mount.
//!
//! **The contract.** An `fsync` (and an `O_SYNC`/`O_DSYNC` write, which is
//! a write plus one) returns `0` only once what it covers is as durable as
//! the mount's `--fsync-mode` says, and it never returns `EIO` merely
//! because S3 is *transiently* unreachable: it keeps retrying, as nfs(5)'s
//! `hard` mounts keep retrying an RPC. A failure the classifier calls
//! permanent (`constellation_store_s3::classify`: access denied, no such
//! bucket, a disabled KMS key, content this node lost) answers `EIO` at
//! once. In every failed case the data stays where it was — the chunks in
//! the local cache and in the durable `pending_upload` rows, the journal
//! unshipped — the background rounds keep retrying it, and the next
//! `fsync` waits for it again: a failed `fsync` is never followed by a
//! false success (the "fsyncgate" lesson: never drop dirty data on error,
//! never let a later `fsync` report it durable when it is not).
//!
//! **What changed and what did not.** What each `--fsync-mode` waits *for*
//! is unchanged (plan 39 §6 leaves `local`'s chunk wait to the
//! maintainer). What changed is what happens while it waits: the
//! attempt-counted give-ups on the sync path — the inode drain's three
//! PUT tries, each wrapping object_store's own retry series, the journal
//! barrier's one round, a forwarded commit left in doubt — no longer end
//! the `fsync`. A failed attempt is classified, and a transient one is
//! retried after a capped exponential backoff with jitter ([`Backoff`]:
//! 100 ms doubling to 5 s), re-using the same machinery (the drain, its
//! peer handoff, the round) rather than a second uploader. Background
//! uploads, `close()` and `--write-mode` behave exactly as before: only
//! the `fsync` family runs inside a [`Scope`].
//!
//! **Bounded, on request.** Three things end a wait before durability:
//!
//! - a cancelled [`CancelToken`]: `EINTR`, the data still pending. The
//!   token means "the caller is gone", and each frontend decides when that
//!   is: the FUSE adapter cancels it only once the calling thread has a
//!   fatal signal pending (killable, as nfs(5) `hard` is; a timer or a
//!   handled `SIGINT` keeps waiting — plan 39 §3.3);
//! - the operator's opt-in soft timeout (`--fsync-timeout`,
//!   `CONSTELLATION_FSYNC_TIMEOUT`): `EIO` once it elapses. nfs(5) warns
//!   that `soft` "can cause silent data corruption"; here it cannot lose
//!   data (nothing is dropped), but an application that treats `EIO` as
//!   "gone" may, which is the same trade-off: responsiveness over
//!   integrity;
//! - the kernel's own FUSE request timeout (Linux 6.15+:
//!   `fs.fuse.default_request_timeout` / `max_request_timeout`), which,
//!   when it fires, aborts the *whole connection*. When the host sets one,
//!   every wait is capped just below it ([`kernel_cap`]), logged once.
//!
//! **Observability.** A wait past ten seconds — counted from the start of
//! the call, so a first attempt that is itself slow (object_store's own
//! retry series) counts — is logged once per inode at warn ("S3
//! unreachable, still trying"), and once at info when it ends;
//! [`FsyncWaits::status`] feeds `node.status.fsync` and `/metrics`.
//!
//! **Mechanics.** The retry loop is [`FsyncWaits::run`]. The waits inside
//! one attempt (the drain's and the barrier's replies from the sync task)
//! do not know they run on the `fsync` path: a [`Scope`] on the thread
//! tells them (`recv`), so they poll the interrupt and the deadline while
//! they wait, and record why they failed (`note`) for the loop to decide.
//! Outside a scope they block as they always did.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use constellation_fs_core::Ino;
use constellation_types::Code;
use constellation_vfs::CancelToken;

pub use crate::sync::ErrorClass;

/// `CONSTELLATION_FSYNC_TIMEOUT`: the opt-in soft timeout (`--fsync-timeout`
/// wins over it).
pub const FSYNC_TIMEOUT_ENV: &str = "CONSTELLATION_FSYNC_TIMEOUT";

/// A wait this long is logged (once per inode, at warn).
pub const WARN_AFTER: Duration = Duration::from_secs(10);

/// The backoff between attempts: first and largest delay.
pub const BACKOFF_FIRST: Duration = Duration::from_millis(100);
pub const BACKOFF_CAP: Duration = Duration::from_secs(5);

/// How often a wait inside a scope looks at its interrupt and deadline.
const POLL: Duration = Duration::from_millis(50);

/// Parse a duration as the CLI and the environment give it: `500ms`,
/// `2s`, `5m`, `1h`, or a bare number of seconds. `0`, `off`, `none` and
/// `hard` mean no timeout (`Ok(None)`).
pub fn parse_timeout(raw: &str) -> Result<Option<Duration>, String> {
    let raw = raw.trim();
    if matches!(raw, "" | "0" | "off" | "none" | "hard") {
        return Ok(None);
    }
    let split = raw
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(raw.len());
    let (number, unit) = raw.split_at(split);
    let value: f64 = number
        .parse()
        .map_err(|_| format!("invalid duration {raw:?} (expected e.g. 30s, 500ms, 2m)"))?;
    let scale = match unit.trim() {
        "" | "s" | "sec" | "secs" => 1.0,
        "ms" => 0.001,
        "m" | "min" => 60.0,
        "h" => 3600.0,
        other => return Err(format!("invalid duration unit {other:?} in {raw:?}")),
    };
    let secs = value * scale;
    if !secs.is_finite() || secs < 0.0 {
        return Err(format!("invalid duration {raw:?}"));
    }
    Ok((secs > 0.0).then(|| Duration::from_secs_f64(secs)))
}

/// The kernel's FUSE request timeout for a connection that, like ours,
/// sets none itself (fuser never offers `FUSE_REQUEST_TIMEOUT`):
/// `default_request_timeout`, capped by `max_request_timeout`, which on
/// its own also opts every connection in (Documentation/admin-guide/
/// sysctl/fs.rst). `None`: no timeout (both 0, or a kernel before 6.15
/// without the files).
pub fn kernel_request_timeout_secs(default: u64, max: u64) -> Option<u64> {
    match (default, max) {
        (0, 0) => None,
        (d, 0) => Some(d),
        (0, m) => Some(m),
        (d, m) => Some(d.min(m)),
    }
}

/// How long an `fsync` may wait under a kernel request timeout of
/// `timeout`: a fifth below it, at most five seconds below, so the answer
/// reaches the kernel before its timer (which also counts the time the
/// request queued before the daemon read it) aborts the connection.
pub fn kernel_cap(timeout: Duration) -> Duration {
    timeout.saturating_sub((timeout / 5).min(Duration::from_secs(5)))
}

fn read_sysctl(name: &str) -> u64 {
    std::fs::read_to_string(format!("/proc/sys/fs/fuse/{name}"))
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

/// The host's kernel FUSE request timeout, if one is set.
pub fn host_kernel_request_timeout() -> Option<Duration> {
    kernel_request_timeout_secs(
        read_sysctl("default_request_timeout"),
        read_sysctl("max_request_timeout"),
    )
    .map(Duration::from_secs)
}

/// The soft timeout from the environment (a bad value is warned about
/// and ignored: a stale profile must not brick a mount).
pub fn timeout_from_env() -> Option<Duration> {
    let raw = std::env::var(FSYNC_TIMEOUT_ENV).ok()?;
    match parse_timeout(&raw) {
        Ok(timeout) => timeout,
        Err(error) => {
            tracing::warn!(%error, "ignoring {FSYNC_TIMEOUT_ENV}");
            None
        }
    }
}

/// Capped exponential backoff with full jitter: the n-th delay is drawn
/// from `[d/2, d]` with `d = min(cap, first · 2ⁿ)`, so many `fsync`s
/// cut off together do not retry in lockstep.
#[derive(Debug, Clone)]
pub struct Backoff {
    next: Duration,
    cap: Duration,
}

impl Backoff {
    pub fn new(first: Duration, cap: Duration) -> Self {
        Self { next: first, cap }
    }

    pub fn next_delay(&mut self) -> Duration {
        let d = self.next.min(self.cap);
        self.next = (self.next * 2).min(self.cap);
        d / 2 + d.mul_f64(crate::view::jitter() / 2.0)
    }
}

/// Why one attempt failed, as the waits inside it saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    /// Worth waiting out.
    Transient(String),
    /// Not without an operator: `EIO` now.
    Permanent(String),
    /// The application gave up (`EINTR`).
    Interrupted,
    /// The soft timeout or the kernel cap elapsed (`EIO`).
    TimedOut,
}

struct ScopeState {
    cancel: Option<CancelToken>,
    deadline: Option<Instant>,
    failure: Option<Failure>,
    /// The wait's once-per-inode warning, checked while a reply is awaited.
    warn: Option<Warn>,
}

thread_local! {
    static SCOPE: RefCell<Option<ScopeState>> = const { RefCell::new(None) };
}

/// The current thread runs one `fsync` attempt (see the module doc).
pub struct Scope(());

impl Scope {
    fn enter(cancel: Option<CancelToken>, deadline: Option<Instant>, warn: Option<Warn>) -> Scope {
        SCOPE.with(|s| {
            *s.borrow_mut() = Some(ScopeState {
                cancel,
                deadline,
                failure: None,
                warn,
            })
        });
        Scope(())
    }

    /// What the attempt's waits recorded (the first failure wins).
    fn take(&self) -> Option<Failure> {
        SCOPE.with(|s| s.borrow_mut().as_mut().and_then(|st| st.failure.take()))
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        SCOPE.with(|s| *s.borrow_mut() = None);
    }
}

/// Whether the current thread runs inside an `fsync` attempt.
pub fn in_scope() -> bool {
    SCOPE.with(|s| s.borrow().is_some())
}

/// Record why a wait inside the current attempt failed (a no-op outside
/// one). The first record of an attempt is kept.
pub fn note(failure: Failure) {
    SCOPE.with(|s| {
        if let Some(st) = s.borrow_mut().as_mut() {
            if st.failure.is_none() {
                st.failure = Some(failure);
            }
        }
    });
}

/// [`note`] a classified sync-task failure.
pub fn note_sync_failure(failure: &crate::sync::SyncFailure) {
    note(match failure.class {
        ErrorClass::Transient => Failure::Transient(failure.message.clone()),
        ErrorClass::Permanent => Failure::Permanent(failure.message.clone()),
    });
}

fn stop_reason() -> Option<Failure> {
    SCOPE.with(|s| {
        let s = s.borrow();
        let st = s.as_ref()?;
        if st.cancel.as_ref().is_some_and(|c| c.is_cancelled()) {
            return Some(Failure::Interrupted);
        }
        if st.deadline.is_some_and(|d| Instant::now() >= d) {
            return Some(Failure::TimedOut);
        }
        None
    })
}

fn warn_in_scope(message: &str) {
    let warn = SCOPE.with(|s| s.borrow().as_ref().and_then(|st| st.warn.clone()));
    if let Some(warn) = warn {
        warn.maybe(message);
    }
}

/// Wait for a sync-task reply. Outside a scope, exactly
/// `blocking_recv`. Inside one, the wait also ends on an interrupt or the
/// deadline (recorded with [`note`]), and a wait past [`WARN_AFTER`] is
/// logged even while the first attempt is still running; the request keeps
/// running in the background, its reply dropped. `Err(())`: no reply.
pub(crate) fn recv<T>(
    rt: &tokio::runtime::Handle,
    mut rx: tokio::sync::oneshot::Receiver<T>,
) -> Result<T, ()> {
    if !in_scope() {
        return rx.blocking_recv().map_err(|_| ());
    }
    loop {
        if let Some(stop) = stop_reason() {
            note(stop);
            return Err(());
        }
        match rt.block_on(async { tokio::time::timeout(POLL, &mut rx).await }) {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(_)) => return Err(()),
            Err(_) => warn_in_scope("no answer yet (S3 or the sequencer slow or unreachable)"),
        }
    }
}

/// Counters for `node.status.fsync` (see [`FsyncWaits::status`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FsyncWaitStatus {
    /// `hard`, or `soft` with a timeout.
    pub mode: &'static str,
    /// The soft timeout (0: none).
    pub timeout_ms: u64,
    /// The cap the kernel's FUSE request timeout imposes (0: none).
    pub kernel_cap_ms: u64,
    /// `fsync`s (and `O_SYNC` writes) waiting for durability now, counted
    /// from the start of the call.
    pub waiting: u64,
    /// How long the oldest of them has waited (0: none).
    pub longest_wait_ms: u64,
    /// The longest any `fsync` has waited since start.
    pub max_wait_ms: u64,
    /// `fsync`s that had to retry at least once.
    pub waited: u64,
    /// Attempts retried after a transient failure.
    pub retries: u64,
    /// `fsync`s answered `EIO` by the soft timeout or the kernel cap.
    pub timeouts: u64,
    /// `fsync`s answered `EIO` by a permanent failure.
    pub permanent_errors: u64,
    /// `fsync`s answered `EINTR`.
    pub interrupted: u64,
}

/// A node's `fsync` policy and its waits (see the module doc). Shared by
/// every view of an engine.
pub struct FsyncWaits {
    timeout: Option<Duration>,
    kernel_cap: Option<Duration>,
    next_id: AtomicU64,
    /// Waits in progress: id → (start, inode).
    waiting: Mutex<HashMap<u64, (Instant, Ino)>>,
    /// Inodes whose wait was logged at warn, until it ends.
    warned: Arc<Mutex<HashSet<Ino>>>,
    max_wait_ms: AtomicU64,
    waited: AtomicU64,
    retries: AtomicU64,
    timeouts: AtomicU64,
    permanent_errors: AtomicU64,
    interrupted: AtomicU64,
}

impl FsyncWaits {
    /// The policy for a mount: `timeout` (`None`: wait forever), capped by
    /// the host's kernel FUSE request timeout when one is set.
    pub fn new(timeout: Option<Duration>) -> Self {
        let kernel = host_kernel_request_timeout();
        let waits = Self::with_kernel_timeout(timeout, kernel);
        if let (Some(kernel), Some(cap)) = (kernel, waits.kernel_cap) {
            tracing::warn!(
                kernel_timeout_s = kernel.as_secs(),
                cap_ms = cap.as_millis() as u64,
                "the kernel's FUSE request timeout is set (fs.fuse.default_request_timeout / \
                 max_request_timeout): an fsync waiting for S3 answers EIO just before it, \
                 rather than letting the kernel abort the whole connection"
            );
        }
        waits
    }

    pub fn with_kernel_timeout(timeout: Option<Duration>, kernel: Option<Duration>) -> Self {
        Self {
            timeout,
            kernel_cap: kernel.map(kernel_cap),
            next_id: AtomicU64::new(1),
            waiting: Mutex::new(HashMap::new()),
            warned: Arc::default(),
            max_wait_ms: AtomicU64::new(0),
            waited: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
            permanent_errors: AtomicU64::new(0),
            interrupted: AtomicU64::new(0),
        }
    }

    /// The longest an `fsync` may wait: the soft timeout or the kernel
    /// cap, whichever is shorter (`None`: forever).
    pub fn limit(&self) -> Option<Duration> {
        match (self.timeout, self.kernel_cap) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    pub fn status(&self) -> FsyncWaitStatus {
        let now = Instant::now();
        let waiting = self.waiting.lock().unwrap();
        FsyncWaitStatus {
            mode: if self.timeout.is_some() {
                "soft"
            } else {
                "hard"
            },
            timeout_ms: self.timeout.map_or(0, |t| t.as_millis() as u64),
            kernel_cap_ms: self.kernel_cap.map_or(0, |t| t.as_millis() as u64),
            waiting: waiting.len() as u64,
            longest_wait_ms: waiting
                .values()
                .map(|(started, _)| now.duration_since(*started).as_millis() as u64)
                .max()
                .unwrap_or(0),
            max_wait_ms: self.max_wait_ms.load(Relaxed),
            waited: self.waited.load(Relaxed),
            retries: self.retries.load(Relaxed),
            timeouts: self.timeouts.load(Relaxed),
            permanent_errors: self.permanent_errors.load(Relaxed),
            interrupted: self.interrupted.load(Relaxed),
        }
    }

    /// Run `attempt` until it succeeds, a failure is not worth waiting out,
    /// the application interrupts, or the limit elapses (see the module
    /// doc). `attempt` runs inside a [`Scope`]; an `Err` it returns with no
    /// failure recorded is a refusal that has nothing to do with waiting
    /// (a local error, a lock fence) and is returned as it is.
    pub fn run(
        &self,
        ino: Ino,
        cancel: Option<CancelToken>,
        mut attempt: impl FnMut() -> Result<(), Code>,
    ) -> Result<(), Code> {
        let started = Instant::now();
        let deadline = self.limit().map(|limit| started + limit);
        let mut backoff = Backoff::new(BACKOFF_FIRST, BACKOFF_CAP);
        // Listed (`waiting`, `longest_wait_ms`) and watched for the warning
        // from the start: a first attempt can itself take a minute and more
        // (object_store's retry series inside each PUT try).
        let wait = WaitEntry::new(self, ino, started);
        let mut retried = false;
        loop {
            let scope = Scope::enter(cancel.clone(), deadline, Some(wait.warn.clone()));
            let result = attempt();
            let failure = scope.take();
            drop(scope);
            let code = match result {
                Ok(()) => return Ok(()),
                Err(code) => code,
            };
            let message = match failure {
                None => return Err(code),
                Some(Failure::Interrupted) => {
                    self.interrupted.fetch_add(1, Relaxed);
                    return Err(Code::Intr);
                }
                Some(Failure::TimedOut) => return Err(self.timed_out(ino, started)),
                Some(Failure::Permanent(message)) => {
                    self.permanent_errors.fetch_add(1, Relaxed);
                    tracing::warn!(
                        ino,
                        error = %message,
                        "fsync failed: the object store refused it for a reason waiting \
                         will not fix (EIO; the data stays pending and is retried)"
                    );
                    return Err(Code::Io);
                }
                Some(Failure::Transient(message)) => message,
            };
            if !std::mem::replace(&mut retried, true) {
                self.waited.fetch_add(1, Relaxed);
            }
            wait.warn.maybe(&message);
            self.retries.fetch_add(1, Relaxed);
            // The backoff, cut short by an interrupt or the deadline.
            let until = Instant::now() + backoff.next_delay();
            loop {
                if cancel.as_ref().is_some_and(|c| c.is_cancelled()) {
                    self.interrupted.fetch_add(1, Relaxed);
                    return Err(Code::Intr);
                }
                let now = Instant::now();
                if deadline.is_some_and(|d| now >= d) {
                    return Err(self.timed_out(ino, started));
                }
                if now >= until {
                    break;
                }
                std::thread::sleep((until - now).min(POLL));
            }
        }
    }

    fn timed_out(&self, ino: Ino, started: Instant) -> Code {
        self.timeouts.fetch_add(1, Relaxed);
        tracing::warn!(
            ino,
            waited_ms = started.elapsed().as_millis() as u64,
            limit_ms = self.limit().map_or(0, |l| l.as_millis() as u64),
            "fsync timed out waiting for S3 (EIO; the data stays pending and is retried)"
        );
        Code::Io
    }
}

/// The warn-once-per-inode state of one wait, shared by its
/// [`WaitEntry`] and the scope of each of its attempts.
#[derive(Clone)]
struct Warn {
    ino: Ino,
    started: Instant,
    /// [`FsyncWaits::warned`].
    inodes: Arc<Mutex<HashSet<Ino>>>,
    /// This wait logged the inode's warning (and so logs its end).
    warned: Arc<std::sync::atomic::AtomicBool>,
}

impl Warn {
    fn maybe(&self, message: &str) {
        if self.warned.load(Relaxed) || self.started.elapsed() < WARN_AFTER {
            return;
        }
        if self.inodes.lock().unwrap().insert(self.ino) {
            self.warned.store(true, Relaxed);
            tracing::warn!(
                ino = self.ino,
                waited_s = self.started.elapsed().as_secs(),
                error = %message,
                "fsync: S3 unreachable, still trying"
            );
        }
    }
}

/// One `fsync` in progress: listed in `waiting` until it ends, however it
/// ends.
struct WaitEntry<'a> {
    waits: &'a FsyncWaits,
    id: u64,
    warn: Warn,
}

impl<'a> WaitEntry<'a> {
    fn new(waits: &'a FsyncWaits, ino: Ino, started: Instant) -> Self {
        let id = waits.next_id.fetch_add(1, Relaxed);
        waits.waiting.lock().unwrap().insert(id, (started, ino));
        Self {
            waits,
            id,
            warn: Warn {
                ino,
                started,
                inodes: waits.warned.clone(),
                warned: Arc::default(),
            },
        }
    }
}

impl Drop for WaitEntry<'_> {
    fn drop(&mut self) {
        let waited = self.warn.started.elapsed();
        self.waits.waiting.lock().unwrap().remove(&self.id);
        self.waits
            .max_wait_ms
            .fetch_max(waited.as_millis() as u64, Relaxed);
        if self.warn.warned.load(Relaxed)
            && self.waits.warned.lock().unwrap().remove(&self.warn.ino)
        {
            tracing::info!(
                ino = self.warn.ino,
                waited_s = waited.as_secs(),
                "fsync: done waiting for S3"
            );
        }
    }
}

/// The threads waiting `fsync`s finish on (plan 39 §3.3): a frontend that
/// can answer from another thread hands its `fsync` here, so a long S3
/// outage pins none of its workers — the ones that must stay free to
/// deliver the `FUSE_INTERRUPT` that ends such a wait. A pool of its own,
/// not the completion pool: its jobs wait unboundedly by design, and the
/// completion pool's must not (`crate::completion`'s module doc).
pub fn pool() -> &'static crate::completion::CompletionPool {
    static POOL: std::sync::OnceLock<crate::completion::CompletionPool> =
        std::sync::OnceLock::new();
    POOL.get_or_init(|| crate::completion::CompletionPool::named("fsync-wait", 1024))
}

/// A node's shared policy, for a view built outside an engine (tests).
pub fn default_waits() -> Arc<FsyncWaits> {
    static WAITS: std::sync::OnceLock<Arc<FsyncWaits>> = std::sync::OnceLock::new();
    WAITS
        .get_or_init(|| Arc::new(FsyncWaits::with_kernel_timeout(None, None)))
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn waits(timeout: Option<Duration>) -> FsyncWaits {
        FsyncWaits::with_kernel_timeout(timeout, None)
    }

    #[test]
    fn durations_parse_as_the_cli_takes_them() {
        assert_eq!(parse_timeout("2s"), Ok(Some(Duration::from_secs(2))));
        assert_eq!(parse_timeout("500ms"), Ok(Some(Duration::from_millis(500))));
        assert_eq!(parse_timeout("3"), Ok(Some(Duration::from_secs(3))));
        assert_eq!(parse_timeout("2m"), Ok(Some(Duration::from_secs(120))));
        for none in ["0", "off", "hard", ""] {
            assert_eq!(parse_timeout(none), Ok(None), "{none}");
        }
        assert!(parse_timeout("2 fortnights").is_err());
        assert!(parse_timeout("-1s").is_err());
    }

    #[test]
    fn the_kernel_timeout_is_read_as_the_kernel_applies_it() {
        assert_eq!(kernel_request_timeout_secs(0, 0), None);
        assert_eq!(kernel_request_timeout_secs(30, 0), Some(30));
        // `max` alone opts every connection in.
        assert_eq!(kernel_request_timeout_secs(0, 60), Some(60));
        assert_eq!(kernel_request_timeout_secs(90, 60), Some(60));
        assert_eq!(kernel_request_timeout_secs(30, 60), Some(30));
        // Answered before the kernel's timer fires.
        assert_eq!(kernel_cap(Duration::from_secs(60)), Duration::from_secs(55));
        assert_eq!(kernel_cap(Duration::from_secs(10)), Duration::from_secs(8));
        let w = FsyncWaits::with_kernel_timeout(
            Some(Duration::from_secs(120)),
            Some(Duration::from_secs(60)),
        );
        assert_eq!(w.limit(), Some(Duration::from_secs(55)));
        assert_eq!(waits(None).limit(), None);
    }

    #[test]
    fn backoff_doubles_to_its_cap_with_jitter() {
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_secs(5));
        let delays: Vec<Duration> = (0..12).map(|_| b.next_delay()).collect();
        assert!(delays[0] >= Duration::from_millis(50) && delays[0] <= Duration::from_millis(100));
        for d in &delays[8..] {
            assert!(*d >= Duration::from_millis(2500) && *d <= Duration::from_secs(5));
        }
    }

    /// The `hard` contract: transient failures are waited out, however
    /// many, and the call succeeds once the attempt does.
    #[test]
    fn transient_failures_are_retried_until_the_attempt_succeeds() {
        let w = waits(None);
        let calls = Cell::new(0);
        let result = w.run(7, None, || {
            calls.set(calls.get() + 1);
            if calls.get() < 4 {
                note(Failure::Transient("connection refused".into()));
                return Err(Code::Io);
            }
            Ok(())
        });
        assert_eq!(result, Ok(()));
        assert_eq!(calls.get(), 4);
        let s = w.status();
        assert_eq!((s.waited, s.retries, s.waiting), (1, 3, 0));
        assert!(s.max_wait_ms > 0);
    }

    /// A slow first attempt is a wait too: listed in `waiting` and
    /// `longest_wait_ms` while it runs, before anything failed.
    #[test]
    fn a_slow_first_attempt_is_listed_while_it_runs() {
        let w = waits(None);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel::<()>();
        std::thread::scope(|scope| {
            let w = &w;
            let run = scope.spawn(move || {
                w.run(7, None, || {
                    started_tx.send(()).unwrap();
                    finish_rx.recv().unwrap();
                    Ok(())
                })
            });
            started_rx.recv().unwrap();
            std::thread::sleep(Duration::from_millis(30));
            let s = w.status();
            assert_eq!(s.waiting, 1);
            assert!(s.longest_wait_ms >= 30, "{s:?}");
            finish_tx.send(()).unwrap();
            assert_eq!(run.join().unwrap(), Ok(()));
        });
        let s = w.status();
        assert_eq!((s.waiting, s.waited, s.retries), (0, 0, 0));
        assert!(s.max_wait_ms >= 30);
    }

    #[test]
    fn a_permanent_failure_is_eio_at_once() {
        let w = waits(None);
        let calls = Cell::new(0);
        let result = w.run(7, None, || {
            calls.set(calls.get() + 1);
            note(Failure::Permanent("AccessDenied".into()));
            Err(Code::Io)
        });
        assert_eq!(result, Err(Code::Io));
        assert_eq!(calls.get(), 1);
        assert_eq!(w.status().permanent_errors, 1);
    }

    /// A refusal no wait recorded (a local error, the lock fence) is
    /// returned as it was, unretried.
    #[test]
    fn a_refusal_that_is_not_a_wait_is_returned_unchanged() {
        let w = waits(None);
        assert_eq!(w.run(7, None, || Err(Code::NoSpace)), Err(Code::NoSpace));
    }

    #[test]
    fn the_soft_timeout_ends_the_wait_with_eio() {
        let w = waits(Some(Duration::from_millis(300)));
        let started = Instant::now();
        let result = w.run(7, None, || {
            note(Failure::Transient("timed out".into()));
            Err(Code::Io)
        });
        assert_eq!(result, Err(Code::Io));
        assert!(started.elapsed() >= Duration::from_millis(300));
        assert!(started.elapsed() < Duration::from_secs(3));
        assert_eq!(w.status().timeouts, 1);
    }

    #[test]
    fn an_interrupt_ends_the_wait_with_eintr() {
        let w = waits(None);
        let token = CancelToken::new();
        let cancel = token.clone();
        let canceller = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            cancel.cancel();
        });
        let result = w.run(7, Some(token), || {
            note(Failure::Transient("timed out".into()));
            Err(Code::Io)
        });
        canceller.join().unwrap();
        assert_eq!(result, Err(Code::Intr));
        assert_eq!(w.status().interrupted, 1);
    }

    /// A reply wait inside an attempt ends on the interrupt (the request
    /// keeps running; its reply is dropped), and outside one it blocks as
    /// before.
    #[test]
    fn a_reply_wait_inside_a_scope_ends_on_the_interrupt() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let w = waits(None);
        let token = CancelToken::new();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let mut rx = Some(rx);
        let cancel = token.clone();
        let canceller = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(150));
            cancel.cancel();
        });
        let result = w.run(7, Some(token), || {
            recv(rt.handle(), rx.take().unwrap()).map_err(|()| Code::Io)
        });
        canceller.join().unwrap();
        assert_eq!(result, Err(Code::Intr));
        drop(tx);
        let (tx, rx) = tokio::sync::oneshot::channel::<u8>();
        tx.send(3).unwrap();
        assert_eq!(recv(rt.handle(), rx), Ok(3));
    }
}
