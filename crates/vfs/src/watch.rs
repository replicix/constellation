//! The request watchdog, [`OpWatch`] (EC2 campaign 7, finding B-2;
//! generalised from the FUSE adapter's `fuse_watch` by plan 31 §6.8).
//!
//! A FUSE request the daemon reads and never answers hangs its caller
//! for good, uninterruptibly, and nothing reports it: the daemon is
//! healthy otherwise, `status` is green, the other files work. Campaign
//! 7 saw a `getattr` (from `git add`'s path walk) and a file's `read`
//! stay unanswered for the rest of the daemon's life, with the kernel's
//! `waiting` counter the only trace.
//!
//! Every op a view handles is registered here for its whole duration
//! (`OpWatch::enter`, at the top of each op, until its responder has
//! answered): the operation, the inode (its [`WatchKey`]), the OS thread,
//! and the *stage* the op last noted ([`stage`]: the wait it is about to
//! enter — a write shard, the inode's operation lock, the session wait, a
//! chunk fetch, the core's reply, ...). A monitor thread logs any op older
//! than `CONSTELLATION_FUSE_REQUEST_STALL_S` (default 30 s; 0 disables the
//! monitor, not the registry) at WARN, with all of that, again every
//! further threshold it stays, and once more when it finally completes.
//! `status` reports the count (`fuse_requests`), and the stalled ops
//! themselves ([`OpWatch::snapshot`]). The env var names and the log text
//! are unchanged from `fuse_watch`, so operators' tooling keeps working.
//!
//! A blocking lock request (`F_SETLKW`, `flock` without `LOCK_NB`) waits
//! for as long as another owner holds the file — by design, unbounded.
//! It is registered as `blocking` ([`OpWatch::enter_blocking`]) and
//! listed, never counted as a stall.
//!
//! With `CONSTELLATION_FUSE_STALL_BACKTRACE=1` the monitor also asks the
//! stalled thread for its backtrace (a `SIGUSR2`, handled by capturing
//! one and writing it to stderr): a diagnostic for a build with symbols,
//! off by default. Naming the thread and delivering the signal are host
//! services (`constellation_platform`'s `Process::current_thread`/
//! `enable_backtraces`/`request_backtrace`).
//!
//! **An instance, not a process global.** `fuse_watch` was one static
//! registry; an [`OpWatch`] is a value a host hands its views (the daemon
//! shares one across its views, so `status`'s cumulative counters outlive
//! any one view's unmount, as the static registry's did; a host may give
//! each view its own). Each has its own monitor thread, which ends when
//! the last handle to the registry is dropped.
//!
//! **Which view an op belongs to.** A view is handed a tagged handle to
//! the shared registry ([`OpWatch::for_view`]): the registry is still one,
//! but each op registered through the handle names its view's id, and the
//! view's labels come with the tag. `node.ops` filters and groups by it
//! ([`OpWatch::ops`]); ops registered through an untagged handle belong to
//! no view.
//!
//! **Which op the current thread is handling** is a thread-local here:
//! engine waits name their stage through [`stage`] without knowing the
//! registry, and a wait the engine moves to a thread of its own (a
//! blocking lock's `lock-wait` thread) carries the op along —
//! [`current`] on the spawning thread, [`Current::adopt`] (or
//! [`Watched::adopt`] by the owner of the registration) on the new one —
//! so the waiter's stages still name it.

use crate::types::Ino;
use constellation_platform::ThreadRef;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

/// What an op in flight is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WatchKey {
    /// An inode (as the frontend numbered it).
    Ino(Ino),
    /// The whole view (a view-wide barrier).
    View,
}

impl WatchKey {
    /// The inode `status` reports (0 for a view-wide op).
    pub fn ino(self) -> Ino {
        match self {
            WatchKey::Ino(ino) => ino,
            WatchKey::View => 0,
        }
    }
}

impl From<Ino> for WatchKey {
    fn from(ino: Ino) -> Self {
        WatchKey::Ino(ino)
    }
}

/// Which view the ops of a tagged [`OpWatch`] handle belong to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewTag {
    pub id: u64,
    /// The view's full label map (`ViewSpec::labels`).
    pub labels: BTreeMap<String, String>,
}

/// One op in flight.
struct Entry {
    id: u64,
    view: Option<Arc<ViewTag>>,
    op: &'static str,
    key: WatchKey,
    thread: ThreadRef,
    started: Instant,
    blocking: bool,
    stage: Mutex<&'static str>,
    /// Set once the monitor reported it as stalled.
    stalled: AtomicBool,
    /// How many thresholds it had crossed when last reported.
    reported: AtomicU64,
}

impl Entry {
    fn age(&self) -> Duration {
        self.started.elapsed()
    }

    fn stage(&self) -> &'static str {
        *self.stage.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn set_stage(&self, stage: &'static str) {
        *self.stage.lock().unwrap_or_else(|e| e.into_inner()) = stage;
    }
}

struct Registry {
    label: &'static str,
    next: AtomicU64,
    active: Mutex<HashMap<u64, Arc<Entry>>>,
    stalled_total: AtomicU64,
    stalled_completed: AtomicU64,
    threshold: Duration,
    backtraces: bool,
}

/// `CONSTELLATION_FUSE_REQUEST_STALL_S`: seconds an op may be in flight
/// before it is reported (default 30; 0 disables the monitor).
pub fn threshold_from_env() -> Duration {
    Duration::from_secs(
        std::env::var("CONSTELLATION_FUSE_REQUEST_STALL_S")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30),
    )
}

/// `CONSTELLATION_FUSE_STALL_BACKTRACE=1`: ask a stalled thread for its
/// backtrace.
pub fn backtraces_from_env() -> bool {
    std::env::var("CONSTELLATION_FUSE_STALL_BACKTRACE")
        .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
}

/// A registry of the ops in flight, and its stall monitor (module doc).
/// Cheap to clone: clones share the registry.
#[derive(Clone)]
pub struct OpWatch {
    inner: Arc<Registry>,
    view: Option<Arc<ViewTag>>,
}

impl OpWatch {
    /// A watchdog configured from the environment
    /// (`CONSTELLATION_FUSE_REQUEST_STALL_S`,
    /// `CONSTELLATION_FUSE_STALL_BACKTRACE`); `label` names its monitor
    /// thread and the backtrace marker (the FUSE daemon's: `fuse-watch`).
    pub fn from_env(label: &'static str) -> Self {
        Self::new(label, threshold_from_env(), backtraces_from_env())
    }

    /// A watchdog reporting ops older than `threshold` (zero: none, the
    /// registry still counts them), with or without backtraces.
    pub fn new(label: &'static str, threshold: Duration, backtraces: bool) -> Self {
        if backtraces {
            if let Err(error) = constellation_platform::native()
                .process
                .enable_backtraces(label)
            {
                tracing::warn!(%error, "stalled-request backtraces are unavailable");
            }
        }
        let watch = Self::with_registry(label, threshold, backtraces);
        if !threshold.is_zero() {
            let registry = Arc::downgrade(&watch.inner);
            let spawned = std::thread::Builder::new()
                .name(label.into())
                .spawn(move || monitor(registry, threshold));
            if let Err(error) = spawned {
                tracing::warn!(%error, "could not start the FUSE request watchdog");
            }
        }
        watch
    }

    /// A registry with no monitor thread: stalls are only found when
    /// [`Self::check_stalls`] is called (tests; a host driving its own
    /// monitor).
    pub fn manual(label: &'static str, threshold: Duration) -> Self {
        Self::with_registry(label, threshold, false)
    }

    fn with_registry(label: &'static str, threshold: Duration, backtraces: bool) -> Self {
        Self {
            view: None,
            inner: Arc::new(Registry {
                label,
                next: AtomicU64::new(1),
                active: Mutex::new(HashMap::new()),
                stalled_total: AtomicU64::new(0),
                stalled_completed: AtomicU64::new(0),
                threshold,
                backtraces,
            }),
        }
    }

    /// A handle to this registry whose ops belong to view `id` (with
    /// `labels`): what a view registers its ops through. Replaces any tag
    /// this handle had; the registry, counters and monitor stay shared.
    pub fn for_view(&self, id: u64, labels: BTreeMap<String, String>) -> OpWatch {
        OpWatch {
            inner: self.inner.clone(),
            view: Some(Arc::new(ViewTag { id, labels })),
        }
    }

    /// The label this watchdog was created with.
    pub fn label(&self) -> &'static str {
        self.inner.label
    }

    fn register(&self, op: &'static str, key: WatchKey, blocking: bool) -> Watched {
        let r = &self.inner;
        let entry = Arc::new(Entry {
            id: r.next.fetch_add(1, Ordering::Relaxed),
            view: self.view.clone(),
            op,
            key,
            thread: constellation_platform::native().process.current_thread(),
            started: Instant::now(),
            blocking,
            stage: Mutex::new("running"),
            stalled: AtomicBool::new(false),
            reported: AtomicU64::new(0),
        });
        r.active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(entry.id, entry.clone());
        // The op the current thread is handling (see the module doc).
        set_current(Some(entry.clone()));
        Watched {
            entry,
            registry: self.inner.clone(),
        }
    }

    /// Register the op the current thread starts handling.
    pub fn enter(&self, op: &'static str, key: impl Into<WatchKey>) -> Watched {
        self.register(op, key.into(), false)
    }

    /// As [`Self::enter`], for an op that may wait unboundedly by design
    /// (a blocking lock): listed, never counted as stalled.
    pub fn enter_blocking(&self, op: &'static str, key: impl Into<WatchKey>) -> Watched {
        self.register(op, key.into(), true)
    }

    /// One monitor pass: report every op past the threshold (the monitor
    /// thread runs this periodically).
    pub fn check_stalls(&self) {
        check(&self.inner);
    }

    /// Every op in flight right now, oldest first (`node.ops`).
    pub fn ops(&self) -> Vec<InFlightOp> {
        // Copied out first, as `snapshot` does: every op's `enter` and
        // drop takes this lock, so it is not held across the per-entry
        // stage locks.
        let active: Vec<Arc<Entry>> = self
            .inner
            .active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect();
        let mut ops: Vec<InFlightOp> = active
            .iter()
            .map(|e| InFlightOp {
                op: e.op,
                ino: e.key.ino(),
                age: e.age(),
                stage: e.stage(),
                tid: e.thread.tid,
                blocking: e.blocking,
                stalled: e.stalled.load(Ordering::Relaxed),
                view: e.view.clone(),
            })
            .collect();
        ops.sort_by_key(|o| std::cmp::Reverse(o.age));
        ops
    }

    /// The stall threshold this registry reports at.
    pub fn threshold(&self) -> Duration {
        self.inner.threshold
    }

    /// The watchdog's view, for `status`.
    pub fn snapshot(&self) -> WatchSnapshot {
        let r = &self.inner;
        let active: Vec<Arc<Entry>> = r
            .active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect();
        let mut stalled_ops: Vec<StalledOp> = active
            .iter()
            .filter(|e| e.stalled.load(Ordering::Relaxed) || (e.blocking && e.age() >= r.threshold))
            .map(|e| StalledOp {
                op: e.op,
                ino: e.key.ino(),
                age_s: e.age().as_secs(),
                stage: e.stage(),
                tid: e.thread.tid,
                blocking: e.blocking,
                view: e.view.as_ref().map(|v| v.id),
            })
            .collect();
        stalled_ops.sort_by_key(|r| std::cmp::Reverse(r.age_s));
        WatchSnapshot {
            in_flight: active.len() as u64,
            stalled: active
                .iter()
                .filter(|e| e.stalled.load(Ordering::Relaxed))
                .count() as u64,
            stalled_total: r.stalled_total.load(Ordering::Relaxed),
            stalled_completed: r.stalled_completed.load(Ordering::Relaxed),
            oldest_s: active
                .iter()
                .filter(|e| !e.blocking)
                .map(|e| e.age().as_secs())
                .max()
                .unwrap_or(0),
            stall_threshold_s: r.threshold.as_secs(),
            stalled_ops,
        }
    }
}

/// [`OpWatch::snapshot`]'s answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchSnapshot {
    /// Ops being handled right now.
    pub in_flight: u64,
    /// Of them, reported as stalled.
    pub stalled: u64,
    /// Ops ever reported as stalled.
    pub stalled_total: u64,
    /// Of those, the ones that did complete eventually.
    pub stalled_completed: u64,
    /// The age of the oldest non-blocking op in flight.
    pub oldest_s: u64,
    pub stall_threshold_s: u64,
    /// The stalled ops, and the blocking ones past the threshold; oldest
    /// first.
    pub stalled_ops: Vec<StalledOp>,
}

/// One op in [`WatchSnapshot::stalled_ops`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StalledOp {
    pub op: &'static str,
    pub ino: Ino,
    pub age_s: u64,
    /// What the op last noted it was waiting on.
    pub stage: &'static str,
    /// The OS thread handling it.
    pub tid: u64,
    /// A blocking lock request: unbounded by design, not a stall.
    pub blocking: bool,
    /// The view (by id) the op belongs to, when its handle was tagged.
    pub view: Option<u64>,
}

/// One op in flight, as [`OpWatch::ops`] lists it.
#[derive(Debug, Clone)]
pub struct InFlightOp {
    pub op: &'static str,
    pub ino: Ino,
    pub age: Duration,
    /// What the op last noted it was waiting on.
    pub stage: &'static str,
    pub tid: u64,
    /// A blocking lock request: unbounded by design, not a stall.
    pub blocking: bool,
    /// The monitor has reported it as stalled.
    pub stalled: bool,
    pub view: Option<Arc<ViewTag>>,
}

/// A registered op; dropping it deregisters (drop it after the op's
/// responder has answered).
pub struct Watched {
    entry: Arc<Entry>,
    registry: Arc<Registry>,
}

impl Watched {
    /// Note what this op is about to wait on.
    pub fn stage(&self, stage: &'static str) {
        self.entry.set_stage(stage);
    }

    /// Make this op the calling thread's current one: the owner of the
    /// registration moved it to another thread (a deferred completion),
    /// and that thread's [`stage`] calls should name it.
    pub fn adopt(&self) {
        set_current(Some(self.entry.clone()));
    }
}

impl Drop for Watched {
    fn drop(&mut self) {
        let r = &self.registry;
        r.active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.entry.id);
        clear_current(&self.entry);
        if self.entry.stalled.load(Ordering::Relaxed) {
            r.stalled_completed.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                op = self.entry.op,
                ino = self.entry.key.ino(),
                tid = self.entry.thread.tid,
                took = ?self.entry.age(),
                last_stage = self.entry.stage(),
                "a stalled FUSE request completed"
            );
        }
    }
}

thread_local! {
    /// The op the current thread is handling (see the module doc).
    static CURRENT: RefCell<Option<Arc<Entry>>> = const { RefCell::new(None) };
}

fn set_current(entry: Option<Arc<Entry>>) {
    CURRENT.with(|c| *c.borrow_mut() = entry);
}

/// Clear the current thread's op if it is still `entry` (an op that
/// ended after another one started on this thread leaves that one alone).
fn clear_current(entry: &Arc<Entry>) {
    CURRENT.with(|c| {
        let mut cur = c.borrow_mut();
        if cur.as_ref().is_some_and(|e| Arc::ptr_eq(e, entry)) {
            *cur = None;
        }
    });
}

/// Note what the current thread's op is about to wait on. A no-op on a
/// thread handling no op (the sync task's own flushes).
pub fn stage(stage: &'static str) {
    CURRENT.with(|c| {
        if let Some(entry) = c.borrow().as_ref() {
            entry.set_stage(stage);
        }
    });
}

/// The op the current thread is handling, to hand to another thread.
pub fn current() -> Option<Current> {
    CURRENT.with(|c| c.borrow().clone().map(Current))
}

/// An op handed from one thread to another ([`current`]).
pub struct Current(Arc<Entry>);

impl Current {
    /// Make it the calling thread's current op.
    pub fn adopt(self) {
        set_current(Some(self.0));
    }
}

fn monitor(registry: Weak<Registry>, threshold: Duration) {
    let period = (threshold / 4).clamp(Duration::from_secs(1), Duration::from_secs(5));
    loop {
        std::thread::sleep(period);
        let Some(r) = registry.upgrade() else {
            return;
        };
        check(&r);
    }
}

fn check(r: &Registry) {
    if r.threshold.is_zero() {
        return;
    }
    let active: Vec<Arc<Entry>> = r
        .active
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .values()
        .cloned()
        .collect();
    for e in active {
        let age = e.age();
        if age < r.threshold {
            continue;
        }
        let crossed = (age.as_secs_f64() / r.threshold.as_secs_f64()) as u64;
        if crossed <= e.reported.swap(crossed, Ordering::Relaxed) {
            continue;
        }
        if e.blocking {
            tracing::info!(
                op = e.op,
                ino = e.key.ino(),
                tid = e.thread.tid,
                ?age,
                stage = e.stage(),
                "a blocking FUSE request is still waiting (by design: another owner holds the file)"
            );
            continue;
        }
        let first = !e.stalled.swap(true, Ordering::Relaxed);
        if first {
            r.stalled_total.fetch_add(1, Ordering::Relaxed);
        }
        tracing::warn!(
            op = e.op,
            ino = e.key.ino(),
            tid = e.thread.tid,
            ?age,
            stage = e.stage(),
            first,
            "a FUSE request is stalled: unanswered past the stall threshold"
        );
        if first && r.backtraces {
            request_backtrace(e.thread);
        }
    }
}

/// `CONSTELLATION_FUSE_STALL_BACKTRACE=1`: the platform's handler
/// writes the signalled thread's backtrace to stderr (the daemon log),
/// between `=== <label>: backtrace of stalled thread tid=<tid> ===` and
/// `=== end ===`.
fn request_backtrace(thread: ThreadRef) {
    if constellation_platform::native()
        .process
        .request_backtrace(thread)
        .is_err()
    {
        tracing::warn!(
            tid = thread.tid,
            "could not signal the stalled thread for a backtrace"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_is_listed_while_in_flight_and_gone_after() {
        let watch = OpWatch::manual("test-watch", Duration::from_secs(30));
        let w = watch.enter("getattr", 42);
        stage("write shard lock");
        let s = watch.snapshot();
        assert_eq!(s.in_flight, 1);
        assert_eq!(s.stalled, 0);
        drop(w);
        assert_eq!(watch.snapshot().in_flight, 0);
        // Another thread's request is not this thread's current one.
        let other = {
            let watch = watch.clone();
            std::thread::spawn(move || {
                let _w = watch.enter("read", 7);
                std::thread::sleep(Duration::from_millis(50));
            })
        };
        stage("ignored: no request on this thread");
        other.join().unwrap();
        let s = watch.snapshot();
        assert_eq!(s.stalled_total, 0);
        assert_eq!(s.in_flight, 0);
    }

    #[test]
    fn a_blocking_request_is_listed_but_not_a_stall() {
        let watch = OpWatch::manual("test-watch", Duration::from_millis(20));
        let w = watch.enter_blocking("setlk", 9);
        stage("lock grant");
        std::thread::sleep(Duration::from_millis(50));
        watch.check_stalls();
        let s = watch.snapshot();
        assert_eq!(s.in_flight, 1);
        assert_eq!(s.stalled, 0);
        assert_eq!(s.stalled_total, 0);
        assert_eq!(s.oldest_s, 0);
        assert_eq!(s.stalled_ops.len(), 1, "listed past the threshold");
        assert!(s.stalled_ops[0].blocking);
        assert_eq!(s.stalled_ops[0].stage, "lock grant");
        drop(w);
        assert_eq!(watch.snapshot().stalled_completed, 0);
    }

    #[test]
    fn an_op_past_the_threshold_is_reported_once_and_counted_when_it_completes() {
        let watch = OpWatch::manual("test-watch", Duration::from_millis(20));
        let fast = watch.enter("getattr", 1);
        drop(fast);
        let slow = watch.enter("read", 5);
        stage("chunk fetch: S3");
        watch.check_stalls();
        assert_eq!(watch.snapshot().stalled, 0, "not yet past the threshold");
        std::thread::sleep(Duration::from_millis(30));
        watch.check_stalls();
        watch.check_stalls();
        let s = watch.snapshot();
        assert_eq!((s.stalled, s.stalled_total), (1, 1));
        let op = &s.stalled_ops[0];
        assert_eq!(
            (op.op, op.ino, op.stage, op.blocking),
            ("read", 5, "chunk fetch: S3", false)
        );
        drop(slow);
        let s = watch.snapshot();
        assert_eq!(
            (s.in_flight, s.stalled, s.stalled_total, s.stalled_completed),
            (0, 0, 1, 1)
        );
    }

    #[test]
    fn an_op_adopted_by_another_thread_is_staged_there_and_ends_there() {
        let watch = OpWatch::manual("test-watch", Duration::from_secs(30));
        let w = watch.enter_blocking("setlk", 3);
        // The engine's hop (`locks::ClusterLocks::lock`'s `lock-wait`
        // thread): the spawning thread's current op travels with it.
        let handed = current().expect("this thread handles an op");
        std::thread::spawn(move || {
            handed.adopt();
            stage("lock grant (core reply)");
        })
        .join()
        .unwrap();
        assert_eq!(w.entry.stage(), "lock grant (core reply)");
        // The owner's hop: the registration itself moves, and the op ends
        // on the other thread.
        let other = std::thread::spawn(move || {
            w.adopt();
            stage("session wait after a lock grant");
            let staged = w.entry.stage();
            drop(w);
            // Deregistered: this thread's stages name nothing any more.
            stage("after the op");
            staged
        });
        assert_eq!(other.join().unwrap(), "session wait after a lock grant");
        assert_eq!(watch.snapshot().in_flight, 0);
    }
    #[test]
    fn ops_are_attributed_to_the_view_whose_handle_registered_them() {
        let watch = OpWatch::manual("test-watch", Duration::from_secs(30));
        let labels: BTreeMap<String, String> = [("pv".to_string(), "pv-1".to_string())].into();
        let one = watch.for_view(1, labels.clone());
        let two = watch.for_view(2, BTreeMap::new());
        let _a = one.enter("read", 5);
        let _b = two.enter("lookup", 1);
        let _c = watch.enter("getattr", 9);
        // One registry: every handle sees every op.
        assert_eq!(watch.snapshot().in_flight, 3);
        let ops = two.ops();
        assert_eq!(ops.len(), 3);
        let of = |name: &str| ops.iter().find(|o| o.op == name).unwrap();
        assert_eq!(of("read").view.as_deref(), Some(&ViewTag { id: 1, labels }));
        assert_eq!(of("lookup").view.as_ref().map(|v| v.id), Some(2));
        assert_eq!(of("getattr").view, None, "an untagged handle: no view");
        assert!(ops.iter().all(|o| !o.stalled && !o.blocking));
    }
}
