//! The FUSE request watchdog (EC2 campaign 7, finding B-2).
//!
//! A FUSE request the daemon reads and never answers hangs its caller
//! for good, uninterruptibly, and nothing reports it: the daemon is
//! healthy otherwise, `status` is green, the other files work. Campaign
//! 7 saw a `getattr` (from `git add`'s path walk) and a file's `read`
//! stay unanswered for the rest of the daemon's life, with the kernel's
//! `waiting` counter the only trace.
//!
//! Every request a FUSE worker handles is registered here for its whole
//! duration (`enter`, at the top of each `Filesystem` method): the
//! operation, the inode, the OS thread, and the *stage* the handler last
//! noted (`stage`: the wait it is about to enter — a write shard, the
//! inode's operation lock, the session wait, a chunk fetch, the core's
//! reply, ...). A monitor thread logs any request older than
//! `CONSTELLATION_FUSE_REQUEST_STALL_S` (default 30 s; 0 disables the
//! monitor, not the registry) at WARN, with all of that, again every
//! further threshold it stays, and once more when it finally completes.
//! `status` reports the count (`fuse_requests`), and the stalled
//! requests themselves.
//!
//! A blocking lock request (`F_SETLKW`, `flock` without `LOCK_NB`) waits
//! for as long as another owner holds the file — by design, unbounded.
//! It is registered as `blocking` and listed, never counted as a stall.
//!
//! With `CONSTELLATION_FUSE_STALL_BACKTRACE=1` the monitor also asks the
//! stalled thread for its backtrace (a `SIGUSR2`, handled by capturing
//! one and writing it to stderr): a diagnostic for a build with symbols,
//! off by default. Naming the thread and delivering the signal are host
//! services (`constellation_platform`'s `Process::current_thread`/
//! `enable_backtraces`/`request_backtrace`, plan 31 §6.8).

use constellation_fs_core::Ino;
use constellation_platform::ThreadRef;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// One request in flight.
pub struct Entry {
    id: u64,
    op: &'static str,
    ino: Ino,
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
}

struct Registry {
    next: AtomicU64,
    active: Mutex<HashMap<u64, Arc<Entry>>>,
    stalled_total: AtomicU64,
    stalled_completed: AtomicU64,
    threshold: Duration,
    backtraces: bool,
}

static REGISTRY: OnceLock<Registry> = OnceLock::new();

thread_local! {
    /// The request the current thread is handling (set by [`enter`]).
    static CURRENT: RefCell<Option<Arc<Entry>>> = const { RefCell::new(None) };
}

/// `CONSTELLATION_FUSE_REQUEST_STALL_S`: seconds a request may be in
/// flight before it is reported (default 30; 0 disables the monitor).
pub fn threshold() -> Duration {
    Duration::from_secs(
        std::env::var("CONSTELLATION_FUSE_REQUEST_STALL_S")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30),
    )
}

fn registry() -> &'static Registry {
    REGISTRY.get_or_init(|| {
        let threshold = threshold();
        let backtraces = std::env::var("CONSTELLATION_FUSE_STALL_BACKTRACE")
            .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
        if backtraces {
            if let Err(error) = constellation_platform::native()
                .process
                .enable_backtraces("fuse-watch")
            {
                tracing::warn!(%error, "stalled-request backtraces are unavailable");
            }
        }
        if !threshold.is_zero() {
            let spawned = std::thread::Builder::new()
                .name("fuse-watch".into())
                .spawn(monitor);
            if let Err(error) = spawned {
                tracing::warn!(%error, "could not start the FUSE request watchdog");
            }
        }
        Registry {
            next: AtomicU64::new(1),
            active: Mutex::new(HashMap::new()),
            stalled_total: AtomicU64::new(0),
            stalled_completed: AtomicU64::new(0),
            threshold,
            backtraces,
        }
    })
}

/// A registered request; dropping it deregisters.
pub struct Watched {
    entry: Arc<Entry>,
}

fn register(op: &'static str, ino: Ino, blocking: bool) -> Watched {
    let r = registry();
    let entry = Arc::new(Entry {
        id: r.next.fetch_add(1, Ordering::Relaxed),
        op,
        ino,
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
    CURRENT.with(|c| *c.borrow_mut() = Some(entry.clone()));
    Watched { entry }
}

/// Register the request the current FUSE worker starts handling.
pub fn enter(op: &'static str, ino: Ino) -> Watched {
    register(op, ino, false)
}

/// As [`enter`], for a request that may wait unboundedly by design (a
/// blocking lock): listed, never counted as stalled.
pub fn enter_blocking(op: &'static str, ino: Ino) -> Watched {
    register(op, ino, true)
}

/// Note what the current thread's request is about to wait on. A no-op
/// on a thread handling no request (the sync task's own flushes).
pub fn stage(stage: &'static str) {
    CURRENT.with(|c| {
        if let Some(entry) = c.borrow().as_ref() {
            *entry.stage.lock().unwrap_or_else(|e| e.into_inner()) = stage;
        }
    });
}

impl Watched {
    /// Hand the request to another thread (a blocking lock's waiter):
    /// that thread's [`stage`] calls then name it.
    pub fn adopt(&self) {
        CURRENT.with(|c| *c.borrow_mut() = Some(self.entry.clone()));
    }
}

impl Drop for Watched {
    fn drop(&mut self) {
        let r = registry();
        r.active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.entry.id);
        CURRENT.with(|c| {
            let mut cur = c.borrow_mut();
            if cur.as_ref().is_some_and(|e| e.id == self.entry.id) {
                *cur = None;
            }
        });
        if self.entry.stalled.load(Ordering::Relaxed) {
            r.stalled_completed.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                op = self.entry.op,
                ino = self.entry.ino,
                tid = self.entry.thread.tid,
                took = ?self.entry.age(),
                last_stage = self.entry.stage(),
                "a stalled FUSE request completed"
            );
        }
    }
}

/// The watchdog's view for `status`.
pub fn snapshot() -> constellation_api::FuseRequestsStatus {
    let r = registry();
    let active: Vec<Arc<Entry>> = r
        .active
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .values()
        .cloned()
        .collect();
    let mut stalled_requests: Vec<constellation_api::StalledFuseRequest> = active
        .iter()
        .filter(|e| e.stalled.load(Ordering::Relaxed) || (e.blocking && e.age() >= r.threshold))
        .map(|e| constellation_api::StalledFuseRequest {
            op: e.op.to_string(),
            ino: e.ino,
            age_s: e.age().as_secs(),
            stage: e.stage().to_string(),
            tid: e.thread.tid as u32,
            blocking: e.blocking,
        })
        .collect();
    stalled_requests.sort_by_key(|r| std::cmp::Reverse(r.age_s));
    constellation_api::FuseRequestsStatus {
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
        stalled_requests,
    }
}

fn monitor() {
    let r = registry();
    let period = (r.threshold / 4).clamp(Duration::from_secs(1), Duration::from_secs(5));
    loop {
        std::thread::sleep(period);
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
                    ino = e.ino,
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
                ino = e.ino,
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
}

/// `CONSTELLATION_FUSE_STALL_BACKTRACE=1`: the platform's handler
/// writes the signalled thread's backtrace to stderr (the daemon log),
/// between `=== fuse-watch: backtrace of stalled thread tid=<tid> ===` and
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
        let w = enter("getattr", 42);
        stage("write shard lock");
        let s = snapshot();
        assert!(s.in_flight >= 1);
        assert_eq!(s.stalled, 0);
        drop(w);
        // Another thread's request is not this thread's current one.
        let other = std::thread::spawn(|| {
            let _w = enter("read", 7);
            std::thread::sleep(Duration::from_millis(50));
        });
        stage("ignored: no request on this thread");
        other.join().unwrap();
        let s = snapshot();
        assert_eq!(s.stalled_total, 0);
    }

    #[test]
    fn a_blocking_request_is_listed_but_not_a_stall() {
        let w = enter_blocking("setlk", 9);
        stage("lock grant");
        let s = snapshot();
        assert!(s.in_flight >= 1);
        assert_eq!(s.stalled, 0);
        assert_eq!(s.oldest_s, 0);
        drop(w);
    }
}
