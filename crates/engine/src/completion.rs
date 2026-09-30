//! The engine's completion pool (plan 31 §6.3, §6.4): the threads a
//! deferred op finishes on, so that a frontend thread that handed its
//! `Responder` over is free to take the next request.
//!
//! **Why a pool of its own.** Today the one kind of op that defers through
//! it is a *cold* read — a read whose chunk is in no local cache and must
//! come from a peer or S3 (`View::read`). Plan 31 C7b measured the
//! alternative: with a FUSE mount's four workers (a 4-CPU host) and 16
//! concurrent cold readers behind 200 ms of S3 latency, a warm
//! `open`+`read` of a cached file waited up to 1.26 s for a worker, and the
//! cold readers themselves were serialised four at a time (1.73 s for the
//! burst, 0.55 s with 32 workers). The wait itself stays synchronous — the
//! fetch path `block_on`s the runtime and polls prefetch state — so it
//! needs an OS thread; what deferral buys is that the thread is not one of
//! the frontend's. It is never the tokio blocking pool, for the reason
//! `ClusterLocks::lock`'s `lock-wait` thread is not: that pool is small
//! (four threads on one CPU) and the runtime's own work needs it, so a
//! burst of parked reads could starve the fetches they wait for.
//!
//! **One per process** ([`CompletionPool::global`]): threads are a process
//! resource, so every engine and view of a process shares the bound.
//!
//! **Bounded, grown on demand.** A thread is started only when a job
//! arrives and no thread is idle, up to `max` (`CONSTELLATION_COMPLETION_THREADS`,
//! default 64 — a kernel's in-flight FUSE requests are bounded by its
//! `max_background` plus the synchronous ones, 32 + workers on a 4-CPU
//! host); past that, jobs queue (they wait for S3 either way) and the
//! frontend's threads still go free. A thread idle for
//! [`IDLE_EXIT`] exits, so an idle engine holds none. A job that cannot
//! get a thread (the OS refused one and none exists) runs on the caller,
//! which is exactly the old, non-deferred behaviour.
//!
//! Jobs must be bounded waits: a job that waited for another job of this
//! pool could deadlock it once the pool is full. A cold read waits for
//! the runtime and the store only.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// How long an idle pool thread lingers before exiting.
pub const IDLE_EXIT: Duration = Duration::from_secs(30);

/// Default bound on pool threads (`CONSTELLATION_COMPLETION_THREADS`).
pub const DEFAULT_MAX_THREADS: usize = 64;

/// Hard ceiling on `CONSTELLATION_COMPLETION_THREADS`.
const HARD_MAX_THREADS: usize = 1024;

type Job = Box<dyn FnOnce() + Send + 'static>;

struct State {
    queue: VecDeque<Job>,
    threads: usize,
    idle: usize,
    /// Threads ever started (for tests and status).
    started: u64,
}

struct Shared {
    state: Mutex<State>,
    ready: Condvar,
    max: usize,
    idle_exit: Duration,
}

/// See the module doc. Cheap to clone (one `Arc`).
#[derive(Clone)]
pub struct CompletionPool(Arc<Shared>);

impl CompletionPool {
    /// A pool of at most `max` threads (at least one).
    pub fn new(max: usize) -> Self {
        Self::with_idle_exit(max, IDLE_EXIT)
    }

    fn with_idle_exit(max: usize, idle_exit: Duration) -> Self {
        Self(Arc::new(Shared {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                threads: 0,
                idle: 0,
                started: 0,
            }),
            ready: Condvar::new(),
            max: max.clamp(1, HARD_MAX_THREADS),
            idle_exit,
        }))
    }

    /// Sized from `CONSTELLATION_COMPLETION_THREADS` (default
    /// [`DEFAULT_MAX_THREADS`]).
    pub fn from_env() -> Self {
        let max = std::env::var("CONSTELLATION_COMPLETION_THREADS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_MAX_THREADS);
        Self::new(max)
    }

    /// Run `job` on a pool thread. Returns at once unless no pool thread
    /// exists and none could be started, in which case `job` runs here.
    pub fn submit(&self, job: impl FnOnce() + Send + 'static) {
        let shared = &self.0;
        let mut st = shared.state.lock().unwrap();
        st.queue.push_back(Box::new(job));
        if st.idle > st.queue.len().saturating_sub(1) {
            // An idle thread will take it (one per queued job).
            drop(st);
            shared.ready.notify_one();
            return;
        }
        if st.threads >= shared.max {
            // Full: it waits in the queue for the next free thread.
            return;
        }
        st.threads += 1;
        st.started += 1;
        let n = st.started;
        drop(st);
        let worker = self.0.clone();
        let spawned = std::thread::Builder::new()
            .name(format!("completion-{n}"))
            .spawn(move || worker_loop(&worker));
        if spawned.is_ok() {
            return;
        }
        let mut st = shared.state.lock().unwrap();
        st.threads -= 1;
        if st.threads > 0 {
            // Someone will get to it.
            return;
        }
        // No thread at all: run it (and anything queued) here.
        let jobs: Vec<Job> = st.queue.drain(..).collect();
        drop(st);
        tracing::warn!("could not start a completion thread; completing on the caller");
        for job in jobs {
            job();
        }
    }

    /// The process's pool, shared by every engine and view in it (threads
    /// are a process resource; the bound is the process's), sized from the
    /// environment on first use.
    pub fn global() -> &'static CompletionPool {
        static POOL: std::sync::OnceLock<CompletionPool> = std::sync::OnceLock::new();
        POOL.get_or_init(CompletionPool::from_env)
    }

    /// (threads alive now, threads ever started, jobs queued).
    pub fn stats(&self) -> (usize, u64, usize) {
        let st = self.0.state.lock().unwrap();
        (st.threads, st.started, st.queue.len())
    }

    /// The bound on threads.
    pub fn max_threads(&self) -> usize {
        self.0.max
    }
}

fn worker_loop(shared: &Shared) {
    let mut st = shared.state.lock().unwrap();
    loop {
        if let Some(job) = st.queue.pop_front() {
            drop(st);
            // A panicking job must not take the pool thread's count with
            // it: its responder answers from its own drop, and this thread
            // carries on.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
            st = shared.state.lock().unwrap();
            continue;
        }
        st.idle += 1;
        let (next, timeout) = shared
            .ready
            .wait_timeout_while(st, shared.idle_exit, |s| s.queue.is_empty())
            .unwrap();
        st = next;
        st.idle -= 1;
        if timeout.timed_out() && st.queue.is_empty() {
            st.threads -= 1;
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Instant;

    #[test]
    fn jobs_run_off_the_caller_and_the_pool_stays_bounded() {
        let pool = CompletionPool::new(3);
        let me = std::thread::current().id();
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel();
        // Every job holds until the gate opens, and the gate opens only once
        // all twelve submits have returned. A submit that parked its caller
        // until a job finished would stall here for the gate's full
        // timeout, which the elapsed check below catches without depending
        // on how loaded the machine is.
        let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let started = Instant::now();
        for i in 0..12 {
            let (running, peak, tx, gate) =
                (running.clone(), peak.clone(), tx.clone(), gate.clone());
            pool.submit(move || {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                let (open, cv) = &*gate;
                let guard = open.lock().unwrap();
                let _ = cv
                    .wait_timeout_while(guard, Duration::from_secs(10), |open| !*open)
                    .unwrap();
                running.fetch_sub(1, Ordering::SeqCst);
                tx.send((i, std::thread::current().id())).unwrap();
            });
        }
        // Submitting parked nobody: all twelve jobs were queued before any
        // could finish.
        assert!(started.elapsed() < Duration::from_secs(5));
        // The pool fills up to its bound while the gate is shut: three jobs
        // are held at once before any is let go.
        let deadline = Instant::now() + Duration::from_secs(10);
        while running.load(Ordering::SeqCst) < 3 {
            assert!(
                Instant::now() < deadline,
                "the pool never reached its bound"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        {
            let (open, cv) = &*gate;
            *open.lock().unwrap() = true;
            cv.notify_all();
        }
        let mut seen = Vec::new();
        for _ in 0..12 {
            let (i, thread) = rx.recv_timeout(Duration::from_secs(10)).unwrap();
            assert_ne!(thread, me, "a job ran on the submitting thread");
            seen.push(i);
        }
        seen.sort();
        assert_eq!(seen, (0..12).collect::<Vec<_>>());
        assert_eq!(
            peak.load(Ordering::SeqCst),
            3,
            "never more than max at once"
        );
        let (threads, ever, queued) = pool.stats();
        assert_eq!((threads, queued), (3, 0));
        assert_eq!(ever, 3, "threads are reused, not one per job");
    }

    #[test]
    fn an_idle_thread_is_reused_and_idle_threads_exit() {
        let pool = CompletionPool::with_idle_exit(8, Duration::from_millis(100));
        for _ in 0..5 {
            let (tx, rx) = mpsc::channel();
            pool.submit(move || tx.send(()).unwrap());
            rx.recv_timeout(Duration::from_secs(5)).unwrap();
            // Let the thread go idle before the next job.
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(pool.stats().1, 1, "sequential jobs share one thread");
        let deadline = Instant::now() + Duration::from_secs(5);
        while pool.stats().0 != 0 {
            assert!(Instant::now() < deadline, "idle thread never exited");
            std::thread::sleep(Duration::from_millis(20));
        }
        // And the pool still works after shrinking to nothing.
        let (tx, rx) = mpsc::channel();
        pool.submit(move || tx.send(()).unwrap());
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }

    #[test]
    fn a_panicking_job_does_not_shrink_the_pool() {
        let pool = CompletionPool::new(1);
        pool.submit(|| panic!("job panics"));
        let (tx, rx) = mpsc::channel();
        pool.submit(move || tx.send(()).unwrap());
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(pool.stats().1, 1);
    }
}
