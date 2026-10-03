// CONSTELLATION PATCH: the single-writer lock with a priority class (see
// CONSTELLATION-PATCH.md, change 3).
//
// Upstream serializes write transactions with a plain `Mutex<()>`. A mutex
// is not fair: a thread that releases it and asks again takes it back
// before a woken waiter runs. Under a dozen busy writers, one particular
// writer could wait seconds for a lock each holder kept for milliseconds.
// Constellation's authority core is such a writer and must not: everything
// a node's authority does waits for its step.
//
// A thread that set `set_write_priority(true)` is served before every
// ordinary writer that is waiting. An ordinary writer that has waited
// `DEFER_MAX` is overdue: the lock goes to an overdue writer before the
// next priority one whenever the last holder was a priority writer, so a
// priority writer that writes continuously alternates with the others
// instead of starving them.

use std::cell::Cell;
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

const DEFER_MAX: Duration = Duration::from_millis(50);

thread_local! {
    static PRIORITY: Cell<bool> = const { Cell::new(false) };
}

/// Make this thread's write transactions priority ones (`true`) or
/// ordinary ones (`false`); returns the previous setting.
pub fn set_write_priority(on: bool) -> bool {
    PRIORITY.with(|p| p.replace(on))
}

#[derive(Default)]
struct State {
    held: bool,
    /// The current (or last) holder is a priority writer.
    priority_held: bool,
    priority_waiting: usize,
    overdue: usize,
}

#[derive(Default)]
pub struct WriterLock {
    state: Mutex<State>,
    priority: Condvar,
    ordinary: Condvar,
}

/// Holds the lock until dropped.
pub struct WriterGuard<'a> {
    lock: &'a WriterLock,
}

/// Every wait is bounded, so a wakeup that went to a waiter which could
/// not take the lock is never the last one. Each waiter re-checks this
/// often: negligible for the dozen or so writers a database sees here; a
/// design for hundreds of waiters would hand the lock over explicitly
/// instead. Ordinary writers among themselves are served as upstream's
/// mutex served them (`notify_one`, and a releasing thread may take the
/// lock straight back): only the priority class is ordered.
const RECHECK: Duration = Duration::from_millis(10);

impl WriterLock {
    fn state(&self) -> MutexGuard<'_, State> {
        // Only this module touches the state, and nothing in it panics
        // while holding it.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn lock(&self) -> WriterGuard<'_> {
        let mut s = self.state();
        let priority = PRIORITY.with(Cell::get);
        if priority {
            s.priority_waiting += 1;
            while s.held || (s.priority_held && s.overdue > 0) {
                s = self
                    .priority
                    .wait_timeout(s, RECHECK)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
            s.priority_waiting -= 1;
        } else {
            let deadline = Instant::now() + DEFER_MAX;
            let mut overdue = false;
            loop {
                let now = Instant::now();
                if !overdue && now >= deadline {
                    overdue = true;
                    s.overdue += 1;
                }
                let turn = s.priority_waiting == 0 || (overdue && s.priority_held);
                if !s.held && turn {
                    break;
                }
                let wait = if overdue {
                    RECHECK
                } else {
                    (deadline - now).min(RECHECK)
                };
                s = self
                    .ordinary
                    .wait_timeout(s, wait)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
            if overdue {
                s.overdue -= 1;
            }
        }
        s.held = true;
        s.priority_held = priority;
        WriterGuard { lock: self }
    }
}

impl Drop for WriterGuard<'_> {
    fn drop(&mut self) {
        let mut s = self.lock.state();
        s.held = false;
        if s.priority_held && s.overdue > 0 {
            self.lock.ordinary.notify_all();
        } else if s.priority_waiting > 0 {
            self.lock.priority.notify_one();
        } else {
            self.lock.ordinary.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    /// Eight writers take the lock back to back; a priority writer still
    /// gets it within about one of their holds.
    #[test]
    fn a_priority_writer_goes_ahead_of_busy_writers() {
        let lock = Arc::new(WriterLock::default());
        let stop = Arc::new(AtomicBool::new(false));
        let hold = Duration::from_millis(5);
        let writers: Vec<_> = (0..8)
            .map(|_| {
                let lock = lock.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let g = lock.lock();
                        std::thread::sleep(hold);
                        drop(g);
                    }
                })
            })
            .collect();
        std::thread::sleep(Duration::from_millis(50));
        let before = set_write_priority(true);
        assert!(!before);
        let mut waits = Vec::new();
        for _ in 0..40 {
            let started = Instant::now();
            let g = lock.lock();
            waits.push(started.elapsed());
            drop(g);
            std::thread::sleep(Duration::from_millis(2));
        }
        set_write_priority(false);
        stop.store(true, Ordering::Relaxed);
        for w in writers {
            w.join().unwrap();
        }
        waits.sort();
        let median = waits[waits.len() / 2];
        // About one 5 ms hold; generous for a loaded host (a plain mutex:
        // hundreds of ms).
        assert!(
            median < Duration::from_millis(100),
            "median priority wait {median:?}; all: {waits:?}"
        );
    }

    /// Ordinary writers are never starved by a priority writer that keeps
    /// asking: an overdue one gets the next turn after a priority hold.
    #[test]
    fn ordinary_writers_defer_for_a_bounded_time() {
        let lock = Arc::new(WriterLock::default());
        let stop = Arc::new(AtomicBool::new(false));
        let hog = {
            let lock = lock.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                set_write_priority(true);
                while !stop.load(Ordering::Relaxed) {
                    let g = lock.lock();
                    std::thread::sleep(Duration::from_millis(1));
                    drop(g);
                }
            })
        };
        std::thread::sleep(Duration::from_millis(20));
        for _ in 0..10 {
            let started = Instant::now();
            drop(lock.lock());
            // DEFER_MAX, then one priority hold (1 ms).
            assert!(
                started.elapsed() < Duration::from_millis(500),
                "an ordinary writer waited {:?}",
                started.elapsed()
            );
        }
        stop.store(true, Ordering::Relaxed);
        hog.join().unwrap();
    }
}
