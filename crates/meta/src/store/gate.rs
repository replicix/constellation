//! Write priority for the node's authority core.
//!
//! Every metadata write of a node takes fjall's single-writer lock
//! (`SingleWriterTxDatabase::write_tx`): FUSE workers executing local
//! writes on the fast path, the background passes, and the authority core
//! applying the log, its delegates' streams and its backups' appends all
//! queue on it. Upstream's lock is a plain mutex, which is not fair: under
//! a busy mount (a dozen FUSE workers creating, linking and unlinking at
//! once) the core lost it to them over and over, waiting up to seconds per
//! transaction for a lock each holder kept for milliseconds, and a core
//! step took seconds. Nothing else of the node's authority runs meanwhile
//! — its replies to forwarded writes, its lock grants and renewals, its
//! delegation and backup traffic — so the other nodes saw it silent
//! (`stress-ng-fs-nodes`).
//!
//! A thread inside [`priority_writes`] (the core's step) is served before
//! every ordinary writer waiting for the lock; ordinary writers defer to it
//! for a bounded time (the vendored fjall's `WriterLock`, see
//! `vendor/fjall/CONSTELLATION-PATCH.md`).

use fjall::{SingleWriterTxDatabase, SingleWriterWriteTx};
use std::cell::Cell;
use std::panic::Location;
use std::sync::Mutex;
use std::time::{Duration, Instant};

thread_local! {
    static PRIORITY: Cell<bool> = const { Cell::new(false) };
    static STATS: Cell<PriorityStats> = const { Cell::new(PriorityStats::ZERO) };
}

/// Where a [`priority_writes`] scope's time went in the store: waits for
/// the writer lock (another writer's transaction ahead of it) and its own
/// syncs to stable storage. The driver reports them with a slow core step.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PriorityStats {
    pub lock_waits: u32,
    pub lock_wait_us: u64,
    pub lock_wait_max_us: u64,
    /// The transaction the longest wait was behind: where it was opened
    /// (the store function's `write_tx` call) and how long it had held the
    /// lock when this scope got it.
    pub lock_wait_max_behind: Option<(&'static Location<'static>, u64)>,
    pub syncs: u32,
    pub sync_us: u64,
}

impl PriorityStats {
    const ZERO: Self = Self {
        lock_waits: 0,
        lock_wait_us: 0,
        lock_wait_max_us: 0,
        lock_wait_max_behind: None,
        syncs: 0,
        sync_us: 0,
    };
}

/// A priority wait this long for the writer lock is reported with the
/// transaction it waited behind ([`GatedDb::write_tx`]).
const SLOW_PRIORITY_WAIT: Duration = Duration::from_millis(100);

/// The writer that took the lock last: where its transaction was opened,
/// when, and on which thread.
#[derive(Clone)]
struct Holder {
    at: &'static Location<'static>,
    since: Instant,
    thread: std::thread::Thread,
}

fn note(f: impl FnOnce(&mut PriorityStats)) {
    if PRIORITY.with(Cell::get) {
        STATS.with(|s| {
            let mut v = s.get();
            f(&mut v);
            s.set(v);
        });
    }
}

/// A sync to stable storage, counted inside a [`priority_writes`] scope.
pub(crate) fn note_sync(took: Duration) {
    note(|s| {
        s.syncs += 1;
        s.sync_us += took.as_micros() as u64;
    });
}

/// Run `f` with this thread's metadata writes ahead of every other
/// writer's (see the module docs), and say where its store time went.
/// Meant for the authority core's step: short, synchronous, and on the
/// node's critical path.
pub fn priority_writes<T>(f: impl FnOnce() -> T) -> (T, PriorityStats) {
    struct Restore(bool, bool, PriorityStats);
    impl Drop for Restore {
        fn drop(&mut self) {
            PRIORITY.with(|p| p.set(self.0));
            fjall::set_write_priority(self.1);
            STATS.with(|s| s.set(self.2));
        }
    }
    let restore = Restore(
        PRIORITY.with(|p| p.replace(true)),
        fjall::set_write_priority(true),
        STATS.with(|s| s.replace(PriorityStats::ZERO)),
    );
    let out = f();
    let stats = STATS.with(Cell::get);
    drop(restore);
    (out, stats)
}

/// The store's database, with the writer-lock waits of a
/// [`priority_writes`] scope counted; everything else derefs to it.
pub(crate) struct GatedDb {
    db: SingleWriterTxDatabase,
    /// The last writer to take the lock: the one a priority writer waits
    /// behind (it is served next).
    holder: Mutex<Option<Holder>>,
}

impl GatedDb {
    pub(crate) fn new(db: SingleWriterTxDatabase) -> Self {
        Self {
            db,
            holder: Mutex::new(None),
        }
    }

    fn holder(&self) -> std::sync::MutexGuard<'_, Option<Holder>> {
        self.holder.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A write transaction. A priority writer (a [`priority_writes`]
    /// scope) waits only for the transaction holding the lock; when that
    /// took longer than [`SLOW_PRIORITY_WAIT`] the holder is reported —
    /// where its transaction was opened (the store function, by
    /// `#[track_caller]`), its thread, and how long it held the lock.
    #[track_caller]
    pub(crate) fn write_tx(&self) -> SingleWriterWriteTx<'_> {
        let at = Location::caller();
        let priority = PRIORITY.with(Cell::get);
        let ahead = if priority {
            self.holder().clone()
        } else {
            None
        };
        let started = Instant::now();
        let tx = self.db.write_tx();
        let now = Instant::now();
        let waited = now - started;
        *self.holder() = Some(Holder {
            at,
            since: now,
            thread: std::thread::current(),
        });
        let waited_us = waited.as_micros() as u64;
        let behind = ahead.map(|h| {
            let held = now.saturating_duration_since(h.since);
            (h, held)
        });
        if waited >= SLOW_PRIORITY_WAIT {
            if let Some((h, held)) = &behind {
                tracing::warn!(
                    waited_ms = waited.as_millis() as u64,
                    op = %at,
                    holder_op = %h.at,
                    holder_thread = h.thread.name().unwrap_or("?"),
                    holder_held_ms = held.as_millis() as u64,
                    "a priority metadata write waited for the writer lock"
                );
            }
        }
        note(|s| {
            s.lock_waits += 1;
            s.lock_wait_us += waited_us;
            if waited_us >= s.lock_wait_max_us {
                s.lock_wait_max_us = waited_us;
                s.lock_wait_max_behind = behind.map(|(h, held)| (h.at, held.as_micros() as u64));
            }
        });
        tx
    }
}

impl std::ops::Deref for GatedDb {
    type Target = SingleWriterTxDatabase;

    fn deref(&self) -> &SingleWriterTxDatabase {
        &self.db
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Meta;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    /// Busy ordinary writers take the store's lock back to back (a writer
    /// that lets it go takes it again before a woken waiter runs); the
    /// core's step still gets it within about one of their transactions.
    /// (A plain mutex here: median 0.85 s, worst 12.5 s.)
    #[test]
    fn a_priority_scope_goes_ahead_of_busy_writers() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let hold = Duration::from_millis(5);
        let writers: Vec<_> = (0..8)
            .map(|_| {
                let meta = meta.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let tx = meta.db.write_tx();
                        std::thread::sleep(hold);
                        drop(tx);
                    }
                })
            })
            .collect();
        std::thread::sleep(Duration::from_millis(50));
        let mut waits = Vec::new();
        for _ in 0..40 {
            let ((), stats) = priority_writes(|| drop(meta.db.write_tx()));
            assert_eq!(stats.lock_waits, 1);
            waits.push(Duration::from_micros(stats.lock_wait_us));
            std::thread::sleep(Duration::from_millis(2));
        }
        stop.store(true, Ordering::Relaxed);
        for w in writers {
            w.join().unwrap();
        }
        waits.sort();
        let median = waits[waits.len() / 2];
        // One writer's hold (5 ms), with a margin for a loaded host (the
        // shared test host runs at load 100-300): the unfair mutex's
        // median was 0.85 s, far above either.
        assert!(
            median < Duration::from_millis(100),
            "a priority write waited {median:?} (median; all: {waits:?})"
        );
        // Outside the scope, writes are ordinary again.
        assert!(!PRIORITY.with(Cell::get));
        assert!(!fjall::set_write_priority(false));
    }

    /// A priority scope that writes continuously does not starve the
    /// ordinary writers: one that has deferred for its bound gets the next
    /// turn after a priority transaction.
    #[test]
    fn ordinary_writers_are_not_starved_by_a_busy_priority_scope() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let hog = {
            let meta = meta.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                priority_writes(|| {
                    while !stop.load(Ordering::Relaxed) {
                        let tx = meta.db.write_tx();
                        std::thread::sleep(Duration::from_millis(1));
                        drop(tx);
                    }
                })
            })
        };
        std::thread::sleep(Duration::from_millis(20));
        for _ in 0..10 {
            let started = Instant::now();
            drop(meta.db.write_tx());
            assert!(
                started.elapsed() < Duration::from_millis(500),
                "an ordinary writer waited {:?}",
                started.elapsed()
            );
        }
        stop.store(true, Ordering::Relaxed);
        hog.join().unwrap();
    }

    /// A priority write that waits long for the lock says which
    /// transaction it waited behind: where it was opened and how long it
    /// held the lock.
    #[test]
    fn a_slow_priority_wait_names_the_transaction_ahead() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let slow = {
            let meta = meta.clone();
            std::thread::spawn(move || {
                let tx = meta.db.write_tx();
                held_tx.send(line!() - 1).unwrap();
                std::thread::sleep(Duration::from_millis(150));
                drop(tx);
            })
        };
        let line = held_rx.recv().unwrap();
        let ((), stats) = priority_writes(|| drop(meta.db.write_tx()));
        slow.join().unwrap();
        assert!(stats.lock_wait_max_us >= 100_000, "{stats:?}");
        let (at, held_us) = stats.lock_wait_max_behind.expect("the holder ahead");
        assert!(at.file().ends_with("gate.rs"), "{at}");
        assert_eq!(at.line(), line, "{at}");
        assert!(held_us >= 100_000, "{stats:?}");
        // Unrelated to the wait: an uncontended write names nobody slow.
        let ((), stats) = priority_writes(|| drop(meta.db.write_tx()));
        assert!(stats.lock_wait_max_us < 100_000);
    }
}
