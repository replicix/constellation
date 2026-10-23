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

/// The store gate's totals since the process started: what the writer
/// lock and the syncs cost, for the node's status. A priority writer is
/// the authority core's step ([`priority_writes`]); an ordinary one is a
/// FUSE worker or a background pass. Process-wide (a node has one store).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GateTotals {
    /// Priority write transactions that took the lock, the time they
    /// waited for it in total (µs) and the longest wait.
    pub priority_lock_waits: u64,
    pub priority_lock_wait_us: u64,
    pub priority_lock_wait_max_us: u64,
    /// Syncs to stable storage inside a priority scope, and their time.
    pub priority_syncs: u64,
    pub priority_sync_us: u64,
    /// The same for ordinary writers' lock waits.
    pub writer_lock_waits: u64,
    pub writer_lock_wait_us: u64,
    pub writer_lock_wait_max_us: u64,
}

mod totals {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

    pub(super) static PRIORITY_WAITS: AtomicU64 = AtomicU64::new(0);
    pub(super) static PRIORITY_WAIT_US: AtomicU64 = AtomicU64::new(0);
    pub(super) static PRIORITY_WAIT_MAX_US: AtomicU64 = AtomicU64::new(0);
    pub(super) static PRIORITY_SYNCS: AtomicU64 = AtomicU64::new(0);
    pub(super) static PRIORITY_SYNC_US: AtomicU64 = AtomicU64::new(0);
    pub(super) static WRITER_WAITS: AtomicU64 = AtomicU64::new(0);
    pub(super) static WRITER_WAIT_US: AtomicU64 = AtomicU64::new(0);
    pub(super) static WRITER_WAIT_MAX_US: AtomicU64 = AtomicU64::new(0);

    /// Count one wait; the maximum is written only when it is beaten (a
    /// load, not a read-modify-write, in the common case).
    pub(super) fn wait(count: &AtomicU64, total: &AtomicU64, max: &AtomicU64, us: u64) {
        count.fetch_add(1, Relaxed);
        total.fetch_add(us, Relaxed);
        if us > max.load(Relaxed) {
            max.fetch_max(us, Relaxed);
        }
    }
}

/// [`GateTotals`] now.
pub fn gate_totals() -> GateTotals {
    use std::sync::atomic::Ordering::Relaxed;
    GateTotals {
        priority_lock_waits: totals::PRIORITY_WAITS.load(Relaxed),
        priority_lock_wait_us: totals::PRIORITY_WAIT_US.load(Relaxed),
        priority_lock_wait_max_us: totals::PRIORITY_WAIT_MAX_US.load(Relaxed),
        priority_syncs: totals::PRIORITY_SYNCS.load(Relaxed),
        priority_sync_us: totals::PRIORITY_SYNC_US.load(Relaxed),
        writer_lock_waits: totals::WRITER_WAITS.load(Relaxed),
        writer_lock_wait_us: totals::WRITER_WAIT_US.load(Relaxed),
        writer_lock_wait_max_us: totals::WRITER_WAIT_MAX_US.load(Relaxed),
    }
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
        use std::sync::atomic::Ordering::Relaxed;
        s.syncs += 1;
        s.sync_us += took.as_micros() as u64;
        totals::PRIORITY_SYNCS.fetch_add(1, Relaxed);
        totals::PRIORITY_SYNC_US.fetch_add(took.as_micros() as u64, Relaxed);
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
    /// Tests only ([`crate::Meta::set_write_stall`]): how long every ordinary
    /// writer, and (`core_stall_ms`) every priority writer, holds the writer
    /// lock after taking it (0: not at all). One relaxed load per write.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) stall_ms: std::sync::atomic::AtomicU64,
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) core_stall_ms: std::sync::atomic::AtomicU64,
    /// Tests only: [`crate::Meta::set_writer_wait_hook_for_tests`].
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) wait_hook: Mutex<Option<WriterWaitHook>>,
    /// Tests only: [`crate::Meta::set_read_hook_for_tests`].
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) read_hook: Mutex<Option<WriterWaitHook>>,
}

/// See [`crate::Meta::set_writer_wait_hook_for_tests`].
#[cfg(any(test, feature = "test-util"))]
pub type WriterWaitHook = std::sync::Arc<dyn Fn() + Send + Sync>;

impl GatedDb {
    pub(crate) fn new(db: SingleWriterTxDatabase) -> Self {
        Self {
            db,
            holder: Mutex::new(None),
            #[cfg(any(test, feature = "test-util"))]
            stall_ms: std::sync::atomic::AtomicU64::new(0),
            #[cfg(any(test, feature = "test-util"))]
            core_stall_ms: std::sync::atomic::AtomicU64::new(0),
            #[cfg(any(test, feature = "test-util"))]
            wait_hook: Mutex::new(None),
            #[cfg(any(test, feature = "test-util"))]
            read_hook: Mutex::new(None),
        }
    }

    /// A read transaction (a snapshot). Debug and test builds check that
    /// this thread holds none of the lock tables' mutex (plan 41 L1b), and
    /// tests may park every read ([`crate::Meta::set_read_hook_for_tests`]:
    /// an uncached read on a stalled device).
    pub(crate) fn read_tx(&self) -> fjall::Snapshot {
        crate::locks::assert_no_guard("GatedDb::read_tx");
        #[cfg(any(test, feature = "test-util"))]
        {
            let hook = self
                .read_hook
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if let Some(hook) = hook {
                hook();
            }
        }
        self.db.read_tx()
    }

    #[cfg(any(test, feature = "test-util"))]
    fn in_process_stall(&self, priority: bool) -> Option<Duration> {
        let ms = if priority {
            self.core_stall_ms
                .load(std::sync::atomic::Ordering::Relaxed)
        } else {
            self.stall_ms.load(std::sync::atomic::Ordering::Relaxed)
        };
        (ms > 0).then(|| Duration::from_millis(ms))
    }

    #[cfg(not(any(test, feature = "test-util")))]
    fn in_process_stall(&self, _priority: bool) -> Option<Duration> {
        None
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
        crate::locks::assert_no_guard("GatedDb::write_tx");
        #[cfg(any(test, feature = "test-util"))]
        {
            let hook = self
                .wait_hook
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if let Some(hook) = hook {
                hook();
            }
        }
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
        // The store-stall fault point holds the lock after taking it: an
        // ordinary writer's (finding 08: a FUSE writer's slow journal
        // write, the core's step waiting behind it), and with `core` the
        // core's own commit's too (its own `write(2)` stalled).
        if let Some(hold) = fault_store_stall(priority).or_else(|| self.in_process_stall(priority))
        {
            tracing::warn!(
                hold_ms = hold.as_millis() as u64,
                op = %at,
                priority,
                "fault injection: holding the metadata writer lock (testing only)"
            );
            std::thread::sleep(hold);
        }
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
        if priority {
            totals::wait(
                &totals::PRIORITY_WAITS,
                &totals::PRIORITY_WAIT_US,
                &totals::PRIORITY_WAIT_MAX_US,
                waited_us,
            );
        } else {
            totals::wait(
                &totals::WRITER_WAITS,
                &totals::WRITER_WAIT_US,
                &totals::WRITER_WAIT_MAX_US,
                waited_us,
            );
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

/// `CONSTELLATION_FAULT_STORE_STALL_FILE=<path>` (load campaign): while
/// that file exists, an ordinary writer (a FUSE worker or a background
/// pass, not a [`priority_writes`] scope, so not the core's step) holds the
/// writer lock for the number of milliseconds the file holds after taking
/// it, and the core's next step waits behind it (finding 08's slow
/// journal write). `<ms> once`: the first writer to see the file removes
/// it, so one transaction stalls. `pid=<n>`: only the daemon of that pid
/// stalls (several daemons may share one path). `caller=<n>`: only a write
/// made for the FUSE request of that caller (the kernel's task id, as FUSE
/// names it: [`fault_caller_scope`]). `core`: the core's own commits stall
/// too (plan 41 L3: finding 08's own-commit case, a priority writer's
/// `write(2)` held up on the device); `core-only`: only those (the stall
/// is then the core's own, whatever the FUSE writers do). Write the file whole (to a temporary
/// name, then rename): a reader that sees a prefix stalls for the wrong
/// time. Unset, one cached lookup.
fn fault_store_stall(priority: bool) -> Option<Duration> {
    let path = store_stall_path()?;
    let text = std::fs::read_to_string(path).ok()?;
    let spec = parse_store_stall(&text)?;
    if priority && !spec.core {
        return None;
    }
    if !priority && spec.core_only {
        return None;
    }
    if spec.pid.is_some_and(|p| p != std::process::id()) {
        return None;
    }
    if spec
        .caller
        .is_some_and(|c| FAULT_CALLER.with(Cell::get) != c)
    {
        return None;
    }
    if spec.once && std::fs::remove_file(path).is_err() {
        // Another writer took this one.
        return None;
    }
    Some(Duration::from_millis(spec.ms))
}

fn store_stall_path() -> Option<&'static std::path::PathBuf> {
    static PATH: std::sync::OnceLock<Option<std::path::PathBuf>> = std::sync::OnceLock::new();
    PATH.get_or_init(|| {
        std::env::var_os("CONSTELLATION_FAULT_STORE_STALL_FILE").map(std::path::PathBuf::from)
    })
    .as_ref()
}

thread_local! {
    /// The caller of the FUSE request this thread is handling, for
    /// [`fault_store_stall`]'s `caller=` (0: none).
    static FAULT_CALLER: Cell<u32> = const { Cell::new(0) };
}

/// While the returned guard lives, this thread's writes are made for the
/// FUSE request of `caller` (the kernel's task id), as far as the
/// store-stall fault point is concerned. Inert (no thread-local touched)
/// while `CONSTELLATION_FAULT_STORE_STALL_FILE` is unset.
pub fn fault_caller_scope(caller: Option<u32>) -> FaultCallerScope {
    if store_stall_path().is_none() {
        return FaultCallerScope(None);
    }
    FaultCallerScope(Some(FAULT_CALLER.with(|c| c.replace(caller.unwrap_or(0)))))
}

/// See [`fault_caller_scope`].
pub struct FaultCallerScope(Option<u32>);

impl Drop for FaultCallerScope {
    fn drop(&mut self) {
        if let Some(prev) = self.0 {
            FAULT_CALLER.with(|c| c.set(prev));
        }
    }
}

/// [`fault_store_stall`]'s file: `<ms>[ once][ core|core-only][ pid=<n>][ caller=<n>]`.
#[derive(Debug, PartialEq, Eq)]
struct StoreStall {
    ms: u64,
    once: bool,
    pid: Option<u32>,
    caller: Option<u32>,
    /// The core's own commits stall too.
    core: bool,
    /// Only the core's own commits stall (implies `core`): the slow
    /// journal write is the core's, deterministically (a FUSE writer
    /// holding the lock stalls the core only if it has something to write
    /// meanwhile).
    core_only: bool,
}

fn parse_store_stall(text: &str) -> Option<StoreStall> {
    let mut it = text.split_whitespace();
    let ms: u64 = it.next()?.parse().ok()?;
    let mut spec = StoreStall {
        ms,
        once: false,
        pid: None,
        caller: None,
        core: false,
        core_only: false,
    };
    for w in it {
        if w == "once" && !spec.once {
            spec.once = true;
            continue;
        }
        if w == "core" && !spec.core {
            spec.core = true;
            continue;
        }
        if w == "core-only" && !spec.core_only {
            spec.core = true;
            spec.core_only = true;
            continue;
        }
        if let Some(p) = w.strip_prefix("pid=").filter(|_| spec.pid.is_none()) {
            spec.pid = Some(p.parse().ok()?);
            continue;
        }
        let p = w
            .strip_prefix("caller=")
            .filter(|_| spec.caller.is_none())?;
        spec.caller = Some(p.parse().ok()?);
    }
    (ms > 0).then_some(spec)
}

impl crate::Meta {
    /// Tests only (`test-util`; plan 41 L3): every ordinary writer of this
    /// store holds the writer lock for `stall` after taking it, until it is
    /// set to `None` — a slow journal write on the device, in process (the
    /// fault file's `<ms>`).
    #[cfg(any(test, feature = "test-util"))]
    pub fn set_write_stall(&self, stall: Option<Duration>) {
        self.db.stall_ms.store(
            stall.map_or(0, |d| d.as_millis().max(1) as u64),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Tests only (`test-util`; plan 41 L3): the same for the core's own
    /// commits (priority writers: the fault file's `core`), finding 08's
    /// own-commit case.
    #[cfg(any(test, feature = "test-util"))]
    pub fn set_core_write_stall(&self, stall: Option<Duration>) {
        self.db.core_stall_ms.store(
            stall.map_or(0, |d| d.as_millis().max(1) as u64),
            std::sync::atomic::Ordering::Relaxed,
        );
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

    #[test]
    fn store_stall_spec() {
        let spec = |ms, once, pid, caller| {
            Some(StoreStall {
                ms,
                once,
                pid,
                caller,
                core: false,
                core_only: false,
            })
        };
        assert_eq!(parse_store_stall("5000"), spec(5000, false, None, None));
        assert_eq!(
            parse_store_stall(" 20000 once\n"),
            spec(20000, true, None, None)
        );
        assert_eq!(
            parse_store_stall("800 once pid=42"),
            spec(800, true, Some(42), None)
        );
        assert_eq!(
            parse_store_stall("800 pid=7 caller=99 once"),
            spec(800, true, Some(7), Some(99))
        );
        // `core`: the core's own commits stall too.
        assert_eq!(
            parse_store_stall("9000 once core pid=3"),
            Some(StoreStall {
                ms: 9000,
                once: true,
                pid: Some(3),
                caller: None,
                core: true,
                core_only: false,
            })
        );
        // `core-only`: only the core's own commits (implies `core`).
        assert_eq!(
            parse_store_stall("5000 once core-only"),
            Some(StoreStall {
                ms: 5000,
                once: true,
                pid: None,
                caller: None,
                core: true,
                core_only: true,
            })
        );
        for bad in [
            "1 core core",
            "1 core-only core-only",
            "1 caller=x",
            "1 caller=2 caller=3",
            "",
            "0",
            "x",
            "100 twice",
            "100 once more",
            "1 pid=x",
            "1 once once",
        ] {
            assert_eq!(parse_store_stall(bad), None, "{bad:?}");
        }
    }

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

    /// The totals `node.status` reports: priority and ordinary lock waits
    /// and the core's syncs, cumulative. Process-wide, so tests of one
    /// binary that write at the same time add to them too: deltas, `>=`.
    #[test]
    fn gate_totals_count_priority_and_ordinary_waits_and_syncs() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let before = gate_totals();
        // An ordinary write (no wait to speak of), then a priority write
        // that waits behind a 150 ms holder, then a priority sync.
        drop(meta.db.write_tx());
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let slow = {
            let meta = meta.clone();
            std::thread::spawn(move || {
                let tx = meta.db.write_tx();
                held_tx.send(()).unwrap();
                std::thread::sleep(Duration::from_millis(150));
                drop(tx);
            })
        };
        held_rx.recv().unwrap();
        let ((), stats) = priority_writes(|| {
            drop(meta.db.write_tx());
            note_sync(Duration::from_millis(3));
        });
        slow.join().unwrap();
        assert!(stats.lock_wait_us >= 100_000, "{stats:?}");
        let after = gate_totals();
        // The slow holder's own write is an ordinary one too.
        assert!(after.writer_lock_waits >= before.writer_lock_waits + 2);
        assert!(after.priority_lock_waits > before.priority_lock_waits);
        assert!(
            after.priority_lock_wait_us >= before.priority_lock_wait_us + 100_000,
            "{before:?} -> {after:?}"
        );
        assert!(after.priority_lock_wait_max_us >= 100_000, "{after:?}");
        assert!(after.priority_syncs > before.priority_syncs);
        assert!(after.priority_sync_us >= before.priority_sync_us + 3_000);
    }
}
