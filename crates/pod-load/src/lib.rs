//! The writer/creator/reader trio of the harness's `upgrade-under-load`
//! (`crates/harness/src/scenarios/handover.rs`), for a filesystem the
//! harness can only reach through a Kubernetes pod: plan 37 K5b's
//! `csi-engine-pod-handoff-under-load` runs it in a workload pod on a PV
//! while the node plugin hands the PV's FUSE session to a new engine pod.
//!
//! - **writer**: 4 KiB records `pwrite`n through one descriptor held open
//!   throughout, an `fsync` every 16, at a steady rate (write-back data is
//!   always in flight); the descriptor's final `fsync` and `close(2)` are
//!   checked (a close is where a write-back session is published);
//! - **creator**: new files under `created/`, each opened, written and
//!   closed (the close checked);
//! - **reader**: `fixed` re-read through a descriptor held open throughout,
//!   and through a fresh `open` each round (an `open` always crosses the
//!   FUSE connection, a read of a cached file may not).
//!
//! Every call is timed and every outcome counted: an error is recorded with
//! its portable [`Code`] (`ENOTCONN`, `EIO`, ...), a short write or a read
//! returning other bytes than were written is an error too, and every call
//! slower than [`Opts::slow_ms`] is recorded with its wall-clock start, so
//! the harness can line the client's stalls up with the node plugin's
//! handoff window (pod and plugin share the node's clock). Contents are
//! seeded ([`record`]): the harness rebuilds every byte from the seed and
//! the counts in the [`Summary`] and checks them on another node. Every
//! acknowledgement is timed too (a writer `fsync`, a creator `close`), so
//! the harness can tell what a snapshot cut at a given time must hold.
//!
//! Control is through files in a directory of the pod's own (not the PV):
//! `ready` once the trio runs, `progress` (calls so far, rewritten once a
//! second), `stop` (created by the harness) ends it, and `summary.json`
//! (written by rename, so never seen half-written) is the outcome.

use constellation_types::Code;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::IntoRawFd;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The writer's record size.
pub const RECORD: usize = 4096;
/// The writer `fsync`s after every this many records.
pub const SYNC_EVERY: u64 = 16;
/// The size of the file the reader re-reads.
pub const FIXED_LEN: usize = 256 * 1024;
/// At most this many errors are kept (all are counted).
const KEEP: usize = 200;

/// A seeded, verifiable byte pattern for record `i` (the same xorshift as
/// `upgrade-under-load`'s).
pub fn record(seed: u64, i: u64, len: usize) -> Vec<u8> {
    let mut x = seed ^ i.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

/// The writer's `appended` file after `records` records.
pub fn appended_data(seed: u64, records: u64) -> Vec<u8> {
    let mut data = Vec::with_capacity(records as usize * RECORD);
    for i in 0..records {
        data.extend_from_slice(&record(seed, i, RECORD));
    }
    data
}

/// The creator's `created/f<i>`.
pub fn created_data(seed: u64, i: u64) -> Vec<u8> {
    record(seed ^ 0xc0ffee, i, 1000 + (i as usize % 7) * 997)
}

/// The reader's `fixed`.
pub fn fixed_data(seed: u64) -> Vec<u8> {
    record(seed, u64::MAX, FIXED_LEN)
}

/// How hard the trio runs.
#[derive(Debug, Clone, Copy)]
pub struct Opts {
    pub seed: u64,
    /// The writer's rate (KiB/s). Steady rather than flat out: a rollout
    /// can take kubelet minutes, and everything written lands in the S3
    /// the cluster uses (an in-memory floci in the harness).
    pub write_kib_s: u64,
    /// Files the creator makes per second.
    pub creates_s: u64,
    /// Rounds the reader makes per second.
    pub reads_s: u64,
    /// Calls at least this slow are recorded one by one.
    pub slow_ms: u64,
}

impl Default for Opts {
    fn default() -> Opts {
        Opts {
            seed: 42,
            write_kib_s: 512,
            creates_s: 20,
            reads_s: 20,
            slow_ms: 100,
        }
    }
}

/// One failed call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpError {
    /// `writer`, `creator` or `reader`, and the call.
    pub op: String,
    /// When it started, ms since the epoch.
    pub at_ms: u64,
    /// The errno's POSIX name (`ENOTCONN`), or `SHORT`/`MISMATCH` for a
    /// call that returned without an error but did not do what it was
    /// asked.
    pub code: String,
    pub message: String,
}

/// One call slower than [`Opts::slow_ms`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SlowCall {
    pub op: String,
    /// When it started, ms since the epoch.
    pub start_ms: u64,
    pub ms: u64,
}

/// What the trio did, written to `summary.json` when it stops.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Summary {
    pub seed: u64,
    /// Records the writer wrote (`appended` holds exactly these).
    pub appended: u64,
    /// Files the creator wrote and closed (`created/f0` ..).
    pub created: u64,
    /// The reader's rounds.
    pub reads: u64,
    /// Calls made, of every kind.
    pub calls: u64,
    /// Every failed call, counted by code (`ENOTCONN`, `EIO`, `SHORT`, ...).
    pub errors_by_code: BTreeMap<String, u64>,
    /// The first [`KEEP`] failed calls.
    pub errors: Vec<OpError>,
    /// The longest single call.
    pub longest: Option<SlowCall>,
    /// Every call at least [`Opts::slow_ms`] long, in the order they ended
    /// (each call takes that long, so a thread adds at most
    /// `1000 / slow_ms` a second: bounded by the run's length).
    pub slow: Vec<SlowCall>,
    /// `(ms since the epoch, records)`: each successful writer `fsync`,
    /// when it returned and how many records of `appended` it made
    /// durable.
    pub synced: Vec<(u64, u64)>,
    /// `(ms since the epoch, files)`: each successful creator `close`,
    /// when it returned and how many of `created/f0..` were closed then.
    pub closed: Vec<(u64, u64)>,
    pub started_ms: u64,
    pub stopped_ms: u64,
}

impl Summary {
    /// Every failed call (short writes and wrong reads included).
    pub fn error_count(&self) -> u64 {
        self.errors_by_code.values().sum()
    }

    /// The longest call that overlapped `[from_ms, to_ms]`, in ms (0 when
    /// none slower than [`Opts::slow_ms`] did).
    pub fn longest_within(&self, from_ms: u64, to_ms: u64) -> u64 {
        self.slow
            .iter()
            .chain(self.longest.iter())
            .filter(|c| c.start_ms <= to_ms && c.start_ms + c.ms >= from_ms)
            .map(|c| c.ms)
            .max()
            .unwrap_or(0)
    }

    /// What had been acknowledged before `at_ms`: records of `appended` a
    /// returned `fsync` covered, and files of `created/` a returned
    /// `close` ended.
    pub fn acked_before(&self, at_ms: u64) -> (u64, u64) {
        let last = |acks: &[(u64, u64)]| {
            acks.iter()
                .filter(|(t, _)| *t < at_ms)
                .map(|(_, n)| *n)
                .max()
                .unwrap_or(0)
        };
        (last(&self.synced), last(&self.closed))
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The trio's shared tally.
struct Tally {
    slow_ms: u64,
    calls: AtomicU64,
    /// The longest call so far, so a fast call takes no lock.
    longest_ms: AtomicU64,
    inner: Mutex<Summary>,
}

impl Tally {
    /// A call `op` that started at `started` (`at_ms` on the wall clock)
    /// ended now.
    fn timed(&self, op: &str, at_ms: u64, started: Instant) {
        self.took(op, at_ms, started.elapsed().as_millis() as u64);
    }

    /// A call `op` that started at `at_ms` took `ms`.
    fn took(&self, op: &str, at_ms: u64, ms: u64) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if ms < self.slow_ms && ms <= self.longest_ms.load(Ordering::Relaxed) {
            return;
        }
        let mut s = self.inner.lock().unwrap();
        let call = SlowCall {
            op: op.to_string(),
            start_ms: at_ms,
            ms,
        };
        if s.longest.as_ref().is_none_or(|l| ms > l.ms) {
            self.longest_ms.store(ms, Ordering::Relaxed);
            s.longest = Some(call.clone());
        }
        if ms >= self.slow_ms {
            s.slow.push(call);
        }
    }

    fn failed(&self, op: &str, at_ms: u64, code: &str, message: String) {
        let mut s = self.inner.lock().unwrap();
        *s.errors_by_code.entry(code.to_string()).or_default() += 1;
        if s.errors.len() < KEEP {
            s.errors.push(OpError {
                op: op.to_string(),
                at_ms,
                code: code.to_string(),
                message,
            });
        }
    }

    fn io_failed(&self, op: &str, at_ms: u64, e: &io::Error) {
        self.failed(
            op,
            at_ms,
            Code::from_io_error(e).posix_name(),
            e.to_string(),
        );
    }
}

/// `close(2)` of `file`, its status checked (`File`'s drop ignores it).
fn close(file: File) -> io::Result<()> {
    let fd = file.into_raw_fd();
    // SAFETY: `fd` was just taken out of a `File`, so it is open and owned
    // here, and nothing else closes it.
    if unsafe { libc::close(fd) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Sleeps so a loop runs `per_s` times a second (never catching up in a
/// burst after a stall: a stalled call is the subject, not something to
/// make up for).
struct Pace {
    every: Duration,
    next: Instant,
}

impl Pace {
    fn new(per_s: f64) -> Pace {
        Pace {
            every: Duration::from_secs_f64(1.0 / per_s.max(0.001)),
            next: Instant::now(),
        }
    }

    fn wait(&mut self) {
        self.next += self.every;
        let now = Instant::now();
        if self.next > now {
            std::thread::sleep(self.next - now);
        } else {
            self.next = now;
        }
    }
}

/// Run the trio in `dir` until `stop` is set; `fixed` and `created/` are
/// made first. Fails only when that setup fails: every later error is
/// counted in the [`Summary`], never returned.
pub fn run(
    dir: &Path,
    opts: Opts,
    stop: Arc<AtomicBool>,
    calls: Arc<AtomicU64>,
) -> io::Result<Summary> {
    let seed = opts.seed;
    std::fs::create_dir_all(dir.join("created"))?;
    let fixed = fixed_data(seed);
    std::fs::write(dir.join("fixed"), &fixed)?;
    let appended = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(dir.join("appended"))?;
    let held = File::open(dir.join("fixed"))?;
    let tally = Arc::new(Tally {
        slow_ms: opts.slow_ms.max(1),
        calls: AtomicU64::new(0),
        longest_ms: AtomicU64::new(0),
        inner: Mutex::new(Summary {
            seed,
            started_ms: now_ms(),
            ..Summary::default()
        }),
    });
    let mut threads = Vec::new();

    let (t, s) = (tally.clone(), stop.clone());
    let records_s = (opts.write_kib_s.max(1) * 1024) as f64 / RECORD as f64;
    threads.push(std::thread::spawn(move || {
        let file = appended;
        let mut pace = Pace::new(records_s);
        let mut i = 0u64;
        while !s.load(Ordering::SeqCst) {
            let data = record(seed, i, RECORD);
            let (at, t0) = (now_ms(), Instant::now());
            match file.write_at(&data, i * RECORD as u64) {
                Ok(n) if n == RECORD => {}
                Ok(n) => t.failed(
                    "writer: pwrite",
                    at,
                    "SHORT",
                    format!("{n} of {RECORD} bytes at record {i}"),
                ),
                Err(e) => {
                    t.io_failed("writer: pwrite", at, &e);
                    t.timed("writer: pwrite", at, t0);
                    pace.wait();
                    continue;
                }
            }
            t.timed("writer: pwrite", at, t0);
            i += 1;
            if i.is_multiple_of(SYNC_EVERY) {
                let (at, t0) = (now_ms(), Instant::now());
                match file.sync_all() {
                    Ok(()) => t.inner.lock().unwrap().synced.push((now_ms(), i)),
                    Err(e) => t.io_failed("writer: fsync", at, &e),
                }
                t.timed("writer: fsync", at, t0);
            }
            pace.wait();
        }
        let (at, t0) = (now_ms(), Instant::now());
        match file.sync_all() {
            Ok(()) => t.inner.lock().unwrap().synced.push((now_ms(), i)),
            Err(e) => t.io_failed("writer: final fsync", at, &e),
        }
        t.timed("writer: final fsync", at, t0);
        let (at, t0) = (now_ms(), Instant::now());
        if let Err(e) = close(file) {
            t.io_failed("writer: close", at, &e);
        }
        t.timed("writer: close", at, t0);
        t.inner.lock().unwrap().appended = i;
    }));

    let (t, s) = (tally.clone(), stop.clone());
    let created_dir = dir.join("created");
    let creates_s = opts.creates_s.max(1) as f64;
    threads.push(std::thread::spawn(move || {
        let mut pace = Pace::new(creates_s);
        let mut i = 0u64;
        while !s.load(Ordering::SeqCst) {
            let data = created_data(seed, i);
            let path = created_dir.join(format!("f{i}"));
            let (at, t0) = (now_ms(), Instant::now());
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path);
            t.timed("creator: open", at, t0);
            let file = match file {
                Ok(f) => f,
                Err(e) => {
                    t.io_failed("creator: open", at, &e);
                    pace.wait();
                    continue;
                }
            };
            let (at, t0) = (now_ms(), Instant::now());
            let mut ok = match file.write_at(&data, 0) {
                Ok(n) if n == data.len() => true,
                Ok(n) => {
                    t.failed(
                        "creator: write",
                        at,
                        "SHORT",
                        format!("{n} of {} bytes of f{i}", data.len()),
                    );
                    false
                }
                Err(e) => {
                    t.io_failed("creator: write", at, &e);
                    false
                }
            };
            t.timed("creator: write", at, t0);
            let (at, t0) = (now_ms(), Instant::now());
            match close(file) {
                Ok(()) if ok => t.inner.lock().unwrap().closed.push((now_ms(), i + 1)),
                Ok(()) => {}
                Err(e) => {
                    t.io_failed("creator: close", at, &e);
                    ok = false;
                }
            }
            t.timed("creator: close", at, t0);
            // A failed file is rewritten under the same name: `created`
            // counts f0.. as written and closed.
            if ok {
                i += 1;
            }
            pace.wait();
        }
        t.inner.lock().unwrap().created = i;
    }));

    let (t, s) = (tally.clone(), stop.clone());
    let fixed_path = dir.join("fixed");
    let reads_s = opts.reads_s.max(1) as f64;
    threads.push(std::thread::spawn(move || {
        let mut pace = Pace::new(reads_s);
        let mut buf = vec![0u8; FIXED_LEN];
        let mut rounds = 0u64;
        let check = |what: &str, at: u64, got: io::Result<usize>, buf: &[u8]| match got {
            Ok(n) if n == FIXED_LEN && buf == &fixed[..] => {}
            Ok(n) => t.failed(
                what,
                at,
                "MISMATCH",
                format!("{n} bytes read, content differs from what was written"),
            ),
            Err(e) => t.io_failed(what, at, &e),
        };
        while !s.load(Ordering::SeqCst) {
            let (at, t0) = (now_ms(), Instant::now());
            let got = read_full(&held, &mut buf);
            t.timed("reader: pread (held)", at, t0);
            check("reader: pread (held)", at, got, &buf);

            let (at, t0) = (now_ms(), Instant::now());
            match File::open(&fixed_path) {
                Ok(f) => {
                    t.timed("reader: open", at, t0);
                    let (at, t0) = (now_ms(), Instant::now());
                    let got = read_full(&f, &mut buf);
                    t.timed("reader: pread", at, t0);
                    check("reader: pread", at, got, &buf);
                    let (at, t0) = (now_ms(), Instant::now());
                    if let Err(e) = close(f) {
                        t.io_failed("reader: close", at, &e);
                    }
                    t.timed("reader: close", at, t0);
                }
                Err(e) => {
                    t.timed("reader: open", at, t0);
                    t.io_failed("reader: open", at, &e);
                }
            }
            rounds += 1;
            pace.wait();
        }
        t.inner.lock().unwrap().reads = rounds;
    }));

    // The caller's view of progress, while the threads run.
    while !stop.load(Ordering::SeqCst) {
        calls.store(tally.calls.load(Ordering::Relaxed), Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(50));
    }
    for th in threads {
        if th.join().is_err() {
            tally.failed("trio", now_ms(), "PANIC", "a load thread panicked".into());
        }
    }
    let mut summary = tally.inner.lock().unwrap().clone();
    summary.calls = tally.calls.load(Ordering::Relaxed);
    summary.stopped_ms = now_ms();
    calls.store(summary.calls, Ordering::Relaxed);
    Ok(summary)
}

/// `pread`s from offset 0 until `buf` is full or the file ends.
fn read_full(file: &File, buf: &mut [u8]) -> io::Result<usize> {
    let mut at = 0;
    while at < buf.len() {
        match file.read_at(&mut buf[at..], at as u64)? {
            0 => break,
            n => at += n,
        }
    }
    Ok(at)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The trio on a local directory: every byte it wrote is what the
    /// seed says, no call failed, and the summary counts what is there.
    #[test]
    fn the_trio_writes_what_the_seed_says() {
        let dir = tempfile::tempdir().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let calls = Arc::new(AtomicU64::new(0));
        let opts = Opts {
            seed: 7,
            write_kib_s: 4096,
            creates_s: 200,
            reads_s: 200,
            slow_ms: 100,
        };
        let flag = stop.clone();
        let stopper = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(400));
            flag.store(true, Ordering::SeqCst);
        });
        let s = run(dir.path(), opts, stop, calls.clone()).unwrap();
        stopper.join().unwrap();
        assert_eq!(s.error_count(), 0, "{:?}", s.errors);
        assert!(s.appended > 0 && s.created > 0 && s.reads > 0, "{s:?}");
        assert_eq!(calls.load(Ordering::Relaxed), s.calls);
        // The final `fsync` acknowledged every record, the last `close`
        // every file.
        assert_eq!(s.acked_before(u64::MAX), (s.appended, s.created));
        let appended = std::fs::read(dir.path().join("appended")).unwrap();
        assert_eq!(appended, appended_data(7, s.appended));
        for i in 0..s.created {
            let got = std::fs::read(dir.path().join(format!("created/f{i}"))).unwrap();
            assert_eq!(got, created_data(7, i), "f{i}");
        }
        assert_eq!(
            std::fs::read(dir.path().join("fixed")).unwrap(),
            fixed_data(7)
        );
    }

    #[test]
    fn a_window_takes_the_calls_that_overlap_it() {
        let call = |start_ms, ms| SlowCall {
            op: "w".into(),
            start_ms,
            ms,
        };
        let s = Summary {
            slow: vec![call(1_000, 300), call(5_000, 900), call(9_000, 150)],
            longest: Some(call(5_000, 900)),
            ..Summary::default()
        };
        assert_eq!(s.longest_within(1_200, 2_000), 300);
        assert_eq!(s.longest_within(5_800, 9_100), 900);
        assert_eq!(s.longest_within(9_100, 9_200), 150);
        assert_eq!(s.longest_within(2_000, 4_000), 0);
    }

    /// However many slow calls came before it (a rollout under load can
    /// take minutes of them), a call overlapping the window is still seen.
    #[test]
    fn a_window_after_many_slow_calls_still_sees_its_own() {
        let tally = Tally {
            slow_ms: 100,
            calls: AtomicU64::new(0),
            longest_ms: AtomicU64::new(0),
            inner: Mutex::new(Summary::default()),
        };
        tally.took("w", 0, 20_000);
        for i in 0..(KEEP as u64 * 5) {
            tally.took("w", 30_000 + i * 1_000, 500);
        }
        tally.took("w", 2_000_000, 50);
        tally.took("w", 2_000_100, 640);
        let s = tally.inner.lock().unwrap().clone();
        assert_eq!(s.slow.len(), KEEP * 5 + 2);
        assert_eq!(s.longest_within(2_000_200, 2_000_500), 640);
        assert_eq!(s.longest_within(1_900_000, 1_950_000), 0);
    }

    #[test]
    fn acknowledgements_before_a_time() {
        let s = Summary {
            synced: vec![(1_000, 16), (2_000, 32), (3_000, 48)],
            closed: vec![(1_500, 1), (2_500, 2)],
            ..Summary::default()
        };
        assert_eq!(s.acked_before(500), (0, 0));
        assert_eq!(s.acked_before(2_000), (16, 1));
        assert_eq!(s.acked_before(2_001), (32, 1));
        assert_eq!(s.acked_before(9_000), (48, 2));
    }
}
