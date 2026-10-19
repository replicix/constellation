//! `log-sink-stall` (plan 44 §7.3): a daemon's log sink stalls, and no
//! daemon thread notices. The node's stderr is `daemon.log` (here the
//! harness's `mount.log`); a buffered `write(2)` to a busy btrfs waits
//! seconds, and before plan 44 every thread that logged waited with it: the
//! authority core in the middle of a step, a FUSE worker holding the
//! store's writer lock, the holder's heartbeat thread (a backup's takeover
//! window is 1.5 s; the loaded runs of 2026-10-17 saw a core step stuck
//! 12.5 s in a stderr write, and a delegation lost to it).
//!
//! The stall is the daemon's own fault point
//! (`CONSTELLATION_FAULT_LOG_STALL_FILE`: each sink write sleeps holding
//! std's stderr lock, as a stalled `write(2)` would), armed for one daemon
//! by pid. Verification is on in the campaign runs; this scenario's daemons
//! log the lock targets at debug, so the core logs on every renewal.
//!
//! - **Part A (no drops, nothing stalls):** two nodes, `b` the root's
//!   holder and `a` its backup; a writer on `b` holds an `flock` and
//!   appends + `fsync`s every 100 ms; `b`'s sink is stalled 12 s three
//!   times, 15 s apart. `a` never takes over, `b`'s epoch holds, the
//!   writer sees no error and no append waits 2 s, `b`'s
//!   `log.longest_write_ms` shows the stall (else the scenario proved
//!   nothing), `log.dropped_lines` is 0 (the harness's 128 MiB queue), and
//!   `b`'s log holds lines stamped inside each stall window (queued, not
//!   lost).
//! - **Part B (drops, counted and marked):** a lone node with a 64 KiB queue
//!   at `RUST_LOG=debug`, declared with `verify::expect_log_drops`; one 5 s
//!   stall (armed for its pid) under a workload: lines were dropped, the
//!   log's `log lines dropped` lines sum to the status counter and are as
//!   many as `drop_episodes`, and no op waited 1 s (the synchronous
//!   writer's ops wait the stall out).
//! - **Part C (the delegation of the evidence):** `/d1` delegated to `b`
//!   (5 s grants), a writer on `b` in `/d1`, `b`'s sink stalled 12 s twice:
//!   `b`'s generation unchanged, `a` reclaimed and recalled nothing, `b`'s
//!   grant never lapsed, no write error, no write waits 2 s (a stalled core
//!   no longer loses the delegation, since the off-core renewal lane, so the
//!   latency is what shows the synchronous writer).
//!
//! **The control arm** (`CONSTELLATION_HARNESS_LOG_SINK_STALL_CONTROL=1`,
//! run once per gate) runs A, B and C with `CONSTELLATION_LOG_QUEUE_BYTES=0`,
//! the synchronous writer, where the fault sleeps in the logging thread as a
//! stalled disk does (without the fix). They must **fail** their
//! assertions (a seal or takeover of `b`, a lost delegation, an error, a
//! slow append or a slow op); an arm that passes fails the scenario: the stall did not
//! reach a logging thread, so it proved nothing.

use super::delegroot::{delegate_d1, delegated_pair_with, dist, Writer};
use super::m11::{deleg_of, dump_logs_on_failure, n, unmount_all};
use super::m9::{cluster, wait_for_backup};
use super::{eventually, lease_of};
use crate::client::Client;
use anyhow::{bail, ensure, Context, Result};
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const NAME: &str = "log-sink-stall";
const CONTROL_ENV: &str = "CONSTELLATION_HARNESS_LOG_SINK_STALL_CONTROL";
const STALL_MS: u64 = 12_000;
/// Between the starts of two stalls (as each is observed).
const GAP: Duration = Duration::from_secs(15);
/// An append + `fsync` of part A's writer may not wait this long.
const APPEND_BOUND: Duration = Duration::from_secs(2);
/// An op of part B's workload, during a 5 s stall of the log sink: well
/// under the stall, which the synchronous writer's ops wait out.
const PART_B_OP_BOUND: Duration = Duration::from_secs(1);

/// `log-sink-stall`.
pub fn log_sink_stall(_seed: u64) -> Result<()> {
    let control = std::env::var(CONTROL_ENV).is_ok_and(|v| !matches!(v.trim(), "" | "0"));
    if control {
        eprintln!("    {NAME}: the control arm: parts A, B and C on the synchronous log writer");
    }
    part_a(control)?;
    part_b(control)?;
    part_c(control)
}

/// The control arm's verdict on a part: its stability assertions must have
/// failed; the normal arm's: none may have.
fn judge(part: &str, control: bool, violations: Vec<String>) -> Result<()> {
    if control {
        ensure!(
            !violations.is_empty(),
            "{NAME}: part {part}: the stall did not reach a logging thread: the scenario proves \
             nothing (the control arm, a synchronous log writer, passed every assertion)"
        );
        eprintln!(
            "    {NAME}: part {part}: the control arm failed as it must: {}",
            violations.join("; ")
        );
        Ok(())
    } else {
        ensure!(
            violations.is_empty(),
            "{NAME}: part {part}: {}",
            violations.join("; ")
        );
        Ok(())
    }
}

/// A part's result without its violations, for the log dump on a failure
/// (a control arm's expected violations are not one).
fn unit(r: &Result<Vec<String>>) -> Result<()> {
    match r {
        Ok(_) => Ok(()),
        Err(e) => bail!("{e:#}"),
    }
}

/// A fault file of this run for `part`.
fn fault_file(part: &str) -> PathBuf {
    std::env::temp_dir().join(format!("harness-{NAME}-{}-{part}", std::process::id()))
}

/// Arm one stall of `ms` on the daemon `pid` (`None`: whichever daemon of
/// the file's env reads it first): the file is written whole, by a rename
/// (a daemon that read a prefix would stall for the wrong time).
fn arm(file: &Path, ms: u64, pid: Option<u32>) -> Result<()> {
    let tmp = file.with_extension("tmp");
    let spec = match pid {
        Some(pid) => format!("{ms} once pid={pid}"),
        None => format!("{ms} once"),
    };
    std::fs::write(&tmp, spec)?;
    std::fs::rename(&tmp, file)?;
    Ok(())
}

/// Wait until a daemon took the armed stall (it removes the file as the
/// stalled write starts); when that was seen, in unix ms. `None`: no daemon
/// wrote a log line in `within`. In the normal arm that is a scenario that
/// proved nothing; in the control arm it is a daemon that stopped logging
/// because the stalls already took the root from it ([`stalled_out`]).
fn wait_taken(file: &Path, within: Duration) -> Option<i64> {
    let t = Instant::now();
    while file.exists() {
        if t.elapsed() >= within {
            return None;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Some(unix_ms())
}

/// The stall `file` armed was not taken: an error, except in the control
/// arm, where it ends the stalls (the synchronous writer already cost the
/// daemon what the arm shows).
fn stalled_out(control: bool, file: &Path, what: &str) -> Result<()> {
    ensure!(
        control,
        "{NAME}: {what}: no daemon wrote a log line in 30 s: the stall was never taken, the \
         scenario proves nothing"
    );
    eprintln!(
        "    {NAME}: {what}: the control arm's daemon logs nothing any more (it lost what it held): \
         no more stalls"
    );
    let _ = std::fs::remove_file(file);
    Ok(())
}

fn unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn log_of(c: &Client) -> Result<serde_json::Value> {
    Ok(c.control_status()?["log"].clone())
}

/// `node.logs.flush` on `c`: everything logged so far is in its log file.
fn flush(c: &Client) -> Result<serde_json::Value> {
    c.control_call(
        "node.logs.flush",
        serde_json::json!({"within_ms": 10_000}),
        Duration::from_secs(20),
    )
}

/// The holder of an `flock` on a file of the mount, appending a line and
/// `fsync`ing it every 100 ms until stopped.
type AppendOp = (Duration, Option<String>);

struct Appender {
    stop: Arc<AtomicBool>,
    /// Each append + `fsync`: how long it took, and its error.
    ops: Arc<Mutex<Vec<AppendOp>>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Appender {
    fn start(path: &Path) -> Result<Appender> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("opening {}", path.display()))?;
        // SAFETY: flock(2) on a descriptor this function owns.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        ensure!(
            rc == 0,
            "flock {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
        let stop = Arc::new(AtomicBool::new(false));
        let ops = Arc::new(Mutex::new(Vec::new()));
        let thread = {
            let (stop, ops) = (stop.clone(), ops.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let t = Instant::now();
                    let r = file.write_all(b"x\n").and_then(|_| file.sync_all());
                    ops.lock()
                        .unwrap()
                        .push((t.elapsed(), r.err().map(|e| e.to_string())));
                    std::thread::sleep(Duration::from_millis(100));
                }
            })
        };
        Ok(Appender {
            stop,
            ops,
            thread: Some(thread),
        })
    }

    /// Stop; `(ops, the longest, the errors)`.
    fn finish(mut self) -> (usize, Duration, Vec<String>) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let deadline = Instant::now() + Duration::from_secs(30);
            while !t.is_finished() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
            if t.is_finished() {
                let _ = t.join();
            }
        }
        let ops = self.ops.lock().unwrap();
        (
            ops.len(),
            ops.iter().map(|(d, _)| *d).max().unwrap_or_default(),
            ops.iter().filter_map(|(_, e)| e.clone()).collect(),
        )
    }
}

impl Drop for Appender {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Part A: the holder's sink stalls three times under a lock holder's
/// appends; nothing seals, takes over or waits.
fn part_a(control: bool) -> Result<()> {
    let file = fault_file("a");
    let file_s = file.display().to_string();
    let mut extra: Vec<(&str, &str)> = vec![
        ("CONSTELLATION_LOCK_TTL_MS", "5000"),
        (
            "RUST_LOG",
            "info,constellation_authority::core::locks=debug",
        ),
        ("CONSTELLATION_FAULT_LOG_STALL_FILE", &file_s),
    ];
    if control {
        extra.push(("CONSTELLATION_LOG_QUEUE_BYTES", "0"));
    }
    // `b` mounts first: the root's holder; `a` its backup.
    let (_env, _root, mut clients, _) = cluster(&format!("{NAME}-a"), &["b", "a"], &extra, 0)?;
    let result = (|| -> Result<Vec<String>> {
        let (b, a) = (&clients[0], &clients[1]);
        let lease0 = lease_of(b)?;
        ensure!(lease0["held"] == true, "b does not hold the root: {lease0}");
        let epoch0 = lease0["epoch"].as_u64().unwrap_or(0);
        wait_for_backup(b, crate::load::scaled(Duration::from_secs(30)))?;
        let takeovers0 = n(&crate::verify::counters(a), "backup_takeovers");
        let pid = b.pid().context("b has no pid")?;
        let appender = Appender::start(&b.mnt.join("lk"))?;
        // Some appends land first: the token checker reads their `tagged
        // op landed` lines (a log with none is a checker gone blind, which
        // is what a control arm whose only tagged write was refused would
        // otherwise look like).
        std::thread::sleep(Duration::from_secs(3));
        let mut windows = Vec::new();
        for round in 0..3 {
            arm(&file, STALL_MS, Some(pid))?;
            let Some(at) = wait_taken(&file, Duration::from_secs(30)) else {
                stalled_out(control, &file, &format!("part A: stall {round}"))?;
                break;
            };
            eprintln!("    {NAME}: part A: stall {round} of {STALL_MS} ms began at {at}");
            windows.push((at + 1_000, at + STALL_MS as i64 - 1_000));
            std::thread::sleep(GAP);
        }
        let (appends, longest, errors) = appender.finish();
        std::thread::sleep(Duration::from_secs(1));

        let mut v = Vec::new();
        let takeovers = n(&crate::verify::counters(a), "backup_takeovers");
        if takeovers != takeovers0 {
            v.push(format!(
                "a took over through its backup {} time(s) while b's log sink stalled",
                takeovers - takeovers0
            ));
        }
        match lease_of(b) {
            Ok(lease) if lease["held"] == true && lease["epoch"].as_u64() == Some(epoch0) => {}
            Ok(lease) => v.push(format!("b's lease is not epoch {epoch0} held: {lease}")),
            Err(e) => v.push(format!("b's lease: {e:#}")),
        }
        if !errors.is_empty() {
            v.push(format!(
                "the writer on b saw {} errors, the first {:?}",
                errors.len(),
                errors[0]
            ));
        }
        if longest >= APPEND_BOUND {
            v.push(format!(
                "an append + fsync on b waited {longest:?} (bound {APPEND_BOUND:?})"
            ));
        }
        eprintln!(
            "    {NAME}: part A: {appends} appends, longest {longest:?}; a's backup takeovers \
             +{}",
            takeovers - takeovers0
        );
        if control {
            return Ok(v);
        }
        // The normal arm: the stall happened, nothing was dropped, and what
        // b logged meanwhile was queued, not lost.
        let log = log_of(b)?;
        let longest_write = log["longest_write_ms"].as_u64().unwrap_or(0);
        ensure!(
            longest_write >= STALL_MS - 1_000,
            "b's longest log write was {longest_write} ms: the stall never reached its log \
             writer, the scenario proved nothing ({log})"
        );
        if log["dropped_lines"].as_u64() != Some(0) {
            v.push(format!("b dropped log lines: {log}"));
        }
        flush(b)?;
        let text = b.log_text();
        for (round, (from, to)) in windows.iter().enumerate() {
            let inside = text
                .lines()
                .filter_map(|l| crate::verify::line_unix_ms(&crate::verify::strip_ansi(l)))
                .filter(|t| t >= from && t <= to)
                .count();
            eprintln!("    {NAME}: part A: stall {round}: {inside} log line(s) stamped inside it");
            if inside == 0 {
                v.push(format!(
                    "b's log has no line stamped inside stall {round} ({from}..{to}): its lines \
                     were lost, or it logged none (the scenario proves nothing)"
                ));
            }
        }
        Ok(v)
    })();
    dump_logs_on_failure(NAME, &clients, &unit(&result));
    unmount_all(&mut clients);
    let _ = std::fs::remove_file(&file);
    judge("A", control, result?)
}

/// Part B: a small queue overflows behind a stalled sink; the drops are
/// counted and marked, and nothing waits.
fn part_b(control: bool) -> Result<()> {
    let file = fault_file("b");
    let file_s = file.display().to_string();
    let queue = if control { "0" } else { "65536" };
    let extra: Vec<(&str, &str)> = vec![
        ("CONSTELLATION_LOG_QUEUE_BYTES", queue),
        ("RUST_LOG", "debug"),
        ("CONSTELLATION_FAULT_LOG_STALL_FILE", &file_s),
    ];
    let (_env, _root, mut clients, _) = cluster(&format!("{NAME}-b"), &["c"], &extra, 0)?;
    let result = (|| -> Result<Vec<String>> {
        let c = &clients[0];
        // This daemon drops on purpose: its drops are reported, not a
        // violation of the checkers that read its log.
        crate::verify::expect_log_drops(c);
        arm(&file, 5_000, Some(c.pid().context("c has no pid")?))?;
        let from = Instant::now();
        if wait_taken(&file, Duration::from_secs(30)).is_none() {
            stalled_out(false, &file, "part B")?;
        }
        // A workload through the stall: create, write, fsync, unlink.
        let mut slowest = (Duration::ZERO, Instant::now());
        let mut ops = 0u64;
        let mut all = Vec::new();
        while from.elapsed() < Duration::from_secs(5) {
            let at = Instant::now();
            let path = c.mnt.join(format!("w-{ops}"));
            let mut f = std::fs::File::create(&path)?;
            f.write_all(&vec![b'p'; 4096])?;
            f.sync_all()?;
            drop(f);
            std::fs::remove_file(&path)?;
            let took = at.elapsed();
            all.push(took);
            if took > slowest.0 {
                slowest = (took, at);
            }
            ops += 1;
            // A steady, moderate rate: a fast host would otherwise write
            // thousands of metadata segments to the bucket in the 5 s,
            // which the bucket log check then has to read back.
            std::thread::sleep(Duration::from_millis(5));
        }
        eprintln!(
            "    {NAME}: part B: {ops} ops through the stall: {}",
            dist(&all)
        );
        if control {
            // The synchronous writer: the ops wait the stall out.
            let mut v = Vec::new();
            if slowest.0 >= PART_B_OP_BOUND {
                v.push(format!(
                    "an op waited {:?} behind the stalled log sink (bound {PART_B_OP_BOUND:?})",
                    slowest.0
                ));
            }
            return Ok(v);
        }
        crate::hostlag::latency_bound(
            "an op during the log sink stall",
            slowest.1,
            slowest.0,
            Duration::ZERO,
            PART_B_OP_BOUND,
        )?;
        // The sink recovers; the writer closes the episodes. The counters
        // and the log's summary lines then agree.
        let mut last = String::new();
        eventually(
            "the log's summary lines account for the status counters",
            Duration::from_secs(30),
            || {
                flush(c)?;
                let log = log_of(c)?;
                let dropped = log["dropped_lines"].as_u64().unwrap_or(0);
                let episodes = log["drop_episodes"].as_u64().unwrap_or(0);
                ensure!(dropped > 0, "no line was dropped: {log}");
                let (sum, count) = summaries(&c.log_text());
                last = format!("status {dropped} lines / {episodes} episodes; log {sum} / {count}");
                ensure!(
                    sum == dropped && count == episodes,
                    "the log's summary lines say {sum} lines in {count} episode(s): {log}"
                );
                Ok(())
            },
        )?;
        eprintln!("    {NAME}: part B: {last}");
        Ok(Vec::new())
    })();
    dump_logs_on_failure(NAME, &clients, &unit(&result));
    unmount_all(&mut clients);
    let _ = std::fs::remove_file(&file);
    judge("B", control, result?)
}

/// The `log lines dropped` summary lines of `text`: the sum of their
/// `dropped_lines` and their number.
fn summaries(text: &str) -> (u64, u64) {
    let mut sum = 0;
    let mut count = 0;
    for l in text.lines().filter(|l| l.contains("log lines dropped")) {
        count += 1;
        sum += crate::verify::field(&crate::verify::strip_ansi(l), "dropped_lines")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
    }
    (sum, count)
}

/// Part C: the delegation of the evidence. `b`'s sink stalls under its
/// delegate writes; its grant is renewed through the stall.
fn part_c(control: bool) -> Result<()> {
    let file = fault_file("c");
    let file_s = file.display().to_string();
    let mut extra: Vec<(&str, &str)> = vec![
        ("RUST_LOG", "info,constellation_authority=debug"),
        ("CONSTELLATION_FAULT_LOG_STALL_FILE", &file_s),
    ];
    if control {
        extra.push(("CONSTELLATION_LOG_QUEUE_BYTES", "0"));
    }
    let (_env, _root, mut clients) = delegated_pair_with(&format!("{NAME}-c"), true, &extra)?;
    let result = (|| -> Result<Vec<String>> {
        delegate_d1(NAME, &clients, true)?;
        let (a, b) = (&clients[0], &clients[1]);
        let w = Writer::start(b.mnt.clone(), "d1");
        w.wait_ops_after(Duration::ZERO, 20, Duration::from_secs(30))?;
        // The generation b holds /d1 under.
        let held = |d: &serde_json::Value| -> Option<u64> {
            d["mine"].as_array()?.iter().find_map(|m| {
                (m[3] == false && m[2].as_i64().unwrap_or(0) > 0)
                    .then(|| m[1].as_u64().unwrap_or(0))
            })
        };
        let gen0 = held(&deleg_of(b)?).context("b holds no delegation of /d1")?;
        let (da0, db0) = (deleg_of(a)?, deleg_of(b)?);
        let pid = b.pid().context("b has no pid")?;
        let started = w.now();
        for round in 0..2 {
            arm(&file, STALL_MS, Some(pid))?;
            let Some(at) = wait_taken(&file, Duration::from_secs(30)) else {
                stalled_out(control, &file, &format!("part C: stall {round}"))?;
                break;
            };
            eprintln!("    {NAME}: part C: stall {round} of {STALL_MS} ms began at {at}");
            std::thread::sleep(GAP);
        }
        let (ops, errs) = w.after(started);
        // Let the verdict settle (a control arm's wedged daemon recovers).
        std::thread::sleep(Duration::from_secs(1));
        let mut v = Vec::new();
        let (da, db) = (deleg_of(a), deleg_of(b));
        let (da, db) = match (da, db) {
            (Ok(da), Ok(db)) => (da, db),
            (da, db) => {
                v.push(format!(
                    "a delegation report did not answer: a {:?}, b {:?}",
                    da.err().map(|e| format!("{e:#}")),
                    db.err().map(|e| format!("{e:#}"))
                ));
                return Ok(v);
            }
        };
        let delta = |d: &serde_json::Value, d0: &serde_json::Value, k: &str| {
            n(d, k).saturating_sub(n(d0, k))
        };
        let gen = held(&db);
        if gen != Some(gen0) {
            v.push(format!(
                "b's generation {gen0} did not survive: now {gen:?}: {db}"
            ));
        }
        for k in ["reclaimed", "recalls_sent"] {
            if delta(&da, &da0, k) != 0 {
                v.push(format!("a's `{k}` moved by {}: {da}", delta(&da, &da0, k)));
            }
        }
        if delta(&db, &db0, "lapsed") != 0 {
            v.push(format!(
                "b's grant lapsed {} time(s): {db}",
                delta(&db, &db0, "lapsed")
            ));
        }
        if !errs.is_empty() {
            v.push(format!(
                "{} of b's writes failed, the first {:?}",
                errs.len(),
                errs[0]
            ));
        }
        // Since the delegate renews its grant off the core, a logging core
        // no longer loses the delegation, but the writes are still served by
        // threads that log: none may wait for the sink.
        if let Some(slowest) = ops.iter().max().filter(|d| **d >= APPEND_BOUND) {
            v.push(format!(
                "a write of b's under its delegation waited {slowest:?} (bound {APPEND_BOUND:?})"
            ));
        }
        eprintln!(
            "    {NAME}: part C: b's writes through the stalls: {}; generation {gen0} -> {gen:?}; \
             a reclaimed +{} recalls +{}; b lapsed +{}",
            dist(&ops),
            delta(&da, &da0, "reclaimed"),
            delta(&da, &da0, "recalls_sent"),
            delta(&db, &db0, "lapsed"),
        );
        w.finish();
        if control {
            return Ok(v);
        }
        let log = log_of(b)?;
        let longest_write = log["longest_write_ms"].as_u64().unwrap_or(0);
        ensure!(
            longest_write >= STALL_MS - 1_000,
            "b's longest log write was {longest_write} ms: the stall never reached its log \
             writer, the scenario proved nothing ({log})"
        );
        Ok(v)
    })();
    dump_logs_on_failure(NAME, &clients, &unit(&result));
    unmount_all(&mut clients);
    let _ = std::fs::remove_file(&file);
    judge("C", control, result?)
}
