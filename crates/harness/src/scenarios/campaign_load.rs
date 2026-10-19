//! The host-load campaign's phase-3 scenarios (load campaign §5.4): the
//! shapes the existing catalog does not cover, each pausing, stalling or
//! starving a daemon itself at the point that matters and checking what
//! must hold whatever the load did (`crate::verify`'s checkers). They run
//! by name only (`scenarios::CAMPAIGN`), under any profile the runner
//! adds:
//!
//! - `lock-contention-paused-holder`: a flock'ed counter on three nodes;
//!   the holder is frozen across each lock boundary
//!   ([`crate::timings::Timings::boundaries`]): the count is exact, an op
//!   after the thaw answers `EIO` only when its grant was fenced, and
//!   always when the freeze outlasted the window the holder honours
//!   (`lock-lapse` and past) or the owner outwaited the grant.
//! - `lock-contention-stalled-owner`: the same counter, with the owner's
//!   (the root's) store stalled 5/12/20 s while every node contends,
//!   finding F1's interleaving in the real daemon: exact count, the
//!   fencing-token order (checks and landed writes) and the errno classes.
//! - `delegate-paused-across-ttl`: a delegate frozen around its grant's
//!   lapse and outwait under its own writes and a root-side cross-subtree
//!   rename: every write once (the bucket log), every acknowledged one
//!   there, the moved file in one place, no generation the root ended
//!   live at the delegate afterwards (a recalled generation's late answer
//!   installs no stale grant; live past the clock-skew margin in two
//!   samples in a row), errors only in doubt past the deadline.
//! - `acks3-under-load`: an `ack=s3` holder whose store stalls 1-3 s at a
//!   time (run it under `cpu:25@a`): no fast takeover of a live holder, no
//!   rollback of an acknowledged write (finding F2b's twin).
//! - `endpoint-wedge-probe`: one node of a busy three-node cluster frozen
//!   8 s per round (F11): links come back, no FUSE request stalls, no
//!   write fails.
//!
//! `CONSTELLATION_HARNESS_ROUNDS` sets the rounds per duration (default 3;
//! the review asks 20 per lock boundary and 10 wedge rounds for the
//! campaign proper), `CONSTELLATION_HARNESS_STALLS_MS` the owner stalls.

use super::m9::{cluster, node_id};
use super::{eventually, lease_of};
use crate::client::Client;
use crate::timings::Timings;
use crate::verify::{ErrnoLog, Evidence, FileId, Ledger, OpCtx};
use anyhow::{bail, Context, Result};
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const LOCK_TTL_MS: u64 = 3_000;

fn rounds(default: usize) -> usize {
    std::env::var("CONSTELLATION_HARNESS_ROUNDS")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn stalls_ms() -> Vec<u64> {
    std::env::var("CONSTELLATION_HARNESS_STALLS_MS")
        .ok()
        .map(|v| v.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .filter(|v: &Vec<u64>| !v.is_empty())
        .unwrap_or_else(|| vec![5_000, 12_000, 20_000])
}

fn n(v: &serde_json::Value, k: &str) -> u64 {
    v[k].as_u64().unwrap_or(0)
}

fn locks(c: &Client) -> Result<serde_json::Value> {
    Ok(c.control_status()?["locks"].clone())
}

fn read_counter(path: &Path) -> Result<u64> {
    let b = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    anyhow::ensure!(b.len() == 8, "{}: {} bytes", path.display(), b.len());
    Ok(u64::from_be_bytes(b[..8].try_into().unwrap()))
}

/// Arm the store-stall hook at `file` for one write of `ms` on the daemon
/// `pid` (written whole, by a rename).
fn arm_stall(file: &Path, ms: u64, pid: u32) -> Result<()> {
    let tmp = file.with_extension("tmp");
    std::fs::write(&tmp, format!("{ms} once pid={pid}"))?;
    std::fs::rename(&tmp, file)?;
    Ok(())
}

/// What one counter worker did.
#[derive(Debug, Default, Clone, Copy)]
struct Count {
    done: u64,
    /// The write went out but the `fsync` failed: it may have landed.
    doubt: u64,
}

/// Why an increment failed: at which step, and the error.
#[derive(Debug)]
struct IncFail {
    /// The `fsync` failed after the write went out: the increment may
    /// have landed (in doubt) unless it was fenced (`EIO`: the dirty
    /// write is discarded at the publication, F10).
    at_fsync: bool,
    err: std::io::Error,
}

impl IncFail {
    fn eio(&self) -> bool {
        self.err.raw_os_error() == Some(libc::EIO)
    }
}

/// One increment under the held lock of `f` (read, add one, write,
/// `fsync`); every step's error is the application's.
fn increment(f: &std::fs::File) -> std::result::Result<(), IncFail> {
    let early = |err| IncFail {
        at_fsync: false,
        err,
    };
    let mut buf = [0u8; 8];
    f.read_at(&mut buf, 0).map_err(early)?;
    f.write_at(&(u64::from_be_bytes(buf) + 1).to_be_bytes(), 0)
        .map_err(early)?;
    f.sync_all().map_err(|err| IncFail {
        at_fsync: true,
        err,
    })
}

/// Record a failed increment (under the lock) and count it: an `fsync`
/// that failed for anything but a fence leaves the increment in doubt.
fn note_failed(node: &str, fail: &IncFail, t: Instant, errnos: &ErrnoLog, c: &mut Count) {
    errnos.record(
        node,
        if fail.at_fsync {
            "increment fsync"
        } else {
            "increment"
        },
        &fail.err,
        t,
        OpCtx {
            under_lock: true,
            ..Default::default()
        },
    );
    if fail.at_fsync && !fail.eio() {
        c.doubt += 1;
    }
}

/// Increment `mnt/counter` under `flock` until `stop`, every error in the
/// errno log.
fn counter_until(mnt: &Path, node: &str, errnos: &ErrnoLog, stop: &AtomicBool) -> Count {
    let mut c = Count::default();
    let Ok(f) = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(mnt.join("counter"))
    else {
        return c;
    };
    while !stop.load(Ordering::Relaxed) {
        let t = Instant::now();
        // SAFETY: flock(2) on an fd this function owns.
        if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX) } != 0 {
            let e = std::io::Error::last_os_error();
            errnos.record(
                node,
                "flock",
                &e,
                t,
                OpCtx {
                    lock_request: true,
                    ..Default::default()
                },
            );
            std::thread::sleep(Duration::from_millis(200));
            continue;
        }
        let t = Instant::now();
        match increment(&f) {
            Ok(()) => c.done += 1,
            Err(fail) => note_failed(node, &fail, t, errnos, &mut c),
        }
        // SAFETY: as above.
        unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_UN) };
        std::thread::sleep(Duration::from_millis(30));
    }
    c
}

/// The checks every counter scenario ends with: the count exact (within
/// what was in doubt), the errors in the documented set, the token order
/// and lease epochs from the logs (which must have seen grants, tagged
/// ops and landed writes).
fn finish_counter(
    name: &str,
    clients: &[Client],
    acked: u64,
    doubt: u64,
    errnos: &ErrnoLog,
) -> Result<()> {
    let got = read_counter(&clients[0].mnt.join("counter"))?;
    eprintln!("    {name}: counter {got}: {acked} increments acknowledged, {doubt} in doubt");
    // The errors first: a miscount is read against what the increments
    // were answered.
    let refs: Vec<&Client> = clients.iter().collect();
    let ev = Evidence::from_clients(&refs, super::m9::client_deadline());
    let rep = errnos.judge(&ev);
    eprintln!("    {name}: errors by class {:?}", rep.counts);
    anyhow::ensure!(
        got >= acked && got <= acked + doubt,
        "lock exclusion: the counter is {got}, {acked} increments were acknowledged and {doubt} \
         were in doubt (lost or doubled increments)"
    );
    anyhow::ensure!(
        rep.bugs.is_empty(),
        "errors outside the documented set: {}",
        rep.bugs.join("; ")
    );
    let counts = crate::verify::check_logs(&refs)?;
    anyhow::ensure!(
        counts.grants > 0 && counts.tagged > 0 && counts.landed > 0,
        "the log checkers read nothing they must have seen: {counts:?}"
    );
    Ok(())
}

fn counter_cluster(name: &str) -> Result<(crate::s3env::S3Env, tempfile::TempDir, Vec<Client>)> {
    let ttl = LOCK_TTL_MS.to_string();
    let log = format!("info,{}", crate::verify::LOG_DIRECTIVES);
    let stall = std::env::temp_dir().join(format!("harness-{name}-stall-{}", std::process::id()));
    let (env, root, clients, _) = cluster(
        name,
        &["a", "b", "c"],
        &[
            ("CONSTELLATION_LOCK_TTL_MS", &ttl),
            ("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "0"),
            ("RUST_LOG", &log),
            (
                "CONSTELLATION_FAULT_STORE_STALL_FILE",
                &stall.display().to_string(),
            ),
        ],
        0,
    )?;
    std::fs::write(clients[0].mnt.join("counter"), 0u64.to_be_bytes())?;
    for c in &clients[1..] {
        eventually("the counter everywhere", Duration::from_secs(60), || {
            anyhow::ensure!(read_counter(&c.mnt.join("counter"))? == 0);
            Ok(())
        })?;
    }
    Ok((env, root, clients))
}

fn stall_file(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("harness-{name}-stall-{}", std::process::id()))
}

/// See the module docs.
pub(super) fn lock_contention_paused_holder(_seed: u64) -> Result<()> {
    const NAME: &str = "lock-contention-paused-holder";
    let (_env, _root, mut clients) = counter_cluster(NAME)?;
    let result = (|| -> Result<()> {
        let t = Timings::from_pairs(&[
            ("CONSTELLATION_LOCK_TTL_MS", LOCK_TTL_MS.to_string()),
            ("CONSTELLATION_LEASE_TTL_MS", super::m9::TTL_MS.to_string()),
        ]);
        let lapse_window = t.boundary("lock-lapse").expect("a boundary");
        let durations: Vec<(String, Duration)> = [
            "lock-half",
            "lock-lapse-500",
            "lock-lapse+500",
            "lock-outwait+500",
        ]
        .iter()
        .map(|s| (s.to_string(), t.duration_of(s).expect("a boundary")))
        .collect();
        let errnos = ErrnoLog::new();
        let stop = Arc::new(AtomicBool::new(false));
        // The contender: c, on and on.
        let contender = {
            let (mnt, e, stop) = (clients[2].mnt.clone(), errnos.clone(), stop.clone());
            std::thread::spawn(move || counter_until(&mnt, "c", &e, &stop))
        };
        let b = &clients[1];
        let (mut acked, mut doubt, mut lapses, mut outwaits) = (0u64, 0u64, 0u64, 0u64);
        for (what, d) in &durations {
            for round in 0..rounds(3) {
                let f = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(b.mnt.join("counter"))?;
                // SAFETY: flock(2) on an fd this scope owns.
                if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX) } != 0 {
                    bail!("b's flock: {}", std::io::Error::last_os_error());
                }
                match increment(&f) {
                    Ok(()) => acked += 1,
                    Err(fail) => bail!("b's increment under a fresh grant: {fail:?}"),
                }
                let (fenced0, expired0) = (
                    crate::verify::fenced_ops(&locks(b)?),
                    n(&locks(&clients[0])?, "recalls_expired"),
                );
                b.pause()?;
                std::thread::sleep(*d);
                b.resume()?;
                let thawed = Instant::now();
                let after = increment(&f);
                // SAFETY: as above.
                unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_UN) };
                drop(f);
                let (fenced, expired) = (
                    crate::verify::fenced_ops(&locks(b)?),
                    n(&locks(&clients[0])?, "recalls_expired"),
                );
                let eio = match &after {
                    Ok(()) => {
                        acked += 1;
                        false
                    }
                    Err(fail) => {
                        let mut c = Count::default();
                        note_failed("b", fail, thawed, &errnos, &mut c);
                        doubt += c.doubt;
                        fail.eio()
                    }
                };
                // Past the window the holder honours (`ttl - margin` from
                // its last renewal, which came at most `(ttl - margin) / 2`
                // before the freeze), the grant lapsed on b's own clock: its
                // I/O on the file fails until it unlocks, whatever the owner
                // did meanwhile (engine `locks.rs`, NFSv4's rule).
                if *d >= lapse_window {
                    anyhow::ensure!(
                        eio,
                        "{what} round {round}: b's grant lapsed at least {:?} before the thaw, yet \
                         its increment after the thaw answered {after:?}: a lapsed grant honoured",
                        d.saturating_sub(lapse_window)
                    );
                }
                let outwaited = expired > expired0;
                eprintln!(
                    "    {NAME}: {what} ({d:?}) round {round}: after the thaw {after:?}; fenced \
                     +{} outwaited +{}",
                    fenced - fenced0,
                    expired - expired0
                );
                // An outwaited grant always fences its old holder; an EIO
                // is always a fence.
                anyhow::ensure!(
                    !outwaited || eio,
                    "{what} round {round}: the owner outwaited b's grant, yet b's increment \
                     after the thaw answered {after:?}: two holders"
                );
                anyhow::ensure!(
                    !eio || fenced > fenced0,
                    "{what} round {round}: b's increment answered EIO but b fenced nothing"
                );
                lapses += u64::from(eio);
                outwaits += u64::from(outwaited);
            }
        }
        stop.store(true, Ordering::Relaxed);
        let cc = contender
            .join()
            .map_err(|_| anyhow::anyhow!("the contender panicked"))?;
        eprintln!(
            "    {NAME}: {} rounds: {lapses} fenced after the thaw, {outwaits} outwaited; c \
             incremented {} (+{} in doubt)",
            durations.len() * rounds(3),
            cc.done,
            cc.doubt
        );
        finish_counter(NAME, &clients, acked + cc.done, doubt + cc.doubt, &errnos)
    })();
    super::m11::dump_logs_on_failure(NAME, &clients, &result);
    for c in clients.iter_mut().rev() {
        let _ = c.resume();
        let _ = c.unmount();
    }
    result
}

/// See the module docs.
pub(super) fn lock_contention_stalled_owner(_seed: u64) -> Result<()> {
    const NAME: &str = "lock-contention-stalled-owner";
    let (_env, _root, mut clients) = counter_cluster(NAME)?;
    let file = stall_file(NAME);
    let result = (|| -> Result<()> {
        let errnos = ErrnoLog::new();
        let stop = Arc::new(AtomicBool::new(false));
        let workers: Vec<_> = clients
            .iter()
            .map(|c| {
                let (mnt, node, e, stop) =
                    (c.mnt.clone(), c.name.clone(), errnos.clone(), stop.clone());
                std::thread::spawn(move || counter_until(&mnt, &node, &e, &stop))
            })
            .collect();
        let a_pid = clients[0].pid().context("a is not mounted")?;
        std::thread::sleep(Duration::from_secs(2));
        for ms in stalls_ms() {
            arm_stall(&file, ms, a_pid)?;
            let armed = Instant::now();
            // Taken by a's next ordinary write (its own increments' FUSE
            // writes, a background pass), held `ms`.
            while file.exists() && armed.elapsed() < Duration::from_secs(30) {
                std::thread::sleep(Duration::from_millis(50));
            }
            anyhow::ensure!(!file.exists(), "no write on a took the {ms} ms stall");
            eprintln!("    {NAME}: a's store stalled {ms} ms");
            std::thread::sleep(Duration::from_millis(ms + 3_000));
        }
        stop.store(true, Ordering::Relaxed);
        let mut total = Count::default();
        for (c, w) in clients.iter().zip(workers) {
            let r = w.join().map_err(|_| anyhow::anyhow!("a worker panicked"))?;
            eprintln!(
                "    {NAME}: {} incremented {} (+{} in doubt)",
                c.name, r.done, r.doubt
            );
            total.done += r.done;
            total.doubt += r.doubt;
        }
        finish_counter(NAME, &clients, total.done, total.doubt, &errnos)
    })();
    let _ = std::fs::remove_file(&file);
    super::m11::dump_logs_on_failure(NAME, &clients, &result);
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}

/// A ledgered writer in `dir` (relative to `mnt`) until `stop`: the
/// slowest write and when it began.
fn ledger_writer(
    mnt: &Path,
    dir: &str,
    node: &str,
    ledger: &Ledger,
    errnos: &ErrnoLog,
    stop: &AtomicBool,
) -> (Duration, Instant, u64) {
    let _ = std::fs::create_dir_all(mnt.join(dir));
    let (mut slowest, mut at, mut seq) = (Duration::ZERO, Instant::now(), 0u64);
    while !stop.load(Ordering::Relaxed) {
        seq += 1;
        let rel = format!("{dir}/f{}", seq % 16);
        let id = FileId {
            path: rel.clone(),
            writer: node.to_string(),
            seq,
        };
        let t = Instant::now();
        if let Err((step, e)) = ledger.write(mnt, &rel, id, 4096 + (seq as usize % 7) * 1000) {
            errnos.record(node, step, &e, t, OpCtx::default());
        }
        if t.elapsed() > slowest {
            (slowest, at) = (t.elapsed(), t);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    (slowest, at, seq)
}

/// After quiescence (no op answered in doubt can still land), wait until
/// `ledger` checks out on every client's mount (or 90 s), and fail with
/// what it found otherwise.
fn ledger_converges(name: &str, clients: &[Client], ledger: &Ledger) -> Result<()> {
    let refs: Vec<&Client> = clients.iter().collect();
    crate::verify::wait_quiescent(&refs, crate::load::scaled(Duration::from_secs(120)))?;
    let deadline = Instant::now() + crate::load::scaled(Duration::from_secs(90));
    loop {
        let refused: u64 = clients
            .iter()
            .map(|c| {
                crate::verify::counters(c)["replay_conflicts"]
                    .as_u64()
                    .unwrap_or(0)
            })
            .sum();
        let bad: Vec<String> = clients
            .iter()
            .flat_map(|c| {
                ledger
                    .check(&c.mnt)
                    .violations(refused)
                    .into_iter()
                    .map(move |v| format!("{}: {v}", c.name))
            })
            .collect();
        if bad.is_empty() {
            for c in clients {
                let r = ledger.check(&c.mnt);
                eprintln!(
                    "    {name}: ledger on {}: {} ops, {} intact, {} in doubt, {} in conflict \
                     copies; counters {}",
                    c.name,
                    r.acked_ops,
                    r.intact,
                    r.in_doubt,
                    r.in_conflict_copy.len(),
                    crate::verify::counters(c)
                );
            }
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("acknowledged writes: {}", bad.join("; "));
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// See the module docs.
pub(super) fn acks3_under_load(seed: u64) -> Result<()> {
    const NAME: &str = "acks3-under-load";
    let file = stall_file(NAME);
    let (env, _root, mut clients, _) = cluster(
        NAME,
        &["a", "b", "c"],
        &[
            ("CONSTELLATION_ACK", "s3"),
            (
                "CONSTELLATION_FAULT_STORE_STALL_FILE",
                &file.display().to_string(),
            ),
        ],
        0,
    )?;
    let result = (|| -> Result<()> {
        let epoch = lease_of(&clients[0])?["epoch"].as_u64().unwrap_or(0);
        let ack = clients[0].control_status()?["ack"].clone();
        anyhow::ensure!(ack["policy"] == "s3", "a is not under ack=s3: {ack}");
        let (ledger, errnos) = (
            Ledger::with_client_deadline(super::m9::client_deadline()),
            ErrnoLog::new(),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (mnt, l, e, stop) = (
                clients[0].mnt.clone(),
                ledger.clone(),
                errnos.clone(),
                stop.clone(),
            );
            std::thread::spawn(move || ledger_writer(&mnt, "w-a", "a", &l, &e, &stop))
        };
        let a_pid = clients[0].pid().context("a is not mounted")?;
        let secs = std::env::var("CONSTELLATION_HARNESS_LOAD_PROBE_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60u64);
        let until = Instant::now() + Duration::from_secs(secs);
        let mut k = seed;
        let mut stalls = 0;
        while Instant::now() < until {
            k = k
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let ms = 1_000 + (k >> 33) % 2_001;
            arm_stall(&file, ms, a_pid)?;
            stalls += 1;
            std::thread::sleep(Duration::from_millis(ms + 2_000));
        }
        stop.store(true, Ordering::Relaxed);
        let (slowest, at, ops) = writer
            .join()
            .map_err(|_| anyhow::anyhow!("the writer panicked"))?;
        eprintln!(
            "    {NAME}: {ops} writes on a, {stalls} store stalls of 1-3 s, slowest {slowest:?}"
        );
        ledger_converges(NAME, &clients, &ledger)?;
        let refs: Vec<&Client> = clients.iter().collect();
        let rep = errnos.judge(&Evidence::from_clients(&refs, super::m9::client_deadline()));
        anyhow::ensure!(
            rep.bugs.is_empty(),
            "errors outside the documented set: {}",
            rep.bugs.join("; ")
        );
        // A live holder is never taken over fast, and nothing it
        // acknowledged rolls back.
        let mut takeovers = 0;
        for c in &clients[1..] {
            takeovers += n(&c.control_status()?["ack"], "s3_fast_takeovers");
        }
        let a = crate::verify::counters(&clients[0]);
        let epoch_now = lease_of(&clients[0])?["epoch"].as_u64().unwrap_or(0);
        anyhow::ensure!(
            takeovers == 0 && epoch_now == epoch,
            "a live ack=s3 holder stalling 1-3 s was taken over ({takeovers} fast takeovers, epoch \
             {epoch} -> {epoch_now}): F2b"
        );
        anyhow::ensure!(
            a["rolled_back"] == 0 && a["local_rolled_back"] == 0 && a["depositions"] == 0,
            "acknowledged writes rolled back on the holder: {a}"
        );
        let prefix = clients[0]
            .backend
            .strip_prefix(&format!("s3://{}/", crate::s3env::BUCKET))
            .context("backend outside the harness bucket")?
            .to_string();
        crate::verify::s3_log_check(&env.direct_endpoint, &prefix)?;
        crate::hostlag::latency_bound(
            "a write on a stalling ack=s3 holder",
            at,
            slowest,
            Duration::from_millis(3_000),
            Duration::from_secs(15),
        )
    })();
    let _ = std::fs::remove_file(&file);
    super::m11::dump_logs_on_failure(NAME, &clients, &result);
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}

/// See the module docs.
pub(super) fn endpoint_wedge_probe(_seed: u64) -> Result<()> {
    const NAME: &str = "endpoint-wedge-probe";
    const FREEZE: Duration = Duration::from_secs(8);
    // The stall watch counts a request stalled past freeze + 10 s, so
    // "no FUSE request stalled" sees what a freeze could wedge (its
    // default, 30 s or half the hang bound, would not).
    let stall_s = (FREEZE + Duration::from_secs(10)).as_secs().to_string();
    let (env, _root, mut clients, _) = cluster(
        NAME,
        &["a", "b", "c"],
        &[("CONSTELLATION_FUSE_REQUEST_STALL_S", &stall_s)],
        0,
    )?;
    let result = (|| -> Result<()> {
        {
            let refs: Vec<&Client> = clients.iter().collect();
            super::m11::wait_for_connected_peers(&refs)?;
        }
        let stalled0: Vec<u64> = clients
            .iter()
            .map(|c| {
                c.control_status()
                    .map(|s| n(&s["fuse_requests"], "stalled_total"))
                    .unwrap_or(0)
            })
            .collect();
        let (ledger, errnos) = (
            Ledger::with_client_deadline(super::m9::client_deadline()),
            ErrnoLog::new(),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let writers: Vec<_> = clients
            .iter()
            .map(|c| {
                let (mnt, node, l, e, stop) = (
                    c.mnt.clone(),
                    c.name.clone(),
                    ledger.clone(),
                    errnos.clone(),
                    stop.clone(),
                );
                std::thread::spawn(move || {
                    ledger_writer(&mnt, &format!("w-{node}"), &node, &l, &e, &stop)
                })
            })
            .collect();
        let mut recovered = Vec::new();
        for round in 0..rounds(10) {
            let k = round % clients.len();
            clients[k].pause()?;
            std::thread::sleep(FREEZE);
            clients[k].resume()?;
            let thawed = Instant::now();
            let refs: Vec<&Client> = clients.iter().collect();
            super::m11::wait_for_connected_peers(&refs).with_context(|| {
                format!(
                    "round {round}: links after {}'s {FREEZE:?} freeze",
                    clients[k].name
                )
            })?;
            recovered.push((thawed.elapsed(), thawed));
            eprintln!(
                "    {NAME}: round {round}: {} frozen {FREEZE:?}; every link back {:?} after the thaw",
                clients[k].name,
                thawed.elapsed()
            );
            std::thread::sleep(Duration::from_secs(2));
        }
        stop.store(true, Ordering::Relaxed);
        let mut slowest = (Duration::ZERO, Instant::now());
        for w in writers {
            let (s, at, _) = w.join().map_err(|_| anyhow::anyhow!("a writer panicked"))?;
            if s > slowest.0 {
                slowest = (s, at);
            }
        }
        for (c, s0) in clients.iter().zip(&stalled0) {
            let s = c.control_status()?;
            anyhow::ensure!(
                n(&s["fuse_requests"], "stalled_total") == *s0,
                "{}: FUSE requests stalled across the freezes: {}",
                c.name,
                s["fuse_requests"]
            );
        }
        ledger_converges(NAME, &clients, &ledger)?;
        let refs: Vec<&Client> = clients.iter().collect();
        let rep = errnos.judge(&Evidence::from_clients(&refs, super::m9::client_deadline()));
        anyhow::ensure!(
            rep.bugs.is_empty(),
            "errors outside the documented set: {}",
            rep.bugs.join("; ")
        );
        let prefix = clients[0]
            .backend
            .strip_prefix(&format!("s3://{}/", crate::s3env::BUCKET))
            .context("backend outside the harness bucket")?
            .to_string();
        crate::verify::s3_log_check(&env.direct_endpoint, &prefix)?;
        let (worst, worst_at) = recovered
            .iter()
            .max_by_key(|(took, _)| *took)
            .copied()
            .unwrap_or((Duration::ZERO, Instant::now()));
        crate::hostlag::latency_bound(
            "the links' recovery after a freeze",
            worst_at,
            worst,
            Duration::ZERO,
            Duration::from_secs(10),
        )?;
        crate::hostlag::latency_bound(
            "a write across a node's freeze",
            slowest.1,
            slowest.0,
            FREEZE,
            FREEZE + Duration::from_secs(20),
        )
    })();
    super::m11::dump_logs_on_failure(NAME, &clients, &result);
    for c in clients.iter_mut().rev() {
        let _ = c.resume();
        let _ = c.unmount();
    }
    result
}

/// For `within`, every generation `b` holds live (not stopped, unexpired)
/// must not be one the root `a` ended (`ended` accumulates what the root
/// reported ended, across calls): a recalled or reclaimed generation's
/// late answer never installs it again (cluster-locks.md, delegations).
/// `until_ms` is on `b`'s protocol clock, which may run up to `skew` off
/// the harness's, and further by the wall-clock step the load applied to
/// `b` (`clock:step`): see [`StaleWatch`].
fn no_stale_generation(
    a: &Client,
    b: &Client,
    ended: &mut std::collections::BTreeSet<u64>,
    skew: Duration,
    within: Duration,
) -> Result<()> {
    let until = Instant::now() + within;
    let mut watch = StaleWatch::new(skew);
    loop {
        let root = a.control_status()?["delegation"].clone();
        for g in root["gens"].as_array().into_iter().flatten() {
            if g[6] == true {
                ended.insert(g[2].as_u64().unwrap_or(0));
            }
        }
        let mine = b.control_status()?["delegation"]["mine"].clone();
        // The step the load applied to `b`'s wall clock moves `until_ms`
        // with it (plan 43 C7-H1).
        watch.step_ms = crate::load::clock_step_now(&b.name);
        if let Some((gen, until_ms, now)) = watch.sample(&mine, ended, super::snapsched::now_ms()) {
            bail!(
                "b holds generation {gen} live (until {until_ms}, harness clock {now}) that the \
                 root ended, in two samples in a row: a late answer installed a stale grant"
            );
        }
        if Instant::now() >= until {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// [`no_stale_generation`]'s rule over `delegation.mine` samples. A
/// generation is suspect when the root ended it and `b` holds it neither
/// stopped nor lapsed even if `b`'s clock runs `skew` ahead of the
/// harness's (`until_ms > now + skew + step`); it is a stale grant once suspect
/// in two samples in a row, so neither a clock-skew transient nor one
/// sample racing the stop is called one.
struct StaleWatch {
    skew_ms: i64,
    /// The wall-clock step applied to the node being watched (ms): its
    /// `until_ms` values run that far ahead (behind, if negative) of the
    /// harness's clock on top of `skew_ms`.
    step_ms: i64,
    suspect: std::collections::BTreeSet<u64>,
}

impl StaleWatch {
    fn new(skew: Duration) -> StaleWatch {
        StaleWatch {
            skew_ms: skew.as_millis() as i64,
            step_ms: 0,
            suspect: Default::default(),
        }
    }

    /// One sample of `mine` (`[dir, gen, until_ms, stopped, …]` rows) at
    /// harness time `now` (ms): the stale grant `(gen, until_ms, now)`, if
    /// one was suspect in the previous sample too.
    fn sample(
        &mut self,
        mine: &serde_json::Value,
        ended: &std::collections::BTreeSet<u64>,
        now: i64,
    ) -> Option<(u64, i64, i64)> {
        let mut suspect = std::collections::BTreeSet::new();
        let mut stale = None;
        for m in mine.as_array().into_iter().flatten() {
            let (gen, until_ms, stopped) = (
                m[1].as_u64().unwrap_or(0),
                m[2].as_i64().unwrap_or(0),
                m[3] == true,
            );
            if !stopped && until_ms > now + self.skew_ms + self.step_ms && ended.contains(&gen) {
                suspect.insert(gen);
                if stale.is_none() && self.suspect.contains(&gen) {
                    stale = Some((gen, until_ms, now));
                }
            }
        }
        self.suspect = suspect;
        stale
    }
}

/// See the module docs.
pub(super) fn delegate_paused_across_ttl(_seed: u64) -> Result<()> {
    const NAME: &str = "delegate-paused-across-ttl";
    const DELEG_TTL_MS: u64 = 5_000;
    let ttl = DELEG_TTL_MS.to_string();
    let (env, _root, mut clients, _) = super::m11::cluster(
        NAME,
        &["a", "b"],
        &[("CONSTELLATION_DELEGATION_TTL_MS", &ttl)],
        0,
    )?;
    let result = (|| -> Result<()> {
        let (a, b) = (&clients[0], &clients[1]);
        let b_id = node_id(b)?;
        std::fs::create_dir(a.mnt.join("d1"))?;
        std::fs::create_dir(a.mnt.join("d2"))?;
        eventually("d1 on b", Duration::from_secs(20), || {
            anyhow::ensure!(b.mnt.join("d1").is_dir());
            Ok(())
        })?;
        super::m11::delegate(a, "/d1", b_id)?;
        super::m11::wait_installed(b, "/d1", Duration::from_secs(20))?;
        let t = Timings::from_pairs(&[
            ("CONSTELLATION_DELEGATION_TTL_MS", ttl.clone()),
            ("CONSTELLATION_LEASE_TTL_MS", super::m11::TTL_MS.to_string()),
        ]);
        let durations: Vec<(String, Duration)> =
            ["deleg-lapse-500", "deleg-lapse+500", "deleg-outwait+1000"]
                .iter()
                .map(|s| (s.to_string(), t.duration_of(s).expect("a boundary")))
                .collect();
        let (ledger, errnos) = (
            Ledger::with_client_deadline(t.client_deadline()),
            ErrnoLog::new(),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (mnt, l, e, stop) = (b.mnt.clone(), ledger.clone(), errnos.clone(), stop.clone());
            std::thread::spawn(move || ledger_writer(&mnt, "d1/w-b", "b", &l, &e, &stop))
        };
        let mut moved = Vec::new();
        let mut ended = std::collections::BTreeSet::new();
        for (i, (what, d)) in durations.iter().enumerate() {
            for round in 0..rounds(3) {
                let x = format!("x-{i}-{round}");
                std::fs::write(a.mnt.join("d1").join(&x), x.as_bytes())?;
                eventually(&format!("{x} on b"), Duration::from_secs(30), || {
                    anyhow::ensure!(b.mnt.join("d1").join(&x).is_file());
                    Ok(())
                })?;
                b.pause()?;
                // The root moves a file out of the frozen delegate's
                // subtree: it needs the delegate's answer, or its
                // generation's end.
                let mover = {
                    let (from, to) = (a.mnt.join("d1").join(&x), a.mnt.join("d2").join(&x));
                    std::thread::spawn(move || {
                        let t = Instant::now();
                        (std::fs::rename(from, to), t)
                    })
                };
                std::thread::sleep(*d);
                b.resume()?;
                let (r, t0) = mover
                    .join()
                    .map_err(|_| anyhow::anyhow!("the mover panicked"))?;
                // A recalled generation's late answer installs nothing: no
                // generation the root ended is live at b afterwards.
                no_stale_generation(a, b, &mut ended, t.margin, Duration::from_secs(5))
                    .with_context(|| format!("{what} round {round}"))?;
                eprintln!(
                    "    {NAME}: {what} ({d:?}) round {round}: the root's rename of {x} {r:?} \
                     after {:?}",
                    t0.elapsed()
                );
                match r {
                    Ok(()) => moved.push(x),
                    Err(e) => errnos.record("a", "cross-subtree rename", &e, t0, OpCtx::default()),
                }
            }
        }
        stop.store(true, Ordering::Relaxed);
        let (slowest, at, ops) = writer
            .join()
            .map_err(|_| anyhow::anyhow!("the writer panicked"))?;
        eprintln!(
            "    {NAME}: b wrote {ops} payloads, slowest {slowest:?}; {} files moved",
            moved.len()
        );
        ledger_converges(NAME, &clients, &ledger)?;
        // Every moved file in one place, on both nodes.
        for c in &clients {
            for x in &moved {
                eventually(
                    &format!("{x} moved on {}", c.name),
                    Duration::from_secs(60),
                    || {
                        anyhow::ensure!(
                            c.mnt.join("d2").join(x).is_file()
                                && !c.mnt.join("d1").join(x).exists(),
                            "{x} is not (only) in d2"
                        );
                        Ok(())
                    },
                )?;
            }
        }
        // Errors only in doubt past the deadline (the parked ops' fate
        // under a lost generation), and none else.
        let refs: Vec<&Client> = clients.iter().collect();
        let mut ev = Evidence::from_clients(&refs, t.client_deadline());
        ev.allow_in_doubt = true;
        let rep = errnos.judge(&ev);
        eprintln!("    {NAME}: errors by class {:?}", rep.counts);
        anyhow::ensure!(
            rep.bugs.is_empty(),
            "errors outside the documented set: {}",
            rep.bugs.join("; ")
        );
        for c in &clients {
            eprintln!(
                "    {NAME}: {}'s delegation {}",
                c.name,
                c.control_status()?["delegation"]
            );
        }
        let prefix = clients[0]
            .backend
            .strip_prefix(&format!("s3://{}/", crate::s3env::BUCKET))
            .context("backend outside the harness bucket")?
            .to_string();
        // No rid executed twice, one node per epoch.
        crate::verify::s3_log_check(&env.direct_endpoint, &prefix)?;
        crate::verify::check_logs(&refs)?;
        crate::hostlag::latency_bound(
            "a delegate's write across its freeze",
            at,
            slowest,
            t.reclaim_horizon(),
            t.reclaim_horizon() + Duration::from_secs(15),
        )
    })();
    super::m11::dump_logs_on_failure(NAME, &clients, &result);
    for c in clients.iter_mut().rev() {
        let _ = c.resume();
        let _ = c.unmount();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_boundaries_the_scenarios_pause_for_exist() {
        let t = Timings::from_pairs(&[
            ("CONSTELLATION_LOCK_TTL_MS", "3000"),
            ("CONSTELLATION_DELEGATION_TTL_MS", "5000"),
            ("CONSTELLATION_LEASE_TTL_MS", "20000"),
        ]);
        for b in [
            "lock-half",
            "lock-lapse-500",
            "lock-lapse+500",
            "lock-outwait+500",
            "deleg-lapse-500",
            "deleg-lapse+500",
            "deleg-outwait+1000",
        ] {
            assert!(t.duration_of(b).is_some(), "{b}");
        }
        assert_eq!(
            t.duration_of("lock-lapse+500"),
            Some(Duration::from_millis(2_500))
        );
        assert_eq!(
            t.duration_of("deleg-outwait+1000"),
            Some(Duration::from_millis(7_000))
        );
    }

    /// Expectation (plan 43 C7-H1): a node whose wall clock the load stepped
    /// forward by 3 s reports `until_ms` 3 s ahead of the harness's clock; a
    /// generation held live on its own clock is not suspect for that, but
    /// one held 3 s past it is. A backward step lowers the threshold the
    /// same amount.
    #[test]
    fn a_stepped_node_is_judged_on_its_own_clock() {
        let ended: std::collections::BTreeSet<u64> = [3].into();
        let row = |until: i64| serde_json::json!([[7, 3, until, false, 0, 0, 0]]);
        let now = 1_000_000;
        let mut w = StaleWatch::new(Duration::from_millis(1_000));
        w.step_ms = 3_000;
        for at in [now, now + 500] {
            assert_eq!(w.sample(&row(now + 3_900), &ended, at), None);
        }
        assert_eq!(w.sample(&row(now + 8_000), &ended, now), None);
        assert!(w.sample(&row(now + 8_000), &ended, now + 500).is_some());
        let mut w = StaleWatch::new(Duration::from_millis(1_000));
        w.step_ms = -3_000;
        // Its clock is 3 s behind: the threshold is now - 2 s.
        assert_eq!(w.sample(&row(now - 2_500), &ended, now), None);
        assert_eq!(w.sample(&row(now - 2_500), &ended, now + 500), None);
        assert_eq!(w.sample(&row(now - 900), &ended, now), None);
        assert!(w.sample(&row(now - 900), &ended, now + 500).is_some());
    }

    #[test]
    fn a_stale_generation_is_one_held_past_the_skew_in_two_samples() {
        // The scenario's margin covers the ±500 ms clock injectors.
        let t = Timings::from_pairs(&[
            ("CONSTELLATION_DELEGATION_TTL_MS", "5000".to_string()),
            (
                "CONSTELLATION_LEASE_TTL_MS",
                super::super::m11::TTL_MS.to_string(),
            ),
        ]);
        assert!(t.margin >= Duration::from_millis(500), "{t:?}");
        let ended: std::collections::BTreeSet<u64> = [3].into();
        let row = |gen: u64, until: i64, stopped: bool| {
            serde_json::json!([7, gen, until, stopped, 0, 0, 0])
        };
        let mine = |rows: Vec<serde_json::Value>| serde_json::Value::Array(rows);
        let now = 1_000_000;
        let mut w = StaleWatch::new(Duration::from_millis(1_000));
        // Within the skew of the harness's clock: b may have lapsed it.
        assert_eq!(
            w.sample(&mine(vec![row(3, now + 400, false)]), &ended, now),
            None
        );
        assert_eq!(
            w.sample(&mine(vec![row(3, now + 900, false)]), &ended, now + 500),
            None
        );
        // Live past the skew, but once: not yet a stale grant.
        let mut w = StaleWatch::new(Duration::from_millis(1_000));
        assert_eq!(
            w.sample(&mine(vec![row(3, now + 5_000, false)]), &ended, now),
            None
        );
        // Twice in a row: one.
        assert_eq!(
            w.sample(&mine(vec![row(3, now + 5_000, false)]), &ended, now + 500),
            Some((3, now + 5_000, now + 500))
        );
        // Suspect, then not, then suspect again: no two in a row.
        let mut w = StaleWatch::new(Duration::from_millis(1_000));
        assert_eq!(
            w.sample(&mine(vec![row(3, now + 5_000, false)]), &ended, now),
            None
        );
        assert_eq!(
            w.sample(&mine(vec![row(3, now + 5_000, true)]), &ended, now + 500),
            None
        );
        assert_eq!(
            w.sample(&mine(vec![row(3, now + 5_000, false)]), &ended, now + 1_000),
            None
        );
        // Stopped, or a generation the root did not end: never.
        let mut w = StaleWatch::new(Duration::from_millis(1_000));
        for at in [now, now + 500] {
            assert_eq!(
                w.sample(
                    &mine(vec![row(3, now + 5_000, true), row(4, now + 5_000, false)]),
                    &ended,
                    at
                ),
                None
            );
        }
    }
}
