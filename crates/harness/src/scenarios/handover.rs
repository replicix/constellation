//! Plan 31 §6.11 / C4b: FUSE session handover through a real kernel
//! mount — `constellation daemon --upgrade` replaces the daemon's image
//! while the mount stays live.
//!
//! - `session-handover-idle`: no op in flight at the upgrade. The mount
//!   never disappears (a watcher `stat`s it every few milliseconds across
//!   the handover: same `st_dev`, never an error), descriptors opened
//!   before it keep working (read and write), the contents match the
//!   model, and the new image serves (`status` reports the next
//!   generation, `/proc/<pid>/cmdline` is the resumed image's).
//! - `upgrade-under-load`: a writer appending through one descriptor held
//!   open throughout, a creator making, writing and closing new files, and
//!   a reader re-reading a file through a descriptor held open throughout,
//!   across three upgrades in a row. Zero errors of any kind (`ENOTCONN`
//!   and `EIO` included) for every syscall, every write lands (the file is
//!   compared byte for byte with what was written, before and after a
//!   remount), and each upgrade's stall is reported.
//!
//! Both pin their mount to `/dev/fuse` (plan 38 §3(e)): a handover is a
//! `/dev/fuse`-only capability, so a leg of the transport matrix lane
//! running with `CONSTELLATION_FUSE_TRANSPORT=auto` must not turn these
//! into tests of a refusal. `transport-detach-refused` (`super::transport`)
//! is the scenario for the refusal.

use super::{setup, ts};
use crate::client::Client;
use crate::model::Model;
use crate::workload::Workload;
use anyhow::{bail, ensure, Context, Result};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub(super) fn bin() -> PathBuf {
    std::env::var_os("CONSTELLATION_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let target = std::env::var_os("CARGO_TARGET_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("target"));
            target.join("release/constellation")
        })
}

/// A client whose mount is **handover-capable**, which on plan 38's
/// ladder means `/dev/fuse`: `CONSTELLATION_FUSE_TRANSPORT=dev-fuse`,
/// explicitly, so these scenarios keep testing the handover on the
/// transport handover exists on even when the whole lane runs with
/// `=auto` (plan 38 §3(e): a ring session refuses to be detached, and
/// `transport-detach-refused` is the scenario for *that*). Not a
/// weakening: a daemon whose plain mounts are on a ring cannot be
/// upgraded in place either, by design.
fn dev_fuse_client(env: &crate::s3env::S3Env, root: &Path, prefix: &str) -> Result<Client> {
    let backend = format!("s3://{}/{prefix}", crate::s3env::BUCKET);
    let mut c = Client::new(root, "c0", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_FUSE_TRANSPORT", "dev-fuse");
    c.fs_create()?;
    c.mount()?;
    Ok(c)
}

/// The daemon's handover generation, from its control socket.
pub(super) fn generation(c: &Client) -> Result<u64> {
    let status = c.control_status()?;
    status["handover"]["generation"]
        .as_u64()
        .context("status has no handover.generation")
}

/// `constellation daemon --upgrade --state-dir <state>` as a child
/// process, whatever it answers: the caller decides whether a refusal is
/// the expected outcome (`transport-detach-refused`) or a failure.
pub(super) fn try_upgrade(c: &Client) -> Result<std::process::Output> {
    std::process::Command::new(bin())
        .args(["daemon", "--upgrade", "--timeout-s", "120", "--state-dir"])
        .arg(c.state_dir())
        .output()
        .context("running daemon --upgrade")
}

/// [`try_upgrade`] that must succeed (the daemon re-execs the binary it
/// was started from). Returns the time it took.
pub(super) fn upgrade(c: &Client) -> Result<Duration> {
    let started = Instant::now();
    let out = try_upgrade(c)?;
    let took = started.elapsed();
    let stdout = String::from_utf8_lossy(&out.stdout);
    if !out.status.success() {
        bail!(
            "daemon --upgrade failed ({}): {stdout}{}\n{}",
            out.status,
            String::from_utf8_lossy(&out.stderr),
            c.tail_log()
        );
    }
    ensure!(
        stdout.contains("upgraded: pid"),
        "daemon --upgrade said: {stdout}"
    );
    Ok(took)
}

/// `stat`s the mountpoint every 5 ms until stopped: every error, and every
/// `st_dev` other than the first, is a gap the kernel showed.
pub(super) struct Watcher {
    stop: Arc<AtomicBool>,
    thread: std::thread::JoinHandle<Vec<String>>,
}

impl Watcher {
    pub(super) fn start(mnt: &Path) -> Result<Watcher> {
        let dev = std::fs::metadata(mnt)?.dev();
        let mnt = mnt.to_path_buf();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let thread = std::thread::spawn(move || {
            let mut problems = Vec::new();
            while !flag.load(Ordering::SeqCst) {
                match std::fs::metadata(&mnt) {
                    Ok(m) if m.dev() == dev => {}
                    Ok(m) => problems.push(format!("st_dev changed: {dev} -> {}", m.dev())),
                    Err(e) => problems.push(format!("stat {}: {e}", mnt.display())),
                }
                if std::fs::read_dir(&mnt)
                    .and_then(|mut d| d.next().transpose())
                    .is_err()
                {
                    problems.push(format!("readdir {} failed", mnt.display()));
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            problems
        });
        Ok(Watcher { stop, thread })
    }

    pub(super) fn finish(self) -> Vec<String> {
        self.stop.store(true, Ordering::SeqCst);
        self.thread
            .join()
            .unwrap_or_else(|_| vec!["watcher panicked".into()])
    }
}

pub(super) fn daemon_cmdline(pid: u32) -> String {
    std::fs::read(format!("/proc/{pid}/cmdline"))
        .map(|b| String::from_utf8_lossy(&b).replace('\0', " "))
        .unwrap_or_default()
}

pub fn session_handover_idle(seed: u64) -> Result<()> {
    let (env, root) = setup("handover-idle")?;
    let _proxy = env.s3_proxy()?;
    let mut c = dev_fuse_client(&env, root.path(), &format!("handover-idle-{}", ts()))?;
    let result = (|| -> Result<()> {
        let mut model = Model::default();
        let mut wl = Workload::new(seed, "w");
        wl.run_block(&c.mnt, &mut model, 80)?;
        model.verify(&c.mnt).context("before the upgrade")?;
        let pid = c.pid().context("no daemon pid")?;
        let before = generation(&c)?;

        // Descriptors opened before the handover: one to read, one to
        // write, one directory.
        std::fs::write(c.mnt.join("held-read"), b"read me across the handover")?;
        let held_read = std::fs::File::open(c.mnt.join("held-read"))?;
        let mut held_write = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(c.mnt.join("held-write"))?;
        held_write.write_all(b"before;")?;
        let held_dir = std::fs::File::open(&c.mnt)?;

        let watcher = Watcher::start(&c.mnt)?;
        let took = upgrade(&c)?;
        std::thread::sleep(Duration::from_millis(200));
        let gaps = watcher.finish();
        ensure!(gaps.is_empty(), "the mount showed gaps: {gaps:?}");
        eprintln!("session-handover-idle: upgrade took {took:?}");

        ensure!(
            generation(&c)? == before + 1,
            "the daemon's generation did not advance"
        );
        ensure!(
            c.pid() == Some(pid) && daemon_cmdline(pid).contains("--resume-from"),
            "pid {pid} is not the resumed image: {}",
            daemon_cmdline(pid)
        );
        let mut buf = vec![0u8; 64];
        let n = held_read.read_at(&mut buf, 0)?;
        ensure!(
            &buf[..n] == b"read me across the handover",
            "read through a held descriptor: {:?}",
            String::from_utf8_lossy(&buf[..n])
        );
        held_write.write_all(b"after")?;
        held_write.sync_all()?;
        drop(held_write);
        let text = std::fs::read(c.mnt.join("held-write"))?;
        ensure!(
            text == b"before;after",
            "write through a held descriptor: {:?}",
            String::from_utf8_lossy(&text)
        );
        held_dir.metadata()?;
        drop(held_dir);
        drop(held_read);
        model.write_file(
            Path::new("held-read"),
            b"read me across the handover".to_vec(),
        );
        model.write_file(Path::new("held-write"), b"before;after".to_vec());

        model.verify(&c.mnt).context("after the upgrade")?;
        wl.run_block(&c.mnt, &mut model, 40)?;
        model.verify(&c.mnt).context("new work on the new image")?;
        c.unmount()?;
        c.mount()?;
        model.verify(&c.mnt).context("after a remount")?;
        Ok(())
    })();
    let _ = c.unmount();
    result
}

/// A seeded, verifiable byte pattern for record `i`.
fn record(seed: u64, i: u64, len: usize) -> Vec<u8> {
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

pub(super) struct Load {
    pub(super) stop: Arc<AtomicBool>,
    pub(super) errors: Arc<Mutex<Vec<String>>>,
    /// The longest a single syscall of the load took.
    pub(super) max_stall_us: Arc<AtomicU64>,
    pub(super) ops: Arc<AtomicU64>,
    pub(super) threads: Vec<std::thread::JoinHandle<Result<u64>>>,
}

impl Load {
    fn note(errors: &Mutex<Vec<String>>, what: String) {
        let mut e = errors.lock().unwrap();
        if e.len() < 50 {
            e.push(what);
        }
    }
}

const RECORD: usize = 4096;

/// Start the writer (appends records through one descriptor held open,
/// `fsync`ing every 16), the creator (new files, written and closed) and
/// the reader (re-reads a file through a held descriptor).
pub(super) fn start_load(mnt: &Path, seed: u64) -> Result<Load> {
    let stop = Arc::new(AtomicBool::new(false));
    let errors = Arc::new(Mutex::new(Vec::new()));
    let max_stall_us = Arc::new(AtomicU64::new(0));
    let ops = Arc::new(AtomicU64::new(0));
    std::fs::create_dir_all(mnt.join("created"))?;
    let fixed: Vec<u8> = record(seed, u64::MAX, 256 * 1024);
    std::fs::write(mnt.join("fixed"), &fixed)?;
    let timed = {
        let max = max_stall_us.clone();
        let ops = ops.clone();
        move |started: Instant| {
            max.fetch_max(started.elapsed().as_micros() as u64, Ordering::SeqCst);
            ops.fetch_add(1, Ordering::Relaxed);
        }
    };
    let mut threads = Vec::new();
    {
        let (stop, errors, timed) = (stop.clone(), errors.clone(), timed.clone());
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(mnt.join("appended"))?;
        threads.push(std::thread::spawn(move || -> Result<u64> {
            let mut i = 0u64;
            while !stop.load(Ordering::SeqCst) {
                let data = record(seed, i, RECORD);
                let t = Instant::now();
                match file.write_at(&data, i * RECORD as u64) {
                    Ok(n) if n == RECORD => {}
                    Ok(n) => Load::note(&errors, format!("appended: short write {n} at {i}")),
                    Err(e) => {
                        Load::note(&errors, format!("appended: write {i}: {e}"));
                        continue;
                    }
                }
                timed(t);
                i += 1;
                if i.is_multiple_of(16) {
                    let t = Instant::now();
                    if let Err(e) = file.sync_all() {
                        Load::note(&errors, format!("appended: fsync at {i}: {e}"));
                    }
                    timed(t);
                }
            }
            let t = Instant::now();
            if let Err(e) = file.sync_all() {
                Load::note(&errors, format!("appended: final fsync: {e}"));
            }
            timed(t);
            Ok(i)
        }));
    }
    {
        let (stop, errors, timed) = (stop.clone(), errors.clone(), timed.clone());
        let dir = mnt.join("created");
        threads.push(std::thread::spawn(move || -> Result<u64> {
            let mut i = 0u64;
            while !stop.load(Ordering::SeqCst) {
                let data = record(seed ^ 0xc0ffee, i, 1000 + (i as usize % 7) * 997);
                let t = Instant::now();
                if let Err(e) = std::fs::write(dir.join(format!("f{i}")), &data) {
                    Load::note(&errors, format!("created: f{i}: {e}"));
                    continue;
                }
                timed(t);
                i += 1;
            }
            Ok(i)
        }));
    }
    {
        let (stop, errors, timed) = (stop.clone(), errors.clone(), timed.clone());
        let file = std::fs::File::open(mnt.join("fixed"))?;
        threads.push(std::thread::spawn(move || -> Result<u64> {
            let mut rounds = 0u64;
            let mut buf = vec![0u8; fixed.len()];
            while !stop.load(Ordering::SeqCst) {
                let t = Instant::now();
                match file.read_at(&mut buf, 0) {
                    Ok(n) if buf[..n] == fixed[..n] && n > 0 => {}
                    Ok(n) => Load::note(&errors, format!("fixed: read {n} bytes, content differs")),
                    Err(e) => Load::note(&errors, format!("fixed: read: {e}")),
                }
                timed(t);
                rounds += 1;
                std::thread::sleep(Duration::from_millis(2));
            }
            Ok(rounds)
        }));
    }
    Ok(Load {
        stop,
        errors,
        max_stall_us,
        ops,
        threads,
    })
}

/// Every record of `appended` and every created file, as written.
pub(super) fn verify_load(mnt: &Path, seed: u64, appended: u64, created: u64) -> Result<()> {
    let mut file = std::fs::File::open(mnt.join("appended"))?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;
    ensure!(
        data.len() as u64 == appended * RECORD as u64,
        "appended is {} bytes, {appended} records written",
        data.len()
    );
    for i in 0..appended {
        let at = (i as usize) * RECORD;
        ensure!(
            data[at..at + RECORD] == record(seed, i, RECORD)[..],
            "appended: record {i} differs"
        );
    }
    for i in 0..created {
        let want = record(seed ^ 0xc0ffee, i, 1000 + (i as usize % 7) * 997);
        let got = std::fs::read(mnt.join("created").join(format!("f{i}")))
            .with_context(|| format!("created/f{i}"))?;
        ensure!(got == want, "created/f{i} differs");
    }
    Ok(())
}

/// Plan 44 §4.4, W6: across `upgrades` handovers the daemon's log has, in
/// order, each `exec'ing the new binary` line (the old image's) followed by
/// the new image's `resuming a handed-over daemon` line, and **no loss**,
/// stalled sink or not: what the old image's writer had not written at its
/// `exec` travels to the new image in a memfd and is the first thing the new
/// image writes (so the old image's `exec'ing` line can be in the file only
/// after its new image has started). A chunk that was being written at the
/// `exec` may be written by both images: consecutive repeats of a marker
/// line are one, and a line before them says so. A line the old image could
/// not carry is counted as dropped (status, a summary line), which fails
/// this check and the run's verification.
fn check_upgrade_log(c: &Client, upgrades: usize) -> Result<()> {
    // Under a `log-stall` item the new image writes what the old one carried
    // over one 256 KiB chunk per stalled `write()`: wait for all of it.
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let report = c
            .control("node.logs.flush", serde_json::json!({"within_ms": 10_000}))
            .context("flushing the daemon's log")?;
        if report["complete"].as_bool() == Some(true) {
            break;
        }
        ensure!(
            Instant::now() < deadline,
            "the daemon's log was still not written after 120 s: {report}"
        );
        // (A flush that returns at once, a third waiter, must not be called
        // back to back.)
        std::thread::sleep(Duration::from_millis(100));
    }
    let text = c.log_text();
    let mut sequence: Vec<&str> = Vec::new();
    let (mut carried, mut handovers_with_a_tail) = (0u64, 0usize);
    let (mut repeats, mut summaries, mut warnings) = (0usize, 0usize, 0usize);
    for line in text.lines() {
        let line = crate::verify::strip_ansi(line);
        let marker = if line.contains("exec'ing the new binary") {
            Some("exec")
        } else if line.contains("resuming a handed-over daemon") {
            Some("resume")
        } else {
            None
        };
        if let Some(marker) = marker {
            if sequence.last() == Some(&marker) {
                repeats += 1;
            } else {
                sequence.push(marker);
            }
        } else if line.contains("line(s) above were unwritten at the previous image's exec") {
            handovers_with_a_tail += 1;
            carried += line
                .split("the ")
                .nth(1)
                .and_then(|r| r.split(' ').next())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0);
        } else if line.contains("could not hand its log over")
            || line.contains("could not be read; its")
        {
            warnings += 1;
        } else if line.contains("log lines dropped") {
            summaries += 1;
        }
    }
    let expected: Vec<&str> = (0..upgrades).flat_map(|_| ["exec", "resume"]).collect();
    ensure!(
        sequence == expected && warnings == 0 && summaries == 0,
        "the log's marker lines are {sequence:?} ({warnings} loss warning(s), {summaries} summary \
         line(s)), expected each `exec'ing the new binary` line followed by its `resuming` line \
         and no loss; log:\n{}",
        c.tail_log_n(60)
    );
    let status = c.control_status()?;
    let log = &status["log"];
    ensure!(
        log["dropped_lines"].as_u64() == Some(0)
            && log["lost_at_previous_exec"].as_u64() == Some(0),
        "the final image's status says dropped {:?}, lost at the previous exec {:?}: {log}",
        log["dropped_lines"],
        log["lost_at_previous_exec"]
    );
    if handovers_with_a_tail > 0 {
        eprintln!(
            "    verify: upgrade-under-load: {carried} line(s) carried over {handovers_with_a_tail} \
             exec(s) in a memfd, written first by the new image ({repeats} repeated marker \
             line(s) from a write in flight)"
        );
    }
    Ok(())
}

pub fn upgrade_under_load(seed: u64) -> Result<()> {
    let (env, root) = setup("handover-load")?;
    let _proxy = env.s3_proxy()?;
    let mut c = dev_fuse_client(&env, root.path(), &format!("handover-load-{}", ts()))?;
    let result = (|| -> Result<()> {
        let pid = c.pid().context("no daemon pid")?;
        let first = generation(&c)?;
        let watcher = Watcher::start(&c.mnt)?;
        let load = start_load(&c.mnt, seed)?;
        std::thread::sleep(Duration::from_millis(1500));
        let mut took = Vec::new();
        for _ in 0..3 {
            let ops_before = load.ops.load(Ordering::SeqCst);
            took.push(upgrade(&c)?);
            std::thread::sleep(Duration::from_millis(1500));
            ensure!(
                load.ops.load(Ordering::SeqCst) > ops_before,
                "the load made no progress after an upgrade"
            );
        }
        load.stop.store(true, Ordering::SeqCst);
        let mut counts = Vec::new();
        for t in load.threads {
            counts.push(
                t.join()
                    .map_err(|_| anyhow::anyhow!("a load thread panicked"))??,
            );
        }
        let gaps = watcher.finish();
        let errors = load.errors.lock().unwrap().clone();
        eprintln!(
            "upgrade-under-load: upgrades took {took:?}; {} ops, longest syscall {} ms; \
             appended {} records, created {} files, {} reads",
            load.ops.load(Ordering::SeqCst),
            load.max_stall_us.load(Ordering::SeqCst) / 1000,
            counts[0],
            counts[1],
            counts[2]
        );
        ensure!(errors.is_empty(), "the load saw errors: {errors:?}");
        if crate::load::stalls_log() {
            // A stalled log sink must not reach the application through the
            // handover: not through the old image's flush (200 ms), nor the
            // new image's first line (its writer starts before it).
            let gap = Duration::from_micros(load.max_stall_us.load(Ordering::SeqCst));
            ensure!(
                gap < Duration::from_secs(3),
                "the longest syscall across the upgrades took {gap:?} under a stalled log sink"
            );
        }
        ensure!(gaps.is_empty(), "the mount showed gaps: {gaps:?}");
        ensure!(
            generation(&c)? == first + 3,
            "three upgrades, generation did not advance by three"
        );
        ensure!(c.pid() == Some(pid), "the daemon's pid changed");
        verify_load(&c.mnt, seed, counts[0], counts[1]).context("after the upgrades")?;
        check_upgrade_log(&c, 3)?;
        c.unmount()?;
        c.mount()?;
        verify_load(&c.mnt, seed, counts[0], counts[1]).context("after a remount")?;
        Ok(())
    })();
    let _ = c.unmount();
    result
}

/// The floor the node armed at its last fast takeover, as it logged it:
/// the later of its acknowledgement wait and its lock wait.
fn floor_logged(node: &Client) -> Option<Duration> {
    node.log_text()
        .lines()
        .rfind(|l| l.contains("acknowledging no mutation until its delegation horizon"))
        .map(|l| {
            let l = crate::verify::strip_ansi(l);
            let ms = |k: &str| {
                crate::verify::field(&l, k)
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0)
            };
            Duration::from_millis(ms("wait_ms").max(ms("lock_wait_ms")))
        })
}

/// Plan 43 C7-H2 (review R3-3): an upgrade right after a seal waits out the
/// successor's takeover floor before it hands over (the drain releases the
/// lease, which the floor holds back), and the wait must not stall the
/// node's applications: the views keep serving until the drain. Reader R
/// holds a strict delegation from A, A dies, its backup X takes over and is
/// upgraded at once, with a reader of `f` and a writer running on X's
/// mount. The upgrade takes about the floor (23.5 s with the default lock
/// TTL; less when A's beats put the end of its lease earlier, plan 43
/// C7-F F-a: the floor X logged is the reference). The takeover's own gate holds X's first reads and writes for the
/// read floor (6.5 s here: reported, it is the takeover's doing, not the
/// upgrade's); after it no read on X's mount waits more than a few
/// seconds, whichever of the wait, the detach or the exec it falls in, and
/// none fails. (With the wait after the detach, as in the first version of
/// this change, every read stalled for the whole 24 s.)
pub fn upgrade_after_a_seal(_seed: u64) -> Result<()> {
    use super::m9::{cluster, wait_for_backup, wait_holds};
    const NAME: &str = "upgrade-after-a-seal";
    // The longest a read on X's mount may take once the takeover gate is
    // open (it opens `1.5 + 3 + 2 = 6.5 s` after the marker).
    const READ_BOUND: Duration = Duration::from_secs(4);
    const GATE_OPEN: Duration = Duration::from_secs(8);
    let deleg = "3000".to_string();
    let (_env, _root, mut clients, _) = cluster(
        NAME,
        &["a", "x", "r"],
        &[
            ("CONSTELLATION_FUSE_TRANSPORT", "dev-fuse"),
            ("CONSTELLATION_CTO", "strict"),
            ("CONSTELLATION_READ_DELEGATION_TTL_MS", &deleg),
            ("CONSTELLATION_READ_INDEX_BUDGET_MS", "20000"),
        ],
        0,
    )?;
    let result = (|| -> Result<()> {
        let backups = wait_for_backup(&clients[0], Duration::from_secs(30))?;
        let x_idx = (0..clients.len())
            .find(|i| super::m9::node_id(&clients[*i]).ok() == Some(backups[0]))
            .context("the backup is not one of ours")?;
        let r_idx = (1..clients.len())
            .find(|i| *i != x_idx)
            .expect("three nodes");
        for _ in 0..5 {
            std::fs::read(clients[r_idx].mnt.join("f")).context("R reading f")?;
        }
        let epoch = super::lease_of(&clients[0])?["epoch"].as_u64().unwrap_or(0);
        clients[0].kill9()?;
        let took = wait_holds(&clients[x_idx], epoch, Duration::from_secs(15))?;
        eprintln!(
            "    {NAME}: {} took over after {took:?}; upgrading it at once",
            clients[x_idx].name
        );
        let first = generation(&clients[x_idx])?;
        let stop = Arc::new(AtomicBool::new(false));
        // (start since the upgrade began, how long, outcome) per op.
        type Ops = Arc<Mutex<Vec<(Duration, Duration, std::io::Result<()>)>>>;
        let begun = Instant::now();
        let spawn = |ops: Ops, op: Box<dyn Fn(u64) -> std::io::Result<()> + Send>, every: u64| {
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut i = 0;
                while !stop.load(Ordering::Relaxed) {
                    i += 1;
                    let started = begun.elapsed();
                    let r = op(i);
                    ops.lock()
                        .unwrap()
                        .push((started, begun.elapsed() - started, r));
                    std::thread::sleep(Duration::from_millis(every));
                }
            })
        };
        let reads: Ops = Arc::default();
        let writes: Ops = Arc::default();
        let rpath = clients[x_idx].mnt.join("f");
        let wdir = clients[x_idx].mnt.clone();
        let t_reads = spawn(
            reads.clone(),
            Box::new(move |_| std::fs::read(&rpath).map(|_| ())),
            20,
        );
        let t_writes = spawn(
            writes.clone(),
            Box::new(move |i| std::fs::write(wdir.join(format!("w{i}")), b"w")),
            100,
        );
        let upgrade_took = upgrade(&clients[x_idx])?;
        std::thread::sleep(Duration::from_secs(3));
        stop.store(true, Ordering::Relaxed);
        for t in [t_reads, t_writes] {
            t.join().ok();
        }
        let (reads, writes) = (reads.lock().unwrap(), writes.lock().unwrap());
        let longest = |ops: &[(Duration, Duration, std::io::Result<()>)]| {
            ops.iter()
                .max_by_key(|o| o.1)
                .map(|o| (o.0, o.1))
                .unwrap_or_default()
        };
        let read_errors: Vec<String> = reads
            .iter()
            .filter_map(|o| o.2.as_ref().err().map(|e| format!("+{:?}: {e}", o.0)))
            .collect();
        let write_errors: Vec<String> = writes
            .iter()
            .filter_map(|o| o.2.as_ref().err().map(|e| format!("+{:?}: {e}", o.0)))
            .collect();
        eprintln!(
            "    {NAME}: the upgrade took {upgrade_took:?}; {} reads on X's mount, longest \
             (start, took) {:?}, {} errors; {} writes, longest {:?}, {} errors",
            reads.len(),
            longest(&reads),
            read_errors.len(),
            writes.len(),
            longest(&writes),
            write_errors.len()
        );
        ensure!(
            generation(&clients[x_idx])? == first + 1,
            "the upgrade did not advance the generation"
        );
        // The floor X armed: the later of its two waits as it logged them
        // (plan 43 C7-F F-a: the cap on the dead holder's lease can end
        // the floor before the bound, and where in its 20 s lease A was
        // killed decides how much is left of it).
        let floor = floor_logged(&clients[x_idx]);
        eprintln!("    {NAME}: X's floor as it logged it: {floor:?}");
        ensure!(
            floor.is_some_and(|f| upgrade_took + Duration::from_secs(1) >= f),
            "the upgrade took only {upgrade_took:?}: it did not wait out the takeover floor \
             ({floor:?})"
        );
        let after_gate: Vec<_> = reads
            .iter()
            .filter(|o| o.0 >= GATE_OPEN)
            .map(|o| (o.0, o.1))
            .collect();
        let worst = after_gate.iter().max_by_key(|o| o.1).copied();
        eprintln!(
            "    {NAME}: {} reads started after the gate opened, longest (start, took) {worst:?}",
            after_gate.len()
        );
        // Reads must have been *served* during the wait itself, between the
        // gate's opening and the drain: with the views detached early one
        // read blocks for the whole wait and none starts in it.
        let in_wait = after_gate
            .iter()
            .filter(|o| o.0 + Duration::from_secs(3) < upgrade_took)
            .count();
        // A read every 20 ms: about 50 a second between the gate's opening
        // and the drain, if the wait reaches that far (a floor the cap
        // shortened may end before the gate does).
        let window = upgrade_took.saturating_sub(GATE_OPEN + Duration::from_secs(3));
        ensure!(
            in_wait as f64 > (window.as_secs_f64() * 50.0 * 0.5).min(100.0),
            "only {in_wait} reads were served during the upgrade's wait ({window:?} between the \
             gate and the drain): the views were detached before it"
        );
        ensure!(
            worst.is_some_and(|w| w.1 <= READ_BOUND),
            "a read on X's mount stalled {worst:?} (bound {READ_BOUND:?}) after the takeover \
             gate opened: the views were detached during the wait"
        );
        ensure!(read_errors.is_empty(), "reads failed: {read_errors:?}");
        ensure!(write_errors.is_empty(), "writes failed: {write_errors:?}");
        clients[0].mount()?;
        Ok(())
    })();
    super::m9::unmount_all(&mut clients);
    result
}
