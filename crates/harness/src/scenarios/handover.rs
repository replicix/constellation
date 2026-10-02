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
fn upgrade(c: &Client) -> Result<Duration> {
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
        ensure!(gaps.is_empty(), "the mount showed gaps: {gaps:?}");
        ensure!(
            generation(&c)? == first + 3,
            "three upgrades, generation did not advance by three"
        );
        ensure!(c.pid() == Some(pid), "the daemon's pid changed");
        verify_load(&c.mnt, seed, counts[0], counts[1]).context("after the upgrades")?;
        c.unmount()?;
        c.mount()?;
        verify_load(&c.mnt, seed, counts[0], counts[1]).context("after a remount")?;
        Ok(())
    })();
    let _ = c.unmount();
    result
}
