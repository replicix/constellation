//! Plan 39: `fsync` under S3 outages — `hard` by default, `soft` on
//! request, killable (not interruptible, as NFS `hard`), and `fsyncdir` as
//! a real barrier.
//!
//! The outage scenarios mount with `--fsync-mode s3`: under the default
//! `local`, a single node's S3 cut puts it in a continuation epoch within
//! about a second, and an epoch's `fsync` does not wait for the bucket at
//! all (plan 30 §M10; what `local` should wait for is plan 39 §6's open
//! question). `fio-blips` covers `local` before the epoch starts.
//!
//! - `fsync-hard-outage`: S3 is cut for ~20 s while an `fsync` waits on
//!   unuploaded data. The `fsync` neither fails nor returns early: it
//!   returns 0 only after the heal, `node.status.fsync` shows the wait, and
//!   a fresh node reads exactly the bytes back from the bucket.
//! - `fsync-soft-timeout`: the same cut under `--fsync-timeout 2s`. The
//!   `fsync` answers `EIO` after about two seconds, and the data is not
//!   lost: after the heal it reaches the bucket by itself (a fresh node
//!   reads it, no `pending_upload` row is left), and the next `fsync` on
//!   the same descriptor returns 0.
//! - `fsync-interrupt`: during a cut, a process blocked in `fsync` is sent
//!   `SIGINT`, which it handles (Python's `KeyboardInterrupt`): its `fsync`
//!   keeps waiting — a handled signal is no reason to fail one, so no
//!   `EINTR` reaches it. Then `SIGKILL`: the kernel keeps a request that
//!   is already in the daemon waiting even for a dying caller, so the
//!   daemon notices the fatal signal itself and ends the wait — the
//!   process is reaped within seconds, not left unkillable until S3
//!   returns — and its data still goes up after the heal.
//! - `fsyncdir-barrier`: under `--fsync-mode s3`, `fsync` of a directory
//!   after a rename into it ships the journal before it returns (the
//!   classic "fsync the directory" durability pattern); before plan 39 the
//!   kernel was told `ENOSYS` and answered every directory `fsync` 0
//!   without asking.

use super::{eventually, setup, ts};
use crate::client::Client;
use crate::s3env::BUCKET;
use anyhow::{Context, Result};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::time::{Duration, Instant};

/// How long the hard scenario keeps S3 cut under a waiting `fsync`.
const OUTAGE: Duration = Duration::from_secs(20);

fn seeded(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

fn status_u64(c: &Client, path: &[&str]) -> Result<u64> {
    let mut v = c.control_status()?;
    for key in path {
        v = v[*key].take();
    }
    v.as_u64()
        .with_context(|| format!("status.{} missing", path.join(".")))
}

/// Create the empty file `path` on `c` while S3 is still reachable, and
/// wait until `c` holds the lease the create took.
///
/// What these scenarios test is an `fsync`'s wait for *data* during a cut,
/// so the node must already hold the lease when S3 goes: no lease can be
/// had without S3 (plan 39: a mutation that needs one during an outage
/// waits out its 120 s client deadline and fails `EIO` — before any
/// `fsync`). They used to cut right after the mount and create the file
/// during the cut, which passed only unprivileged: there the mount's
/// root-owner adoption (`engine::node::adopt_root`, a `Setattr` through
/// the lease path) had taken the lease already; as root that adoption is
/// skipped, so the create waited 120 s and the scenario failed.
fn create_holding(c: &Client, path: &Path) -> Result<()> {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o644)
        .open(path)?;
    eventually(
        "the node holds the lease before the cut",
        Duration::from_secs(30),
        || {
            let lease = c.control_status()?["lease"].clone();
            anyhow::ensure!(lease["held"] == true, "{lease}");
            Ok(())
        },
    )
}

/// The file `create_holding` made, with `data` written and not yet flushed
/// (written during a cut: its chunks exist only on this node until
/// something uploads them).
fn staged(path: &Path, data: &[u8]) -> Result<std::fs::File> {
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    f.write_all(data)?;
    Ok(f)
}

/// A second node, mounted on a fresh state dir after the heal, must read
/// `data` at `name` from the bucket.
fn read_back_elsewhere(
    root: &Path,
    endpoint: &str,
    backend: &str,
    name: &str,
    data: &[u8],
) -> Result<()> {
    let mut other = Client::new(root, "reader", endpoint, backend)?;
    other.mount()?;
    let result = eventually(
        "the fsynced file read back from the bucket on another node",
        Duration::from_secs(60),
        || {
            let got = std::fs::read(other.mnt.join(name))?;
            anyhow::ensure!(
                got == data,
                "read {} bytes, want {} (equal: {})",
                got.len(),
                data.len(),
                got == data
            );
            Ok(())
        },
    );
    other.unmount()?;
    result
}

pub(super) fn fsync_hard_outage(seed: u64) -> Result<()> {
    let (env, root) = setup("fsync-hard")?;
    let proxy = env.s3_proxy()?;
    let prefix = format!("fsynchard-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut c = Client::new(root.path(), "c0", &env.endpoint, &backend)?;
    c.fs_create()?;
    c.mount_view(None, &["--fsync-mode", "s3"])?;
    anyhow::ensure!(
        c.control_status()?["fsync"]["mode"] == "hard",
        "the default fsync policy is hard"
    );
    let data = seeded(seed, 3 << 20);
    create_holding(&c, &c.mnt.join("f"))?;
    // Cut before the data: a sequential writer's full chunks start
    // uploading as soon as they are sealed, so data written before the cut
    // is already up.
    proxy.cut()?;
    let file = staged(&c.mnt.join("f"), &data)?;
    let started = Instant::now();
    let outcome = std::thread::scope(|scope| -> Result<Result<(), std::io::Error>> {
        let fsync = scope.spawn(|| file.sync_all());
        std::thread::sleep(OUTAGE);
        let waiting = !fsync.is_finished();
        let status = c.control_status();
        proxy.heal()?;
        let result = fsync.join().expect("the fsync thread");
        anyhow::ensure!(
            waiting,
            "fsync returned during the S3 cut after {:?}: {result:?}",
            started.elapsed()
        );
        let status = status?;
        anyhow::ensure!(
            status["fsync"]["waiting"].as_u64() == Some(1)
                && status["fsync"]["longest_wait_ms"].as_u64().unwrap_or(0) > 5_000,
            "node.status.fsync during the cut: {}",
            status["fsync"]
        );
        Ok(result)
    })?;
    let waited = started.elapsed();
    outcome.with_context(|| format!("fsync failed after {waited:?} (it must wait out the cut)"))?;
    anyhow::ensure!(waited >= OUTAGE, "fsync returned after only {waited:?}");
    anyhow::ensure!(status_u64(&c, &["fsync", "waited"])? >= 1);
    anyhow::ensure!(status_u64(&c, &["fsync", "max_wait_ms"])? >= OUTAGE.as_millis() as u64);
    anyhow::ensure!(status_u64(&c, &["fsync", "timeouts"])? == 0);
    anyhow::ensure!(status_u64(&c, &["fsync", "permanent_errors"])? == 0);
    anyhow::ensure!(
        c.log_text().contains("S3 unreachable, still trying"),
        "the wait was logged once it passed 10 s"
    );
    drop(file);
    read_back_elsewhere(root.path(), &env.endpoint, &backend, "f", &data)?;
    c.unmount()?;
    Ok(())
}

pub(super) fn fsync_soft_timeout(seed: u64) -> Result<()> {
    let (env, root) = setup("fsync-soft")?;
    let proxy = env.s3_proxy()?;
    let prefix = format!("fsyncsoft-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut c = Client::new(root.path(), "c0", &env.endpoint, &backend)?;
    c.fs_create()?;
    c.mount_view(None, &["--fsync-mode", "s3", "--fsync-timeout", "2s"])?;
    let fsync = c.control_status()?["fsync"].clone();
    anyhow::ensure!(
        fsync["mode"] == "soft" && fsync["timeout_ms"] == 2000,
        "--fsync-timeout 2s: {fsync}"
    );
    let data = seeded(seed, 2 << 20);
    create_holding(&c, &c.mnt.join("f"))?;
    proxy.cut()?;
    let file = staged(&c.mnt.join("f"), &data)?;
    let started = Instant::now();
    let result = file.sync_all();
    let took = started.elapsed();
    let still_pending = status_u64(&c, &["writeback", "pending_uploads"])?;
    proxy.heal()?;
    let error = match result {
        Ok(()) => anyhow::bail!("fsync succeeded during the cut ({took:?})"),
        Err(error) => error,
    };
    anyhow::ensure!(
        error.raw_os_error() == Some(libc::EIO),
        "fsync failed with {error}, want EIO"
    );
    anyhow::ensure!(
        took >= Duration::from_millis(1900) && took < Duration::from_secs(15),
        "the 2 s soft timeout answered after {took:?}"
    );
    anyhow::ensure!(still_pending > 0, "the data stays pending after the EIO");
    anyhow::ensure!(status_u64(&c, &["fsync", "timeouts"])? >= 1);

    // Nothing was dropped: after the heal the background rounds upload it
    // with no further fsync, and another node reads it from the bucket.
    eventually(
        "the timed-out fsync's chunks uploaded after the heal",
        Duration::from_secs(60),
        || {
            let pending = status_u64(&c, &["writeback", "pending_uploads"])?;
            anyhow::ensure!(pending == 0, "{pending} pending uploads");
            Ok(())
        },
    )?;
    read_back_elsewhere(root.path(), &env.endpoint, &backend, "f", &data)?;
    // And the descriptor's next fsync is truthful: 0 now.
    file.sync_all()
        .context("fsync after the heal on the same descriptor")?;
    drop(file);
    c.unmount()?;
    Ok(())
}

pub(super) fn fsync_interrupt(seed: u64) -> Result<()> {
    let (env, root) = setup("fsync-intr")?;
    let proxy = env.s3_proxy()?;
    let prefix = format!("fsyncintr-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut c = Client::new(root.path(), "c0", &env.endpoint, &backend)?;
    c.fs_create()?;
    c.mount_view(None, &["--fsync-mode", "s3"])?;
    // Over io_uring the fsync runs on an offload thread while its
    // interrupt arrives on the /dev/fuse reader (transport-matrix leg).
    eprintln!(
        "  transport: {}",
        c.control_status()?["mounts"][0]["transport"]
    );
    let data = seeded(seed, 1 << 20);
    let input = root.path().join("payload");
    std::fs::write(&input, &data)?;
    let target = c.mnt.join("f");
    create_holding(&c, &target)?;

    proxy.cut()?;
    // Python's default SIGINT handling is an interactive program's Ctrl-C:
    // a handler that raises `KeyboardInterrupt` once the interrupted call
    // returns. So an `fsync` that answered `EINTR` to it would end the
    // process at once (with the default handler raising, PEP 475's retry
    // does not apply), and one that keeps waiting keeps the process
    // blocked.
    let script = format!(
        "import os\nfd = os.open({target:?}, os.O_WRONLY | os.O_CREAT, 0o644)\n\
         os.write(fd, open({input:?}, 'rb').read())\nos.fsync(fd)\nprint('fsync returned')\n",
        target = target.display().to_string(),
        input = input.display().to_string(),
    );
    let mut child = std::process::Command::new("python3")
        .arg("-c")
        .arg(&script)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("python3")?;
    let outcome = (|| -> Result<()> {
        std::thread::sleep(Duration::from_secs(4));
        anyhow::ensure!(
            child.try_wait()?.is_none(),
            "the fsync returned during the cut"
        );
        anyhow::ensure!(
            status_u64(&c, &["fsync", "waiting"])? == 1,
            "an fsync is waiting"
        );
        // A handled signal: the kernel sends FUSE_INTERRUPT, and the
        // fsync keeps waiting.
        // SAFETY: a plain kill(2) of our own child.
        unsafe { libc::kill(child.id() as i32, libc::SIGINT) };
        std::thread::sleep(Duration::from_secs(3));
        anyhow::ensure!(
            child.try_wait()?.is_none(),
            "a handled SIGINT ended the fsync (EINTR) instead of leaving it waiting"
        );
        anyhow::ensure!(status_u64(&c, &["fsync", "waiting"])? == 1);
        anyhow::ensure!(status_u64(&c, &["fsync", "interrupted"])? == 0);
        // A fatal one: no second FUSE_INTERRUPT comes (the kernel sends
        // one per request); the daemon sees the pending SIGKILL itself.
        let killed = Instant::now();
        // SAFETY: as above.
        unsafe { libc::kill(child.id() as i32, libc::SIGKILL) };
        loop {
            if child.try_wait()?.is_some() {
                break;
            }
            anyhow::ensure!(
                killed.elapsed() < Duration::from_secs(5),
                "the process stayed blocked in fsync after SIGKILL (unkillable)"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        let mut stdout = String::new();
        if let Some(mut out) = child.stdout.take() {
            std::io::Read::read_to_string(&mut out, &mut stdout)?;
        }
        anyhow::ensure!(
            !stdout.contains("fsync returned"),
            "fsync completed during the cut: {stdout}"
        );
        eprintln!(
            "  SIGKILL reaped the fsync's caller in {:?}",
            killed.elapsed()
        );
        eventually(
            "the killed caller's fsync wait ended",
            Duration::from_secs(5),
            || {
                anyhow::ensure!(status_u64(&c, &["fsync", "interrupted"])? >= 1);
                anyhow::ensure!(status_u64(&c, &["fsync", "waiting"])? == 0);
                Ok(())
            },
        )?;
        Ok(())
    })();
    if outcome.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    proxy.heal()?;
    outcome?;
    // The killed caller's fsync left its data pending, and it goes up.
    read_back_elsewhere(root.path(), &env.endpoint, &backend, "f", &data)?;
    c.unmount()?;
    Ok(())
}

pub(super) fn fsyncdir_barrier(seed: u64) -> Result<()> {
    let (env, root) = setup("fsyncdir")?;
    let _proxy = env.s3_proxy()?;
    let prefix = format!("fsyncdir-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut c = Client::new(root.path(), "c0", &env.endpoint, &backend)?
        // Rounds rare enough that only a barrier ships the rename in
        // time for the check below.
        .with_env("CONSTELLATION_SYNC_INTERVAL_MS", "60000");
    c.fs_create()?;
    c.mount_view(None, &["--fsync-mode", "s3"])?;
    let dir = c.mnt.join("d");
    std::fs::create_dir(&dir)?;
    let data = seeded(seed, 64 << 10);
    for i in 0..5 {
        let tmp = dir.join(format!(".tmp{i}"));
        let file = staged(&tmp, &data)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, dir.join(format!("final{i}")))?;
        std::fs::File::open(&dir)?
            .sync_all()
            .context("fsync of the directory")?;
        let backlog = status_u64(&c, &["spool", "journal_backlog"])?;
        anyhow::ensure!(
            backlog == 0,
            "fsync(dir) returned with {backlog} journal records unshipped (--fsync-mode s3)"
        );
    }
    // The barrier's promise, checked the hard way: the node dies at once,
    // and a fresh node sees every rename.
    c.kill9()?;
    let mut other = Client::new(root.path(), "reader", &env.endpoint, &backend)?;
    other.mount()?;
    let result = eventually(
        "the renames on another node",
        Duration::from_secs(30),
        || {
            for i in 0..5 {
                let got = std::fs::read(other.mnt.join(format!("d/final{i}")))?;
                anyhow::ensure!(got == data, "d/final{i} differs");
            }
            anyhow::ensure!(!other.mnt.join("d/.tmp0").exists());
            Ok(())
        },
    );
    other.unmount()?;
    result
}
