//! A close the kernel sends from inside its caller's `execve(2)` is
//! answered while the caller holds a cluster lock on the mount.
//!
//! `execve` closes the process's `O_CLOEXEC` descriptors itself, and for
//! each one on a FUSE mount it sends a `FLUSH` and waits for the answer,
//! uninterruptibly, holding the process's exec lock. A daemon whose
//! answer needs something that lock guards never answers: plan 31's
//! closing gate saw every `git` that a process holding an `flock` on the
//! mount forked hang in its first close, until the connection was
//! aborted (`git-under-flock*`, `lock-failover`,
//! `lock-holder-killed-contention`). The close took its caller's fencing
//! token, which named the caller's process across pid reuse by reading
//! its `/proc/<pid>/stat`, and that read waits for the exec on a kernel
//! with Linux commit 6650527444da (`stat` takes the exec lock) and without
//! e780259b54e6 (which has the exec close its descriptors before taking
//! it). On a
//! kernel with both, or neither, the read never waits there, and the test
//! passes with or without the fix: a green run on such a kernel proves
//! nothing about it.
//!
//! Here the test process takes an `flock` (a cluster lock: P2P is on),
//! keeps a written `O_CLOEXEC` descriptor of another file open, and runs
//! `/bin/true`, whose exec closes it. On a hang the test aborts its own
//! FUSE connection (which ends the exec) so that it can clean up, then
//! fails.
//!
//! Needs FUSE (`/dev/fuse`, `fusermount3`); skipped, loudly, without it.

use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn bin() -> &'static str {
    static BIN: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    BIN.get_or_init(|| {
        std::env::var("NEXTEST_BIN_EXE_constellation")
            .or_else(|_| std::env::var("CARGO_BIN_EXE_constellation"))
            .unwrap_or_else(|_| env!("CARGO_BIN_EXE_constellation").to_string())
    })
}

fn fuse_available() -> bool {
    Path::new("/dev/fuse").exists()
        && std::env::var_os("PATH").is_some_and(|path| {
            std::env::split_paths(&path).any(|d| d.join("fusermount3").exists())
        })
}

/// A `constellation` with its registry, config, data and runtime dirs in
/// `dir`. P2P stays on: without it the mount's locks are local, and the
/// close would take no fencing token.
fn command(dir: &Path, args: &[&str]) -> Command {
    let mut cmd = Command::new(bin());
    cmd.args(args)
        .env("CONSTELLATION_REGISTRY", dir.join("config/registry.toml"))
        .env("XDG_CONFIG_HOME", dir.join("config"))
        .env("XDG_DATA_HOME", dir.join("data"))
        .env("XDG_RUNTIME_DIR", dir.join("run"))
        .env("CONSTELLATION_LOCKS", "cluster")
        .stdin(Stdio::null());
    cmd
}

fn mounted(mnt: &Path) -> bool {
    std::fs::read_to_string("/proc/self/mountinfo")
        .unwrap()
        .lines()
        .any(|l| l.split(' ').nth(4) == Some(mnt.to_str().unwrap()))
}

/// The daemon and its mount, gone however the test ends.
struct Mounted {
    daemon: Child,
    mnt: PathBuf,
}

impl Drop for Mounted {
    fn drop(&mut self) {
        if mounted(&self.mnt) {
            let _ = Command::new("fusermount3")
                .arg("-u")
                .arg(&self.mnt)
                .status();
        }
        let t = Instant::now();
        while self.daemon.try_wait().unwrap().is_none() && t.elapsed() < Duration::from_secs(30) {
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = self.daemon.kill();
        let _ = self.daemon.wait();
        if mounted(&self.mnt) {
            let _ = Command::new("fusermount3")
                .arg("-uz")
                .arg(&self.mnt)
                .status();
        }
    }
}

/// End every request of the FUSE connection behind `mnt` (its fusectl
/// `abort`, which the mount's owner may write): the only way out of a
/// `FLUSH` the daemon will never answer.
fn abort_connection(mnt: &Path) {
    let dev = std::fs::metadata(mnt).unwrap().dev();
    let conn = libc::minor(dev);
    let _ = std::fs::write(format!("/sys/fs/fuse/connections/{conn}/abort"), "1");
}

#[test]
fn a_close_inside_execve_is_answered_while_the_caller_holds_a_lock() {
    if !fuse_available() {
        eprintln!("skipping: needs /dev/fuse and fusermount3");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let (s3, state, mnt) = (dir.join("s3"), dir.join("state"), dir.join("mnt"));
    for d in [&s3, &state, &mnt] {
        std::fs::create_dir_all(d).unwrap();
    }
    let created = command(dir, &["fs", "create", "t", "--s3", s3.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        created.status.success(),
        "fs create: {}",
        String::from_utf8_lossy(&created.stderr)
    );
    let log = std::fs::File::create(dir.join("mount.log")).unwrap();
    let daemon = command(
        dir,
        &[
            "mount",
            "/",
            mnt.to_str().unwrap(),
            "--s3",
            s3.to_str().unwrap(),
            "--state-dir",
            state.to_str().unwrap(),
            "--foreground",
        ],
    )
    .stdout(log.try_clone().unwrap())
    .stderr(log)
    .spawn()
    .unwrap();
    let m = Mounted {
        daemon,
        mnt: mnt.clone(),
    };
    let t = Instant::now();
    while !mounted(&mnt) {
        assert!(
            t.elapsed() < Duration::from_secs(60),
            "the mount did not appear: {}",
            std::fs::read_to_string(dir.join("mount.log")).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // std opens every file `O_CLOEXEC`.
    let turn = std::fs::File::create(mnt.join("turn")).unwrap();
    // SAFETY: flock(2) on a descriptor we own.
    assert_eq!(unsafe { libc::flock(turn.as_raw_fd(), libc::LOCK_EX) }, 0);
    let written = {
        use std::io::Write;
        let mut f = std::fs::File::create(mnt.join("f")).unwrap();
        f.write_all(b"written under the lock").unwrap();
        f
    };

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(Command::new("/bin/true").status());
    });
    let ran = rx.recv_timeout(Duration::from_secs(20));
    if ran.is_err() {
        abort_connection(&mnt);
        let _ = rx.recv_timeout(Duration::from_secs(20));
        panic!(
            "the exec of /bin/true did not finish within 20 s: the daemon did not \
             answer the FLUSH of the descriptor the exec closed"
        );
    }
    assert!(ran.unwrap().unwrap().success());

    drop(written);
    // SAFETY: as above.
    assert_eq!(unsafe { libc::flock(turn.as_raw_fd(), libc::LOCK_UN) }, 0);
    drop(turn);
    assert_eq!(
        std::fs::read(mnt.join("f")).unwrap(),
        b"written under the lock"
    );
    drop(m);
}
