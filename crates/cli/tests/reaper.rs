//! The zombie reaper drops whatever its daemon left mounted, however the
//! daemon died. A real `constellation mount` on the local file backend
//! is `kill -9`ed (which is also what the reaper's own parent dying looks
//! like: the reaper is the daemon's child); the mount must be gone, and
//! the reaper with it, within seconds, with nobody calling `umount`.
//!
//! A daemon whose parent does not reap it at once stays a zombie for a
//! while: that is gone too. And the reaper reaps only holding
//! `daemon.lock` itself: whoever holds it keeps the records and mounts.
//!
//! Needs FUSE (`/dev/fuse`, `fusermount3`), so it is `#[ignore]`d:
//!
//!   cargo test -p constellation --test reaper -- --ignored

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_constellation");

fn wait_until(what: &str, within: Duration, mut f: impl FnMut() -> bool) {
    let t = Instant::now();
    while t.elapsed() < within {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("{what} did not happen within {within:?}");
}

fn mounted(mnt: &Path) -> bool {
    std::fs::read_to_string("/proc/self/mountinfo")
        .unwrap()
        .lines()
        .any(|l| l.split(' ').nth(4) == Some(mnt.to_str().unwrap()))
}

/// Pids whose command line names `needle` and `word`.
fn pids_with(word: &str, needle: &str) -> Vec<u32> {
    let mut out = Vec::new();
    for e in std::fs::read_dir("/proc").unwrap().flatten() {
        let Some(pid) = e.file_name().to_str().and_then(|n| n.parse::<u32>().ok()) else {
            continue;
        };
        let Ok(raw) = std::fs::read(format!("/proc/{pid}/cmdline")) else {
            continue;
        };
        let cmd = String::from_utf8_lossy(&raw).replace('\0', " ");
        if cmd.contains(word) && cmd.contains(needle) {
            out.push(pid);
        }
    }
    out
}

/// A daemon on the local file backend in `dir`, mounted, with its reaper
/// up: (daemon, mountpoint, state dir).
fn start_daemon(dir: &Path) -> (std::process::Child, std::path::PathBuf, std::path::PathBuf) {
    let (s3, mnt, state) = (
        format!("{}", dir.join("s3").display()),
        dir.join("mnt"),
        dir.join("state"),
    );
    std::fs::create_dir_all(&mnt).unwrap();
    std::fs::create_dir_all(&state).unwrap();
    let st = Command::new(BIN)
        .args(["fs", "create", "reaper-test", "--s3", &s3])
        .status()
        .unwrap();
    assert!(st.success(), "fs create: {st}");
    let daemon = Command::new(BIN)
        .args(["mount", "/"])
        .arg(&mnt)
        .args(["--s3", &s3, "--state-dir"])
        .arg(&state)
        .arg("--foreground")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_until("the mount", Duration::from_secs(30), || mounted(&mnt));
    wait_until("the reaper", Duration::from_secs(10), || {
        reapers(&state).len() == 1
    });
    (daemon, mnt, state)
}

fn reapers(state: &Path) -> Vec<u32> {
    pids_with("zombie-reaper", state.to_str().unwrap())
}

fn sigkill(pid: u32) {
    // SAFETY: a signal to a process of this test.
    unsafe { libc::kill(pid as i32, libc::SIGKILL) };
}

fn is_zombie(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/status")).is_ok_and(|s| {
        s.lines()
            .any(|l| l.starts_with("State:") && l.contains('Z'))
    })
}

#[test]
#[ignore = "needs FUSE"]
fn a_killed_daemons_mount_and_reaper_are_gone_within_seconds() {
    let dir = tempfile::tempdir().unwrap();
    let (mut daemon, mnt, state) = start_daemon(dir.path());

    sigkill(daemon.id());
    let _ = daemon.wait();

    wait_until("the mount's removal", Duration::from_secs(20), || {
        !mounted(&mnt)
    });
    wait_until("the reaper's exit", Duration::from_secs(10), || {
        reapers(&state).is_empty()
    });
    let log = std::fs::read_to_string(state.join("reaper.log")).unwrap_or_default();
    assert!(log.contains("lazy unmount: done"), "reaper.log: {log:?}");
}

#[test]
#[ignore = "needs FUSE"]
fn an_unreaped_zombie_daemon_is_gone_too() {
    let dir = tempfile::tempdir().unwrap();
    let (mut daemon, mnt, state) = start_daemon(dir.path());

    // Killed, and left a zombie: nobody waits for it until the end.
    sigkill(daemon.id());
    wait_until("the daemon's death", Duration::from_secs(10), || {
        is_zombie(daemon.id())
    });
    wait_until("the mount's removal", Duration::from_secs(20), || {
        !mounted(&mnt)
    });
    wait_until("the reaper's exit", Duration::from_secs(10), || {
        reapers(&state).is_empty()
    });
    assert!(is_zombie(daemon.id()), "the daemon was reaped meanwhile");
    let log = std::fs::read_to_string(state.join("reaper.log")).unwrap_or_default();
    assert!(log.contains("lazy unmount: done"), "reaper.log: {log:?}");
    let _ = daemon.wait();
}

#[test]
#[ignore = "needs FUSE"]
fn the_reaper_leaves_a_state_dir_whose_lock_someone_holds_alone() {
    let dir = tempfile::tempdir().unwrap();
    let (mut daemon, mnt, state) = start_daemon(dir.path());
    let reaper = reapers(&state)[0];

    // Hold the reaper still while the daemon dies and someone else (a
    // new daemon, as far as the reaper can tell) takes daemon.lock.
    // SAFETY: signals to processes of this test.
    unsafe { libc::kill(reaper as i32, libc::SIGSTOP) };
    sigkill(daemon.id());
    let _ = daemon.wait();
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(state.join("daemon.lock"))
        .unwrap();
    // SAFETY: a plain `flock(2)` on a descriptor we own.
    assert_eq!(
        unsafe { libc::flock(std::os::fd::AsRawFd::as_raw_fd(&file), libc::LOCK_EX) },
        0
    );
    unsafe { libc::kill(reaper as i32, libc::SIGCONT) };

    wait_until("the reaper's exit", Duration::from_secs(10), || {
        reapers(&state).is_empty()
    });
    let log = std::fs::read_to_string(state.join("reaper.log")).unwrap_or_default();
    let records = std::fs::read_dir(state.join("mounts")).unwrap().count();
    let still_mounted = mounted(&mnt);
    let _ = Command::new("fusermount3").arg("-uz").arg(&mnt).status();
    drop(file);
    assert!(still_mounted, "the reaper unmounted under another's lock");
    assert_eq!(records, 1, "the reaper took another's records");
    assert!(!log.contains("is gone"), "reaper.log: {log:?}");
}
