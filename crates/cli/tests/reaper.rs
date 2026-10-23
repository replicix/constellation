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

/// nextest remaps binary paths when a test runs from an extracted archive
/// (`--archive-file`/`--extract-to`): it exposes the remapped path as
/// `NEXTEST_BIN_EXE_constellation` and also rewrites `CARGO_BIN_EXE_constellation`
/// in the environment, so read those at runtime instead of trusting the
/// path `env!` baked in at compile time.
fn bin() -> &'static str {
    static BIN: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    BIN.get_or_init(|| {
        std::env::var("NEXTEST_BIN_EXE_constellation")
            .or_else(|_| std::env::var("CARGO_BIN_EXE_constellation"))
            .unwrap_or_else(|_| env!("CARGO_BIN_EXE_constellation").to_string())
    })
}

/// What the reaper's log says of a mount it dropped. A daemon killed a few
/// ms after its first mount dies inside the kernel-fix probe, with a thread
/// in `D` behind the probe mount's request (even without fix 0001): the
/// reaper then takes the wedged path, which logs a count.
fn lazily_unmounted(log: &str) -> bool {
    log.contains("lazy unmount: done")
        || (log.contains("aborted mount(s) lazily unmounted")
            && !log.contains("; 0 aborted mount(s) lazily unmounted"))
}

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
    let st = Command::new(bin())
        .args(["fs", "create", "reaper-test", "--s3", &s3])
        .status()
        .unwrap();
    assert!(st.success(), "fs create: {st}");
    let daemon = Command::new(bin())
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
    assert!(lazily_unmounted(&log), "reaper.log: {log:?}");
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
    assert!(lazily_unmounted(&log), "reaper.log: {log:?}");
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
    // The views' records (named by id), not a kernel probe's.
    let records = std::fs::read_dir(state.join("mounts"))
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().parse::<u64>().is_ok())
        .count();
    let still_mounted = mounted(&mnt);
    let _ = Command::new("fusermount3").arg("-uz").arg(&mnt).status();
    drop(file);
    assert!(still_mounted, "the reaper unmounted under another's lock");
    assert_eq!(records, 1, "the reaper took another's records");
    assert!(!log.contains("is gone"), "reaper.log: {log:?}");
}

/// Mountpoints of the kernel-fix probe mounts (plan 42 K6) of the daemon
/// `pid`: its probe's directory is named `constellation-kernel-probe-<pid>-…`.
fn probe_mounts(pid: u32) -> Vec<String> {
    let needle = format!("constellation-kernel-probe-{pid}-");
    std::fs::read_to_string("/proc/self/mountinfo")
        .unwrap()
        .lines()
        .filter(|l| l.contains(" - fuse constellation-probe "))
        .filter_map(|l| l.split(' ').nth(4))
        .filter(|mp| mp.contains(&needle))
        .map(str::to_string)
        .collect()
}

/// A daemon on the local file backend in `dir` (a file system created
/// there), not waited for: (daemon, mountpoint, state dir).
fn spawn_daemon(dir: &Path) -> (std::process::Child, std::path::PathBuf, std::path::PathBuf) {
    let (s3, mnt, state) = (
        format!("{}", dir.join("s3").display()),
        dir.join("mnt"),
        dir.join("state"),
    );
    std::fs::create_dir_all(&mnt).unwrap();
    std::fs::create_dir_all(&state).unwrap();
    if !Path::new(&s3).exists() {
        let st = Command::new(bin())
            .args(["fs", "create", "reaper-test", "--s3", &s3])
            .status()
            .unwrap();
        assert!(st.success(), "fs create: {st}");
    }
    let daemon = Command::new(bin())
        .args(["mount", "/"])
        .arg(&mnt)
        .args(["--s3", &s3, "--state-dir"])
        .arg(&state)
        .arg("--foreground")
        .env_remove("CONSTELLATION_FUSE_KERNEL_PROBE")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    (daemon, mnt, state)
}

/// `kill -9` a daemon while its probe's mount is up. The probe lives for
/// a few ms on a kernel without fix 0001 (about 2 s on one with it, where
/// its thread is in `D` meanwhile), so the daemon is stopped `delay` after
/// its view's record shows up (written right after the probe is started,
/// whose mount follows within ms):
/// nothing of the daemon's runs from then on, the mount appears if it was
/// stopped late enough (not before `fusermount3` was started), and the
/// kill follows. `Some((pid, probe mountpoints left))` if the mount was
/// there when the daemon died, `None` if it never came (the daemon is dead
/// then too, and its view's mount unmounted). `while_stopped` runs once the
/// daemon is stopped, before it is killed.
fn kill_mid_probe(
    daemon: &mut std::process::Child,
    state: &Path,
    mnt: &Path,
    delay: Duration,
    while_stopped: impl FnOnce(),
) -> Option<(u32, Vec<String>)> {
    let pid = daemon.id();
    // From the daemon's spawn on, polled tightly: it is mounting and
    // probing within 100 ms. (The view's record, a number: not the
    // probe's own, which is what is being tested.)
    let t = Instant::now();
    let view_recorded = || {
        std::fs::read_dir(state.join("mounts")).is_ok_and(|d| {
            d.flatten()
                .any(|e| e.file_name().to_string_lossy().parse::<u64>().is_ok())
        })
    };
    while !view_recorded() {
        assert!(t.elapsed() < Duration::from_secs(60), "no view record");
        std::hint::spin_loop();
    }
    let seen = Instant::now();
    while seen.elapsed() < delay {
        std::hint::spin_loop();
    }
    // SAFETY: a signal to a process of this test.
    unsafe { libc::kill(pid as i32, libc::SIGSTOP) };
    while_stopped();
    let t = Instant::now();
    while probe_mounts(pid).is_empty() && t.elapsed() < Duration::from_millis(300) {
        std::thread::sleep(Duration::from_millis(1));
    }
    sigkill(pid);
    // Not `wait()`: a daemon killed here may stay a zombie behind the probe
    // mount's request (a thread in `D`), and `waitpid` does not return for
    // it until the connection is aborted.
    wait_until("the daemon's death", Duration::from_secs(10), || {
        is_zombie(pid)
    });
    let left = probe_mounts(pid);
    if left.is_empty() {
        let _ = Command::new("fusermount3").arg("-uz").arg(mnt).status();
        reap_child(daemon);
        wait_until("the reaper's exit", Duration::from_secs(30), || {
            reapers(state).is_empty()
        });
        return None;
    }
    Some((pid, left))
}

/// Wait for a killed daemon to be gone (it may take its reaper or the next
/// daemon to release it), and reap it.
fn reap_child(child: &mut std::process::Child) {
    wait_until("the daemon's exit", Duration::from_secs(30), || {
        child.try_wait().unwrap().is_some()
    });
}

/// How long after the probe's record to stop the daemon on attempt
/// `attempt`: swept over the probe's few ms, since where in it the stop
/// lands depends on the host.
fn stop_delay(attempt: u32) -> Duration {
    Duration::from_micros(300 + 250 * u64::from(attempt % 10))
}

/// A daemon `kill -9`ed while its probe's mount is up leaves a dead FUSE
/// mount (and, on a kernel with fix 0001, a zombie held by a thread in
/// `D`) that the reaper must clear like a view's: the probe's mount is in
/// `state_dir/mounts`.
#[test]
#[ignore = "needs FUSE"]
fn a_daemon_killed_mid_probe_leaves_no_probe_mount_behind() {
    for attempt in 1..=30 {
        let dir = tempfile::tempdir().unwrap();
        let (mut daemon, mnt, state) = spawn_daemon(dir.path());
        let Some((pid, left)) =
            kill_mid_probe(&mut daemon, &state, &mnt, stop_delay(attempt), || {})
        else {
            // The probe ended before the kill: try again.
            eprintln!("attempt {attempt}: the probe was over");
            continue;
        };
        wait_until("the probe mount's removal", Duration::from_secs(30), || {
            probe_mounts(pid).is_empty()
        });
        wait_until("the reaper's exit", Duration::from_secs(10), || {
            reapers(&state).is_empty()
        });
        reap_child(&mut daemon);
        assert!(
            !state.join("mounts").join(format!("probe-{pid}")).exists(),
            "the probe's record is still there"
        );
        assert!(
            left.iter().all(|mp| !Path::new(mp).exists()),
            "the probe's directory is still there: {left:?}"
        );
        return;
    }
    panic!("never caught a probe mid-run");
}

/// The same with the reaper out of the way: the next daemon of the state
/// dir aborts and unmounts what the killed one's probe left.
#[test]
#[ignore = "needs FUSE"]
fn the_next_daemon_clears_the_probe_mount_a_killed_one_left() {
    for attempt in 1..=30 {
        let dir = tempfile::tempdir().unwrap();
        let (mut daemon, mnt, state) = spawn_daemon(dir.path());
        let caught = kill_mid_probe(&mut daemon, &state, &mnt, stop_delay(attempt), || {
            // The daemon's reaper is up (it starts before the daemon
            // mounts); out of the way, only the next daemon can act.
            wait_until("the reaper", Duration::from_secs(10), || {
                reapers(&state).len() == 1
            });
            for r in reapers(&state) {
                sigkill(r);
            }
        });
        let Some((pid, _left)) = caught else {
            eprintln!("attempt {attempt}: the probe was over");
            continue;
        };
        // Nobody acts but a new daemon.
        std::thread::sleep(Duration::from_secs(2));
        assert!(!probe_mounts(pid).is_empty(), "something else cleared it");

        let mnt2 = dir.path().join("mnt2");
        std::fs::create_dir_all(&mnt2).unwrap();
        let mut next = Command::new(bin())
            .args(["mount", "/"])
            .arg(&mnt2)
            .args(["--s3"])
            .arg(dir.path().join("s3"))
            .args(["--state-dir"])
            .arg(&state)
            .arg("--foreground")
            .env_remove("CONSTELLATION_FUSE_KERNEL_PROBE")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let cleared = std::panic::catch_unwind(|| {
            wait_until(
                "the old probe mount's removal",
                Duration::from_secs(60),
                || probe_mounts(pid).is_empty(),
            );
        });
        // Whatever happened, take the new daemon (and its reaper) down.
        sigkill(next.id());
        let _ = next.wait();
        wait_until("the reaper's exit", Duration::from_secs(30), || {
            reapers(&state).is_empty()
        });
        for mp in probe_mounts(pid) {
            let _ = Command::new("fusermount3").arg("-uz").arg(mp).status();
        }
        cleared.unwrap();
        reap_child(&mut daemon);
        assert!(
            !state.join("mounts").join(format!("probe-{pid}")).exists(),
            "the old probe's record is still there"
        );
        return;
    }
    panic!("never caught a probe mid-run");
}
