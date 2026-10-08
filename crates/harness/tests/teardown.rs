//! A harness killed with SIGKILL mid-scenario leaves nothing behind: its
//! `constellation mount` daemons die with it (`PR_SET_PDEATHSIG`,
//! `crate::spawn`) and their zombie reapers lazily unmount. Its floci and
//! toxiproxy containers do stay: the next run of the same prefix sweeps
//! them (`sweep::containers`, covered by its unit test); this test does
//! not exercise that and removes them itself.
//!
//! Runs `harness run baseline` under a private temp dir and docker prefix,
//! kills the harness once a daemon is mounted, and then looks for any
//! process or mount mentioning the temp dir. Needs docker, FUSE and a
//! built `constellation` (`CONSTELLATION_BIN`, else `target/debug`), so it
//! is `#[ignore]`d:
//!
//!   cargo test -p constellation-harness --test teardown -- --ignored

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn procs_mentioning(needle: &str) -> Vec<(u32, String)> {
    let me = std::process::id();
    let mut out = Vec::new();
    for e in std::fs::read_dir("/proc").unwrap().flatten() {
        let Some(pid) = e.file_name().to_str().and_then(|n| n.parse::<u32>().ok()) else {
            continue;
        };
        let Ok(raw) = std::fs::read(format!("/proc/{pid}/cmdline")) else {
            continue;
        };
        let cmd = String::from_utf8_lossy(&raw).replace('\0', " ");
        if pid != me && cmd.contains(needle) {
            out.push((pid, cmd));
        }
    }
    out
}

fn mounts_under(dir: &Path) -> Vec<String> {
    std::fs::read_to_string("/proc/self/mountinfo")
        .unwrap()
        .lines()
        .filter(|l| {
            l.split(' ')
                .nth(4)
                .is_some_and(|m| m.starts_with(dir.to_str().unwrap()))
        })
        .map(String::from)
        .collect()
}

fn wait_until(what: &str, within: Duration, mut f: impl FnMut() -> bool) {
    let t = Instant::now();
    while t.elapsed() < within {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("{what} did not happen within {within:?}");
}

fn constellation_bin() -> PathBuf {
    std::env::var_os("CONSTELLATION_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/constellation")
        })
}

/// nextest remaps binary paths when a test runs from an extracted archive
/// (`--archive-file`/`--extract-to`): it exposes the remapped path as
/// `NEXTEST_BIN_EXE_harness` and also rewrites `CARGO_BIN_EXE_harness` in
/// the environment, so read those at runtime instead of trusting the path
/// `env!` baked in at compile time.
fn harness_bin() -> String {
    std::env::var("NEXTEST_BIN_EXE_harness")
        .or_else(|_| std::env::var("CARGO_BIN_EXE_harness"))
        .unwrap_or_else(|_| env!("CARGO_BIN_EXE_harness").to_string())
}

#[test]
#[ignore = "needs docker, FUSE and a built constellation"]
fn a_sigkilled_harness_leaves_no_daemon_and_no_mount() {
    let tmp = tempfile::tempdir().unwrap();
    let tmp_s = tmp.path().to_str().unwrap().to_string();
    let prefix = format!("htdkill-{}", std::process::id());
    let mut harness = Command::new(harness_bin())
        .args(["run", "baseline"])
        .env("TMPDIR", &tmp_s)
        .env("CONSTELLATION_BIN", constellation_bin())
        .env("CONSTELLATION_HARNESS_DOCKER_PREFIX", &prefix)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    // Mid-scenario: a daemon of the scenario is up and mounted.
    wait_until("a mounted daemon", Duration::from_secs(180), || {
        !mounts_under(tmp.path()).is_empty()
            && procs_mentioning(&tmp_s)
                .iter()
                .any(|(_, c)| c.contains(" mount "))
    });
    assert!(
        harness.try_wait().unwrap().is_none(),
        "the scenario ended before the kill"
    );

    // SAFETY: SIGKILL to our own child.
    unsafe { libc::kill(harness.id() as i32, libc::SIGKILL) };
    let _ = harness.wait();

    let result = std::panic::catch_unwind(|| {
        wait_until(
            "every daemon and reaper gone",
            Duration::from_secs(60),
            || procs_mentioning(&tmp_s).is_empty(),
        );
        wait_until("every mount gone", Duration::from_secs(30), || {
            mounts_under(tmp.path()).is_empty()
        });
    });
    for (pid, _) in procs_mentioning(&tmp_s) {
        unsafe { libc::kill(pid as i32, libc::SIGKILL) };
    }
    let _ = Command::new("docker")
        .args([
            "rm",
            "-f",
            &format!("{prefix}-floci"),
            &format!("{prefix}-toxiproxy"),
        ])
        .output();
    let _ = Command::new("docker")
        .args(["network", "rm", &prefix])
        .output();
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}
