//! `umount` of a daemon's last view waits for its clean-unmount drain, and
//! a `mount` issued meanwhile waits for it too instead of failing.
//!
//! A verification run on a loaded host found `umount NAME` printing
//! "unmounted" at once while the daemon only then started draining
//! thousands of write-back uploads; a `mount` right after attached to the
//! draining daemon, was refused, and waited for it with nothing to show,
//! past its caller's patience. Here the drain is held
//! (`CONSTELLATION_FAULT_HOLD_DRAIN_FILE`) for as long as the test needs,
//! with the write-back queue it has to drain kept from uploading before
//! (`CONSTELLATION_FAULT_HOLD_SYNC_FILE`):
//! `umount` must still be waiting (and saying what for), a `mount` issued
//! meanwhile must still be waiting rather than failing, and once the drain
//! is let go both finish — `umount` successfully, after the daemon's exit,
//! and the remount with every byte intact. What `umount` promised is
//! then in the backend: a fresh state dir that only has the backend to go
//! on reads it all back, with the old daemon gone (`kill -9`ed, had it
//! lingered).
//!
//! Needs FUSE (`/dev/fuse`, `fusermount3`); skipped, loudly, without it.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
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

fn fuse_available() -> bool {
    Path::new("/dev/fuse").exists()
        && ["/usr/bin/fusermount3", "/bin/fusermount3"]
            .iter()
            .any(|p| Path::new(p).exists())
}

/// A `constellation` with its registry, config, data and runtime dirs in
/// `dir`, no P2P endpoint, the drain held while `hold` exists and the
/// sync rounds (the write-back uploads) while `hold` + `-sync` does.
fn command(dir: &Path, hold: &Path, args: &[&str]) -> Command {
    command_env(dir, hold, args, &[])
}

fn command_env(dir: &Path, hold: &Path, args: &[&str], env: &[(&str, &str)]) -> Command {
    let mut cmd = Command::new(bin());
    cmd.envs(env.iter().copied());
    cmd.args(args)
        .env("CONSTELLATION_REGISTRY", dir.join("config/registry.toml"))
        .env("XDG_CONFIG_HOME", dir.join("config"))
        .env("XDG_DATA_HOME", dir.join("data"))
        .env("XDG_RUNTIME_DIR", dir.join("run"))
        .env("CONSTELLATION_P2P", "off")
        .env("CONSTELLATION_FAULT_HOLD_DRAIN_FILE", hold)
        .env("CONSTELLATION_FAULT_HOLD_SYNC_FILE", sync_hold(hold))
        .env_remove("CONSTELLATION_NO_DAEMONIZE")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

fn sync_hold(hold: &Path) -> PathBuf {
    PathBuf::from(format!("{}-sync", hold.display()))
}

fn finish(mut child: Child, what: &str, within: Duration) -> Output {
    let t = Instant::now();
    while child.try_wait().unwrap().is_none() {
        if t.elapsed() > within {
            let _ = child.kill();
            let out = child.wait_with_output().unwrap();
            panic!(
                "{what} still running after {within:?}; stderr: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    child.wait_with_output().unwrap()
}

fn run(dir: &Path, hold: &Path, args: &[&str]) -> Output {
    let child = command(dir, hold, args).spawn().unwrap();
    finish(
        child,
        &format!("constellation {args:?}"),
        Duration::from_secs(180),
    )
}

fn ok(out: &Output, what: &str) {
    assert!(
        out.status.success(),
        "{what} failed: {}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn p(path: &Path) -> &str {
    path.to_str().unwrap()
}

fn mounted(mnt: &Path) -> bool {
    std::fs::read_to_string("/proc/self/mountinfo")
        .unwrap()
        .lines()
        .any(|l| l.split(' ').nth(4) == Some(mnt.to_str().unwrap()))
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

fn alive(pid: u32) -> bool {
    // A zombie has exited: its files (and `daemon.lock`) are closed.
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => !stat
            .rsplit_once(')')
            .is_some_and(|(_, rest)| rest.trim_start().starts_with('Z')),
        Err(_) => false,
    }
}

/// Deterministic, incompressible-enough content for file `i`.
fn content(i: usize) -> Vec<u8> {
    let mut x = 0x9e37_79b9_7f4a_7c15u64 ^ (i as u64);
    (0..(96 * 1024 + i * 4099))
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

const FILES: usize = 24;

fn write_files(root: &Path) {
    std::fs::create_dir_all(root.join("d")).unwrap();
    for i in 0..FILES {
        std::fs::write(root.join(format!("d/f{i}")), content(i)).unwrap();
    }
}

fn check_files(root: &Path, what: &str) {
    for i in 0..FILES {
        let got = std::fs::read(root.join(format!("d/f{i}")))
            .unwrap_or_else(|e| panic!("{what}: reading d/f{i}: {e}"));
        assert!(got == content(i), "{what}: d/f{i} differs");
    }
}

/// Unmount whatever the test left mounted (a failed assertion included).
struct Cleanup {
    hold: PathBuf,
    mounts: Vec<PathBuf>,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.hold);
        let _ = std::fs::remove_file(sync_hold(&self.hold));
        for mnt in &self.mounts {
            if mounted(mnt) {
                let _ = Command::new("fusermount3").arg("-uz").arg(mnt).status();
            }
        }
    }
}

#[test]
fn umount_waits_for_the_drain_and_a_mount_meanwhile_waits_too() {
    if !fuse_available() {
        eprintln!("skipping: needs /dev/fuse and fusermount3");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let s3 = dir.join("s3");
    let state = dir.join("state");
    let mnt = dir.join("mnt");
    let fresh_mnt = dir.join("fresh-mnt");
    let hold = dir.join("hold-drain");
    std::fs::create_dir_all(&s3).unwrap();
    std::fs::create_dir_all(&mnt).unwrap();
    std::fs::create_dir_all(&fresh_mnt).unwrap();
    let _cleanup = Cleanup {
        hold: hold.clone(),
        mounts: vec![mnt.clone(), fresh_mnt.clone()],
    };

    ok(
        &run(dir, &hold, &["fs", "create", "myfs", "--s3", p(&s3)]),
        "fs create",
    );
    ok(
        &run(
            dir,
            &hold,
            &[
                "mount",
                "myfs",
                p(&mnt),
                "--state-dir",
                p(&state),
                "--write-mode",
                "back",
            ],
        ),
        "mount",
    );
    assert!(mounted(&mnt));
    // Queued, not uploaded: the drain has it all to do.
    std::fs::write(sync_hold(&hold), b"").unwrap();
    write_files(&mnt);
    let daemon_pid: u32 = std::fs::read_to_string(state.join("daemon.pid"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();

    // The drain will wait until the hold file is gone.
    std::fs::write(&hold, b"").unwrap();
    let held_marker = PathBuf::from(format!("{}.held", hold.display()));
    let umount = command(dir, &hold, &["umount", "myfs"]).spawn().unwrap();
    wait_until(
        "the daemon's drain to start",
        Duration::from_secs(60),
        || held_marker.exists(),
    );
    assert!(!mounted(&mnt), "the view is detached before the drain");
    std::thread::sleep(Duration::from_secs(3));
    let mut umount = umount;
    assert!(
        umount.try_wait().unwrap().is_none(),
        "umount returned while the daemon was still draining: {:?}",
        umount.wait_with_output().unwrap()
    );

    // A mount now must wait for the drain, not fail and not attach.
    let mut remount = command(dir, &hold, &["mount", "myfs", p(&mnt)])
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_secs(5));
    assert!(
        remount.try_wait().unwrap().is_none(),
        "a mount during the drain did not wait for it: {:?}",
        remount.wait_with_output().unwrap()
    );
    assert!(
        !mounted(&mnt),
        "nothing is mounted onto the draining daemon"
    );

    // Let the drain go: umount finishes, after the daemon has exited.
    std::fs::remove_file(sync_hold(&hold)).unwrap();
    std::fs::remove_file(&hold).unwrap();
    let out = finish(umount, "umount", Duration::from_secs(120));
    ok(&out, "umount");
    let mut stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        stderr.contains(&format!("{FILES} uploads")) && stderr.contains("drain complete"),
        "umount did not report the drain it waited for: {stderr}"
    );
    // What umount promised must survive the old daemon's sudden death.
    if alive(daemon_pid) {
        unsafe {
            libc::kill(daemon_pid as i32, libc::SIGKILL);
        }
        panic!("umount returned before the daemon (pid {daemon_pid}) exited");
    }

    // The remount that waited comes up, and says what it waited for.
    let out = finish(remount, "remount", Duration::from_secs(120));
    ok(&out, "remount after the drain");
    stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        stderr.contains("drain"),
        "the waiting mount said nothing about the drain: {stderr}"
    );
    assert!(mounted(&mnt));
    check_files(&mnt, "the remount");
    ok(&run(dir, &hold, &["umount", "myfs"]), "second umount");
    assert!(!mounted(&mnt));

    // Everything is in the backend: a node with nothing but the backend
    // (an ad-hoc mount on a fresh state dir) reads it all.
    ok(
        &run(
            dir,
            &hold,
            &[
                "mount",
                "/",
                p(&fresh_mnt),
                "--s3",
                p(&s3),
                "--state-dir",
                p(&dir.join("fresh-state")),
            ],
        ),
        "fresh mount",
    );
    check_files(&fresh_mnt, "a fresh node");
    ok(
        &run(
            dir,
            &hold,
            &["umount", "/", "--state-dir", p(&dir.join("fresh-state"))],
        ),
        "umount of the fresh mount",
    );
    assert!(!mounted(&fresh_mnt));
}

/// A drain that cannot ship everything (a queued chunk lost from the
/// node's disk before it was uploaded: `CONSTELLATION_FAULT_LOSE_CHUNKS`)
/// is not a clean unmount: `umount` waits for the daemon, then fails and
/// says what was left in the state dir. It used to return success either
/// way — before the drain, or once the daemon's socket was gone.
#[test]
fn umount_fails_when_the_drain_cannot_ship_everything() {
    if !fuse_available() {
        eprintln!("skipping: needs /dev/fuse and fusermount3");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let s3 = dir.join("s3");
    let state = dir.join("state");
    let mnt = dir.join("mnt");
    let hold = dir.join("hold-drain");
    std::fs::create_dir_all(&s3).unwrap();
    std::fs::create_dir_all(&mnt).unwrap();
    let _cleanup = Cleanup {
        hold: hold.clone(),
        mounts: vec![mnt.clone()],
    };
    let lost = content(7);
    let lost_hex = constellation_fs_core::ChunkHash::of(&lost).to_hex();
    let env = [("CONSTELLATION_FAULT_LOSE_CHUNKS", lost_hex.as_str())];
    let run_env = |args: &[&str]| {
        let child = command_env(dir, &hold, args, &env).spawn().unwrap();
        finish(
            child,
            &format!("constellation {args:?}"),
            Duration::from_secs(180),
        )
    };
    ok(
        &run_env(&["fs", "create", "myfs", "--s3", p(&s3)]),
        "fs create",
    );
    ok(
        &run_env(&[
            "mount",
            "myfs",
            p(&mnt),
            "--state-dir",
            p(&state),
            "--write-mode",
            "back",
        ]),
        "mount",
    );
    std::fs::write(mnt.join("broken"), &lost).unwrap();
    let out = run_env(&["umount", "myfs"]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "umount reported success although the drain left a chunk unshipped: {stderr}"
    );
    assert!(
        stderr.contains("without shipping everything"),
        "umount did not say what the drain left behind: {stderr}"
    );
    assert!(!mounted(&mnt));
}

/// A view still in use when it is unmounted (a file open on it) is
/// detached at once but ends only when that file is closed, and only then
/// can the daemon drain: `umount` waits for both, saying what it waits
/// for, instead of returning while the view is still being served.
#[test]
fn umount_waits_for_a_view_still_in_use() {
    if !fuse_available() {
        eprintln!("skipping: needs /dev/fuse and fusermount3");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let s3 = dir.join("s3");
    let state = dir.join("state");
    let mnt = dir.join("mnt");
    let hold = dir.join("hold-drain");
    std::fs::create_dir_all(&s3).unwrap();
    std::fs::create_dir_all(&mnt).unwrap();
    let _cleanup = Cleanup {
        hold: hold.clone(),
        mounts: vec![mnt.clone()],
    };
    ok(
        &run(dir, &hold, &["fs", "create", "myfs", "--s3", p(&s3)]),
        "fs create",
    );
    ok(
        &run(
            dir,
            &hold,
            &[
                "mount",
                "myfs",
                p(&mnt),
                "--state-dir",
                p(&state),
                "--write-mode",
                "back",
            ],
        ),
        "mount",
    );
    write_files(&mnt);
    let open = std::fs::File::open(mnt.join("d/f0")).unwrap();
    let mut umount = command(dir, &hold, &["umount", "myfs"]).spawn().unwrap();
    std::thread::sleep(Duration::from_secs(8));
    assert!(
        umount.try_wait().unwrap().is_none(),
        "umount returned while its view was still in use: {:?}",
        umount.wait_with_output().unwrap()
    );
    drop(open);
    let out = finish(umount, "umount", Duration::from_secs(120));
    ok(&out, "umount");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("still in use") && stderr.contains("drain complete"),
        "umount did not say what it waited for: {stderr}"
    );
    assert!(!mounted(&mnt));
}

/// `umount --no-wait` leaves the drain to the daemon; a second `umount`
/// meanwhile waits for it (and reports its outcome) rather than saying
/// nothing is mounted.
#[test]
fn a_second_umount_waits_for_a_drain_in_progress() {
    if !fuse_available() {
        eprintln!("skipping: needs /dev/fuse and fusermount3");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let s3 = dir.join("s3");
    let state = dir.join("state");
    let mnt = dir.join("mnt");
    let hold = dir.join("hold-drain");
    std::fs::create_dir_all(&s3).unwrap();
    std::fs::create_dir_all(&mnt).unwrap();
    let _cleanup = Cleanup {
        hold: hold.clone(),
        mounts: vec![mnt.clone()],
    };
    ok(
        &run(dir, &hold, &["fs", "create", "myfs", "--s3", p(&s3)]),
        "fs create",
    );
    ok(
        &run(
            dir,
            &hold,
            &[
                "mount",
                "myfs",
                p(&mnt),
                "--state-dir",
                p(&state),
                "--write-mode",
                "back",
            ],
        ),
        "mount",
    );
    std::fs::write(sync_hold(&hold), b"").unwrap();
    write_files(&mnt);
    std::fs::write(&hold, b"").unwrap();
    let held_marker = PathBuf::from(format!("{}.held", hold.display()));
    ok(
        &run(dir, &hold, &["umount", "myfs", "--no-wait"]),
        "umount --no-wait",
    );
    assert!(!mounted(&mnt));
    wait_until(
        "the daemon's drain to start",
        Duration::from_secs(60),
        || held_marker.exists(),
    );
    let mut again = command(dir, &hold, &["umount", "myfs"]).spawn().unwrap();
    std::thread::sleep(Duration::from_secs(3));
    assert!(
        again.try_wait().unwrap().is_none(),
        "a second umount did not wait for the drain: {:?}",
        again.wait_with_output().unwrap()
    );
    std::fs::remove_file(sync_hold(&hold)).unwrap();
    std::fs::remove_file(&hold).unwrap();
    let out = finish(again, "second umount", Duration::from_secs(120));
    ok(&out, "second umount");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("drain complete"),
        "the second umount did not report the drain: {stderr}"
    );
}
