//! `harness smoke`: the end-to-end smoke test, ported from `tests/smoke.sh`
//! (which is now a thin wrapper that runs this). Same steps, same
//! assertions, same knobs:
//!
//! - `CONSTELLATION_BIN`: the binary under test (default
//!   `${CARGO_TARGET_DIR:-target}/debug/constellation`, like `tests/lib.sh`);
//! - the optional positional backend: a local directory (default, a fresh
//!   temp dir) or `s3://bucket/prefix`, whose endpoint and credentials come
//!   from the `AWS_*` environment as before (the command inherits it; unlike
//!   [`crate::client::Client`], nothing is overridden here).
//!
//! The steps: create a fs (a second create must be refused), mount, POSIX
//! namespace ops, a 3.5 MiB multi-chunk file, partial in-place edit,
//! truncate, append, unlink-while-open, rm/rmdir, unmount, remount and
//! verify persistence, remount with a cold chunk cache, `status`, and the
//! node's `status` naming the FUSE transport the mount negotiated.

use crate::client::is_mountpoint;
use anyhow::{bail, ensure, Context, Result};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Plan 38 §5 on a real mount: `status` names the transport the mount
/// negotiated in `mounts[]` and in `fuse.mounts[]` alike, and the ops this
/// run just made through it are counted under that same `transport`
/// label. Whatever the transport (`CONSTELLATION_FUSE_TRANSPORT` may ask
/// for the ring), the three must agree; a mount that fell back says it
/// is on `/dev/fuse`.
fn transport_is_reported(status: &serde_json::Value, mnt: &Path) -> Result<()> {
    let mount = &status["mounts"][0];
    let transport = mount["transport"]
        .as_str()
        .with_context(|| format!("FAIL: status names no transport: {mount}"))?;
    ensure!(
        matches!(transport, "dev_fuse" | "uring" | "uring_zc"),
        "FAIL: unknown transport {transport:?}"
    );
    let fuse = status["fuse"]["mounts"]
        .as_array()
        .context("FAIL: status has no fuse.mounts")?;
    let ours = fuse
        .iter()
        .find(|f| f["id"] == mount["id"] && f["mountpoint"] == mount["mountpoint"])
        .with_context(|| format!("FAIL: fuse.mounts has no {}: {fuse:?}", mnt.display()))?;
    ensure!(
        ours["transport"] == transport,
        "FAIL: fuse.mounts says {} but mounts says {transport}",
        ours["transport"]
    );
    if !ours["last_fallback"].is_null() {
        ensure!(
            transport == "dev_fuse",
            "FAIL: a fallback recorded on a {transport} mount: {ours}"
        );
    }
    // The Z2b review's gap: what was asked for (the knob, else the engine
    // profile's default: `dev-fuse` under `CONSTELLATION_PROFILE=mobile`,
    // the shipped `auto` otherwise) against what the mount got. A mount
    // that asked for the ring and is on `/dev/fuse` names one of the fixed
    // reasons, and the process-wide counter holds exactly that one
    // fallback (the smoke daemon has one mount); one that asked for
    // `dev-fuse` took none.
    let env = |key: &str| {
        std::env::var(key)
            .ok()
            .map(|v| v.trim().to_ascii_lowercase())
            .filter(|v| !v.is_empty())
    };
    let asked = env("CONSTELLATION_FUSE_TRANSPORT").unwrap_or_else(|| {
        match env("CONSTELLATION_PROFILE").as_deref() {
            Some("mobile") => "dev-fuse".into(),
            _ => "auto".into(),
        }
    });
    let counted: u64 = status["fuse"]["transport_fallbacks"]
        .as_array()
        .context("FAIL: status has no fuse.transport_fallbacks")?
        .iter()
        .map(|f| f["count"].as_u64().unwrap_or(0))
        .sum();
    if asked == "dev-fuse" || asked == "dev_fuse" || transport != "dev_fuse" {
        ensure!(
            ours["last_fallback"].is_null() && counted == 0,
            "FAIL: asked for {asked}, got {transport}, yet a fallback is recorded: {}",
            status["fuse"]
        );
    } else {
        let reason = ours["last_fallback"]["reason"].as_str().with_context(|| {
            format!("FAIL: asked for {asked}, got dev_fuse, and no last_fallback says why: {ours}")
        })?;
        ensure!(
            [
                "no_io_uring_feature",
                "kernel_not_offered",
                "cluster_locks",
                "handover_capable",
                "ring_setup_failed"
            ]
            .contains(&reason),
            "FAIL: unknown fallback reason {reason:?}"
        );
        ensure!(
            counted == 1,
            "FAIL: transport_fallbacks must count this mount's one fallback: {}",
            status["fuse"]
        );
        println!("   asked for {asked}, on dev_fuse: {reason}");
    }
    let series = status["vfs_ops"]["series"]
        .as_array()
        .context("FAIL: status has no vfs_ops.series")?;
    let fuse_rows: Vec<&serde_json::Value> =
        series.iter().filter(|s| s["frontend"] == "fuse").collect();
    ensure!(!fuse_rows.is_empty(), "FAIL: no FUSE op was counted");
    ensure!(
        fuse_rows.iter().all(|s| s["transport"] == transport),
        "FAIL: FUSE op series not all labelled transport={transport}: {fuse_rows:?}"
    );
    Ok(())
}

/// `tests/lib.sh`'s `say`.
fn say(msg: &str) {
    println!("== {msg}");
}

fn bin() -> PathBuf {
    std::env::var_os("CONSTELLATION_BIN")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let target = std::env::var_os("CARGO_TARGET_DIR")
                .filter(|v| !v.is_empty())
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("target"));
            target.join("debug/constellation")
        })
}

/// The mount under test, driven like `fs_mount`/`fs_unmount` in `lib.sh`:
/// a `--foreground` child whose log is appended to `mount.log`.
struct Mount {
    bin: PathBuf,
    backend: String,
    mnt: PathBuf,
    state: PathBuf,
    log: PathBuf,
    child: Option<Child>,
}

impl Mount {
    fn cmd(&self, args: &[&str]) -> Command {
        let mut c = Command::new(&self.bin);
        c.args(args);
        c
    }

    fn mount(&mut self) -> Result<()> {
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)?;
        let child = self
            .cmd(&["mount", "/"])
            // The snapshot scheduler is driven by hand here (`snapshot
            // sched run`, `policy_round_trip`): its periodic tick, an hour
            // away, never races an assertion.
            .env("CONSTELLATION_SNAPSCHED_TICK_MS", "3600000")
            .arg(&self.mnt)
            .args(["--s3", &self.backend, "--state-dir"])
            .arg(&self.state)
            .arg("--foreground")
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()
            .with_context(|| format!("spawning {}", self.bin.display()))?;
        self.child = Some(child);
        for _ in 0..100 {
            if is_mountpoint(&self.mnt) {
                return Ok(());
            }
            if let Some(st) = self.child.as_mut().unwrap().try_wait()? {
                self.child = None;
                bail!("FAIL: mount process died ({st})");
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        bail!("FAIL: mount did not appear")
    }

    fn unmount(&mut self) {
        let ok = |prog: &str, mnt: &Path| {
            Command::new(prog)
                .arg("-u")
                .arg(mnt)
                .stderr(Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
        };
        let _ = ok("fusermount3", &self.mnt) || ok("fusermount", &self.mnt);
        if let Some(mut child) = self.child.take() {
            // `wait "$MOUNT_PID"` in lib.sh, but bounded.
            let deadline = Instant::now() + Duration::from_secs(120);
            while Instant::now() < deadline {
                if matches!(child.try_wait(), Ok(Some(_))) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// `fs_cleanup`: unmount, dump the mount log if the run failed, remove the
/// work dir.
struct Cleanup {
    mount: Mount,
    work: PathBuf,
    passed: bool,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        self.mount.unmount();
        if !self.passed {
            if let Ok(log) = fs::read_to_string(&self.mount.log) {
                if !log.is_empty() {
                    println!("--- mount log ---");
                    print!("{log}");
                }
            }
        }
        let _ = fs::remove_dir_all(&self.work);
    }
}

fn cmp(a: &Path, b: &Path) -> Result<()> {
    let (x, y) = (fs::read(a)?, fs::read(b)?);
    ensure!(
        x == y,
        "FAIL: {} and {} differ (sizes {} vs {})",
        a.display(),
        b.display(),
        x.len(),
        y.len()
    );
    Ok(())
}

/// `dd if=/dev/urandom` for `len` bytes.
fn random_bytes(len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    fs::File::open("/dev/urandom")?.read_exact(&mut buf)?;
    Ok(buf)
}

/// `printf XYZ | dd of=f bs=1 seek=N conv=notrunc`.
fn overwrite_at(path: &Path, offset: u64, data: &[u8]) -> Result<()> {
    fs::OpenOptions::new()
        .write(true)
        .open(path)?
        .write_all_at(data, offset)?;
    Ok(())
}

/// `echo tail >> f`.
fn append(path: &Path, data: &[u8]) -> Result<()> {
    fs::OpenOptions::new()
        .append(true)
        .open(path)?
        .write_all(data)?;
    Ok(())
}

/// `$(cat f)`: content with trailing newlines stripped.
fn cat_trimmed(path: &Path) -> Result<String> {
    Ok(fs::read_to_string(path)?.trim_end_matches('\n').to_string())
}

pub fn run(backend: Option<String>) -> Result<()> {
    // `fs_setup`: mktemp -d /tmp/constellation-test.XXXXXX.
    let base = if Path::new("/tmp").is_dir() {
        PathBuf::from("/tmp")
    } else {
        std::env::temp_dir()
    };
    let work = tempfile::Builder::new()
        .prefix("constellation-test.")
        .tempdir_in(base)?
        .keep();
    let backend = backend
        .filter(|b| !b.is_empty())
        .unwrap_or_else(|| work.join("backend").display().to_string());
    let mnt = work.join("mnt");
    fs::create_dir_all(&mnt)?;
    let mut cleanup = Cleanup {
        mount: Mount {
            bin: bin(),
            backend,
            mnt: mnt.clone(),
            state: work.join("state"),
            log: work.join("mount.log"),
            child: None,
        },
        work: work.clone(),
        passed: false,
    };
    smoke(&mut cleanup.mount, &work)?;
    cleanup.passed = true;
    println!("SMOKE TEST PASSED");
    Ok(())
}

/// Plan 32 M5c on the mounted filesystem: a 2 MiB file only `/dir@m2`
/// keeps (written after `m1`, removed before `m3`), then `snapshot space
/// --verify`, the `snapshot ls` size columns and footer, `-p`, and `snapshot
/// delete --dry-run`'s reclaim estimate — all from the node's accounting
/// index, which the first of these requests builds (`auto`).
fn snapshot_space(m: &Mount, state: &str) -> Result<()> {
    say("snapshot sizes: space --verify, ls USED/WRITTEN/REFER, delete --dry-run");
    let mnt = &m.mnt;
    // Success and stdout; a failure's stderr is in the error.
    let run = |args: &[&str]| -> Result<(bool, String)> {
        let mut all = vec!["snapshot"];
        all.extend_from_slice(args);
        all.extend(["--state-dir", state]);
        let out = m.cmd(&all).output()?;
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        if !out.status.success() {
            println!(
                "snapshot {args:?} failed ({}): {}",
                out.status,
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok((out.status.success(), stdout))
    };
    let create = |selector: &str| -> Result<()> {
        let (ok, text) = run(&["create", selector])?;
        ensure!(ok, "FAIL: snapshot create {selector}: {text}");
        Ok(())
    };
    create("/dir@m1")?;
    fs::write(mnt.join("dir/only-m2.bin"), random_bytes(2 << 20)?)?;
    create("/dir@m2")?;
    fs::remove_file(mnt.join("dir/only-m2.bin"))?;
    // Publishes the removal: the live refresh `--verify` forces sees it.
    create("/dir@m3")?;

    let (ok, text) = run(&["space", "--verify"])?;
    print!("{text}");
    ensure!(
        ok && text.contains("verify: 0 mismatches")
            && text.contains("live data (logical)")
            && text.contains("snapshots, total (usedbysnapshots)")
            && text.contains("awaiting GC")
            && text.contains("note: per-snapshot USED values do not sum")
            && text.contains("logical bytes, pre-compression"),
        "FAIL: snapshot space --verify: {text}"
    );
    let (ok, text) = run(&["space", "/dir"])?;
    ensure!(
        ok && text.starts_with("space of /dir:") && text.contains("2.0M"),
        "FAIL: snapshot space /dir: {text}"
    );

    // Sizes are ready now (`--verify` brought the index current), but a
    // snapshot change since would answer `building`: poll briefly.
    let deadline = Instant::now() + Duration::from_secs(30);
    let table = loop {
        let (ok, text) = run(&["ls", "/dir"])?;
        ensure!(ok, "FAIL: snapshot ls: {text}");
        if !text.contains("building (") || Instant::now() > deadline {
            break text;
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    print!("{table}");
    let m2 = table
        .lines()
        .find(|l| l.starts_with("/dir@m2 "))
        .with_context(|| format!("FAIL: no /dir@m2 in snapshot ls: {table}"))?;
    let cells: Vec<&str> = m2
        .split("  ")
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .collect();
    // NAME CREATED ORIGIN USED WRITTEN REFER …
    ensure!(
        cells.get(3) == Some(&"2.0M") && cells.get(4) == Some(&"2.0M"),
        "FAIL: /dir@m2's USED and WRITTEN: {table}"
    );
    ensure!(
        table
            .lines()
            .last()
            .is_some_and(|l| l.starts_with("USED/WRITTEN/REFER as of commit ")
                && l.ends_with("· logical bytes, pre-compression")),
        "FAIL: snapshot ls footer: {table}"
    );
    // `-p`: exact bytes, no footer; `-s used` puts the biggest last.
    let (ok, exact) = run(&["ls", "/dir", "-p", "-o", "name,used", "-s", "used"])?;
    ensure!(
        ok && exact
            .lines()
            .last()
            .is_some_and(|l| l.split_whitespace().eq(["/dir@m2", "2097152"])),
        "FAIL: snapshot ls -p -s used: {exact}"
    );

    let (ok, text) = run(&["delete", "--dry-run", "/dir@m1%m3"])?;
    print!("{text}");
    ensure!(
        ok && text.contains("would delete /dir@m2")
            && text.contains("would reclaim ≈ 2.0M in 2 chunks (after GC)"),
        "FAIL: snapshot delete --dry-run: {text}"
    );
    let (ok, text) = run(&["ls", "/dir", "-o", "name"])?;
    ensure!(
        ok && text.contains("/dir@m2"),
        "FAIL: a dry run deleted: {text}"
    );
    Ok(())
}

/// `snapshot policy set|show|ls|pause|resume|rm` and `snapshot ls
/// --orphaned` against the mounted `/dir` (plan 32 M2), which holds one
/// held manual snapshot: the policy expires nothing, so `set` writes
/// without asking. Leaves `/dir` without a policy.
fn policy_round_trip(m: &Mount, state: &str) -> Result<()> {
    say("snapshot policy set/show/ls/pause/resume/rm, snapshot sched status/run, snapshot ls --orphaned");
    let run = |args: &[&str]| -> Result<(std::process::ExitStatus, String, String)> {
        let out = m
            .cmd(args)
            .args(["--state-dir", state])
            .stdin(Stdio::null())
            .output()?;
        Ok((
            out.status,
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        ))
    };
    let (st, _, err) = run(&["snapshot", "policy", "set", "/dir", "1h:1d 7m:1d"])?;
    ensure!(
        st.code() == Some(2) && err.contains("1h:1d 7m:1d\n      ^ "),
        "FAIL: policy set of an invalid policy ({st}): {err}"
    );
    fs::write(m.mnt.join("dir/policy-file"), "x")?;
    let (st, _, err) = run(&["snapshot", "policy", "set", "/dir/policy-file", "1h:1d"])?;
    ensure!(
        !st.success() && err.contains("not a directory"),
        "FAIL: policy set on a file ({st}): {err}"
    );
    fs::remove_file(m.mnt.join("dir/policy-file"))?;
    let (st, out, err) = run(&["snapshot", "policy", "set", "/dir", "30s:10m", "--dry-run"])?;
    ensure!(
        st.success()
            && out.contains("/dir (ino ")
            && out.contains("(no policy) -> 30s:10m")
            && out.contains("creates every 30s; will expire nothing; ")
            && out.contains("warning: sub-minute tier")
            && out.contains("dry run: nothing written"),
        "FAIL: policy set --dry-run ({st}): {out}{err}"
    );
    let (st, _, err) = run(&["snapshot", "policy", "show", "/dir"])?;
    ensure!(
        !st.success() && err.contains("no snapshot policy"),
        "FAIL: a dry run wrote a policy ({st}): {err}"
    );
    let (st, out, err) = run(&["snapshot", "policy", "set", "/dir", "1d:7d   1h:1d"])?;
    ensure!(
        st.success() && out.contains("/dir: snapshot policy set: 1h:1d 1d:7d"),
        "FAIL: policy set ({st}): {out}{err}"
    );
    let (st, out, err) = run(&["snapshot", "policy", "show", "/dir"])?;
    ensure!(
        st.success()
            && out.contains("  policy:  1h:1d 1d:7d\n")
            && out.contains("  state:   armed\n")
            && out.contains("would expire 0 of 1 snapshots"),
        "FAIL: policy show ({st}): {out}{err}"
    );
    let ls_row = |want: &str, autos: &str| -> Result<()> {
        let (st, out, err) = run(&["snapshot", "policy", "ls"])?;
        let row: Vec<String> = out
            .lines()
            .find(|l| l.starts_with("/dir "))
            .map(|l| {
                l.split("  ")
                    .filter(|c| !c.is_empty())
                    .map(|c| c.trim().to_string())
                    .collect()
            })
            .unwrap_or_default();
        ensure!(
            st.success()
                && out.starts_with("PATH ")
                && row.len() == 5
                && row[3] == want
                && row[4] == autos,
            "FAIL: policy ls, want {want} with {autos} auto ({st}): {out}{err}"
        );
        Ok(())
    };
    ls_row("armed", "0")?;

    // The scheduler (plan 32 M3): /dir is due, nobody leads yet.
    let sched_row = |out: &str| -> String {
        out.lines()
            .find(|l| l.starts_with("/dir "))
            .map(|l| l.split_whitespace().collect::<Vec<_>>().join(" "))
            .unwrap_or_default()
    };
    let (st, out, err) = run(&["snapshot", "sched", "status"])?;
    ensure!(
        st.success()
            && out.contains(": not leading (scheduler enabled, tick 3600000 ms")
            && out.contains("\nPATH ")
            && sched_row(&out).starts_with("/dir 1h:1d 1d:7d due now (auto-"),
        "FAIL: snapshot sched status, due ({st}): {out}{err}"
    );
    let (st, out, err) = run(&["snapshot", "sched", "run", "--dry-run"])?;
    ensure!(
        st.success()
            && out.starts_with("dry run: no lease taken, nothing created\n")
            && sched_row(&out).contains(" would_create -"),
        "FAIL: snapshot sched run --dry-run ({st}): {out}{err}"
    );
    ls_row("armed", "0")?;
    let (st, out, err) = run(&["snapshot", "sched", "run"])?;
    let created = sched_row(&out);
    let auto_name = created.split(' ').nth(1).unwrap_or_default().to_string();
    ensure!(
        st.success()
            && out.starts_with("this node leads the scheduler\n")
            && auto_name.starts_with("auto-")
            && created.contains(" created "),
        "FAIL: snapshot sched run ({st}): {out}{err}"
    );
    let (st, out, err) = run(&["snapshot", "sched", "status"])?;
    ensure!(
        st.success()
            && out.contains(": leader (scheduler enabled")
            && out.contains("created 1  skipped-empty 0  create-failed 0")
            && sched_row(&out).starts_with("/dir 1h:1d 1d:7d armed "),
        "FAIL: snapshot sched status, after a run ({st}): {out}{err}"
    );
    // The bucket is covered: a second run has nothing to do.
    let (st, out, err) = run(&["snapshot", "sched", "run"])?;
    ensure!(
        st.success() && out.ends_with("nothing due\n"),
        "FAIL: snapshot sched run, covered ({st}): {out}{err}"
    );
    ls_row("armed", "1")?;

    let (st, out, err) = run(&["snapshot", "policy", "pause", "/dir"])?;
    ensure!(
        st.success() && out.contains("/dir: paused (1h:1d 1d:7d; paused)"),
        "FAIL: policy pause ({st}): {out}{err}"
    );
    ls_row("paused", "1")?;
    let (st, out, err) = run(&["snapshot", "policy", "resume", "/dir"])?;
    ensure!(
        st.success() && out.contains("/dir: armed (1h:1d 1d:7d)"),
        "FAIL: policy resume ({st}): {out}{err}"
    );
    ls_row("armed", "1")?;
    let (st, out, err) = run(&["snapshot", "policy", "rm", "/dir", "--expire", "--yes"])?;
    ensure!(
        !st.success() && err.contains("not available until expiry ships"),
        "FAIL: policy rm --expire ({st}): {out}{err}"
    );
    // No terminal, no answer: declined.
    let (st, _, err) = run(&["snapshot", "policy", "rm", "/dir"])?;
    ensure!(
        !st.success() && err.contains("[y/N]") && err.contains("nothing removed"),
        "FAIL: policy rm without --yes ({st}): {err}"
    );
    ls_row("armed", "1")?;
    let (st, out, err) = run(&["snapshot", "policy", "rm", "/dir", "--yes"])?;
    ensure!(
        st.success()
            && out.contains("/dir: snapshot policy removed; 1 auto snapshot(s) kept (orphaned)"),
        "FAIL: policy rm ({st}): {out}{err}"
    );
    // The scheduler's snapshot outlives its policy, orphaned.
    ls_row("orphaned", "1")?;
    let (st, out, err) = run(&["snapshot", "ls", "--orphaned"])?;
    ensure!(
        st.success()
            && out.contains(&format!("/dir@{auto_name}"))
            && out.contains("auto (orphaned)"),
        "FAIL: snapshot ls --orphaned ({st}): {out}{err}"
    );
    // Deleted by hand, the stream is gone, and the steps below see /dir's
    // snapshots as they were.
    let (st, out, err) = run(&["snapshot", "delete", &format!("/dir@{auto_name}")])?;
    ensure!(
        st.success(),
        "FAIL: snapshot delete of {auto_name} ({st}): {out}{err}"
    );
    let (st, out, err) = run(&["snapshot", "policy", "ls"])?;
    ensure!(
        st.success() && out == "no snapshot policies\n",
        "FAIL: policy ls after rm ({st}): {out}{err}"
    );
    let (st, out, err) = run(&["snapshot", "ls", "--orphaned"])?;
    ensure!(
        st.success() && out == "no snapshots\n",
        "FAIL: snapshot ls --orphaned ({st}): {out}{err}"
    );
    Ok(())
}

fn smoke(m: &mut Mount, work: &Path) -> Result<()> {
    let mnt = m.mnt.clone();
    let backend = m.backend.clone();
    ensure!(
        m.bin.is_file(),
        "constellation binary not found at {} (build it, or set CONSTELLATION_BIN)",
        m.bin.display()
    );

    say(&format!("fs create + doctor ({backend})"));
    let out = m
        .cmd(&[
            "fs",
            "create",
            "tests",
            "--s3",
            &backend,
            "--chunk-size",
            "1048576",
            "--compression",
            "zstd:3",
        ])
        .status()?;
    ensure!(out.success(), "fs create failed ({out})");
    let out = m.cmd(&["doctor", "tests", "--s3", &backend]).status()?;
    ensure!(out.success(), "doctor failed ({out})");
    let second = m
        .cmd(&["fs", "create", "tests", "--s3", &backend])
        .stderr(Stdio::null())
        .status()?;
    ensure!(!second.success(), "FAIL: double create succeeded");

    say("mount");
    m.mount()?;

    say("basic namespace ops");
    fs::create_dir_all(mnt.join("dir/sub"))?;
    fs::write(mnt.join("dir/hello.txt"), "hello constellation\n")?;
    ensure!(cat_trimmed(&mnt.join("dir/hello.txt"))? == "hello constellation");
    std::os::unix::fs::symlink("hello.txt", mnt.join("dir/link"))?;
    ensure!(fs::read_link(mnt.join("dir/link"))? == Path::new("hello.txt"));
    ensure!(cat_trimmed(&mnt.join("dir/link"))? == "hello constellation");
    fs::rename(mnt.join("dir/hello.txt"), mnt.join("dir/sub/renamed.txt"))?;
    ensure!(cat_trimmed(&mnt.join("dir/sub/renamed.txt"))? == "hello constellation");
    let names: Vec<_> = fs::read_dir(mnt.join("dir"))?
        .map(|e| e.map(|e| e.file_name()))
        .collect::<std::io::Result<_>>()?;
    ensure!(
        !names.iter().any(|n| n == "hello.txt"),
        "FAIL: old name still present"
    );

    say("snapshot policy check (local, caret, truncated, --json, --against a held snapshot)");
    let state = m.state.display().to_string();
    let local = m
        .cmd(&["snapshot", "policy", "check", "1d:7d 1h:1d"])
        .output()?;
    let text = String::from_utf8_lossy(&local.stdout);
    ensure!(
        local.status.success()
            && text.contains("ok: 1h:1d 1d:7d")
            && text.contains("steady-state bound: <= 31")
            && text.contains("simulated: "),
        "FAIL: policy check: {text}"
    );
    let bad = m
        .cmd(&["snapshot", "policy", "check", "1h:1d 7m:1d"])
        .output()?;
    let err = String::from_utf8_lossy(&bad.stderr);
    ensure!(
        bad.status.code() == Some(2) && err.contains("1h:1d 7m:1d\n      ^ "),
        "FAIL: policy check of an invalid policy ({}): {err}",
        bad.status
    );
    // A policy that keeps everything stops early, and says where.
    let dense = m.cmd(&["snapshot", "policy", "check", "1m:*"]).output()?;
    let text = String::from_utf8_lossy(&dense.stdout);
    ensure!(
        dense.status.success()
            // 4095 minutes from a `now` that is not on the minute.
            && text.contains("simulated: at least 4096 snapshots after 2d20h1")
            && text.contains("(stopped early"),
        "FAIL: policy check of a keep-everything policy: {text}"
    );
    // `--json` reports a parse error as JSON with or without `--against`
    // (the expression is checked before any daemon is asked).
    let bad_json = m
        .cmd(&[
            "snapshot",
            "policy",
            "check",
            "1h:1d 7m:1d",
            "--json",
            "--against",
            "/dir",
            "--state-dir",
            &state,
        ])
        .output()?;
    let parsed: serde_json::Value = serde_json::from_slice(&bad_json.stdout)
        .context("FAIL: policy check --json --against: not JSON")?;
    ensure!(
        bad_json.status.code() == Some(2)
            && parsed["ok"] == false
            && parsed["error"]["offset"] == 6,
        "FAIL: policy check --json of an invalid policy ({}): {parsed}",
        bad_json.status
    );
    let held = m
        .cmd(&[
            "snapshot",
            "create",
            "/dir@smoke",
            "--by",
            "csi:smoke",
            "--state-dir",
            &state,
        ])
        .status()?;
    ensure!(held.success(), "FAIL: snapshot create --by ({held})");
    let against = m
        .cmd(&[
            "snapshot",
            "policy",
            "check",
            "1h:1d",
            "--against",
            "/dir",
            "--state-dir",
            &state,
        ])
        .output()?;
    let text = String::from_utf8_lossy(&against.stdout);
    ensure!(
        against.status.success()
            && text.contains("would expire 0 of 1 snapshots")
            && text.contains("held: csi"),
        "FAIL: policy check --against ({}): {text}{}",
        against.status,
        String::from_utf8_lossy(&against.stderr)
    );

    policy_round_trip(m, &state)?;

    say("multi-chunk file (3.5 MiB across 1 MiB chunks)");
    let reference = work.join("random.bin");
    let mut data = random_bytes(3 << 20)?;
    data.extend(random_bytes(512 << 10)?);
    fs::write(&reference, &data)?;
    fs::copy(&reference, mnt.join("dir/random.bin"))?;
    cmp(&reference, &mnt.join("dir/random.bin"))?;

    say("partial in-place edit");
    overwrite_at(&mnt.join("dir/random.bin"), 2_000_000, b"XYZ")?;
    overwrite_at(&reference, 2_000_000, b"XYZ")?;
    cmp(&reference, &mnt.join("dir/random.bin"))?;

    say("truncate");
    fs::OpenOptions::new()
        .write(true)
        .open(mnt.join("dir/random.bin"))?
        .set_len(1_500_000)?;
    fs::OpenOptions::new()
        .write(true)
        .open(&reference)?
        .set_len(1_500_000)?;
    cmp(&reference, &mnt.join("dir/random.bin"))?;

    say("append");
    append(&mnt.join("dir/random.bin"), b"tail\n")?;
    append(&reference, b"tail\n")?;
    cmp(&reference, &mnt.join("dir/random.bin"))?;

    snapshot_space(m, &state)?;

    say("unlink while open");
    fs::write(mnt.join("orphan.txt"), "orphan data\n")?;
    let mut held = fs::File::open(mnt.join("orphan.txt"))?;
    fs::remove_file(mnt.join("orphan.txt"))?;
    ensure!(!mnt.join("orphan.txt").exists());
    let mut text = String::new();
    held.seek(SeekFrom::Start(0))?;
    held.read_to_string(&mut text)?;
    ensure!(
        text.trim_end_matches('\n') == "orphan data",
        "FAIL: unlinked-but-open file read back {text:?}"
    );
    drop(held);

    say("rm/rmdir");
    fs::remove_file(mnt.join("dir/link"))?;
    fs::remove_file(mnt.join("dir/sub/renamed.txt"))?;
    fs::remove_dir(mnt.join("dir/sub"))?;
    ensure!(
        fs::remove_dir(mnt.join("dir")).is_err(),
        "FAIL: rmdir non-empty succeeded"
    );

    say("unmount");
    m.unmount();

    say("remount and verify persistence");
    m.mount()?;
    cmp(&reference, &mnt.join("dir/random.bin"))?;
    ensure!(mnt.join("dir").is_dir());
    ensure!(!mnt.join("orphan.txt").exists());

    say("cold cache read (fresh chunk cache, data pulled from backend)");
    m.unmount();
    let _ = fs::remove_dir_all(m.state.join("cache"));
    m.mount()?;
    cmp(&reference, &mnt.join("dir/random.bin"))?;

    say("status");
    let status = m.cmd(&["status", "--s3", &backend]).output()?;
    ensure!(
        status.status.success(),
        "status failed ({}): {}",
        status.status,
        String::from_utf8_lossy(&status.stderr)
    );
    ensure!(
        String::from_utf8_lossy(&status.stdout).contains("uuid"),
        "FAIL: status output has no uuid"
    );

    say("node status: the FUSE transport (plan 38 §5)");
    let node = m
        .cmd(&["status", &backend, "--state-dir"])
        .arg(&m.state)
        .output()?;
    ensure!(
        node.status.success(),
        "node status failed ({}): {}",
        node.status,
        String::from_utf8_lossy(&node.stderr)
    );
    transport_is_reported(&serde_json::from_slice(&node.stdout)?, &mnt)?;

    m.unmount();
    Ok(())
}
