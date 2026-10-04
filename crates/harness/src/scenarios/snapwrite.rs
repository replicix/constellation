//! Plan 32 §11 "Compliance and performance": what an active snapshot
//! policy costs the sequencer's writes (`snapsched-write-overhead`).
//!
//! Two nodes on the scenario's docker S3. `b` creates the filesystem, takes
//! the root lease at mount and keeps it: it is the sequencer, it writes the
//! tree and it runs fio. It mounts with `CONSTELLATION_SNAPSCHED=0`, so the
//! scheduler can only lead on `a` — the plan's shape, a leader that is not
//! the lease holder and creates every snapshot through the holder-side batch
//! forwarded over P2P. Everything else (tick, TTLs, write mode, cache) is the
//! product default.
//!
//! 1. `b` writes a 100k-file tree under `/proj/tree` (1,000 directories × 100
//!    small files, one chunk each) under `--write-mode back`, the documented
//!    way to bulk-import, then switches back to `through` (draining the
//!    uploads) and the cluster settles (journal drained, `a` sees the last
//!    file).
//! 2. [`RUNS`] pairs of fio runs on `b`, alternating so host-load drift
//!    hits both legs alike: **off** (no policy anywhere), then **on**
//!    (`10s:1h` on `/proj`, set through `a`; the run starts once `a` leads
//!    and the first auto snapshot of the tree has landed, and the policy is
//!    removed afterwards — `policy rm`, the snapshots kept as orphans). Each
//!    run is one fresh file under `/proj/fio`: sequential 1 MiB writes,
//!    `psync`, [`FIO_SIZE`] bytes or [`FIO_RUNTIME_S`] seconds, whichever
//!    comes first, an `fsync` every [`FSYNC_EVERY`] writes and one at the
//!    end; the file is removed after the run. The periodic `fsync` is what
//!    makes the policy do work during a run: a file being written is
//!    published only at `fsync` or `close`, so without it the tree would
//!    not change between the run's start and its end, every bucket inside
//!    the run would be skipped as empty (the default `skip-empty`), and a
//!    snapshot would land inside a run only when a bucket happened to fall
//!    between the final `fsync` and fio's exit.
//! 3. Printed: MB/s per run (fio's own `write.bw_bytes`), the median per leg
//!    and the regression of the medians, plus the auto snapshots each run
//!    saw created.
//!
//! **Asserted** (the invariants only): the root lease's `(holder, epoch)`
//! is `b`'s and never changes — read before the first run, after every run
//! and once a second during every run; `a` leads and creates at least one
//! snapshot during every "on" run; no auto snapshot is created during an
//! "off" run. The plan's ≤ 3 % throughput bound is **judged on the printed
//! medians, not asserted**: on a shared host the run-to-run spread of one
//! leg alone is often larger than 3 %, so a hard bound would fail on noise
//! as often as on a regression.
//!
//! `CONSTELLATION_HARNESS_WOH_FILES`, `_FIO_SIZE`, `_FIO_RUNTIME_S` and
//! `_RUNS` override the defaults (e.g. a quick look at a smaller tree).

use super::m11::dump_logs_on_failure;
use super::m9::node_id;
use super::snapsched::{is_leader, now_ms, polled, root_lease, sched_status, stat};
use super::{eventually, journal_drained, set_xattr, setup, ts, wait_for_p2p};
use crate::client::Client;
use crate::s3env::BUCKET;
use anyhow::{ensure, Context, Result};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

const POLICY: &str = "10s:1h";
const POLICY_XATTR: &str = "user.constellation.snapshots";
const DIRS: usize = 1_000;
/// Files in the tree (`DIRS` directories share them evenly).
const FILES: usize = 100_000;
/// fio's `--size` per run: the cap on what one run writes (floci keeps
/// every byte in memory until the bucket goes, so the scenario's whole
/// footprint is about `2 × RUNS × FIO_SIZE`).
const FIO_SIZE: &str = "2G";
/// fio's `--runtime` per run (no `time_based`: a run ends at the size or
/// at this, whichever comes first).
const FIO_RUNTIME_S: u64 = 30;
const RUNS: usize = 3;
/// fio's `--fsync`: an `fsync` every this many 1 MiB writes, so the file's
/// growth is published (and the tree changes) a few times per bucket.
const FSYNC_EVERY: u64 = 256;

fn knob<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(format!("CONSTELLATION_HARNESS_WOH_{name}"))
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

pub fn snapsched_write_overhead(_seed: u64) -> Result<()> {
    const NAME: &str = "snapsched-write-overhead";
    let (env, root) = setup(NAME)?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/woh-{}", ts());
    let node = |name: &str| -> Result<Client> {
        Ok(Client::new(root.path(), name, &env.endpoint, &backend)?.with_own_node_key())
    };
    let mut a = node("a")?;
    let mut b = node("b")?.with_env("CONSTELLATION_SNAPSCHED", "0");
    b.fs_create()?;
    b.mount()?;
    a.mount()?;
    let mut clients = [a, b];
    let result = body(&mut clients);
    dump_logs_on_failure(NAME, &clients, &result);
    for c in clients.iter_mut().rev() {
        if c.is_mounted() {
            let _ = c.unmount();
        }
    }
    result
}

/// One fio run's outcome.
struct Run {
    on: bool,
    mb_s: f64,
    /// Auto snapshots of `/proj` created while fio ran.
    snapshots: usize,
}

fn body(clients: &mut [Client; 2]) -> Result<()> {
    let (a, b) = (&clients[0], &clients[1]);
    let files: usize = knob("FILES", FILES);
    let fio_size: String = knob("FIO_SIZE", FIO_SIZE.to_string());
    let runtime: u64 = knob("FIO_RUNTIME_S", FIO_RUNTIME_S);
    let runs: usize = knob("RUNS", RUNS);
    let b_id = node_id(b)?;
    eventually("b holds the root lease", Duration::from_secs(30), || {
        let (holder, _) = root_lease(b)?;
        ensure!(holder == b_id, "holder {holder}, want b ({b_id})");
        Ok(())
    })?;
    wait_for_p2p(&[a, b])?;
    let status = sched_status(b)?;
    ensure!(
        status["enabled"] == false,
        "b's scheduler should be disabled: {status}"
    );

    // 1. The tree, from b, eight writers at once, under `--write-mode
    // back` as the docs advise for a bulk import (one S3 round trip per
    // small file's close otherwise); switching back to `through`, the
    // default the fio runs measure, drains the upload queue.
    let tree = b.mnt.join("proj/tree");
    let t0 = Instant::now();
    b.set_write_mode("back")?;
    let per_dir = files.div_ceil(DIRS);
    std::thread::scope(|s| -> Result<()> {
        let workers: Vec<_> = (0..8)
            .map(|w| {
                let tree = &tree;
                s.spawn(move || -> Result<()> {
                    for d in (w..DIRS).step_by(8) {
                        let dir = tree.join(format!("d{d:04}"));
                        std::fs::create_dir_all(&dir)?;
                        for f in 0..per_dir.min(files.saturating_sub(d * per_dir)) {
                            std::fs::write(dir.join(format!("f{f:03}")), format!("{d}/{f}\n"))?;
                        }
                    }
                    Ok(())
                })
            })
            .collect();
        for w in workers {
            w.join().expect("tree writer panicked")?;
        }
        Ok(())
    })
    .context("writing the tree")?;
    let made = t0.elapsed();
    b.set_write_mode("through")?;
    eventually("b's journal drains", Duration::from_secs(600), || {
        journal_drained(b)
    })?;
    let last = format!("proj/tree/d{:04}/f{:03}", DIRS - 1, per_dir - 1);
    eventually("a sees the whole tree", Duration::from_secs(300), || {
        ensure!(a.mnt.join(&last).is_file(), "{last} not on a yet");
        Ok(())
    })?;
    eprintln!(
        "    snapsched-write-overhead: tree of {files} files in {DIRS} dirs written in {made:.1?}, \
         settled after {:.1?}",
        t0.elapsed()
    );
    std::fs::create_dir_all(b.mnt.join("proj/fio"))?;
    pin_lease(clients, b_id)?;
    let (a, b) = (&clients[0], &clients[1]);

    let lease_before = root_lease(b)?;
    eprintln!(
        "    snapsched-write-overhead: root lease before: holder {} epoch {}",
        lease_before.0, lease_before.1
    );
    let mut results: Vec<Run> = Vec::new();
    for i in 0..runs {
        for on in [false, true] {
            if on {
                arm(a)?;
            }
            let before = polled(a)?.len();
            let started = now_ms();
            let (bytes, secs, bw) = fio_watched(
                b,
                &format!("run{i}-{}", leg(on)),
                &fio_size,
                runtime,
                lease_before,
            )?;
            let ended = now_ms();
            let snapshots = polled(a)?
                .iter()
                .filter(|s| s.created >= started && s.created <= ended)
                .count();
            if on {
                disarm(a)?;
                ensure!(
                    snapshots > 0,
                    "no auto snapshot was created during policy run {i} ({secs:.1} s)"
                );
            } else {
                ensure!(
                    snapshots == 0 && polled(a)?.len() == before,
                    "{snapshots} auto snapshot(s) appeared during the no-policy run {i}"
                );
            }
            let lease = root_lease(b)?;
            ensure!(
                lease == lease_before,
                "the root lease moved: (holder, epoch) {lease_before:?} -> {lease:?}"
            );
            let mb_s = bw as f64 / 1e6;
            eprintln!(
                "    snapsched-write-overhead: run {i} policy {:<3} {mb_s:8.1} MB/s ({} MiB in \
                 {secs:.1} s; wall incl. end fsync {:.1} MB/s), {snapshots} auto snapshot(s) \
                 during it",
                leg(on),
                bytes >> 20,
                bytes as f64 / 1e6 / ((ended - started) as f64 / 1000.0),
            );
            results.push(Run {
                on,
                mb_s,
                snapshots,
            });
        }
    }
    let lease_after = root_lease(b)?;
    eprintln!(
        "    snapsched-write-overhead: root lease after:  holder {} epoch {}",
        lease_after.0, lease_after.1
    );
    ensure!(
        lease_after == lease_before,
        "the root lease moved: (holder, epoch) {lease_before:?} -> {lease_after:?}"
    );
    let status = sched_status(a)?;
    eprintln!(
        "    snapsched-write-overhead: a's scheduler: created {}, skipped_empty {}, \
         create_failed {}, ticks {}",
        stat(&status, "created"),
        stat(&status, "skipped_empty"),
        stat(&status, "create_failed"),
        stat(&status, "ticks"),
    );
    let median = |on: bool| {
        let mut v: Vec<f64> = results
            .iter()
            .filter(|r| r.on == on)
            .map(|r| r.mb_s)
            .collect();
        v.sort_by(f64::total_cmp);
        v[v.len() / 2]
    };
    let (off, on) = (median(false), median(true));
    let regression = (off - on) / off * 100.0;
    let fmt = |on: bool| {
        results
            .iter()
            .filter(|r| r.on == on)
            .map(|r| format!("{:.1}", r.mb_s))
            .collect::<Vec<_>>()
            .join(", ")
    };
    eprintln!(
        "    snapsched-write-overhead: RESULT no policy [{}] median {off:.1} MB/s; {POLICY} \
         [{}] median {on:.1} MB/s; regression {regression:+.2}% (plan bound: <= 3%, judged on \
         these medians, not asserted); {} auto snapshots during the policy runs; fio: \
         sequential 1 MiB psync writes, {fio_size} or {runtime} s per run, fsync every \
         {FSYNC_EVERY} MiB and at the end",
        fmt(false),
        fmt(true),
        results
            .iter()
            .filter(|r| r.on)
            .map(|r| r.snapshots)
            .sum::<usize>(),
    );
    Ok(())
}

/// The runs measure writes at the root lease holder, `b`. On a loaded host
/// `b` can stall past its backup's silence bound while it writes the tree
/// (seen: 1.5 s, the backup `a` sealed its epoch and took over), and with
/// forwarding on `b`'s fio writes would then go to `a` until `b` asks for
/// the lease back, at a moment of its own choosing — in the middle of a
/// run. So the lease is put back first: `a` unmounts (handing the lease
/// over), `b` writes and takes it, `a` mounts again. Said in the log, since
/// it is a takeover the measurement did not cause.
fn pin_lease(clients: &mut [Client; 2], b_id: u64) -> Result<()> {
    let (holder, epoch) = root_lease(&clients[1])?;
    if holder == b_id {
        return Ok(());
    }
    eprintln!(
        "    snapsched-write-overhead: NOTE the root lease moved to node {holder} (epoch {epoch}) \
         while the tree was written (a takeover under host load); a remounts so b takes it back"
    );
    clients[0].unmount()?;
    std::fs::write(clients[1].mnt.join("proj/fio/.pin"), b"pin")?;
    eventually(
        "b holds the root lease again",
        Duration::from_secs(120),
        || {
            let (holder, _) = root_lease(&clients[1])?;
            ensure!(holder == b_id, "holder {holder}, want b ({b_id})");
            Ok(())
        },
    )?;
    clients[0].mount()?;
    wait_for_p2p(&[&clients[0], &clients[1]])?;
    eventually("a sees the tree again", Duration::from_secs(120), || {
        ensure!(clients[0].mnt.join("proj/fio/.pin").is_file(), "not yet");
        Ok(())
    })
}

fn leg(on: bool) -> &'static str {
    if on {
        "on"
    } else {
        "off"
    }
}

/// `10s:1h` on `/proj`, set through `a` (forwarded to the holder as
/// `setfattr` would be); returns once `a` leads and the first snapshot the
/// policy takes has landed.
fn arm(a: &Client) -> Result<()> {
    let before = polled(a)?.len();
    set_xattr(&a.mnt.join("proj"), POLICY_XATTR, POLICY.as_bytes())
        .context("setting the policy")?;
    eventually("a leads the scheduler", Duration::from_secs(60), || {
        ensure!(is_leader(a)?, "not yet");
        Ok(())
    })?;
    eventually(
        "the policy's first snapshot",
        Duration::from_secs(120),
        || {
            let n = polled(a)?.len();
            ensure!(n > before, "{n} auto snapshots, {before} before");
            Ok(())
        },
    )
}

/// Remove the policy (its snapshots stay, orphaned) and wait until a tick
/// already planned could not still create one.
fn disarm(a: &Client) -> Result<()> {
    let (ok, out, err) = a.snapshot_cli(&["policy", "rm", "/proj", "--yes"])?;
    ensure!(ok, "policy rm: {out}{err}");
    eventually("a sees no policy root", Duration::from_secs(30), || {
        let roots = sched_status(a)?["roots"].as_array().map_or(0, Vec::len);
        ensure!(roots == 0, "{roots} root(s)");
        Ok(())
    })?;
    // A tick that planned before the removal may still be creating.
    let settle = Instant::now() + Duration::from_secs(15);
    let mut n = polled(a)?.len();
    loop {
        std::thread::sleep(Duration::from_secs(3));
        let now = polled(a)?.len();
        if now == n && Instant::now() >= settle {
            return Ok(());
        }
        n = now;
    }
}

/// One fio run on `b`, while a sampler reads the root lease once a second
/// (it must stay `lease`). Returns (bytes written, fio's runtime in s,
/// fio's write bandwidth in bytes/s).
fn fio_watched(
    b: &Client,
    name: &str,
    size: &str,
    runtime: u64,
    lease: (u64, u64),
) -> Result<(u64, f64, u64)> {
    let dir = b.mnt.join("proj/fio");
    let done = AtomicBool::new(false);
    let moved: Mutex<Option<(u64, u64)>> = Mutex::new(None);
    let out = std::thread::scope(|s| {
        s.spawn(|| {
            while !done.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_secs(1));
                if let Ok(now) = root_lease(b) {
                    if now != lease {
                        *moved.lock().unwrap() = Some(now);
                    }
                }
            }
        });
        let out = fio(&dir, name, size, runtime);
        done.store(true, Ordering::Relaxed);
        out
    })?;
    if let Some(now) = *moved.lock().unwrap() {
        anyhow::bail!("the root lease moved during fio run {name}: {lease:?} -> {now:?}");
    }
    let file = dir.join(format!("{name}.0.0"));
    std::fs::remove_file(&file).with_context(|| format!("removing {}", file.display()))?;
    Ok(out)
}

fn fio(dir: &Path, name: &str, size: &str, runtime: u64) -> Result<(u64, f64, u64)> {
    let out = Command::new("fio")
        .arg(format!("--name={name}"))
        .arg(format!("--directory={}", dir.display()))
        .arg(format!("--size={size}"))
        .arg(format!("--runtime={runtime}"))
        .arg(format!("--fsync={FSYNC_EVERY}"))
        .args([
            "--rw=write",
            "--bs=1M",
            "--ioengine=psync",
            "--numjobs=1",
            "--fallocate=none",
            "--end_fsync=1",
            "--output-format=json",
        ])
        .output()
        .context("running fio")?;
    ensure!(
        out.status.success(),
        "fio failed ({}): {}{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    // fio may print notes before the JSON document.
    let json = &text[text.find('{').context("no JSON from fio")?..];
    let v: serde_json::Value = serde_json::from_str(json).context("parsing fio's JSON")?;
    let w = &v["jobs"][0]["write"];
    Ok((
        w["io_bytes"].as_u64().unwrap_or(0),
        w["runtime"].as_u64().unwrap_or(0) as f64 / 1000.0,
        w["bw_bytes"].as_u64().unwrap_or(0),
    ))
}
