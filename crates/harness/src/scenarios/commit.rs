//! Plan 43 C0's `ack-latency`: what an acknowledgement costs per topology,
//! actor and op — the baseline the commit rule's milestones are measured
//! against (`bench/commit-rule/run.sh`, `docs/plans/v1/wip/43-commit-rule.md`
//! §7 C0 and §9). Measure-only: nothing is asserted about the numbers; the
//! scenario fails only when a topology is not what it claims (the
//! acknowledgement mode it is supposed to measure is not the one the node
//! reports), or an op fails.
//!
//! `ACK_LATENCY_TOPOLOGIES` (a comma-separated list of the numbers below)
//! runs only those topologies.
//!
//! Per topology, actor and op, `ACK_LATENCY_SAMPLES` (default 300) samples
//! of `create` (`O_CREAT|O_EXCL`), `write` (4 KiB + `fsync`), `rename`,
//! `unlink` and `chmod`, each on a target prepared untimed; and the first
//! `flock(LOCK_EX)` on a fresh file from a node that is not the file's
//! sequencer (then `LOCK_UN`; not in topology 1). Topologies:
//!
//! 1. `1-single`: one node;
//! 2. `2-backup`: two nodes, the holder's own ops and the other node's
//!    (forwarded) ops, after the holder listed its backup (placement off in
//!    every topology with a second node: otherwise the dominant writer is
//!    handed the directory and its ops stop being forwards);
//! 3. `3-delegated`: two nodes listed `b, a` (b holds the root), placement
//!    off, a directory delegated to a: a's own ops, and b's forwards into it
//!    — the delegate has no backup (a is the root's only peer);
//! 4. `4-delegated-backup`: three nodes, a directory delegated to b, which
//!    has a backup: b's own ops;
//! 5. `5-wan`: two nodes under `CONSTELLATION_HARNESS_LOAD=net-wan` (the load
//!    profile is process-global, so this topology runs *only* then): the
//!    lone root's own ops and a forwarded op;
//! 6. `6-ack-s3`: `CONSTELLATION_ACK=s3`, the holder's own ops.
//!
//! One JSON document on stdout:
//! `{"scenario":"ack-latency","load":…,"results":[{"topology","actor","op","n","p50_us","p90_us","p99_us"}]}`.

use super::m11::{delegate, wait_installed};
use super::m14::flock_timed;
use super::m8::pct;
use super::m9::{ack_of, cluster, node_id, unmount_all, wait_for_backup};
use super::{eventually, lease_of};
use crate::client::Client;
use anyhow::{ensure, Context, Result};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

fn samples() -> usize {
    std::env::var("ACK_LATENCY_SAMPLES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300)
}

/// `ACK_LATENCY_TOPOLOGIES` (e.g. `2,3`): run only these topologies, by
/// number (G1 compares topology 3's delegate with topology 2's holder);
/// unset, every topology the load profile allows.
fn topologies() -> Option<Vec<u32>> {
    let v = std::env::var("ACK_LATENCY_TOPOLOGIES").ok()?;
    Some(v.split(',').filter_map(|t| t.trim().parse().ok()).collect())
}

/// A topology's run: its rows for `n` samples per op and actor.
type Topology = fn(usize) -> Result<Rows>;

/// One topology's results, accumulated as JSON rows.
struct Rows {
    topology: &'static str,
    rows: Vec<serde_json::Value>,
}

impl Rows {
    fn new(topology: &'static str) -> Self {
        eprintln!("    ack-latency: topology {topology}");
        Self {
            topology,
            rows: Vec::new(),
        }
    }

    fn add(&mut self, actor: &str, op: &str, mut lat: Vec<Duration>) {
        lat.sort();
        let us = |p: f64| pct(&lat, p).as_micros() as u64;
        eprintln!(
            "    ack-latency: {} {actor} {op}: n={} p50={}us p90={}us p99={}us",
            self.topology,
            lat.len(),
            us(0.5),
            us(0.9),
            us(0.99)
        );
        self.rows.push(json!({
            "topology": self.topology,
            "actor": actor,
            "op": op,
            "n": lat.len(),
            "p50_us": us(0.5),
            "p90_us": us(0.9),
            "p99_us": us(0.99),
        }));
    }
}

/// Create `count` empty files `<tag>-<i>` in `dir`, untimed.
fn prepare(dir: &Path, tag: &str, count: usize) -> Result<Vec<PathBuf>> {
    (0..count)
        .map(|i| {
            let p = dir.join(format!("{tag}-{i}"));
            std::fs::write(&p, b"").with_context(|| format!("preparing {}", p.display()))?;
            Ok(p)
        })
        .collect()
}

fn timed<T>(f: impl FnOnce() -> std::io::Result<T>, what: &str) -> Result<Duration> {
    let t = Instant::now();
    f().with_context(|| what.to_string())?;
    Ok(t.elapsed())
}

/// The five ops of one actor on its own mount, in the directory `dir` (a
/// path on that actor's mount): `n` samples each.
fn measure_ops(rows: &mut Rows, actor: &str, dir: &Path, n: usize) -> Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    // create: O_CREAT|O_EXCL on a fresh name; the descriptor closes after
    // the clock stops.
    let mut create = Vec::with_capacity(n);
    for i in 0..n {
        let p = dir.join(format!("create-{i}"));
        let t = Instant::now();
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&p)
            .with_context(|| format!("creating {}", p.display()))?;
        create.push(t.elapsed());
        drop(f);
    }
    rows.add(actor, "create", create);
    // write: 4 KiB and fsync on a prepared file.
    let block = vec![0xa5u8; 4096];
    let targets = prepare(dir, "write", n)?;
    let mut write = Vec::with_capacity(n);
    for p in &targets {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(p)
            .with_context(|| format!("opening {}", p.display()))?;
        let t = Instant::now();
        f.write_all(&block)?;
        f.sync_all()?;
        write.push(t.elapsed());
    }
    rows.add(actor, "write", write);
    // rename.
    let targets = prepare(dir, "rename", n)?;
    let mut rename = Vec::with_capacity(n);
    for (i, p) in targets.iter().enumerate() {
        let to = dir.join(format!("renamed-{i}"));
        rename.push(timed(|| std::fs::rename(p, &to), "rename")?);
    }
    rows.add(actor, "rename", rename);
    // unlink.
    let targets = prepare(dir, "unlink", n)?;
    let mut unlink = Vec::with_capacity(n);
    for p in &targets {
        unlink.push(timed(|| std::fs::remove_file(p), "unlink")?);
    }
    rows.add(actor, "unlink", unlink);
    // chmod: alternate the mode so every call changes something.
    let targets = prepare(dir, "chmod", n)?;
    let mut chmod = Vec::with_capacity(n);
    for (i, p) in targets.iter().enumerate() {
        use std::os::unix::fs::PermissionsExt;
        let mode = if i % 2 == 0 { 0o600 } else { 0o640 };
        chmod.push(timed(
            || std::fs::set_permissions(p, std::fs::Permissions::from_mode(mode)),
            "chmod",
        )?);
    }
    rows.add(actor, "chmod", chmod);
    Ok(())
}

/// The first `flock(LOCK_EX)` of `n` fresh files, taken from `other` (not
/// their sequencer) on files `creator` made in `creator_dir`
/// (`other_dir` is the same directory as `other`'s mount shows it).
fn measure_flock(
    rows: &mut Rows,
    actor: &str,
    creator_dir: &Path,
    other_dir: &Path,
    n: usize,
) -> Result<()> {
    std::fs::create_dir_all(creator_dir)?;
    let mut lat = Vec::with_capacity(n);
    for i in 0..n {
        let name = format!("flock-{i}");
        std::fs::write(creator_dir.join(&name), b"").with_context(|| format!("creating {name}"))?;
        // The other node opens it once its replica has the entry.
        let p = other_dir.join(&name);
        let mut f = None;
        eventually(&format!("{name} visible"), Duration::from_secs(30), || {
            // (No `O_CREAT`: a node that has not applied the creator's
            // entry yet must not create the file itself.)
            f = Some(
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&p)
                    .with_context(|| format!("opening {}", p.display()))?,
            );
            Ok(())
        })?;
        let f = f.expect("opened");
        let (r, d) = flock_timed(&f, libc::LOCK_EX);
        r.map_err(|c| anyhow::anyhow!("flock {name}: {c:?}"))?;
        lat.push(d);
        let (r, _) = flock_timed(&f, libc::LOCK_UN);
        r.map_err(|c| anyhow::anyhow!("unlock {name}: {c:?}"))?;
    }
    rows.add(actor, "flock", lat);
    Ok(())
}

/// A topology that is not what it claims fails the scenario.
fn assert_policy(c: &Client, want: &str, topology: &str) -> Result<()> {
    let a = ack_of(c)?;
    ensure!(
        a["policy"] == want,
        "{topology}: {} is under policy {} (wanted {want}): {a}",
        c.name,
        a["policy"]
    );
    Ok(())
}

fn finish(mut all: Vec<Rows>, load: Option<String>) {
    let results: Vec<serde_json::Value> = all.drain(..).flat_map(|r| r.rows).collect();
    println!(
        "{}",
        json!({
            "scenario": "ack-latency",
            "load": load.unwrap_or_else(|| "none".into()),
            "results": results,
        })
    );
}

/// `ack-latency`: see the module doc.
pub fn ack_latency(_seed: u64) -> Result<()> {
    let n = samples();
    let load = crate::load::label();
    let wan = load.as_deref() == Some("net-wan");
    let only = topologies();
    let want = |t: u32| only.as_ref().is_none_or(|o| o.contains(&t));
    let mut all: Vec<Rows> = Vec::new();
    if wan {
        if want(5) {
            all.push(topology_wan(n)?);
        }
    } else {
        let runs: [(u32, Topology); 5] = [
            (1, topology_single),
            (2, topology_backup),
            (3, topology_delegated),
            (4, topology_delegated_backup),
            (6, topology_ack_s3),
        ];
        for (t, run) in runs {
            if want(t) {
                all.push(run(n)?);
            }
        }
    }
    finish(all, load);
    Ok(())
}

fn topology_single(n: usize) -> Result<Rows> {
    const NAME: &str = "ack-latency-1-single";
    let (_env, _root, mut clients, _) = cluster(NAME, &["a"], &[], 0)?;
    let result = (|| -> Result<Rows> {
        let mut rows = Rows::new("1-single");
        let a = &clients[0];
        assert_policy(a, "local", "1-single")?;
        measure_ops(&mut rows, "holder", &a.mnt.join("t1"), n)?;
        Ok(rows)
    })();
    unmount_all(&mut clients);
    result
}

fn topology_backup(n: usize) -> Result<Rows> {
    const NAME: &str = "ack-latency-2-backup";
    let (_env, _root, mut clients, _) = cluster(
        NAME,
        &["a", "b"],
        &[("CONSTELLATION_DELEGATION_PLACEMENT", "off")],
        0,
    )?;
    let result = (|| -> Result<Rows> {
        let mut rows = Rows::new("2-backup");
        let (a, b) = (&clients[0], &clients[1]);
        wait_for_backup(a, Duration::from_secs(60))?;
        assert_policy(a, "backup", "2-backup")?;
        std::fs::create_dir_all(a.mnt.join("t2"))?;
        let (da, db) = (a.mnt.join("t2"), b.mnt.join("t2"));
        eventually("t2 visible on b", Duration::from_secs(30), || {
            ensure!(db.is_dir());
            Ok(())
        })?;
        measure_ops(&mut rows, "holder", &da.join("own"), n)?;
        measure_ops(&mut rows, "forward", &db.join("fwd"), n)?;
        measure_flock(&mut rows, "forward", &da.join("lk"), &db.join("lk"), n)?;
        Ok(rows)
    })();
    unmount_all(&mut clients);
    result
}

fn topology_delegated(n: usize) -> Result<Rows> {
    const NAME: &str = "ack-latency-3-delegated";
    // `b` listed first: it holds the root; `a` is the delegate of `/d1`.
    let (_env, _root, mut clients, _) = cluster(
        NAME,
        &["b", "a"],
        &[("CONSTELLATION_DELEGATION_PLACEMENT", "off")],
        0,
    )?;
    let result = (|| -> Result<Rows> {
        let mut rows = Rows::new("3-delegated");
        let (b, a) = (&clients[0], &clients[1]);
        wait_for_backup(b, Duration::from_secs(60))?;
        assert_policy(b, "backup", "3-delegated")?;
        std::fs::create_dir(b.mnt.join("d1"))?;
        eventually("d1 visible on a", Duration::from_secs(30), || {
            ensure!(a.mnt.join("d1").is_dir());
            Ok(())
        })?;
        delegate(b, "/d1", node_id(a)?)?;
        wait_installed(a, "/d1", Duration::from_secs(30))?;
        // The delegate has no backup of its own: its only peer is the root.
        let d = a.control_status()?["delegation"].clone();
        ensure!(
            d["backups"].as_array().is_none_or(|v| v.is_empty()),
            "3-delegated: the delegate has a backup: {d}"
        );
        measure_ops(&mut rows, "delegate", &a.mnt.join("d1/own"), n)?;
        measure_ops(&mut rows, "forward", &b.mnt.join("d1/fwd"), n)?;
        measure_flock(
            &mut rows,
            "forward",
            &a.mnt.join("d1/lk"),
            &b.mnt.join("d1/lk"),
            n,
        )?;
        Ok(rows)
    })();
    unmount_all(&mut clients);
    result
}

fn topology_delegated_backup(n: usize) -> Result<Rows> {
    const NAME: &str = "ack-latency-4-delegated-backup";
    let (_env, _root, mut clients, _) = cluster(
        NAME,
        &["a", "b", "c"],
        &[("CONSTELLATION_DELEGATION_PLACEMENT", "off")],
        0,
    )?;
    let result = (|| -> Result<Rows> {
        let mut rows = Rows::new("4-delegated-backup");
        let (a, b, c) = (&clients[0], &clients[1], &clients[2]);
        wait_for_backup(a, Duration::from_secs(60))?;
        std::fs::create_dir(a.mnt.join("d1"))?;
        for x in [b, c] {
            eventually("d1 visible", Duration::from_secs(30), || {
                ensure!(x.mnt.join("d1").is_dir());
                Ok(())
            })?;
        }
        delegate(a, "/d1", node_id(b)?)?;
        wait_installed(b, "/d1", Duration::from_secs(30))?;
        eventually("b chose a backup", Duration::from_secs(30), || {
            let d = b.control_status()?["delegation"].clone();
            ensure!(
                d["backups"].as_array().is_some_and(|v| !v.is_empty()),
                "4-delegated-backup: the delegate has no backup yet: {d}"
            );
            Ok(())
        })?;
        measure_ops(&mut rows, "delegate", &b.mnt.join("d1/own"), n)?;
        measure_flock(
            &mut rows,
            "forward",
            &b.mnt.join("d1/lk"),
            &c.mnt.join("d1/lk"),
            n,
        )?;
        Ok(rows)
    })();
    unmount_all(&mut clients);
    result
}

fn topology_wan(n: usize) -> Result<Rows> {
    const NAME: &str = "ack-latency-5-wan";
    let (_env, _root, mut clients, _) = cluster(
        NAME,
        &["a", "b"],
        &[("CONSTELLATION_DELEGATION_PLACEMENT", "off")],
        0,
    )?;
    let result = (|| -> Result<Rows> {
        let mut rows = Rows::new("5-wan");
        let (a, b) = (&clients[0], &clients[1]);
        // The peers are beyond the RTT budget: the root is lone.
        eventually("a is a lone root", Duration::from_secs(60), || {
            let a = ack_of(a)?;
            ensure!(
                a["policy"] == "local" && a["backups"].as_array().is_none_or(|v| v.is_empty()),
                "5-wan: a is not lone: {a}"
            );
            Ok(())
        })?;
        let lease = lease_of(a)?;
        ensure!(
            lease["held"] == true,
            "5-wan: a does not hold the root: {lease}"
        );
        std::fs::create_dir_all(a.mnt.join("t5"))?;
        eventually("t5 visible on b", Duration::from_secs(60), || {
            ensure!(b.mnt.join("t5").is_dir());
            Ok(())
        })?;
        measure_ops(&mut rows, "root", &a.mnt.join("t5/own"), n)?;
        measure_ops(&mut rows, "forward", &b.mnt.join("t5/fwd"), n)?;
        measure_flock(
            &mut rows,
            "forward",
            &a.mnt.join("t5/lk"),
            &b.mnt.join("t5/lk"),
            n,
        )?;
        Ok(rows)
    })();
    unmount_all(&mut clients);
    result
}

fn topology_ack_s3(n: usize) -> Result<Rows> {
    const NAME: &str = "ack-latency-6-ack-s3";
    let (_env, _root, mut clients, _) = cluster(
        NAME,
        &["a", "b"],
        &[
            ("CONSTELLATION_ACK", "s3"),
            ("CONSTELLATION_DELEGATION_PLACEMENT", "off"),
        ],
        0,
    )?;
    let result = (|| -> Result<Rows> {
        let mut rows = Rows::new("6-ack-s3");
        let (a, b) = (&clients[0], &clients[1]);
        assert_policy(a, "s3", "6-ack-s3")?;
        measure_ops(&mut rows, "holder", &a.mnt.join("t6"), n)?;
        measure_flock(
            &mut rows,
            "forward",
            &a.mnt.join("t6/lk"),
            &b.mnt.join("t6/lk"),
            n,
        )?;
        Ok(rows)
    })();
    unmount_all(&mut clients);
    result
}
