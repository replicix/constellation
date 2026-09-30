//! Plan 31 C8: the engine's host lifecycle on Linux, driven through
//! `node.lifecycle` (the manual source a desktop host pushes into, the
//! same path a phone's OS drives).
//!
//! - `lifecycle-suspend-mid-write`: a writer on the lease holder records
//!   every file whose `fsync` returned (its acknowledgements) while the
//!   holder is suspended mid-stream; the suspension publishes, ships and
//!   releases within its deadline; another node takes the lease over and
//!   every node reads every acknowledged file byte-exact; the holder
//!   resumes, the writer goes on, and nothing acknowledged is ever lost.
//! - `lifecycle-resume-rejoin`: a suspended holder's lease moves to
//!   another node, which writes, renames and deletes; the resumed node
//!   catches up, rejoins P2P and serves (and writes) again.
//! - `lifecycle-metered-uploads`: an `unmetered-only` node on a metered
//!   network uploads no chunk for plain closes (its S3 chunk PUT counter
//!   stands still, the pending queue grows, the other node sees none of
//!   the content), an `fsync`'d file still goes up at once, and the first
//!   unmetered `NetworkChanged` drains everything to every node.

use super::m9::{cluster, unmount_all};
use super::{ensure_no_conflicts, eventually, lease_of, wait_for_p2p};
use crate::client::Client;
use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A file whose `fsync` returned: its name, content hash and length.
#[derive(Clone, Debug)]
struct Acked {
    name: String,
    hash: blake3::Hash,
    len: usize,
}

/// Seeded content of `len` bytes (a blake3 XOF of the seed and name).
fn content(seed: u64, name: &str, len: usize) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&seed.to_le_bytes());
    hasher.update(name.as_bytes());
    let mut out = vec![0u8; len];
    hasher.finalize_xof().fill(&mut out);
    out
}

/// Write `data` to `path`, `fsync` it, close it: `Ok` is the
/// acknowledgement (the `fsync` returned).
fn write_synced(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let mut f = std::fs::File::create(path)?;
    f.write_all(data)?;
    f.sync_all()?;
    Ok(())
}

/// One writer thread: `<tag>-<i>` files of 4 KiB to 192 KiB, each
/// written, `fsync`ed and closed, recording every acknowledged one.
struct Writer {
    stop: Arc<AtomicBool>,
    acked: Arc<Mutex<Vec<Acked>>>,
    errors: Arc<Mutex<Vec<String>>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Writer {
    fn start(mnt: PathBuf, tag: &str, seed: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let acked = Arc::new(Mutex::new(Vec::new()));
        let errors = Arc::new(Mutex::new(Vec::new()));
        let (s, a, e, tag) = (stop.clone(), acked.clone(), errors.clone(), tag.to_string());
        let thread = std::thread::spawn(move || {
            let mut i = 0u64;
            while !s.load(Ordering::Relaxed) {
                let name = format!("{tag}-{i}");
                let len =
                    4096 + ((seed.wrapping_mul(31).wrapping_add(i * 7919)) % (188 * 1024)) as usize;
                let data = content(seed, &name, len);
                match write_synced(&mnt.join(&name), &data) {
                    Ok(()) => a.lock().unwrap().push(Acked {
                        name,
                        hash: blake3::hash(&data),
                        len,
                    }),
                    Err(err) => {
                        e.lock().unwrap().push(format!("{name}: {err}"));
                        std::thread::sleep(Duration::from_millis(200));
                    }
                }
                i += 1;
            }
        });
        Self {
            stop,
            acked,
            errors,
            thread: Some(thread),
        }
    }

    fn count(&self) -> usize {
        self.acked.lock().unwrap().len()
    }

    /// Stop after the op in flight; the acknowledged files and the errors.
    fn finish(mut self) -> (Vec<Acked>, Vec<String>) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
        (
            std::mem::take(&mut *self.acked.lock().unwrap()),
            std::mem::take(&mut *self.errors.lock().unwrap()),
        )
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Every `files` entry reads back byte-exact on `c`, within `deadline`.
fn verify(c: &Client, files: &[Acked], deadline: Duration) -> Result<()> {
    eventually(
        &format!("{} reads all {} acknowledged file(s)", c.name, files.len()),
        deadline,
        || {
            for f in files {
                let got = std::fs::read(c.mnt.join(&f.name))
                    .with_context(|| format!("{} reading {}", c.name, f.name))?;
                anyhow::ensure!(
                    got.len() == f.len && blake3::hash(&got) == f.hash,
                    "{} read {} as {} byte(s), want {} (hash mismatch: {})",
                    c.name,
                    f.name,
                    got.len(),
                    f.len,
                    blake3::hash(&got) != f.hash
                );
            }
            Ok(())
        },
    )
}

/// `node.lifecycle` with `event` (the wire's externally tagged enum).
fn lifecycle(c: &Client, event: Value) -> Result<Value> {
    let report = c
        .control("node.lifecycle", json!({ "event": event }))
        .with_context(|| format!("node.lifecycle on {}", c.name))?;
    anyhow::ensure!(
        report["applied"] == true,
        "{}: the event was not applied: {report}",
        c.name
    );
    Ok(report)
}

fn suspend(c: &Client, deadline_ms: u64) -> Result<Value> {
    let t = Instant::now();
    let report = lifecycle(
        c,
        json!({ "Suspending": { "deadline_in_ms": deadline_ms } }),
    )?;
    let s = &report["status"]["last_suspend"];
    eprintln!(
        "    {}: suspended in {:?}: {}",
        c.name,
        t.elapsed(),
        serde_json::to_string(s).unwrap_or_default()
    );
    Ok(report)
}

/// The suspension did everything by its deadline: every view published,
/// the flush finished, nothing left behind, the lease released.
fn check_clean_suspend(c: &Client, report: &Value) -> Result<()> {
    let s = &report["status"]["last_suspend"];
    let st = &report["status"];
    anyhow::ensure!(
        s["within_deadline"] == true
            && s["flushed"] == true
            && s["lease_released"] == true
            && s["journal_backlog"] == 0
            && s["pending_uploads"] == 0
            && s["views"].as_u64().unwrap_or(0) >= 1
            && s["views_synced"] == s["views"],
        "{}: the suspension did not finish cleanly: {s}",
        c.name
    );
    anyhow::ensure!(
        st["state"] == "suspended"
            && st["suspended"] == true
            && st["forward_only"] == true
            && st["background_paused"] == true
            && st["p2p_accepts_inbound"] == false
            && st["p2p_gossip"] == false,
        "{}: not suspended as expected: {st}",
        c.name
    );
    let lease = lease_of(c)?;
    anyhow::ensure!(lease["held"] != true, "{} still holds: {lease}", c.name);
    Ok(())
}

fn resume(c: &Client) -> Result<()> {
    let report = lifecycle(c, json!("Resumed"))?;
    let st = &report["status"];
    anyhow::ensure!(
        st["last_resume"]["was_suspended"] == true
            && st["state"] == "foreground"
            && st["suspended"] == false
            && st["forward_only"] == false
            && st["background_paused"] == false
            && st["p2p_accepts_inbound"] == true
            && st["p2p_gossip"] == true,
        "{}: not resumed as expected: {st}",
        c.name
    );
    eprintln!("    {}: resumed: {}", c.name, st["last_resume"]);
    Ok(())
}

/// Wait until `c` holds the lease.
fn wait_holder(c: &Client, deadline: Duration) -> Result<()> {
    eventually(&format!("{} holds the lease", c.name), deadline, || {
        let lease = lease_of(c)?;
        anyhow::ensure!(lease["held"] == true, "{}: {lease}", c.name);
        Ok(())
    })
}

/// `count` fsync'd files `<tag>-<i>` on `c`, acknowledged.
fn write_acked(c: &Client, seed: u64, tag: &str, count: usize) -> Result<Vec<Acked>> {
    let mut out = Vec::new();
    for i in 0..count {
        let name = format!("{tag}-{i}");
        let data = content(seed, &name, 8192 + i * 1024);
        write_synced(&c.mnt.join(&name), &data)
            .with_context(|| format!("{} writing {name}", c.name))?;
        out.push(Acked {
            name,
            hash: blake3::hash(&data),
            len: data.len(),
        });
    }
    Ok(out)
}

pub fn lifecycle_suspend_mid_write(seed: u64) -> Result<()> {
    const NAME: &str = "lifecycle-suspend-mid-write";
    let (_env, _root, mut clients, _) = cluster(NAME, &["a", "b", "c"], &[], 0)?;
    let result = (|| -> Result<()> {
        // Node 0 holds the lease (`cluster`); its writer streams fsync'd
        // files, and the suspension lands in the middle of the stream.
        let writer = Writer::start(clients[0].mnt.clone(), "w1", seed);
        eventually(
            "the writer acknowledges 15 files",
            Duration::from_secs(60),
            || {
                anyhow::ensure!(writer.count() >= 15, "{} so far", writer.count());
                Ok(())
            },
        )?;
        let report = suspend(&clients[0], 15_000)?;
        check_clean_suspend(&clients[0], &report)?;
        // Another node continues: it takes the lease over with its first
        // write (a released lease, no TTL to wait out) — and with it the
        // writer's ops that arrive during the suspension, which forward to
        // it as any non-holder's do (dialing out: the suspended node
        // accepts no connection and has left gossip, but may still send
        // its own requests).
        let t = Instant::now();
        let mut theirs = write_acked(&clients[1], seed, "b1", 10)?;
        eprintln!(
            "    {NAME}: b's first 10 writes after the suspension took {:?}",
            t.elapsed()
        );
        wait_holder(&clients[1], Duration::from_secs(30))?;
        let (mut acked, errors) = writer.finish();
        eprintln!(
            "    {NAME}: phase 1: {} acknowledged, {} failed ({:?})",
            acked.len(),
            errors.len(),
            errors.iter().take(3).collect::<Vec<_>>()
        );
        // An op arriving while suspended waits or forwards like any
        // non-holder's; none fails.
        anyhow::ensure!(
            errors.is_empty(),
            "writes failed across the suspension: {errors:?}"
        );
        // Every acknowledged file is on every other node, byte-exact...
        for c in &clients[1..] {
            verify(c, &acked, Duration::from_secs(60))?;
            verify(c, &theirs, Duration::from_secs(60))?;
        }
        // ...and the suspended node still serves its local reads.
        verify(
            &clients[0],
            &acked[..acked.len().min(5)],
            Duration::from_secs(10),
        )?;
        resume(&clients[0])?;
        let refs: Vec<&Client> = clients.iter().collect();
        wait_for_p2p(&refs)?;
        // The writer goes on after the resume.
        let writer = Writer::start(clients[0].mnt.clone(), "w3", seed ^ 0x5eed);
        eventually(
            "the writer acknowledges 10 more",
            Duration::from_secs(60),
            || {
                anyhow::ensure!(writer.count() >= 10, "{} so far", writer.count());
                Ok(())
            },
        )?;
        let (more, errors) = writer.finish();
        anyhow::ensure!(
            errors.is_empty(),
            "writes failed after the resume: {errors:?}"
        );
        acked.extend(more);
        theirs.extend(write_acked(&clients[2], seed, "c1", 5)?);
        acked.extend(theirs);
        for c in &clients {
            verify(c, &acked, Duration::from_secs(90))?;
        }
        eprintln!(
            "    {NAME}: {} acknowledged file(s) intact on all 3 nodes",
            acked.len()
        );
        ensure_no_conflicts(&refs)
    })();
    if result.is_err() {
        for c in &clients {
            eprintln!("--- {} log ---\n{}", c.name, c.tail_log_n(60));
        }
    }
    unmount_all(&mut clients);
    result
}

pub fn lifecycle_resume_rejoin(seed: u64) -> Result<()> {
    const NAME: &str = "lifecycle-resume-rejoin";
    let (_env, _root, mut clients, _) = cluster(NAME, &["a", "b", "c"], &[], 0)?;
    let result = (|| -> Result<()> {
        let mut files = write_acked(&clients[0], seed, "pre", 10)?;
        let report = suspend(&clients[0], 10_000)?;
        check_clean_suspend(&clients[0], &report)?;
        // Another node takes the writes over, and changes what the
        // suspended node has cached.
        files.extend(write_acked(&clients[1], seed, "mid", 10)?);
        wait_holder(&clients[1], Duration::from_secs(30))?;
        std::fs::rename(clients[1].mnt.join("pre-0"), clients[1].mnt.join("moved-0"))?;
        std::fs::remove_file(clients[1].mnt.join("pre-1"))?;
        files.retain(|f| f.name != "pre-1");
        for f in files.iter_mut() {
            if f.name == "pre-0" {
                f.name = "moved-0".into();
            }
        }
        verify(&clients[2], &files, Duration::from_secs(60))?;
        // Suspended, the node still reads what it has.
        let local = std::fs::read(clients[0].mnt.join("pre-5")).context("a local read")?;
        anyhow::ensure!(!local.is_empty(), "pre-5 read empty while suspended");
        let t = Instant::now();
        resume(&clients[0])?;
        // It catches up with everything the cluster did meanwhile...
        verify(&clients[0], &files, Duration::from_secs(60))?;
        eventually(
            "the removed and moved names are gone",
            Duration::from_secs(60),
            || {
                for gone in ["pre-0", "pre-1"] {
                    anyhow::ensure!(
                        !clients[0].mnt.join(gone).exists(),
                        "{gone} still visible on a"
                    );
                }
                Ok(())
            },
        )?;
        eprintln!("    {NAME}: a caught up {:?} after the resume", t.elapsed());
        // ...rejoins P2P...
        let refs: Vec<&Client> = clients.iter().collect();
        wait_for_p2p(&refs)?;
        // ...and serves writes again (forwarded to the holder, or holding).
        files.extend(write_acked(&clients[0], seed, "post", 10)?);
        for c in &clients {
            verify(c, &files, Duration::from_secs(60))?;
        }
        let status = clients[0].control_status()?;
        anyhow::ensure!(
            status["lifecycle"]["state"] == "foreground"
                && status["lifecycle"]["events"].as_u64() == Some(2),
            "a's lifecycle after the resume: {}",
            status["lifecycle"]
        );
        ensure_no_conflicts(&refs)
    })();
    if result.is_err() {
        for c in &clients {
            eprintln!("--- {} log ---\n{}", c.name, c.tail_log_n(60));
        }
    }
    unmount_all(&mut clients);
    result
}

/// The node's chunk PUTs so far (its `s3.by_area` `PUT chunks…` rows).
fn chunk_puts(c: &Client) -> Result<u64> {
    let status = c.control_status()?;
    let areas = status["s3"]["by_area"]
        .as_object()
        .context("no s3.by_area in status")?;
    Ok(areas
        .iter()
        .filter(|(k, _)| k.starts_with("PUT chunk"))
        .map(|(_, v)| v.as_u64().unwrap_or(0))
        .sum())
}

pub fn lifecycle_metered_uploads(seed: u64) -> Result<()> {
    const NAME: &str = "lifecycle-metered-uploads";
    let (_env, _root, mut clients, _) = cluster(
        NAME,
        &["a", "b"],
        &[("CONSTELLATION_PROFILE_UPLOADS", "unmetered-only")],
        0,
    )?;
    let result = (|| -> Result<()> {
        let (a, b) = (&clients[0], &clients[1]);
        let status = a.control_status()?;
        anyhow::ensure!(
            status["lifecycle"]["profile"]["uploads"] == "unmetered-only",
            "profile not applied: {}",
            status["lifecycle"]
        );
        let report = lifecycle(
            a,
            json!({ "NetworkChanged": { "reachable": true, "metered": true } }),
        )?;
        anyhow::ensure!(
            report["status"]["uploads_held"] == true && report["status"]["network_metered"] == true,
            "uploads not held on a metered network: {}",
            report["status"]
        );
        // Let a round that started before the event finish.
        std::thread::sleep(Duration::from_secs(1));
        let puts_before = chunk_puts(a)?;
        // Plain closes: journaled locally, their chunks wait.
        let mut files = Vec::new();
        for i in 0..8 {
            let name = format!("metered-{i}");
            let data = content(seed, &name, 200 * 1024 + i * 4096);
            std::fs::write(a.mnt.join(&name), &data)?;
            files.push(Acked {
                name,
                hash: blake3::hash(&data),
                len: data.len(),
            });
        }
        std::thread::sleep(Duration::from_secs(4));
        let held = a.control_status()?;
        let pending = held["writeback"]["pending_uploads"].as_u64().unwrap_or(0);
        let puts_held = chunk_puts(a)?;
        eprintln!(
            "    {NAME}: metered: chunk PUTs {puts_before} -> {puts_held}, pending {pending}, \
             deferrals {}",
            held["lifecycle"]["upload_deferrals"]
        );
        anyhow::ensure!(
            puts_held == puts_before,
            "chunk uploads ran on a metered network ({puts_before} -> {puts_held})"
        );
        anyhow::ensure!(
            pending >= 8,
            "the pending queue should hold the 8 files' chunks: {pending}"
        );
        for f in &files {
            if let Ok(got) = std::fs::read(b.mnt.join(&f.name)) {
                anyhow::ensure!(
                    got.len() != f.len || blake3::hash(&got) != f.hash,
                    "{} reached b while its chunks were held",
                    f.name
                );
            }
        }
        // An explicit durability request still uploads at once.
        let synced = write_acked(a, seed, "metered-fsync", 1)?;
        let puts_synced = chunk_puts(a)?;
        anyhow::ensure!(
            puts_synced > puts_held,
            "an fsync on a metered network did not upload ({puts_held} -> {puts_synced})"
        );
        verify(b, &synced, Duration::from_secs(30))?;
        // Unmetered again: everything drains, nothing lost.
        let report = lifecycle(
            a,
            json!({ "NetworkChanged": { "reachable": true, "metered": false } }),
        )?;
        anyhow::ensure!(
            report["status"]["uploads_held"] == false,
            "uploads still held: {}",
            report["status"]
        );
        eventually("a's pending uploads drain", Duration::from_secs(60), || {
            let s = a.control_status()?;
            let pending = s["writeback"]["pending_uploads"]
                .as_u64()
                .unwrap_or(u64::MAX);
            anyhow::ensure!(pending == 0, "pending {pending}");
            Ok(())
        })?;
        let puts_after = chunk_puts(a)?;
        anyhow::ensure!(
            puts_after >= puts_synced + 8,
            "the held chunks were not uploaded ({puts_synced} -> {puts_after})"
        );
        files.extend(synced);
        for c in [a, b] {
            verify(c, &files, Duration::from_secs(60))?;
        }
        eprintln!("    {NAME}: unmetered: chunk PUTs -> {puts_after}; every file on both nodes");
        ensure_no_conflicts(&[a, b])
    })();
    if result.is_err() {
        for c in &clients {
            eprintln!("--- {} log ---\n{}", c.name, c.tail_log_n(60));
        }
    }
    unmount_all(&mut clients);
    result
}
