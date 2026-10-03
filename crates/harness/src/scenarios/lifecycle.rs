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

/// How the sequencer acknowledges in one `writeback-close-metered-nonowner`
/// run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CloseRace {
    /// No backup (`CONSTELLATION_BACKUPS=0`): `Local`, nothing streams
    /// ahead — the case chunk 39b met on real S3 (a holder's first
    /// seconds, or no peer within the RTT budget).
    Unbacked,
    /// A backs B: the pre-S3 stream carries A's own record to it past its
    /// held chunks (B answers `OwnChunks::Streamed`), so the hold keeps
    /// every chunk on A — with the default safety timer.
    Backed,
    /// `fs create --ack-policy s3`: B's acknowledgement waits for the log.
    AckS3,
}

/// A non-owner's `--write-mode back` close with its uploads held (plan
/// 31 C8): B holds the lease and creates `d/f<i>`; A, metered under
/// `unmetered-only`, overwrites it at once — before it has applied B's
/// create — and closes. Before the fix the close forwarded a manifest
/// naming A's held chunks, B answered with a base A had not applied, A
/// waited for its own record in the log (`AwaitingLog`; under `ack=s3`
/// B answered `Held` until the record shipped), and B could not ship it
/// until A's chunks were up: the close sat out the 120 s forward
/// deadline and ended in doubt. Now B's answer says whether only A's
/// upload brings the record back (`OwnChunks::Upload`: unbacked, and
/// `ack=s3`, where B answers `Held` at once) and A uploads at once; a
/// backed B's stream carries it (`Streamed`), and nothing is uploaded on
/// the metered network. Then the review's dependent ops: a `chmod`, and a
/// `rename`, right after a close that returned at once with its chunk
/// still held. Their records name no chunk, but B's ship plan defers them
/// with the close, so B's answer names the close's inode (`Upload`, or
/// `Streamed` when backed) and A uploads its chunks; before, the answer
/// was `None` and the op stalled 120 s, `EIO` in doubt.
pub fn writeback_close_metered_nonowner(seed: u64) -> Result<()> {
    const NAME: &str = "writeback-close-metered-nonowner";
    let (env, root) = super::setup(NAME)?;
    let proxy = env.s3_proxy()?;
    for (i, mode) in [CloseRace::Unbacked, CloseRace::Backed, CloseRace::AckS3]
        .into_iter()
        .enumerate()
    {
        proxy.remove_all_toxics()?;
        let dir = root.path().join(format!("{mode:?}"));
        std::fs::create_dir_all(&dir)?;
        close_race(NAME, &env, &proxy, &dir, seed ^ i as u64, mode)
            .with_context(|| format!("{mode:?}"))?;
    }
    Ok(())
}

fn close_race(
    name: &str,
    env: &crate::s3env::S3Env,
    proxy: &crate::toxiproxy::Proxy<'_>,
    root: &Path,
    seed: u64,
    mode: CloseRace,
) -> Result<()> {
    const ROUNDS: usize = 4;
    let backend = format!("s3://{}/{name}-{mode:?}-{}", super::BUCKET, super::ts());
    let mk = |node: &str| -> Result<Client> {
        let c = Client::new(root, node, &env.endpoint, &backend)?
            .with_own_node_key()
            .with_env("CONSTELLATION_LEASE_PLACEMENT", "off");
        Ok(match mode {
            CloseRace::Unbacked => c.with_env("CONSTELLATION_BACKUPS", "0"),
            CloseRace::Backed => c,
            CloseRace::AckS3 => c.with_env("CONSTELLATION_ACK", "s3"),
        })
    };
    let mut b = mk("b")?;
    let mut a = mk("a")?
        .with_write_mode("back")
        .with_env("CONSTELLATION_PROFILE_UPLOADS", "unmetered-only");
    b.fs_create()?;
    b.mount()?;
    a.mount()?;
    let result = (|| -> Result<()> {
        wait_for_p2p(&[&a, &b])?;
        std::fs::create_dir(b.mnt.join("d"))?;
        eventually("B holds the lease", Duration::from_secs(30), || {
            anyhow::ensure!(lease_of(&b)?["held"] == true, "{}", lease_of(&b)?);
            Ok(())
        })?;
        eventually("d/ visible on A", Duration::from_secs(30), || {
            anyhow::ensure!(a.mnt.join("d").is_dir(), "not yet");
            Ok(())
        })?;
        let want = match mode {
            CloseRace::Unbacked => "local",
            CloseRace::Backed => "backup",
            CloseRace::AckS3 => "s3",
        };
        eventually(
            &format!("B acknowledges {want}"),
            Duration::from_secs(30),
            || {
                let ack = b.control_status()?["ack"].clone();
                anyhow::ensure!(ack["policy"] == want, "{ack}");
                Ok(())
            },
        )?;
        let report = lifecycle(
            &a,
            json!({ "NetworkChanged": { "reachable": true, "metered": true } }),
        )?;
        anyhow::ensure!(
            report["status"]["uploads_held"] == true,
            "A's uploads are not held: {}",
            report["status"]
        );
        std::thread::sleep(Duration::from_millis(500));
        let puts_before = chunk_puts(&a)?;
        // A real S3's round trip: B's ship of its create lags the
        // forward of A's close, as it did on AWS and OVH.
        proxy.latency(60, 20)?;
        let mut files = Vec::new();
        let mut slowest = Duration::ZERO;
        for i in 0..ROUNDS {
            let file = format!("d/f{i}");
            std::fs::write(b.mnt.join(&file), b"from b")?;
            let data = content(seed, &file, 300 * 1024 + i * 4096);
            let t = Instant::now();
            std::fs::write(a.mnt.join(&file), &data)?;
            let took = t.elapsed();
            slowest = slowest.max(took);
            anyhow::ensure!(
                took < Duration::from_secs(10),
                "A's back close of {file} stalled {took:?}"
            );
            files.push(Acked {
                name: file,
                hash: blake3::hash(&data),
                len: data.len(),
            });
        }
        let status = a.control_status()?;
        let pending = status["writeback"]["pending_uploads"].as_u64().unwrap_or(0);
        let uploads = status["ack"]["own_record_uploads"].as_u64().unwrap_or(0);
        let puts = chunk_puts(&a)? - puts_before;
        let held_for_upload = b.control_status()?["ack"]["held_for_upload"]
            .as_u64()
            .unwrap_or(0);
        eprintln!(
            "    {name} [{mode:?}]: slowest close {slowest:?}, own-record uploads {uploads}, \
             chunk PUTs {puts}, pending {pending}, awaited log {} (streamed {}), \
             B held for upload {held_for_upload}",
            status["ack"]["awaited_log"], status["ack"]["awaited_log_streamed"]
        );
        match mode {
            CloseRace::Backed => {
                anyhow::ensure!(
                    uploads == 0 && puts == 0 && pending > 0,
                    "a backed holder's stream should answer A's closes with every chunk \
                     still held (uploads {uploads}, PUTs {puts}, pending {pending})"
                );
            }
            CloseRace::Unbacked | CloseRace::AckS3 => {
                anyhow::ensure!(
                    uploads > 0 && puts > 0,
                    "no close waited on its own record (uploads {uploads}, PUTs {puts}): \
                     the race was not reproduced"
                );
                anyhow::ensure!(
                    mode != CloseRace::AckS3 || held_for_upload > 0,
                    "under ack=s3 B never asked for A's upload (held for upload \
                     {held_for_upload})"
                );
            }
        }
        // Review must-fix 1: an op right after a close that returned on an
        // applied base with its chunk still held depends on that close
        // (plan 30 §M4's key dependence) though its own records name no
        // chunk: B ships it only once A's chunk is up. A creates the
        // file, waits until it applied its own create, writes it without
        // `O_TRUNC` (the close returns at once, chunk pending), then
        // `chmod`s it — or renames it. Before, B answered `None` and the
        // op stalled 120 s, EIO in doubt.
        let uploads_before = a.control_status()?["ack"]["own_record_uploads"]
            .as_u64()
            .unwrap_or(0);
        for (i, probe) in ["chmod", "rename"].into_iter().enumerate() {
            let file = format!("d/{probe}");
            std::fs::write(a.mnt.join(&file), b"")?;
            std::thread::sleep(Duration::from_secs(3));
            let data = content(seed, &file, 200 * 1024 + i * 4096);
            let t = Instant::now();
            {
                use std::io::Write;
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(a.mnt.join(&file))?;
                f.write_all(&data)?;
            }
            let close = t.elapsed();
            let t = Instant::now();
            let kept = if probe == "chmod" {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(
                    a.mnt.join(&file),
                    std::fs::Permissions::from_mode(0o600),
                )?;
                file
            } else {
                let to = format!("{file}-moved");
                std::fs::rename(a.mnt.join(&file), a.mnt.join(&to))?;
                to
            };
            let took = t.elapsed();
            eprintln!("    {name} [{mode:?}]: {kept}: close {close:?}, then {probe} {took:?}");
            anyhow::ensure!(
                close < Duration::from_secs(10) && took < Duration::from_secs(10),
                "A's {probe} right after its back close stalled (close {close:?}, \
                 {probe} {took:?})"
            );
            files.push(Acked {
                name: kept,
                hash: blake3::hash(&data),
                len: data.len(),
            });
        }
        let probe_uploads = a.control_status()?["ack"]["own_record_uploads"]
            .as_u64()
            .unwrap_or(0)
            - uploads_before;
        eprintln!("    {name} [{mode:?}]: own-record uploads for the probes {probe_uploads}");
        if mode != CloseRace::Backed {
            anyhow::ensure!(
                probe_uploads > 0,
                "no dependent op asked for A's upload: the dependency was not reproduced"
            );
        } else {
            let pending = a.control_status()?["writeback"]["pending_uploads"]
                .as_u64()
                .unwrap_or(0);
            anyhow::ensure!(
                probe_uploads == 0 && pending > 0,
                "a backed holder's stream should answer the dependent ops with the chunks \
                 still held (uploads {probe_uploads}, pending {pending})"
            );
        }
        // A reads its own writes at once.
        verify(&a, &files, Duration::from_secs(5))?;
        proxy.remove_all_toxics()?;
        let report = lifecycle(
            &a,
            json!({ "NetworkChanged": { "reachable": true, "metered": false } }),
        )?;
        anyhow::ensure!(
            report["status"]["uploads_held"] == false,
            "uploads still held: {}",
            report["status"]
        );
        for c in [&a, &b] {
            verify(c, &files, Duration::from_secs(60))?;
        }
        ensure_no_conflicts(&[&a, &b])
    })();
    if result.is_err() {
        for c in [&a, &b] {
            eprintln!("--- {} log ---\n{}", c.name, c.tail_log_n(60));
        }
    }
    let _ = a.unmount();
    let _ = b.unmount();
    result
}
