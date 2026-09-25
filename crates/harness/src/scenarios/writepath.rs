//! The small-file write path (the OVH run's finding 3): S3 round trips per
//! small-file close, on the sequencer and on a non-owner, in both write
//! modes.
//!
//! - `small-file-write-path`: under injected S3 latency, the sequencer and
//!   a non-owner each close a batch of small unique files, first under
//!   `--write-mode through`, then under `back`. Through: one S3 request
//!   per file on the writer (the conditional create: no `gc/condemned`
//!   read, no HEAD in front of it), and a close costs one S3 round trip.
//!   Back: a close costs no S3 round trip on either node — the non-owner
//!   forwards its manifest with the chunks still uploading, and the
//!   sequencer holds the manifest back until the non-owner reports them
//!   up. Every file then reads back right on a third node.
//! - `nonowner-back-crash`: under slow S3, a non-owner closes files under
//!   `back` and is killed with its uploads still in flight, then
//!   remounted: its journaled pending chunks go up after the restart, and
//!   the sequencer — which never shipped the manifests meanwhile, and
//!   whose own reader of one of the files waited for the bytes — ships
//!   them once it finds the chunks in S3 (the restarted node's reports
//!   died with it). Another node sees every file's content; nothing the
//!   log names is missing.

use super::m11::s3_breakdown;
use super::m8::dist;
use super::m9::cluster;
use super::{eventually, lease_of};
use crate::client::Client;
use crate::reqlog::CountingProxy;
use anyhow::{Context, Result};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::time::{Duration, Instant};

fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// `open(O_CREAT|O_EXCL)`, one write of bytes unique to this file (a new
/// chunk), `close`: timed.
fn unique_write(dir: &Path, tag: &str, i: usize) -> Result<Duration> {
    let body = content(tag, i);
    let t = Instant::now();
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o644)
        .open(dir.join(format!("{tag}-{i}")))?;
    f.write_all(&body)?;
    drop(f);
    Ok(t.elapsed())
}

fn content(tag: &str, i: usize) -> Vec<u8> {
    format!("{tag} file {i} ").repeat(64).into_bytes()
}

fn p50(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v.get(v.len() / 2).copied().unwrap_or_default()
}

/// Requests `p` saw, by method, on chunk objects and on the condemned
/// pointer.
fn chunk_requests(p: &CountingProxy) -> (usize, usize, usize) {
    let reqs = p.requests();
    let puts = reqs
        .iter()
        .filter(|r| r.method == "PUT" && r.touches("/chunks/"))
        .count();
    let heads = reqs
        .iter()
        .filter(|r| r.method == "HEAD" && r.touches("/chunks/"))
        .count();
    let condemned = reqs
        .iter()
        .filter(|r| r.method == "GET" && r.touches("gc/condemned.json"))
        .count();
    (puts, heads, condemned)
}

fn wait_content(reader: &Client, tag: &str, count: usize, within: Duration) -> Result<()> {
    eventually(
        &format!("{tag}: every file's content on {}", reader.name),
        within,
        || {
            for i in 0..count {
                let path = reader.mnt.join(format!("shared/{tag}-{i}"));
                let got = std::fs::read(&path).with_context(|| format!("{}", path.display()))?;
                anyhow::ensure!(
                    got == content(tag, i),
                    "{} reads {} bytes, not the {} written",
                    path.display(),
                    got.len(),
                    content(tag, i).len()
                );
            }
            Ok(())
        },
    )
}

fn awaited(c: &Client) -> Result<u64> {
    Ok(c.control_status()?["writeback"]["remote_chunks_awaited"]
        .as_u64()
        .unwrap_or(0))
}

pub fn small_file_write_path(_seed: u64) -> Result<()> {
    const NAME: &str = "small-file-write-path";
    let lat = knob("WRITEPATH_LAT_MS", 100);
    let count = knob("WRITEPATH_FILES", 12) as usize;
    let (env, _root, mut clients, proxies) = cluster(
        NAME,
        &["a", "b", "c"],
        // The root stays the only sequencer (a delegate would be a
        // different path): see `nonowner-op-latency`.
        &[("CONSTELLATION_DELEGATION_PLACEMENT", "off")],
        3,
    )?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        std::fs::create_dir(a.mnt.join("shared"))?;
        for x in &clients {
            eventually("shared visible", Duration::from_secs(30), || {
                anyhow::ensure!(x.mnt.join("shared").is_dir());
                Ok(())
            })?;
            for i in 0..3 {
                std::fs::write(x.mnt.join(format!("shared/warm-{}-{i}", x.name)), b"w")?;
            }
        }
        anyhow::ensure!(lease_of(a)?["held"] == true, "a does not hold the lease");
        let backups = super::m9::wait_for_backup(a, Duration::from_secs(60))?;
        eprintln!("    {NAME}: a's backups {backups:?}");
        std::thread::sleep(Duration::from_secs(1));
        env.existing_s3_proxy().latency(lat, 0)?;
        let rtt = Duration::from_millis(2 * lat);
        eprintln!("    {NAME}: S3 latency injected: every request >= {rtt:?}");
        let reader = &clients[2];
        let mut failures = Vec::new();
        for mode in ["through", "back"] {
            for x in &clients {
                x.set_write_mode(mode)?;
            }
            for (k, writer) in clients[..2].iter().enumerate() {
                let role = if k == 0 { "sequencer" } else { "non-owner" };
                let tag = format!("{mode}-{}", writer.name);
                let dir = writer.mnt.join("shared");
                for p in &proxies {
                    p.reset();
                }
                let mut lats = Vec::new();
                for i in 0..count {
                    lats.push(unique_write(&dir, &tag, i).with_context(|| format!("{tag} {i}"))?);
                }
                let median = p50(lats.clone());
                let closes_done = Instant::now();
                wait_content(reader, &tag, count, Duration::from_secs(90))?;
                let visible_after = closes_done.elapsed();
                // Back: the uploads ran after the closes; let them and
                // their reports finish before counting.
                eventually(
                    &format!("{tag}: uploads drained"),
                    Duration::from_secs(60),
                    || {
                        for c in &clients {
                            let s = c.control_status()?;
                            anyhow::ensure!(
                                s["writeback"]["pending_uploads"] == 0,
                                "{} still has pending uploads: {}",
                                c.name,
                                s["writeback"]
                            );
                        }
                        Ok(())
                    },
                )?;
                let (puts, heads, condemned) = chunk_requests(&proxies[k]);
                let (_, owner_heads, owner_condemned) = chunk_requests(&proxies[0]);
                let breakdown = proxies
                    .iter()
                    .zip(&clients)
                    .map(|(p, c)| format!("[{}] {}", c.name, s3_breakdown(p)))
                    .collect::<Vec<_>>()
                    .join(" ");
                eprintln!(
                    "    {NAME}: {mode:<7} {role:<9} close p50 {median:>10?} ({}); on {}: chunk \
                     PUTs {puts}, chunk HEADs {heads}, condemned GETs {condemned} for {count} \
                     files; readable on {} {visible_after:?} after the last close\n        S3: \
                     {breakdown}",
                    dist(lats),
                    writer.name,
                    reader.name
                );
                // The writer's S3 requests for its chunks: one create per
                // file, nothing in front of it.
                if puts < count || puts > count + 2 {
                    failures.push(format!(
                        "{mode} {role}: {puts} chunk PUTs for {count} unique files"
                    ));
                }
                if heads > 0 || condemned > 0 {
                    failures.push(format!(
                        "{mode} {role}: {heads} chunk HEADs and {condemned} condemned GETs in \
                         front of unique uploads"
                    ));
                }
                if k == 1 && (owner_heads > 1 || owner_condemned > 1) {
                    failures.push(format!(
                        "{mode} non-owner: the sequencer checked S3 itself ({owner_heads} HEADs, \
                         {owner_condemned} condemned GETs) instead of taking the report"
                    ));
                }
                match mode {
                    "through" if median >= rtt + rtt / 2 => failures.push(format!(
                        "through {role}: close p50 {median:?} is more than one S3 round trip \
                         ({rtt:?})"
                    )),
                    "back" if median >= rtt / 2 => failures.push(format!(
                        "back {role}: close p50 {median:?} >= half an S3 round trip ({:?})",
                        rtt / 2
                    )),
                    _ => {}
                }
            }
        }
        for c in &clients {
            anyhow::ensure!(awaited(c)? == 0, "{} still awaits forwarded chunks", c.name);
        }
        env.existing_s3_proxy().remove_all_toxics()?;
        anyhow::ensure!(failures.is_empty(), "{NAME}: {failures:#?}");
        Ok(())
    })();
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}

pub fn nonowner_back_crash(_seed: u64) -> Result<()> {
    const NAME: &str = "nonowner-back-crash";
    let count = knob("WRITEPATH_FILES", 12) as usize;
    // Slow enough that the writer is killed with its uploads still in
    // flight (every S3 request >= 2 x this).
    let lat = knob("WRITEPATH_CRASH_LAT_MS", 1000);
    // Counting relays chain through toxiproxy: the injected latency
    // applies to every node.
    let (env, _root, mut clients, _proxies) = cluster(
        NAME,
        &["a", "b", "c"],
        &[("CONSTELLATION_DELEGATION_PLACEMENT", "off")],
        3,
    )?;
    let result = (|| -> Result<()> {
        std::fs::create_dir(clients[0].mnt.join("shared"))?;
        for x in &clients {
            eventually("shared visible", Duration::from_secs(30), || {
                anyhow::ensure!(x.mnt.join("shared").is_dir());
                Ok(())
            })?;
            std::fs::write(x.mnt.join(format!("shared/warm-{}", x.name)), b"w")?;
        }
        anyhow::ensure!(lease_of(&clients[0])?["held"] == true, "a does not hold");
        // The writer is the non-owner that is not a's backup: a backup's
        // restart is its own failover path (M9), not this one.
        let backups = super::m9::wait_for_backup(&clients[0], Duration::from_secs(60))?;
        let w = if backups.contains(&super::m9::node_id(&clients[1])?) {
            2
        } else {
            1
        };
        let r = 3 - w;
        let (writer, reader) = (clients[w].name.clone(), clients[r].name.clone());
        eprintln!("    {NAME}: a's backups {backups:?}; writer {writer}, reader {reader}");
        clients[w].set_write_mode("back")?;
        env.existing_s3_proxy().latency(lat, 0)?;
        let rtt = Duration::from_millis(2 * lat);
        let dir = clients[w].mnt.join("shared");
        // Phase 1, the writer alive: a reader on the sequencer — the one
        // node with the manifests before the chunks are up — waits for
        // the bytes rather than failing.
        let tag = "alive";
        let lats = (0..count)
            .map(|i| unique_write(&dir, tag, i))
            .collect::<Result<Vec<_>>>()?;
        eprintln!(
            "    {NAME}: {writer} closed {count} files (S3 round trip >= {rtt:?}): {}",
            dist(lats)
        );
        let t = Instant::now();
        let got = std::fs::read(clients[0].mnt.join(format!("shared/{tag}-{}", count - 1)))
            .context("the sequencer's read of a chunk still uploading on the writer")?;
        anyhow::ensure!(
            got == content(tag, count - 1),
            "the sequencer read something else"
        );
        eprintln!(
            "    {NAME}: the sequencer's read waited {:?} for the bytes",
            t.elapsed()
        );
        wait_content(&clients[r], tag, count, Duration::from_secs(120))?;
        // Phase 2: the writer killed with its uploads in flight. The
        // sequencer awaits them and ships no manifest naming them.
        let tag = "crash";
        let lats = (0..count)
            .map(|i| unique_write(&dir, tag, i))
            .collect::<Result<Vec<_>>>()?;
        eprintln!("    {NAME}: {writer} closed {count} more: {}", dist(lats));
        clients[w].kill9()?;
        let awaiting = awaited(&clients[0])?;
        eprintln!("    {NAME}: {writer} killed; a awaits {awaiting} chunk(s)");
        anyhow::ensure!(
            awaiting > 0,
            "every upload landed before the kill: raise the latency"
        );
        for i in 0..count {
            let path = clients[r].mnt.join(format!("shared/{tag}-{i}"));
            if let Ok(got) = std::fs::read(&path) {
                anyhow::ensure!(
                    got.is_empty() || got == content(tag, i),
                    "{} reads {} bytes of something else before the upload",
                    path.display(),
                    got.len()
                );
            }
        }
        clients[w].mount()?;
        eprintln!("    {NAME}: {writer} remounted");
        let t = Instant::now();
        wait_content(&clients[r], tag, count, Duration::from_secs(180))?;
        eprintln!(
            "    {NAME}: every file readable on {reader} {:?} after the remount",
            t.elapsed()
        );
        eventually("a no longer awaits", Duration::from_secs(120), || {
            let n = awaited(&clients[0])?;
            anyhow::ensure!(n == 0, "a still awaits {n}");
            Ok(())
        })?;
        env.existing_s3_proxy().remove_all_toxics()?;
        let fsck = clients[r].fsck(false)?;
        let out = String::from_utf8_lossy(&fsck.stdout).to_string()
            + &String::from_utf8_lossy(&fsck.stderr);
        anyhow::ensure!(
            !out.contains("dangling_manifest_ref"),
            "fsck: a manifest names a chunk S3 lacks: {out}"
        );
        Ok(())
    })();
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}
