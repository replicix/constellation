//! `slow-s3-no-seal`: a live holder is never sealed by its backup because
//! S3 is slow.
//!
//! Campaign 6 follow-up: with every S3 request taking hundreds of
//! milliseconds to seconds (a bucket across an ocean, a throttled
//! endpoint), a backup sometimes stopped hearing from its live holder
//! for `backup_takeover_ms` (1.5 s) and sealed its epoch (`holder
//! silent`). Nothing is lost — the seal is safe — but the holder can no
//! longer acknowledge anything until it reconfigures, and every forward
//! to it fails or waits meanwhile. The backup channel is P2P only, so S3
//! latency must never silence it.
//!
//! Three nodes, product lease/sync/retry defaults, the root lease pinned
//! (placement and idle release off, so every lease change is a finding),
//! every S3 request >= 2 x `SLOWSEAL_LAT_MS` (default 750: 1.5 s). All
//! three nodes write small files (write-through), rename and mkdir for
//! `SLOWSEAL_SECS` (180) while the harness samples every node's `ack`
//! block. Asserts: no seal anywhere, the holder and its epoch unchanged,
//! and the holder kept a backup (it had one for at least 90 % of the run).
//!
//! Under a host-load profile (`CONSTELLATION_HARNESS_LOAD`) the last check
//! is replaced: a backup whose store is held up (a `store-stall` item: its
//! core commits nothing for seconds) acknowledges nothing and is dropped
//! by design (`backup_timeouts`; keeping it would park every `ack=backup`
//! reply for the stall), so the time a holder has no backup cannot be
//! bounded by a share of the run there. What stays: no seal, the lease and
//! epoch unchanged, and a backup brought back within [`REBACKUP`] of the
//! injected stalls ending (a backup-less stretch outside every stall's
//! window, or one still on at the end, fails). The time without a backup,
//! and the share of it inside the stall windows, is reported.

use super::lease_of;
use super::m9::{ack_of, cluster, wait_for_backup};
use crate::client::Client;
use anyhow::{Context, Result};
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

const UNSET: &str = "\u{0}unset";

/// Under a load profile: how long after the injected stalls end the
/// holder may still be without a backup (the reconfiguration rate limit,
/// the link and the catch-up).
const REBACKUP: Duration = Duration::from_secs(10);

/// Whether the interval `(from, to)` overlaps a store stall or the
/// [`REBACKUP`] after it.
fn excused(stalls: &[(Instant, Instant)], from: Instant, to: Instant) -> bool {
    stalls
        .iter()
        .any(|&(armed, end)| armed <= to && from <= end + REBACKUP)
}

/// One writer's loop: small files (a new chunk each), a rename and a
/// mkdir now and then, until `stop`.
fn writer(
    dir: std::path::PathBuf,
    tag: String,
    stop: Arc<AtomicBool>,
    ops: Arc<AtomicU64>,
) -> Result<()> {
    std::fs::create_dir_all(&dir)?;
    let mut i = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let p = dir.join(format!("f{i}"));
        let mut f = std::fs::File::create(&p).with_context(|| format!("{tag}: create {i}"))?;
        f.write_all(format!("{tag}:{i}:{}", "x".repeat(64)).as_bytes())?;
        drop(f);
        if i % 5 == 4 {
            std::fs::rename(&p, dir.join(format!("r{i}")))
                .with_context(|| format!("{tag}: rename {i}"))?;
        }
        if i % 7 == 6 {
            std::fs::create_dir(dir.join(format!("d{i}")))
                .with_context(|| format!("{tag}: mkdir {i}"))?;
        }
        ops.fetch_add(1, Ordering::Relaxed);
        i += 1;
        std::thread::sleep(Duration::from_millis(20));
    }
    Ok(())
}

fn seals(c: &Client) -> Result<u64> {
    Ok(ack_of(c)?["seals"].as_u64().unwrap_or(0))
}

pub fn slow_s3_no_seal(_seed: u64) -> Result<()> {
    const NAME: &str = "slow-s3-no-seal";
    let lat = knob("SLOWSEAL_LAT_MS", 750);
    let secs = knob("SLOWSEAL_SECS", 180);
    let filter = std::env::var("SLOWSEAL_RUST_LOG").unwrap_or_default();
    let mut extra: Vec<(&str, &str)> = vec![
        ("CONSTELLATION_SYNC_INTERVAL_MS", "500"),
        ("CONSTELLATION_LEASE_TTL_MS", "60000"),
        ("CONSTELLATION_S3_MAX_RETRIES", UNSET),
        ("CONSTELLATION_S3_RETRY_TIMEOUT_MS", UNSET),
        ("CONSTELLATION_LEASE_PLACEMENT", "off"),
        ("CONSTELLATION_DELEGATION_PLACEMENT", "off"),
    ];
    if !filter.is_empty() {
        extra.push(("RUST_LOG", filter.as_str()));
    }
    let (env, _root, mut clients, _proxies) = cluster(NAME, &["a", "b", "c"], &extra, 3)?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        let backups = wait_for_backup(a, Duration::from_secs(60))?;
        let lease0 = lease_of(a)?;
        let epoch0 = lease0["epoch"].as_u64().unwrap_or(0);
        eprintln!("    {NAME}: a holds epoch {epoch0}, backups {backups:?}");
        let seals0: Vec<u64> = clients.iter().map(seals).collect::<Result<_>>()?;
        env.existing_s3_proxy().latency(lat, 0)?;
        eprintln!(
            "    {NAME}: S3 latency injected: every request >= {:?}; writing for {secs} s",
            Duration::from_millis(2 * lat)
        );
        let stop = Arc::new(AtomicBool::new(false));
        let ops = Arc::new(AtomicU64::new(0));
        let handles: Vec<_> = clients
            .iter()
            .map(|c| {
                let dir = c.mnt.join(format!("w-{}", c.name));
                let (tag, stop, ops) = (c.name.clone(), stop.clone(), ops.clone());
                std::thread::spawn(move || writer(dir, tag, stop, ops))
            })
            .collect();
        let started = Instant::now();
        let mut failures = Vec::new();
        let mut backup_less = Duration::ZERO;
        // Under a load profile: the backup-less time inside the stalls'
        // windows, and the current run of it outside them.
        let loaded = crate::load::profile().is_some();
        let mut excused_less = Duration::ZERO;
        let mut unexcused_run = Duration::ZERO;
        let mut last = Instant::now();
        while started.elapsed() < Duration::from_secs(secs) {
            std::thread::sleep(Duration::from_secs(2));
            let dt = last.elapsed();
            last = Instant::now();
            for (k, c) in clients.iter().enumerate() {
                let s = seals(c)?;
                if s > seals0[k] {
                    failures.push(format!(
                        "{} sealed a live holder ({} seal(s)) at t+{:?}",
                        c.name,
                        s - seals0[k],
                        started.elapsed()
                    ));
                }
            }
            let l = lease_of(a)?;
            if l["held"] != true || l["epoch"].as_u64() != Some(epoch0) {
                failures.push(format!(
                    "a lost the lease at t+{:?}: {l}",
                    started.elapsed()
                ));
            }
            let ack = ack_of(a)?;
            if ack["backups"].as_array().is_none_or(|b| b.is_empty()) {
                backup_less += dt;
                if loaded {
                    let stalls = crate::load::store_stalls_since(started);
                    if excused(&stalls, last - dt, last) {
                        excused_less += dt;
                        unexcused_run = Duration::ZERO;
                    } else {
                        unexcused_run += dt;
                        if unexcused_run > REBACKUP {
                            failures.push(format!(
                                "the holder has been without a backup for {unexcused_run:?} \
                                 outside any injected stall (+{REBACKUP:?}) at t+{:?}",
                                started.elapsed()
                            ));
                        }
                    }
                }
            } else {
                unexcused_run = Duration::ZERO;
            }
            if !failures.is_empty() {
                break;
            }
        }
        stop.store(true, Ordering::Relaxed);
        for h in handles {
            h.join().map_err(|_| anyhow::anyhow!("writer panicked"))??;
        }
        for c in &clients {
            let s = c.control_status()?;
            eprintln!(
                "    {NAME}: {}: lease {} ack seals {} backups {} removed {} ack timeouts {} \
                 appends {} acks {}",
                c.name,
                s["lease"],
                s["ack"]["seals"],
                s["ack"]["backups"],
                s["ack"]["backups_removed"],
                s["ack"]["backup_ack_timeouts"],
                s["ack"]["backup_appends"],
                s["ack"]["backup_acks"]
            );
        }
        eprintln!(
            "    {NAME}: {} ops in {:?}; the holder had no backup for {:?}",
            ops.load(Ordering::Relaxed),
            started.elapsed(),
            backup_less
        );
        env.existing_s3_proxy().remove_all_toxics()?;
        if loaded {
            eprintln!(
                "    {NAME}: under load: {backup_less:?} without a backup, {excused_less:?} of it \
                 within an injected stall or {REBACKUP:?} after one"
            );
            // The stalls end (a stall armed late may still be on): a
            // backup must be back within `REBACKUP` of the last one.
            let stalls = crate::load::store_stalls_since(started);
            let settled = stalls
                .iter()
                .map(|&(_, end)| end)
                .max()
                .unwrap_or(started)
                .max(Instant::now())
                + REBACKUP;
            let back = loop {
                let ack = ack_of(a)?;
                if ack["backups"].as_array().is_some_and(|b| !b.is_empty()) {
                    break true;
                }
                if Instant::now() >= settled {
                    break false;
                }
                std::thread::sleep(Duration::from_secs(1));
            };
            if !back {
                failures.push(format!(
                    "no backup was brought back within {REBACKUP:?} after the stalls ended"
                ));
            }
        } else if backup_less > Duration::from_secs(secs / 10) {
            failures.push(format!(
                "the holder went without a backup for {backup_less:?} of {secs} s"
            ));
        }
        anyhow::ensure!(failures.is_empty(), "{NAME}: {failures:#?}");
        Ok(())
    })();
    if result.is_err() || std::env::var_os("SLOWSEAL_KEEP_LOGS").is_some() {
        super::ovh::keep_logs(NAME, &clients);
    }
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}
