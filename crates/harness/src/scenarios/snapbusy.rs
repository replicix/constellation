//! Fix snap-drain-busy: snapshots of a busy holder under S3 latency.
//!
//! A snapshot's drain (`SyncRequest::Barrier`) used to wait for the
//! holder's *whole* journal to be empty at the end of a sync round, and
//! its publish (`publish_now`) refused while any row was unshipped. A
//! holder whose own writer rewrites a file every few milliseconds never
//! has an empty journal once S3 adds a few tens of milliseconds per
//! request, so `snapshot create` took 9–45 s or failed — "journal not
//! shipped: no lease" on the very node holding the lease, or a forwarded
//! batch's 30 s timeout — on real S3 and on floci behind a 25 ms relay,
//! while every floci run (sub-millisecond S3) passed.
//!
//! Two nodes; `c0` holds the root lease and replaces `project/counter`
//! (write, then rename over it) every 5 ms for the whole scenario; toxiproxy adds 25 ms to each
//! direction of every S3 request. Then, timed:
//!
//! 1. an idle snapshot first (no writer, same latency): the reference
//!    cost of the drain → publish → `snaps/` object chain;
//! 2. three snapshots taken on the holder itself, three forwarded from
//!    `c1`, and a clone on the holder (`clone.create`'s namespace
//!    barrier, the other `Barrier` user) — each must finish within
//!    [`bound`], and each snapshot must hold every write acknowledged
//!    before it was asked for, and none made after it returned;
//! 3. the lease never moved.
//!
//! The bound: with the fix, a busy snapshot costs what an idle one does
//! plus at most two segment PUTs (the one in flight when the barrier is
//! admitted, and the one carrying its position) and the publish's few
//! extra requests behind the writer's — a handful of round trips. So it
//! is 3× the idle snapshot plus 40 injected round trips (2 s at 50 ms),
//! which leaves room for a loaded CI host and is still an order of
//! magnitude under the 9–45 s the old drain took.

use super::{eventually, lease_of, setup, ts, wait_for_p2p};
use crate::client::Client;
use crate::s3env::BUCKET;
use anyhow::{Context, Result};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Added to each direction of every S3 request.
const LATENCY_MS: u64 = 25;
/// The holder's writer period.
const WRITE_EVERY: Duration = Duration::from_millis(5);

fn bound(idle: Duration) -> Duration {
    idle * 3 + Duration::from_millis(40 * 2 * LATENCY_MS)
}

/// The counter a snapshot (or clone) froze, read on `c` at `path`.
fn frozen_counter(c: &Client, path: &str) -> Result<u64> {
    let full = c.mnt.join(path);
    let mut value = 0;
    eventually(
        &format!("{path} is readable on {}", c.name),
        Duration::from_secs(20),
        || {
            value = std::fs::read_to_string(&full)?.trim().parse::<u64>()?;
            Ok(())
        },
    )?;
    Ok(value)
}

pub(super) fn snapshot_busy_latency(_seed: u64) -> Result<()> {
    let (env, root) = setup("snapshot-busy-latency")?;
    let proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/snap-busy-{}", ts());
    let tune = |c: Client, key: &str| {
        let _ = std::fs::remove_file(key);
        // A long idle release keeps the lease with c0 between its writes.
        c.with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "30000")
            .with_env("CONSTELLATION_NODE_KEY", key)
    };
    let mut c0 = tune(
        Client::new(root.path(), "c0", &env.endpoint, &backend)?,
        "/tmp/.constellation-snap-busy-c0.key",
    );
    c0.fs_create()?;
    c0.mount()?;
    let mut c1 = tune(
        Client::new(root.path(), "c1", &env.endpoint, &backend)?,
        "/tmp/.constellation-snap-busy-c1.key",
    );
    c1.mount()?;
    wait_for_p2p(&[&c0, &c1])?;
    std::fs::create_dir(c0.mnt.join("project"))?;
    std::fs::write(c0.mnt.join("project/counter"), b"0")?;
    eventually("c0 holds the lease", Duration::from_secs(20), || {
        let lease = lease_of(&c0)?;
        anyhow::ensure!(lease["held"] == true, "c0 does not hold the lease: {lease}");
        Ok(())
    })?;
    let before = lease_of(&c0)?;
    proxy.latency(LATENCY_MS, 0)?;

    let started = Instant::now();
    c0.snapshot_create("/project@idle")
        .context("the idle snapshot")?;
    let idle = started.elapsed();
    let limit = bound(idle);
    eprintln!(
        "    snapshot-busy-latency: idle snapshot {idle:?} at +{LATENCY_MS} ms each way; \
         bound {limit:?}"
    );

    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(AtomicU64::new(0));
    let writer = {
        let (mnt, stop, acked) = (c0.mnt.clone(), stop.clone(), acked.clone());
        std::thread::spawn(move || -> Result<u64> {
            let mut k = 0u64;
            while !stop.load(Ordering::Relaxed) {
                k += 1;
                // Write-then-rename: each step replaces the counter whole,
                // so any point in time holds a number (an in-place
                // rewrite's truncate is a state of its own, empty).
                std::fs::write(mnt.join("project/.next"), k.to_string())?;
                std::fs::rename(mnt.join("project/.next"), mnt.join("project/counter"))?;
                acked.store(k, Ordering::Relaxed);
                std::thread::sleep(WRITE_EVERY);
            }
            Ok(k)
        })
    };
    let mut timings = Vec::new();
    let phase = (|| -> Result<()> {
        // The writer runs a while before the first snapshot, so the
        // journal has a backlog to keep refilling.
        std::thread::sleep(Duration::from_millis(500));
        let mut take = |who: &Client, name: &str| -> Result<()> {
            let floor = acked.load(Ordering::Relaxed);
            let started = Instant::now();
            who.snapshot_create(&format!("/project@{name}"))
                .with_context(|| format!("snapshot {name} from {}", who.name))?;
            let took = started.elapsed();
            let ceiling = acked.load(Ordering::Relaxed) + 1;
            timings.push((name.to_string(), took));
            let frozen = frozen_counter(
                &c0,
                &format!("project/.constellation/snapshot/{name}/counter"),
            )?;
            eprintln!(
                "    snapshot-busy-latency: {name} from {} took {took:?}; froze counter \
                 {frozen} (acknowledged before: {floor}, by its return: <{ceiling})",
                who.name
            );
            anyhow::ensure!(
                took <= limit,
                "snapshot {name} from {} took {took:?} on a busy holder (bound {limit:?})",
                who.name
            );
            anyhow::ensure!(
                frozen >= floor,
                "snapshot {name} misses acknowledged writes: counter {frozen} < {floor}"
            );
            // The write in flight when it returned may be in it: it was
            // concurrent. Anything later may not. This bound is loose by
            // one by construction and only catches gross leaks; the lower
            // bound above is the real oracle.
            anyhow::ensure!(
                frozen <= ceiling,
                "snapshot {name} holds a write made after it returned: {frozen} > {ceiling}"
            );
            Ok(())
        };
        for i in 0..3 {
            take(&c0, &format!("holder{i}"))?;
            std::thread::sleep(Duration::from_millis(200));
        }
        for i in 0..3 {
            take(&c1, &format!("forwarded{i}"))?;
            std::thread::sleep(Duration::from_millis(200));
        }
        // `clone.create` on the holder: its namespace barrier is the
        // same position rule.
        let started = Instant::now();
        c0.clone_snapshot("/project@holder0", "/clone0")
            .context("clone on the busy holder")?;
        let took = started.elapsed();
        timings.push(("clone0".into(), took));
        eprintln!("    snapshot-busy-latency: clone on the holder took {took:?}");
        anyhow::ensure!(
            took <= limit,
            "clone on a busy holder took {took:?} (bound {limit:?})"
        );
        let original = frozen_counter(&c0, "project/.constellation/snapshot/holder0/counter")?;
        let cloned = frozen_counter(&c0, "clone0/counter")?;
        anyhow::ensure!(
            cloned == original,
            "the clone holds counter {cloned}, its snapshot {original}"
        );
        Ok(())
    })();
    stop.store(true, Ordering::Relaxed);
    let written = writer
        .join()
        .map_err(|_| anyhow::anyhow!("the writer panicked"))??;
    proxy.heal()?;
    phase?;
    let after = lease_of(&c0)?;
    eprintln!(
        "    snapshot-busy-latency: {written} writes; timings {timings:?}; \
         lease before {before} after {after}"
    );
    anyhow::ensure!(
        after["held"] == true
            && after["holder"] == before["holder"]
            && after["epoch"] == before["epoch"],
        "the lease moved: before {before}, after {after}"
    );
    anyhow::ensure!(lease_of(&c1)?["held"] != true, "c1 took the lease");
    c1.unmount()?;
    c0.unmount()
}
