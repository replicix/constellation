//! The control plane stays live while the daemon's shared runtime is not.
//!
//! `control-runtime-hog`: a daemon whose runtime has two workers is hit by
//! the runtime-hog fault (`CONSTELLATION_FAULT_RUNTIME_HOG_FILE`): two
//! tasks that each block a worker thread for 12 s (`thread::sleep`), so
//! the runtime runs nothing at all while the process, its other threads
//! and the host are fine. The control socket is not served by that
//! runtime (the control server's own thread, `cst-control`): `node.status`
//! must be answered, within the 10 s bound the harness gives it and in
//! well under a second, for the whole hog. Before, accept, Hello/Welcome
//! and the response each waited for a free worker: "the daemon did not
//! answer Hello" (seen under host load 5-21 as a flake of unrelated
//! scenarios).

use super::eventually;
use super::m9::{cluster, unmount_all};
use crate::client::Client;
use crate::procsnap;
use anyhow::{Context, Result};
use std::time::{Duration, Instant};

/// How many runtime workers of `c`'s daemon are inside a `nanosleep`: the
/// hog's blocked workers (an idle worker parks in `futex`).
fn sleeping_workers(c: &Client) -> Result<usize> {
    let pid = c.pid().context("no daemon pid")?;
    Ok(procsnap::threads(pid)
        .iter()
        .filter(|t| t.comm.starts_with("tokio-rt-worker"))
        .filter(|t| procsnap::classify_thread(t) == procsnap::Activity::Sleep)
        .count())
}

pub fn control_runtime_hog(_seed: u64) -> Result<()> {
    const NAME: &str = "control-runtime-hog";
    const WORKERS: usize = 2;
    let dir = tempfile::tempdir()?;
    let hog = dir.path().join("runtime-hog");
    let hog_str = hog.display().to_string();
    let (_env, _root, mut clients, _) = cluster(
        NAME,
        &["a"],
        &[
            ("CONSTELLATION_FAULT_RUNTIME_HOG_FILE", &hog_str),
            ("CONSTELLATION_TOKIO_THREADS", &WORKERS.to_string()),
        ],
        0,
    )?;
    let result = (|| -> Result<()> {
        let a = &clients[0];
        // The control: before the hog.
        a.control_call(
            "node.status",
            serde_json::json!({}),
            Duration::from_secs(10),
        )?;
        // Both workers blocked for 12 s, then (the file is removed below)
        // never again.
        std::fs::write(&hog, "2:12000:600000")?;
        eventually("both workers blocked", Duration::from_secs(10), || {
            let n = sleeping_workers(a)?;
            anyhow::ensure!(n >= WORKERS, "{n} of {WORKERS} workers blocked");
            Ok(())
        })?;
        let hog_seen = Instant::now();
        let mut slowest = Duration::ZERO;
        let mut calls = 0;
        // Fresh connections, one after the other: accept, Hello/Welcome,
        // the request, the response, each of them without a worker.
        while hog_seen.elapsed() < Duration::from_secs(7) {
            let t = Instant::now();
            a.control_call(
                "node.status",
                serde_json::json!({}),
                Duration::from_secs(10),
            )
            .with_context(|| {
                format!(
                    "node.status {:?} into the runtime hog (workers blocked: {:?})",
                    hog_seen.elapsed(),
                    sleeping_workers(a)
                )
            })?;
            slowest = slowest.max(t.elapsed());
            // The liveness probe's call: answered (a 12 s hog is under the
            // 30 s after which the daemon's ping fails).
            a.control_call("node.ping", serde_json::json!({}), Duration::from_secs(10))
                .context("node.ping into the runtime hog")?;
            calls += 1;
            std::thread::sleep(Duration::from_millis(100));
        }
        let still = sleeping_workers(a)?;
        anyhow::ensure!(
            still >= WORKERS,
            "the runtime was free again after {:?} ({still} of {WORKERS} workers blocked): the \
             calls proved nothing",
            hog_seen.elapsed()
        );
        eprintln!(
            "    {NAME}: {calls} node.status calls over {:?} with {still} of {WORKERS} workers \
             blocked, the slowest {slowest:?}",
            hog_seen.elapsed()
        );
        anyhow::ensure!(
            slowest < Duration::from_secs(2),
            "node.status took {slowest:?} with the runtime blocked"
        );
        // The runtime is back after the hog, and so is everything that
        // runs on it.
        std::fs::remove_file(&hog)?;
        eventually("the workers are free", Duration::from_secs(30), || {
            let n = sleeping_workers(a)?;
            anyhow::ensure!(n == 0, "{n} workers still blocked");
            Ok(())
        })?;
        std::fs::write(a.mnt.join("after"), b"x").context("a write after the hog")?;
        a.control_status()?;
        Ok(())
    })();
    if result.is_err() {
        for c in &clients {
            eprintln!("    {NAME}: {} log tail:\n{}", c.name, c.tail_log_n(60));
        }
    }
    unmount_all(&mut clients);
    result
}
