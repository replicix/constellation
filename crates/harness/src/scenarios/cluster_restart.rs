//! `p2p-cluster-restart`: forwarding to the root after nodes of a P2P
//! cluster are `kill -9`ed and restarted together.
//!
//! `git-under-flock-faults` seed 1001 killed all four nodes at once and
//! restarted them one after the other. Afterwards the forwards to the
//! root timed out for minutes, and the root logged inbound QUIC
//! "authentication failed". The cause was not identity: every node kept
//! its key. A UDP socket rebind (iroh rebinds on every major link change,
//! and container bridges come and go on a shared host about once a
//! second) lost the wakeup of a per-peer iroh actor that was sending a
//! dial's Initial packets. That actor's inbox filled up, and iroh's socket
//! actor, which hands every dial and every new connection to these actors
//! one at a time, blocked on it. The whole endpoint could then neither
//! dial nor hand over an accepted connection for 60 s (the stuck actor's
//! idle timer), while its peers' QUIC handshakes with it completed and
//! their requests went unanswered. A restart is when the most dials are
//! in flight, many of them to dead addresses. The fix is in
//! `vendor/netwatch` (see its `CONSTELLATION-PATCH.md`).
//!
//! The scenario: four nodes, each with its own node key kept in its state
//! dir (so a restarted node is the same `EndpointId` at a new port). `a`
//! creates the filesystem and holds the root lease at the start (it may move
//! to another node after the first round). Three rounds, with
//! the victims and their restart order drawn from the seed: the whole
//! cluster, then one node, then two. Each round warms every node's P2P
//! forward to the holder, `kill -9`s the victims at once, waits a moment,
//! and remounts them one by one in a random order, up to
//! `CONSTELLATION_CLUSTER_RESTART_GAP_MS` (default 7000) apart, so another victim may
//! still be down while one comes back. From the kill until the round is
//! measured, a docker bridge of the harness's own is created and removed
//! over and over ([`LinkChurn`]), the link churn a container host has
//! anyway (`CONSTELLATION_CLUSTER_RESTART_LINK_CHURN=0` turns it off). From the last
//! remount on, every node that does not hold the lease writes until a
//! write is a P2P forward (`forwarded_ok` grows, `inbox_ops` does not).
//! Each must succeed within [`RECOVERY_BOUND`] of the last remount.
//! Everything converges at the end.

use super::{ensure_no_conflicts, eventually, inbox_counter, lease_of, setup, ts, wait_for_peers};
use crate::client::Client;
use crate::docker::Network;
use crate::model::Model;
use crate::s3env::BUCKET;
use anyhow::{Context, Result};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const NAME: &str = "p2p-cluster-restart";

/// How long after the last node's remount every other node's write must
/// be a P2P forward to the root. A node that comes back reads the
/// registry, so it dials the root's current address at once, and one
/// handshake takes a round trip. The slowest legitimate path is a node
/// that was up all along and holds a dial to the root's dead previous
/// address: that dial fails after `DIAL_TIMEOUT` (5 s), and the
/// re-dial goes to the root's new address, learned from the root's own
/// connection to it or from the next registry read (every 5 s), so
/// 5 s + 5 s. Measured with the fix: at most 3 s, most of it the restarted
/// root re-adopting its lease. Without it: 50-60 s.
const RECOVERY_BOUND: Duration = Duration::from_secs(10);

/// Waited beyond [`RECOVERY_BOUND`] before giving up, only so that a
/// failure reports how long recovery really took.
const DIAGNOSTIC_WAIT: Duration = Duration::from_secs(60);

pub(super) fn p2p_cluster_restart(seed: u64) -> Result<()> {
    let mut rng = StdRng::seed_from_u64(seed);
    let max_gap_ms: u64 = std::env::var("CONSTELLATION_CLUSTER_RESTART_GAP_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(7000)
        .max(1);
    let churn =
        std::env::var("CONSTELLATION_CLUSTER_RESTART_LINK_CHURN").map_or(true, |v| v != "0");
    let (env, root) = setup(NAME)?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/cluster-restart-{}", ts());
    let names = ["a", "b", "c", "d"];
    let mut clients = Vec::new();
    for name in names {
        clients.push(
            Client::new(root.path(), name, &env.endpoint, &backend)?
                .with_own_node_key()
                .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "600000"),
        );
    }
    clients[0].fs_create()?;
    clients[0].mount()?;
    let mut model = Model::default();
    let mut n_files = 0u64;
    write(&clients[0], &mut model, &mut n_files)?;
    for c in &mut clients[1..] {
        c.mount()?;
    }
    wait_for_peers(&clients.iter().collect::<Vec<_>>())?;

    let mut rounds: Vec<Vec<usize>> = vec![(0..clients.len()).collect()];
    for k in [1, 2] {
        let mut all: Vec<usize> = (0..clients.len()).collect();
        all.shuffle(&mut rng);
        all.truncate(k);
        rounds.push(all);
    }
    let mut failures = Vec::new();
    for (round, victims) in rounds.iter().enumerate() {
        let mut order = victims.clone();
        order.shuffle(&mut rng);
        let names_of = |v: &[usize]| -> String {
            v.iter()
                .map(|&i| clients[i].name.as_str())
                .collect::<Vec<_>>()
                .join(",")
        };
        let what = format!(
            "round {round}: kill -9 {{{}}}, restart order {}",
            names_of(victims),
            names_of(&order)
        );
        // Every node forwards to the holder over P2P before the kill.
        warm(&clients, &mut model, &mut n_files, &what)?;
        let link_churn = churn.then(LinkChurn::start);
        for &i in victims {
            clients[i].kill9()?;
        }
        std::thread::sleep(Duration::from_millis(rng.random_range(200..1500)));
        for (k, &i) in order.iter().enumerate() {
            if k > 0 {
                std::thread::sleep(Duration::from_millis(rng.random_range(0..max_gap_ms)));
            }
            clients[i]
                .mount()
                .with_context(|| format!("{what}: remounting {}", clients[i].name))?;
        }
        let remounted = Instant::now();
        let took = recovery(&clients, &mut model, &mut n_files, remounted)?;
        if let Some(link_churn) = link_churn {
            link_churn.finish().with_context(|| what.clone())?;
        }
        let slowest = took.iter().map(|(_, d)| *d).max().unwrap_or_default();
        eprintln!(
            "    {NAME}: {what}: P2P forwards to the root after {}",
            took.iter()
                .map(|(n, d)| format!("{n} {} ms", d.as_millis()))
                .collect::<Vec<_>>()
                .join(", ")
        );
        if slowest > RECOVERY_BOUND {
            failures.push(format!(
                "{what}: a P2P forward to the root took {slowest:?} after the last remount \
                 (bound {RECOVERY_BOUND:?})"
            ));
        }
    }
    if !failures.is_empty() {
        keep_logs(&clients);
        anyhow::bail!("{}", failures.join("; "));
    }
    let refs: Vec<&Client> = clients.iter().collect();
    eventually("every node converges", Duration::from_secs(60), || {
        for c in &clients {
            model
                .verify(&c.mnt)
                .with_context(|| format!("via {}", c.name))?;
        }
        Ok(())
    })?;
    ensure_no_conflicts(&refs)?;
    for c in &mut clients {
        c.unmount()?;
    }
    Ok(())
}

/// Write one new file through `c`; returns whether it went to the root
/// as a P2P forward (not through the S3 inbox, not applied locally).
fn write(c: &Client, model: &mut Model, n: &mut u64) -> Result<bool> {
    let ok = c.control_status()?["forwarded_ok"].as_u64().unwrap_or(0);
    let inbox = inbox_counter(c, "inbox_ops")?;
    *n += 1;
    let name = format!("f-{}-{n}", c.name);
    let data = format!("{name} via {}", c.name).into_bytes();
    std::fs::write(c.mnt.join(&name), &data).with_context(|| format!("writing {name}"))?;
    model.write_file(Path::new(&name), data);
    let ok_after = c.control_status()?["forwarded_ok"].as_u64().unwrap_or(0);
    let inbox_after = inbox_counter(c, "inbox_ops")?;
    Ok(ok_after > ok && inbox_after == inbox)
}

fn holder(clients: &[Client]) -> Option<usize> {
    clients
        .iter()
        .position(|c| lease_of(c).is_ok_and(|l| l["held"] == true))
}

/// Until each node but the holder has had a write forwarded over P2P.
fn warm(clients: &[Client], model: &mut Model, n: &mut u64, what: &str) -> Result<()> {
    eventually(
        &format!("{what}: every node forwards to the holder over P2P"),
        Duration::from_secs(60),
        || {
            let h = holder(clients).context("no node holds the lease")?;
            for (i, c) in clients.iter().enumerate() {
                if i != h {
                    anyhow::ensure!(
                        write(c, model, n)?,
                        "{}'s write was not a P2P forward",
                        c.name
                    );
                }
            }
            Ok(())
        },
    )
}

/// Per node that does not hold the lease, how long after `since` its
/// first P2P-forwarded write landed. Writes that are not forwarded are
/// retried every 200 ms. Fails if some node gets none within
/// [`RECOVERY_BOUND`] + [`DIAGNOSTIC_WAIT`].
fn recovery(
    clients: &[Client],
    model: &mut Model,
    n: &mut u64,
    since: Instant,
) -> Result<Vec<(String, Duration)>> {
    let mut done: Vec<Option<Duration>> = vec![None; clients.len()];
    let deadline = RECOVERY_BOUND + DIAGNOSTIC_WAIT;
    loop {
        // The holder may only be known once a restarted root re-adopts
        // its lease; until then every node keeps writing.
        let h = holder(clients);
        for (i, c) in clients.iter().enumerate() {
            if Some(i) == h || done[i].is_some() {
                continue;
            }
            if write(c, model, n)? {
                done[i] = Some(since.elapsed());
            }
        }
        if let Some(h) = h {
            if (0..clients.len()).all(|i| i == h || done[i].is_some()) {
                return Ok(clients
                    .iter()
                    .zip(&done)
                    .filter_map(|(c, d)| d.map(|d| (c.name.clone(), d)))
                    .collect());
            }
        }
        if since.elapsed() > deadline {
            let missing: Vec<&str> = clients
                .iter()
                .zip(&done)
                .enumerate()
                .filter(|(i, (_, d))| Some(*i) != h && d.is_none())
                .map(|(_, (c, _))| c.name.as_str())
                .collect();
            keep_logs(clients);
            anyhow::bail!(
                "no P2P forward to the root (holder {:?}) from {missing:?} within {deadline:?} \
                 of the last remount",
                h.map(|h| clients[h].name.as_str())
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Copy every incarnation's mount log out of the scenario's temp dir.
fn keep_logs(clients: &[Client]) {
    let dir = std::env::temp_dir().join(format!("harness-{NAME}-logs-{}", ts()));
    if std::fs::create_dir_all(&dir).is_err() {
        return;
    }
    for c in clients {
        for log in c.log_files() {
            let file = log.file_name().unwrap_or_default().to_string_lossy();
            let _ = std::fs::copy(&log, dir.join(format!("{}-{file}", c.name)));
        }
    }
    eprintln!("    {NAME}: mount logs kept in {}", dir.display());
}

/// A docker bridge network of this harness run's own, created and removed
/// in a loop until dropped: an interface with an address appearing and
/// disappearing on the host, which iroh treats as a major link change
/// (it rebinds its sockets).
struct LinkChurn {
    stop: Arc<AtomicBool>,
    created: Arc<AtomicUsize>,
    last_error: Arc<Mutex<Option<String>>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl LinkChurn {
    /// One create or remove about every 300 ms (each `docker` call
    /// takes some 100 ms on its own).
    const PERIOD: Duration = Duration::from_millis(300);

    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let created = Arc::new(AtomicUsize::new(0));
        let last_error = Arc::new(Mutex::new(None));
        let name = format!("{}-linkchurn", crate::s3env::docker_prefix());
        let thread = {
            let (stop, created, last_error) = (stop.clone(), created.clone(), last_error.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    // Removed when dropped at the end of the iteration.
                    match Network::create(&name) {
                        Ok(net) => {
                            created.fetch_add(1, Ordering::Relaxed);
                            std::thread::sleep(Self::PERIOD);
                            drop(net);
                        }
                        Err(e) => *last_error.lock().unwrap() = Some(format!("{e:#}")),
                    }
                    std::thread::sleep(Self::PERIOD);
                }
            })
        };
        Self {
            stop,
            created,
            last_error,
            thread: Some(thread),
        }
    }

    /// Stop the churn. An error if not a single bridge could be created:
    /// without link changes the scenario would not exercise the bug.
    fn finish(mut self) -> Result<()> {
        self.join();
        if self.created.load(Ordering::Relaxed) == 0 {
            let why = self.last_error.lock().unwrap().take().unwrap_or_default();
            anyhow::bail!("link churn was requested but no docker bridge could be created: {why}");
        }
        Ok(())
    }

    fn join(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for LinkChurn {
    fn drop(&mut self) {
        self.join();
    }
}
