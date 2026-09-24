//! One simulated run: N nodes on a current-thread runtime with paused
//! time, a seeded workload, a seeded fault plan, then quiescence and the
//! checks.

use super::bus::{Bus, StreamFaults};
use super::check;
use super::clock::Clock;
use super::history::{check_linearizable, check_linearizable_witnessed, History, NsOp, NsRet};
use super::node::{read_lease, CommitRecord, NodeEnv, NodeHandle};
use super::store::{Bucket, Fault, OpKind, Rule, When};
use constellation_authority::{ClientReply, Config, NodeId, Stats};
use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{Meta, MutateOp, MutateOutcome, Rid};
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A fault the plan schedules at a simulated time.
#[derive(Debug, Clone)]
pub enum FaultKind {
    /// Kill the current lease holder; restart it after `restart_ms`
    /// (`None`: never) with its journal (`keep_journal`) or fresh.
    CrashHolder {
        restart_ms: Option<u64>,
        keep_journal: bool,
    },
    CrashNode {
        node: NodeId,
        restart_ms: Option<u64>,
        keep_journal: bool,
    },
    /// Cut P2P between two nodes for `for_ms`.
    Partition { a: NodeId, b: NodeId, for_ms: u64 },
    /// Cut one node's path to S3 for `for_ms`.
    CutS3 { node: NodeId, for_ms: u64 },
    /// Stop a node for `for_ms` while its timers keep running.
    Pause { node: NodeId, for_ms: u64 },
    /// Plan 30 M0's fault knob: delay the holder's forward replies by
    /// `delay_ms` for `for_ms` (bug A's trigger).
    SlowReplies { delay_ms: u64, for_ms: u64 },
    /// A brand-new node (fresh id, no durable state, no clients) joins
    /// and bootstraps from the log: the "freshly bootstrapped replica" of
    /// M4's convergence check.
    JoinFresh,
    /// Scripted S3 error codes from now on (M4's `faulty.rs` idea).
    S3Rule(Rule),
}

#[derive(Debug, Clone)]
pub struct ScheduledFault {
    pub at_ms: u64,
    pub kind: FaultKind,
}

#[derive(Clone)]
pub struct SimConfig {
    pub nodes: u64,
    pub clients_per_node: u64,
    pub ops_per_client: u64,
    pub names: usize,
    pub s3_latency: (u64, u64),
    pub p2p_delay: (u64, u64),
    pub p2p_drop: f64,
    /// Random faults drawn from the seed, in addition to `faults`.
    pub random_faults: usize,
    pub faults: Vec<ScheduledFault>,
    /// Simulated time to wait for quiescence after the workload.
    pub settle_ms: u64,
    pub core: Arc<dyn Fn(NodeId, u32) -> Config + Send + Sync>,
    /// Test-only: node 1's driver panics after handling this many
    /// events, to pin that a panic fails the seed instead of hanging it.
    pub panic_after_events: Option<u64>,
    /// Plan 30 §M6: after each op, a client reads the name it touched
    /// with this probability (and a random name with a quarter of it).
    pub read_ratio: f64,
    /// Plan 30 §M6: reads go through the session wait
    /// (`Meta::session_ready`, polled), bounded by `session_wait_ms`.
    /// `false` is the non-vacuity knob: reads never wait.
    pub session_wait: bool,
    pub session_wait_ms: u64,
    /// Plan 30 §M7: faults on log-stream frames (loss, reorder, the
    /// holder dropping a subscriber).
    pub stream_faults: StreamFaults,
}

/// Core tunables scaled down for simulation (seconds, not minutes).
pub fn sim_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = Config::defaults(node_id, incarnation);
    c.ttl_ms = 6_000;
    c.expiry_margin_ms = 500;
    c.idle_release_ms = 3_000;
    c.dwell_ms = 1_500;
    c.wanted_grace_ms = 1_500;
    c.handoff_pause_ms = 800;
    c.forward_timeout_ms = 400;
    c.forward_retries = 3;
    c.forward_backoff_ms = 150;
    c.acquire_deadline_ms = 12_000;
    c.acquire_retry_min_ms = 50;
    c.acquire_retry_max_ms = 1_000;
    c.handoff_request_timeout_ms = 2_000;
    c.sync_interval_ms = 150;
    c.idle_max_ms = 2_000;
    c.tail_width = 8;
    c.publish_every = 4;
    c.publish_idle_ms = 5_000;
    c.replay_drain_ms = 200;
    c.replay_lease_fallback_ms = 5_000;
    c.held_tail_staleness_ms = 2_000;
    c.atime_ship_max_delay_ms = 60_000;
    c.inbox_warm_max_ms = 1_000;
    c.inbox_cold_max_ms = 2_000;
    c.inbox_hot_ms = 20;
    c.inbox_recheck_ms = 300;
    c.inbox_deadline_ms = 8_000;
    c.inbox_p2p_grace_ms = 600;
    c.escalate_window_ms = 4_000;
    c.escalate_ops = 8;
    c.escalate_wait_ms = 1_500;
    c.escalate_retry_ms = 800;
    c.stream_heartbeat_ms = 300;
    c.stream_timeout_ms = 1_000;
    c.stream_backstop_ms = 2_000;
    c.stream_ring_segments = 16;
    c.stream_buffer_segments = 32;
    c.stream_retry_min_ms = 150;
    c.stream_retry_max_ms = 2_000;
    c
}

/// The M13 configuration: P2P off, every non-holder write through the
/// holder's S3 inbox, escalation on sustained demand.
pub fn inbox_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = sim_core_config(node_id, incarnation);
    c.p2p = false;
    c
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            nodes: 3,
            clients_per_node: 2,
            ops_per_client: 6,
            names: 4,
            s3_latency: (5, 40),
            p2p_delay: (1, 15),
            p2p_drop: 0.0,
            random_faults: 2,
            faults: Vec::new(),
            settle_ms: 90_000,
            core: Arc::new(sim_core_config),
            panic_after_events: None,
            read_ratio: 0.0,
            session_wait: true,
            session_wait_ms: 2_000,
            stream_faults: StreamFaults::default(),
        }
    }
}

#[derive(Debug, Default)]
pub struct Report {
    pub seed: u64,
    pub ops_invoked: usize,
    pub ops_returned: usize,
    pub tentative: usize,
    pub segments: usize,
    pub fenced: usize,
    pub commits: usize,
    pub s3_puts: u64,
    pub s3_gets: u64,
    pub p2p_sent: u64,
    pub converged_checked: bool,
    /// The generic tester ran too (few tentative ops).
    pub stateright_checked: bool,
    /// Refusals explained only by an acked-then-rolled-back effect (L2).
    pub observed_tentative: usize,
    /// Plan 30 §M6: the per-node session checks.
    pub sessions: super::session::SessionReport,
    pub stats: BTreeMap<NodeId, Stats>,
    pub faults: Vec<String>,
    pub simulated_ms: u64,
}

pub struct Cluster {
    pub nodes: Mutex<BTreeMap<NodeId, Arc<NodeHandle>>>,
    pub env: NodeEnv,
}

impl Cluster {
    pub fn get(&self, id: NodeId) -> Arc<NodeHandle> {
        self.nodes.lock().unwrap()[&id].clone()
    }

    pub fn ids(&self) -> Vec<NodeId> {
        self.nodes.lock().unwrap().keys().copied().collect()
    }

    pub fn restart(&self, id: NodeId, meta: Option<Arc<Meta>>) {
        let handle = NodeHandle::start(&self.env, id, meta);
        self.nodes.lock().unwrap().insert(id, Arc::new(handle));
    }

    pub fn crash(&self, id: NodeId) -> Arc<Meta> {
        let node = self.get(id);
        node.crash(&self.env.bus);
        node.meta.clone()
    }
}

fn ns_ret(outcome: &MutateOutcome) -> Result<NsRet, String> {
    match outcome {
        MutateOutcome::Accepted { .. } => Ok(NsRet::Ok),
        MutateOutcome::Exists { .. } => Ok(NsRet::Eexist),
        MutateOutcome::Errno(e) if *e == libc::EEXIST => Ok(NsRet::Eexist),
        MutateOutcome::Errno(e) if *e == libc::ENOENT => Ok(NsRet::Enoent),
        other => Err(format!("unexpected client outcome {other:?}")),
    }
}

fn mutate_op(meta: &Meta, op: &NsOp) -> MutateOp {
    match op {
        NsOp::Create(name) => MutateOp::Create {
            parent: ROOT_INO,
            name: name.clone(),
            ino: meta.allocate_ino(ROOT_INO).expect("ino"),
            mode: 0o644,
            uid: 0,
            gid: 0,
        },
        NsOp::Unlink(name) => MutateOp::Unlink {
            parent: ROOT_INO,
            name: name.clone(),
        },
        NsOp::Rename(a, b) => MutateOp::Rename {
            parent: ROOT_INO,
            name: a.clone(),
            new_parent: ROOT_INO,
            new_name: b.clone(),
        },
    }
}

/// How many times a client resubmits an in-doubt op (same rid) before
/// leaving it in flight.
const MAX_RESUBMITS: u32 = 6;

/// The first panic of the current seed (any task: a node's core, a
/// client, a fault), recorded by the hook `run_seed` installs. A
/// panicking node task used to leave the run waiting for a reply or a
/// quiescence that never came (M5 round 5, long seed 11932: the test
/// sat at 0 % CPU); now the run ends with the panic as its failure.
///
/// Keyed by thread: every task of a run executes on the run's own
/// thread (a current-thread runtime under `block_on`), and seeds run in
/// parallel test threads, so one seed's panic must not fail another.
static PANICKED: Mutex<Vec<(std::thread::ThreadId, String)>> = Mutex::new(Vec::new());
static PANIC_HOOK: std::sync::Once = std::sync::Once::new();

fn install_panic_hook() {
    PANIC_HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let me = std::thread::current().id();
            let mut first = PANICKED.lock().unwrap();
            if !first.iter().any(|(t, _)| *t == me) {
                first.push((me, info.to_string()));
            }
            drop(first);
            previous(info);
        }));
    });
}

/// Resolves with the panic message once any task of this run panicked.
async fn panic_watch() -> String {
    let me = std::thread::current().id();
    loop {
        let hit = PANICKED
            .lock()
            .unwrap()
            .iter()
            .find(|(t, _)| *t == me)
            .map(|(_, m)| m.clone());
        if let Some(msg) = hit {
            return msg;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Stateright's `LinearizabilityTester` runs on a history only while it
/// has at most this many events and this many tentative ops (each a
/// synthetic thread in flight forever): its search is exponential in
/// overlapping ops, and a 72-op crash-heavy history did not finish in
/// minutes. The witnessed check is exact for this spec and always runs.
const STATERIGHT_EVENT_BOUND: usize = 80;
const STATERIGHT_TENTATIVE_BOUND: usize = 6;

/// One step of a simulated client.
#[derive(Clone, Debug)]
pub enum Step {
    Op(NsOp),
    /// Plan 30 §M6: look a name up in the local replica.
    Read(String),
}

/// Plan 30 §M6: a client read — the session wait (polled: this runtime
/// is single-threaded, so the blocking `Meta::session_wait` would stall
/// it), then the lookup.
async fn client_read(
    handle: &NodeHandle,
    history: &History,
    thread: u64,
    name: String,
    wait: Option<u64>,
) {
    let inv = history.tick();
    let keys = [constellation_meta::ReadKey::Dentry(ROOT_INO, name.clone())];
    let mut timed_out = false;
    if let Some(budget) = wait {
        let mut waited = 0;
        while !handle.meta.session_ready(&keys) {
            if waited >= budget || !handle.alive() {
                timed_out = waited >= budget;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
            waited += 5;
        }
    }
    if !handle.alive() {
        return;
    }
    let present = constellation_meta::MetaStore::lookup(&*handle.meta, ROOT_INO, &name)
        .ok()
        .flatten()
        .is_some();
    history.read(super::history::ReadEvt {
        node: handle.id,
        incarnation: handle
            .shared
            .incarnation
            .load(std::sync::atomic::Ordering::SeqCst),
        thread,
        name,
        present,
        inv,
        ret: history.tick(),
        timed_out,
    });
}

#[allow(clippy::too_many_arguments)]
async fn client_thread(
    cluster: Arc<Cluster>,
    history: Arc<History>,
    node: NodeId,
    thread: u64,
    steps: Vec<Step>,
    abandoned: Arc<Mutex<HashSet<Rid>>>,
    failures: Arc<Mutex<Vec<String>>>,
    pace_ms: u64,
    wait: Option<u64>,
) {
    for step in steps {
        let op = match step {
            Step::Op(op) => op,
            Step::Read(name) => {
                let handle = cluster.get(node);
                if handle.alive() {
                    client_read(&handle, &history, thread, name, wait).await;
                }
                continue;
            }
        };
        tokio::time::sleep(Duration::from_millis(pace_ms)).await;
        let handle = cluster.get(node);
        if !handle.alive() {
            // Wait for a restart before issuing anything new.
            let mut waited = 0;
            while !cluster.get(node).alive() && waited < 60_000 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                waited += 100;
            }
            if !cluster.get(node).alive() {
                return;
            }
        }
        let handle = cluster.get(node);
        let rid = handle.next_rid();
        let mop = mutate_op(&handle.meta, &op);
        history.invoke(thread, rid, op.clone());
        let mut attempts = 0u32;
        loop {
            let handle = cluster.get(node);
            super::node::note_waiting(
                &format!("client t{thread}"),
                format!("submit rid {:?} attempt {attempts}", rid),
            );
            if !handle.alive()
                || handle
                    .shared
                    .incarnation
                    .load(std::sync::atomic::Ordering::SeqCst)
                    != rid.incarnation
            {
                // The process died with the call in flight: the op is
                // neither confirmed nor denied to anyone, ever.
                abandoned.lock().unwrap().insert(rid);
                break;
            }
            let answer = handle.submit(rid, mop.clone()).await;
            super::node::note_waiting(&format!("client t{thread}"), "answered".into());
            match answer {
                Ok(ClientReply::Outcome(outcome)) => match ns_ret(&outcome) {
                    Ok(ret) => {
                        history.ret(thread, rid, ret);
                    }
                    Err(e) => failures.lock().unwrap().push(e),
                },
                Ok(ClientReply::InDoubt) | Err(_) => {
                    attempts += 1;
                    if attempts > MAX_RESUBMITS {
                        abandoned.lock().unwrap().insert(rid);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
            break;
        }
    }
}

fn gen_ops(rng: &mut StdRng, n: u64, names: usize) -> Vec<NsOp> {
    let pool: Vec<String> = (0..names).map(|i| format!("f{i}")).collect();
    (0..n)
        .map(|_| match rng.random_range(0..10) {
            0..=4 => NsOp::Create(pool.choose(rng).unwrap().clone()),
            5..=7 => NsOp::Unlink(pool.choose(rng).unwrap().clone()),
            _ => {
                let a = pool.choose(rng).unwrap().clone();
                let mut b = pool.choose(rng).unwrap().clone();
                if b == a {
                    b = pool[(pool.iter().position(|x| *x == a).unwrap() + 1) % pool.len()].clone();
                }
                NsOp::Rename(a, b)
            }
        })
        .collect()
}

/// Plan 30 §M6: interleave reads with `ops`.
fn with_reads(rng: &mut StdRng, ops: Vec<NsOp>, names: usize, ratio: f64) -> Vec<Step> {
    let mut out = Vec::new();
    for op in ops {
        let touched = match &op {
            NsOp::Create(n) | NsOp::Unlink(n) => n.clone(),
            NsOp::Rename(a, b) => {
                if rng.random_bool(0.5) {
                    a.clone()
                } else {
                    b.clone()
                }
            }
        };
        out.push(Step::Op(op));
        if ratio > 0.0 && rng.random_bool(ratio.min(1.0)) {
            out.push(Step::Read(touched));
        }
        if ratio > 0.0 && rng.random_bool((ratio / 4.0).min(1.0)) {
            out.push(Step::Read(format!("f{}", rng.random_range(0..names))));
        }
    }
    out
}

fn gen_faults(rng: &mut StdRng, cfg: &SimConfig, horizon_ms: u64) -> Vec<ScheduledFault> {
    let mut out = Vec::new();
    for _ in 0..cfg.random_faults {
        let at_ms = rng.random_range(500..horizon_ms.max(600));
        let node = rng.random_range(1..=cfg.nodes);
        let other = if cfg.nodes > 1 {
            let mut o = rng.random_range(1..=cfg.nodes);
            while o == node {
                o = rng.random_range(1..=cfg.nodes);
            }
            o
        } else {
            node
        };
        let kind = match rng.random_range(0..8) {
            // A restart always keeps its state dir: a node whose journal
            // is gone comes back under a new id (`JoinFresh`), never as
            // the same node with an empty journal — which production
            // refuses as "state dir reuse or id collision".
            0 | 1 => FaultKind::CrashHolder {
                restart_ms: Some(rng.random_range(1_000..8_000)),
                keep_journal: true,
            },
            2 => {
                if rng.random_bool(0.3) {
                    FaultKind::JoinFresh
                } else {
                    FaultKind::CrashNode {
                        node,
                        restart_ms: Some(rng.random_range(1_000..8_000)),
                        keep_journal: true,
                    }
                }
            }
            3 => FaultKind::Partition {
                a: node,
                b: other,
                for_ms: rng.random_range(500..6_000),
            },
            4 => FaultKind::CutS3 {
                node,
                for_ms: rng.random_range(300..3_000),
            },
            5 => FaultKind::Pause {
                node,
                for_ms: rng.random_range(300..7_000),
            },
            6 => FaultKind::SlowReplies {
                delay_ms: rng.random_range(500..1_500),
                for_ms: rng.random_range(1_000..5_000),
            },
            _ => {
                let fault = *[
                    Fault::Status(500),
                    Fault::Timeout,
                    Fault::AppliedThen(500),
                    Fault::AppliedThenTimeout,
                    Fault::Status(409),
                ]
                .choose(rng)
                .unwrap();
                let (op, pattern) = *[
                    (OpKind::Put, "log/"),
                    (OpKind::Put, "leases/"),
                    (OpKind::Get, "log/"),
                    (OpKind::Get, "leases/"),
                ]
                .choose(rng)
                .unwrap();
                FaultKind::S3Rule(Rule::new(
                    None,
                    op,
                    pattern,
                    When::Random(rng.random_range(0.02..0.25)),
                    fault,
                ))
            }
        };
        out.push(ScheduledFault { at_ms, kind });
    }
    out
}

async fn run_fault(cluster: Arc<Cluster>, fault: ScheduledFault, log: Arc<Mutex<Vec<String>>>) {
    tokio::time::sleep(Duration::from_millis(fault.at_ms)).await;
    let note = |s: String| log.lock().unwrap().push(s);
    match fault.kind {
        FaultKind::CrashHolder {
            restart_ms,
            keep_journal,
        } => {
            let Some(lease) = read_lease(&cluster.env.bucket).await else {
                note(format!("t={} crash-holder: no lease yet", fault.at_ms));
                return;
            };
            let holder = lease.holder;
            if holder == 0 || !cluster.ids().contains(&holder) || !cluster.get(holder).alive() {
                note(format!(
                    "t={} crash-holder: holder {holder} not alive",
                    fault.at_ms
                ));
                return;
            }
            note(format!(
                "t={} crash holder {holder} (restart {restart_ms:?}, keep_journal {keep_journal})",
                fault.at_ms
            ));
            let meta = cluster.crash(holder);
            if let Some(after) = restart_ms {
                tokio::time::sleep(Duration::from_millis(after)).await;
                cluster.restart(holder, keep_journal.then_some(meta));
            }
        }
        FaultKind::CrashNode {
            node,
            restart_ms,
            keep_journal,
        } => {
            if !cluster.get(node).alive() {
                return;
            }
            note(format!(
                "t={} crash node {node} (restart {restart_ms:?}, keep_journal {keep_journal})",
                fault.at_ms
            ));
            let meta = cluster.crash(node);
            if let Some(after) = restart_ms {
                tokio::time::sleep(Duration::from_millis(after)).await;
                cluster.restart(node, keep_journal.then_some(meta));
            }
        }
        FaultKind::Partition { a, b, for_ms } => {
            note(format!(
                "t={} partition {a}<->{b} for {for_ms}ms",
                fault.at_ms
            ));
            cluster.env.bus.set_partition(a, b, true);
            tokio::time::sleep(Duration::from_millis(for_ms)).await;
            cluster.env.bus.set_partition(a, b, false);
        }
        FaultKind::CutS3 { node, for_ms } => {
            note(format!(
                "t={} cut S3 for node {node} for {for_ms}ms",
                fault.at_ms
            ));
            cluster.env.bucket.set_cut(node, true);
            tokio::time::sleep(Duration::from_millis(for_ms)).await;
            cluster.env.bucket.set_cut(node, false);
        }
        FaultKind::Pause { node, for_ms } => {
            note(format!(
                "t={} pause node {node} for {for_ms}ms",
                fault.at_ms
            ));
            let until = tokio::time::Instant::now() + Duration::from_millis(for_ms);
            cluster.get(node).pause_until(until);
        }
        FaultKind::SlowReplies { delay_ms, for_ms } => {
            let Some(lease) = read_lease(&cluster.env.bucket).await else {
                return;
            };
            note(format!(
                "t={} slow replies from holder {} by {delay_ms}ms for {for_ms}ms",
                fault.at_ms, lease.holder
            ));
            cluster
                .env
                .bus
                .set_reply_delay(lease.holder, Some(delay_ms));
            tokio::time::sleep(Duration::from_millis(for_ms)).await;
            cluster.env.bus.set_reply_delay(lease.holder, None);
        }
        FaultKind::S3Rule(rule) => {
            note(format!("t={} S3 rule {rule:?}", fault.at_ms));
            cluster.env.bucket.script(rule);
        }
        FaultKind::JoinFresh => {
            let id = cluster.ids().into_iter().max().unwrap_or(0) + 1;
            note(format!("t={} fresh node {id} joins", fault.at_ms));
            cluster.restart(id, None);
        }
    }
}

/// Run one seed. `Err` carries the failure and the replay command.
pub fn run_seed(seed: u64, cfg: SimConfig) -> Result<Report, String> {
    // `AUTHORITY_SIM_WATCHDOG=1`: an OS thread reports any event whose
    // handling has taken more than two seconds of real time.
    if std::env::var_os("AUTHORITY_SIM_WATCHDOG").is_some() {
        std::thread::spawn(|| loop {
            std::thread::sleep(Duration::from_secs(2));
            if let Some((clock, hist)) = super::node::EVENT_HISTOGRAM.lock().unwrap().as_ref() {
                let mut top: Vec<(&String, &u64)> = hist.iter().collect();
                top.sort_by(|a, b| b.1.cmp(a.1));
                eprintln!(
                    "WATCHDOG: simulated clock {clock}, top events: {:?}",
                    &top[..top.len().min(6)]
                );
            }
            if let Some(waiting) = super::node::WAITING.lock().unwrap().as_ref() {
                eprintln!("WATCHDOG: waiting: {waiting:?}");
            }
            if let Some((node, event, since)) = super::node::IN_FLIGHT.lock().unwrap().clone() {
                if since.elapsed() > Duration::from_secs(2) {
                    eprintln!(
                        "WATCHDOG: node {node} has been handling an event for {:?}: {}",
                        since.elapsed(),
                        &event[..event.len().min(400)]
                    );
                }
            }
        });
    }
    // `RUST_LOG=constellation_authority=debug` narrates a replay.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .without_time()
        .try_init();
    install_panic_hook();
    let me = std::thread::current().id();
    PANICKED.lock().unwrap().retain(|(t, _)| *t != me);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("runtime");
    let result = rt.block_on(async {
        tokio::select! {
            r = run_inner(seed, cfg) => r,
            msg = panic_watch() => Err(format!("a task panicked: {msg}")),
        }
    });
    result.map_err(|e| {
        format!(
            "{e}\n  replay: AUTHORITY_SIM_SEED={seed} cargo test -p constellation-authority --test sim replay_seed -- --nocapture --exact"
        )
    })
}

async fn run_inner(seed: u64, cfg: SimConfig) -> Result<Report, String> {
    let mut rng = StdRng::seed_from_u64(seed);
    let clock = Clock::start();
    let bucket = Bucket::new(seed, cfg.s3_latency);
    let bus = Bus::new(seed, cfg.p2p_delay, cfg.p2p_drop);
    bus.set_stream_faults(cfg.stream_faults.clone());
    let commits: Arc<Mutex<Vec<CommitRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let env = NodeEnv {
        bucket: bucket.clone(),
        bus: bus.clone(),
        clock,
        commits: commits.clone(),
        config: cfg.core.clone(),
        panic_after_events: cfg.panic_after_events,
    };
    let cluster = Arc::new(Cluster {
        nodes: Mutex::new(BTreeMap::new()),
        env,
    });
    for id in 1..=cfg.nodes {
        cluster.restart(id, None);
    }
    let history = Arc::new(History::default());
    let abandoned = Arc::new(Mutex::new(HashSet::new()));
    let failures = Arc::new(Mutex::new(Vec::new()));
    let fault_log = Arc::new(Mutex::new(Vec::new()));

    // The workload.
    let mut clients = Vec::new();
    for node in 1..=cfg.nodes {
        for k in 0..cfg.clients_per_node {
            let ops = gen_ops(&mut rng, cfg.ops_per_client, cfg.names);
            let pace = rng.random_range(20..400);
            // Reads draw from their own generator, so a config without
            // reads replays exactly the schedules it had before M6.
            let mut read_rng = StdRng::seed_from_u64(seed ^ (node << 32) ^ k ^ 0x5e55);
            let steps = with_reads(&mut read_rng, ops, cfg.names, cfg.read_ratio);
            clients.push(tokio::spawn(client_thread(
                cluster.clone(),
                history.clone(),
                node,
                node * 16 + k,
                steps,
                abandoned.clone(),
                failures.clone(),
                pace,
                cfg.session_wait.then_some(cfg.session_wait_ms),
            )));
        }
    }
    // The faults.
    let horizon = cfg.ops_per_client * 400 + 2_000;
    let mut faults = cfg.faults.clone();
    faults.extend(gen_faults(&mut rng, &cfg, horizon));
    let mut fault_tasks = Vec::new();
    for f in faults {
        fault_tasks.push(tokio::spawn(run_fault(
            cluster.clone(),
            f,
            fault_log.clone(),
        )));
    }
    super::node::note_waiting("run", "clients".into());
    for c in clients {
        c.await.expect("client task");
    }
    super::node::note_waiting("run", "faults".into());
    for f in fault_tasks {
        f.await.expect("fault task");
    }
    super::node::note_waiting("run", "settle".into());
    // Every node that is still down and would come back has come back
    // by now (restart timers are inside the fault tasks). A node left
    // dead stays dead.

    // Settle: one more write from a live node. If the lease names a dead
    // node this brings the takeover forward (the model's
    // `failover_pending` exemption is only for the case nobody writes
    // again); and it ships one more segment, past the floor of any hint
    // an `EEXIST` refusal left outstanding (a hint retires only when the
    // applied position reaches the holder's next ship position, so an
    // idle cluster would otherwise keep it forever).
    let settle_thread = 1 << 20;
    if let Some(id) = cluster
        .ids()
        .into_iter()
        .find(|id| cluster.get(*id).alive())
    {
        let handle = cluster.get(id);
        let rid = handle.next_rid();
        let op = NsOp::Create("settle".into());
        let mop = mutate_op(&handle.meta, &op);
        history.invoke(settle_thread, rid, op);
        let mut done = false;
        for _ in 0..3 {
            match cluster.get(id).submit(rid, mop.clone()).await {
                Ok(ClientReply::Outcome(o)) => {
                    if let Ok(ret) = ns_ret(&o) {
                        history.ret(settle_thread, rid, ret);
                    }
                    done = true;
                    break;
                }
                _ => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }
        if !done {
            abandoned.lock().unwrap().insert(rid);
        }
    }

    // An explicit publish from whoever holds the lease now (a snapshot's
    // `PublishNow`): the commit-prefix check then always has a commit at
    // the head to compare, and the control path is exercised.
    if let Some(lease) = read_lease(&bucket).await {
        if cluster.ids().contains(&lease.holder) && cluster.get(lease.holder).alive() {
            let _ = cluster
                .get(lease.holder)
                .control(constellation_authority::Control::PublishNow)
                .await;
        }
    }

    super::node::note_waiting("run", "quiescence".into());
    // Quiescence: no client op in flight, every live replica exactly the
    // log prefix at head, no job in the slot.
    let log_raw = constellation_store_s3::LogStore::new(bucket.raw());
    let mut waited = 0u64;
    let mut quiescent = false;
    let mut extra_settles = 0u64;
    while waited < cfg.settle_ms {
        tokio::time::sleep(Duration::from_millis(250)).await;
        waited += 250;
        // A hint installed after the settle write (a replayed op refused
        // with `EEXIST` after the last segment shipped) retires only when
        // the log moves past its floor: write again, as a busy cluster
        // would. Production leaves such an idle hint be (M9's exit list).
        if waited.is_multiple_of(5_000) {
            let hinted = cluster
                .ids()
                .into_iter()
                .map(|id| cluster.get(id))
                .filter(|n| n.alive())
                .any(|n| n.view().speculation.outstanding > 0 && n.view().clients_in_flight == 0);
            if hinted {
                if let Some(id) = cluster
                    .ids()
                    .into_iter()
                    .find(|id| cluster.get(*id).alive())
                {
                    extra_settles += 1;
                    let handle = cluster.get(id);
                    let rid = handle.next_rid();
                    let op = NsOp::Create(format!("settle-{extra_settles}"));
                    let mop = mutate_op(&handle.meta, &op);
                    history.invoke(settle_thread + extra_settles, rid, op);
                    match handle.submit(rid, mop).await {
                        Ok(ClientReply::Outcome(o)) => {
                            if let Ok(ret) = ns_ret(&o) {
                                history.ret(settle_thread + extra_settles, rid, ret);
                            }
                        }
                        _ => {
                            abandoned.lock().unwrap().insert(rid);
                        }
                    }
                }
            }
        }
        let head = log_raw
            .list_segments()
            .await
            .map(|s| s.into_iter().max().unwrap_or(0))
            .unwrap_or(0);
        let live: Vec<Arc<NodeHandle>> = cluster
            .ids()
            .into_iter()
            .map(|id| cluster.get(id))
            .filter(|n| n.alive())
            .collect();
        let all = live.iter().all(|n| {
            let v = n.view();
            v.clients_in_flight == 0
                && v.job.is_none()
                && super::node::is_quiescent(&n.meta)
                && constellation_authority::Replica::applied_seq(&*n.meta).unwrap_or(0) >= head
        });
        if all {
            quiescent = true;
            break;
        }
    }

    // Checks.
    let mut report = Report {
        seed,
        ..Default::default()
    };
    report.faults = fault_log.lock().unwrap().clone();
    report.simulated_ms = clock.elapsed_ms();
    if let Some(f) = failures.lock().unwrap().first() {
        return Err(format!("{f}\n  faults: {:?}", report.faults));
    }
    let mut tentative: HashSet<Rid> = abandoned.lock().unwrap().clone();
    for id in cluster.ids() {
        let n = cluster.get(id);
        tentative.extend(n.shared.tentative.lock().unwrap().iter().copied());
        report.stats.insert(id, n.view().stats);
    }
    let commit_list = commits.lock().unwrap().clone();
    let snapshots: BTreeSet<u64> = commit_list.iter().map(|c| c.applied).collect();
    let oracle = check::replay_log(bucket.raw(), &snapshots).await;
    report.segments = oracle.segments;
    report.fenced = oracle.fenced;
    report.commits = commit_list.len();
    let events = history.events();
    report.ops_invoked = history.invoked();
    report.ops_returned = history.returned().len();
    report.tentative = tentative.len();
    let lin_context = |e: String| {
        let listing: Vec<String> = events
            .iter()
            .map(|e| match e {
                super::history::HistEvt::Invoke { thread, rid, op } => format!(
                    "  invoke t{thread} rid({},{},{}) {op:?}{}",
                    rid.node,
                    rid.incarnation,
                    rid.seq,
                    if tentative.contains(rid) {
                        " [tentative]"
                    } else {
                        ""
                    }
                ),
                super::history::HistEvt::Return { thread, rid, ret } => format!(
                    "  return t{thread} rid({},{},{}) {ret:?}",
                    rid.node, rid.incarnation, rid.seq
                ),
            })
            .collect();
        format!(
            "{e}\n  faults: {:?}\n  history:\n{}\n  log: {:#?}\n  stats: {:#?}",
            report.faults,
            listing.join("\n"),
            oracle.describe,
            report.stats
        )
    };
    // The log-witnessed check is exact for this spec and always runs; the
    // generic tester's search is exponential in ops left in flight, so it
    // runs only while few ops are tentative (see `history.rs`).
    let witness = check_linearizable_witnessed(&events, &tentative, &oracle.completed_at)
        .map_err(lin_context)?;
    report.observed_tentative = witness.observed_tentative;
    if events.len() <= STATERIGHT_EVENT_BOUND && tentative.len() <= STATERIGHT_TENTATIVE_BOUND {
        check_linearizable(&events, &tentative).map_err(lin_context)?;
        report.stateright_checked = true;
    }
    let counts = bucket.counts();
    report.s3_puts = counts.puts;
    report.s3_gets = counts.gets;
    report.p2p_sent = *bus.sent.lock().unwrap();
    let context = format!(
        "\n  faults: {:?}\n  log: {:#?}\n  stats: {:#?}",
        report.faults, oracle.describe, report.stats
    );
    check::check_commits(&commit_list, &oracle).map_err(|e| format!("{e}{context}"))?;

    let lease = read_lease(&bucket).await;
    let failover_pending = lease.as_ref().is_some_and(|l| {
        l.holder != 0
            && !l.released
            && !(cluster.ids().contains(&l.holder) && cluster.get(l.holder).alive())
    });
    let converged_checkable = quiescent && !failover_pending;
    if converged_checkable {
        let replicas: Vec<(u64, check::Dump)> = cluster
            .ids()
            .into_iter()
            .map(|id| cluster.get(id))
            .filter(|n| n.alive())
            .map(|n| (n.id, n.meta.ns_dump().expect("dump")))
            .collect();
        check::check_convergence(&replicas, &oracle).map_err(|e| format!("{e}{context}"))?;
        report.converged_checked = true;
    } else if !quiescent {
        return Err(format!(
            "the cluster did not reach quiescence within {}ms of simulated time\n  faults: {:?}\n  views: {:?}",
            cfg.settle_ms,
            report.faults,
            cluster
                .ids()
                .into_iter()
                .map(|id| (id, cluster.get(id).alive(), cluster.get(id).view()))
                .collect::<Vec<_>>()
        ));
    }
    // Plan 30 §M6: per-node read-your-writes and monotonic reads, with the
    // log as the witness (see `session.rs`). Enforced when reads wait.
    report.sessions = super::session::check_sessions(
        &events,
        &history.ticks(),
        &history.reads(),
        &tentative,
        &oracle.completed_at,
    );
    if cfg.session_wait {
        if let Some((checker, what)) = report.sessions.violations.first() {
            return Err(format!(
                "session guarantee violated ({checker}): {what}\n  faults: {:?}",
                report.faults
            ));
        }
    }
    check::check_exactly_once(
        &history.returned(),
        &tentative,
        &oracle,
        converged_checkable,
    )
    .map_err(|e| {
        format!(
            "{e}\n  faults: {:?}\n  log: {:#?}\n  stats: {:#?}",
            report.faults, oracle.describe, report.stats
        )
    })?;
    Ok(report)
}
