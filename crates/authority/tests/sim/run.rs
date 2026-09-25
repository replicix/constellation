//! One simulated run: N nodes on a current-thread runtime with paused
//! time, a seeded workload, a seeded fault plan, then quiescence and the
//! checks.

use super::bus::{Bus, StreamFaults};
use super::check;
use super::clock::Clock;
use super::history::dir_of;
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
    /// Plan 30 §M9: kill the holder's first listed backup (as the lease
    /// object names it); restart it after `restart_ms`.
    CrashBackup { restart_ms: Option<u64> },
    /// Plan 30 §M9: cut P2P between the holder and its first listed
    /// backup (S3 stays reachable to both) for `for_ms`.
    PartitionBackup { for_ms: u64 },
    /// Plan 30 §M9: cut the current holder's path to S3 for `for_ms` (it
    /// keeps acknowledging through its backup; what it journals meanwhile
    /// is the backup's tail).
    CutS3Holder { for_ms: u64 },
    /// Plan 30 §M10: an S3 outage for the current holder and `members −
    /// 1` other nodes (the lowest ids), which are also cut from every
    /// other node over P2P; the others keep S3. The sim's epoch
    /// coordinator (`epochs.rs`) forms a continuation epoch among the cut
    /// nodes when the flexible-quorum rule allows. Healed after `for_ms`.
    EpochOutage { members: usize, for_ms: u64 },
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
    /// Plan 30 §M8: reads are `cto=strict` (a delegation, or a ReadIndex
    /// through the core, then the wait at its position), and the
    /// close-to-open check is enforced.
    pub strict: bool,
    /// Plan 30 §M8: each node's clock is off by a seeded constant within
    /// `±clock_skew_ms` (leases and delegations are judged on it).
    pub clock_skew_ms: i64,
    /// Plan 30 §M9: the run's acknowledgements are durable (a backup
    /// within budget, or `ack=s3`): an acknowledged op is never
    /// tentative — its rollback is a failure, not an exemption — and a
    /// refusal explained only by a tentative effect is a failure.
    pub strict_durability: bool,
    /// Plan 30 §M9: pairs whose reported RTT is `ms` (out of budget when
    /// above it).
    pub rtts: Vec<((NodeId, NodeId), u64)>,
    /// Plan 30 §M10: the slack the sim's epoch coordinator forms epochs
    /// under (the cores' `Config::epoch_slack` must agree).
    pub epoch_slack: u32,
    /// Plan 30 §M11: directories under `/` created before the clients
    /// start; a client's names live in its node's home directory
    /// (`dirs[(node − 1) % dirs.len()]`, `""` when empty) with
    /// `cross_ratio` of its renames crossing into another one.
    pub dirs: Vec<String>,
    /// Plan 30 §M11: `(dir, node)` delegated by the initial holder
    /// (node 1) before the clients start.
    pub delegations: Vec<(String, NodeId)>,
    /// Plan 30 §M11 phase 2b: offline designations `(dir, designee)`,
    /// synced into the table by the root at setup.
    pub designations: Vec<(String, NodeId)>,
    pub cross_ratio: f64,
    /// Plan 30 §M11: a writer per node writes data in its home
    /// directory then a marker in another; watchers on every node must
    /// never see a marker without its data (`marker_order`).
    pub marker_pairs: u64,
    /// Random faults may include `JoinFresh`. Off for the M10 configs: a
    /// node enrolled during an epoch is plan 30 §M10's documented gap
    /// (`flex_node_enrolled_during_an_epoch_is_the_known_gap`).
    pub join_fresh: bool,
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
    // Plan 30 §M11 phase 2b: the placement is opted into per configuration
    // (`placement_core_config`); the 2a configurations are what they were.
    c.placement = false;
    c
}

/// The M13 configuration: P2P off, every non-holder write through the
/// holder's S3 inbox, escalation on sustained demand.
pub fn inbox_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = sim_core_config(node_id, incarnation);
    c.p2p = false;
    c
}

/// Plan 30 §M9: the backup configuration. The bus's default RTT (twice
/// the max delay: 30 ms) is within the budget, so a LAN backup is chosen;
/// the seal/takeover, ack-timeout and heartbeat clocks are scaled like
/// the rest (the lease TTL is 6 s here).
pub fn backup_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = sim_core_config(node_id, incarnation);
    c.backup_rtt_budget_ms = 50;
    c.backup_takeover_ms = 1_000;
    c.backup_ack_timeout_ms = 600;
    c.backup_heartbeat_ms = 200;
    c.backup_stable_ms = 200;
    c.backup_reconfig_min_ms = 500;
    c
}

/// Plan 30 §M11 phase 2b: the placement on, with thresholds a short
/// workload reaches (a 2 s window, 6 ops, a 1.5 s dwell, a 1 s
/// cool-down).
pub fn placement_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = sim_core_config(node_id, incarnation);
    c.placement = true;
    c.placement_window_ms = 2_000;
    c.placement_min_ops = 6;
    c.placement_dwell_ms = 1_500;
    c.placement_cooldown_ms = 1_000;
    c
}

/// Plan 30 §M11 phase 2b: delegates with backups (M9's backup
/// configuration: the bus RTT is in budget, so every delegate picks one).
pub fn deleg_backup_core_config(node_id: NodeId, incarnation: u32) -> Config {
    backup_core_config(node_id, incarnation)
}

/// Plan 30 §M9: `ack=s3` — no backups, every acknowledgement waits for
/// the segment, fast takeover on holder silence.
pub fn ack_s3_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = backup_core_config(node_id, incarnation);
    c.ack_s3 = true;
    c
}

/// Plan 30 §M10: flexible-quorum continuation epochs with `f = 1`
/// (the promise TTL a quarter of the 6 s lease; P2P requests for
/// promises answered within 400 ms).
pub fn flex_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = sim_core_config(node_id, incarnation);
    c.epoch_slack = 1;
    c.promise_ttl_ms = c.ttl_ms / 4;
    c.promise_wait_ms = 400;
    c.promise_watch_ms = 1_000;
    c
}

/// Plan 30 §M10 with M9's backups in budget: the claim rule decides
/// whether the epoch carries the holder's `Backup` lease (its backup a
/// member), and an epoch hold acknowledges locally.
pub fn flex_backup_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let b = backup_core_config(node_id, incarnation);
    let mut c = flex_core_config(node_id, incarnation);
    c.backup_rtt_budget_ms = b.backup_rtt_budget_ms;
    c.backup_takeover_ms = b.backup_takeover_ms;
    c.backup_ack_timeout_ms = b.backup_ack_timeout_ms;
    c.backup_heartbeat_ms = b.backup_heartbeat_ms;
    c.backup_stable_ms = b.backup_stable_ms;
    c.backup_reconfig_min_ms = b.backup_reconfig_min_ms;
    c
}

/// The same with the takeover's promise check off (the non-vacuity knob).
pub fn flex_unchecked_core_config(node_id: NodeId, incarnation: u32) -> Config {
    let mut c = flex_core_config(node_id, incarnation);
    c.takeover_promise_check = false;
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
            strict: false,
            clock_skew_ms: 0,
            strict_durability: false,
            rtts: Vec::new(),
            epoch_slack: 0,
            join_fresh: true,
            dirs: Vec::new(),
            delegations: Vec::new(),
            designations: Vec::new(),
            cross_ratio: 0.0,
            marker_pairs: 0,
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
    /// Plan 30 §M8: close-to-open (enforced under `strict`).
    pub cto: super::cto::CtoReport,
    pub stats: BTreeMap<NodeId, Stats>,
    pub faults: Vec<String>,
    pub simulated_ms: u64,
    /// Plan 30 §M9: acknowledged ops that were rolled back at some point
    /// (exempted as tentative unless `strict_durability`).
    pub acked_rolled_back: usize,
    /// Plan 30 §M9: per holder crash, simulated ms from the crash to the
    /// first mutation any node acknowledged afterwards.
    pub failover_ms: Vec<u64>,
    /// Plan 30 §M10: continuation epochs formed (and with a roster node
    /// missing), and the authority samples taken.
    pub epochs_formed: usize,
    pub epochs_missing_node: usize,
    pub authority_samples: u64,
    /// Plan 30 §M11: marker pairs checked and violations seen.
    pub marker_checks: u64,
    pub marker_violations: u64,
    /// Plan 30 §M11: per-directory checks run.
    pub dirs_checked: usize,
    /// `InDoubt` answers sent (every one checked against the log by
    /// `check::check_in_doubt_answers`).
    pub in_doubt_answers: usize,
}

pub struct Cluster {
    pub nodes: Mutex<BTreeMap<NodeId, Arc<NodeHandle>>>,
    pub env: NodeEnv,
    /// Plan 30 §M10: epochs formed (members), and single-authority
    /// violations seen by the sampler.
    pub epochs: Mutex<Vec<Vec<NodeId>>>,
    pub split_brains: Mutex<Vec<String>>,
    pub slack: u32,
    /// Plan 30 §M9: holder crashes (simulated ms) and, once known, the
    /// first acknowledgement after each.
    pub failovers: Mutex<Vec<(u64, Option<u64>)>>,
}

impl Cluster {
    pub fn get(&self, id: NodeId) -> Arc<NodeHandle> {
        self.nodes.lock().unwrap()[&id].clone()
    }

    pub fn ids(&self) -> Vec<NodeId> {
        self.nodes.lock().unwrap().keys().copied().collect()
    }

    pub fn restart(&self, id: NodeId, meta: Option<Arc<Meta>>) {
        // Plan 30 §M10: the epoch state is persisted with the journal.
        let epoch = match (&meta, self.nodes.lock().unwrap().get(&id)) {
            (Some(_), Some(old)) => old.shared.epoch.lock().unwrap().clone(),
            _ => Default::default(),
        };
        let handle = NodeHandle::start_with(&self.env, id, meta, epoch);
        self.nodes.lock().unwrap().insert(id, Arc::new(handle));
    }

    pub fn crash(&self, id: NodeId) -> Arc<Meta> {
        let node = self.get(id);
        node.crash(&self.env.bus);
        node.meta.clone()
    }

    /// A mutation was acknowledged now: the open failover, if any, ends.
    fn note_ack(&self) {
        let now = self.env.clock.elapsed_ms();
        if let Some(f) = self.failovers.lock().unwrap().last_mut() {
            if f.1.is_none() {
                f.1 = Some(now.saturating_sub(f.0));
            }
        }
    }
}

fn ns_ret(outcome: &MutateOutcome) -> Result<NsRet, String> {
    match outcome {
        MutateOutcome::Accepted { .. } => Ok(NsRet::Ok),
        MutateOutcome::Exists { .. } => Ok(NsRet::Eexist),
        MutateOutcome::Errno(e) if *e == libc::EEXIST => Ok(NsRet::Eexist),
        MutateOutcome::Errno(e) if *e == libc::ENOENT => Ok(NsRet::Enoent),
        MutateOutcome::Errno(e) if *e == libc::EROFS || *e == libc::EXDEV => Ok(NsRet::Erofs),
        other => Err(format!("unexpected client outcome {other:?}")),
    }
}

/// Plan 30 §M11: `"d1/x"` is name `x` in directory `d1` (created under
/// the root before the clients start); a bare name is in the root.
fn split_name(meta: &Meta, name: &str) -> (u64, String) {
    match name.rfind('/') {
        Some(i) => {
            let dir = &name[..i];
            let parent = meta
                .child_ino(ROOT_INO, dir)
                .ok()
                .flatten()
                .unwrap_or(ROOT_INO);
            (parent, name[i + 1..].to_string())
        }
        None => (ROOT_INO, name.to_string()),
    }
}

fn mutate_op(meta: &Meta, op: &NsOp) -> MutateOp {
    match op {
        NsOp::Create(name) => {
            let (parent, name) = split_name(meta, name);
            MutateOp::Create {
                parent,
                name,
                ino: meta.allocate_ino(parent).expect("ino"),
                mode: 0o644,
                uid: 0,
                gid: 0,
            }
        }
        NsOp::Unlink(name) => {
            let (parent, name) = split_name(meta, name);
            MutateOp::Unlink { parent, name }
        }
        NsOp::Rename(a, b) => {
            let (parent, name) = split_name(meta, a);
            let (new_parent, new_name) = split_name(meta, b);
            MutateOp::Rename {
                parent,
                name,
                new_parent,
                new_name,
            }
        }
        NsOp::Put(_) => unreachable!("Put is a projection-only op"),
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
    strict: bool,
) {
    let inv = history.tick();
    let (parent, leaf) = split_name(&handle.meta, &name);
    let keys = [constellation_meta::ReadKey::Dentry(parent, leaf.clone())];
    let mut timed_out = false;
    // Plan 30 §M8: a strict lookup — local under a delegation on the
    // directory, else a ReadIndex through the core (answered `Holder` on
    // the sequencer) — then the session wait at the answer's position.
    let mut floor = constellation_meta::Position::ZERO;
    if strict {
        let now = handle.clock.now().0;
        let ask = |handle: &NodeHandle| {
            handle.control(constellation_authority::Control::ReadIndex {
                ino: parent,
                dir: true,
                name: Some(leaf.clone()),
            })
        };
        match handle.meta.read_delegations().valid(parent, now) {
            Some((held, renew)) => {
                floor = held.position;
                if renew {
                    drop(ask(handle));
                }
            }
            None => match ask(handle).await {
                Ok(Ok(constellation_authority::ControlOk::ReadIndex(
                    constellation_authority::ReadAnswer::Position { position, .. },
                ))) => floor = position,
                Ok(Ok(constellation_authority::ControlOk::ReadIndex(
                    constellation_authority::ReadAnswer::Degraded,
                )))
                | Ok(Err(_))
                | Err(_) => timed_out = true,
                // The sequencer's own replica, or tailed to head.
                Ok(Ok(_)) => {}
            },
        }
    }
    if let Some(budget) = wait {
        let mut waited = 0;
        while !handle.meta.session_ready_at(&keys, &floor) {
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
    let present = constellation_meta::MetaStore::lookup(&*handle.meta, parent, &leaf)
        .ok()
        .flatten()
        .is_some();
    tracing::debug!(
        node = handle.id,
        now = handle.clock.now().0,
        name,
        present,
        timed_out,
        ?floor,
        held = handle.meta.read_delegations().valid(parent, handle.clock.now().0).is_some(),
        replays = handle.meta.pending_replays().map(|q| q.len()).unwrap_or(0),
        speculation = ?handle.meta.speculation_counts().ok(),
        "client read"
    );
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
    strict: bool,
) {
    for step in steps {
        let op = match step {
            Step::Op(op) => op,
            Step::Read(name) => {
                let handle = cluster.get(node);
                if handle.alive() {
                    client_read(&handle, &history, thread, name, wait, strict).await;
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
                // Plan 30 §M10: a frozen continuation epoch refuses writes
                // with `EROFS` before executing them; the FUSE caller
                // retries (here: the same rid, like an in-doubt answer).
                Ok(ClientReply::Outcome(MutateOutcome::Errno(e))) if e == libc::EROFS => {
                    attempts += 1;
                    if attempts > MAX_RESUBMITS {
                        abandoned.lock().unwrap().insert(rid);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
                Ok(ClientReply::Outcome(outcome)) => match ns_ret(&outcome) {
                    Ok(ret) => {
                        if ret == NsRet::Ok {
                            cluster.note_ack();
                        }
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

/// Plan 30 §M11: ops on `home`'s names (`home/f<i>`), with `cross_ratio`
/// of the renames moving a name into another directory of `dirs`.
fn gen_ops_in(
    rng: &mut StdRng,
    n: u64,
    names: usize,
    home: &str,
    dirs: &[String],
    cross_ratio: f64,
) -> Vec<NsOp> {
    let prefix = |d: &str, i: usize| {
        if d.is_empty() {
            format!("f{i}")
        } else {
            format!("{d}/f{i}")
        }
    };
    let pool: Vec<String> = (0..names).map(|i| prefix(home, i)).collect();
    (0..n)
        .map(|_| match rng.random_range(0..10) {
            0..=4 => NsOp::Create(pool.choose(rng).unwrap().clone()),
            5..=7 => NsOp::Unlink(pool.choose(rng).unwrap().clone()),
            _ => {
                let a = pool.choose(rng).unwrap().clone();
                let others: Vec<&String> = dirs.iter().filter(|d| d.as_str() != home).collect();
                if !others.is_empty() && rng.random_bool(cross_ratio.min(1.0)) {
                    let d = others.choose(rng).unwrap();
                    let b = prefix(d, rng.random_range(0..names));
                    return NsOp::Rename(a, b);
                }
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
            NsOp::Create(n) | NsOp::Unlink(n) | NsOp::Put(n) => n.clone(),
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
            out.push(Step::Read(touched.clone()));
        }
        if ratio > 0.0 && rng.random_bool((ratio / 4.0).min(1.0)) {
            let random = format!("f{}", rng.random_range(0..names));
            let random = match dir_of(&touched) {
                "" => random,
                d => format!("{d}/{random}"),
            };
            out.push(Step::Read(random));
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
                if rng.random_bool(0.3) && cfg.join_fresh {
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
            cluster
                .failovers
                .lock()
                .unwrap()
                .push((cluster.env.clock.elapsed_ms(), None));
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
        FaultKind::CrashBackup { restart_ms } => {
            let Some(lease) = read_lease(&cluster.env.bucket).await else {
                note(format!("t={} crash-backup: no lease yet", fault.at_ms));
                return;
            };
            let Some(&backup) = lease.backups.first() else {
                note(format!("t={} crash-backup: no backup listed", fault.at_ms));
                return;
            };
            if !cluster.ids().contains(&backup) || !cluster.get(backup).alive() {
                return;
            }
            note(format!(
                "t={} crash backup {backup} of holder {} (restart {restart_ms:?})",
                fault.at_ms, lease.holder
            ));
            let meta = cluster.crash(backup);
            if let Some(after) = restart_ms {
                tokio::time::sleep(Duration::from_millis(after)).await;
                cluster.restart(backup, Some(meta));
            }
        }
        FaultKind::CutS3Holder { for_ms } => {
            let Some(lease) = read_lease(&cluster.env.bucket).await else {
                return;
            };
            note(format!(
                "t={} cut S3 for holder {} for {for_ms}ms",
                fault.at_ms, lease.holder
            ));
            cluster.env.bucket.set_cut(lease.holder, true);
            tokio::time::sleep(Duration::from_millis(for_ms)).await;
            cluster.env.bucket.set_cut(lease.holder, false);
        }
        FaultKind::EpochOutage { members, for_ms } => {
            super::epochs::epoch_outage(cluster.clone(), fault.at_ms, members, for_ms, log.clone())
                .await;
        }
        FaultKind::PartitionBackup { for_ms } => {
            let Some(lease) = read_lease(&cluster.env.bucket).await else {
                note(format!("t={} partition-backup: no lease yet", fault.at_ms));
                return;
            };
            let Some(&backup) = lease.backups.first() else {
                note(format!(
                    "t={} partition-backup: no backup listed",
                    fault.at_ms
                ));
                return;
            };
            note(format!(
                "t={} partition holder {} <-> backup {backup} for {for_ms}ms",
                fault.at_ms, lease.holder
            ));
            cluster.env.bus.set_partition(lease.holder, backup, true);
            tokio::time::sleep(Duration::from_millis(for_ms)).await;
            cluster.env.bus.set_partition(lease.holder, backup, false);
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
            if let Some(gaps) = super::node::TIMER_GAPS.lock().unwrap().as_ref() {
                eprintln!("WATCHDOG: timer gaps (last, count, min gap ms): {gaps:?}");
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
    let in_doubt: super::node::InDoubtLog = Arc::new(Mutex::new(Vec::new()));
    let env = NodeEnv {
        bucket: bucket.clone(),
        bus: bus.clone(),
        clock,
        commits: commits.clone(),
        in_doubt: in_doubt.clone(),
        config: cfg.core.clone(),
        panic_after_events: cfg.panic_after_events,
        clock_skew_ms: cfg.clock_skew_ms,
        seed,
    };
    let cluster = Arc::new(Cluster {
        nodes: Mutex::new(BTreeMap::new()),
        env,
        failovers: Mutex::new(Vec::new()),
        epochs: Mutex::new(Vec::new()),
        split_brains: Mutex::new(Vec::new()),
        slack: cfg.epoch_slack,
    });
    for ((a, b), ms) in &cfg.rtts {
        bus.set_rtt(*a, *b, *ms);
    }
    for id in 1..=cfg.nodes {
        cluster.restart(id, None);
    }
    let history = Arc::new(History::default());
    let abandoned = Arc::new(Mutex::new(HashSet::new()));
    let failures = Arc::new(Mutex::new(Vec::new()));
    let fault_log = Arc::new(Mutex::new(Vec::new()));

    // Plan 30 §M11: the directories, then the delegations (node 1 takes
    // the lease with the first mkdir and delegates as the root).
    let markers: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let marker_stats: Arc<Mutex<(u64, u64)>> = Arc::new(Mutex::new((0, 0)));
    if !cfg.dirs.is_empty() {
        setup_dirs(&cluster, &cfg, &failures).await;
    }

    // The workload.
    let mut clients = Vec::new();
    for node in 1..=cfg.nodes {
        for k in 0..cfg.clients_per_node {
            // Plan 30 §M11: node 1 (the root) writes in the root
            // directory, node `n ≥ 2` in `dirs[n − 2]` (its delegation,
            // when one is configured); a second client writes in the
            // next directory (a forward to its delegate).
            let home = home_dir(&cfg.dirs, node, k);
            let ops = gen_ops_in(
                &mut rng,
                cfg.ops_per_client,
                cfg.names,
                &home,
                &cfg.dirs,
                cfg.cross_ratio,
            );
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
                cfg.strict,
            )));
        }
    }
    // Plan 30 §M11: marker writers and watchers.
    let mut marker_tasks = Vec::new();
    if cfg.marker_pairs > 0 && cfg.dirs.len() >= 2 {
        for node in 1..=cfg.nodes {
            let home = home_dir(&cfg.dirs, node, 0);
            let other = home_dir(&cfg.dirs, node, 1);
            marker_tasks.push(tokio::spawn(marker_writer(
                cluster.clone(),
                history.clone(),
                node,
                (1 << 16) + node,
                home,
                other,
                cfg.marker_pairs,
                markers.clone(),
                abandoned.clone(),
            )));
        }
        for node in 1..=cfg.nodes {
            marker_tasks.push(tokio::spawn(marker_watcher(
                cluster.clone(),
                node,
                markers.clone(),
                marker_stats.clone(),
                failures.clone(),
            )));
        }
    }
    // The faults.
    let horizon = cfg.ops_per_client * 400 + 2_000;
    let mut faults = cfg.faults.clone();
    faults.extend(gen_faults(&mut rng, &cfg, horizon));
    let mut fault_tasks = Vec::new();
    // Plan 30 §M10: the single-authority sampler.
    let sampler = tokio::spawn(super::epochs::sample_authority(cluster.clone()));
    for f in faults {
        fault_tasks.push(tokio::spawn(run_fault(
            cluster.clone(),
            f,
            fault_log.clone(),
        )));
    }
    super::node::note_waiting("run", "clients".into());
    // A client whose op is never answered would run the clock forever
    // (paused time races: 85 000 simulated seconds in 30 s of wall for
    // long-delegated seed 70051 before the fix): a bound, and a failure
    // that names it.
    let client_cap = Duration::from_millis(cfg.settle_ms * 6);
    for c in clients {
        match tokio::time::timeout(client_cap, c).await {
            Ok(r) => r.expect("client task"),
            Err(_) => {
                failures.lock().unwrap().push(format!(
                    "a client op was never answered within {client_cap:?} of simulated time; \
                     waiting: {:?}",
                    super::node::WAITING.lock().unwrap()
                ));
                break;
            }
        }
    }
    for m in marker_tasks {
        // Watchers loop until the writers are done and the markers
        // settled; the writer tasks end on their own.
        let _ = tokio::time::timeout(Duration::from_secs(120), m).await;
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

    sampler.abort();
    // Checks.
    let mut report = Report {
        seed,
        ..Default::default()
    };
    {
        let formed = cluster.epochs.lock().unwrap();
        report.epochs_formed = formed.len();
        report.epochs_missing_node = formed
            .iter()
            .filter(|m| m.len() < cfg.nodes as usize)
            .count();
    }
    report.authority_samples = super::epochs::SAMPLES.with(|s| s.get());
    {
        let (checks, violations) = *marker_stats.lock().unwrap();
        report.marker_checks = checks;
        report.marker_violations = violations;
    }
    if let Some(what) = cluster.split_brains.lock().unwrap().first() {
        return Err(format!(
            "two authorities at once (plan 30 §M10): {what}\n  faults: {:?}",
            fault_log.lock().unwrap()
        ));
    }
    report.faults = fault_log.lock().unwrap().clone();
    report.simulated_ms = clock.elapsed_ms();
    if let Some(f) = failures.lock().unwrap().first() {
        return Err(format!("{f}\n  faults: {:?}", report.faults));
    }
    // Plan 30 §M9: under a durable acknowledgement policy an acknowledged
    // op is never exempt — a rollback of one is exactly what the backup
    // (or `ack=s3`) exists to make impossible, so only ops the client
    // abandoned (its process died with the call in flight) are tentative.
    let mut tentative: HashSet<Rid> = abandoned.lock().unwrap().clone();
    let acked: HashSet<Rid> = history.returned().into_iter().map(|(rid, _)| rid).collect();
    let mut rolled_back_acked = 0usize;
    for id in cluster.ids() {
        let n = cluster.get(id);
        for rid in n.shared.tentative.lock().unwrap().iter() {
            if acked.contains(rid) {
                rolled_back_acked += 1;
            }
            if !cfg.strict_durability {
                tentative.insert(*rid);
            }
        }
        report.stats.insert(id, n.view().stats);
    }
    report.acked_rolled_back = rolled_back_acked;
    report.failover_ms = cluster
        .failovers
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(_, d)| *d)
        .collect();
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
    // Plan 30 §M11: under delegation the log orders keys of different
    // owners independently, so linearizability is per directory: every
    // check runs on the history projected to one directory (a rename
    // across two appears in both as its halves).
    let dirs = super::history::dirs_of(&events);
    let per_dir = !cfg.dirs.is_empty();
    let projections: Vec<Vec<super::history::HistEvt>> = if per_dir {
        dirs.iter()
            .map(|d| super::history::project_dir(&events, d))
            .collect()
    } else {
        vec![events.clone()]
    };
    report.dirs_checked = projections.len();
    let mut witness = super::history::Witness::default();
    for (i, proj) in projections.iter().enumerate() {
        let w = check_linearizable_witnessed(proj, &tentative, &oracle.completed_at)
            .map_err(|e| lin_context(format!("[directory {:?}] {e}", dirs.get(i))))?;
        witness.observed_tentative += w.observed_tentative;
    }
    report.observed_tentative = witness.observed_tentative;
    if cfg.strict_durability && witness.observed_tentative > 0 {
        return Err(lin_context(format!(
            "{} refusal(s) observed an acknowledged effect that was later rolled back              (impossible under a durable acknowledgement policy)",
            witness.observed_tentative
        )));
    }
    if events.len() <= STATERIGHT_EVENT_BOUND && tentative.len() <= STATERIGHT_TENTATIVE_BOUND {
        for proj in &projections {
            check_linearizable(proj, &tentative).map_err(lin_context)?;
        }
        report.stateright_checked = true;
    }
    // Plan 30 §M11: the causal-cut assertion at the root (a delegate
    // waited for its deps before executing, so the root never sees a
    // batch whose deps it lacks), and no marker without its data.
    for (id, stats) in &report.stats {
        if stats.deleg_deps_unsatisfied_at_append > 0 {
            return Err(format!(
                "node {id} appended {} delegate batches whose deps it lacked (causal cut)\n  stats: {:#?}",
                stats.deleg_deps_unsatisfied_at_append, report.stats
            ));
        }
    }
    if report.marker_violations > 0 {
        return Err(format!(
            "marker order violated {} times (of {} checks)\n  faults: {:?}",
            report.marker_violations, report.marker_checks, report.faults
        ));
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
        // What keeps each node from quiescence (phase 2b diagnostics).
        let why: Vec<String> = cluster
            .ids()
            .into_iter()
            .map(|id| cluster.get(id))
            .filter(|n| n.alive())
            .map(|n| {
                let rows = constellation_authority::Replica::take_journal(&*n.meta, 12)
                    .map(|b| {
                        b.into_iter()
                            .map(|(s, r)| format!("{s}:{r:?}"))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                format!(
                    "node {}: journal {:?} held {:?} speculation {:?} live {:?} replays {}",
                    n.id,
                    rows,
                    n.meta.held_summary(),
                    n.meta.speculation_counts().ok(),
                    n.meta.speculation_live_debug(),
                    n.meta.pending_replays().map(|q| q.len()).unwrap_or(0)
                )
            })
            .collect();
        return Err(format!(
            "the cluster did not reach quiescence within {}ms of simulated time\n  why: {:#?}\n  faults: {:?}\n  views: {:?}",
            cfg.settle_ms,
            why,
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
            return Err(lin_context(format!(
                "session guarantee violated ({checker}): {what}"
            )));
        }
    }
    // Plan 30 §M8: close-to-open across nodes (see `cto.rs`); enforced
    // under `strict`, reported otherwise.
    report.cto = super::cto::check_cto(
        &events,
        &history.ticks(),
        &history.reads(),
        &tentative,
        &oracle.completed_at,
    );
    // With P2P off there is no ReadIndex: a strict read tails S3, which
    // covers closes whose records are in the log (inbox writes) but not a
    // holder's own acked-unshipped writes (see `core::readindex`, "P2P
    // off"). Reported there, enforced with P2P.
    let p2p = (cfg.core)(1, 1).p2p;
    if cfg.strict && p2p {
        if let Some(what) = report.cto.violations.first() {
            return Err(format!(
                "close-to-open violated (cto=strict): {what}\n  faults: {:?}\n  stats: {:#?}",
                report.faults, report.stats
            ));
        }
    }
    report.in_doubt_answers = in_doubt.lock().unwrap().len();
    check::check_in_doubt_answers(&in_doubt.lock().unwrap(), &oracle).map_err(|e| {
        format!(
            "{e}\n  faults: {:?}\n  log: {:#?}\n  stats: {:#?}",
            report.faults, oracle.describe, report.stats
        )
    })?;
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

/// Plan 30 §M11: create `cfg.dirs` under the root through node 1 (it
/// takes the lease), then delegate `cfg.delegations` from it; wait until
/// every delegate has installed its generation.
async fn setup_dirs(cluster: &Arc<Cluster>, cfg: &SimConfig, failures: &Arc<Mutex<Vec<String>>>) {
    let handle = cluster.get(1);
    for d in &cfg.dirs {
        let rid = handle.next_rid();
        let op = MutateOp::Mkdir {
            parent: ROOT_INO,
            name: d.clone(),
            ino: handle.meta.allocate_ino(ROOT_INO).expect("ino"),
            mode: 0o755,
            uid: 0,
            gid: 0,
        };
        let mut ok = false;
        for _ in 0..20 {
            match handle.submit(rid, op.clone()).await {
                Ok(ClientReply::Outcome(MutateOutcome::Accepted { .. })) => {
                    ok = true;
                    break;
                }
                Ok(ClientReply::Outcome(MutateOutcome::Errno(e))) if e == libc::EEXIST => {
                    ok = true;
                    break;
                }
                _ => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }
        if !ok {
            failures
                .lock()
                .unwrap()
                .push(format!("setup: could not create directory {d}"));
            return;
        }
    }
    // Every node applies the directories before anything is delegated
    // (a delegate resolves ownership on its own replica).
    for _ in 0..200 {
        let all = cluster.ids().into_iter().all(|id| {
            let n = cluster.get(id);
            cfg.dirs
                .iter()
                .all(|d| n.meta.child_ino(ROOT_INO, d).ok().flatten().is_some())
        });
        if all {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for (dir, node) in &cfg.delegations {
        let Some(ino) = handle.meta.child_ino(ROOT_INO, dir).ok().flatten() else {
            failures
                .lock()
                .unwrap()
                .push(format!("setup: directory {dir} missing on node 1"));
            return;
        };
        let mut done = false;
        for _ in 0..30 {
            match handle
                .control(constellation_authority::Control::Delegate {
                    dir: ino,
                    node: *node,
                })
                .await
            {
                Ok(Ok(_)) => {
                    done = true;
                    break;
                }
                Ok(Err(e)) if e.contains("already holds") => {
                    done = true;
                    break;
                }
                _ => tokio::time::sleep(Duration::from_millis(250)).await,
            }
        }
        if !done {
            failures
                .lock()
                .unwrap()
                .push(format!("setup: could not delegate {dir} to node {node}"));
            return;
        }
    }
    // Plan 30 §M11 phase 2b: the designations, through the root's sync.
    if !cfg.designations.is_empty() {
        let mut entries = Vec::new();
        for (dir, node) in &cfg.designations {
            match handle.meta.child_ino(ROOT_INO, dir).ok().flatten() {
                Some(ino) => entries.push((ino, *node)),
                None => {
                    failures
                        .lock()
                        .unwrap()
                        .push(format!("setup: directory {dir} missing on node 1"));
                    return;
                }
            }
        }
        let mut done = false;
        for _ in 0..30 {
            if let Ok(Ok(_)) = handle
                .control(constellation_authority::Control::SyncDesignations {
                    entries: entries.clone(),
                })
                .await
            {
                done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        if !done {
            failures
                .lock()
                .unwrap()
                .push("setup: could not sync the designations".into());
            return;
        }
    }
    // Installed and renewed on every delegate.
    for _ in 0..400 {
        let all = cfg
            .delegations
            .iter()
            .chain(cfg.designations.iter())
            .all(|(_, node)| {
                let v = cluster.get(*node).view();
                v.delegation
                    .mine
                    .iter()
                    .any(|(_, _, until, stopped, ..)| !*stopped && *until > 0)
            });
        if all {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    failures
        .lock()
        .unwrap()
        .push("setup: delegations were not installed on the delegates".into());
}

/// Plan 30 §M11: write `pairs` data names in `home`, each followed by a
/// marker in `other` registered for the watchers between the two writes.
#[allow(clippy::too_many_arguments)]
async fn marker_writer(
    cluster: Arc<Cluster>,
    history: Arc<History>,
    node: NodeId,
    thread: u64,
    home: String,
    other: String,
    pairs: u64,
    markers: Arc<Mutex<Vec<(String, String)>>>,
    abandoned: Arc<Mutex<HashSet<Rid>>>,
) {
    for k in 0..pairs {
        let data = format!("{home}/m{node}-{k}-data");
        let marker = format!("{other}/m{node}-{k}-marker");
        for name in [data.clone(), marker.clone()] {
            let handle = cluster.get(node);
            if !handle.alive() {
                return;
            }
            let rid = handle.next_rid();
            let op = NsOp::Create(name.clone());
            let mop = mutate_op(&handle.meta, &op);
            history.invoke(thread, rid, op);
            let mut acked = false;
            for _ in 0..MAX_RESUBMITS {
                let handle = cluster.get(node);
                if !handle.alive() {
                    break;
                }
                match handle.submit(rid, mop.clone()).await {
                    Ok(ClientReply::Outcome(outcome)) => {
                        if let Ok(ret) = ns_ret(&outcome) {
                            history.ret(thread, rid, ret);
                            acked = ret == NsRet::Ok;
                        }
                        break;
                    }
                    _ => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
            if !acked {
                abandoned.lock().unwrap().insert(rid);
                return;
            }
            if name == data {
                markers.lock().unwrap().push((marker.clone(), data.clone()));
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Plan 30 §M11: on `node`, every registered marker that is visible must
/// have its data visible in the same snapshot.
async fn marker_watcher(
    cluster: Arc<Cluster>,
    node: NodeId,
    markers: Arc<Mutex<Vec<(String, String)>>>,
    stats: Arc<Mutex<(u64, u64)>>,
    failures: Arc<Mutex<Vec<String>>>,
) {
    for _ in 0..600 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let handle = cluster.get(node);
        if !handle.alive() {
            continue;
        }
        let pairs = markers.lock().unwrap().clone();
        for (marker, data) in pairs {
            let (mp, ml) = split_name(&handle.meta, &marker);
            let (dp, dl) = split_name(&handle.meta, &data);
            let marker_seen = handle.meta.child_ino(mp, &ml).ok().flatten().is_some();
            let data_seen = handle.meta.child_ino(dp, &dl).ok().flatten().is_some();
            let mut s = stats.lock().unwrap();
            s.0 += 1;
            if marker_seen && !data_seen {
                s.1 += 1;
                failures.lock().unwrap().push(format!(
                    "node {node} saw marker {marker} without its data {data}"
                ));
            }
        }
    }
}

/// Plan 30 §M11: a client's home directory (see the workload loop).
fn home_dir(dirs: &[String], node: NodeId, client: u64) -> String {
    if dirs.is_empty() {
        return String::new();
    }
    let base = if node == 1 {
        None
    } else {
        Some(((node - 2) as usize) % dirs.len())
    };
    match (base, client) {
        (None, 0) => String::new(),
        (None, k) => dirs[((k - 1) as usize) % dirs.len()].clone(),
        (Some(b), 0) => dirs[b].clone(),
        (Some(b), k) => dirs[(b + k as usize) % dirs.len()].clone(),
    }
}
