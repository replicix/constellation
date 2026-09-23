//! The S3 inbox (plan 30 §M13): forwarding a mutation to the holder
//! when there is no P2P path to it.
//!
//! Both halves live here. The **requester** half is the last step of
//! `node_runtime::dispatch_forward`'s fallback chain: once the P2P
//! attempts have ended in doubt (or P2P is off), and the lease register
//! names a live, unexpired holder that is not this node, the op is
//! written into a batch object under that holder's epoch
//! (`store_s3::inbox::InboxSubmitter`, group-committed by one submitter
//! task per node) and the task waits for the outcome to arrive through
//! the log: `Completed { rid }` (the op's records are applied by then,
//! so the caller reads its own write) or `Refused { rid, errno }`. The
//! reply to the FUSE thread is the same `MutateOutcome` a P2P forward
//! would produce — `Accepted` with no records (plan 30 §M3a's "completed
//! rid, learn the effect by tailing"), `Errno`, or `Conflict { manifest:
//! None }` for a stale manifest base (`ESTALE` on the wire; the
//! requester rebases from its own replica, which has tailed the refusing
//! segment) — so `fusefs::mutate_op_rebasable` needs no inbox branch of
//! its own, and neither does the stranded-op replay drain
//! (`recovery::drain_pending_replays` submits through the same path).
//!
//! The **holder** half runs from the sync round (`holder_round`): one
//! GET-next per due requester with per-requester idle backoff
//! (`store_s3::inbox::InboxPoller`), each batch executed in order through
//! the same dedup a P2P forward gets plus the position watermark, the
//! outcome journaled with the op (`Meta::pending_inbox_ack`) or as a
//! refusal (`Meta::journal_inbox_refusal`), and the batch deleted once
//! its rows have shipped, keeping each requester's newest one
//! (`gc_keep_newest`). A takeover drains every older epoch's batches
//! inside its gate (`drain_at_takeover`, from `shipper::complete_gate`),
//! before the new holder's view opens.
//!
//! Who gets polled: the write-eligible roster plus the peer directory,
//! minus this node, minus the peers this node is P2P-connected to (they
//! forward directly). A single node polls nothing; a healthy P2P cluster
//! polls nothing; a P2P-off cluster of K nodes costs its holder K-1 GETs
//! per poll round, at the two-tier schedule `PollBackoff` documents (the
//! `CONSTELLATION_INBOX_IDLE_MAX_MS` warm ceiling for a requester that
//! wrote within the last minute, the sync loop's cold ceiling after
//! that). The design record is PROGRESS.md's "Plan 30 M13" sections.
//!
//! Knobs (all `CONSTELLATION_*`, documented in
//! `docs/reference/configuration.md`): `INBOX` (`off` disables the path;
//! non-holders then take the lease as before M13), `INBOX_IDLE_MAX_MS`
//! (warm poll ceiling, default 2000), `INBOX_COLD_MAX_MS` (cold ceiling,
//! default `SYNC_IDLE_MAX_MS`'s 10 000), `INBOX_POLL_WIDTH` (GET-next
//! width, default 4), `INBOX_RECHECK_MS` (how often a waiting requester
//! re-reads the lease, default 1000).

use crate::lease::LeaseKeeper;
use constellation_meta::{execute_mutate, InboxAck, Meta, MutateOp, MutateOutcome, Rid};
use constellation_store_s3::inbox::{
    gc_keep_newest, InboxBatch, InboxKey, InboxOp, InboxPoller, InboxRid, InboxStore,
    InboxSubmitter, MAX_OPS_PER_BATCH,
};
use constellation_store_s3::lease::{now_unix_ms, Lease, LeaseMode, LeaseStore};
use constellation_store_s3::log::PARTITION;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const DEFAULT_WARM_MAX_MS: u64 = 2_000;
pub const DEFAULT_COLD_MAX_MS: u64 = 10_000;
pub const DEFAULT_RECHECK_MS: u64 = 1_000;
/// Plan 30 M13 round 2: the holder's poll interval right after a hit
/// (`CONSTELLATION_INBOX_HOT_MS`) and for [`HOT_GRACE_ROUNDS`] misses
/// after it, and the requester's log-tail interval while it has an op
/// waiting on its outcome (`CONSTELLATION_INBOX_TAIL_MS`). Together they
/// set the floor of an inbox round trip: PUT + hot poll + ship + hot
/// tail, a few tens of milliseconds on a local S3 — against the several
/// hundred the base sync interval on both sides used to cost.
pub const DEFAULT_HOT_MS: u64 = 20;
pub const DEFAULT_TAIL_MS: u64 = 20;
/// Misses after a hit during which a requester stays hot: ~0.5 s of
/// 20 ms polls, enough to bridge one op's ship + tail + the next op's
/// PUT on a single-threaded writer without falling back to the base
/// interval between every op.
pub const HOT_GRACE_ROUNDS: u32 = 25;
/// How long a P2P peer may be disconnected before the inbox counts the
/// path as gone (`CONSTELLATION_INBOX_P2P_GRACE_MS`).
pub const DEFAULT_P2P_GRACE_MS: u64 = 3_000;
/// Plan 30 M13 round 3b, the hybrid: a requester whose inbox demand is
/// *sustained* asks for the lease (today's `wanted_by` handoff, plan 26)
/// and executes locally once it holds; a *sporadic* writer stays on the
/// inbox and never disturbs the holder. "Sustained" is a sliding window
/// of `CONSTELLATION_INBOX_ESCALATE_WINDOW_MS` over this node's
/// inbox-answered ops: at least `CONSTELLATION_INBOX_ESCALATE_OPS` of
/// them, or at least `CONSTELLATION_INBOX_ESCALATE_WAIT_MS` spent
/// waiting on their round trips, in the window. Defaults: 20 ops or 3 s
/// of waiting in 10 s. At the ~135 ms round-trip floor measured on floci
/// that is 2 ops/s sustained, ~2.7 s of every 10 waiting; on real S3
/// (a round trip of several hundred ms) the wait term fires first, at
/// six to ten ops. Either way a lease handoff — a few S3 round trips
/// plus the holder's dwell/grace, ~5–10 s — pays for itself within the
/// next window. One write every few seconds is 2–3 ops and well under a
/// second of waiting per window: never an escalation.
pub const DEFAULT_ESCALATE_WINDOW_MS: u64 = 10_000;
pub const DEFAULT_ESCALATE_OPS: u64 = 20;
pub const DEFAULT_ESCALATE_WAIT_MS: u64 = 3_000;
/// Longest gap between an escalated requester's lease requests
/// (`SyncRequest::Acquire`, which registers `wanted_by`); the first
/// requests back off from 100 ms like the FUSE lease path's own retries.
pub const DEFAULT_ESCALATE_RETRY_MS: u64 = 2_000;
/// Hysteresis: de-escalate only once the window has fallen below half
/// of both thresholds, so a requester hovering at the edge does not flap
/// between asking and not asking.
const DEESCALATE_FRACTION: f64 = 0.5;
/// Round 4: the wait term needs this many ops in the window, and its
/// single largest sample is left out. One slow op is not demand: a
/// requester's first inbox op pays the holder's first-contact cost
/// (registry poll plus cold poll tier, ~4.5 s on the harness rig),
/// which alone exceeded `DEFAULT_ESCALATE_WAIT_MS` and moved the lease
/// for one write. With the largest sample excluded, four more ops must
/// together wait the threshold — on a real S3 (several hundred ms a
/// round trip) that is still six to ten sustained ops, while the
/// sporadic pattern (one write every few seconds, sub-second round
/// trips) never gets near it.
const ESCALATE_WAIT_MIN_OPS: u64 = 5;
/// How long a waiter keeps polling for its outcome after the lease
/// register names this node (an escalation just landed): the takeover
/// gate's drain of lower epochs writes the outcome locally.
const SELF_HOLD_WAIT: Duration = Duration::from_secs(2);
const DEFAULT_POLL_WIDTH: usize = 4;
/// How often a waiting requester looks for its outcome in `completed`
/// (one fjall point read each).
const OUTCOME_POLL: Duration = Duration::from_millis(10);
/// PUT attempts per batch before the waiters are told the inbox is
/// unavailable (they then take the lease path).
const SUBMIT_ATTEMPTS: u32 = 6;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// `CONSTELLATION_INBOX_ESCALATE=off|0|false` keeps a requester on the
/// inbox however sustained its demand (round 3b's hybrid off).
pub fn escalation_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("CONSTELLATION_INBOX_ESCALATE") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false"
        ),
        Err(_) => true,
    })
}

/// `CONSTELLATION_INBOX=off|0|false` disables the inbox path entirely.
pub fn inbox_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("CONSTELLATION_INBOX") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false"
        ),
        Err(_) => true,
    })
}

/// The in-doubt deadline for an inbox-submitted op: `min(2 × TTL,
/// retention / 2)`. Past it the op is in doubt and the caller takes the
/// lease path (whose own deadline bounds the rest). The retention half
/// keeps a re-submission inside the window its dedup rows live in; a TTL
/// large enough to force the clamp is warned about at mount.
pub fn wait_deadline(ttl_ms: u64, retention_s: u64) -> Duration {
    let two_ttl = 2 * ttl_ms;
    let half_retention = retention_s.saturating_mul(1_000) / 2;
    Duration::from_millis(two_ttl.min(half_retention).max(1_000))
}

/// Counters for `status.inbox` (`constellation_api::InboxStatus`).
#[derive(Default)]
pub struct InboxStats {
    pub submitted_batches: AtomicU64,
    pub submitted_ops: AtomicU64,
    pub resubmitted_ops: AtomicU64,
    pub unavailable: AtomicU64,
    pub executed_ops: AtomicU64,
    pub refused_ops: AtomicU64,
    pub deduped_ops: AtomicU64,
    pub drained_batches: AtomicU64,
    pub drained_ops: AtomicU64,
    pub polls: AtomicU64,
    pub poll_hits: AtomicU64,
    pub gc_deleted: AtomicU64,
    pub tracked_requesters: AtomicU64,
    // Round-2 instrumentation (permanent; all sums, divided in `status`):
    /// Requester: from `submit` to the batch being durable.
    pub queue_wait_us_total: AtomicU64,
    /// Requester: from the batch being durable to the outcome applied.
    pub outcome_wait_us_total: AtomicU64,
    /// Requester: `queue_wait + outcome_wait`, i.e. the whole round trip.
    pub round_trip_us_total: AtomicU64,
    pub round_trip_samples: AtomicU64,
    /// Holder: from a batch's `submitted_unix_ms` to its poll hit.
    pub pickup_ms_total: AtomicU64,
    pub pickup_samples: AtomicU64,
    /// Holder: time spent executing batches.
    pub execute_us_total: AtomicU64,
    pub largest_batch_ops: AtomicU64,
    // Round 3b, the hybrid:
    /// Times this node's inbox demand crossed the sustained threshold and
    /// it started asking for the lease.
    pub escalations: AtomicU64,
    /// `SyncRequest::Acquire`s the escalator sent.
    pub lease_requests: AtomicU64,
}

/// Round 3b: this node's recent inbox demand and whether it is asking
/// for the lease because of it.
#[derive(Default)]
struct Escalation {
    /// `(answered_at, round_trip)` of inbox-answered ops, oldest first,
    /// pruned to the window.
    window: std::collections::VecDeque<(Instant, Duration)>,
    /// When the current escalation began; `None` while not escalated.
    since: Option<Instant>,
    /// Lease requests sent during the current escalation.
    attempts: u32,
    last_request: Option<Instant>,
}

impl Escalation {
    fn prune(&mut self, now: Instant, window: Duration) {
        while self
            .window
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > window)
        {
            self.window.pop_front();
        }
    }

    /// `(ops, wait)` over the window: the op count, and the cumulative
    /// round-trip wait with the single largest sample left out (see
    /// [`ESCALATE_WAIT_MIN_OPS`]).
    fn demand(&self) -> (u64, Duration) {
        let total: Duration = self.window.iter().map(|(_, w)| *w).sum();
        let largest = self
            .window
            .iter()
            .map(|(_, w)| *w)
            .max()
            .unwrap_or_default();
        (self.window.len() as u64, total.saturating_sub(largest))
    }
}

struct Queued {
    epoch: u64,
    rid: Rid,
    op: Vec<u8>,
    reply: tokio::sync::oneshot::Sender<Result<InboxKey, SubmitFailure>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubmitFailure {
    /// The epoch this op was queued under is older than the one the
    /// submitter has moved on to: re-read the lease and try again.
    StaleEpoch,
    /// S3 would not take the batch within `SUBMIT_ATTEMPTS`.
    Transport,
}

/// A batch this tenure executed, paired with the journal seq its last
/// row landed at.
type ExecutedBatch = (InboxKey, u64);

/// The holder's per-tenure poll state, alive while this node holds.
struct HolderState {
    poller: InboxPoller,
    last_poll: HashMap<u64, Instant>,
    /// Batches this tenure executed; deletable once shipped
    /// (`journal_acked_seq`), all but each requester's newest.
    executed: Vec<ExecutedBatch>,
}

/// Everything the inbox needs on one node: the S3 handles, the
/// requester queue and submitter, the holder poll state, counters.
pub struct InboxRuntime {
    node_id: u64,
    incarnation: u32,
    enabled: bool,
    store: InboxStore,
    lease_store: LeaseStore,
    peers: constellation_net::Peers,
    roster: Mutex<Vec<u64>>,
    base_ms: u64,
    warm_max_ms: u64,
    cold_max_ms: u64,
    poll_width: usize,
    hot_ms: u64,
    tail_ms: u64,
    p2p_grace: Duration,
    recheck: Duration,
    deadline: Duration,
    queue: Mutex<Vec<Queued>>,
    queue_notify: tokio::sync::Notify,
    submitter: tokio::sync::Mutex<Option<InboxSubmitter>>,
    lease_cache: Mutex<Option<(Instant, Lease)>>,
    /// Round 3a: per holder, when its P2P path first failed since it
    /// last answered (a dial/request error or timeout, or the
    /// connection reported lost). Cleared by any reply or a live
    /// connection. See [`InboxRuntime::p2p_reaches`].
    p2p_down_since: Mutex<HashMap<u64, Instant>>,
    escalation: Mutex<Escalation>,
    escalate_window: Duration,
    escalate_ops: u64,
    escalate_wait: Duration,
    escalate_retry: Duration,
    pending_ops: AtomicU64,
    holder: tokio::sync::Mutex<Option<HolderState>>,
    holder_min_delay_ms: AtomicU64,
    pub stats: InboxStats,
}

impl InboxRuntime {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: u64,
        incarnation: u32,
        backend: Arc<dyn object_store::ObjectStore>,
        e2e: Option<constellation_store_s3::SharedE2eKeys>,
        lease_mode: LeaseMode,
        peers: constellation_net::Peers,
        base_ms: u64,
        ttl_ms: u64,
        retention_s: u64,
    ) -> Arc<Self> {
        let store = match e2e {
            Some(keys) => InboxStore::new_e2e(backend.clone(), keys),
            None => InboxStore::new(backend.clone()),
        };
        let deadline = wait_deadline(ttl_ms, retention_s);
        if 2 * ttl_ms > retention_s.saturating_mul(1_000) / 2 {
            tracing::warn!(
                ttl_ms,
                retention_s,
                deadline_ms = deadline.as_millis() as u64,
                "CONSTELLATION_LEASE_TTL_MS is large enough that the inbox in-doubt deadline \
                 is clamped to half the completion retention window"
            );
        }
        Arc::new(Self {
            node_id,
            incarnation,
            enabled: inbox_enabled(),
            store,
            lease_store: LeaseStore::new(backend, PARTITION, lease_mode),
            peers,
            roster: Mutex::new(Vec::new()),
            base_ms: base_ms.max(1),
            warm_max_ms: env_u64("CONSTELLATION_INBOX_IDLE_MAX_MS", DEFAULT_WARM_MAX_MS),
            cold_max_ms: env_u64(
                "CONSTELLATION_INBOX_COLD_MAX_MS",
                env_u64("CONSTELLATION_SYNC_IDLE_MAX_MS", DEFAULT_COLD_MAX_MS),
            ),
            poll_width: env_u64("CONSTELLATION_INBOX_POLL_WIDTH", DEFAULT_POLL_WIDTH as u64).max(1)
                as usize,
            hot_ms: env_u64("CONSTELLATION_INBOX_HOT_MS", DEFAULT_HOT_MS).max(1),
            tail_ms: env_u64("CONSTELLATION_INBOX_TAIL_MS", DEFAULT_TAIL_MS).max(1),
            p2p_grace: Duration::from_millis(env_u64(
                "CONSTELLATION_INBOX_P2P_GRACE_MS",
                DEFAULT_P2P_GRACE_MS,
            )),
            recheck: Duration::from_millis(
                env_u64("CONSTELLATION_INBOX_RECHECK_MS", DEFAULT_RECHECK_MS).max(100),
            ),
            deadline,
            queue: Mutex::new(Vec::new()),
            queue_notify: tokio::sync::Notify::new(),
            submitter: tokio::sync::Mutex::new(None),
            lease_cache: Mutex::new(None),
            p2p_down_since: Mutex::new(HashMap::new()),
            escalation: Mutex::new(Escalation::default()),
            escalate_window: Duration::from_millis(
                env_u64(
                    "CONSTELLATION_INBOX_ESCALATE_WINDOW_MS",
                    DEFAULT_ESCALATE_WINDOW_MS,
                )
                .max(1_000),
            ),
            escalate_ops: env_u64("CONSTELLATION_INBOX_ESCALATE_OPS", DEFAULT_ESCALATE_OPS).max(2),
            escalate_wait: Duration::from_millis(env_u64(
                "CONSTELLATION_INBOX_ESCALATE_WAIT_MS",
                DEFAULT_ESCALATE_WAIT_MS,
            )),
            escalate_retry: Duration::from_millis(
                env_u64(
                    "CONSTELLATION_INBOX_ESCALATE_RETRY_MS",
                    DEFAULT_ESCALATE_RETRY_MS,
                )
                .max(100),
            ),
            pending_ops: AtomicU64::new(0),
            holder: tokio::sync::Mutex::new(None),
            holder_min_delay_ms: AtomicU64::new(u64::MAX),
            stats: InboxStats::default(),
        })
    }

    /// An inbox that never submits or polls: tests and tools that build
    /// a `SyncDispatchCtx` without a bucket.
    #[cfg(test)]
    pub fn disabled(node_id: u64) -> Arc<Self> {
        let backend: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let mut rt = Self::new(
            node_id,
            0,
            backend,
            None,
            LeaseMode::Cas,
            constellation_net::Peers::disabled(),
            500,
            60_000,
            900,
        );
        Arc::get_mut(&mut rt).expect("fresh Arc").enabled = false;
        rt
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// The write-eligible roster, from the registry poll. Returns
    /// whether it changed.
    pub fn set_roster(&self, roster: Vec<u64>) -> bool {
        let mut current = self.roster.lock().unwrap();
        if *current == roster {
            return false;
        }
        *current = roster;
        true
    }

    /// Ops this node has submitted and is still waiting on. While
    /// non-zero the sync loop tails at its base interval, so the outcome
    /// is seen as soon as it ships.
    pub fn pending_ops(&self) -> u64 {
        self.pending_ops.load(Ordering::Relaxed)
    }

    /// While ops wait on their outcome, the interval the sync loop tails
    /// the log at (`CONSTELLATION_INBOX_TAIL_MS`); `None` otherwise.
    pub fn tail_interval_ms(&self) -> Option<u64> {
        (self.pending_ops() > 0).then_some(self.tail_ms)
    }

    /// Plan 30 M13's path-selection rule (round 3a, tightened in round
    /// 4). With P2P enabled, a holder that is in the peer directory is
    /// reachable unless an *outage* to it — a failed dial, or a
    /// transport error that evicted its connection — has lasted longer
    /// than `CONSTELLATION_INBOX_P2P_GRACE_MS` with nothing heard from
    /// it since. "Not yet talked to" means reachable: the first forward
    /// dials, exactly as before M13 (round 2's rule required a gossip or
    /// RPC signal that a freshly joined peer pair does not have yet, and
    /// sent such writes to the lease path). The inbox is for: P2P
    /// disabled; a holder the directory does not know; or a real
    /// transport outage that outlasts the grace. A slow or timing-out
    /// reply — `Busy` from an application-level timeout on a connection
    /// that is still open — is never an outage: it stays on M2's
    /// same-rid retries and the lease fallback, however long it goes on.
    ///
    /// Outages are learned from the forwards themselves
    /// ([`InboxRuntime::note_p2p_attempt`]), and end the moment
    /// anything is heard from the holder: the directory showing it
    /// connected, or any successful exchange with it (`Peer::last_seen`
    /// — a lease request, a ping, a chunk fetch, a gossip neighbor-up)
    /// later than the outage began. Round 3a cleared the record only
    /// when a forward happened to look while `connected` was set, so a
    /// reply between two forwards followed by an unrelated failed RPC
    /// (a 500 ms ping on a loaded host) left a stale outage in force and
    /// sent a live peer's op to the inbox.
    pub fn p2p_reaches(&self, peers: &constellation_net::Peers, holder: u64) -> bool {
        self.reach(peers.is_enabled(), peer_state(peers, holder), holder)
    }

    fn reach(&self, enabled: bool, known: Option<PeerSeen>, holder: u64) -> bool {
        if !enabled {
            return false;
        }
        let Some(seen) = known else {
            return false;
        };
        let mut down = self.p2p_down_since.lock().unwrap();
        let Some(since) = down.get(&holder).copied() else {
            return true;
        };
        if seen.connected || seen.last_seen.is_some_and(|at| at > since) {
            down.remove(&holder);
            return true;
        }
        since.elapsed() < self.p2p_grace
    }

    /// What a P2P forward to `holder` just came back with. A reply of
    /// any kind (accepted, refused, redirected, or the holder itself
    /// saying `Busy`) ends any outage, and so does a `Busy` from a
    /// timeout while the connection to the holder is still open (`live`:
    /// the holder is slow, not gone). Only a `Busy` with no open
    /// connection — the dial failed, or a transport error evicted it —
    /// starts the grace, if none is running.
    pub async fn note_p2p_attempt(
        &self,
        peers: &constellation_net::Peers,
        holder: u64,
        outcome: &MutateOutcome,
    ) {
        let heard = !matches!(outcome, MutateOutcome::Busy)
            || peer_state(peers, holder).is_some_and(|seen| seen.connected)
            || peers.connection_alive(holder).await;
        self.note_p2p_result(holder, heard);
    }

    fn note_p2p_result(&self, holder: u64, heard: bool) {
        let mut down = self.p2p_down_since.lock().unwrap();
        if heard {
            down.remove(&holder);
        } else {
            down.entry(holder).or_insert_with(Instant::now);
        }
    }

    /// For the path decision's debug line: how long the current outage
    /// to `holder` has lasted, if one is recorded.
    pub fn outage_ms(&self, holder: u64) -> Option<u64> {
        self.p2p_down_since
            .lock()
            .unwrap()
            .get(&holder)
            .map(|since| since.elapsed().as_millis() as u64)
    }

    #[cfg(test)]
    fn wait_deadline_for_tests(&self) -> Duration {
        self.deadline
    }

    // ---------------------------------------------------- escalation

    /// An inbox-answered op took `round_trip`: feed the sustained-demand
    /// window and re-evaluate (see [`DEFAULT_ESCALATE_WINDOW_MS`]).
    pub fn note_inbox_op(&self, round_trip: Duration) {
        let now = Instant::now();
        let mut esc = self.escalation.lock().unwrap();
        esc.window.push_back((now, round_trip));
        self.evaluate(&mut esc, now);
    }

    fn evaluate(&self, esc: &mut Escalation, now: Instant) {
        esc.prune(now, self.escalate_window);
        if !escalation_enabled() {
            esc.since = None;
            return;
        }
        let (ops, wait) = esc.demand();
        let sustained = ops >= self.escalate_ops
            || (ops >= ESCALATE_WAIT_MIN_OPS && wait >= self.escalate_wait);
        let quiet = (ops as f64) < self.escalate_ops as f64 * DEESCALATE_FRACTION
            && wait.as_secs_f64() < self.escalate_wait.as_secs_f64() * DEESCALATE_FRACTION;
        match (esc.since, sustained, quiet) {
            (None, true, _) => {
                esc.since = Some(now);
                esc.attempts = 0;
                esc.last_request = None;
                self.stats.escalations.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    ops,
                    wait_ms = wait.as_millis() as u64,
                    window_ms = self.escalate_window.as_millis() as u64,
                    "inbox: sustained demand; asking for the lease"
                );
            }
            (Some(since), _, true) => {
                tracing::info!(
                    escalated_for_ms = since.elapsed().as_millis() as u64,
                    "inbox: demand fell off; no longer asking for the lease"
                );
                esc.since = None;
            }
            _ => {}
        }
    }

    /// Whether this node's inbox demand is sustained (it is, or should
    /// be, asking for the lease). Re-evaluated on read so an escalation
    /// with no further ops still expires with its window.
    pub fn escalated(&self) -> bool {
        let mut esc = self.escalation.lock().unwrap();
        let now = Instant::now();
        self.evaluate(&mut esc, now);
        esc.since.is_some()
    }

    /// Whether the escalator should send a lease request now: escalated,
    /// and past the backoff since the last one (100 ms doubling to
    /// `CONSTELLATION_INBOX_ESCALATE_RETRY_MS`). Records the attempt.
    fn take_lease_request(&self) -> bool {
        let mut esc = self.escalation.lock().unwrap();
        let now = Instant::now();
        self.evaluate(&mut esc, now);
        if esc.since.is_none() {
            return false;
        }
        let backoff =
            Duration::from_millis(100u64 << esc.attempts.min(10)).min(self.escalate_retry);
        if esc
            .last_request
            .is_some_and(|at| now.duration_since(at) < backoff)
        {
            return false;
        }
        esc.last_request = Some(now);
        esc.attempts = esc.attempts.saturating_add(1);
        self.stats.lease_requests.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Start the escalator: while this node's inbox demand is sustained
    /// and it does not hold the lease, ask for it through the ordinary
    /// `SyncRequest::Acquire` (which registers `wanted_by`; the holder
    /// answers by plan 26's dwell and grace rules, so this creates no new
    /// ping-pong), without stalling any write: ops keep going through
    /// the inbox until the lease arrives, and the takeover gate's drain
    /// of lower epochs then executes whatever is still queued, in order,
    /// before the first local op. Once the requester goes quiet the
    /// existing idle-release logic hands the lease back or on.
    pub fn spawn_escalator(
        self: &Arc<Self>,
        rt: &tokio::runtime::Handle,
        sync_tx: tokio::sync::mpsc::UnboundedSender<crate::fusefs::SyncRequest>,
        lease_views: Arc<Mutex<HashMap<String, Arc<crate::lease::LeaseView>>>>,
    ) {
        if !self.enabled || !escalation_enabled() {
            return;
        }
        let this = self.clone();
        rt.spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(250)).await;
                if !this.escalated() {
                    continue;
                }
                let held = lease_views
                    .lock()
                    .unwrap()
                    .get(PARTITION)
                    .is_some_and(|v| v.status().held);
                if held || !this.take_lease_request() {
                    continue;
                }
                let (reply, rx) = tokio::sync::oneshot::channel();
                if sync_tx
                    .send(crate::fusefs::SyncRequest::Acquire {
                        part: PARTITION.to_string(),
                        reply,
                    })
                    .is_err()
                {
                    return;
                }
                match tokio::time::timeout(Duration::from_secs(30), rx).await {
                    Ok(Ok(Ok(progress))) if progress.acquired => {
                        tracing::info!(
                            "inbox: escalation acquired the lease; writes are local now"
                        );
                    }
                    Ok(Ok(Ok(progress))) => {
                        tracing::debug!(
                            holder = progress.holder,
                            epoch = progress.epoch,
                            "inbox: lease request registered; waiting for the holder"
                        );
                    }
                    Ok(Ok(Err(error))) => tracing::debug!(%error, "inbox: lease request failed"),
                    _ => {}
                }
            }
        });
    }

    /// Wait (bounded) until no inbox-submitted op of this node is still
    /// waiting for its outcome: a P2P forward issued after inbox ops
    /// must land after them (per-requester FIFO across a path switch).
    pub async fn wait_quiescent(&self, bound: Duration) {
        let started = Instant::now();
        while self.pending_ops() > 0 && started.elapsed() < bound {
            tokio::time::sleep(OUTCOME_POLL).await;
        }
    }

    /// The soonest any tracked requester wants polling again, or `None`
    /// when this node polls nobody (not holding, or nobody to poll).
    pub fn holder_min_delay_ms(&self) -> Option<u64> {
        match self.holder_min_delay_ms.load(Ordering::Relaxed) {
            u64::MAX => None,
            ms => Some(ms),
        }
    }

    pub fn status(&self) -> constellation_api::InboxStatus {
        let s = &self.stats;
        constellation_api::InboxStatus {
            enabled: self.enabled,
            submitted_batches: s.submitted_batches.load(Ordering::Relaxed),
            submitted_ops: s.submitted_ops.load(Ordering::Relaxed),
            resubmitted_ops: s.resubmitted_ops.load(Ordering::Relaxed),
            unavailable: s.unavailable.load(Ordering::Relaxed),
            pending_ops: self.pending_ops(),
            next_n: self
                .submitter
                .try_lock()
                .ok()
                .and_then(|g| g.as_ref().map(|s| s.next_n()))
                .unwrap_or(0),
            executed_ops: s.executed_ops.load(Ordering::Relaxed),
            refused_ops: s.refused_ops.load(Ordering::Relaxed),
            deduped_ops: s.deduped_ops.load(Ordering::Relaxed),
            drained_batches: s.drained_batches.load(Ordering::Relaxed),
            drained_ops: s.drained_ops.load(Ordering::Relaxed),
            polls: s.polls.load(Ordering::Relaxed),
            poll_hits: s.poll_hits.load(Ordering::Relaxed),
            gc_deleted: s.gc_deleted.load(Ordering::Relaxed),
            tracked_requesters: s.tracked_requesters.load(Ordering::Relaxed),
            avg_queue_wait_ms: avg_ms(
                s.queue_wait_us_total.load(Ordering::Relaxed),
                s.round_trip_samples.load(Ordering::Relaxed),
            ),
            avg_outcome_wait_ms: avg_ms(
                s.outcome_wait_us_total.load(Ordering::Relaxed),
                s.round_trip_samples.load(Ordering::Relaxed),
            ),
            avg_round_trip_ms: avg_ms(
                s.round_trip_us_total.load(Ordering::Relaxed),
                s.round_trip_samples.load(Ordering::Relaxed),
            ),
            avg_pickup_ms: {
                let n = s.pickup_samples.load(Ordering::Relaxed);
                if n == 0 {
                    0.0
                } else {
                    s.pickup_ms_total.load(Ordering::Relaxed) as f64 / n as f64
                }
            },
            avg_execute_ms: avg_ms(
                s.execute_us_total.load(Ordering::Relaxed),
                s.poll_hits.load(Ordering::Relaxed) + s.drained_batches.load(Ordering::Relaxed),
            ),
            avg_batch_ops: {
                let b = s.submitted_batches.load(Ordering::Relaxed);
                if b == 0 {
                    0.0
                } else {
                    s.submitted_ops.load(Ordering::Relaxed) as f64 / b as f64
                }
            },
            largest_batch_ops: s.largest_batch_ops.load(Ordering::Relaxed),
            escalated: self.escalated(),
            escalations: s.escalations.load(Ordering::Relaxed),
            lease_requests: s.lease_requests.load(Ordering::Relaxed),
            inbox_ops: s.round_trip_samples.load(Ordering::Relaxed),
            // Filled in by the daemon from the lease view (`main.rs`).
            local_ops: 0,
        }
    }

    // ------------------------------------------------------ requester

    /// The lease register, cached for up to `max_age` (one GET
    /// otherwise). `None` when it cannot be read or nobody has ever held.
    async fn read_lease(&self, max_age: Duration) -> Option<Lease> {
        if let Some((at, lease)) = self.lease_cache.lock().unwrap().clone() {
            if at.elapsed() < max_age {
                return Some(lease);
            }
        }
        let lease = self
            .lease_store
            .get()
            .await
            .ok()
            .flatten()
            .map(|(l, _)| l)?;
        *self.lease_cache.lock().unwrap() = Some((Instant::now(), lease.clone()));
        Some(lease)
    }

    /// Queue `op` for the next batch under `epoch` and wait for it to be
    /// durable in the bucket. `guards` (the requester-side ordering
    /// guards, if the caller holds any) are dropped the moment the op is
    /// in the queue: its order is fixed from then on.
    async fn submit(
        &self,
        epoch: u64,
        rid: Rid,
        op: Vec<u8>,
        guards: Option<ForwardGuards>,
    ) -> Result<InboxKey, SubmitFailure> {
        let (reply, rx) = tokio::sync::oneshot::channel();
        self.queue.lock().unwrap().push(Queued {
            epoch,
            rid,
            op,
            reply,
        });
        drop(guards);
        self.queue_notify.notify_one();
        rx.await.unwrap_or(Err(SubmitFailure::Transport))
    }

    /// Start the one submitter task per node.
    pub fn spawn_submitter(self: &Arc<Self>, rt: &tokio::runtime::Handle) {
        if !self.enabled {
            return;
        }
        let this = self.clone();
        rt.spawn(async move { this.run_submitter().await });
    }

    /// Group commit: everything queued while the previous PUT was in
    /// flight goes into the next batch (per epoch, capped at
    /// `MAX_OPS_PER_BATCH`). One PUT in flight at a time, so batch
    /// numbers are sequential and GET-next never sees a gap.
    async fn run_submitter(self: Arc<Self>) {
        loop {
            self.queue_notify.notified().await;
            loop {
                let queued: Vec<Queued> = std::mem::take(&mut *self.queue.lock().unwrap());
                if queued.is_empty() {
                    break;
                }
                let mut by_epoch: std::collections::BTreeMap<u64, Vec<Queued>> =
                    std::collections::BTreeMap::new();
                for q in queued {
                    by_epoch.entry(q.epoch).or_default().push(q);
                }
                for (epoch, items) in by_epoch {
                    self.submit_under(epoch, items).await;
                }
            }
        }
    }

    async fn submit_under(&self, epoch: u64, items: Vec<Queued>) {
        let mut guard = self.submitter.lock().await;
        let fail_all = |items: Vec<Queued>, why: SubmitFailure| {
            for q in items {
                let _ = q.reply.send(Err(why));
            }
        };
        match guard.as_mut() {
            None => {
                // First submission of this mount: a previous incarnation
                // may have written under this epoch (LIST-last).
                match InboxSubmitter::resume(
                    self.store.clone(),
                    self.node_id,
                    self.incarnation,
                    epoch,
                )
                .await
                {
                    Ok(s) => *guard = Some(s),
                    Err(error) => {
                        tracing::warn!(%error, epoch, "inbox: resuming batch numbering failed");
                        fail_all(items, SubmitFailure::Transport);
                        return;
                    }
                }
            }
            Some(s) if epoch > s.epoch() => s.advance_epoch(epoch),
            Some(s) if epoch < s.epoch() => {
                fail_all(items, SubmitFailure::StaleEpoch);
                return;
            }
            Some(_) => {}
        }
        let submitter = guard.as_mut().expect("set above");
        let mut items = items.into_iter().peekable();
        while items.peek().is_some() {
            let chunk: Vec<Queued> = items.by_ref().take(MAX_OPS_PER_BATCH).collect();
            let ops: Vec<InboxOp> = chunk
                .iter()
                .map(|q| InboxOp {
                    rid: InboxRid {
                        node: q.rid.node,
                        incarnation: q.rid.incarnation,
                        seq: q.rid.seq,
                    },
                    op: q.op.clone(),
                })
                .collect();
            let mut key = None;
            for attempt in 1..=SUBMIT_ATTEMPTS {
                match submitter.submit(ops.clone(), now_unix_ms()).await {
                    Ok(k) => {
                        key = Some(k);
                        break;
                    }
                    Err(error) => {
                        tracing::warn!(%error, attempt, epoch, "inbox: batch PUT failed");
                        tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt))).await;
                    }
                }
            }
            match key {
                Some(key) => {
                    self.stats.submitted_batches.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .submitted_ops
                        .fetch_add(ops.len() as u64, Ordering::Relaxed);
                    self.stats
                        .largest_batch_ops
                        .fetch_max(ops.len() as u64, Ordering::Relaxed);
                    tracing::debug!(
                        epoch,
                        n = key.n,
                        ops = ops.len(),
                        "inbox: submitted a batch"
                    );
                    for q in chunk {
                        let _ = q.reply.send(Ok(key));
                    }
                }
                None => fail_all(chunk, SubmitFailure::Transport),
            }
        }
    }

    // --------------------------------------------------------- holder

    /// Who this holder polls: the roster and the peer directory, minus
    /// itself and the peers it is P2P-connected to.
    fn requesters_to_poll(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self.roster.lock().unwrap().clone();
        let mut connected = Vec::new();
        for p in self.peers.snapshot() {
            if p.connected {
                connected.push(p.node_id);
            } else {
                ids.push(p.node_id);
            }
        }
        ids.retain(|id| *id != self.node_id && *id != 0 && !connected.contains(id));
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    fn new_holder_state(&self, epoch: u64) -> HolderState {
        HolderState {
            poller: InboxPoller::two_tier(
                self.store.clone(),
                epoch,
                self.base_ms,
                self.warm_max_ms,
                self.cold_max_ms,
            )
            .with_width(self.poll_width)
            .with_hot(self.hot_ms, HOT_GRACE_ROUNDS),
            last_poll: HashMap::new(),
            executed: Vec::new(),
        }
    }
}

/// What the peer directory knows about a holder (see [`peer_state`]).
#[derive(Clone, Copy, Debug)]
struct PeerSeen {
    /// The last RPC to it succeeded, or gossip has it as a neighbor.
    connected: bool,
    /// When it was last heard from: any successful exchange, or a
    /// gossip neighbor-up (`Peer::last_seen`).
    last_seen: Option<Instant>,
}

/// `Some` for a holder the peer directory knows, `None` for one it does
/// not (or with P2P disabled).
fn peer_state(peers: &constellation_net::Peers, holder: u64) -> Option<PeerSeen> {
    peers
        .snapshot()
        .into_iter()
        .find(|p| p.node_id == holder)
        .map(|p| PeerSeen {
            connected: p.connected,
            last_seen: p.last_seen,
        })
}

fn avg_ms(total_us: u64, samples: u64) -> f64 {
    if samples == 0 {
        0.0
    } else {
        total_us as f64 / samples as f64 / 1_000.0
    }
}

/// What the local log already says about `rid`, as the outcome a
/// forward would have produced.
fn local_outcome(meta: &Meta, rid: Rid, epoch: u64) -> Option<MutateOutcome> {
    match meta.completed_outcome(rid).ok().flatten()? {
        constellation_meta::CompletedOutcome::Executed { .. } => Some(MutateOutcome::Accepted {
            epoch,
            records: Vec::new(),
        }),
        constellation_meta::CompletedOutcome::Refused { errno } if errno == libc::ESTALE => {
            Some(MutateOutcome::Conflict { manifest: None })
        }
        constellation_meta::CompletedOutcome::Refused { errno } => {
            Some(MutateOutcome::Errno(errno))
        }
    }
}

struct PendingGuard<'a>(&'a InboxRuntime);

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.0.pending_ops.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The requester-side ordering guards `dispatch_forward` holds for an
/// op: the conflict-key gate and the in-flight permit.
pub type ForwardGuards = (crate::keygate::KeyGuard, tokio::sync::OwnedSemaphorePermit);

/// Forward `op` through the holder's inbox and wait for its outcome
/// (see the module doc). `Busy` means the inbox could not be used —
/// disabled, no live unexpired holder other than this node, S3 would
/// not take the batch, or the deadline passed with the op in doubt — and
/// the caller falls to the lease path, which resolves the rid against
/// `completed` after its takeover gate has drained this batch.
///
/// `guards` are released as soon as the op is *queued* (plan 30 M13
/// round 2). The gate's job is the order overlapping ops land on the
/// holder in; on this path that order is the submitter queue's, which
/// the holder honours batch by batch, and nothing is installed back on
/// the requester out of log order (no shadows). Holding the gate until
/// the outcome — as the P2P path must, to install shadows in order —
/// serialized every create in a directory behind one inbox round trip,
/// which was the whole throughput ceiling. The one path switch that
/// could reorder, an inbox op followed by a P2P forward of an
/// overlapping op, is covered by `wait_quiescent` in `dispatch_forward`.
pub async fn forward_via_inbox(
    inbox: &InboxRuntime,
    meta: &Meta,
    part: &str,
    op: &MutateOp,
    rid: Rid,
    guards: Option<ForwardGuards>,
) -> MutateOutcome {
    let mut guards = guards;
    if !inbox.enabled() || part != PARTITION {
        return MutateOutcome::Busy;
    }
    let op_bytes = match op.to_postcard() {
        Ok(b) => b,
        Err(_) => return MutateOutcome::Errno(libc::EINVAL),
    };
    let started = Instant::now();
    let mut submitted: Option<(u64, InboxKey)> = None;
    loop {
        // An earlier attempt (P2P, or a previous epoch's batch) may have
        // executed it already.
        let known_epoch = submitted.map(|(e, _)| e).unwrap_or(0);
        if let Some(outcome) = local_outcome(meta, rid, known_epoch) {
            return outcome;
        }
        let Some(lease) = inbox.read_lease(Duration::from_millis(500)).await else {
            inbox.stats.unavailable.fetch_add(1, Ordering::Relaxed);
            return MutateOutcome::Busy;
        };
        if lease.holder == inbox.node_id || lease.is_claimable(now_unix_ms()) {
            // Nobody to leave it with: the lease path (a takeover drains
            // whatever this op already put in the bucket).
            inbox.stats.unavailable.fetch_add(1, Ordering::Relaxed);
            return MutateOutcome::Busy;
        }
        let epoch = lease.epoch;
        if let Some((old_epoch, old_key)) = submitted {
            // Stranded under an older epoch: delete the stale batch (a
            // drain that already read it is unaffected) and re-submit
            // the same rid under the new one.
            if old_epoch < epoch {
                let _ = inbox.store.delete(old_key).await;
                inbox.stats.resubmitted_ops.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    ?rid,
                    old_epoch,
                    epoch,
                    "inbox: a takeover stranded a submitted op; re-submitting by rid"
                );
            }
        }
        let queued_at = Instant::now();
        // Queued in order: from here on the holder executes this op
        // behind everything this node queued before it, so the
        // ordering guards have done their job. Counted as pending from
        // now, so the sync loop tails at the hot interval.
        inbox.pending_ops.fetch_add(1, Ordering::Relaxed);
        let _pending = PendingGuard(inbox);
        let key = match inbox
            .submit(epoch, rid, op_bytes.clone(), guards.take())
            .await
        {
            Ok(key) => key,
            Err(SubmitFailure::StaleEpoch) => {
                *inbox.lease_cache.lock().unwrap() = None;
                continue;
            }
            Err(SubmitFailure::Transport) => {
                inbox.stats.unavailable.fetch_add(1, Ordering::Relaxed);
                return MutateOutcome::Busy;
            }
        };
        let durable_at = Instant::now();
        submitted = Some((epoch, key));
        let mut last_recheck = Instant::now();
        loop {
            if let Some(outcome) = local_outcome(meta, rid, epoch) {
                let queue_wait = durable_at.duration_since(queued_at).as_micros() as u64;
                let outcome_wait = durable_at.elapsed().as_micros() as u64;
                let s = &inbox.stats;
                s.queue_wait_us_total
                    .fetch_add(queue_wait, Ordering::Relaxed);
                s.outcome_wait_us_total
                    .fetch_add(outcome_wait, Ordering::Relaxed);
                s.round_trip_us_total
                    .fetch_add(queue_wait + outcome_wait, Ordering::Relaxed);
                s.round_trip_samples.fetch_add(1, Ordering::Relaxed);
                inbox.note_inbox_op(Duration::from_micros(queue_wait + outcome_wait));
                tracing::debug!(
                    ?rid,
                    epoch,
                    n = key.n,
                    queue_wait_ms = queue_wait / 1_000,
                    outcome_wait_ms = outcome_wait / 1_000,
                    "inbox: op answered through the log"
                );
                return outcome;
            }
            if started.elapsed() >= inbox.deadline {
                tracing::warn!(
                    ?rid,
                    epoch,
                    n = key.n,
                    waited = ?started.elapsed(),
                    "inbox: no outcome before the deadline; the op is in doubt and takes \
                     the lease path"
                );
                inbox.stats.unavailable.fetch_add(1, Ordering::Relaxed);
                return MutateOutcome::Busy;
            }
            tokio::time::sleep(OUTCOME_POLL).await;
            if last_recheck.elapsed() >= inbox.recheck {
                last_recheck = Instant::now();
                match inbox.read_lease(Duration::ZERO).await {
                    Some(l) if l.holder == inbox.node_id => {
                        // Round 3b: this node took the lease (an
                        // escalation landed). Its takeover gate drains
                        // this batch and writes the outcome locally; give
                        // that a moment, then let the lease path — which
                        // this node now satisfies at once — resolve the
                        // rid against `completed`. The stale batch is
                        // gone either way: nobody polls our own prefix.
                        let until = Instant::now() + SELF_HOLD_WAIT;
                        while Instant::now() < until {
                            if let Some(outcome) = local_outcome(meta, rid, epoch) {
                                return outcome;
                            }
                            tokio::time::sleep(OUTCOME_POLL).await;
                        }
                        let _ = inbox.store.delete(key).await;
                        inbox.stats.unavailable.fetch_add(1, Ordering::Relaxed);
                        return MutateOutcome::Busy;
                    }
                    Some(l) if l.epoch > epoch => break, // stranded: re-submit
                    Some(l) if l.is_claimable(now_unix_ms()) => {
                        // The holder is gone; whoever takes over drains
                        // the batch. The lease path resolves the rid.
                        inbox.stats.unavailable.fetch_add(1, Ordering::Relaxed);
                        return MutateOutcome::Busy;
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Why an inbox op could not be executed right now.
#[derive(Debug)]
enum Halt {
    /// The lease view is closed (releasing, or the takeover gate is
    /// pending): stop at this batch and re-fetch it next round.
    Fenced,
    Meta(anyhow::Error),
}

impl From<constellation_meta::MetaError> for Halt {
    fn from(e: constellation_meta::MetaError) -> Self {
        Halt::Meta(e.into())
    }
}

/// Execute one op of one batch as holder. `view` is `Some` on the poll
/// path (admitted through the lease view like every other local
/// mutation) and `None` inside the takeover gate, where the view is
/// deliberately closed and the gate itself is the authority.
fn execute_inbox_op(
    meta: &Meta,
    view: Option<&crate::lease::LeaseView>,
    node_id: u64,
    batch: &InboxBatch,
    i: usize,
    stats: &InboxStats,
) -> Result<(), Halt> {
    let op = &batch.ops[i];
    let rid = Rid {
        node: op.rid.node,
        incarnation: op.rid.incarnation,
        seq: op.rid.seq,
    };
    let ack = InboxAck {
        epoch: batch.epoch,
        node: batch.node,
        n: batch.n,
        i: i as u32,
    };
    // The position watermark first: answered by an earlier tenure, past
    // whatever `completed` still remembers.
    if meta
        .inbox_ack(batch.epoch, batch.node)?
        .is_some_and(|w| w.covers(batch.n, i as u32))
    {
        stats.deduped_ops.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }
    // Then the rid: executed or refused already (this tenure's `recent`
    // or journal, or the log), by any path — a P2P forward that raced
    // this batch, a drain, a re-submission.
    if meta.recent_outcome(rid).is_some() || meta.completed_outcome(rid)?.is_some() {
        meta.journal_inbox_ack(ack)?;
        stats.deduped_ops.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }
    let _admitted = match view {
        Some(view) => {
            let Some(guard) = view.admit() else {
                return Err(Halt::Fenced);
            };
            if view.new_mutation_epoch(node_id).is_none() {
                return Err(Halt::Fenced);
            }
            Some(guard)
        }
        None => None,
    };
    let decoded = MutateOp::from_postcard(&op.op);
    let executed = match &decoded {
        Ok(op) => {
            let armed = meta.pending_inbox_ack(ack);
            let r = execute_mutate(meta, op, Some(rid));
            drop(armed);
            r
        }
        Err(_) => Err(constellation_meta::MetaError::Invalid(
            "undecodable inbox op".into(),
        )),
    };
    match executed {
        Ok(records) => {
            meta.remember_outcome(rid, &records);
            if let Some(view) = view {
                view.touch();
            }
            stats.executed_ops.fetch_add(1, Ordering::Relaxed);
        }
        Err(constellation_meta::MetaError::Conflict) => {
            // A stale manifest base: `ESTALE` on the log, and the
            // requester rebases from its own replica.
            meta.journal_inbox_refusal(rid, libc::ESTALE, ack)?;
            stats.refused_ops.fetch_add(1, Ordering::Relaxed);
        }
        Err(error) => {
            let errno = crate::forward::meta_errno(&error);
            meta.journal_inbox_refusal(rid, errno, ack)?;
            stats.refused_ops.fetch_add(1, Ordering::Relaxed);
        }
    }
    Ok(())
}

/// Execute every op of `batch` in order; returns the journal seq its
/// last row landed at (for GC), or where it had to stop.
fn execute_batch(
    meta: &Meta,
    view: Option<&crate::lease::LeaseView>,
    node_id: u64,
    batch: &InboxBatch,
    stats: &InboxStats,
) -> Result<u64, Halt> {
    for i in 0..batch.ops.len() {
        execute_inbox_op(meta, view, node_id, batch, i, stats)?;
    }
    Ok(meta.journal_next_seq()?.saturating_sub(1))
}

/// The holder's part of one sync round: GC what shipped, then GET-next
/// every due requester and execute what it finds. Never holds the
/// keepers lock across S3 I/O (plan 30 M2b's rule): the epoch and the
/// view are snapshotted under it, admission is per op through the view.
pub async fn holder_round(
    inbox: &Arc<InboxRuntime>,
    meta: &Arc<Meta>,
    keepers: &Arc<tokio::sync::Mutex<HashMap<String, LeaseKeeper>>>,
) -> anyhow::Result<()> {
    if !inbox.enabled {
        return Ok(());
    }
    let snapshot = {
        let g = keepers.lock().await;
        g.get(PARTITION).and_then(|k| {
            if k.is_lost() || k.pending_gate().is_some() {
                None
            } else {
                k.ship_epoch().map(|e| (e, k.view()))
            }
        })
    };
    let mut holder = inbox.holder.lock().await;
    let Some((epoch, view)) = snapshot else {
        if holder.take().is_some() {
            inbox.stats.tracked_requesters.store(0, Ordering::Relaxed);
            inbox.holder_min_delay_ms.store(u64::MAX, Ordering::Relaxed);
        }
        return Ok(());
    };
    if holder.as_ref().is_none_or(|st| st.poller.epoch() != epoch) {
        *holder = Some(inbox.new_holder_state(epoch));
    }
    let st = holder.as_mut().expect("set above");
    let tracked = inbox.requesters_to_poll();
    st.poller.retain_only(&tracked);
    inbox
        .stats
        .tracked_requesters
        .store(tracked.len() as u64, Ordering::Relaxed);

    // GC: batches whose rows have shipped, all but each requester's
    // newest executed one (the LIST-last high-water mark).
    let acked = meta.journal_acked_seq()?;
    let (ready, waiting): (Vec<ExecutedBatch>, Vec<ExecutedBatch>) =
        st.executed.drain(..).partition(|(_, seq)| *seq <= acked);
    let ready_keys: Vec<InboxKey> = ready.iter().map(|(k, _)| *k).collect();
    let victims = gc_keep_newest(&ready_keys);
    st.executed = waiting;
    for (key, seq) in ready {
        if victims.contains(&key) {
            match st.poller.delete(key).await {
                Ok(()) => {
                    inbox.stats.gc_deleted.fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => {
                    tracing::debug!(%error, ?key, "inbox: GC delete failed; retrying next round");
                    st.executed.push((key, seq));
                }
            }
        } else {
            st.executed.push((key, seq));
        }
    }

    // Poll the requesters whose backoff has run out.
    let now = Instant::now();
    for node in tracked {
        let delay = st.poller.delay_ms(node).unwrap_or(0);
        let due = st
            .last_poll
            .get(&node)
            .is_none_or(|t| now.duration_since(*t).as_millis() as u64 >= delay);
        if !due {
            continue;
        }
        loop {
            let run = st.poller.poll(node).await?;
            st.last_poll.insert(node, Instant::now());
            inbox.stats.polls.fetch_add(1, Ordering::Relaxed);
            if run.is_empty() {
                break;
            }
            inbox.stats.poll_hits.fetch_add(1, Ordering::Relaxed);
            let now_ms = now_unix_ms();
            for batch in &run {
                inbox.stats.pickup_ms_total.fetch_add(
                    now_ms.saturating_sub(batch.submitted_unix_ms).max(0) as u64,
                    Ordering::Relaxed,
                );
                inbox.stats.pickup_samples.fetch_add(1, Ordering::Relaxed);
            }
            let executing = Instant::now();
            for batch in &run {
                match execute_batch(meta, Some(&view), inbox.node_id, batch, &inbox.stats) {
                    Ok(seq) => st.executed.push((batch.key(), seq)),
                    Err(Halt::Fenced) => {
                        st.poller.rewind(node, batch.n);
                        inbox
                            .holder_min_delay_ms
                            .store(inbox.base_ms, Ordering::Relaxed);
                        return Ok(());
                    }
                    Err(Halt::Meta(error)) => {
                        st.poller.rewind(node, batch.n);
                        return Err(error.context("executing an inbox batch"));
                    }
                }
            }
            inbox
                .stats
                .execute_us_total
                .fetch_add(executing.elapsed().as_micros() as u64, Ordering::Relaxed);
            if !st.poller.saturated(run.len()) {
                break;
            }
        }
    }
    inbox.holder_min_delay_ms.store(
        st.poller.min_delay_ms().unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    Ok(())
}

/// Inside the takeover gate, after this node's own stranded ops were
/// replayed and before its view opens: execute every batch of every
/// epoch below `new_epoch` (dedup and watermark make re-reads exact) and
/// delete them. Returns `(batches, ops)` drained.
pub async fn drain_at_takeover(
    inbox: &InboxRuntime,
    meta: &Meta,
    node_id: u64,
    new_epoch: u64,
) -> anyhow::Result<(u64, u64)> {
    if !inbox.enabled {
        return Ok((0, 0));
    }
    let batches = inbox.store.drain_below(new_epoch).await?;
    let mut ops = 0u64;
    for batch in &batches {
        match execute_batch(meta, None, node_id, batch, &inbox.stats) {
            Ok(_) => {}
            Err(Halt::Fenced) => unreachable!("no view admission inside the gate"),
            Err(Halt::Meta(error)) => {
                return Err(error.context("draining an old-epoch inbox batch"))
            }
        }
        ops += batch.ops.len() as u64;
        inbox.store.delete(batch.key()).await?;
    }
    if !batches.is_empty() {
        tracing::info!(
            new_epoch,
            batches = batches.len(),
            ops,
            "inbox: drained older epochs' batches inside the takeover gate"
        );
    }
    inbox
        .stats
        .drained_batches
        .fetch_add(batches.len() as u64, Ordering::Relaxed);
    inbox.stats.drained_ops.fetch_add(ops, Ordering::Relaxed);
    // The poll state, if any, belongs to the previous tenure.
    if let Ok(mut holder) = inbox.holder.try_lock() {
        *holder = None;
    }
    Ok((batches.len() as u64, ops))
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_fs_core::types::ROOT_INO;
    use constellation_meta::MetaStore;
    use object_store::memory::InMemory;

    fn rid(node: u64, seq: u64) -> Rid {
        Rid {
            node,
            incarnation: 1,
            seq,
        }
    }

    fn create(name: &str, ino: u64) -> Vec<u8> {
        MutateOp::Create {
            parent: ROOT_INO,
            name: name.into(),
            ino: (1 << 40) | ino,
            mode: 0o644,
            uid: 0,
            gid: 0,
        }
        .to_postcard()
        .unwrap()
    }

    fn batch(epoch: u64, node: u64, n: u64, ops: Vec<(u64, Vec<u8>)>) -> InboxBatch {
        InboxBatch {
            epoch,
            node,
            incarnation: 1,
            n,
            submitted_unix_ms: 0,
            ops: ops
                .into_iter()
                .map(|(seq, op)| InboxOp {
                    rid: InboxRid {
                        node,
                        incarnation: 1,
                        seq,
                    },
                    op,
                })
                .collect(),
        }
    }

    fn holder_meta() -> Meta {
        let meta = Meta::open_in_memory().unwrap();
        meta.holder_epoch_cell()
            .store(1, std::sync::atomic::Ordering::SeqCst);
        meta
    }

    #[test]
    fn deadline_is_two_ttl_clamped_to_half_retention() {
        assert_eq!(wait_deadline(60_000, 900), Duration::from_millis(120_000));
        assert_eq!(wait_deadline(600_000, 900), Duration::from_millis(450_000));
        assert_eq!(wait_deadline(1, 900), Duration::from_millis(1_000));
    }

    /// A batch executes in order, each op with its rid and position
    /// acked; a second read of the same batch (a drain after a crash)
    /// executes nothing and acks nothing new.
    #[test]
    fn a_batch_executes_once_and_acks_every_position() {
        let meta = holder_meta();
        let stats = InboxStats::default();
        let b = batch(1, 9, 0, vec![(1, create("a", 1)), (2, create("b", 2))]);
        let seq = execute_batch(&meta, None, 1, &b, &stats).unwrap();
        assert!(seq >= 2);
        assert_eq!(stats.executed_ops.load(Ordering::Relaxed), 2);
        assert!(meta.lookup(ROOT_INO, "a").unwrap().is_some());
        assert!(meta.completed_position(rid(9, 1)).unwrap().is_some());
        assert_eq!(
            meta.inbox_ack(1, 9).unwrap().map(|w| (w.n, w.i)),
            Some((0, 1))
        );

        let before = meta.journal_len().unwrap();
        execute_batch(&meta, None, 1, &b, &stats).unwrap();
        assert_eq!(
            stats.executed_ops.load(Ordering::Relaxed),
            2,
            "nothing re-executed"
        );
        assert_eq!(stats.deduped_ops.load(Ordering::Relaxed), 2);
        assert_eq!(
            meta.journal_len().unwrap(),
            before,
            "the watermark answered without a row"
        );
    }

    /// A refusal is journaled as `Refused` + `InboxAck`, answered from
    /// `completed` afterwards (never re-evaluated), and a stale manifest
    /// base maps to `ESTALE`.
    #[test]
    fn a_refused_op_is_recorded_and_never_re_evaluated() {
        let meta = holder_meta();
        let stats = InboxStats::default();
        execute_batch(
            &meta,
            None,
            1,
            &batch(1, 9, 0, vec![(1, create("a", 1))]),
            &stats,
        )
        .unwrap();
        // Same name again, new rid: refused with EEXIST.
        let dup = batch(1, 9, 1, vec![(2, create("a", 3))]);
        execute_batch(&meta, None, 1, &dup, &stats).unwrap();
        assert_eq!(stats.refused_ops.load(Ordering::Relaxed), 1);
        assert_eq!(meta.refused_errno(rid(9, 2)).unwrap(), Some(libc::EEXIST));
        assert!(matches!(
            local_outcome(&meta, rid(9, 2), 1),
            Some(MutateOutcome::Errno(e)) if e == libc::EEXIST
        ));
        // Unlink the name, then re-read the old batch: still refused,
        // not executed.
        execute_batch(
            &meta,
            None,
            1,
            &batch(
                1,
                9,
                2,
                vec![(
                    3,
                    MutateOp::Unlink {
                        parent: ROOT_INO,
                        name: "a".into(),
                    }
                    .to_postcard()
                    .unwrap(),
                )],
            ),
            &stats,
        )
        .unwrap();
        execute_batch(&meta, None, 1, &dup, &stats).unwrap();
        assert!(
            meta.lookup(ROOT_INO, "a").unwrap().is_none(),
            "the refused create stayed refused"
        );
        assert_eq!(stats.refused_ops.load(Ordering::Relaxed), 1);
        assert!(matches!(
            local_outcome(&meta, rid(9, 1), 1),
            Some(MutateOutcome::Accepted { records, .. }) if records.is_empty()
        ));
        assert!(local_outcome(&meta, rid(9, 99), 1).is_none());
    }

    /// The takeover drain executes older epochs in order and deletes
    /// them; batches at or above the new epoch are left alone.
    #[tokio::test]
    async fn drain_executes_and_deletes_only_older_epochs() {
        let backend: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let inbox = InboxRuntime::new(
            1,
            1,
            backend,
            None,
            LeaseMode::Cas,
            constellation_net::Peers::disabled(),
            500,
            60_000,
            900,
        );
        let meta = holder_meta();
        meta.holder_epoch_cell()
            .store(3, std::sync::atomic::Ordering::SeqCst);
        inbox
            .store
            .put_batch(&batch(1, 9, 0, vec![(1, create("old", 1))]))
            .await
            .unwrap();
        inbox
            .store
            .put_batch(&batch(2, 8, 0, vec![(1, create("older-epoch-2", 2))]))
            .await
            .unwrap();
        inbox
            .store
            .put_batch(&batch(3, 9, 0, vec![(2, create("current", 3))]))
            .await
            .unwrap();
        let (batches, ops) = drain_at_takeover(&inbox, &meta, 1, 3).await.unwrap();
        assert_eq!((batches, ops), (2, 2));
        assert!(meta.lookup(ROOT_INO, "old").unwrap().is_some());
        assert!(meta.lookup(ROOT_INO, "older-epoch-2").unwrap().is_some());
        assert!(
            meta.lookup(ROOT_INO, "current").unwrap().is_none(),
            "not the drain's business"
        );
        assert_eq!(
            inbox.store.list_all().await.unwrap(),
            vec![InboxKey {
                epoch: 3,
                node: 9,
                n: 0
            }]
        );
        assert_eq!(inbox.stats.drained_batches.load(Ordering::Relaxed), 2);
    }

    /// Round 2: the ordering guards are released the moment an op is
    /// queued, not when its outcome arrives — otherwise every create in
    /// a directory waits one inbox round trip for the previous one. With
    /// no submitter running the submit never completes, and an
    /// overlapping gate acquisition (plus the in-flight permit) must
    /// still go through.
    #[tokio::test]
    async fn submit_releases_the_ordering_guards_once_queued() {
        let backend: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let inbox = InboxRuntime::new(
            7,
            1,
            backend,
            None,
            LeaseMode::Cas,
            constellation_net::Peers::disabled(),
            500,
            60_000,
            900,
        );
        let gate = crate::keygate::KeyGate::new();
        let inflight = Arc::new(tokio::sync::Semaphore::new(1));
        let guard = gate.acquire(vec![1, 2]).await;
        let permit = inflight.clone().acquire_owned().await.unwrap();
        let pending = {
            let inbox = inbox.clone();
            tokio::spawn(async move {
                inbox
                    .submit(1, rid(7, 0), create("a", 1), Some((guard, permit)))
                    .await
            })
        };
        // Overlapping keys, and the one permit: both free once queued.
        let again = tokio::time::timeout(Duration::from_secs(5), gate.acquire(vec![2, 3])).await;
        assert!(again.is_ok(), "the gate stayed held past the enqueue");
        assert_eq!(inflight.available_permits(), 1);
        assert!(
            !pending.is_finished(),
            "nothing submitted it: still waiting"
        );
        pending.abort();
    }

    /// Round 3b's escalation rule: sporadic ops never escalate; a
    /// sustained burst does (by count, or by cumulative wait on a slow
    /// S3); it de-escalates only once the window has fallen below half
    /// of both thresholds; the escalator's request cadence backs off.
    #[test]
    fn escalation_follows_sustained_demand_with_hysteresis() {
        let backend: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let mut inbox = InboxRuntime::new(
            1,
            1,
            backend,
            None,
            LeaseMode::Cas,
            constellation_net::Peers::disabled(),
            500,
            60_000,
            900,
        );
        {
            let r = Arc::get_mut(&mut inbox).expect("fresh");
            r.escalate_window = Duration::from_millis(200);
            r.escalate_ops = 10;
            r.escalate_wait = Duration::from_millis(500);
        }
        if !escalation_enabled() {
            return; // CONSTELLATION_INBOX_ESCALATE=off in this environment
        }
        // Three sporadic ops: nothing.
        for _ in 0..3 {
            inbox.note_inbox_op(Duration::from_millis(30));
        }
        assert!(!inbox.escalated());
        assert_eq!(inbox.stats.escalations.load(Ordering::Relaxed), 0);
        // Ten in the window: sustained by count.
        for _ in 0..7 {
            inbox.note_inbox_op(Duration::from_millis(30));
        }
        assert!(inbox.escalated());
        assert_eq!(inbox.stats.escalations.load(Ordering::Relaxed), 1);
        // Requests: first at once, then backed off.
        assert!(inbox.take_lease_request());
        assert!(!inbox.take_lease_request(), "100 ms backoff");
        assert_eq!(inbox.stats.lease_requests.load(Ordering::Relaxed), 1);
        // The window expires: quiet, de-escalated, and a fresh burst
        // counts as a new escalation.
        std::thread::sleep(Duration::from_millis(250));
        assert!(!inbox.escalated());
        // Sustained by wait (round 4's rule): the largest sample is left
        // out and at least `ESCALATE_WAIT_MIN_OPS` ops are needed, so one
        // very slow op (a first contact) is nothing, and neither are four
        // slow ones whose remainder is under the threshold.
        inbox.note_inbox_op(Duration::from_millis(5_000));
        assert!(!inbox.escalated(), "one slow op is not demand");
        for _ in 0..3 {
            inbox.note_inbox_op(Duration::from_millis(100));
        }
        assert!(!inbox.escalated(), "four ops: 300 ms after the outlier");
        inbox.note_inbox_op(Duration::from_millis(100));
        assert!(!inbox.escalated(), "five ops: 400 ms after the outlier");
        inbox.note_inbox_op(Duration::from_millis(150));
        assert!(inbox.escalated(), "six ops: 550 ms after the outlier");
        assert_eq!(inbox.stats.escalations.load(Ordering::Relaxed), 2);
        // The window empties: de-escalated.
        std::thread::sleep(Duration::from_millis(250));
        assert!(!inbox.escalated());
    }

    /// Round 3a's path rule, one assertion per state: P2P off → no path;
    /// holder unknown → no path; known but never talked to → P2P; a
    /// transport failure within the grace → still P2P (M2's retries and
    /// the lease fallback); past the grace → the inbox; any reply, or the
    /// directory showing it connected again → P2P, grace cleared.
    #[test]
    fn p2p_reaches_follows_failures_not_silence() {
        let backend: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let mut inbox = InboxRuntime::new(
            1,
            1,
            backend,
            None,
            LeaseMode::Cas,
            constellation_net::Peers::disabled(),
            500,
            60_000,
            900,
        );
        Arc::get_mut(&mut inbox).expect("fresh").p2p_grace = Duration::from_millis(20);
        let holder = 2;
        assert!(inbox.wait_deadline_for_tests() > Duration::ZERO);

        let seen = |connected: bool, last_seen: Option<Instant>| PeerSeen {
            connected,
            last_seen,
        };

        // P2P disabled: no path, whatever the directory says.
        assert!(!inbox.reach(false, Some(seen(true, None)), holder));
        assert!(!inbox.p2p_reaches(&constellation_net::Peers::disabled(), holder));
        // Unknown holder: no path.
        assert!(!inbox.reach(true, None, holder));
        // Known, never talked to: reachable — try P2P first.
        assert!(inbox.reach(true, Some(seen(false, None)), holder));
        // An outage starts the grace; inside it, still P2P.
        inbox.note_p2p_result(holder, false);
        assert!(inbox.reach(true, Some(seen(false, None)), holder));
        inbox.note_p2p_result(holder, false); // a second failure does not restart it
        std::thread::sleep(Duration::from_millis(30));
        assert!(
            !inbox.reach(true, Some(seen(false, None)), holder),
            "outage outlasted the grace: the inbox"
        );
        // Anything heard from the holder ends the outage: a reply to a
        // forward ...
        inbox.note_p2p_result(holder, true);
        assert!(inbox.reach(true, Some(seen(false, None)), holder));
        // ... the directory showing it connected, which clears a running
        // grace too ...
        inbox.note_p2p_result(holder, false);
        std::thread::sleep(Duration::from_millis(30));
        assert!(inbox.reach(true, Some(seen(true, None)), holder));
        assert!(
            inbox.reach(true, Some(seen(false, None)), holder),
            "cleared by the connection"
        );
        // ... or any exchange later than the outage began (round 4: a
        // lease reply or a ping between two forwards), even when an
        // unrelated later failure has `connected` off again. One from
        // before the outage does not count.
        let before = Instant::now();
        std::thread::sleep(Duration::from_millis(5));
        inbox.note_p2p_result(holder, false);
        std::thread::sleep(Duration::from_millis(30));
        assert!(
            !inbox.reach(true, Some(seen(false, Some(before))), holder),
            "heard from before the outage began: still an outage"
        );
        let after = Instant::now();
        assert!(
            inbox.reach(true, Some(seen(false, Some(after))), holder),
            "heard from since the outage began: cleared"
        );
        assert!(
            inbox.reach(true, Some(seen(false, None)), holder),
            "and it stays cleared"
        );
        assert_eq!(inbox.outage_ms(holder), None);
        // `note_p2p_attempt`: a non-Busy outcome is a reply; a Busy with
        // the peer unknown/disconnected and no open connection is an
        // outage. (A Busy with an open connection is covered by the
        // rule's definition: `Peers::connection_alive` needs a live
        // endpoint, exercised by `forward-timeout-reexec`.)
        let peers = constellation_net::Peers::disabled();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(inbox.note_p2p_attempt(&peers, holder, &MutateOutcome::Busy));
        assert!(inbox.p2p_down_since.lock().unwrap().contains_key(&holder));
        assert!(inbox.outage_ms(holder).is_some());
        rt.block_on(inbox.note_p2p_attempt(&peers, holder, &MutateOutcome::Errno(libc::EEXIST)));
        assert!(!inbox.p2p_down_since.lock().unwrap().contains_key(&holder));
    }

    /// Group commit: ops queued while a PUT is in flight share the next
    /// batch, and every waiter learns its key.
    #[tokio::test]
    async fn the_submitter_batches_queued_ops() {
        let backend: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let inbox = InboxRuntime::new(
            7,
            1,
            backend,
            None,
            LeaseMode::Cas,
            constellation_net::Peers::disabled(),
            500,
            60_000,
            900,
        );
        inbox.spawn_submitter(&tokio::runtime::Handle::current());
        let mut waits = Vec::new();
        for seq in 0..5u64 {
            let inbox = inbox.clone();
            waits.push(tokio::spawn(async move {
                inbox
                    .submit(4, rid(7, seq), create(&format!("f{seq}"), seq), None)
                    .await
            }));
        }
        let mut keys = Vec::new();
        for w in waits {
            keys.push(w.await.unwrap().unwrap());
        }
        let stored = inbox.store.get_run(4, 7, 0, 16).await.unwrap();
        let total: usize = stored.iter().map(|b| b.ops.len()).sum();
        assert_eq!(total, 5, "every op is in exactly one batch: {stored:?}");
        assert!(stored.len() <= 5);
        for key in keys {
            assert!(stored.iter().any(|b| b.key() == key));
        }
        assert_eq!(inbox.stats.submitted_ops.load(Ordering::Relaxed), 5);
        // A later epoch restarts numbering; an older one is refused.
        let k = inbox
            .submit(5, rid(7, 9), create("g", 9), None)
            .await
            .unwrap();
        assert_eq!((k.epoch, k.n), (5, 0));
        assert_eq!(
            inbox.submit(4, rid(7, 10), create("h", 10), None).await,
            Err(SubmitFailure::StaleEpoch)
        );
    }

    /// The requester side end to end against an in-memory bucket: with
    /// a live holder in the register, the op is submitted; once the
    /// "holder" (the same process, executing the batch as a holder would)
    /// records the outcome, the waiter returns it. A claimable register
    /// means `Busy` (the lease path).
    #[tokio::test]
    async fn forward_via_inbox_returns_the_outcome_from_the_log() {
        let backend: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let inbox = InboxRuntime::new(
            7,
            1,
            backend.clone(),
            None,
            LeaseMode::Cas,
            constellation_net::Peers::disabled(),
            500,
            60_000,
            900,
        );
        inbox.spawn_submitter(&tokio::runtime::Handle::current());
        let requester = Arc::new(Meta::open_in_memory().unwrap());
        let op = MutateOp::Create {
            parent: ROOT_INO,
            name: "via-inbox".into(),
            ino: (1 << 40) | 5,
            mode: 0o644,
            uid: 0,
            gid: 0,
        };

        // No lease object at all: nobody to submit to.
        assert!(matches!(
            forward_via_inbox(&inbox, &requester, PARTITION, &op, rid(7, 1), None).await,
            MutateOutcome::Busy
        ));

        // A live holder (node 2, epoch 4).
        let leases = LeaseStore::new(backend.clone(), PARTITION, LeaseMode::Cas);
        leases
            .try_create(&Lease::granted(PARTITION, 2, 4, 60_000))
            .await
            .unwrap();
        let waiter = {
            let (inbox, requester, op) = (inbox.clone(), requester.clone(), op.clone());
            tokio::spawn(async move {
                forward_via_inbox(&inbox, &requester, PARTITION, &op, rid(7, 1), None).await
            })
        };
        // The batch lands; "the holder" executes it and "ships" — here
        // the outcome is applied straight to the requester's replica, as
        // tailing the holder's segment would.
        let got = loop {
            let run = inbox.store.get_run(4, 7, 0, 4).await.unwrap();
            if !run.is_empty() {
                break run;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert_eq!(got[0].ops[0].rid.seq, 1);
        assert_eq!(inbox.pending_ops(), 1);
        let outcome_records = MutateOp::Records {
            records: vec![
                constellation_meta::LogRecord::Refused {
                    rid: rid(7, 1),
                    errno: libc::EEXIST,
                },
                InboxAck {
                    epoch: 4,
                    node: 7,
                    n: 0,
                    i: 0,
                }
                .record(),
            ],
        };
        execute_mutate(&requester, &outcome_records, None).unwrap();
        let outcome = waiter.await.unwrap();
        assert!(
            matches!(outcome, MutateOutcome::Errno(e) if e == libc::EEXIST),
            "{outcome:?}"
        );
        assert_eq!(inbox.pending_ops(), 0);
    }
}
