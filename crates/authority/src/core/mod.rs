//! The sans-IO authority core: `Core::handle(now, event, replica) ->
//! Vec<Action>`.
//!
//! One `Core` is one node's write-authority state machine for the one
//! log stream (`p0`; plan 29 M0a removed partitions, and M11's delegations
//! are a table *inside* the stream's authority, not a second stream). It
//! owns every decision the daemon's sync task, lease keeper, forwarder,
//! recovery drain, shipper and M13 inbox used to make, and nothing that
//! does IO. The mapping from each of those decision points to its event
//! and action is the "Plan 30 M5" sections of `docs/plans/v1/PROGRESS.md`.
//!
//! # What the core owns, what the driver owns
//!
//! The core owns: the lease view and keeper state (held lease + tag,
//! wanted-by, dwell/grace clocks, the handoff pause, the releasing flag,
//! the takeover gate, the deposition floor, the cached holder, the
//! continuation-epoch hold), the log cursor (next expected sequence,
//! highest epoch seen, head), the sync round's phase and its queued
//! successors, every client op in flight (forward attempts, backoff, the
//! inbox queue and its waiters, the lease path), the
//! replay drain's cursor and the conflict-copy retries, the ack tracker,
//! the holder's inbox poll schedule, the escalation window, the P2P
//! reachability record, and every outstanding correlation id and timer.
//!
//! The driver owns: the replica (`Meta`, reached synchronously through
//! [`crate::replica::Replica`]), the S3 clients (`LogStore`, `LeaseStore`,
//! `InboxStore`, `CommitChain`), the P2P endpoint and its wire encoding,
//! FUSE reply channels, chunk uploads, the tree publisher, and everything
//! the plan keeps out of scope (GC, prune's policy, atime accumulation,
//! the cooperative cache, pins, designations, the continuation-epoch
//! protocol itself — the core only sequences around its state).
//!
//! # The M2b rule, structurally
//!
//! `handle` never awaits, so no arm can wait on what an in-flight round
//! holds — there is no lock and no in-flight future, only state. What used
//! to be "the keepers lock across the final flush and the release CAS" is
//! the *job slot*: at most one authority job (a round, an acquisition, a
//! handoff, a flush) is in progress, later requests queue behind it, and
//! a forwarded or local mutation never enters the slot at all — it is
//! admitted by the lease view (`LeaseState::new_mutation_epoch`) and
//! executed synchronously, exactly the lock-free check-then-write
//! `dispatch_forward` ended up with after the M3a fix. A release's "wait
//! for admitted writes to drain" is therefore vacuous by construction for
//! everything the core executes; the one writer outside it (the FUSE
//! fast path's `LeaseView::admit`) is fenced by the driver re-checking
//! the journal under the mirrored releasing flag before the release CAS.

mod client;
mod holder;
mod inbox;
mod jobs;
mod lease;
mod replay;
#[cfg(test)]
mod tests;

use crate::action::{Action, ControlOk, TimerKind};
use crate::event::{Control, Event, PeerLink, PeerMsg, S3Result};
use crate::ids::{Epoch, Ms, NodeId, OpId, Seq, TimerId};
use crate::replica::Replica;
use constellation_meta::Rid;
use constellation_store_s3::inbox::InboxKey;
use std::collections::{BTreeMap, VecDeque};

pub use client::{meta_errno, ClientPhase};
pub use inbox::InboxView;
pub use jobs::JobKind;
pub use lease::{LeaseState, PendingGate, Plan};

/// Tunables, all read from `CONSTELLATION_*` by the production driver and
/// set directly by the simulation.
#[derive(Debug, Clone)]
pub struct Config {
    pub node_id: NodeId,
    pub incarnation: u32,
    pub partition: String,
    /// `LeaseMode::SingleWriter`: no `If-Match`; takeover from another
    /// node is refused.
    pub single_writer: bool,
    pub ttl_ms: u64,
    /// `lease::EXPIRY_MARGIN_MS`: a lease is usable only with this much
    /// left.
    pub expiry_margin_ms: u64,
    pub idle_release_ms: u64,
    pub dwell_ms: u64,
    pub wanted_grace_ms: u64,
    pub handoff_pause_ms: u64,
    /// `CONSTELLATION_FORWARDING`.
    pub forwarding: bool,
    /// P2P enabled (a handoff can be asked for over the network).
    pub p2p: bool,
    pub forward_timeout_ms: u64,
    pub forward_retries: u32,
    pub forward_backoff_ms: u64,
    pub acquire_deadline_ms: u64,
    pub acquire_retry_min_ms: u64,
    pub acquire_retry_max_ms: u64,
    /// How long an acquisition waits for a peer's `LeaseHandoff` before
    /// treating the request as declined (the holder may be flushing, or
    /// dead: a delivered request with no reply is the one failure the
    /// transport cannot report).
    pub handoff_request_timeout_ms: u64,
    pub sync_interval_ms: u64,
    pub idle_max_ms: u64,
    pub tail_width: usize,
    /// A tree publisher exists (a read-only member has none).
    pub publisher: bool,
    pub publish_every: u64,
    pub publish_idle_ms: u64,
    pub replay_drain_ms: u64,
    pub replay_lease_fallback_ms: u64,
    pub held_tail_staleness_ms: u64,
    pub segment_batch: usize,
    /// Byte ceiling on one shipped segment (`shipper::SEGMENT_MAX_BYTES`).
    pub segment_max_bytes: usize,
    /// `CONSTELLATION_ATIME_SHIP_MAX_DELAY_S`: a holder ships pending
    /// read-time atime on its own once the oldest row is this old.
    pub atime_ship_max_delay_ms: u64,
    /// M4: keep the lease while journal transactions are held back.
    pub keep_lease_while_held: bool,
    /// Plan 30 §M3a's behaviour: install an accepted forward's records
    /// ahead of the log even when the holder's reply says the base is
    /// stale. Off by default (the safe rule: wait for the log instead);
    /// the simulation turns it on to show the divergence it causes.
    pub speculate_on_stale_base: bool,
    // ---- plan 30 §M13: the S3 inbox ----
    /// `CONSTELLATION_INBOX`.
    pub inbox: bool,
    pub inbox_warm_max_ms: u64,
    pub inbox_cold_max_ms: u64,
    pub inbox_hot_ms: u64,
    pub inbox_hot_grace: u32,
    pub inbox_poll_width: usize,
    pub inbox_recheck_ms: u64,
    /// The in-doubt deadline of an inbox-submitted op (`min(2×TTL,
    /// retention/2)`).
    pub inbox_deadline_ms: u64,
    pub inbox_p2p_grace_ms: u64,
    /// The poll interval while an op waits on its inbox outcome.
    pub inbox_tail_ms: u64,
    /// `CONSTELLATION_INBOX_ESCALATE`.
    pub escalation: bool,
    pub escalate_window_ms: u64,
    pub escalate_ops: u64,
    pub escalate_wait_ms: u64,
    pub escalate_retry_ms: u64,
}

impl Config {
    /// Production defaults (the `CONSTELLATION_*` defaults of the code the
    /// core replaces), for `node_id` at `incarnation`.
    pub fn defaults(node_id: NodeId, incarnation: u32) -> Self {
        let ttl_ms = constellation_store_s3::lease::DEFAULT_LEASE_TTL_MS;
        Self {
            node_id,
            incarnation,
            partition: constellation_store_s3::log::PARTITION.to_string(),
            single_writer: false,
            ttl_ms,
            expiry_margin_ms: 1_000,
            idle_release_ms: 30_000,
            dwell_ms: 5_000,
            wanted_grace_ms: 5_000,
            handoff_pause_ms: 2_000,
            forwarding: true,
            p2p: true,
            forward_timeout_ms: 500,
            forward_retries: 3,
            forward_backoff_ms: 200,
            acquire_deadline_ms: 2 * ttl_ms,
            acquire_retry_min_ms: 100,
            acquire_retry_max_ms: 2_000,
            handoff_request_timeout_ms: 5_000,
            sync_interval_ms: 500,
            idle_max_ms: 10_000,
            tail_width: 16,
            publisher: true,
            publish_every: 32,
            publish_idle_ms: 30_000,
            replay_drain_ms: 250,
            replay_lease_fallback_ms: 10_000,
            held_tail_staleness_ms: 5_000,
            segment_batch: 10_000,
            segment_max_bytes: 4 << 20,
            atime_ship_max_delay_ms: 300_000,
            keep_lease_while_held: true,
            speculate_on_stale_base: false,
            inbox: true,
            inbox_warm_max_ms: 2_000,
            inbox_cold_max_ms: 10_000,
            inbox_hot_ms: 20,
            inbox_hot_grace: 25,
            inbox_poll_width: 4,
            inbox_recheck_ms: 1_000,
            inbox_deadline_ms: 2 * ttl_ms,
            inbox_p2p_grace_ms: 3_000,
            inbox_tail_ms: 20,
            escalation: true,
            escalate_window_ms: 10_000,
            escalate_ops: 20,
            escalate_wait_ms: 3_000,
            escalate_retry_ms: 2_000,
        }
    }
}

/// Counters the simulation and `status` read. Every one names the
/// production counter it replaced (`SpoolInfo`, `ForwardState`,
/// `InboxStats`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    pub rounds_completed: u64,
    pub segments_shipped: u64,
    pub segments_applied: u64,
    pub fenced: u64,
    /// Foreign records skipped because pending local ops won.
    pub conflicts: u64,
    pub own_recovered: u64,
    pub forward_dedup_hits: u64,
    pub forward_retries: u64,
    pub forward_redirects: u64,
    pub forward_indoubt_resolved: u64,
    pub lease_path_taken: u64,
    pub shadows_installed: u64,
    pub hints_installed: u64,
    pub speculation_rolled_back: u64,
    pub local_rolled_back: u64,
    pub stranded_replayed: u64,
    pub replay_conflicts: u64,
    pub epoch_markers: u64,
    pub depositions: u64,
    pub takeovers: u64,
    pub handoffs_served: u64,
    pub handoffs_declined: u64,
    pub releases: u64,
    pub publishes: u64,
    pub in_doubt: u64,
    pub queued_behind_takeover: u64,
    /// Accepted forwards not installed ahead of the log because the
    /// holder reported a stale base; answered once the log carried them.
    pub awaited_log: u64,
    /// Forwards answered (any outcome) and forwards that failed at the
    /// transport or timed out (`ForwardState::ok`/`err`).
    pub forwards_ok: u64,
    pub forwards_err: u64,
    /// Gossip-pushed segments applied without a GET.
    pub pushed_applied: u64,
    // ---- M13 ----
    pub inbox_submitted_batches: u64,
    pub inbox_submitted_ops: u64,
    pub inbox_resubmitted_ops: u64,
    /// M13: batches a requester deleted itself before forwarding the op
    /// over P2P instead (the holder became reachable again).
    pub inbox_withdrawn_ops: u64,
    /// M5 round 4: forwards that first had to withdraw *more than one*
    /// durable batch of the same rid (the op was submitted to the inbox
    /// more than once under one epoch).
    pub inbox_multi_batch_withdrawals: u64,
    pub inbox_unavailable: u64,
    pub inbox_executed_ops: u64,
    pub inbox_refused_ops: u64,
    pub inbox_deduped_ops: u64,
    pub inbox_drained_batches: u64,
    pub inbox_drained_ops: u64,
    pub inbox_polls: u64,
    pub inbox_poll_hits: u64,
    pub inbox_gc_deleted: u64,
    pub inbox_answered: u64,
    pub inbox_queue_wait_ms_total: u64,
    pub inbox_outcome_wait_ms_total: u64,
    pub inbox_round_trip_ms_total: u64,
    pub inbox_pickup_ms_total: u64,
    pub inbox_pickup_samples: u64,
    pub inbox_largest_batch_ops: u64,
    pub inbox_escalations: u64,
    pub inbox_lease_requests: u64,
}

/// The log cursor and ship bookkeeping (`Shipper::PartState` + `SpoolInfo`).
#[derive(Debug, Clone)]
pub struct ShipState {
    /// The next sequence this node expects to read or write.
    pub next_seq: Seq,
    /// The highest epoch of any applied segment (fencing floor).
    pub max_epoch: Epoch,
    /// The highest sequence applied or shipped.
    pub head_seq: Seq,
    pub shipped_since_publish: u64,
    pub last_publish: Ms,
    /// When this node last read its own held stream (`held_tail_at`).
    pub held_tail_at: Option<Ms>,
    /// A tailed segment carried an epoch above the one this node holds:
    /// renew now, not at half-TTL (`prepare_renew_now`).
    pub renew_now: bool,
    /// The epoch of the last segment this node shipped (a commit carries
    /// its author's epoch).
    pub last_ship_epoch: Epoch,
    /// The last round's error, if it failed (`SpoolInfo::last_error`).
    pub last_error: Option<String>,
}

impl Default for ShipState {
    fn default() -> Self {
        Self {
            // The log starts at 1 (`Shipper::ensure_part`: `applied + 1`).
            next_seq: 1,
            max_epoch: 0,
            head_seq: 0,
            shipped_since_publish: 0,
            last_publish: Ms(0),
            held_tail_at: None,
            renew_now: false,
            last_ship_epoch: 0,
            last_error: None,
        }
    }
}

/// The driver's continuation-epoch state as last reported
/// (`Control::Epoch`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EpochState {
    pub open: bool,
    pub active: bool,
    pub frozen: bool,
    pub flushing: bool,
    pub base: Seq,
}

/// What an outstanding S3 request was for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum S3For {
    /// A job step (`jobs.rs`); the job that issued it is the current one.
    Job,
    /// A client op reading the lease to learn the holder.
    LearnHolder(Rid),
    /// A holder-side `NotHolder` answer refreshing the cached holder.
    RefreshHolder,
    /// A `Busy` acquisition registering `wanted_by` (result ignored).
    RegisterWanted,
    /// M13: the submitter's batch PUT.
    InboxPut,
    /// M13: the submitter resyncing its batch numbering.
    InboxLastN,
    /// M13: the holder polling one requester.
    InboxPoll(NodeId),
    /// M13: a waiting requester re-reading the lease.
    InboxRecheck,
    /// M13: a GC or drain delete (result only logged).
    InboxGc(InboxKey),
    /// M13: a requester deleting its own durable batch before forwarding
    /// the op over P2P instead.
    InboxWithdraw(Rid),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Timer {
    Poll,
    ForwardTimeout(Rid),
    ForwardBackoff(Rid),
    AcquireRetry(Rid),
    ClientDeadline(Rid),
    ReplayDrain,
    /// A job's outstanding peer request (`req`) went unanswered.
    JobRequestTimeout(OpId),
    InboxPoll,
    InboxRecheck,
    InboxSubmitRetry,
    EscalateTick,
}

impl Timer {
    fn kind(self) -> TimerKind {
        match self {
            Timer::Poll => TimerKind::Poll,
            Timer::ForwardTimeout(_) => TimerKind::ForwardTimeout,
            Timer::ForwardBackoff(_) => TimerKind::ForwardBackoff,
            Timer::AcquireRetry(_) => TimerKind::AcquireRetry,
            Timer::ClientDeadline(_) => TimerKind::ClientDeadline,
            Timer::ReplayDrain => TimerKind::ReplayDrain,
            Timer::JobRequestTimeout(_) => TimerKind::JobRequestTimeout,
            Timer::InboxPoll => TimerKind::InboxPoll,
            Timer::InboxRecheck => TimerKind::InboxRecheck,
            Timer::InboxSubmitRetry => TimerKind::InboxSubmitRetry,
            Timer::EscalateTick => TimerKind::EscalateTick,
        }
    }
}

/// How many of its own shipped segments a holder remembers the touched
/// keys of, for forward replies' `base` (`Core::shipped_touches`).
pub const SHIPPED_TOUCH_WINDOW: usize = 64;

pub struct Core {
    cfg: Config,
    next_op: u64,
    next_timer: u64,
    pub(crate) lease: LeaseState,
    pub(crate) ship: ShipState,
    /// The authority job slot and the requests waiting for it.
    job: Option<jobs::Job>,
    queued_jobs: VecDeque<jobs::JobReq>,
    clients: BTreeMap<Rid, client::ClientOp>,
    /// Rids answered `InDoubt` and not yet resubmitted.
    in_doubt_rids: std::collections::BTreeSet<Rid>,
    /// M13/M5: the durable inbox batch an in-doubt rid left behind, so
    /// its resubmission withdraws it before any P2P forward.
    in_doubt_batches: std::collections::BTreeMap<Rid, Vec<InboxKey>>,
    /// M5 round 3: the keys the last [`SHIPPED_TOUCH_WINDOW`] segments this
    /// holder shipped touched, newest last, so a forward reply's `base`
    /// can name the last shipped position that matters to the op rather
    /// than the head (under load a requester always trails the head by
    /// a segment or two, and the head as the base sent every reply to
    /// `AwaitingLog`). `shipped_floor` is the position below which this
    /// window says nothing: the head at the tenure's start, or the
    /// oldest segment it has forgotten.
    pub(crate) shipped_touches:
        std::collections::VecDeque<(Seq, constellation_meta::replay::TouchSet)>,
    pub(crate) shipped_floor: Seq,
    next_client_order: u64,
    /// Outstanding peer requests → the client op waiting on them.
    by_req: BTreeMap<OpId, Rid>,
    s3: BTreeMap<OpId, S3For>,
    timers: BTreeMap<TimerId, (Timer, Ms)>,
    poll_timer: Option<TimerId>,
    drain_timer: Option<TimerId>,
    idle_rounds: u32,
    /// A nudge arrived while a round was in flight: run another right
    /// after it (`nudged` in the sync loop).
    nudged: bool,
    /// A client's control request waiting for a round (`Barrier`,
    /// `PublishNow`, `Reintegrate`): answered when the round completes.
    round_waiters: Vec<(OpId, Control)>,
    /// `Control::Acquire`/`ClaimOffer` requests answered when the
    /// acquisition in the slot finishes.
    acquire_waiters: Vec<(OpId, Control)>,
    acked: client::AckTracker,
    replay: replay::ReplayState,
    publishing: Option<OpId>,
    /// M4: journal transactions the last upload pass held back.
    held_back: u64,
    /// An acquisition's classified plan, kept across its takeover tail.
    pending_plan: Option<Plan>,
    marker_attempts: u32,
    ship_attempts: u32,
    ship_purpose: jobs::ShipPurpose,
    /// A publish requested explicitly (`PublishNow`): run at the end of
    /// the next round regardless of cadence.
    publish_forced: bool,
    /// Continuation epoch: journal locally, never PUT a segment.
    skip_ship: bool,
    epoch: EpochState,
    /// What the last deposition recovery did (`Control::Reintegrate`).
    last_recovery: Option<String>,
    roster: Vec<NodeId>,
    links: BTreeMap<NodeId, PeerLink>,
    pub(crate) inbox: inbox::InboxState,
    stopped: bool,
    pub stats: Stats,
}

impl Core {
    pub fn new(cfg: Config) -> Self {
        Self {
            lease: LeaseState::default(),
            ship: ShipState::default(),
            next_op: 1,
            next_timer: 1,
            job: None,
            queued_jobs: VecDeque::new(),
            clients: BTreeMap::new(),
            in_doubt_rids: std::collections::BTreeSet::new(),
            in_doubt_batches: std::collections::BTreeMap::new(),
            shipped_touches: std::collections::VecDeque::new(),
            shipped_floor: 0,
            next_client_order: 0,
            by_req: BTreeMap::new(),
            s3: BTreeMap::new(),
            timers: BTreeMap::new(),
            poll_timer: None,
            drain_timer: None,
            idle_rounds: 0,
            nudged: false,
            round_waiters: Vec::new(),
            acquire_waiters: Vec::new(),
            acked: client::AckTracker::default(),
            replay: replay::ReplayState::default(),
            publishing: None,
            held_back: 0,
            pending_plan: None,
            marker_attempts: 0,
            ship_attempts: 0,
            ship_purpose: jobs::ShipPurpose::Journal,
            publish_forced: false,
            skip_ship: false,
            epoch: EpochState::default(),
            last_recovery: None,
            roster: Vec::new(),
            links: BTreeMap::new(),
            inbox: inbox::InboxState::default(),
            stopped: false,
            stats: Stats::default(),
            cfg,
        }
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    pub fn node_id(&self) -> NodeId {
        self.cfg.node_id
    }

    /// The lease this node believes it holds, for `status`.
    pub fn lease(&self) -> &LeaseState {
        &self.lease
    }

    pub fn ship(&self) -> &ShipState {
        &self.ship
    }

    pub fn epoch_state(&self) -> EpochState {
        self.epoch
    }

    /// The current authority job, for `status` and tests.
    pub fn job(&self) -> Option<JobKind> {
        self.job.as_ref().map(|j| j.kind())
    }

    /// Client ops in flight (rid → phase), for tests.
    pub fn clients(&self) -> impl Iterator<Item = (Rid, ClientPhase)> + '_ {
        self.clients.iter().map(|(rid, op)| (*rid, op.phase_kind()))
    }

    /// M13: the inbox's observable state, for `status`.
    pub fn inbox_view(&self, now: Ms) -> InboxView {
        self.inbox.view(now, &self.cfg, &self.roster)
    }

    /// Refused replays whose conflict copy is pending / stalled
    /// (`SpoolInfo::replay_copies_*`).
    pub fn replay_copies(&self, now: Ms) -> (u64, u64) {
        self.replay
            .copy_counts(now, self.cfg.replay_lease_fallback_ms)
    }

    /// The core answered its final flush and takes no more events.
    pub fn stopped(&self) -> bool {
        self.stopped
    }

    /// The first call after construction (mount): the periodic poll and
    /// the replay drain start; the replica's persisted deposition flag
    /// is honoured (`recover_deposed` at mount).
    pub fn start(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        replica.set_holder_epoch(0);
        self.ship.last_publish = now;
        let applied = replica.applied_seq().unwrap_or(0);
        self.ship.next_seq = applied + 1;
        self.ship.head_seq = applied;
        if replica.lost_persisted() {
            self.lease.force_lost();
        }
        self.arm_poll(now, 0, out);
        self.arm_drain(now, out);
    }

    /// Handle one event. Every action returned must be carried out by the
    /// driver after this returns; none of them may be waited on before.
    pub fn handle(&mut self, now: Ms, event: Event, replica: &dyn Replica) -> Vec<Action> {
        let mut out = Vec::new();
        if self.stopped {
            return out;
        }
        match event {
            Event::Submit { rid, op, policy } => {
                self.on_submit(now, rid, op, policy, replica, &mut out)
            }
            Event::Peer { from, msg } => self.on_peer(now, from, msg, replica, &mut out),
            Event::PeerFailed { req, to, outage } => {
                self.on_peer_failed(now, req, to, outage, replica, &mut out)
            }
            Event::S3 { op, result } => self.on_s3(now, op, result, replica, &mut out),
            Event::Timer { id } => self.on_timer(now, id, replica, &mut out),
            Event::UploadsDone { op, result } => {
                self.on_uploads_done(now, op, result, replica, &mut out)
            }
            Event::PublishDone { op, ok } => {
                if self.publishing == Some(op) {
                    self.publishing = None;
                    if ok {
                        self.stats.publishes += 1;
                    }
                }
                self.on_flush_publish_done(now, op, replica, &mut out);
            }
            Event::ConflictCopyDone { queue_seq, ok } => {
                self.on_conflict_copy_done(now, queue_seq, ok, replica, &mut out)
            }
            Event::RebuildDone { op, ok } => self.on_rebuild_done(now, op, ok, replica, &mut out),
            Event::Roster { write_eligible } => {
                self.roster = write_eligible;
                // A holder polls a requester it has just learned of at
                // once (M13 round 2), not at its next round's end.
                if self.inbox.holder.is_some() {
                    self.inbox_holder_tick(now, replica, &mut out);
                }
            }
            Event::Peers { links } => {
                self.links = links.into_iter().map(|l| (l.node, l)).collect();
            }
            Event::Activity {
                last_write,
                acked_seqs,
            } => {
                if last_write > self.lease.last_write {
                    self.lease.last_write = last_write;
                }
                for seq in acked_seqs {
                    self.acked.mark_done(seq);
                }
            }
            Event::Control { op, req } => self.on_control(now, op, req, replica, &mut out),
        }
        self.inbox_after_event(now, &mut out);
        out
    }

    /// When a pending timer fires.
    pub(crate) fn timer_at(&self, id: TimerId) -> Option<Ms> {
        self.timers.get(&id).map(|(_, at)| *at)
    }

    // ---- dispatch ----

    fn on_peer(
        &mut self,
        now: Ms,
        from: NodeId,
        msg: PeerMsg,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        match msg {
            PeerMsg::MutateRequest {
                req,
                rid,
                op,
                acked_through,
            } => self.on_mutate_request(now, from, req, rid, op, acked_through, replica, out),
            PeerMsg::MutateReply {
                req,
                outcome,
                base,
                position,
            } => self.on_mutate_reply(now, from, req, outcome, (base, position), replica, out),
            PeerMsg::LeaseRequest { req } => self.on_lease_request(now, from, req, replica, out),
            PeerMsg::LeaseHandoff {
                req,
                released,
                epoch,
                head_seq,
            } => self.on_lease_handoff(now, from, req, released, epoch, head_seq, replica, out),
            PeerMsg::SegmentPublished {
                seq,
                epoch,
                payload,
            } => self.on_segment_pushed(now, from, seq, epoch, payload, replica, out),
            // Later milestones' messages: acknowledged by the interface,
            // answered by nothing until they are implemented.
            other => {
                tracing::debug!(
                    node = self.cfg.node_id,
                    from,
                    ?other,
                    "unhandled peer message"
                );
            }
        }
    }

    fn on_peer_failed(
        &mut self,
        now: Ms,
        req: OpId,
        to: NodeId,
        outage: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if let Some(rid) = self.by_req.remove(&req) {
            self.stats.forwards_err += 1;
            self.note_p2p_result(now, to, !outage);
            self.on_forward_failed(now, rid, replica, out);
            return;
        }
        self.on_job_peer_failed(now, req, to, replica, out);
    }

    fn on_s3(
        &mut self,
        now: Ms,
        op: OpId,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(purpose) = self.s3.remove(&op) else {
            tracing::debug!(node = self.cfg.node_id, ?op, "stale S3 result dropped");
            return;
        };
        match purpose {
            S3For::Job => self.on_job_s3(now, op, result, replica, out),
            S3For::LearnHolder(rid) => self.on_holder_learned(now, rid, result, replica, out),
            S3For::RefreshHolder => {
                if let S3Result::LeaseGet(Ok(Some((lease, _)))) = result {
                    self.lease.note_object(now, &lease);
                }
            }
            S3For::RegisterWanted => {}
            S3For::InboxPut => self.on_inbox_put(now, result, replica, out),
            S3For::InboxLastN => self.on_inbox_last_n(now, result, replica, out),
            S3For::InboxPoll(node) => self.on_inbox_polled(now, node, result, replica, out),
            S3For::InboxRecheck => self.on_inbox_recheck(now, result, replica, out),
            S3For::InboxGc(key) => self.on_inbox_gc(now, key, result),
            S3For::InboxWithdraw(rid) => self.on_inbox_withdrawn(now, rid, result, replica, out),
        }
    }

    fn on_timer(&mut self, now: Ms, id: TimerId, replica: &dyn Replica, out: &mut Vec<Action>) {
        let Some((timer, _)) = self.timers.remove(&id) else {
            return;
        };
        match timer {
            Timer::Poll => {
                self.poll_timer = None;
                self.on_poll(now, replica, out);
            }
            Timer::ForwardTimeout(rid) => self.on_forward_timeout(now, rid, replica, out),
            Timer::ForwardBackoff(rid) => self.on_forward_backoff(now, rid, replica, out),
            Timer::AcquireRetry(rid) => self.on_acquire_retry(now, rid, replica, out),
            Timer::ClientDeadline(rid) => self.on_client_deadline(now, rid, replica, out),
            Timer::ReplayDrain => {
                self.drain_timer = None;
                self.on_drain_tick(now, replica, out);
                self.arm_drain(now, out);
            }
            Timer::JobRequestTimeout(req) => {
                self.on_job_peer_failed(now, req, 0, replica, out);
            }
            Timer::InboxPoll => {
                self.inbox.poll_timer = None;
                self.inbox_holder_tick(now, replica, out);
            }
            Timer::InboxRecheck => {
                self.inbox.recheck_timer = None;
                self.on_inbox_recheck_tick(now, replica, out);
            }
            Timer::InboxSubmitRetry => {
                self.inbox.retry_timer = None;
                self.inbox_kick(now, replica, out);
            }
            Timer::EscalateTick => {
                self.inbox.escalate_timer = None;
                self.on_escalate_tick(now, replica, out);
            }
        }
    }

    fn on_control(
        &mut self,
        now: Ms,
        op: OpId,
        req: Control,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        match req {
            Control::Nudge => {
                self.nudge(now, out);
                out.push(Action::ControlDone {
                    op,
                    result: Ok(ControlOk::Done),
                });
            }
            Control::Barrier { .. } | Control::PublishNow => {
                if matches!(req, Control::PublishNow) {
                    self.publish_forced = true;
                }
                self.round_waiters.push((op, req));
                self.nudge(now, out);
            }
            Control::Reintegrate => {
                if replica.lost_persisted() && !self.lease.lost {
                    self.lease.force_lost();
                }
                if !self.lease.lost {
                    out.push(Action::ControlDone {
                        op,
                        result: Ok(ControlOk::Text("not deposed: nothing to recover".into())),
                    });
                    return;
                }
                self.round_waiters.push((op, req));
                self.nudge(now, out);
            }
            Control::TailToHead => {
                self.enqueue_job(now, jobs::JobReq::TailToHead { control: op }, replica, out);
            }
            Control::Acquire => {
                self.acquire_waiters.push((op, req));
                self.enqueue_job(
                    now,
                    jobs::JobReq::Acquire {
                        reason: "control-acquire",
                        ask_handoff: self.cfg.p2p,
                    },
                    replica,
                    out,
                );
            }
            Control::ClaimOffer { epoch } => {
                tracing::debug!(node = self.cfg.node_id, epoch, "claiming offered lease");
                self.acquire_waiters.push((op, req));
                self.enqueue_job(
                    now,
                    jobs::JobReq::Acquire {
                        reason: "claim-offer",
                        ask_handoff: self.cfg.p2p,
                    },
                    replica,
                    out,
                );
            }
            Control::Flush => {
                self.enqueue_job(
                    now,
                    jobs::JobReq::Flush {
                        control: op,
                        stop: false,
                    },
                    replica,
                    out,
                );
            }
            Control::Shutdown => {
                self.enqueue_job(
                    now,
                    jobs::JobReq::Flush {
                        control: op,
                        stop: true,
                    },
                    replica,
                    out,
                );
            }
            Control::Epoch {
                open,
                active,
                frozen,
                flushing,
                base,
            } => {
                self.on_epoch_state(
                    now,
                    EpochState {
                        open,
                        active,
                        frozen,
                        flushing,
                        base,
                    },
                    replica,
                    out,
                );
                out.push(Action::ControlDone {
                    op,
                    result: Ok(ControlOk::Done),
                });
            }
        }
    }

    /// The driver's continuation-epoch machine moved (`Control::Epoch`).
    /// Activation turns whatever S3 authority this node has into a local
    /// hold and stops shipping; closing lets the hold go and resumes
    /// shipping (the epoch's journal then reaches S3 through an ordinary
    /// acquisition).
    fn on_epoch_state(
        &mut self,
        now: Ms,
        state: EpochState,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let before = self.epoch;
        self.epoch = state;
        if state.active && !before.active {
            self.skip_ship = true;
            if self.lease.usable(now, &self.cfg) {
                let epoch = self.lease.epoch().unwrap_or(1);
                self.lease.adopt_epoch_hold(now, epoch);
                replica.set_holder_epoch(0);
            }
        } else if state.active && self.lease.usable(now, &self.cfg) && !self.lease.epoch_held() {
            // Re-affirming (a holder carrying S3 authority through an
            // epoch that was already active when it acquired).
            let epoch = self.lease.epoch().unwrap_or(1);
            self.lease.adopt_epoch_hold(now, epoch);
            replica.set_holder_epoch(0);
        }
        if !state.active && before.active && !state.frozen {
            self.skip_ship = false;
            self.lease.release_local();
            replica.set_holder_epoch(0);
            self.nudge(now, out);
        }
        if !state.open && !state.flushing {
            self.skip_ship = false;
        }
        if state.frozen && !before.frozen {
            // A frozen epoch refuses writes (EROFS): every op waiting for
            // the lease hears it now rather than at its deadline.
            self.refuse_waiting_for_lease(now, libc::EROFS, replica, out);
        }
    }

    // ---- ids, timers, the poll ----

    pub(crate) fn op_id(&mut self) -> OpId {
        let id = OpId(self.next_op);
        self.next_op += 1;
        id
    }

    fn set_timer(&mut self, at: Ms, timer: Timer, out: &mut Vec<Action>) -> TimerId {
        let id = TimerId(self.next_timer);
        self.next_timer += 1;
        self.timers.insert(id, (timer, at));
        out.push(Action::SetTimer {
            id,
            at,
            kind: timer.kind(),
        });
        id
    }

    fn cancel_timer(&mut self, id: TimerId, out: &mut Vec<Action>) {
        if self.timers.remove(&id).is_some() {
            out.push(Action::CancelTimer { id });
        }
    }

    pub(crate) fn issue_s3(
        &mut self,
        req: crate::action::S3Op,
        purpose: S3For,
        out: &mut Vec<Action>,
    ) -> OpId {
        let op = self.op_id();
        self.s3.insert(op, purpose);
        out.push(Action::S3 { op, req });
        op
    }

    /// (Re)arm the periodic poll `delay_ms` from now, replacing a pending
    /// one (`poll.as_mut().reset(..)`).
    fn arm_poll(&mut self, now: Ms, delay_ms: u64, out: &mut Vec<Action>) {
        if let Some(id) = self.poll_timer.take() {
            self.cancel_timer(id, out);
        }
        let id = self.set_timer(now.plus(delay_ms), Timer::Poll, out);
        self.poll_timer = Some(id);
    }

    fn arm_drain(&mut self, now: Ms, out: &mut Vec<Action>) {
        if self.drain_timer.is_some() {
            return;
        }
        let id = self.set_timer(now.plus(self.cfg.replay_drain_ms), Timer::ReplayDrain, out);
        self.drain_timer = Some(id);
    }

    /// Run a round as soon as possible (`SyncRequest::Nudge`). A round
    /// already in flight is not cancelled — the M2b rule — it is followed
    /// by another.
    pub(crate) fn nudge(&mut self, now: Ms, out: &mut Vec<Action>) {
        if self.job.is_some() {
            self.nudged = true;
            return;
        }
        self.idle_rounds = 0;
        self.arm_poll(now, 0, out);
    }

    /// The periodic poll fired: run a round unless a job holds the slot,
    /// in which case the round follows it.
    fn on_poll(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.job.is_some() {
            self.nudged = true;
            return;
        }
        self.start_round(now, replica, out);
    }

    /// `next_poll_ms`: doubling idle backoff, capped by the ceiling and,
    /// while holding, by a quarter of the TTL so renewal is never slept
    /// through (`lease_poll_cap_ms`); M13: by the hot tail interval
    /// while an op waits on its inbox outcome.
    pub(crate) fn next_poll_ms(&self, now: Ms) -> u64 {
        let base = self.cfg.sync_interval_ms.max(1);
        let mut next = base
            .saturating_mul(1u64 << self.idle_rounds.min(20))
            .min(self.cfg.idle_max_ms.max(base));
        if self.lease.ship_epoch(now, &self.cfg).is_some() {
            next = next.min((self.cfg.ttl_ms / 4).max(1));
        }
        if !self.inbox.pending.is_empty() {
            next = next.min(self.cfg.inbox_tail_ms.max(1));
        }
        next
    }
}
