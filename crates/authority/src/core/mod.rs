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

mod backup;
mod client;
mod delegate;
mod holder;
mod inbox;
mod jobs;
mod lease;
mod locks;
mod placement;
mod promise;
mod readindex;
mod replay;
mod stream;
#[cfg(test)]
mod tests;

use crate::action::{Action, ControlOk, TimerKind};
use crate::event::{Control, Event, PeerLink, PeerMsg, S3Result};
use crate::ids::{Epoch, Ms, NodeId, OpId, Seq, TimerId};
use crate::replica::Replica;
use constellation_meta::Rid;
use constellation_store_s3::inbox::InboxKey;
use std::collections::{BTreeMap, VecDeque};

pub use backup::AckView;
pub use client::{meta_errno, ClientPhase};
pub use delegate::{DelegKind, DelegView, RecallPhase};

/// Plan 30 §M11 phase 2b: read delegations a *delegate* grants carry
/// `epoch = DELEG_READ_EPOCH_BASE + gen`, so a `Recall` of the
/// generation voids exactly them (`ReadDelegations::void_epoch`).
pub const DELEG_READ_EPOCH_BASE: u64 = 1 << 48;
pub use inbox::InboxView;
pub use jobs::JobKind;
pub use lease::{backup_claim_grace_ms, LeaseState, PendingGate, Plan};
pub use locks::LockView;
pub use promise::{lease_may_carry, resolve_epoch_claims, EpochClaimView};
pub use readindex::ReadView;
pub use stream::StreamView;

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
    /// `CONSTELLATION_FORWARD`.
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
    // ---- plan 30 §M7: direct log streams ----
    /// `CONSTELLATION_LOG_STREAMS` (default on): subscribe to the
    /// holder's log stream, and serve one while holding. Off, or with P2P
    /// off, every node tails S3 exactly as before M7.
    pub log_streams: bool,
    /// The holder sends a heartbeat frame to a subscriber it has sent
    /// nothing for this long.
    pub stream_heartbeat_ms: u64,
    /// A subscription with no frame for this long is dead.
    pub stream_timeout_ms: u64,
    /// A caught-up subscriber still probes S3 (one GET) this often.
    pub stream_backstop_ms: u64,
    /// The holder's ring of recent segments served to new subscribers.
    pub stream_ring_segments: usize,
    pub stream_ring_bytes: usize,
    /// A subscriber's reorder buffer (segments ahead of its cursor).
    pub stream_buffer_segments: usize,
    pub stream_buffer_bytes: usize,
    /// Backoff between subscription attempts (doubling to the max).
    pub stream_retry_min_ms: u64,
    pub stream_retry_max_ms: u64,
    // ---- plan 30 §M8: cto=strict ----
    /// Grant read delegations with ReadIndex answers
    /// (`CONSTELLATION_READ_DELEGATIONS`, default on). Holder-side: a
    /// bounded-mode sequencer still serves strict readers correctly.
    pub read_delegations: bool,
    /// `CONSTELLATION_READ_DELEGATION_TTL_MS` (default 5000).
    pub read_delegation_ttl_ms: u64,
    /// How long a forwarded op's reply is held for recalls before it is
    /// answered `Held` (below the requester's forward timeout).
    pub recall_hold_ms: u64,
    /// The retry delay a `Held` answer asks for.
    pub held_retry_ms: u64,
    /// A strict read's overall budget for its ReadIndex (then degraded:
    /// M6's `CONSTELLATION_SESSION_WAIT_MS`).
    pub read_index_deadline_ms: u64,
    /// Plan 30 §M8: this node has `cto=strict` mounts, which keep a short
    /// kernel cache TTL while no other node has shown itself; the first
    /// sign of one makes every acknowledgement and release wait this long
    /// (the cached entries' lifetime plus slack) once. 0: no strict
    /// mounts here, nothing to drain.
    pub kernel_cache_ttl_ms: u64,
    /// Recall read delegations before acknowledging a mutation that
    /// touched them. Always on in production; the simulation turns it
    /// off to show the close-to-open check finds what it prevents (as
    /// `speculate_on_stale_base` does for M5's rule).
    pub recall_before_ack: bool,
    // ---- plan 30 §M9: backups, seal-based failover, `ack=s3` ----
    /// This mount asks for `ack=s3` (`--ack s3`, `CONSTELLATION_ACK`, or
    /// the filesystem's policy): every acknowledgement waits for the
    /// record's segment to land in S3; no backups are used.
    pub ack_s3: bool,
    /// `CONSTELLATION_BACKUP_RTT_BUDGET_MS` (default 5): a peer is a
    /// backup candidate only while its measured RTT is within this. 0
    /// means no backup ever (today's behaviour).
    pub backup_rtt_budget_ms: u64,
    /// `CONSTELLATION_BACKUPS` (default 1): at most this many backups.
    pub backups_max: usize,
    /// Appends in flight per backup (pipelined; acknowledged
    /// cumulatively by journal position). Rows journaled while every
    /// slot is taken ride the next append: the group commit.
    pub backup_max_inflight: usize,
    /// Pre-S3 stream-ahead sends to a subscriber are at least this far
    /// apart; the rows that become durable meanwhile go in one batch.
    pub stream_ahead_holdoff_ms: u64,
    /// `CONSTELLATION_BACKUP_ACK_TIMEOUT_MS` (default 1000): a backup
    /// that makes no acknowledgement progress for this long is removed by
    /// a lease CAS before the holder acknowledges anything further.
    pub backup_ack_timeout_ms: u64,
    /// `CONSTELLATION_BACKUP_TAKEOVER_MS` (default 1500): a backup that
    /// has not heard from its holder for this long seals the epoch and
    /// takes the lease over; under `ack=s3`, any peer does. Liveness
    /// only: safety comes from the seal and the log-slot CAS.
    pub backup_takeover_ms: u64,
    /// How often the holder sends a heartbeat append to an idle backup
    /// (well inside `backup_takeover_ms`).
    pub backup_heartbeat_ms: u64,
    /// A candidate must have been connected this long before it is added
    /// (a flapping peer is never made a backup).
    pub backup_stable_ms: u64,
    /// Minimum interval between two reconfigurations of the backup set.
    pub backup_reconfig_min_ms: u64,
    /// Rows per `BackupAppend`.
    pub backup_batch_rows: usize,
    /// Plan 30 §M9: stream backup-acked transactions to log-stream
    /// subscribers ahead of S3 (`CONSTELLATION_PRE_S3_STREAMING`, default
    /// on).
    pub pre_s3_streaming: bool,
    /// Take an `ack=s3` lease over on holder silence (default on; the
    /// simulation turns it off to show TTL failover).
    pub fast_takeover: bool,
    /// This node has `cto=strict` mounts: its tenure serves strict reads
    /// from the start, so its lease says so at acquisition (a successor
    /// then waits the horizon out).
    pub strict_mounts: bool,
    // ---- plan 30 §M10: flexible-quorum continuation epochs ----
    /// `meta.json`'s `epoch_slack` (`f`): a continuation epoch may form
    /// with up to `f` write-eligible nodes missing. 0: today's rule.
    pub epoch_slack: u32,
    /// `CONSTELLATION_PROMISE_TTL_S` (default 15 s = lease TTL / 4): how
    /// long a heartbeat promise binds its writer.
    pub promise_ttl_ms: u64,
    /// How long a TTL takeover waits for its promise requests' replies
    /// before it decides on what it has.
    pub promise_wait_ms: u64,
    /// With P2P unavailable, a node with `f > 0` re-reads the lease this
    /// often to notice an unrenewed expiry and promise on its own (a
    /// would-be taker cannot ask it).
    pub promise_watch_ms: u64,
    /// The TTL takeover's promise check. Always on in production; the
    /// simulation turns it off to show its single-authority check finds
    /// what it prevents (as `speculate_on_stale_base` does for M5).
    pub takeover_promise_check: bool,
    // ---- plan 30 §M11: delegated sub-sequencers ----
    /// `CONSTELLATION_DELEGATION` (default on): this node delegates (as
    /// the root) and accepts delegations (as a delegate). Off, or P2P
    /// off, is a strict no-op: no `Delegate` record is ever written.
    pub delegation: bool,
    /// `CONSTELLATION_DELEGATION_TTL_MS` (default 5000): a grant's ttl;
    /// the margin is the lease's (`expiry_margin_ms`).
    pub delegation_ttl_ms: u64,
    /// Plan 30 §M14: `--locks cluster` (`CONSTELLATION_LOCKS`, default
    /// `cluster` with P2P, forced `local` without): lock requests reach
    /// the owning sequencer. Off: the kernel keeps locks node-local and
    /// nothing here runs.
    pub locks: bool,
    /// `CONSTELLATION_LOCK_TTL_MS` (default 5000): a lock grant's ttl;
    /// the margin is the lease's (`expiry_margin_ms`).
    pub lock_ttl_ms: u64,
    /// `CONSTELLATION_LOCK_CACHE_IDLE_MS` (default 30000): a grant with
    /// no local lock under it for this long is released (its renewals
    /// stop); the next local lock asks for a new one.
    pub lock_cache_idle_ms: u64,
    /// Rows per `DelegateStream` batch.
    pub delegation_stream_rows: usize,
    /// The delegate's stream/renew retry tick.
    pub delegation_stream_tick_ms: u64,
    /// The root reclaims a grant nobody renewed (a crashed delegate).
    /// Always on in production; the simulation turns it off to pin the
    /// liveness it provides.
    pub delegation_reclaim_expired: bool,
    // ---- plan 30 §M11 phase 2b: placement ----
    /// `CONSTELLATION_DELEGATION_PLACEMENT` (default on): the root
    /// delegates dominated subtrees by itself (`core::placement`).
    pub placement: bool,
    /// `CONSTELLATION_DELEGATION_WINDOW_MS` (30 s): the sliding window.
    pub placement_window_ms: u64,
    /// `CONSTELLATION_DELEGATION_MIN_OPS` (200): ops per window under a
    /// subtree before it may be delegated (the rate floor).
    pub placement_min_ops: u64,
    /// `CONSTELLATION_DELEGATION_DOMINANCE` (70): the share, in percent,
    /// one node must have to get a subtree; `_LEAVE` (50): below this
    /// for a dwell, the placement recalls it.
    pub placement_dominance_pct: u64,
    pub placement_leave_pct: u64,
    /// Plan 30 §M12: a hot directory several nodes each write at least
    /// this share of (and none dominates) is split into name-hash
    /// ranges delegated to them (GIGA+); 0 turns splitting off.
    pub placement_split_pct: u64,
    /// `CONSTELLATION_DELEGATION_DWELL_MS` (60 s) and
    /// `CONSTELLATION_DELEGATION_COOLDOWN_MS` (30 s).
    pub placement_dwell_ms: u64,
    pub placement_cooldown_ms: u64,
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
            // Plan 30 M5 retuned these with the sans-IO core in the
            // simulation and on the `3node-p2poff-*` meta-bench (round 4's
            // 20 ops / 3 s delayed the switch by most of a bench run). One
            // write every few seconds is 2–3 ops and well under a second
            // of waiting per window: never an escalation. The production
            // knobs (`cli::inbox::knobs`) take their defaults from here.
            escalate_ops: 8,
            escalate_wait_ms: 1_500,
            escalate_retry_ms: 2_000,
            log_streams: true,
            stream_heartbeat_ms: 1_000,
            stream_timeout_ms: 3_500,
            stream_backstop_ms: 10_000,
            stream_ring_segments: 64,
            stream_ring_bytes: 16 << 20,
            stream_buffer_segments: 256,
            stream_buffer_bytes: 64 << 20,
            stream_retry_min_ms: 500,
            stream_retry_max_ms: 5_000,
            read_delegations: true,
            read_delegation_ttl_ms: 5_000,
            recall_hold_ms: 250,
            held_retry_ms: 10,
            read_index_deadline_ms: 2_000,
            recall_before_ack: true,
            kernel_cache_ttl_ms: 0,
            ack_s3: false,
            backup_rtt_budget_ms: 5,
            backups_max: 1,
            backup_max_inflight: 8,
            stream_ahead_holdoff_ms: 5,
            backup_ack_timeout_ms: 1_000,
            backup_takeover_ms: 1_500,
            backup_heartbeat_ms: 300,
            backup_stable_ms: 2_000,
            backup_reconfig_min_ms: 3_000,
            backup_batch_rows: 2_000,
            pre_s3_streaming: true,
            fast_takeover: true,
            strict_mounts: false,
            epoch_slack: 0,
            promise_ttl_ms: 15_000,
            promise_wait_ms: 1_000,
            promise_watch_ms: 5_000,
            takeover_promise_check: true,
            delegation: true,
            delegation_ttl_ms: 5_000,
            locks: true,
            lock_ttl_ms: 5_000,
            lock_cache_idle_ms: 30_000,
            delegation_stream_rows: 256,
            delegation_stream_tick_ms: 50,
            delegation_reclaim_expired: true,
            placement: true,
            placement_window_ms: 30_000,
            placement_min_ops: 200,
            placement_dominance_pct: 70,
            placement_leave_pct: 50,
            placement_split_pct: 20,
            placement_dwell_ms: 60_000,
            placement_cooldown_ms: 30_000,
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
    /// Ops of this node whose wait its own acquisition made moot
    /// (`resolve_moot_waits`): accepted forwards awaiting the log from an
    /// earlier epoch, and inbox ops durable under an earlier epoch —
    /// answered from `completed` or executed by rid at once.
    pub awaiting_log_resolved: u64,
    pub inbox_waits_resolved: u64,
    /// Forwards answered (any outcome) and forwards that failed at the
    /// transport or timed out (`ForwardState::ok`/`err`).
    pub forwards_ok: u64,
    pub forwards_err: u64,
    // ---- M7: log streams ----
    /// Subscriber: segments applied from the holder's stream (no GET).
    pub stream_applied: u64,
    /// Subscriber: subscriptions sent.
    pub stream_subscribes: u64,
    /// Subscriber: streamed segments already applied (dropped).
    pub stream_duplicates: u64,
    /// Subscriber: frames out of order (the stream was dropped).
    pub stream_gaps: u64,
    /// Subscriber: subscriptions refused (not the holder) or ended by the
    /// holder (it stopped holding).
    pub stream_refused: u64,
    pub stream_ended: u64,
    /// Subscriber: subscriptions lost at the transport, silent past the
    /// timeout, or dropped for a full reorder buffer.
    pub stream_lost: u64,
    pub stream_timeouts: u64,
    pub stream_overflows: u64,
    /// Subscriber: rounds that skipped the S3 tail because the stream
    /// covered it.
    pub stream_tail_skips: u64,
    /// Holder: subscriptions served and declined, frames sent, and
    /// subscribers the driver dropped (slow or gone).
    pub stream_served: u64,
    pub stream_declined: u64,
    pub stream_frames_sent: u64,
    pub stream_subscribers_dropped: u64,
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
    // ---- M8: cto=strict ----
    /// Holder: ReadIndex requests answered with a position, and refused
    /// (not the holder, or fenced).
    pub read_index_served: u64,
    pub read_index_refused: u64,
    /// Holder: read delegations granted.
    pub read_grants: u64,
    /// Holder: recalls sent, acked, and outwaited (no ack by the grant's
    /// expiry: an unreachable delegate).
    pub recalls_sent: u64,
    pub recalls_acked: u64,
    pub recalls_expired: u64,
    /// Holder: acknowledgements that waited for recalls, and for how long
    /// in total (ms).
    pub recall_waits: u64,
    pub recall_wait_ms_total: u64,
    /// Holder: forwarded replies answered `Held` (the requester retried).
    pub held_replies: u64,
    /// Requester: forwards answered `Held` and retried.
    pub held_retries: u64,
    /// Reader: ReadIndex requests sent, answered, degraded, and strict
    /// reads answered by tailing S3 (no live sequencer, or P2P off).
    pub read_index_sent: u64,
    pub read_index_answered: u64,
    pub read_index_degraded: u64,
    pub read_index_tailed: u64,
    // ---- M11: delegated sub-sequencers ----
    /// Delegate: ops executed here under a delegation, forwarded to a
    /// delegate, parked for `deps` / an unrenewed grant, and answered
    /// `NotHolder` because the delegation ended.
    pub deleg_executed: u64,
    pub deleg_forwarded: u64,
    pub deleg_deps_waits: u64,
    pub deleg_parked_expired: u64,
    pub deleg_not_owner: u64,
    /// Delegate: delegations installed here, transactions streamed,
    /// batches refused, renewals granted/refused, recalls received.
    pub deleg_installed: u64,
    pub deleg_streamed_txs: u64,
    pub deleg_stream_refused: u64,
    pub deleg_renewals: u64,
    pub deleg_renewals_refused: u64,
    pub deleg_recalls_received: u64,
    /// Root: delegations written, transactions appended, batches
    /// refused, appends whose `deps` were not here (the causal-cut
    /// assertion), cross-subtree ops, recalls sent / drained / expired,
    /// unrenewed grants reclaimed, generations ended.
    pub deleg_delegated: u64,
    pub deleg_appended_txs: u64,
    pub deleg_stream_refusals: u64,
    pub deleg_deps_unsatisfied_at_append: u64,
    pub deleg_cross_subtree: u64,
    pub deleg_recalls_sent: u64,
    pub deleg_recalls_drained: u64,
    pub deleg_recalls_expired: u64,
    pub deleg_reclaimed: u64,
    pub deleg_ended: u64,
    /// Requester: ops sent to the root because the observed stream
    /// table was full (coordinator decision 4).
    pub deps_overflow_to_root: u64,
    /// Root: executions parked for a recall of a write delegation.
    pub deleg_exec_parked: u64,
    // ---- M11 phase 2b ----
    /// Root: generations learned from the log at acquisition (a
    /// predecessor's), ops refused because a designation was involved
    /// (`EROFS`/`EXDEV`), designations delegated, re-delegations after a
    /// cross-subtree op, seals asked of a dead delegate's backup and the
    /// transactions drained from it.
    pub deleg_inherited: u64,
    pub deleg_refused_designated: u64,
    pub deleg_designated: u64,
    pub deleg_redelegated: u64,
    pub deleg_seals_sent: u64,
    pub deleg_sealed_drained: u64,
    /// Delegate: streams restarted for a new root, backup appends and
    /// acks, acknowledgements parked for the backup or the segment.
    pub deleg_restreams: u64,
    /// Phase 2b round 2: forwards held by the root that a retry
    /// executed here as the delegate (the grant installed meanwhile).
    pub deleg_retry_executed: u64,
    /// M12 round 2: forwards to a delegate answered `NotHolder` while
    /// this node's table still named it the owner (its grant not yet
    /// installed there), retried instead of executed here.
    pub deleg_grant_lag_retries: u64,
    /// Phase 2b round 2: drain ticks that held a delegate-accepted
    /// shadow's replay while its generation was live.
    pub replays_held_for_stream: u64,
    /// Phase 2b: stream batches, backup appends re-sent after a lost
    /// answer.
    pub deleg_stream_timeouts: u64,
    pub deleg_backup_appends: u64,
    pub deleg_backup_acks: u64,
    pub deleg_acks_parked: u64,
    /// Backup of a delegate: transactions persisted, seals.
    pub deleg_backup_persisted: u64,
    pub deleg_backup_seals: u64,
    /// Placement: evaluations, delegations made, recalls made,
    /// candidates skipped for cool-down / an unreachable node.
    pub place_evaluations: u64,
    pub place_delegated: u64,
    pub place_recalled: u64,
    pub place_skipped_cooldown: u64,
    pub place_skipped_unreachable: u64,
    /// Plan 30 §M12: directories split into ranges, and range
    /// generations recalled (a directory whose every range was recalled
    /// is merged back).
    pub place_splits: u64,
    pub place_range_recalls: u64,
    /// Delegate: ReadIndex answered for its subtree, read grants given.
    pub deleg_read_index_served: u64,
    pub deleg_read_grants: u64,
    /// Plan 30 §M14: locks. Owner side.
    pub lock_grants: u64,
    pub lock_would_block: u64,
    pub lock_grace_refusals: u64,
    pub lock_grace_periods: u64,
    pub lock_recalls_sent: u64,
    pub lock_recalls_released: u64,
    pub lock_recalls_expired: u64,
    pub lock_waiters_parked: u64,
    pub lock_waiting_replies: u64,
    /// Plan 30 §M14: remote waiters dropped after `4 × ttl` of silence
    /// (killed while parked; a live one re-sends every `ttl/2`).
    pub lock_waiters_dropped: u64,
    pub lock_wait_ms_total: u64,
    pub lock_renewals_served: u64,
    pub lock_reclaimed: u64,
    pub lock_moved: u64,
    /// Node side.
    pub lock_requests: u64,
    pub lock_unavailable: u64,
    pub lock_grant_ms_total: u64,
    pub lock_grant_ms: [u64; 14],
    pub lock_granted_recalled: u64,
    pub lock_renewals: u64,
    pub lock_lost: u64,
    pub lock_released: u64,
    pub lock_idle_released: u64,
    pub lock_released_replies: u64,
    // ---- M9: backups, seals, `ack=s3` ----
    /// Holder: backups added to / removed from the lease, and the lease
    /// CASes spent on it (a reconfiguration is one CAS when it lands).
    pub backups_added: u64,
    pub backups_removed: u64,
    pub reconfig_cas: u64,
    /// Holder: appends sent, acks received, backups that timed out.
    pub backup_appends: u64,
    pub backup_acks: u64,
    pub backup_ack_timeouts: u64,
    /// Holder: acknowledgements that waited for durability (a backup's
    /// ack, or the segment landing), and the total wait.
    pub acks_waited: u64,
    pub ack_wait_ms_total: u64,
    /// Holder: acknowledgements abandoned by a deposition (answered in
    /// doubt / busy; retried by rid).
    pub acks_aborted: u64,
    /// Holder: transactions streamed ahead of S3 to subscribers.
    pub streamed_ahead: u64,
    /// Subscriber: streamed transactions installed / dropped (not on an
    /// applied base, or out of order).
    pub streamed_installed: u64,
    pub streamed_dropped: u64,
    /// Backup: appends persisted, epochs sealed, takeovers completed by
    /// seal, and how many transactions a takeover re-applied.
    pub backup_persisted: u64,
    pub seals: u64,
    pub backup_takeovers: u64,
    pub backup_tail_applied: u64,
    /// Any peer: fast takeovers of an `ack=s3` lease.
    pub s3_fast_takeovers: u64,
    /// Successor: takeovers that had to wait out the predecessor's
    /// delegation horizon before acknowledging mutations.
    pub ack_floor_waits: u64,
    /// Holder: ReadIndex answers and grants refused for stale S3
    /// liveness (a probe was started instead).
    pub stale_liveness_refusals: u64,
    /// Plan 30 §M10's claim rule: activations that did not carry this
    /// node's lease into the epoch.
    pub epoch_carry_refused: u64,
    /// Plan 30 §M9: definitive refusals of forwarded ops journaled as
    /// outcomes (`Refused { rid, errno }`), and replays of never
    /// acknowledged ops whose refusal was an outcome rather than a
    /// conflict copy.
    pub refusals_journaled: u64,
    pub unacked_replays_refused: u64,
    // ---- M10: heartbeat promises ----
    /// Promises persisted and PUT (on an observed unrenewed expiry, on a
    /// peer's request, or to advertise a changed slack).
    pub promise_puts: u64,
    /// Promise requests this node answered with a promise, and refused
    /// (in an open epoch, or retired).
    pub promise_requests_answered: u64,
    pub promise_requests_refused: u64,
    /// Would-be taker: promise checks run, promise requests sent, TTL
    /// takeovers refused for too few promises, and takeovers exempt as
    /// the flush re-claim of the carried lease.
    pub promise_checks: u64,
    pub promise_requests_sent: u64,
    pub takeovers_refused_promises: u64,
    pub promise_flush_exempt: u64,
    /// Activations that found this node's lease claim stale (a member
    /// knew a later epoch): deposed.
    pub epoch_stale_claims: u64,
    /// M12 round 2: epoch holds adopted by a member that restarted into
    /// an open epoch carrying its own lease (flex-crash seed 30299).
    pub epoch_holds_adopted_late: u64,
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
    /// Plan 30 §M9: the next round must read this holder's own stream
    /// (a strict read was refused for stale S3 liveness).
    pub probe_now: bool,
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
            probe_now: false,
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
    /// M8: a strict read learning who holds the lease.
    ReadHolder(OpId),
    /// M14: a lock request learning the holder; renewals relearning it.
    LockHolder(OpId),
    LockRenewHolder,
    /// M9: the holder's backup-set reconfiguration CAS, and its re-read
    /// after a conflict.
    Reconfig,
    ReconfigReread,
    /// M9: a sealed backup (or a peer of an `ack=s3` holder) reading the
    /// lease before it tries to take over.
    TakeoverGet,
    /// M9: the holder marking its tenure as one that serves strict reads
    /// (`Lease::granted_delegations`), and its re-read after a conflict.
    MarkGranting,
    MarkGrantingReread,
    /// M10: a takeover's heartbeat read, and a promise PUT (the slack it
    /// advertises).
    Heartbeats,
    HeartbeatPut(u32),
    /// M10: the idle promise watch's lease read.
    PromiseWatch,
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
    StreamHeartbeat,
    StreamWatchdog,
    GrantExpiry(u64),
    GrantQuarantine,
    HeldReply(u64),
    ReadIndexTimeout(OpId),
    ReadIndexRetry(OpId),
    ReadIndexDeadline(OpId),
    BackupTick,
    BackupWatch,
    /// Plan 30 §M9 round 2: the pre-S3 stream-ahead hold-off (batches
    /// the rows that became durable meanwhile into one send).
    StreamAhead,
    PromiseWait,
    PromiseWatch,
    DelegExpiry(u64),
    DelegRenew(u64),
    /// M14.
    LockRequestTimeout(OpId),
    LockRetry(OpId),
    LockHeldReply(u64),
    LockGrantExpiry(constellation_meta::locks::GrantId),
    LockRenewTick,
    LockWaiterTick,
    LockRenewTimeout(OpId),
    LockTestTimeout(OpId),
    DelegStream,
    /// Phase 2b: the placement's evaluation tick.
    Placement,
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
            Timer::StreamHeartbeat => TimerKind::StreamHeartbeat,
            Timer::StreamWatchdog => TimerKind::StreamWatchdog,
            Timer::GrantExpiry(_) => TimerKind::GrantExpiry,
            Timer::GrantQuarantine => TimerKind::GrantQuarantine,
            Timer::HeldReply(_) => TimerKind::HeldReply,
            Timer::ReadIndexTimeout(_) => TimerKind::ReadIndexTimeout,
            Timer::ReadIndexRetry(_) => TimerKind::ReadIndexRetry,
            Timer::ReadIndexDeadline(_) => TimerKind::ReadIndexDeadline,
            Timer::BackupTick => TimerKind::BackupTick,
            Timer::BackupWatch => TimerKind::BackupWatch,
            Timer::StreamAhead => TimerKind::BackupTick,
            Timer::PromiseWait => TimerKind::PromiseWait,
            Timer::PromiseWatch => TimerKind::PromiseWatch,
            Timer::DelegExpiry(_) => TimerKind::DelegExpiry,
            Timer::LockRequestTimeout(_) => TimerKind::LockRequestTimeout,
            Timer::LockRetry(_) => TimerKind::LockRetry,
            Timer::LockHeldReply(_) => TimerKind::LockHeldReply,
            Timer::LockGrantExpiry(_) => TimerKind::LockGrantExpiry,
            Timer::LockRenewTick => TimerKind::LockRenewTick,
            Timer::LockWaiterTick => TimerKind::LockWaiterTick,
            Timer::LockRenewTimeout(_) => TimerKind::LockRenewTimeout,
            Timer::LockTestTimeout(_) => TimerKind::LockTestTimeout,
            Timer::DelegRenew(_) => TimerKind::DelegRenew,
            Timer::DelegStream => TimerKind::DelegStream,
            Timer::Placement => TimerKind::Placement,
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
    stream: stream::StreamState,
    /// M8: ReadIndex requests, grants being recalled, parked acks.
    pub(crate) rd: readindex::ReadState,
    /// M9: the holder's backups and acknowledgement gate.
    pub(crate) ack: backup::AckState,
    /// M9: this node as a backup, and as a pre-S3 stream subscriber.
    pub(crate) bk: backup::BackupState,
    /// M10: heartbeat promises, the takeover check, the carried lease.
    pub(crate) pr: promise::PromiseState,
    /// M11: delegations held here and, as the root, the generations.
    pub(crate) dl: delegate::DelegationState,
    /// Plan 30 §M11 phase 2b: the placement window (the root).
    pl: placement::PlacementState,
    /// Plan 30 §M14.
    lk: locks::LockState,
    /// The `now` of the event being handled (for `issue_s3`'s send time).
    last_now: Ms,
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
            stream: stream::StreamState::default(),
            rd: readindex::ReadState::default(),
            ack: backup::AckState::default(),
            bk: backup::BackupState::default(),
            pr: promise::PromiseState::default(),
            dl: delegate::DelegationState::default(),
            pl: placement::PlacementState::default(),
            lk: locks::LockState::default(),
            last_now: Ms(0),
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
    /// The sizes of the core's growable containers (diagnostics: a
    /// state that grows without bound shows here).
    pub fn container_sizes(&self) -> Vec<(&'static str, usize)> {
        let mut v = vec![
            ("timers", self.timers.len()),
            ("clients", self.clients.len()),
            ("by_req", self.by_req.len()),
            ("queued_jobs", self.queued_jobs.len()),
            ("round_waiters", self.round_waiters.len()),
            ("acquire_waiters", self.acquire_waiters.len()),
            ("links", self.links.len()),
            ("inbox_pending", self.inbox.pending.len()),
        ];
        v.extend(self.stream.container_sizes());
        v.extend(self.rd.container_sizes());
        v.extend(self.dl.container_sizes());
        v.extend(self.lk.container_sizes());
        v
    }

    pub fn job(&self) -> Option<JobKind> {
        self.job.as_ref().map(|j| j.kind())
    }

    /// Client ops in flight (rid → phase), for tests.
    pub fn clients(&self) -> impl Iterator<Item = (Rid, ClientPhase)> + '_ {
        self.clients.iter().map(|(rid, op)| (*rid, op.phase_kind()))
    }

    /// M9: the acknowledgement gate's observable state, for `status`.
    pub fn ack_view(&self) -> AckView {
        self.backup_view()
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
        self.last_now = now;
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
        self.read_start(now, replica, out);
        self.locks_start(replica);
        self.backup_start(now, replica, out);
        self.promise_start(now, replica, out);
    }

    /// Handle one event. Every action returned must be carried out by the
    /// driver after this returns; none of them may be waited on before.
    pub fn handle(&mut self, now: Ms, event: Event, replica: &dyn Replica) -> Vec<Action> {
        let mut out = Vec::new();
        if self.stopped {
            return out;
        }
        self.last_now = now;
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
            Event::LockFlushed { ino, grant, ok } => {
                self.on_lock_flushed(now, ino, grant, ok, replica, &mut out)
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
                if write_eligible.iter().any(|n| *n != self.cfg.node_id) {
                    self.note_foreign(now, replica, &mut out);
                }
                self.roster = write_eligible;
                // A holder polls a requester it has just learned of at
                // once (M13 round 2), not at its next round's end.
                if self.inbox.holder.is_some() {
                    self.inbox_holder_tick(now, replica, &mut out);
                }
            }
            Event::Peers { links } => {
                tracing::trace!(
                    node = self.cfg.node_id,
                    links = ?links.iter().map(|l| (l.node, l.connected)).collect::<Vec<_>>(),
                    "peers"
                );
                if links.iter().any(|l| l.node != self.cfg.node_id) {
                    self.note_foreign(now, replica, &mut out);
                }
                self.links = links.into_iter().map(|l| (l.node, l)).collect();
                self.stream_on_peers(now);
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
            Event::SubscriberGone { node, req } => self.on_subscriber_gone(node, req),
            Event::Slack { epoch_slack } => self.on_slack(now, epoch_slack, replica, &mut out),
            Event::Control { op, req } => self.on_control(now, op, req, replica, &mut out),
        }
        self.resolve_moot_waits(now, replica, &mut out);
        self.inbox_after_event(now, &mut out);
        self.stream_after_event(now, &mut out);
        self.backup_after_event(now, replica, &mut out);
        self.promise_after_event(now, replica, &mut out);
        self.deleg_after_event(now, replica, &mut out);
        self.locks_after_event(now, replica, &mut out);
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
                deps,
            } => self.on_mutate_request(now, from, req, rid, op, acked_through, deps, replica, out),
            PeerMsg::MutateReply {
                req,
                outcome,
                base,
                position,
                gen,
            } => self.on_mutate_reply(now, from, req, outcome, (base, position, gen), replica, out),
            PeerMsg::LeaseRequest { req } => self.on_lease_request(now, from, req, replica, out),
            PeerMsg::LeaseHandoff {
                req,
                released,
                epoch,
                head_seq,
            } => self.on_lease_handoff(now, from, req, released, epoch, head_seq, replica, out),
            PeerMsg::SegmentPublished { seq, epoch } => {
                self.on_segment_pushed(now, from, seq, epoch, out)
            }
            PeerMsg::LogSubscribe { req, from: seq } => {
                self.on_log_subscribe(now, from, req, seq, out)
            }
            PeerMsg::LogUnsubscribe { req } => self.on_log_unsubscribe(from, req, out),
            PeerMsg::LogStream {
                req,
                n,
                epoch,
                head,
                segment,
            } => self.on_log_stream(now, from, req, n, epoch, head, segment, replica, out),
            PeerMsg::LogStreamEnd { req, refused } => {
                self.on_log_stream_end(now, from, req, refused, out)
            }
            PeerMsg::ReadIndex {
                req,
                ino,
                dir,
                name,
            } => self.on_read_index(now, from, req, ino, dir, name, replica, out),
            PeerMsg::ReadIndexReply { req, outcome } => {
                self.on_read_index_reply(now, from, req, outcome, replica, out)
            }
            PeerMsg::DelegationRecall { req, ino, .. } => {
                self.on_delegation_recall(from, req, ino, replica, out)
            }
            PeerMsg::DelegationRecalled { req } => {
                self.on_delegation_recalled(now, req, replica, out)
            }
            PeerMsg::BackupAppend {
                req,
                epoch,
                holder,
                config_version,
                from: from_jseq,
                txs,
                through,
            } => self.on_backup_append(
                now,
                from,
                req,
                (epoch, holder, config_version),
                from_jseq,
                txs,
                through,
                replica,
                out,
            ),
            PeerMsg::BackupAck {
                req,
                epoch,
                acked,
                sealed,
            } => self.on_backup_ack(now, from, req, epoch, acked, sealed, replica, out),
            PeerMsg::StreamAhead { epoch, base, txs } => {
                self.on_stream_ahead(now, from, epoch, base, txs, replica, out)
            }
            PeerMsg::PromiseRequest {
                req,
                expires_unix_ms,
            } => self.on_promise_request(now, from, req, expires_unix_ms, replica, out),
            PeerMsg::PromiseReply {
                req,
                until,
                epoch_slack,
            } => self.on_promise_reply(now, from, req, until, epoch_slack, replica, out),
            PeerMsg::DelegateStream { req, gen, txs } => {
                self.on_delegate_stream(now, from, req, gen, txs, replica, out)
            }
            PeerMsg::DelegateStreamAck {
                req,
                gen,
                through,
                refused,
            } => self.on_delegate_stream_ack(now, req, gen, through, refused, replica, out),
            PeerMsg::DelegRenew { req, gen, backup } => {
                self.on_deleg_renew(now, from, req, gen, backup, out)
            }
            PeerMsg::DelegRenewed {
                req,
                gen,
                ttl_ms,
                locks,
            } => {
                // The renewal installs the generation's window first; the
                // handoff is tagged with the generation itself.
                self.on_deleg_renewed(now, req, gen, ttl_ms, replica, out);
                if ttl_ms > 0 && !locks.is_empty() {
                    self.lock_install_moved(now, gen, locks, replica);
                }
            }
            PeerMsg::DelegRecall { req, gen, .. } => {
                self.on_deleg_recall(now, from, req, gen, replica, out)
            }
            PeerMsg::DelegBackupAppend { req, gen, txs } => {
                self.on_deleg_backup_append(now, from, req, gen, txs, replica, out)
            }
            PeerMsg::DelegBackupAck {
                req,
                gen,
                acked,
                sealed,
            } => self.on_deleg_backup_ack(now, from, req, gen, acked, sealed, replica, out),
            PeerMsg::DelegSeal { req, gen } => {
                self.on_deleg_seal(now, from, req, gen, replica, out)
            }
            PeerMsg::DelegSealed {
                req,
                gen,
                sealed,
                txs,
            } => self.on_deleg_sealed(now, from, req, gen, sealed, txs, replica, out),
            PeerMsg::DelegRecalled {
                req,
                gen,
                through,
                locks,
            } => {
                if !locks.is_empty() {
                    // Back in the root's table (tag 0).
                    self.lock_install_moved(now, 0, locks, replica);
                }
                self.on_deleg_recalled(now, req, gen, through, replica, out)
            }
            PeerMsg::LockRequest {
                req,
                ino,
                mode,
                blocking,
                sent,
            } => self.on_lock_request(now, from, req, ino, mode, blocking, sent, replica, out),
            PeerMsg::LockReply { req, outcome } => {
                self.on_lock_reply(now, from, req, outcome, replica, out)
            }
            PeerMsg::LockGranted { ino, sent, outcome } => {
                self.on_lock_granted_push(now, from, ino, sent, outcome, replica, out)
            }
            PeerMsg::LockRecall { req, ino, grant } => {
                self.on_lock_recall(now, from, req, ino, grant, replica, out)
            }
            PeerMsg::LockRecalled { req } => self.on_lock_recalled_ack(req),
            PeerMsg::LockReleased { ino, grant } => {
                self.on_lock_released(now, from, ino, grant, replica, out)
            }
            PeerMsg::LockRenew { req, entries } => {
                self.on_lock_renew(now, from, req, entries, replica, out)
            }
            PeerMsg::LockRenewed { req, results } => {
                self.on_lock_renewed(now, from, req, results, replica, out)
            }
            PeerMsg::LockMirror { ver, grants } => self.on_lock_mirror(from, ver, grants),
            PeerMsg::LockTest { req, ino, mode } => {
                self.on_lock_test(now, from, req, ino, mode, replica, out)
            }
            PeerMsg::LockTestReply { req, outcome } => self.on_lock_test_reply(req, outcome, out),
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
        if self.on_stream_failed(now, req, out) {
            return;
        }
        if self.on_read_request_failed(now, req, to, outage, out) {
            return;
        }
        if self.on_backup_request_failed(now, req, to) {
            return;
        }
        if self.on_promise_request_failed(now, req, replica, out) {
            return;
        }
        if self.on_deleg_request_failed(now, req, out) {
            return;
        }
        if self.on_lock_request_failed(now, req, to, outage, replica, out) {
            return;
        }
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
        self.note_s3_liveness(op, &result);
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
            S3For::ReadHolder(op) => self.on_read_holder_learned(now, op, result, replica, out),
            S3For::LockHolder(op) => self.on_lock_holder_learned(now, op, result, replica, out),
            S3For::LockRenewHolder => self.on_lock_renew_holder(now, result, out),
            S3For::Reconfig => self.on_reconfig_put(now, result, replica, out),
            S3For::ReconfigReread => self.on_reconfig_reread(now, result, replica, out),
            S3For::TakeoverGet => self.on_takeover_get(now, result, replica, out),
            S3For::MarkGranting => self.on_mark_granting_put(now, result, replica, out),
            S3For::MarkGrantingReread => self.on_mark_granting_reread(now, result, replica, out),
            S3For::Heartbeats => self.on_heartbeats(now, result, replica, out),
            S3For::HeartbeatPut(slack) => self.on_heartbeat_put(slack, result),
            S3For::PromiseWatch => {
                if let S3Result::LeaseGet(Ok(Some((lease, _)))) = result {
                    self.lease.note_object(now, &lease);
                }
            }
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
            Timer::StreamHeartbeat => self.on_stream_heartbeat(now, out),
            Timer::StreamWatchdog => self.on_stream_watchdog(now, out),
            Timer::GrantExpiry(grant) => self.on_grant_expiry(now, grant, replica, out),
            Timer::GrantQuarantine => self.on_grant_quarantine(now, replica, out),
            Timer::HeldReply(park) => self.on_held_reply_timer(park, out),
            Timer::ReadIndexTimeout(req) => self.on_read_index_timeout(now, req, out),
            Timer::ReadIndexRetry(op) => self.on_read_index_retry(now, op, replica, out),
            Timer::ReadIndexDeadline(op) => self.on_read_index_deadline(op, replica, out),
            Timer::BackupTick => {
                self.ack.tick_timer = None;
                self.backup_tick(now, replica, out);
            }
            Timer::StreamAhead => {
                self.on_stream_ahead_timer(now, replica, out);
            }
            Timer::BackupWatch => {
                self.bk.watch_timer = None;
                self.on_backup_watch(now, replica, out);
            }
            Timer::PromiseWait => self.on_promise_wait(now, replica, out),
            Timer::PromiseWatch => {
                self.pr.watch_timer = None;
                self.on_promise_watch(now, out);
            }
            Timer::DelegExpiry(gen) => self.on_deleg_expiry(now, gen, replica, out),
            Timer::DelegRenew(gen) => self.on_deleg_renew_timer(now, gen, out),
            Timer::LockRequestTimeout(req) => self.on_lock_request_timeout(now, req, out),
            Timer::LockRetry(op) => self.on_lock_retry(now, op, replica, out),
            Timer::LockHeldReply(w) => self.on_lock_held_reply_timer(w, out),
            Timer::LockGrantExpiry(id) => self.on_lock_grant_expiry(now, id, replica, out),
            Timer::LockRenewTick => self.on_lock_renew_tick(now, replica, out),
            Timer::LockWaiterTick => self.on_lock_waiter_tick(now, replica, out),
            Timer::LockRenewTimeout(req) => self.on_lock_renew_timeout(req, replica, out),
            Timer::LockTestTimeout(req) => self.on_lock_test_timeout(req, out),
            Timer::DelegStream => self.on_deleg_stream_timer(now, replica, out),
            Timer::Placement => self.on_placement_timer(now, replica, out),
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
            Control::Journaled => {
                out.push(Action::ControlDone {
                    op,
                    result: Ok(ControlOk::Done),
                });
            }
            Control::InDoubt { rid } => {
                self.note_in_doubt(rid);
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
                    self.deleg_on_lease_gone(now, replica, out);
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
            Control::ReadIndex { ino, dir, name } => {
                self.on_read_index_control(now, op, ino, dir, name, replica, out)
            }
            Control::Recall { inos } => self.on_recall_control(now, op, inos, replica, out),
            Control::Lock {
                ino,
                mode,
                blocking,
            } => self.on_lock_control(now, op, ino, mode, blocking, replica, out),
            Control::LockIdle { ino } => self.on_lock_idle_control(now, op, ino, replica, out),
            Control::LockTest { ino, mode } => {
                self.on_lock_test_control(now, op, ino, mode, replica, out)
            }
            Control::Epoch {
                open,
                active,
                frozen,
                flushing,
                base,
                members,
                carrier,
                stale_below,
            } => {
                self.pr.carried = carrier;
                self.on_epoch_state(
                    now,
                    EpochState {
                        open,
                        active,
                        frozen,
                        flushing,
                        base,
                    },
                    &members,
                    stale_below,
                    replica,
                    out,
                );
                out.push(Action::ControlDone {
                    op,
                    result: Ok(ControlOk::Done),
                });
            }
            Control::SyncDesignations { entries } => {
                self.on_control_sync_designations(now, &entries, replica, out);
                out.push(Action::ControlDone {
                    op,
                    result: Ok(ControlOk::Done),
                });
            }
            Control::Delegate { dir, node, range } => {
                let range = constellation_meta::delegation::Range {
                    bits: range.0,
                    idx: range.1,
                };
                self.on_control_delegate(now, op, dir, node, range, replica, out)
            }
            Control::Undelegate { dir } => self.on_control_undelegate(now, op, dir, replica, out),
            Control::Retire => {
                self.on_retire(now, replica, out);
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
    ///
    /// Plan 30 §M10's claim rule, enforced here since M9: the epoch
    /// carries this node's lease only if the lease's acknowledgement
    /// policy cannot be taken over from outside the epoch
    /// (`epoch_may_carry`). A lease not carried stays an S3 lease: its
    /// holder keeps acknowledging under its own policy (which, with S3
    /// unreachable, means `ack=s3` writes stall — the promise `ack=s3`
    /// makes), the epoch holds no authority, and on the probe that finds
    /// S3 back the S3 holder closes the epoch like an epoch holder would.
    fn on_epoch_state(
        &mut self,
        now: Ms,
        state: EpochState,
        members: &[NodeId],
        stale_below: Epoch,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let before = self.epoch;
        self.epoch = state;
        self.deleg_on_epoch(now, state.active && !state.frozen, replica, out);
        if state.active && !before.active {
            self.skip_ship = true;
            // Plan 30 §M10's claim resolution: a member whose lease claim
            // is below an epoch another member knows of was taken over
            // and has not heard yet — deposed, not carried.
            if !self.lease.epoch_held() && self.epoch_claim_stale(stale_below) {
                self.stats.epoch_stale_claims += 1;
                let mine = self.lease.epoch().unwrap_or(0);
                self.deposed(now, 0, stale_below, mine, replica, out);
            } else if self.lease.usable(now, &self.cfg)
                && self.carries_mine()
                && self.epoch_may_carry(members)
            {
                let epoch = self.lease.epoch().unwrap_or(1);
                self.lease.adopt_epoch_hold(now, epoch);
                replica.set_holder_epoch(0);
            }
        } else if state.active
            && self.lease.usable(now, &self.cfg)
            && !self.lease.epoch_held()
            && self.carries_mine()
            && self.epoch_may_carry(members)
        {
            // Re-affirming (a holder carrying S3 authority through an
            // epoch that was already active when it acquired).
            let epoch = self.lease.epoch().unwrap_or(1);
            self.lease.adopt_epoch_hold(now, epoch);
            replica.set_holder_epoch(0);
        }
        // M12 round 2 (flex-crash seed 30299): the epoch carries this
        // node's own lease, but the hold was never adopted — the node was
        // paused, or died, between the activation and the hold's
        // persistence, and restarted into the open epoch with no lease in
        // memory. Nobody else can hold it (the carrier is this node's
        // lease; the members promise nothing while the epoch is open, so
        // no taker gets in), and without a holder the epoch never
        // flushes or closes: every member refuses promises forever and
        // the cluster has no authority. Adopt it here; the flush and the
        // close follow once S3 is back.
        if state.open
            && !self.lease.epoch_held()
            && !self.lease.lost
            && self.lease.held.is_none()
            && self.pr.restored_hold.is_none()
            && !replica.lost_persisted()
        {
            if let Some(c) = self.pr.carried.filter(|c| c.node == self.cfg.node_id) {
                tracing::info!(
                    node = self.cfg.node_id,
                    epoch = c.epoch,
                    "adopting the epoch hold the activation carried for this node's lease \
                     (restarted into the open epoch before the hold was persisted)"
                );
                self.stats.epoch_holds_adopted_late += 1;
                self.skip_ship = true;
                self.lease.adopt_epoch_hold(now, c.epoch);
                replica.set_holder_epoch(0);
            }
        }
        self.restore_epoch_hold(now, replica);
        if !state.open && before.open {
            // Plan 30 §M9/§M10 rule (b): a member backup ran no seal
            // watch while its epoch was open; it resumes now, with a
            // fresh silence window for the holder's re-acquisition.
            self.backup_watch_after_epoch(now, out);
        }
        if !state.active && before.active && !state.frozen {
            self.skip_ship = false;
            self.lease.release_local();
            self.deleg_on_lease_gone(now, replica, out);
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
        // Plan 30 §M9: the *send* time bounds the S3 liveness a
        // successful result proves (see `backup::note_s3_liveness`).
        self.ack.s3_sent.insert(op, self.last_now);
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
