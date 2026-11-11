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
mod deleg_windows;
mod delegate;
mod holder;
mod inbox;
mod jobs;
mod kept;
mod lease;
mod locks;
mod mode;
mod placement;
mod promise;
mod readindex;
mod replay;
mod roster;
mod seal_tail;
mod stream;
#[cfg(test)]
mod tests;

use crate::action::{Action, ControlOk, TimerKind};
use crate::event::{CasFailure, Control, Event, PeerLink, PeerMsg, S3Result};
#[cfg(test)]
use crate::ids::Ms;
use crate::ids::{Epoch, HonourMs, NodeId, Now, OpId, Seq, TimerId, WaitMs};
use crate::replica::Replica;
use constellation_meta::Rid;
use constellation_store_s3::inbox::InboxKey;
use constellation_types::Code;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub use backup::{
    ack_counter, beat_sent_ms, commit_name, liveness_fresh, AckView, Asker, CommitRule, GroupOn,
    RuleStream, SealRecord, ACK_COUNTERS,
};
pub use client::{ClientPhase, ABANDONED_CODE};
pub use deleg_windows::{
    decide, DelegWindows, Refusal, RootLease, Window, WindowStats, WindowsClock,
};
pub use delegate::{DelegKind, DelegRenewCert, DelegView, RecallPhase};
pub use roster::RosterView;
pub use seal_tail::WaitingForGroup;

/// Plan 30 §M11 phase 2b: read delegations a *delegate* grants carry
/// `epoch = DELEG_READ_EPOCH_BASE + gen`, so a `DelegateNodeEnded` of the
/// generation voids exactly them (`ReadDelegations::void_epoch`).
pub const DELEG_READ_EPOCH_BASE: u64 = 1 << 48;
pub use inbox::InboxView;
pub use jobs::JobKind;
pub use lease::{backup_claim_grace_ms, LeaseState, PendingGate, Plan, ReconfigDoubt};
pub use locks::LockView;
pub use mode::AuthorityMode;
pub use promise::{lease_may_carry, resolve_epoch_claims, EpochClaimView};
pub use readindex::ReadView;
pub use stream::StreamView;

/// Tunables, all read from `CONSTELLATION_*` by the production driver and
/// set directly by the simulation.
#[derive(Debug, Clone)]
pub struct Config {
    pub node_id: NodeId,
    pub incarnation: u32,
    /// Plan 43 C7-H1: the id of the protocol clock this core's `Now`
    /// readings come from (`ProtocolClock::id`; never 0). It names the
    /// clock a stamp that outlives one process is on (the lease body's
    /// honour end, the heartbeat), so a reader knows whether it may compare
    /// it with its own readings. The engine sets it from the process's
    /// clock, the simulation from the node incarnation's.
    pub clock_id: u64,
    /// Plan 43 C7a: the bound on this node's oscillator rate error, in ppm
    /// (the protocol clock's `rate_budget_ppm`; 0: none budgeted). The
    /// engine sets it from the process's clock, the simulation from
    /// `SimConfig::rate_budget_ppm`. The clock itself scales its readings
    /// by it; the core reads it for the beat cap's span
    /// ([`Core::beat_cap_span_ms`]).
    pub clock_rate_budget_ppm: u32,
    pub partition: String,
    /// `LeaseMode::SingleWriter`: no `If-Match`; takeover from another
    /// node is refused, a released lease's included. So no node asks for
    /// the lease (no inbox escalation, no `wanted_by` registration, no P2P
    /// handoff request) and a holder never hands it over: one that did
    /// left nobody who could hold it, and every write waited out its
    /// deadline (`InDoubt`, `EIO`).
    pub single_writer: bool,
    /// Fault injection for the simulation's negative control (never set by
    /// the engine): a delegate does not wait for an op's `deps` before it
    /// executes (`delegate.rs`, the model's rule 1), so it can answer ahead
    /// of its cause. The causal-cut oracle must then fail the run.
    pub skip_deleg_dep_wait: bool,
    /// Fault injection for the simulation's negative control (never set by
    /// the engine): the root ends a sticky grant by its TTL as it does a
    /// placed one (plan 46 M1.3 step 4, `ttl_reclaims_sticky`). The sim's
    /// `sticky_ends_only_by_holder_or_operator` must then fail the run.
    pub ttl_reclaims_sticky: bool,
    /// Fault injection for the simulation's negative control (never set by
    /// the engine): a restarted holder executes writes and grants read
    /// delegations at once, without waiting for the read delegations its
    /// previous process granted (ADR-46, `Core::deleg_restart_quarantined`).
    /// The sim's close-to-open oracle must then fail the run.
    pub restart_skips_write_quarantine: bool,
    /// Fault injection for the simulation's negative control (never set by
    /// the engine): a holder's release of sticky subtrees journals its row
    /// at once, without draining its in-flight ops there and waiting for
    /// the root to append through its last row (plan 46 M1.3 step 5,
    /// `holder_releases_before_drain`). The sim fails the run on a release
    /// journaled before the drain (`deleg_sticky_release_undrained`).
    pub holder_releases_before_drain: bool,
    /// Fault injection for the simulation's negative control (never set by
    /// the engine): the root ends a sticky stream when the holder releases
    /// some of its subtrees (plan 46 M1.3 step 5, MUST-1,
    /// `partial_release_ends_stream`). The sim's
    /// `sticky_ends_only_by_holder_or_operator` must then fail the run.
    pub partial_release_ends_stream: bool,
    /// Fault injection for the simulation's negative control (never set by
    /// the engine): a node runs a forward under one of its generations
    /// whatever generation the forward was routed by (ADR-47 without the
    /// delegate fence). The sim's `nonholder_refused_a_designee_ran` must
    /// then fail `sticky-stale-route`.
    pub fence_accepts_unrouted: bool,
    /// Fault injection for the simulation's negative control (never set by
    /// the engine): the lease holder runs an inbox-ever forward under its
    /// own non-stealable generation while its lease honour is current,
    /// instead of refusing it in the generation's stream (fence C9). A
    /// successor's refusal of the retry then contradicts the run (check 1,
    /// `refused_and_completed`).
    pub holder_runs_inbox_ever: bool,
    /// Fault injection for the simulation's negative control (never set by
    /// the engine): the root and its inbox drain refuse an op under an
    /// unreachable non-stealable grant for good whatever the op's
    /// `touched_gen` (main before ADR-47's root rule). The sim's
    /// `nonholder_refused_a_designee_ran` / `refused_and_completed` must
    /// then fail `sticky-ttl`.
    pub root_ignores_touched: bool,
    /// Fault injection for the simulation (never set by the engine): a
    /// requester puts an op in the inbox whatever its `touched_gen` (no
    /// `inbox_ok` hold). The drain alone must then carry safety: the sim
    /// expects no violation and `inbox_touched_halts > 0`.
    pub inbox_ignores_touched: bool,
    /// Fault injection for the simulation's negative control (never set by
    /// the engine): the operator's abandon of a non-stealable stream ends it
    /// at once, without recalling or waiting out the read delegations its
    /// holder granted (plan 46 M1.3 step 13, C11, the model's
    /// `abandon_skips_read_recall`). The sim's `no_read_under_an_abandoned_stream`
    /// (and close-to-open) must then fail the run.
    pub abandon_skips_read_recall: bool,
    /// Test only, the sim's negative control (ADR-50, the model's
    /// `roster_partial_shrinks`): a registry read that could not be
    /// completed replaces the roster like a complete one, so a record that
    /// vanished or did not parse shrinks what the node knows. The sim's
    /// `singleton_after_peer_known` must then fail `singleton-peer-join`.
    pub roster_installs_partial: bool,
    pub ttl_ms: u64,
    /// `lease::EXPIRY_MARGIN_MS`: a lease is usable only with this much
    /// left.
    pub expiry_margin_ms: u64,
    pub idle_release_ms: u64,
    /// Plan 43 C7-H2: how long a flush or shutdown waits for the takeover
    /// gate and floor to end before it gives up and leaves the lease to
    /// lapse (more than the longest floor: 29.5 s at the defaults).
    pub flush_floor_cap_ms: u64,
    pub dwell_ms: u64,
    pub wanted_grace_ms: u64,
    pub handoff_pause_ms: u64,
    /// `CONSTELLATION_FORWARD`.
    pub forwarding: bool,
    /// Plan 31 C8: the engine profile's `LeaseMode::ForwardOnly` — the
    /// initial [`AuthorityMode::forward_only`] (`Control::Authority`
    /// changes it at run time).
    pub forward_only: bool,
    /// P2P enabled (a handoff can be asked for over the network).
    pub p2p: bool,
    pub forward_timeout_ms: u64,
    pub forward_retries: u32,
    pub forward_backoff_ms: u64,
    /// Plan 43 C2 (§5.3 item 5, §11 Q9): the end of the client deadline
    /// (`Deadlines::client_deadline`, twice `ttl_ms`)
    /// a client op spends *settling* rather than trying: its own attempts
    /// end this long before the deadline, and a settle of its rid (its
    /// outcome, or its refusal for good) runs until the deadline. Derived,
    /// no production knob: `0` (the default) is a quarter of the client
    /// deadline (the op tries for 1.5 TTL, settles for 0.5); a set value (the
    /// simulation's override) is capped at half the deadline
    /// (`Deadlines::settle_window`).
    pub settle_window_ms: u64,
    /// The store's request timeout (`0`: not known). Only the deadlines
    /// checks D13 and D14 read it.
    pub s3_request_timeout_ms: u64,
    pub acquire_retry_min_ms: u64,
    pub acquire_retry_max_ms: u64,
    /// EC2 campaign 8 A-1: while this node's own S3 path is stalled
    /// (`Event::OwnS3`) and some peer still reaches S3, a client op that
    /// has not been answered this long after its submission fails
    /// (`InDoubt`, `EIO` to FUSE) instead of waiting out
    /// the client deadline: nothing that needs this node's S3 (a lease
    /// read, an acquisition, an inbox batch) can complete, and forwarding
    /// has had this long to reach a holder
    /// (`CONSTELLATION_S3_LESS_OP_DEADLINE_MS`, default 20 s).
    pub s3_less_deadline_ms: u64,
    /// A-1: the longest pause between two forwards of an op while this
    /// node's S3 is stalled (the forward retries' backoff is capped here
    /// instead of ending in the lease path).
    pub s3_less_retry_ms: u64,
    /// How long an acquisition waits for a peer's `LeaseHandoff` before
    /// treating the request as declined (the holder may be flushing, or
    /// dead: a delivered request with no reply is the one failure the
    /// transport cannot report).
    pub handoff_request_timeout_ms: u64,
    pub sync_interval_ms: u64,
    pub idle_max_ms: u64,
    pub tail_width: usize,
    /// How often a follower whose GET-next probe found nothing confirms,
    /// with one LIST, that the empty probe means "at head" rather than
    /// "pruned past" (`Core::gap_check_due`; DESIGN.md §14 "Falling
    /// behind segment GC"). A hint or a stream head past the cursor
    /// triggers the same check at most every `gap_hint_check_ms`. The
    /// takeover CAS never relies on the interval: a claim is preceded by
    /// the check unless a handoff's head was reached.
    pub gap_check_ms: u64,
    pub gap_hint_check_ms: u64,
    /// A tree publisher exists (a read-only member has none).
    pub publisher: bool,
    pub publish_every: u64,
    pub publish_idle_ms: u64,
    pub replay_drain_ms: u64,
    pub replay_lease_fallback_ms: u64,
    /// Plan 46 M0.2 (R5): how long a placeholder (`repair drop-held` rolled
    /// back another node's unanswered row; `StrandedOp::await_retry`)
    /// waits for its requester's retry before the sequencer in force
    /// settles the rid (`Refused { rid, ABANDONED }`). `0` (the default) is
    /// the cap, `client_deadline - 2 * settle_window` (60 s at the
    /// defaults): a larger value is clamped to it
    /// ([`Config::drop_held_await`]) so that an own op on the placeholder's
    /// keys still has its window left when the requester is dead. The clock
    /// does not run while the epoch refuses writes.
    /// `CONSTELLATION_DROP_HELD_AWAIT_MS`.
    pub drop_held_await_ms: u64,
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
    /// `CONSTELLATION_READ_AHEAD_GRANTS` (default 128): the most children
    /// of a directory one ReadIndex answer grants delegations on besides
    /// the one asked for (a cold scan then costs one round trip per this
    /// many entries, not one per entry). 0: none.
    pub read_ahead_grants: usize,
    /// `CONSTELLATION_READ_GRANTS_PER_NODE` (default 8192): the most read
    /// delegations one node may hold live from this sequencer before a
    /// read-ahead grants nothing more (the single grant a read asks for is
    /// still made). Bounds the grant table `touching` scans on every write
    /// and the recalls a write to a much-scanned file fans out to.
    pub read_grants_per_node: usize,
    /// How long a forwarded op's reply is held for recalls before it is
    /// answered `Held` (below the requester's forward timeout).
    pub recall_hold_ms: u64,
    /// The safety net under the sequencer's `OwnChunks` answer: how long
    /// this node's forwarded op may wait for its own records in
    /// `AwaitingLog` when the sequencer said its stream would carry them
    /// past this node's pending chunks (`OwnChunks::Streamed`) before the
    /// driver uploads them anyway (`Action::UploadAwaited`), and how often
    /// an upload the sequencer asked for is repeated while the op still
    /// waits (a pass that failed). `CONSTELLATION_OWN_RECORD_WAIT_MS`,
    /// default 10000; 0 turns the mechanism off, `Upload` answers included
    /// (the stall comes back: for reproducing it). See
    /// `Core::await_own_records`.
    pub own_record_wait_ms: u64,
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
    /// means no backup ever, and no peer is within budget: a stealable
    /// delegation (placed or manual) is refused whatever the link measures.
    pub backup_rtt_budget_ms: u64,
    /// `CONSTELLATION_BACKUPS` (default 1): at most this many backups.
    pub backups_max: usize,
    /// At most this many of the generations a node delegates pick a backup
    /// (the lowest first; the others' group is the delegate and the root).
    /// Unlimited in production; the simulation's
    /// `delegated-two-gens-one-backed` gives one node a backed generation
    /// beside an unbacked one.
    pub delegation_backed_gens_max: usize,
    /// Appends in flight per backup (pipelined; acknowledged
    /// cumulatively by journal position). Rows journaled while every
    /// slot is taken ride the next append: the group commit.
    pub backup_max_inflight: usize,
    /// Pre-S3 stream-ahead sends to a subscriber are at least this far
    /// apart; the rows that become durable meanwhile go in one batch.
    pub stream_ahead_holdoff_ms: u64,
    /// Plan 46 M1.4 step 2: a batch of an epoch hold's stream-ahead that a
    /// member has not acknowledged within this long is sent again, from the
    /// member's acknowledged seq (the member's store takes it idempotently).
    /// The hold's answers wait for a member's ack, so a batch lost on the
    /// way must not park them for the epoch's life.
    pub hold_resend_ms: u64,
    /// Plan 46 M1.4 step 2's structural control, never set in production
    /// (`AUTHORITY_SIM_HOLD_ANSWERS_UNCOMMITTED=1`): a hold answers from the
    /// owner's disk as before (`Final`), without a member's copy. The
    /// simulation's `strict_commit` must fail with it on the flex configs.
    pub hold_answers_uncommitted: bool,
    /// Plan 46 M1.4 step 6a's negative control, never set in production
    /// (`AUTHORITY_SIM_RESUMED_SPAN_DROPPED=1`): a tenure forgets the rule of
    /// its earlier tenures' unshipped rows, as before ADR-53 (every check of
    /// `Core::unsettled_root_row` is off).
    pub resumed_span_dropped: bool,
    /// Plan 46 M1.4 step 6a's backstop test, never set in production
    /// (`AUTHORITY_SIM_RESUMED_SPAN_DROPPED=emission`): the paths that
    /// decide an earlier tenure's row early (`answer_need_of`,
    /// `completed_row_settled`) are blind to it, so that only the emission
    /// gate holds it (`held_at_emission_root_row`).
    pub root_row_emission_only: bool,
    /// A negative control for the simulation's oracle, never set in
    /// production (`AUTHORITY_SIM_DEDUP_DELEG_UNAWAITED=1`): the root answers
    /// from a delegate's row it appended without waiting for the append to
    /// be acknowledged. `strict_commit` must fail with it.
    pub dedup_deleg_rows_unawaited: bool,
    /// `CONSTELLATION_BACKUP_ACK_TIMEOUT_MS` (default 1000): a backup
    /// that makes no acknowledgement progress for this long is removed by
    /// a lease CAS before the holder acknowledges anything further —
    /// unless it is known alive (`Event::BackupAlive` within this long)
    /// and has not answered short meanwhile: then only after
    /// `backup_slow_max_ms`.
    pub backup_ack_timeout_ms: u64,
    /// `CONSTELLATION_BACKUP_SLOW_MAX_MS` (default 10000): how long a
    /// backup known alive may go without acknowledgement progress (its
    /// own authority core is slow: a loaded node) before it is removed
    /// anyway; also how long an append waits for its answer.
    pub backup_slow_max_ms: u64,
    /// `CONSTELLATION_BACKUP_TAKEOVER_MS` (default 1500): a backup that
    /// has not heard from its holder for this long seals the epoch and
    /// takes the lease over; under `ack=s3`, any peer does. Liveness
    /// only: safety comes from the seal and the log-slot CAS. The
    /// silence is measured as `liveness_lag_aware` says.
    pub backup_takeover_ms: u64,
    /// The silence watches (a backup's seal watch, `ack=s3`'s holder
    /// watch) discount this node's own lag (`Event::OwnLag`: silence
    /// counts from the end of a lapse, at most one window plus the
    /// lapses' own length later) and
    /// lengthen the window while the holder's beats have lately arrived
    /// late (`Core::silence_window`: up to four windows). Default on;
    /// off is the simulation's non-vacuity knob: plain silence, as the
    /// watches measured it before (a backup that lagged, or a holder
    /// whose beats ran late, was sealed `backup_takeover_ms` into the
    /// silence).
    pub liveness_lag_aware: bool,
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
    /// ADR-51 negative control: a takeover never collects the kept rows of
    /// the tenure it ends (the claim proceeds as before the ADR).
    pub kept_fetch_skipped: bool,
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
    /// off, is a strict no-op: no `DelegateNode` record is ever written.
    pub delegation: bool,
    /// `CONSTELLATION_DELEGATION_TTL_MS` (default 5000): a grant's ttl;
    /// the margin is the lease's (`expiry_margin_ms`).
    pub delegation_ttl_ms: u64,
    /// Plan 30 §M14: `--locks cluster` (`CONSTELLATION_LOCKS`, default
    /// `cluster` with P2P, forced `local` without): lock requests reach
    /// the owning sequencer. Off: the kernel keeps locks node-local and
    /// nothing here runs.
    pub locks: bool,
    /// Tests only (the simulator's negative control, never set by the
    /// daemon): a fast successor trusts any lock mirror like the old root's
    /// final one — its records settled as sent, its lapsed ones kept —
    /// which admits a late renewal of a grant the old root had outwaited and
    /// granted over (`Core::lock_install_mirror`).
    pub lock_trust_nonfinal_mirror: bool,
    /// `CONSTELLATION_LOCK_TTL_MS` (default 20000): a lock grant's ttl;
    /// the margin is the lease's (`expiry_margin_ms`). Long, because a
    /// holder that stalls past it loses the lock under the application
    /// (whose process is then fenced); a waiter behind a crashed holder
    /// waits up to `ttl + margin` for it.
    pub lock_ttl_ms: u64,
    /// `CONSTELLATION_LOCK_CACHE_IDLE_MS` (default 30000): a grant with
    /// no local lock under it for this long is released (its renewals
    /// stop); the next local lock asks for a new one.
    pub lock_cache_idle_ms: u64,
    /// Rows per `DelegateStream` batch.
    pub delegation_stream_rows: usize,
    /// Plan 46 M1.2 step 13's negative control, never set in production (the
    /// simulation's `AUTHORITY_SIM_LONE_DELEGATE=1`): the root grants to a
    /// node with no group within budget, and that delegate answers its own
    /// node's clients on its disk alone (`Singleton`) while no peer is
    /// linked within the budget: the deleted lone delegate. The oracles
    /// (I1, I5) must fail with it.
    pub lone_delegate: bool,
    /// Plan 46 M1.2 step 14's negative control, never set in production (the
    /// simulation's `AUTHORITY_SIM_DELEG_APPEND_REPLAY=1`): a stranding pass
    /// queues a root's append of a delegate's row for replay again, as it did
    /// when the lease epoch stood for "a root's append". The sim fails on
    /// `Stats::deleg_append_queued` and on the rows that land past a seal.
    pub deleg_append_replay: bool,
    /// Plan 46 M1.1 step 3's negative control, never set in production: a
    /// delegate that records no backup stamps its rows with
    /// a version that is in no history (its group's plus
    /// `RowCommit::PER_ROW_SKEW`) instead of its configuration's — the journal-time class of the mutable "peer in
    /// budget" assessment that rows no longer carry. The simulation's
    /// `row_version_matches_config` oracle must fail with it
    /// (`AUTHORITY_SIM_PER_ROW_CLASS=1`).
    pub per_row_class: bool,
    /// The delegate's stream/renew retry tick.
    pub delegation_stream_tick_ms: u64,
    /// Simulation only: the root drops every Nth of its unsolicited
    /// acknowledgements of a delegate's commit (`root_stream_push`; 0: none),
    /// the loss a lost push stands for (ADR-41).
    pub delegation_push_drop_every: u64,
    /// Simulation only: the root loses the first send of every Nth recall it
    /// starts (`Core::start_recall`; 0: none), the loss a request's timeout
    /// stands for in production (L3).
    pub delegation_recall_drop_every: u64,
    /// Simulation only: the delegate loses the answer to the first recall of
    /// every Nth generation (`Core::on_deleg_recall`; 0: none) (L3).
    pub delegation_recalled_drop_every: u64,
    /// Simulation only (a negative control): the renewal's stream head is
    /// stamped with this node's own clock at the send, as before plan 46
    /// M1.2 step 11, not with the root's send time of the answer received
    /// (`DelegRenew::stream_head_at`). With the settle off
    /// (`Meta::settle_tagged_writes`) a head reported while a pinned write
    /// is still committing is then claimed for it, and the next holder's
    /// section sees it land.
    pub deleg_head_at_own_clock: bool,
    /// Simulation only: the root streams every Nth batch that carries a
    /// `DelegateNode` as uncommitted (`StreamAhead.committed = false`,
    /// `Core::stream_ahead`; 0: none), so a delegate learns the grant and
    /// waits for the log (plan 46 M1.2 step 8b). It errs only toward
    /// waiting longer: the segment that carries the row commits it.
    pub stream_ahead_uncommit_grant_every: u64,
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
            clock_id: 1,
            clock_rate_budget_ppm: 1_000,
            partition: constellation_store_s3::log::PARTITION.to_string(),
            single_writer: false,
            skip_deleg_dep_wait: false,
            ttl_reclaims_sticky: false,
            restart_skips_write_quarantine: false,
            holder_releases_before_drain: false,
            partial_release_ends_stream: false,
            fence_accepts_unrouted: false,
            holder_runs_inbox_ever: false,
            root_ignores_touched: false,
            inbox_ignores_touched: false,
            abandon_skips_read_recall: false,
            roster_installs_partial: false,
            ttl_ms,
            expiry_margin_ms: 1_000,
            idle_release_ms: 30_000,
            flush_floor_cap_ms: 40_000,
            dwell_ms: 5_000,
            wanted_grace_ms: 5_000,
            handoff_pause_ms: 2_000,
            forwarding: true,
            forward_only: false,
            p2p: true,
            forward_timeout_ms: 500,
            forward_retries: 3,
            forward_backoff_ms: 200,
            settle_window_ms: 0,
            s3_request_timeout_ms: 0,
            acquire_retry_min_ms: 100,
            acquire_retry_max_ms: 2_000,
            s3_less_deadline_ms: 20_000,
            s3_less_retry_ms: 1_000,
            handoff_request_timeout_ms: 5_000,
            sync_interval_ms: 500,
            idle_max_ms: 10_000,
            tail_width: 16,
            gap_check_ms: 300_000,
            gap_hint_check_ms: 5_000,
            publisher: true,
            publish_every: 32,
            publish_idle_ms: 30_000,
            replay_drain_ms: 250,
            replay_lease_fallback_ms: 10_000,
            drop_held_await_ms: 0,
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
            read_ahead_grants: 128,
            read_grants_per_node: 8192,
            recall_hold_ms: 250,
            own_record_wait_ms: 10_000,
            held_retry_ms: 10,
            read_index_deadline_ms: 2_000,
            recall_before_ack: true,
            kernel_cache_ttl_ms: 0,
            ack_s3: false,
            backup_rtt_budget_ms: 5,
            backups_max: 1,
            delegation_backed_gens_max: usize::MAX,
            backup_max_inflight: 8,
            stream_ahead_holdoff_ms: 5,
            hold_resend_ms: 250,
            hold_answers_uncommitted: false,
            resumed_span_dropped: false,
            root_row_emission_only: false,
            dedup_deleg_rows_unawaited: false,
            backup_ack_timeout_ms: 1_000,
            backup_slow_max_ms: 10_000,
            backup_takeover_ms: 1_500,
            backup_heartbeat_ms: 300,
            backup_stable_ms: 2_000,
            backup_reconfig_min_ms: 3_000,
            backup_batch_rows: 2_000,
            pre_s3_streaming: true,
            fast_takeover: true,
            strict_mounts: false,
            epoch_slack: 0,
            kept_fetch_skipped: false,
            promise_ttl_ms: 15_000,
            promise_wait_ms: 1_000,
            promise_watch_ms: 5_000,
            takeover_promise_check: true,
            liveness_lag_aware: true,
            delegation: true,
            delegation_ttl_ms: 5_000,
            locks: true,
            lock_trust_nonfinal_mirror: false,
            lock_ttl_ms: 20_000,
            lock_cache_idle_ms: 30_000,
            delegation_stream_rows: 256,
            lone_delegate: false,
            deleg_append_replay: false,
            per_row_class: false,
            delegation_stream_tick_ms: 50,
            delegation_push_drop_every: 0,
            delegation_recall_drop_every: 0,
            delegation_recalled_drop_every: 0,
            deleg_head_at_own_clock: false,
            stream_ahead_uncommit_grant_every: 0,
            delegation_reclaim_expired: true,
            placement: true,
            placement_window_ms: 30_000,
            placement_min_ops: 200,
            placement_dominance_pct: 70,
            placement_leave_pct: 50,
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
    /// Forwarded ops the root parked unexecuted for the recall of read
    /// delegations on what they touch (`Core::recall_before_execute`).
    pub forward_recalls_first: u64,
    /// The root's own ops parked unexecuted for the same
    /// (`execute_local_outcome`).
    pub local_recalls_first: u64,
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
    /// The log was found pruned past this replica's cursor and the
    /// replica was rebuilt from the head commit.
    pub retention_gaps: u64,
    pub takeovers: u64,
    /// Plan 43 C7b: foreign lease bodies observed that carry no `ttl_ms` (an
    /// older build's, or a singleton's): judged by this node's own TTL and
    /// the wall compare as well (`LeaseState::obs_passed_for`).
    pub lease_ttl_unknown_bodies: u64,
    /// Plan 43 C7b: client deadlines moved once to cover this node's own
    /// claim of a lease (`Core::on_client_deadline_timer`).
    pub client_deadlines_extended: u64,
    pub handoffs_served: u64,
    pub handoffs_declined: u64,
    pub releases: u64,
    pub publishes: u64,
    pub in_doubt: u64,
    pub queued_behind_takeover: u64,
    /// EC2 campaign 8 A-1: forward retries made past `forward_retries`
    /// because this node's own S3 is stalled (the lease path would wait
    /// on it).
    pub s3_less_retries: u64,
    /// A-1: ops waiting on the lease path that were forwarded again
    /// because this node's own S3 is stalled.
    pub s3_less_forwards: u64,
    /// A-1: client ops answered in doubt at `s3_less_deadline_ms`.
    pub s3_less_deadlines: u64,
    /// A-1: forwards answered `Held`, and delegation renewals left
    /// unanswered, while this node re-adopts the live lease a previous
    /// incarnation of it left behind (a restart).
    pub readopt_for_forward: u64,
    /// Acquisition CASes that failed without an answer and that the
    /// re-read found landed (`jobs::acquire_cas_reread`).
    pub acquire_cas_in_doubt_landed: u64,
    /// `AwaitingLog` ops a non-holder sent to the new holder once the log
    /// moved past the epoch that accepted them without them
    /// (`client::reroute_ended_epoch_waits`).
    pub awaiting_log_rerouted: u64,
    /// Stranded foreign entries streamed by a node an admin retired,
    /// forgotten instead of replayed (`replay::drop_retired_origin`).
    pub replays_of_retired_dropped: u64,
    /// Accepted forwards not installed ahead of the log because the
    /// holder reported a stale base; answered once the log carried them.
    pub awaited_log: u64,
    /// Plan 30 §M9 (the OVH run's finding 4): accepted forwards that
    /// could not install as a shadow answered from the pre-S3 stream
    /// (their transaction installed here) instead of the log.
    pub awaited_log_streamed: u64,
    /// Of `awaited_log_streamed`, the replies of a delegate (their
    /// position names a delegation stream): answered from the root's
    /// pre-S3 stream once it carried the root's append of them.
    pub awaited_log_streamed_deleg: u64,
    /// Uploads of this node's pending chunks for its own forwarded op's
    /// records (`Action::UploadAwaited`): once when the sequencer answered
    /// `OwnChunks::Upload`, and once per `own_record_wait_ms` the op then
    /// still waited, or waited past a `Streamed` answer.
    pub own_record_uploads: u64,
    /// Chunk metered-own-rows: forward replies whose position this node
    /// observes less rows of its own whose effects it carries
    /// (`Core::settle_own_rows`): no wait, and no upload, for them.
    pub own_rows_excused: u64,
    /// Plan 43 C3: this node's own delegate rows a takeover gate re-appended
    /// after the predecessor's tail, verbatim under their stream origin
    /// (`Core::reappend_own_delegate_rows`).
    pub own_deleg_rows_reappended: u64,
    /// Plan 43 C3: a re-appended row's queued replay that could not be
    /// forgotten (it is then dropped by the replay, its rid completed).
    pub own_deleg_reappend_forget_failed: u64,
    /// Ops held (per check) because their `deps` named a transaction its
    /// generation ended without, while this node's replays settled; and
    /// ops re-sent with fresh `deps` after that
    /// (`Core::hold_for_lost_deps`). Executions refused for it at a
    /// delegate or the root (`Held`, or left in the inbox).
    pub deps_lost_holds: u64,
    pub deps_refreshed: u64,
    pub deps_lost_refused: u64,
    /// Drain ticks that stopped at the head of the replay queue because
    /// its rid is in flight as a client op (a fast-path row stranded by a
    /// deposition and resubmitted through the core): a handful per
    /// episode, never running on for the client deadline.
    pub replays_waiting_on_client: u64,
    /// Delegate transactions a root did not append because their `deps`
    /// named a newer root tenure (it was deposed without knowing yet).
    pub deleg_append_newer_tenure: u64,
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
    /// M13: batches a requester withdrew itself (overwrote with a
    /// tombstone) before forwarding the op over P2P instead (the holder
    /// became reachable again) or holding it back.
    pub inbox_withdrawn_ops: u64,
    /// Holder: withdrawn batches (tombstones) its polls read and stepped
    /// past.
    pub inbox_tombstones_read: u64,
    /// EC2 follow-up: ops waiting in the inbox forwarded over P2P once
    /// the holder became reachable (it polls only unconnected requesters).
    pub inbox_rerouted_to_p2p: u64,
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
    /// Late-landing S1: inbox reads a drain discarded as older than
    /// `Deadlines::inbox_read_fresh` (nothing executed from them), and the
    /// re-reads that followed from a gate drain or a leftover re-read.
    pub inbox_stale_reads_discarded: u64,
    pub inbox_stale_relists: u64,
    /// Late-landing S2b: forwarded attempts parked unexecuted (at the root or
    /// a delegate) that were dropped at their wait's end because their latest
    /// message was older than `Deadlines::inbox_read_fresh`.
    pub stale_parks_dropped: u64,
    /// Late-landing S2a: settles sent for a stranded replay entry that is
    /// foreign or unacknowledged (never executed).
    pub unacked_replay_settles_sent: u64,
    /// Late-landing F1: settles that began with the withdrawal of the rid's
    /// inbox batches, and those whose tombstone could not be written within
    /// the first window (the settle then goes on without the inbox route).
    pub settles_withdrawn_first: u64,
    /// Of those, the settles that withdrew more than one batch of the rid
    /// (the forward's counterpart is `inbox_multi_batch_withdrawals`).
    pub settle_multi_batch_withdrawals: u64,
    pub settle_withdraw_failed: u64,
    /// Late-landing S2a: a foreign queue entry's settle held back while its
    /// requester's own attempts may still run.
    pub foreign_settles_deferred: u64,
    /// U1(a): tombstones the settle gave up on, written later in the
    /// background, and those dropped after the bounded tries.
    pub bg_tombstones_written: u64,
    pub bg_tombstones_dropped: u64,
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
    /// EC2 finding 2: rounds in which this holder kept the lease its
    /// wanters asked for because they are across a P2P partition from
    /// the nodes using it.
    pub leases_kept_for_p2p_side: u64,
    // ---- M8: cto=strict ----
    /// Holder: ReadIndex requests answered with a position, and refused
    /// (not the holder, or fenced).
    pub read_index_served: u64,
    pub read_index_refused: u64,
    /// Holder: read delegations granted.
    pub read_grants: u64,
    /// Holder: of those, the read-ahead grants on children of a directory.
    pub read_ahead_grants: u64,
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
    /// Holder: of those, answered at once because the acknowledgement
    /// waits for chunks only the requester can upload
    /// (`Core::held_for_upload`, `OwnChunks::Upload`).
    pub held_for_upload: u64,
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
    /// ADR-47: forwards a delegate answered `Fenced` (not routed by the
    /// generation it holds the keys under; nothing parked or journaled).
    pub deleg_fenced_forwards: u64,
    pub deleg_forwarded: u64,
    pub deleg_deps_waits: u64,
    pub deleg_parked_expired: u64,
    /// Delegate: grants given up unrenewed past the root's reclaim
    /// horizon (the root is dead or cut off).
    pub deleg_lapsed: u64,
    /// Acquisitions started because ops waited for a root whose lease
    /// ran out (`dead_root_check`).
    pub dead_root_acquires: u64,
    pub deleg_not_owner: u64,
    /// Plan 46 M0.3 (S3): a delegate's own op forwarded to the root after its
    /// grant had time to reach it, which the root took as the generation
    /// disowned and recalled (parking the op behind the recall) instead of
    /// answering `Held`.
    pub deleg_disowned_recalls: u64,
    /// Delegate: delegations installed here, transactions streamed,
    /// batches refused, renewals granted/refused, recalls received.
    pub deleg_installed: u64,
    /// Delegate: generations this node held before a reboot, not taken up
    /// again (plan 43 C2e follow-up: the journal may lack rows it
    /// acknowledged).
    pub deleg_rebooted_holder: u64,
    /// Delegate: generations installed while the delegate read grants the
    /// previous process made may still be honoured, whose writes and read
    /// grants wait until they have expired (`Core::deleg_restart_quarantined`).
    pub deleg_restart_quarantines: u64,
    /// Delegate: own writes parked by that quarantine.
    pub deleg_restart_q_parks: u64,
    /// Delegate: forwarded writes answered `Held` by that quarantine.
    pub deleg_restart_q_held: u64,
    /// Delegate: quarantines that ran to their horizon.
    pub deleg_restart_q_waited_out: u64,
    /// ... ended before the horizon because every node dropped the read
    /// delegations of the previous process (`DelegReadVoid`, scope `Restart`).
    pub deleg_restart_q_voided: u64,
    /// Delegate: read grants not made because of that quarantine (the
    /// position is answered).
    pub deleg_read_grants_quarantined: u64,
    /// Delegate: installs of a generation put off because an ended
    /// generation's rows were still in the journal, its end not yet
    /// applied from the log (path B).
    pub deleg_install_waits_end: u64,
    /// Delegate: installs put off because the grant's `DelegateNode` record
    /// is not committed yet (plan 46 M1.2 step 8b).
    pub deleg_install_waits_commit: u64,
    /// Root: grants that grew a live stream instead of opening one (plan 46
    /// M1.2 step 9). Delegate: grown subtrees learned from the table, and
    /// those it came to honour (after the commit and a renewal's answer).
    pub deleg_grows: u64,
    pub deleg_grows_learned: u64,
    pub deleg_grows_honoured: u64,
    /// Root, simulation only (`Config::stream_ahead_uncommit_grant_every`):
    /// batches carrying a `DelegateNode` streamed ahead, and those sent
    /// as uncommitted by the knob.
    pub stream_grant_batches: u64,
    pub stream_grants_uncommitted: u64,
    /// ... and how long those installs waited in all and at most.
    pub deleg_install_wait_ms: u64,
    pub deleg_install_wait_max_ms: u64,
    /// Designee: journal syncs before a stream batch (its rows leave only
    /// once they are on its disk).
    pub deleg_designee_syncs: u64,
    /// Designee: those syncs that failed (the stream backs off and asks
    /// again).
    pub deleg_designee_sync_failures: u64,
    /// Delegate: generations stopped before a row whose dependency was
    /// lost with another generation (`DelegateState::lost_from`).
    pub deleg_lost_deps_stopped: u64,
    /// Root: delegate rows not appended, a dependency lost with its
    /// generation.
    pub deleg_append_deps_lost: u64,
    /// Plan 46 M1.2 step 4: records of a delegate's transaction the root's
    /// verbatim append journaled but its tolerant apply skipped. The sim
    /// fails a seed that has any.
    pub deleg_append_skipped: u64,
    /// Appended delegate rows with a rid and no `Completed`/`Refused` for
    /// it (must stay 0; the sim fails a seed that has one).
    pub deleg_append_uncompleted: u64,
    /// Root: a sealed tail's drain stopped at a row whose dependency it
    /// does not hold yet (another generation of the dead delegate).
    pub deleg_drain_deps_waits: u64,
    pub deleg_streamed_txs: u64,
    pub deleg_stream_refused: u64,
    /// Delegate: stream acknowledgements that took none of the batch (the
    /// root lacks, or lost, a dependency): the stream backs off.
    pub deleg_stream_no_progress: u64,
    /// Delegate: parked acknowledgements of a delegated row released, and
    /// how long they waited in all (the base the delay compares with).
    pub deleg_stream_acks_released: u64,
    pub deleg_stream_ack_wait_ms: u64,
    pub deleg_renewals: u64,
    pub deleg_renewals_refused: u64,
    /// Root: first renewals of a generation that waited for the root's
    /// read delegations on the subtree to be recalled.
    pub deleg_renew_read_waits: u64,
    /// Renewals the off-core lane sent while the core was stuck in a step
    /// ([`Core::on_deleg_renew_sent_off_core`]).
    pub deleg_renewals_off_core: u64,
    /// Root, plan 41 R2: renewals its P2P service answered off the core
    /// ([`Core::on_deleg_renew_answered_off_core`]); delegate: off-core
    /// answers applied as extensions ([`Core::on_deleg_renewed_off_core`]).
    pub deleg_root_answered_off_core: u64,
    /// Plan 41: own lock grants the renewal lane extended while the core
    /// was busy, and the lane's answers the core applied.
    pub lock_lane_own: u64,
    pub lock_lane_results: u64,
    pub deleg_off_core_answers_applied: u64,
    /// Delegations this node lost (a lapse; a refusal after the grant ran
    /// out) while a freeze of its heartbeat thread covered the time from
    /// the renewal's due time to the loss: nothing in the process could
    /// renew. The length (ms) and generation of the latest.
    pub deleg_lapses_frozen: u64,
    pub deleg_last_freeze_ms: u64,
    pub deleg_last_freeze_gen: u64,
    /// Delegations this node lost (a lapse, or the log ending the generation
    /// after its grant ran out) while a renewal sent in time to have saved
    /// them was unanswered: the root did not answer, its core being stuck.
    pub deleg_lapses_root_silent: u64,
    /// The generation of the latest loss counted in `deleg_lapses_root_silent`.
    pub deleg_last_silent_gen: u64,
    /// Every delegation this node lost (a lapse, a refusal after the grant
    /// ran out, the log ending the generation after it ran out): the
    /// classified ones above are a part of it.
    pub deleg_grants_lost: u64,
    pub deleg_recalls_received: u64,
    /// Delegate (step 10): a release joined one the root had not acknowledged
    /// yet, and one for a part of the stream that could journal no row (it
    /// ended the whole stream at the cut).
    pub deleg_release_merged: u64,
    pub deleg_release_rowless_partial: u64,
    /// Delegate (plan 46 M1.2 step 14): rowless releases by reason: the cut
    /// capped below the delegate's holder appends, a stream never installed
    /// here or restarted without, a lost dependency, a plain stop. The first
    /// two ask the root to seal the group (`ReleaseNote::seal`).
    pub deleg_release_rowless_capped: u64,
    pub deleg_release_rowless_unseen: u64,
    /// Of the never-installed ones, those that asked for the seal (this node
    /// holds rows of the generation, or held it on another boot), and the
    /// releases of a stream this node restarted after a reboot.
    pub deleg_release_rowless_unseen_sealed: u64,
    pub deleg_release_rowless_rebooted: u64,
    pub deleg_release_rowless_lost_dep: u64,
    pub deleg_release_rowless_plain: u64,
    /// Root (plan 46 M1.2 step 14): rowless releases that began a seal of the
    /// group (`ReleaseNote::seal`).
    pub deleg_release_seals: u64,
    /// Root (step 10): resends of a partial release request, and the
    /// requests that became a recall of the stream after them.
    pub deleg_release_partial_resent: u64,
    /// Root: delegations written, transactions appended, batches
    /// refused, batches refused for lacking their `deps` (the causal-cut
    /// assertion, below), cross-subtree ops, recalls sent / drained / expired,
    /// unrenewed grants reclaimed, generations ended.
    pub deleg_delegated: u64,
    pub deleg_appended_txs: u64,
    pub deleg_stream_refusals: u64,
    /// Batches the root refused because its replica lacks their deps:
    /// nothing was appended. A delegate waited for them before executing, so
    /// this is legitimate only at a root that was deposed without knowing it
    /// (the delegate re-streamed to the new root): at the current root it is
    /// a row executed ahead of its cause. Core cannot tell the two; the
    /// simulation judges it against the lease history (`sim/oracle.rs`), a
    /// harness scenario with no deposition asserts it zero.
    pub deleg_deps_refused: u64,
    pub deleg_cross_subtree: u64,
    pub deleg_recalls_sent: u64,
    /// ADR-44: recalls a shippable takeover gate started on demand
    /// (`Core::gate_recall_on_demand`).
    pub deleg_gate_recalls: u64,
    /// ADR-44: generations recalled in the gate that were re-granted.
    pub deleg_gate_redelegated: u64,
    pub deleg_recalls_drained: u64,
    /// Plan 46 M1.2 step 10: batches whose release rows (or release cut) the
    /// root appended.
    pub deleg_releases_appended: u64,
    pub deleg_recalls_expired: u64,
    /// L3: recalls sent again, recalls ended (start to end) with their
    /// total and longest time in ms and those past a second, and the sends
    /// and answers the simulation dropped on purpose.
    pub deleg_recalls_resent: u64,
    pub deleg_recalls_timed: u64,
    pub deleg_recall_ms_sum: u64,
    pub deleg_recall_ms_max: u64,
    pub deleg_recall_over_1s: u64,
    pub deleg_recalls_dropped: u64,
    pub deleg_recalleds_dropped: u64,
    /// Recalls received for a generation not handed back yet.
    pub deleg_recalls_first: u64,
    pub deleg_reclaimed: u64,
    pub deleg_ended: u64,
    /// Root: ended generations forgotten (`Core::deleg_forget_ended`).
    pub deleg_forgotten: u64,
    /// Root: lease releases that waited for a generation's recall in
    /// flight to end it (`Core::release_recall_waits`), and the ones whose
    /// wait reached its bound with a generation not ended
    /// (`Core::on_release_recall_bound`).
    pub deleg_release_waits: u64,
    pub deleg_release_wait_bound: u64,
    /// Root: grants a recall answer handed back while a release waited
    /// for that recall (`Core::lock_install_returned`).
    pub lock_returned_in_release: u64,
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
    /// A designation's refusal of an op this node had sent to the
    /// designee, asked again instead of answered (the op was sent to the designee, it may have run).
    pub designation_refusals_held: u64,
    /// ADR-47: requester: `Fenced` answers resent routed by the answering
    /// generation, and ops routed afresh past the resend limit.
    pub fenced_resends: u64,
    pub fence_bounce_limit: u64,
    /// ADR-47: the root's answers `HolderUnreachable` (row 9), the inbox
    /// drain's halts of a touched op under a non-stealable grant, and a
    /// forward of a rid with an outcome answered from it instead of
    /// redirected (row 4).
    pub root_touched_holds: u64,
    pub inbox_touched_halts: u64,
    /// ADR-47: the drain's halts of an op under a non-stealable grant of its
    /// own requester (that node may run the op locally at any time).
    pub inbox_own_grant_halts: u64,
    /// ADR-47: forwards of an inbox-ever rid answered `Fenced` by a
    /// designee, and `Fenced` answers a requester routed through the lease
    /// holder instead of resending to the designee.
    pub inbox_ever_fenced: u64,
    /// Fence C9.2: inbox-ever forwards a non-stealable holder refused
    /// `EROFS` in its own stream.
    pub holder_inbox_refusals: u64,
    /// Fence C9.3: touched inbox-ever rids the root sent to a live
    /// non-stealable holder.
    pub inbox_ever_redirects: u64,
    pub inbox_local_withdrawals: u64,
    pub inbox_ever_rerouted: u64,
    /// ADR-47: the requester's inbox liveness hold (`inbox_ok`), by what its
    /// keys resolved to on its table (`root`: lags a routed generation;
    /// `nonplaced`: a stealable grant; `cross`: a set with a stealable
    /// grant) and how the op touched a generation (`unknown`: send history
    /// lost; `sent`: sent under it; `local`: offered to this node's own).
    pub inbox_hold_root_unknown: u64,
    pub inbox_hold_root_sent: u64,
    pub inbox_hold_root_local: u64,
    pub inbox_hold_nonplaced_unknown: u64,
    pub inbox_hold_nonplaced_sent: u64,
    pub inbox_hold_nonplaced_local: u64,
    pub inbox_hold_cross_unknown: u64,
    pub inbox_hold_cross_sent: u64,
    pub inbox_hold_cross_local: u64,
    pub redirect_answered_from_outcome: u64,
    /// ADR-47: submissions whose send history is unknown (a replay, a
    /// fast-path in-doubt resubmission, an evicted or older-incarnation rid).
    pub touched_unknown_submits: u64,
    pub deleg_designated: u64,
    /// Plan 46 M1.3 step 4: sticky grants made (a stream's first or a growth).
    pub deleg_sticky: u64,
    /// Sticky streams the root ended: by the holder's own release (no
    /// recall of the root's; its release row reached the root), by the
    /// operator's `undelegate` (`_operator`: the recall asks the holder to
    /// release), after any other recall of the root's (`_recalled`: a
    /// continuation epoch's, the one exception of plan 46 §5.4, or a
    /// violation), by an operator's abandon, or by anything else (a TTL
    /// end, which the oracle `sticky_ends_only_by_holder_or_operator`
    /// fails).
    pub deleg_sticky_ended_released: u64,
    pub deleg_sticky_ended_operator: u64,
    pub deleg_sticky_ended_recalled: u64,
    /// Recalls of a sticky stream the root started: by a continuation epoch
    /// (the exception) and by any other cause (a violation of "the root
    /// never takes back": counted whole, or as a request for some of its
    /// subtrees).
    pub deleg_sticky_recalled_epoch: u64,
    pub deleg_sticky_recalled_other: u64,
    /// Plan 46 M1.3 step 5, the holder's release (`Control::StickyRelease`):
    /// releases journaled (`_partial`: of some of the stream's subtrees
    /// only; the stream lives on), releases journaled before the drain
    /// (`_undrained`: the knob `holder_releases_before_drain`; any is a
    /// violation), ops held at the holder while a release drains
    /// (`_held`), and releases given up (`_given_up`: out of time).
    pub deleg_sticky_releases: u64,
    pub deleg_sticky_releases_partial: u64,
    pub deleg_sticky_release_undrained: u64,
    pub deleg_sticky_release_held: u64,
    pub deleg_sticky_releases_given_up: u64,
    pub deleg_sticky_ended_abandoned: u64,
    /// Plan 46 M1.3 step 13: `repair abandon` of non-stealable streams:
    /// commands accepted, those that waited for read delegations, streams
    /// whose readers all answered the void, streams that waited the
    /// delegations out (the deadline), streams their holder's release ended
    /// meanwhile.
    pub deleg_abandons: u64,
    pub deleg_abandon_read_waits: u64,
    pub deleg_abandon_reads_recalled: u64,
    pub deleg_abandon_reads_expired: u64,
    pub deleg_abandon_superseded: u64,
    pub deleg_sticky_ended_other: u64,
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
    /// Plan 46 M0.2 (C2g′), the replay queue's placeholders
    /// (`StrandedOp::await_retry`: another node's row `repair drop-held`
    /// rolled back, awaiting its requester's retry): entries held behind
    /// one by the drain (R2) and ops parked behind an entry of the queue
    /// ahead of them (R3); placeholders resolved because their rid has an
    /// outcome (the retry executed, or it was settled) and settled by the
    /// bound (`drop_held_await_ms`, R5: `status.ack.await_retry_settled`).
    pub replays_held_for_placeholder: u64,
    pub ops_parked_behind_queue: u64,
    pub await_retry_resolved: u64,
    pub await_retry_settled: u64,
    /// Inbox batches halted at an op behind the replay queue (the ops
    /// behind it in the batch wait too: the batch is executed in order).
    pub inbox_halted: u64,
    /// Settles of placeholders started (one per attempt: not while one is in
    /// flight).
    pub await_retry_settles_sent: u64,
    /// Defect C: settles of refused entries' rids started before their
    /// conflict copy (`Core::copy_after_settle`), and entries the log
    /// showed completed (the write survived: forgotten, no copy).
    pub refused_entry_settles_sent: u64,
    /// An acquisition asked while this node held its own unreleased lease
    /// became a renewal at the next round (`Core::begin_acquire`).
    pub acquire_renewed_instead: u64,
    /// Plan 46 M1.3 step 3 (bucket C): acquisitions whose claim CAS lost to
    /// another write of the lease object, whose waiting ops retry at the
    /// shortest backoff.
    pub acquire_claims_lost_retried: u64,
    /// Plan 46 M1.3 step 3 (bucket A): settles answered by the takeover gate
    /// after the lease was won while the op waited (`Core::settled_by_gate`).
    pub gate_settles_after_win: u64,
    /// Plan 46 M1.3 step 3 (bucket O): settles of a silent holder whose lease
    /// this node may claim by TTL that took the lease path, not the route.
    pub settle_claims_not_routes: u64,
    pub refused_entries_survived: u64,
    /// A refusal answered a refused entry's settle before the log here showed
    /// the rid's row (the copy waits for it).
    pub refused_entry_answers_awaiting_log: u64,
    /// Review (R7): shadows of non-stealable answers rolled back after the
    /// bound because the owner's row never reached the log.
    pub stale_non_stealable_shadows: u64,
    /// Phase 2b: stream batches, backup appends re-sent after a lost
    /// answer.
    pub deleg_stream_timeouts: u64,
    /// Delegate: stream acknowledgements that counted though they did not
    /// answer the batch in flight (it had been given up on and re-sent,
    /// or a later one was in flight), and ones dropped as another root's
    /// or round's.
    pub deleg_stream_acks_unmatched: u64,
    pub deleg_stream_acks_stale: u64,
    /// Root: acknowledgements sent unsolicited because a delegate's committed
    /// prefix rose after its batch was answered (plan 43 Q16, ADR-41).
    pub deleg_stream_acks_pushed: u64,
    /// Delegate: acknowledgements that raised the root's committed prefix.
    pub deleg_root_committed_raised: u64,
    /// Delegate: parked acknowledgements (of a stream's rows) that waited
    /// for the longest time, ones that waited a second or more, and ones
    /// released by the log while their group's commit was not heard (not by
    /// the commit): what a lost push leaves (ADR-41).
    pub deleg_stream_ack_wait_max_ms: u64,
    /// The waits of parked acknowledgements by `ceil(log2(ms + 1))` (the last
    /// bucket takes the rest): the sweeps' p50 and p90.
    pub deleg_stream_ack_wait_hist: [u64; 16],
    pub deleg_stream_acks_slow: u64,
    pub deleg_stream_acks_late: u64,
    pub deleg_stream_acks_by_log: u64,
    /// Delegate: empty batches sent to ask the root for its commit while an
    /// answer waited on it (`deleg_poke_root`, ADR-41).
    pub deleg_root_pokes: u64,
    /// Of them, the fallback asks of an archive-bound stream (plan 46 M1.2).
    pub deleg_root_pokes_archive_bound: u64,
    /// Delegate: of those asks, the answers that raised the root's committed
    /// prefix (the push they stood in for had not).
    pub deleg_root_asks_raised: u64,
    /// Root: pushes dropped on purpose (`Config::delegation_push_drop_every`).
    pub deleg_stream_acks_push_dropped: u64,
    /// Delegate: renewal answers dropped as another root's or round's, or
    /// for a generation no longer this node's.
    pub deleg_renewed_stale: u64,
    /// Root: stream batches that kept a generation's grant from being
    /// outwaited (a live delegate streaming, its renewals behind).
    pub deleg_live_by_stream: u64,
    /// Root: table syncs run because the lease became usable after the
    /// last sync (`Core::deleg_root_sync_due`).
    pub deleg_root_resyncs: u64,
    pub deleg_backup_appends: u64,
    pub deleg_backup_acks: u64,
    pub deleg_acks_parked: u64,
    /// Plan 43 C2 (D7): parked delegate acknowledgements aborted because
    /// their row left the journal without the log carrying it.
    pub deleg_acks_dropped: u64,
    /// Plan 43 C2: `DelegateNodeGroup` records this root journaled for a delegate's
    /// request; delegate rows (gated, uncommitted) a recall of their
    /// generation dropped instead of replaying (`speculation.deleg_dropped`).
    pub deleg_group_records: u64,
    pub deleg_dropped: u64,
    /// Plan 43 C2 (Q1): seals asked again because the recorded backup had
    /// not answered (the generation is not ended without its tail).
    pub deleg_seal_waits: u64,
    /// Plan 46 M1.2 step 7 (c3-spec D6): seals asked of a generation's
    /// previous root, rows its answers carried past the cursor, ends that
    /// waited for one, and renewals of the delegate that settled the set.
    pub deleg_prev_root_seals: u64,
    pub deleg_prev_root_rows: u64,
    pub deleg_prev_root_waits: u64,
    pub deleg_prev_root_renewals: u64,
    /// ... and recalls whose drained answer settled the set.
    pub deleg_prev_root_recalls: u64,
    /// ... and persists of the previous-roots set that failed (the set is then
    /// only in memory).
    pub deleg_prev_roots_persist_failed: u64,
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
    /// Placement: candidates skipped because the node holds designations
    /// only, or one is pending (a stream never mixes grant kinds).
    pub place_skipped_designated: u64,
    /// Plan 46 M1.2 step 13: candidates (and operator grants) refused
    /// because the node has no group within the backup RTT budget: a far
    /// writer is forwarded to the root.
    pub place_skipped_far: u64,
    /// Delegate: ReadIndex answered for its subtree, read grants given.
    pub deleg_read_index_served: u64,
    pub deleg_read_grants: u64,
    /// Plan 30 §M14: locks. Owner side.
    pub lock_grants: u64,
    pub lock_would_block: u64,
    pub lock_grace_refusals: u64,
    /// Plan 43 F-b: non-blocking requests told to wait for a grace, and
    /// for a tenure floor, a barrier or a delegate's authority; grace
    /// ends that served parked waiters (`Timer::LockGraceEnd`).
    pub lock_grace_waits: u64,
    pub lock_not_yet_waits: u64,
    /// Non-blocking requests told to wait by an owner that may not grant
    /// at all yet (no fresh S3 liveness, a lease not marked as granting),
    /// which answered `Busy` before (Q18c).
    pub lock_stale_owner_waits: u64,
    /// Non-blocking requests told to wait because their owner could not be
    /// reached (its link dead, the lease read again still naming it): a
    /// failover in progress (they failed `ENOLCK` before).
    pub lock_unreachable_waits: u64,
    /// Plan 43 C7-F F-b N3: lease reads made for lock requests that need
    /// to know who holds the root, and the requests that shared one in
    /// flight or took the result of one answered a moment before (one
    /// read per request before).
    pub lock_holder_reads: u64,
    pub lock_holder_read_shared: u64,
    pub lock_holder_read_cached: u64,
    /// Plan 43 C7-F follow-up (H1): grants of a delegate's recall answer
    /// that an earlier copy of the answer had handed back already (dropped).
    pub lock_returned_duplicates: u64,
    /// Answers that grant and extend nothing and went ahead of a held
    /// answer to the same peer (they used to queue behind it), and answers
    /// that waited for the order of the answers to their peer alone.
    pub lock_order_bypassed: u64,
    pub lock_order_holds: u64,
    pub lock_grace_end_serves: u64,
    /// Owner: waiters whose turn came with a request too old for a pushed
    /// grant, asked to ask again (`lock_asked_again`); those that did not
    /// within the hold and were passed over until their next request;
    /// requests refused (parked, or `EAGAIN`) for a turn held for an asked
    /// waiter. Node: asks answered by a re-sent request.
    pub lock_asked_again: u64,
    pub lock_ask_again_passed_over: u64,
    pub lock_reserved_waits: u64,
    pub lock_asks_answered: u64,
    /// Owner: blocking requests parked behind an older waiter of another
    /// node whose mode conflicts with theirs, though no grant was in the
    /// way (`Core::lock_serve`'s first come, first served).
    pub lock_queued_behind: u64,
    /// Owner: such requests served past the bound with older waiters still
    /// parked (`Core::lock_queue_bound_ms`).
    pub lock_queue_bound_served: u64,
    /// Node: ops told to wait whose waiter a grant to another op of the
    /// node had served, sent again at once (`Core::lock_op_outcome`).
    pub lock_sibling_resends: u64,
    /// Node: `NotOwner` answers naming back the node that had sent the op
    /// (two non-holders naming each other): the next attempt read the lease.
    pub lock_not_owner_bounces: u64,
    /// Node: upgrade requests sent under a recalled grant that local locks
    /// pin (`lock_route_op`); owner: recalled grants upgraded in place
    /// (`lock_try_grant`).
    pub lock_recalled_upgrade_requests: u64,
    pub lock_recalled_upgrades: u64,
    /// Node: ops woken by a grant installed for another op of the node
    /// (`lock_wake_covered`), and `LockCancel`s sent for the waiters they
    /// and ghost waiters left; owner: waiters dropped by one.
    pub lock_covered_woken: u64,
    pub lock_cancels_sent: u64,
    pub lock_cancelled: u64,
    pub lock_grace_periods: u64,
    pub lock_recalls_sent: u64,
    pub lock_recalls_released: u64,
    pub lock_recalls_expired: u64,
    /// Owner: waiters that re-parked at their old queue position after
    /// a grant to them went unused (`LockState::served`); releases that
    /// named an id this owner had replaced; node: pushes accepted from a
    /// node other than the one the op last asked, and replies to a
    /// request a push had answered, installed over the held id; and
    /// recalls of a grant this node neither held nor could still
    /// install, answered with its release.
    pub lock_requeued_in_place: u64,
    pub lock_released_superseded: u64,
    pub lock_pushes_from_other_owner: u64,
    pub lock_late_replies_installed: u64,
    pub lock_recalls_unheld_released: u64,
    pub lock_waiters_parked: u64,
    pub lock_waiting_replies: u64,
    /// Plan 30 §M14: remote waiters dropped after `4 × ttl` of silence
    /// (killed while parked; a live one re-sends every `ttl/2`).
    pub lock_waiters_dropped: u64,
    /// Owner: peers taken for unreachable (a recall undeliverable, or
    /// grants pushed to them unused twice in a row; `LockState::
    /// unreachable`), and waiters passed over meanwhile because only a
    /// request of their own can carry their grant.
    pub lock_peers_unreachable: u64,
    pub lock_unreachable_passed_over: u64,
    /// Owner: waiters a node's previous incarnation left queued, dropped
    /// when its new incarnation asked; requests from an older
    /// incarnation than one already seen, ignored.
    pub lock_incarnation_waiters_dropped: u64,
    pub lock_stale_incarnation_requests: u64,
    pub lock_wait_ms_total: u64,
    pub lock_renewals_served: u64,
    /// Owner side: grant/renewal answers held for the restart horizon's
    /// durable write (`Action::PersistLockHorizon`), the writes issued,
    /// and the ones that failed (their answers refused).
    pub lock_horizon_held: u64,
    pub lock_horizon_writes: u64,
    pub lock_horizon_failed: u64,
    /// The read-grant horizons' write-ahead lane (`Action::PersistGrantHorizon`):
    /// the writes issued, the ones that failed, and the reads answered
    /// with no grant because no durable horizon covered it yet.
    pub grant_horizon_writes: u64,
    pub grant_horizon_failed: u64,
    pub read_grants_uncovered: u64,
    /// Plan 41 L0: renewal answers (lock grants, own and peers', and
    /// delegations) held because the tenure's S3 liveness was stale or
    /// it was not marked as granting; the lease reads issued for them;
    /// the held answers refused (the tenure was gone, or not proven).
    pub renew_held: u64,
    pub renew_probes: u64,
    pub renew_refused: u64,
    /// Plan 41 D3: the lease reads the off-core lanes made while this core
    /// was stuck (`Event::RootProbed`) and what they found: this tenure live
    /// (its send is now the core's own proof too), deposed (the core finds
    /// out here, as after its own probe's), or nothing either way.
    pub root_probes: u64,
    pub root_probes_live: u64,
    pub root_probes_deposed: u64,
    pub lock_reclaimed: u64,
    pub lock_moved: u64,
    /// Of those, the grants that carried what was left of their record
    /// instead of a restamp (every moved grant does).
    pub lock_moved_carried: u64,
    /// Root: a delegate's grants on inodes that left its subtree
    /// (unlinked under their lock), installed with the batch that carried
    /// them (`Core::lock_install_leaving`).
    pub lock_leaving_installed: u64,
    /// Grants handed to a delegation that ended without handing them
    /// back, reinstated in the root's table.
    pub lock_reinstated: u64,
    /// Root: grants in a recall answer that came after the generation had
    /// ended here (outwaited, sealed) or with no generation known here (the
    /// lease lost since), dropped (`Core::lock_install_returned`).
    pub lock_returned_after_end: u64,
    /// Delegate: generations that started inside the root's lock grace
    /// (carried with their first granting renewal).
    pub lock_graces_inherited: u64,
    /// Subtree/namespace lock floors noted (moves, recalls, outwaits,
    /// takeovers, mirrors).
    pub lock_dir_floors: u64,
    /// Root: grants held back while a new tenure waited for its
    /// inherited delegates' stream heads.
    pub lock_tenure_waits: u64,
    /// Owner side: outwait barriers set (a grant record expired
    /// unreleased; see `LockState::barriers`).
    pub lock_outwait_barriers: u64,
    /// Owner side: grants held back until a cut settled an outwait
    /// barrier on their inode.
    pub lock_barrier_waits: u64,
    /// Root: cuts that named more live streams than a floor holds
    /// (`STREAMS_CAP`): the oldest were left out (`Core::lock_cut_here`).
    pub lock_cut_truncated: u64,
    /// Delegate: requests held back because too little of the
    /// delegation was left to grant a lock worth holding (the
    /// delegation's renewal is asked for at once).
    pub lock_short_authority_waits: u64,
    /// Root: lease renewals brought forward because a grant it answered
    /// was capped by the lease below what its holder needs
    /// (`Core::lease_renew_for_grants`).
    pub lease_renewals_for_grants: u64,
    /// Holder: due lease renewals that waited for its own other lease CAS
    /// (a reconfiguration, a mark's clear) to land rather than race it
    /// (`Core::renew_waits_for_own_cas`).
    pub lease_renewals_after_own_cas: u64,
    /// Holder: lease renewals that fell due while a job waited for the
    /// takeover gate's inbox drain, sent with the drain still in flight
    /// (`Core::on_gate_drain_renew`).
    pub gate_drain_renewals: u64,
    /// Node side.
    pub lock_requests: u64,
    pub lock_unavailable: u64,
    pub lock_grant_ms_total: u64,
    pub lock_grant_ms: [u64; 14],
    pub lock_granted_recalled: u64,
    pub lock_renewals: u64,
    pub lock_lost: u64,
    pub lock_released: u64,
    /// F1: recalled grants left to their owner's outwait instead of
    /// released (a mutation tagged with them ended in doubt).
    pub lock_doubt_lapses: u64,
    // ---- plan 43 C7-F F-c ----
    /// Owner: late renewals admitted (a settled record, or its lapsed
    /// memory) and refused (`Lost`: not proven).
    pub lock_late_admitted: u64,
    pub lock_late_refused: u64,
    /// Old root: final mirrors begun (one per lease). Successor: fast
    /// takeovers whose mirror was the final one of the lease replaced
    /// (installed settled), and ones whose final mirror named another.
    pub lock_mirror_final_sent: u64,
    pub lock_mirror_final_installed: u64,
    pub lock_mirror_final_mismatch: u64,
    /// Holder: lapsing grants that gave up at the park's bound.
    pub lock_lapse_parks_gave_up: u64,
    pub lock_idle_released: u64,
    pub lock_released_replies: u64,
    // ---- M9: backups, seals, `ack=s3` ----
    /// Holder: backups added to / removed from the lease, and the lease
    /// CASes spent on it (a reconfiguration is one CAS when it lands).
    pub backups_added: u64,
    pub backups_removed: u64,
    pub reconfig_cas: u64,
    /// Plan 46 M1.1 step 5: configurations closed by a landed change of the
    /// backup set or the singleton flag ([`Core::sync_cfg_spans`]).
    pub cfg_spans_closed: u64,
    /// Singleton bumps issued (`reconfig_cas` counts them too).
    pub singleton_bumps: u64,
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
    /// Stream-ahead transactions this replica held already (a restarted
    /// subscriber, or the holder streaming from its start again).
    pub streamed_held_already: u64,
    /// ADR-51: speculative streamed copies an epoch member's ack promoted
    /// to committed (`Replica::promote_streamed`).
    pub streamed_promoted: u64,
    /// Backup: appends persisted, epochs sealed, takeovers completed by
    /// seal, and how many transactions a takeover re-applied.
    pub backup_persisted: u64,
    pub seals: u64,
    /// Plan 37 §8: holds a holder replaced on its own state dir asked of
    /// this backup's seal watch (`PeerMsg::BackupHold`).
    pub backup_holds: u64,
    /// Backup roles ended by the holder's dismissal
    /// (`Event::HolderAlive { listed: false }`).
    pub backup_dismissals: u64,
    pub backup_takeovers: u64,
    pub backup_tail_applied: u64,
    /// Any peer: fast takeovers of an `ack=s3` lease.
    pub s3_fast_takeovers: u64,
    /// Successor: takeovers that had to wait out the predecessor's
    /// delegation horizon before acknowledging mutations.
    pub ack_floor_waits: u64,
    /// Plan 43 C7-F F-a0 (b): inherited marks cleared (CAS issued).
    pub mark_clears: u64,
    /// Plan 43 C7-F F-a: floors the predecessor's beats shortened
    /// (`ack_floor_capped`, by `ack_floor_cap_saved_ms` in all against the
    /// bound), and takeovers whose cap had passed by the time the marker
    /// landed, which armed none (`ack_floor_capped_out`; not counted in
    /// `ack_floor_waits`).
    pub ack_floor_capped: u64,
    pub ack_floor_capped_out: u64,
    pub ack_floor_cap_saved_ms: u64,
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
    /// Plan 43 C2 (§5.3 item 5), as a sequencer: settles of a rid with no
    /// outcome here, refused for good (`Refused { rid, ABANDONED_CODE }`),
    /// and unexecuted parks of a rid's earlier attempt a settle dropped.
    pub settles_refused: u64,
    pub settle_dropped_parks: u64,
    /// Plan 46 M1.2 step 13: second deadlines of this node's own op, parked
    /// as the root for a generation's recall, held until the root's outwait
    /// of that generation ends (`client_deadline`).
    pub settles_held_for_outwait: u64,
    /// Plan 46 M1.2 step 13: second deadlines moved later because the
    /// sequencer said it holds the settle for a generation's outwait
    /// (`Held::outwait_ms`).
    pub settles_extended_for_outwait: u64,
    /// ... because a generation of this node's lapsed during the settle and
    /// its row lands when the root ends it.
    pub settles_extended_for_lapsed_gen: u64,
    /// ... because the root answered the settle and only its row's
    /// durability was left (`ClientOp::durable_grace`).
    pub settles_extended_for_durability: u64,
    /// Settles the root did not redirect to a generation it inherited and
    /// has not heard its delegate on (`on_forward`): they park behind the
    /// recall.
    pub settles_not_redirected_unheard: u64,
    /// Plan 46 M0.3 (S1): settles a takeover gate answered on the keys it
    /// owns (a refusal journaled, or a dedup'd outcome), settles it left
    /// `Held` because its journal work was not done yet, and settles of a
    /// rid that took effect, left `Held` (never `Accepted` before the
    /// quarantine).
    pub gate_settles_answered: u64,
    pub gate_settles_held: u64,
    pub gate_settles_held_executed: u64,
    /// Plan 46 M0.3 (S5, ADR-42): a takeover gate whose journal work is
    /// done: generations of other nodes it took on at that point
    /// (`deleg_gated_inherited`, also counted in `deleg_inherited`),
    /// renewals it answered with a grant (`deleg_renewals_gated`),
    /// transactions it appended from their delegates' batches
    /// (`deleg_appended_gated`), and settles on their keys it redirected to
    /// the live delegate (`gate_settles_redirected`).
    pub deleg_gated_inherited: u64,
    pub deleg_renewals_gated: u64,
    pub deleg_appended_gated: u64,
    pub gate_settles_redirected: u64,
    /// Plan 43 C2 (§5.3 item 5), as a requester: ops whose own attempts
    /// reached their deadline and that were settled — settles sent (one per
    /// op, however many attempts it takes), answered `Abandoned`, answered
    /// with the op's own late outcome, settled here because the op never
    /// left this node (`Abandoned` at once), and those no sequencer answered
    /// within the settle window (the unsettled `EIO`, I4's one exception).
    pub settles_sent: u64,
    pub settled_abandoned: u64,
    pub settled_late_success: u64,
    pub settled_local: u64,
    pub client_unsettled_eio: u64,
    /// Of those, the ones whose op a non-stealable holder's refusal was held
    /// back for (I4's exception for an unreachable non-stealable holder: the
    /// holder may have run the op, and its row lands late).
    pub unsettled_nonstealable_holder: u64,
    /// Plan 30 §M14 phase 2: ops answered `LockLapsed` — refused here
    /// (as executor) or by the executor that answered this node's
    /// forward, because a cluster-lock grant they were issued under had
    /// ended. Nothing was journaled.
    pub lock_lapsed_refusals: u64,
    pub unacked_replays_refused: u64,
    /// Plan 43 C2a: definitive answers this node gave as a sequencer, by
    /// the rule they waited for (`ACK_COUNTERS`: group, final, singleton,
    /// archive_waits) — a `MutateReply` to a requester,
    /// or an outcome to this node's own client that was not another node's
    /// answer passed on.
    pub ack_classes: [u64; 4],
    /// Plan 43 C1-2 (D4): gated answers released with the gate off, whose
    /// rows were re-tagged `Lone` (answered as a lone node would).
    pub acks_downgraded_lone: u64,
    /// Plan 43 C2 (§5.3): shadows and hints this node installed for a
    /// definitive answer its sequencer did not say is committed
    /// (`MutateReply.committed` false) by a sequencer that is not
    /// non-stealable: none after C2 (a lone root answers other nodes from
    /// the archive). Warned once (`status.ack.ungated_shadows`).
    pub ungated_shadows: u64,
    /// The same for a non-stealable sequencer's answers (a designee's or a
    /// sticky holder's): uncommitted by design
    /// (`status.ack.ungated_shadows_non_stealable`).
    pub ungated_shadows_non_stealable: u64,
    /// Plan 43 C2f: answers parked for durability whose row was rolled back
    /// while they waited (`repair drop-held`), aborted instead of released
    /// when the log covered the seq they waited for.
    pub acks_row_gone: u64,
    /// I1 emission gate (`Core::hold_unsettled_answer`): answers resting on
    /// a delegation row (`gen != 0`) its group or the log does not hold,
    /// which a path let reach the point where an answer leaves, and which
    /// were parked there instead. Non-zero means a path bypassed
    /// `Core::answer_need_of` (a debug build asserts).
    pub answers_held_at_emission: u64,
    /// `answers_held_at_emission` by the path that hit the gate: a local
    /// client's `finish`, a delegate's own execution's reply, the holder's
    /// executed-or-dedup reply, its designation refusal, and a parked
    /// reply's release.
    pub held_at_emission_finish: u64,
    pub held_at_emission_deleg_exec: u64,
    pub held_at_emission_holder_reply: u64,
    pub held_at_emission_designation: u64,
    pub held_at_emission_release: u64,
    /// Plan 46 M1.4 step 6a (ADR-53): the held answers of
    /// `answers_held_at_emission` that rested on this node's own root row
    /// (`Core::unsettled_root_row`): an earlier tenure's unshipped row whose
    /// answer reached the point it leaves without a path having waited for
    /// the archive. Expected zero where every path decides it first
    /// (`Core::answer_need_of`, `Core::completed_row_settled`).
    pub held_at_emission_root_row: u64,
    /// Plan 43 C1-3: the tail fetch. `SealTail`s answered by this node as a
    /// holder (it sealed its own epoch), as a backup, and refused;
    /// attempts of a claim to fetch, fetches completed (of those, of a
    /// lease with an inherited tail), the time spent waiting for a group,
    /// groups the operator abandoned, member copies dropped, and takeover
    /// claims of a lease whose group may hold acknowledged rows that
    /// carried no tail (must stay 0 except after an abandon).
    pub seal_tail_holder: u64,
    /// Seals of a claimant left unanswered: this node's store could not be
    /// read (plan 43 C2e review: not an empty tail, not "I hold nothing").
    pub seal_tail_unreadable: u64,
    /// ADR-51: `SealTail { kept }` requests this node answered (it sealed the
    /// hold streams of the tenure and reported its kept rows), and the ones
    /// it refused (its own epoch carries the tenure; a newer renewal seen).
    pub seal_tail_kept: u64,
    pub seal_kept_refused_epoch_open: u64,
    pub seal_kept_refused_stale_key: u64,
    /// ADR-51 (K4): `StreamAhead` batches of a stream this node had sealed,
    /// refused; and holds that ended as deposed because every other member
    /// of their epoch had sealed their stream.
    pub stream_ahead_sealed_refused: u64,
    pub hold_ended_sealed: u64,
    /// ADR-51: kept fetches begun and completed; those the operator's abandon
    /// completed with a source unanswered.
    pub kept_fetches: u64,
    pub kept_fetches_completed: u64,
    pub kept_abandoned: u64,
    /// ADR-51: transactions a completed fetch carried (once each), those it
    /// found past a gap no source covers (must stay 0), fetches that did not
    /// complete because two sources kept different transactions at one
    /// seq, ones completed by the operator's `--force`, and kept rows that a
    /// claim's takeover gate still queued as foreign replays (must stay 0
    /// but after an abandon).
    pub kept_rows_carried: u64,
    pub kept_past_gap: u64,
    pub kept_diverged: u64,
    pub kept_forced: u64,
    pub kept_uncarried_replayed: u64,
    /// Reads of the persisted backup seal that failed after a start that
    /// could not read it (`status.ack.sealed_unread` while it lasts).
    pub backup_seal_unread_reads: u64,
    pub seal_tail_backup: u64,
    pub seal_tail_refused: u64,
    /// Seals refused because this holder's renewal had landed with a newer
    /// key than the claimant read (its claim CAS could not land).
    pub seal_tail_stale_key: u64,
    /// A self-sealed holder claimed its own lease at once, because the lease
    /// had moved off every key a sealer read (D7 amended).
    pub sealed_own_early_claims: u64,
    /// A claim CAS that wrote this node's own epoch although the node had
    /// sealed it (never: D7 makes it the takeover at the next epoch).
    pub sealed_own_epoch_rewrites: u64,
    /// Plan 46 M0.3 (G): of those, refusals that proved this node holds
    /// nothing of the epoch (`SealedTail::empty`).
    pub seal_tail_empty: u64,
    /// A listed backup that claimed at the expiry and sealed its role first.
    pub claim_seals: u64,
    pub tail_fetch_attempts: u64,
    pub tail_fetches: u64,
    /// Plan 46 M0.3 (G): of those, claims whose every source proved it
    /// holds nothing of the tail (the claim went on with an empty one).
    pub tail_fetch_proved_empty: u64,
    pub inherited_tail_fetches: u64,
    pub tail_fetch_wait_ms_total: u64,
    pub groups_abandoned: u64,
    /// Of those, the delegation groups abandoned with `--force` although
    /// the root had heard from the delegate or a strict member within
    /// `delegation_ttl_ms` (plan 43 C2e review).
    pub groups_abandoned_forced: u64,
    pub member_copies_retired: u64,
    /// Plan 43 C1-4: a backup's contiguous tail moved to a member copy when
    /// its holder's lease moved on to a later epoch.
    pub tails_kept_as_member_copy: u64,
    /// Plan 43 C1-4: member-copy rows dropped as never committed (past a
    /// marker's `through`, or unacknowledged at a newer marker).
    pub member_copies_dropped: u64,
    /// Plan 43 C1-4: member-copy rows an operator's abandon queued.
    pub member_copies_abandoned: u64,
    /// Plan 43 C1-4: this node's committed unshipped transactions moved to
    /// a member copy (a deposition, a takeover's strand) instead of queued.
    pub rows_member_copied: u64,
    /// Plan 43 C1-4: committed requester/subscriber entries a takeover
    /// gate took out because the tail it re-applies carries their rows.
    pub committed_superseded: u64,
    /// Plan 43 C1-4 (`speculation.committed_rolled_back`): committed
    /// entries rolled back and replayed. Only an operator's abandon may
    /// raise it.
    pub committed_rolled_back: u64,
    /// Plan 43 C1-4: a gated `Local` row put in the replay queue. Must stay
    /// 0 (a debug assertion backs it).
    pub replay_gated_enqueued: u64,
    /// Plan 46 M1.2 step 14: a root's append of a delegate's row put in the
    /// replay queue by a stranding pass (`Stranded::deleg_append_queued`).
    /// Must stay 0: an appended row is dropped or kept as a member copy.
    pub deleg_append_queued: u64,
    /// Plan 43 C1-4: tail transactions past a gap, not re-shipped (never
    /// acknowledged; their clients retry by rid).
    pub tail_beyond_gap_dropped: u64,
    /// Plan 46 M1.2 step 5 (c3-spec D3): a sealed tail that did not apply
    /// (`MetaError::TailDiverged`), one per pass of the closed gate. Must
    /// stay 0; the sim fails a seed that has one.
    pub tail_diverged: u64,
    /// Plan 46 M1.2 step 5: tail transactions whose rid already had an
    /// outcome here (the log carried them), not re-shipped.
    pub tail_done_skipped: u64,
    /// Plan 46 M1.2 step 5: the longest the store's writer lock was held by
    /// one takeover's re-ship (ms; c3-spec `reship.duration_ms_max`).
    pub reship_duration_ms_max: u64,
    /// The sync after a re-ship's commit failed (the re-ship stood).
    pub reship_sync_failed: u64,
    pub inherited_tails_cleared: u64,
    pub ttl_claims_without_tail: u64,
    /// Plan 43 C1-2: journal rows the core wrote without setting their
    /// class (they took the session's published one). Must stay 0.
    pub journal_class_from_session: u64,
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
    /// Continuation epochs that carried the lease the previous epoch's
    /// close let go, claimed while its re-claim was pending: held again
    /// (`epoch_closed_claim`).
    pub epoch_closed_leases_reheld: u64,
    /// Continuation-epoch handoffs declined because the requester had not
    /// applied this holder's whole log (flex-crash seed 30702).
    pub epoch_handoffs_behind: u64,
    /// Journal transactions a continuation epoch's hold owner streamed
    /// ahead to members, and those a member installed (plan 30 §M10 ×
    /// §M9: members keep following the hold owner's log stream).
    pub epoch_streamed_ahead: u64,
    pub epoch_streamed_installed: u64,
    /// A member's accepted forwards whose `base` only the epoch stream
    /// could deliver, answered once it did.
    pub epoch_forwards_streamed: u64,
    /// Plan 46 M1.4 step 1, hold owner: member acknowledgements of the
    /// stream-ahead recorded, their summed latency (batch sent to ack
    /// received, per batch), and those ignored (a node outside the epoch,
    /// another epoch, no hold).
    pub hold_acks: u64,
    pub hold_ack_wait_ms: u64,
    pub hold_acks_ignored: u64,
    /// Plan 46 M1.4 step 2: batches of the hold's stream-ahead sent again
    /// to a member that had not acknowledged them.
    pub hold_resends: u64,
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
    pub last_publish: WaitMs,
    /// When this node last read its own held stream (`held_tail_at`).
    pub held_tail_at: Option<WaitMs>,
    /// A tailed segment carried an epoch above the one this node holds:
    /// renew now, not at half-TTL (`prepare_renew_now`).
    pub renew_now: bool,
    /// The lease (by its expiry) a capped grant asked to renew at the
    /// next round (`Core::lease_renew_for_grants`); nothing clears it, a
    /// renewal makes it stale (it no longer matches the lease's expiry).
    pub renew_early_of: Option<i64>,
    /// The epoch of the last segment this node shipped (a commit carries
    /// its author's epoch).
    pub last_ship_epoch: Epoch,
    /// A due renewal waited for this holder's own other lease CAS
    /// (`Core::renew_waits_for_own_cas`): the next round renews once it
    /// has landed (`Core::renew_after_own_cas`).
    pub renew_after_cas: bool,
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
            last_publish: WaitMs(0),
            held_tail_at: None,
            renew_now: false,
            renew_early_of: None,
            renew_after_cas: false,
            last_ship_epoch: 0,
            last_error: None,
            probe_now: false,
        }
    }
}

/// EC2 campaign 8 A-1: this node's own S3 path as the driver last
/// reported it (`Event::OwnS3`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OwnS3 {
    /// No S3 request of this node has completed for the driver's stall
    /// window.
    pub stalled: bool,
    /// While stalled: whether a live peer answered that it reaches S3
    /// (`Some(true)`: the outage is this node's own), that none does
    /// (`Some(false)`: a bucket outage, the continuation epoch's), or
    /// nobody answered (`None`).
    pub peers_reach_s3: Option<bool>,
    /// When the stall was first reported (wait clock; status shows the
    /// elapsed time).
    pub since: Option<WaitMs>,
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
    /// ADR-51: a kept fetch reading the heartbeats for the slack `f` (no
    /// promise check ran).
    KeptSlack,
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
    /// Late-landing U1(a): a background try of a tombstone a settle gave up on.
    InboxTombstoneBg(InboxKey),
    /// ADR-47: the holder re-reading the takeover drain's leftovers.
    InboxLeftover(NodeId),
    /// The takeover gate's inbox drain of the epochs below this one,
    /// issued beside the job slot (`Core::issue_drain`): the holder renews
    /// its lease while the drain is read.
    GateDrain(Epoch),
    /// M13: a requester deleting its own durable batch before forwarding
    /// the op over P2P instead.
    InboxWithdraw(Rid, constellation_store_s3::inbox::InboxKey),
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
    /// Plan 43 C7-F F-a0 (b): the holder clearing the mark it inherited
    /// once nothing it stands on is live, and the re-read that settles a
    /// clear whose outcome is not known.
    ClearMark,
    ClearMarkReread,
    /// M10: a takeover's heartbeat read, and a promise PUT (the slack it
    /// advertises).
    Heartbeats,
    HeartbeatPut(u32),
    /// M10: the idle promise watch's lease read.
    PromiseWatch,
    /// A non-holder whose ops wait for a root's append reading whether
    /// that root's lease has run out (`Core::dead_root_check`).
    DeadRoot,
    /// Plan 41 L0: a fast tenure's lease read that renewal answers held
    /// for stale liveness wait for (`Core::renew_kick`).
    RenewProbe,
}

/// A pending timer's deadline, on the clock it was set against (plan 43
/// C7-H1). Two deadlines are only compared when they are on the same one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimerAt {
    Honour(HonourMs),
    Wait(WaitMs),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Timer {
    Poll,
    ForwardTimeout(Rid),
    ForwardBackoff(Rid),
    /// A settle's tombstone retry, or the end of its first window
    /// (`Core::on_withdraw_timer`).
    WithdrawRetry(Rid),
    /// `Core::await_own_records`'s safety timer for the op ran out.
    OwnRecordWait(Rid),
    /// `Core::repeat_observed_upload`'s timer.
    ObservedUpload,
    AcquireRetry(Rid),
    ClientDeadline(Rid),
    ReplayDrain,
    /// A job's outstanding peer request (`req`) went unanswered.
    JobRequestTimeout(OpId),
    InboxPoll,
    InboxRecheck,
    InboxTombstoneBg,
    InboxSubmitRetry,
    EscalateTick,
    StreamHeartbeat,
    StreamWatchdog,
    GrantExpiry(u64),
    GrantQuarantine,
    /// The write quarantine of a restarted holder's generation ends
    /// (`Core::on_deleg_restart_quarantine`).
    DelegRestartQuarantine(u64),
    /// Plan 43 C7-H2: `Core::service_pending_flushes`.
    FlushFloor,
    HeldReply(u64),
    ReadIndexTimeout(OpId),
    ReadIndexRetry(OpId),
    ReadIndexDeadline(OpId),
    BackupTick,
    BackupWatch,
    /// Plan 30 §M9 round 2: the pre-S3 stream-ahead hold-off (batches
    /// the rows that became durable meanwhile into one send).
    StreamAhead,
    /// Plan 46 M1.4 step 2: resend the hold's stream-ahead to a member
    /// that has not acknowledged it.
    HoldResend,
    PromiseWait,
    PromiseWatch,
    DelegExpiry(u64),
    /// L3: an unanswered recall is sent again (`Core::on_release_resend`).
    ReleaseResend(u64),
    /// Plan 46 M1.2 step 14: the wait of a released recorded generation for
    /// its group (`Core::deleg_release_seal_round`).
    DelegReleaseSeal(u64),
    DelegRenew(u64),
    DelegLapse(u64),
    /// M14.
    LockRequestTimeout(OpId),
    LockRetry(OpId),
    LockHeldReply(u64),
    LockGrantExpiry(constellation_meta::locks::GrantId),
    LockRenewTick,
    LockWaiterTick,
    /// Plan 43 F-b: the earliest end of a lock grace
    /// (`Core::lock_arm_grace_end`).
    LockGraceEnd,
    /// Plan 43 C7-F F-c: the final lock mirror is due or re-sent
    /// (`Core::lock_final_step`).
    LockFinal,
    LockRenewTimeout(OpId),
    LockTestTimeout(OpId),
    /// M14 phase 2: re-check whether `(ino, grant)`'s release may go
    /// (`on_lock_flushed`).
    LockReleaseWait(
        constellation_fs_core::Ino,
        constellation_meta::locks::GrantId,
    ),
    DelegStream,
    /// Path B: an install that waits for an ended generation's end is
    /// tried again (`Core::on_deleg_install_retry`).
    DelegInstallRetry,
    /// Phase 2b: the placement's evaluation tick.
    Placement,
    /// A release's wait for the generations being recalled reached its
    /// bound (`Core::on_release_recall_bound`).
    ReleaseRecallBound,
    /// Plan 46 M1.2 step 11: the leaves waiting for grants to end look
    /// again (`Core::on_leave_wait_timer`).
    LeaveWait,
    /// Plan 46 M1.3 step 5: the holder's releases in progress look again
    /// (`Core::on_sticky_release_timer`).
    StickyRelease,
    /// Plan 46 M1.3 step 13: an abandon's wait for the read delegations of
    /// the streams it ends reached its deadline (`Core::on_abandon_timer`).
    AbandonWait,
    /// The lease renewal falls due while a job waits for the takeover
    /// gate's inbox drain (`Core::on_gate_drain_renew`).
    GateDrainRenew,
}

impl Timer {
    fn kind(self) -> TimerKind {
        match self {
            Timer::Poll => TimerKind::Poll,
            Timer::ForwardTimeout(_) => TimerKind::ForwardTimeout,
            Timer::ForwardBackoff(_) => TimerKind::ForwardBackoff,
            Timer::WithdrawRetry(_) => TimerKind::WithdrawRetry,
            Timer::OwnRecordWait(_) => TimerKind::OwnRecordWait,
            Timer::ObservedUpload => TimerKind::ObservedUpload,
            Timer::AcquireRetry(_) => TimerKind::AcquireRetry,
            Timer::ClientDeadline(_) => TimerKind::ClientDeadline,
            Timer::ReplayDrain => TimerKind::ReplayDrain,
            Timer::JobRequestTimeout(_) => TimerKind::JobRequestTimeout,
            Timer::InboxPoll => TimerKind::InboxPoll,
            Timer::InboxRecheck => TimerKind::InboxRecheck,
            Timer::InboxTombstoneBg => TimerKind::InboxTombstoneBg,
            Timer::InboxSubmitRetry => TimerKind::InboxSubmitRetry,
            Timer::EscalateTick => TimerKind::EscalateTick,
            Timer::StreamHeartbeat => TimerKind::StreamHeartbeat,
            Timer::StreamWatchdog => TimerKind::StreamWatchdog,
            Timer::GrantExpiry(_) => TimerKind::GrantExpiry,
            Timer::GrantQuarantine => TimerKind::GrantQuarantine,
            Timer::FlushFloor => TimerKind::FlushFloor,
            Timer::HeldReply(_) => TimerKind::HeldReply,
            Timer::ReadIndexTimeout(_) => TimerKind::ReadIndexTimeout,
            Timer::ReadIndexRetry(_) => TimerKind::ReadIndexRetry,
            Timer::ReadIndexDeadline(_) => TimerKind::ReadIndexDeadline,
            Timer::BackupTick => TimerKind::BackupTick,
            Timer::BackupWatch => TimerKind::BackupWatch,
            Timer::StreamAhead => TimerKind::BackupTick,
            Timer::HoldResend => TimerKind::BackupTick,
            Timer::PromiseWait => TimerKind::PromiseWait,
            Timer::PromiseWatch => TimerKind::PromiseWatch,
            Timer::DelegExpiry(_) => TimerKind::DelegExpiry,
            Timer::ReleaseResend(_) => TimerKind::ReleaseResend,
            Timer::DelegReleaseSeal(_) => TimerKind::DelegReleaseSeal,
            Timer::LockRequestTimeout(_) => TimerKind::LockRequestTimeout,
            Timer::LockRetry(_) => TimerKind::LockRetry,
            Timer::LockHeldReply(_) => TimerKind::LockHeldReply,
            Timer::LockGrantExpiry(_) => TimerKind::LockGrantExpiry,
            Timer::LockRenewTick => TimerKind::LockRenewTick,
            Timer::LockReleaseWait(..) => TimerKind::LockReleaseWait,
            Timer::LockWaiterTick => TimerKind::LockWaiterTick,
            Timer::LockGraceEnd => TimerKind::LockGraceEnd,
            Timer::LockFinal => TimerKind::LockFinal,
            Timer::LockRenewTimeout(_) => TimerKind::LockRenewTimeout,
            Timer::LockTestTimeout(_) => TimerKind::LockTestTimeout,
            Timer::DelegRenew(_) => TimerKind::DelegRenew,
            Timer::DelegLapse(_) => TimerKind::DelegLapse,
            Timer::DelegStream | Timer::DelegInstallRetry | Timer::DelegRestartQuarantine(_) => {
                TimerKind::DelegStream
            }
            Timer::Placement => TimerKind::Placement,
            Timer::ReleaseRecallBound
            | Timer::LeaveWait
            | Timer::StickyRelease
            | Timer::AbandonWait => TimerKind::DelegExpiry,
            Timer::GateDrainRenew => TimerKind::Poll,
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
    /// Plan 43 C7-H2: flushes and shutdowns (`Control::Flush`, `Shutdown`)
    /// that wait for the takeover gate and floor to end before they
    /// release the lease; they do not hold the job slot meanwhile (the
    /// gate completes in rounds).
    pending_flushes: Vec<jobs::PendingFlush>,
    flush_floor_timer: Option<TimerId>,
    clients: BTreeMap<Rid, client::ClientOp>,
    /// Rids answered `InDoubt` and not yet resubmitted.
    in_doubt_rids: std::collections::BTreeSet<Rid>,
    /// Rids whose unsettled `EIO` was counted in `unsettled_nonstealable_holder`,
    /// until the driver takes them (`take_unsettled_designation`).
    unsettled_designation_rids: std::collections::BTreeSet<Rid>,
    /// The same for the unsettled `EIO`s of ops a delegate had executed
    /// (`ClientOp::accepted_by_delegate`; `take_unsettled_accepted`).
    unsettled_accepted_rids: std::collections::BTreeSet<Rid>,
    /// And those of ops whose settle gave up on a tombstone
    /// (`ClientOp::settle_withdraw_failed`; `take_withdraw_failed`): the
    /// late-landing U1(a) label `isolated-from-s3`, for the sim oracle.
    withdraw_failed_rids: std::collections::BTreeSet<Rid>,
    /// Whether `unsettled_designation_rids` is filled (a consumer calls
    /// `track_unsettled_designation`; production cores do not, since nothing
    /// would drain it).
    track_unsettled_designation: bool,
    /// ADR-47 audit (the sim driver; `track_fence_audit`): refusals the root
    /// decided for an op under a non-stealable grant it does not reach (the
    /// forward's `EROFS`/`EXDEV`, its own op's, a drain's journaled
    /// `Refused`; the flag: the drain's), and the ops a non-stealable
    /// generation of this node executed. Filled only when tracked.
    track_fence_audit: bool,
    /// Tests of the I1 emission gate only: an answer held at emission is
    /// expected (no debug assertion).
    pub(crate) emission_hold_expected: bool,
    final_nonholder_refusals: Vec<(Rid, Code, bool)>,
    nonstealable_executions: Vec<(Rid, u64)>,
    /// ADR-47: the `touched_gen` of the rids in `in_doubt_rids` (non-zero
    /// values only); a resubmission starts from it.
    in_doubt_touched: std::collections::BTreeMap<Rid, u64>,
    /// ADR-47: the highest own rid evicted from `in_doubt_rids`; an own rid
    /// at or below it is submitted with an unknown send history.
    in_doubt_evicted_max: Option<Rid>,
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
    timers: BTreeMap<TimerId, (Timer, TimerAt)>,
    poll_timer: Option<TimerId>,
    drain_timer: Option<TimerId>,
    /// The takeover gate's inbox drain in flight beside the job slot: the
    /// gate's epoch, the request, and when it was issued (the freshness
    /// bound of what it returns). See `Core::issue_drain`.
    gate_drain: Option<(Epoch, OpId, WaitMs)>,
    /// `Timer::GateDrainRenew`, armed while a job waits for the drain.
    gate_drain_timer: Option<TimerId>,
    idle_rounds: u32,
    /// Polls while a continuation epoch is open, since it opened or S3
    /// was last seen back (`next_poll_ms`'s probe backoff), and whether
    /// an S3 request failed since.
    epoch_probes: u32,
    epoch_s3_down: bool,
    /// A nudge arrived while a round was in flight: run another right
    /// after it (`nudged` in the sync loop).
    nudged: bool,
    /// The dead-root check's lease read is in flight (`dead_root_check`).
    dead_root_get: bool,
    /// A client's control request waiting for a round (`PublishNow`,
    /// `Reintegrate`): answered when the round completes.
    round_waiters: Vec<(OpId, Control)>,
    /// `Control::Barrier`s waiting for the journal to ship through the
    /// position each was admitted at (`jobs::BarrierWait`).
    barriers: Vec<jobs::BarrierWait>,
    /// `Control::Acquire`/`ClaimOffer` requests answered when the
    /// acquisition in the slot finishes.
    acquire_waiters: Vec<(OpId, Control)>,
    /// Plan 43 C7b: when the last `Plan::Busy` says this node's observation
    /// lets it claim (`Plan::Busy::retry_at`): the retry of the ops waiting
    /// for the lease is at that instant when it is sooner than the backoff
    /// (`schedule_acquire_retry`).
    acquire_retry_hint: Option<crate::ids::WaitMs>,
    /// Plan 46 M1.3 step 3 (bucket C): claims lost in a row (reset by a
    /// won acquisition or a lease held by another); only the first
    /// `CLAIM_LOST_FAST_RETRIES` of them retry at the shortest backoff.
    claims_lost_streak: u32,
    acked: client::AckTracker,
    replay: replay::ReplayState,
    /// Chunk metered-own-rows: an observed position's upload, repeated
    /// while the watermark stays unreached (`Core::on_observed_upload`).
    observed_upload: client::ObservedUpload,
    publishing: Option<OpId>,
    /// M4: journal transactions the last upload pass held back.
    held_back: u64,
    /// An acquisition's classified plan, kept across its takeover tail.
    pending_plan: Option<Plan>,
    /// When the last retention gap check ran (`None`: never since start,
    /// so the first empty probe after a mount runs one).
    gap_checked_at: Option<WaitMs>,
    marker_attempts: u32,
    ship_attempts: u32,
    ship_purpose: jobs::ShipPurpose,
    /// A publish requested explicitly (`PublishNow`): run at the end of
    /// the next round regardless of cadence.
    publish_forced: bool,
    /// Continuation epoch: journal locally, never PUT a segment.
    skip_ship: bool,
    epoch: EpochState,
    /// The active epoch was replaced by its carrier's fresh proposal,
    /// not activated yet (chunk epoch-liveness-gap: the member reports
    /// open, neither active nor frozen, in between). The activated
    /// epoch's close-release is not run for that transient state: it runs
    /// if the epoch closes from it (an aborted proposal), and not at all
    /// once the new epoch activates, as at any open-to-open step.
    epoch_replacing: bool,
    /// What the last deposition recovery did (`Control::Reintegrate`).
    last_recovery: Option<String>,
    /// The one membership object (ADR-50): the write-eligible roster, the
    /// peer directory, the nodes heard from, the lapses and the audience.
    roster: roster::Roster,
    /// Each connected peer's measured round trips in the last placement
    /// tick (one per directory snapshot), for the delegation group's
    /// admission ([`Self::delegate_group_in_budget`]): a link is judged by
    /// its best recent sample, so a single slow one (load, a rejoin) does
    /// not refuse it, as one sample over budget does not recall a grant.
    /// Cleared when the link goes down or is not measured.
    rtt_recent: BTreeMap<NodeId, VecDeque<(WaitMs, u64)>>,
    /// When this node last heard from each peer (a message, or the driver's
    /// `PeerLink::last_seen`), on its wait clock. Kept across the link
    /// table's replacement (`Event::Peers`), which drops a link's last
    /// exchange once it disconnects: `repair abandon` weighs a node
    /// cut from this one only by it (plan 43 C2e review).
    peer_heard: BTreeMap<NodeId, WaitMs>,
    /// EC2 campaign 8 A-1: this node's own S3 path (`Event::OwnS3`).
    pub(crate) own_s3: OwnS3,
    /// A-1: no re-adoption of this node's own lease for a forward before
    /// this (a failed one backs off for a TTL).
    pub(crate) readopt_refused_until: WaitMs,
    /// EC2 finding 2: as the holder, when each other node last had a
    /// mutation executed here over P2P (a forward), and when each last
    /// had one executed through the S3 inbox — which side of a P2P
    /// partition uses the lease (`Core::keeps_lease_for_p2p_side`).
    demand_p2p: BTreeMap<NodeId, WaitMs>,
    demand_inbox: BTreeMap<NodeId, WaitMs>,
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
    /// Plan 41 R1: the root's delegation windows (`GenState`'s `until`),
    /// shared with whoever else may extend them ([`DelegWindows`]).
    pub(crate) dw: std::sync::Arc<DelegWindows>,
    /// Plan 41 L2: the certificate the lock lane judges by, published at the
    /// end of every step (`lock_lane_view`).
    lane_cell: std::sync::Arc<crate::lane::LaneCell>,
    /// Plan 30 §M11 phase 2b: the placement window (the root).
    pl: placement::PlacementState,
    /// Plan 30 §M14.
    pub(crate) lk: locks::LockState,
    /// Plan 31 C8: forward-only and suspended (`mode.rs`).
    pub(crate) mode: AuthorityMode,
    /// The `now` of the event being handled (for `issue_s3`'s send time).
    clk: Now,
    stopped: bool,
    pub stats: Stats,
}

impl Core {
    pub fn new(cfg: Config) -> Self {
        // Plan 43 C2: request ids carry the incarnation in their high bits (22
        // of them, below the lanes' reserved bits), so an answer to a request
        // of an earlier incarnation of this node — a delegate's parked answer
        // released after the requester rebooted, now that answers wait for a
        // group — names no request of this one (locks-unlinked-delegated
        // seed 494, -hcrash seed 438).
        let next_op = (u64::from(cfg.incarnation) & 0x3f_ffff) << 40 | 1;
        Self {
            lease: {
                let mut lease = LeaseState::default();
                lease.release_when_idle = cfg.forward_only;
                lease
            },
            ship: ShipState::default(),
            next_op,
            next_timer: 1,
            job: None,
            queued_jobs: VecDeque::new(),
            pending_flushes: Vec::new(),
            flush_floor_timer: None,
            clients: BTreeMap::new(),
            in_doubt_rids: std::collections::BTreeSet::new(),
            unsettled_designation_rids: std::collections::BTreeSet::new(),
            unsettled_accepted_rids: std::collections::BTreeSet::new(),
            withdraw_failed_rids: std::collections::BTreeSet::new(),
            track_unsettled_designation: false,
            track_fence_audit: false,
            emission_hold_expected: false,
            final_nonholder_refusals: Vec::new(),
            nonstealable_executions: Vec::new(),
            in_doubt_touched: std::collections::BTreeMap::new(),
            in_doubt_evicted_max: None,
            in_doubt_batches: std::collections::BTreeMap::new(),
            shipped_touches: std::collections::VecDeque::new(),
            shipped_floor: 0,
            next_client_order: 0,
            by_req: BTreeMap::new(),
            s3: BTreeMap::new(),
            timers: BTreeMap::new(),
            poll_timer: None,
            drain_timer: None,
            gate_drain: None,
            gate_drain_timer: None,
            idle_rounds: 0,
            epoch_probes: 0,
            epoch_s3_down: false,
            nudged: false,
            dead_root_get: false,
            round_waiters: Vec::new(),
            barriers: Vec::new(),
            acquire_waiters: Vec::new(),
            acquire_retry_hint: None,
            claims_lost_streak: 0,
            acked: client::AckTracker::default(),
            replay: replay::ReplayState::default(),
            observed_upload: Default::default(),
            publishing: None,
            held_back: 0,
            pending_plan: None,
            gap_checked_at: None,
            marker_attempts: 0,
            ship_attempts: 0,
            ship_purpose: jobs::ShipPurpose::Journal,
            publish_forced: false,
            skip_ship: false,
            epoch: EpochState::default(),
            epoch_replacing: false,
            last_recovery: None,
            roster: roster::Roster::new(),
            rtt_recent: BTreeMap::new(),
            peer_heard: BTreeMap::new(),
            own_s3: OwnS3::default(),
            readopt_refused_until: WaitMs(0),
            demand_p2p: BTreeMap::new(),
            demand_inbox: BTreeMap::new(),
            inbox: inbox::InboxState::default(),
            stream: stream::StreamState::default(),
            rd: readindex::ReadState::default(),
            ack: backup::AckState::default(),
            bk: backup::BackupState::default(),
            pr: promise::PromiseState::default(),
            dl: delegate::DelegationState::default(),
            dw: DelegWindows::new(),
            lane_cell: crate::lane::LaneCell::new(),
            pl: placement::PlacementState::default(),
            lk: locks::LockState::default(),
            mode: AuthorityMode {
                forward_only: cfg.forward_only,
                suspended: false,
            },
            clk: Now::same(0),
            stopped: false,
            stats: Stats::default(),
            cfg,
        }
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    /// The bounds derived from this core's configuration (computed on use,
    /// not held: the configuration can still change under a core, as the
    /// promise TTL does when a promise clamps it).
    pub fn dl_t(&self) -> crate::deadlines::Deadlines {
        crate::deadlines::Deadlines::new(&self.cfg)
    }

    pub fn node_id(&self) -> NodeId {
        self.cfg.node_id
    }

    /// Plan 31 C8: forward-only and suspended, for `status`.
    pub fn authority_mode(&self) -> AuthorityMode {
        self.mode
    }

    /// The lease this node believes it holds, for `status`.
    /// Plan 41 R1: the root's delegation windows. The driver shares this
    /// handle with whoever may extend them besides the core, and gives it
    /// the clock the guard-side decisions read ([`DelegWindows::set_clock`]).
    pub fn deleg_windows(&self) -> &std::sync::Arc<DelegWindows> {
        &self.dw
    }

    /// Use `windows` as this core's delegation windows (before the first
    /// event): the daemon and a simulation create the table first and share
    /// it with the answerer.
    pub fn set_deleg_windows(&mut self, windows: std::sync::Arc<DelegWindows>) {
        self.dw = windows;
    }

    /// Plan 41 L2: the lock lane's certificate cell (the driver shares it
    /// with the lane; [`Core::set_lane_cell`]).
    pub fn lane_cell(&self) -> &std::sync::Arc<crate::lane::LaneCell> {
        &self.lane_cell
    }

    /// Use `cell` as this core's certificate cell (before the first event).
    pub fn set_lane_cell(&mut self, cell: std::sync::Arc<crate::lane::LaneCell>) {
        self.lane_cell = cell;
    }

    pub fn take_drained_rows(&mut self) -> Vec<(u64, u64)> {
        std::mem::take(&mut self.dl.drained_rows)
    }

    pub fn lease(&self) -> &LeaseState {
        &self.lease
    }

    pub fn ship(&self) -> &ShipState {
        &self.ship
    }

    /// The peers whose link this core sees connected, for `status`.
    pub fn connected_links(&self) -> Vec<NodeId> {
        self.roster
            .links()
            .values()
            .filter(|l| l.connected)
            .map(|l| l.node)
            .collect()
    }

    /// EC2 campaign 8 A-1: this node's own S3 path, for `status`.
    pub fn own_s3(&self) -> OwnS3 {
        self.own_s3
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
            ("barriers", self.barriers.len()),
            ("acquire_waiters", self.acquire_waiters.len()),
            ("links", self.roster.links().len()),
            ("inbox_pending", self.inbox.pending.len()),
            ("dw_slots", self.dw.len()),
            ("s3_in_flight", self.s3.len()),
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

    /// The job in progress, in full (its phase and what it waits for),
    /// for the driver's stuck-job report.
    pub fn job_detail(&self) -> Option<String> {
        self.job.as_ref().map(|j| format!("{j:?}"))
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
    pub fn inbox_view(&self, now: Now) -> InboxView {
        self.inbox
            .view(now, &self.cfg, self.roster.write_eligible())
    }

    /// Refused replays whose conflict copy is pending / stalled
    /// (`SpoolInfo::replay_copies_*`).
    pub fn replay_copies(&self, now: Now) -> (u64, u64) {
        self.replay
            .copy_counts(now, self.cfg.replay_lease_fallback_ms)
    }

    /// A drop's refused entries waiting for their rid to be settled (no
    /// sequencer reachable, or its answer not in the log here yet) /
    /// those waiting for at least the lease-fallback interval.
    pub fn replay_settles(&self, now: Now) -> (u64, u64) {
        self.replay
            .settle_counts(now, self.cfg.replay_lease_fallback_ms)
    }

    /// The core answered its final flush and takes no more events.
    pub fn stopped(&self) -> bool {
        self.stopped
    }

    /// The first call after construction (mount): the periodic poll and
    /// the replay drain start; the replica's persisted deposition flag
    /// is honoured (`recover_deposed` at mount).
    pub fn start(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        self.clk = now;
        self.clear_holder_epoch(replica);
        self.ship.last_publish = now.wait;
        let applied = replica.applied_seq().unwrap_or(0);
        self.ship.next_seq = applied + 1;
        self.ship.head_seq = applied;
        // Plan 43 §11 Q20 (h): the fencing floor is the replica's, not 0 —
        // a restarted node skips a superseded-epoch segment exactly as the
        // incarnation that applied the earlier ones would have.
        self.ship.max_epoch = self.ship.max_epoch.max(replica.applied_epoch());
        if replica.lost_persisted() {
            self.lease.force_lost();
        }
        self.arm_poll(now, 0, out);
        self.arm_drain(now, out);
        self.floor_start(now, replica);
        self.read_start(now, replica, out);
        self.deleg_restart_start(now, replica, out);
        self.locks_start(now, replica);
        self.backup_start(now, replica, out);
        self.promise_start(now, replica, out);
        // EC2 campaign 8 A-1: learn who holds the lease now, not at the
        // first forward a peer sends here. A restarted holder whose
        // previous incarnation's lease is still in force then re-adopts
        // it for that forward (`readopt_own_lease_for_forward`) instead of
        // answering `NotHolder { 0 }` because it has not read the lease.
        if self.cfg.p2p && self.cfg.forwarding {
            self.issue_s3(crate::action::S3Op::LeaseGet, S3For::RefreshHolder, out);
        }
        // Plan 30 §M11: the delegations the table names for this node are
        // its own again (a restart, a `daemon --upgrade` or a K5 handoff
        // keeps the node identity and the journal): installed now and
        // renewed at once, their unstreamed rows re-streamed from what the
        // log has — not left to the root's reclaim of a grant nobody
        // renews, which held every write under them for the grant's ttl.
        self.delegation_sync(now, replica, out);
    }

    /// Handle one event. Every action returned must be carried out by the
    /// driver after this returns; none of them may be waited on before.
    pub fn handle(&mut self, now: Now, event: Event, replica: &dyn Replica) -> Vec<Action> {
        // Plan 43 C1-2: every row the core journals carries a class it set
        // (`Core::journal_class`); a row that fell back to the class the
        // session publishes is a call site that forgot it. Counted, and a
        // debug assertion for the tests and the simulation.
        let from_session = replica.class_from_session();
        let out = self.handle_event(now, event, replica);
        let missed = replica.class_from_session().saturating_sub(from_session);
        if missed > 0 {
            self.stats.journal_class_from_session += missed;
            debug_assert!(
                false,
                "{missed} journal row(s) written by the core without a class of its own"
            );
        }
        out
    }

    fn handle_event(&mut self, now: Now, event: Event, replica: &dyn Replica) -> Vec<Action> {
        let mut out = Vec::new();
        if self.stopped {
            self.dw.close_all();
            self.dw.publish(None, &[]);
            self.lane_cell.publish(Default::default());
            return out;
        }
        self.clk = now;
        // A lease change that landed since the last event closes the old
        // configuration before any row of this one is journaled.
        self.sync_cfg_spans(replica);
        self.deleg_root_sync_due(now, replica, &mut out);
        match event {
            Event::Submit {
                rid,
                op,
                policy,
                tag,
            } => self.on_submit(now, rid, op, policy, tag, replica, &mut out),
            Event::SubmitInDoubt {
                rid,
                op,
                policy,
                tag,
            } => {
                self.note_in_doubt(rid);
                self.on_submit(now, rid, op, policy, tag, replica, &mut out)
            }
            Event::Peer { from, msg } => {
                self.note_peer_heard(from, now.wait);
                // ADR-50: every message kind from a node counts, here,
                // before any row of it is journaled.
                self.note_heard(now, from, replica);
                self.on_peer(now, from, msg, replica, &mut out)
            }
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
            Event::DelegStreamSynced { gen, through, ok } => {
                self.on_deleg_stream_synced(now, gen, through, ok, replica, &mut out)
            }
            Event::GrantHorizonPersisted {
                kind,
                until,
                durable,
            } => self.on_grant_horizon_persisted(kind, until, durable),
            Event::LockHorizonPersisted { until, durable } => {
                self.on_lock_horizon_persisted(now, until, durable, replica, &mut out)
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
            Event::Roster {
                seq,
                write_eligible,
                read_only,
                complete,
                lapses,
                issued,
            } => {
                let foreign = write_eligible.iter().any(|n| *n != self.cfg.node_id);
                let complete = complete || self.cfg.roster_installs_partial;
                let read = roster::RosterRead {
                    seq,
                    write_eligible,
                    read_only,
                    complete,
                    lapses,
                    issued,
                };
                if !self.roster.install_read(read) {
                    return out;
                }
                if foreign {
                    self.note_foreign(now, replica, &mut out);
                }
                // A holder polls a requester it has just learned of at
                // once (M13 round 2), not at its next round's end.
                if self.inbox.holder.is_some() {
                    self.inbox_holder_tick(now, replica, &mut out);
                }
                self.kept_roster_arrived(now, replica, &mut out);
            }
            Event::Peers {
                registry_complete,
                links,
                lapses,
            } => {
                tracing::trace!(
                    node = self.cfg.node_id,
                    links = ?links.iter().map(|l| (l.node, l.connected)).collect::<Vec<_>>(),
                    "peers"
                );
                if links.iter().any(|l| l.node != self.cfg.node_id) {
                    self.note_foreign(now, replica, &mut out);
                }
                for l in &links {
                    if let Some(at) = l.last_seen {
                        self.note_peer_heard(l.node, at);
                    }
                }
                self.note_rtt_samples(now.wait, &links);
                self.roster.install_links(links, registry_complete, lapses);
                self.stream_on_peers(now);
            }
            Event::OwnS3 {
                stalled,
                peers_reach_s3,
            } => self.on_own_s3(now, stalled, peers_reach_s3, replica, &mut out),
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
            Event::HolderAlive {
                from,
                epoch,
                candidacy,
                listed,
                at,
                seq,
                clock,
                sent,
                holder_expires,
                guard,
            } => self.on_holder_alive(
                from,
                epoch,
                candidacy,
                listed,
                at,
                (seq, clock, sent, holder_expires, guard),
                replica,
            ),
            Event::OwnLag { from, until } => {
                self.roster.note_lapse();
                self.on_own_lag(from, until);
                // The directory this node holds may predate the lapse: not
                // a singleton again until a read issued after it arrives (a
                // parked ack waits, or is released by the log, not by the
                // lone test: backup-crash-slow seed 1416 released one
                // `Lone` inside this very event).
            }
            Event::OwnFreeze { from, until } => self.on_own_freeze(from, until),
            Event::DelegRenewSentOffCore {
                gen,
                round,
                to,
                req,
                sent,
            } => self.on_deleg_renew_sent_off_core(gen, round, to, req, sent),
            Event::LaneOwn { renewed } => self.on_lane_own(now, renewed, replica, &mut out),
            Event::LaneRenewed {
                from,
                sent,
                results,
            } => self.on_lane_renewed(now, from, sent, results, replica, &mut out),
            Event::LaneUnanswered { to } => self.on_lane_unanswered(to, replica, &mut out),
            Event::DelegRenewAnsweredOffCore { gen, from, ttl_ms } => {
                self.on_deleg_renew_answered_off_core(now, gen, from, ttl_ms, &mut out)
            }
            Event::RootProbed { epoch, sent, read } => {
                self.on_root_probed(now, epoch, sent, read, replica, &mut out)
            }
            Event::BackupAlive { from, at } => self.on_backup_alive(from, at),
            Event::Slack { epoch_slack } => self.on_slack(now, epoch_slack, replica, &mut out),
            Event::Control { op, req } => self.on_control(now, op, req, replica, &mut out),
        }
        self.grant_horizon_after_event(replica, &mut out);
        self.deleg_root_sync_due(now, replica, &mut out);
        self.resolve_moot_waits(now, replica, &mut out);
        self.reroute_ended_epoch_waits(now, replica, &mut out);
        self.inbox_after_event(now, &mut out);
        self.stream_after_event(now, &mut out);
        self.backup_after_event(now, replica, &mut out);
        self.root_stream_push(now, replica, &mut out);
        self.renew_after_own_cas(now, &mut out);
        self.promise_after_event(now, replica, &mut out);
        self.deleg_after_event(now, replica, &mut out);
        self.report_deleg_durable(replica);
        if self.cfg.delegation {
            self.withdraw_lost_deps_inbox_ops(now, replica, &mut out);
        }
        self.locks_after_event(now, replica, &mut out);
        self.renew_after_event(now, replica, &mut out);
        self.note_client_states(now);
        // Plan 41 R2: what the root's off-core answerer may rest on, as of
        // the end of this step and before the driver dispatches its actions.
        self.deleg_publish_root(replica);
        self.lock_publish_lane(now, replica);
        out
    }

    /// When a pending timer fires, on the clock it was set against.
    pub(crate) fn timer_at(&self, id: TimerId) -> Option<TimerAt> {
        self.timers.get(&id).map(|(_, at)| *at)
    }

    /// A pending timer's deadline as raw milliseconds, whichever clock it is
    /// on (tests that do not step the clocks, where the three agree).
    #[cfg(test)]
    pub(crate) fn timer_ms(&self, id: TimerId) -> Option<Ms> {
        Some(match self.timer_at(id)? {
            TimerAt::Honour(at) => Ms(at.0),
            TimerAt::Wait(at) => Ms(at.0),
        })
    }

    /// A pending HONOUR timer's deadline (tests).
    #[cfg(test)]
    pub(crate) fn timer_honour(&self, id: TimerId) -> Option<HonourMs> {
        match self.timer_at(id)? {
            TimerAt::Honour(at) => Some(at),
            other => panic!("not an honour timer: {other:?}"),
        }
    }

    // ---- dispatch ----

    fn on_peer(
        &mut self,
        now: Now,
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
                applied,
                tag,
                settle,
                routed_gen,
                touched_gen,
                inbox_ever,
            } => self.on_mutate_request(
                now,
                from,
                req,
                rid,
                op,
                acked_through,
                (deps, applied),
                tag,
                settle,
                holder::Fence {
                    routed_gen,
                    touched_gen,
                    inbox_ever,
                },
                replica,
                out,
            ),
            PeerMsg::MutateReply {
                req,
                outcome,
                base,
                position,
                gen,
                own_chunks,
                own_rows,
                // Plan 43 C1-4: the requester's shadow or hint is installed as
                // committed from it.
                committed,
            } => self.on_mutate_reply(
                now,
                from,
                req,
                outcome,
                (base, position, gen, own_chunks, own_rows, committed),
                replica,
                out,
            ),
            PeerMsg::LeaseRequest { req, epoch_applied } => {
                self.on_lease_request(now, from, req, epoch_applied, replica, out)
            }
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
                ahead,
            } => self.on_read_index(now, from, req, ino, dir, name, ahead, replica, out),
            PeerMsg::ReadIndexReply { req, outcome } => {
                self.on_read_index_reply(now, from, req, outcome, replica, out)
            }
            PeerMsg::DelegationRecall { req, ino, .. } => {
                self.on_delegation_recall(from, req, ino, replica, out)
            }
            PeerMsg::DelegationRecalled { req } => {
                self.on_delegation_recalled(now, req, replica, out)
            }
            PeerMsg::DelegReadVoid { req, gen, scope } => {
                self.on_deleg_read_void(from, req, gen, scope, replica, out)
            }
            PeerMsg::BackupAppend {
                req,
                epoch,
                holder,
                config_version,
                candidacy,
                from: from_jseq,
                txs,
                through,
                sent_ms,
                expires_ms,
                clock,
            } => self.on_backup_append(
                now,
                from,
                req,
                (epoch, holder, config_version, candidacy),
                from_jseq,
                txs,
                (through, (clock, sent_ms, expires_ms)),
                replica,
                out,
            ),
            PeerMsg::BackupAck {
                req,
                epoch,
                acked,
                sealed,
                fresh,
            } => self.on_backup_ack(now, from, req, epoch, acked, sealed, fresh, replica, out),
            PeerMsg::StreamAhead {
                epoch,
                base,
                txs,
                committed,
            } => self.on_stream_ahead(now, from, epoch, base, txs, committed, replica, out),
            PeerMsg::StreamAheadRefused { epoch, sealed } => {
                self.on_stream_ahead_refused(now, from, epoch, sealed, replica, out)
            }
            PeerMsg::StreamAheadAck { epoch, through } => {
                self.on_stream_ahead_ack(now, from, epoch, through, replica, out)
            }
            PeerMsg::BackupHold { epoch, for_ms } => self.on_backup_hold(now, from, epoch, for_ms),
            PeerMsg::PromiseRequest { req, key } => {
                self.on_promise_request(now, from, req, key, replica, out)
            }
            PeerMsg::PromiseReply {
                req,
                granted,
                epoch_slack,
            } => self.on_promise_reply(now, from, req, granted, epoch_slack, replica, out),
            PeerMsg::DelegateStream {
                req,
                gen,
                round,
                txs,
                leaving,
                leaving_barriers,
                released,
            } => self.on_delegate_stream(
                now,
                from,
                req,
                gen,
                round,
                txs,
                leaving,
                leaving_barriers,
                released,
                replica,
                out,
            ),
            PeerMsg::DelegateStreamAck {
                req,
                gen,
                round,
                through,
                committed,
                refused,
                archive_bound,
            } => self.on_delegate_stream_ack(
                now,
                from,
                req,
                gen,
                round,
                through,
                committed,
                refused,
                archive_bound,
                replica,
                out,
            ),
            PeerMsg::DelegRenew {
                req,
                gen,
                round,
                backup,
                stream_head,
                stream_head_at,
                first,
            } => {
                self.lock_note_delegate_head(now, from, gen, stream_head, stream_head_at);
                self.deleg_prev_roots_renewed(now, from, gen, stream_head, replica, out);
                self.on_deleg_renew(
                    now,
                    from,
                    req,
                    gen,
                    round,
                    delegate::RenewFrom::Core { backup, first },
                    replica,
                    out,
                )
            }
            // The off-core lane's renewal: no head to note (the as-of
            // time of the root's lock cut stays the core's last, and a
            // new tenure's `tenure_waiting` still waits for a head the
            // core reports), no backup to take.
            PeerMsg::DelegRenewOffCore { req, gen, round } => self.on_deleg_renew(
                now,
                from,
                req,
                gen,
                round,
                delegate::RenewFrom::OffCore,
                replica,
                out,
            ),
            PeerMsg::DelegRenewedOffCore {
                req,
                gen,
                round,
                ttl_ms,
            } => self.on_deleg_renewed_off_core(now, from, req, gen, round, ttl_ms, replica, out),
            PeerMsg::DelegRenewed {
                req,
                gen,
                round,
                ttl_ms,
                locks,
                lock_grace_ms,
                lock_floor,
                lock_cut_at,
                lock_cut,
                lock_barrier,
                lock_carry,
                sent_at,
                ends,
            } => {
                // The renewal installs the generation's window first; the
                // handoff is tagged with the generation itself. A grace
                // the root carried binds a delegate that starts serving
                // with this renewal. An answer from another root or round,
                // or for a generation no longer this node's, carries
                // nothing (its cut would replace the current root's).
                let serving = self.deleg_mine_until(gen).is_some();
                if !self.on_deleg_renewed(now, from, req, gen, round, ttl_ms, ends, replica, out) {
                    return;
                }
                self.deleg_note_root_answer(gen, from, sent_at);
                if ttl_ms > 0 && !serving {
                    self.lock_take_grace(now, gen, lock_grace_ms);
                }
                if ttl_ms > 0 {
                    self.lock_take_floor(gen, &lock_floor);
                    self.lock_take_cut(
                        now,
                        from,
                        gen,
                        (lock_cut_at, *lock_cut),
                        lock_barrier,
                        replica,
                        out,
                    );
                }
                if ttl_ms > 0 && !locks.is_empty() {
                    self.lock_install_moved(now, gen, locks, &lock_carry, replica);
                }
                // Plan 46 M1.2 step 9: the subtrees the stream grew by, whose
                // commit this node learned before it sent this renewal, are
                // honoured with the lock content they came with.
                if ttl_ms > 0 {
                    let sent = self
                        .dl
                        .mine
                        .get_mut(&gen)
                        .and_then(|d| d.core_answer_sent.take());
                    if let Some(sent) = sent {
                        let unlocked = self.deleg_unpark_grown(now, gen, sent, out);
                        if !unlocked.is_empty() {
                            self.lock_take_grace_dirs(now, &unlocked, lock_grace_ms);
                        }
                    }
                }
            }
            PeerMsg::ReleaseRequest { req, gen, dirs } => {
                self.on_release_request(now, from, req, gen, dirs, replica, out)
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
                prev_roots,
            } => self.on_deleg_sealed(now, from, req, gen, sealed, txs, prev_roots, replica, out),
            // Plan 43 C1-3: a claimant asks for the tail of an epoch this
            // node was a member of the group of, and the answer.
            PeerMsg::SealTail {
                req,
                epoch,
                config_version,
                from: from_seq,
                key,
                kept,
                tenure,
            } => self.on_seal_tail(
                now,
                from,
                req,
                (epoch, config_version, from_seq),
                key,
                (kept, tenure),
                replica,
                out,
            ),
            PeerMsg::SealedTail {
                req,
                epoch,
                held,
                empty,
                txs,
                through,
                next,
                members,
                sealed,
                refused,
            } => {
                // Plan 46 M0.3 (G): a member that answers is up. A restarted
                // holder heard from here is reachable again for this node's
                // forwards and settles (which make it re-adopt its lease).
                self.note_p2p_result(now, from, true);
                self.on_sealed_tail(
                    now,
                    from,
                    req,
                    epoch,
                    (held, empty),
                    (txs, through, next),
                    seal_tail::KeptFields {
                        members,
                        sealed,
                        refused,
                    },
                    replica,
                    out,
                )
            }
            PeerMsg::LockRequest {
                req,
                ino,
                mode,
                blocking,
                sent,
                incarnation,
                waited_ms,
            } => self.on_lock_request(
                now,
                from,
                req,
                ino,
                mode,
                blocking,
                sent,
                incarnation,
                waited_ms,
                replica,
                out,
            ),
            PeerMsg::LockReply { req, outcome } => {
                self.on_lock_reply(now, from, req, outcome, replica, out)
            }
            PeerMsg::LockGranted { ino, sent, outcome } => {
                self.on_lock_granted_push(now, from, ino, sent, outcome, replica, out)
            }
            PeerMsg::LockCancel {
                ino,
                exclusive,
                shared,
                upto,
            } => self.on_lock_cancel(now, from, ino, exclusive, shared, upto, replica, out),
            PeerMsg::LockRecall { req, ino, grant } => {
                self.on_lock_recall(now, from, req, ino, grant, replica, out)
            }
            PeerMsg::LockRecalled { req } => self.on_lock_recalled_ack(from, req),
            PeerMsg::LockReleased {
                ino,
                grant,
                position,
            } => self.on_lock_released(now, from, ino, grant, position, replica, out),
            PeerMsg::LockRenew {
                req,
                entries,
                sent,
                clock,
            } => self.on_lock_renew(now, from, req, entries, (clock, sent.0), replica, out),
            PeerMsg::LockRenewed { req, results } => {
                self.on_lock_renewed(now, from, req, results, replica, out)
            }
            PeerMsg::LockMirror {
                ver,
                grants,
                floor,
                final_for,
            } => self.on_lock_mirror(from, ver, grants, floor, final_for),
            PeerMsg::LockTest { req, ino, mode } => {
                self.on_lock_test(now, from, req, ino, mode, replica, out)
            }
            PeerMsg::LockTestReply { req, outcome } => self.on_lock_test_reply(req, outcome, out),
        }
    }

    fn on_peer_failed(
        &mut self,
        now: Now,
        req: OpId,
        to: NodeId,
        outage: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.on_leave_note_failed(req) {
            return;
        }
        if self.on_stream_failed(now, req, out) {
            return;
        }
        if self.on_read_request_failed(now, req, to, outage, out) {
            return;
        }
        if self.on_backup_request_failed(now.wait, req, to) {
            return;
        }
        // Plan 43 C1-3: a member that did not answer a `SealTail`.
        if self.on_tail_fetch_failed(now, req, replica, out) {
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
        now: Now,
        op: OpId,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(purpose) = self.s3.remove(&op) else {
            tracing::debug!(node = self.cfg.node_id, ?op, "stale S3 result dropped");
            return;
        };
        self.note_s3_liveness(now, op, &result);
        self.note_epoch_probe(&result);
        // Plan 43 C7b: every lease read, whatever asked for it, is observed
        // here once, before any handler judges it (the handling time is the
        // `t0` of the observation rule, never the GET's send time).
        if let S3Result::LeaseGet(Ok(object)) = &result {
            // Plan 43 C1-3: a lease of a later epoch that no longer names
            // a tail this node keeps a copy of ends the copy.
            if let Some((lease, _)) = object {
                self.note_group_wait_lease(lease);
            }
            if self.lease.observe(now, object.as_ref())
                && object.as_ref().is_some_and(|(l, _)| {
                    l.holder != 0 && l.holder != self.cfg.node_id && l.ttl_ms == 0
                })
            {
                self.stats.lease_ttl_unknown_bodies += 1;
            }
        }
        match purpose {
            S3For::Job => self.on_job_s3(now, op, result, replica, out),
            S3For::GateDrain(epoch) => self.on_gate_drain(now, epoch, op, result, replica, out),
            S3For::LearnHolder(rid) => self.on_holder_learned(now, rid, result, replica, out),
            S3For::RefreshHolder => {
                if let S3Result::LeaseGet(Ok(Some((lease, _)))) = result {
                    self.lease.note_object(&lease);
                }
            }
            S3For::RegisterWanted => {}
            S3For::InboxPut => self.on_inbox_put(now, result, replica, out),
            S3For::InboxLastN => self.on_inbox_last_n(now, result, replica, out),
            S3For::InboxPoll(node) => self.on_inbox_polled(now, node, op, result, replica, out),
            S3For::InboxRecheck => self.on_inbox_recheck(now, result, replica, out),
            S3For::InboxGc(key) => self.on_inbox_gc(now, key, result),
            S3For::InboxTombstoneBg(key) => self.on_bg_tombstone(now, key, result, out),
            S3For::InboxLeftover(node) => {
                self.on_inbox_leftover_read(now, node, op, result, replica, out)
            }
            S3For::InboxWithdraw(rid, key) => {
                self.on_inbox_withdrawn(now, rid, key, result, replica, out)
            }
            S3For::ReadHolder(op) => self.on_read_holder_learned(now, op, result, replica, out),
            S3For::LockHolder(op) => self.on_lock_holder_learned(now, op, result, replica, out),
            S3For::LockRenewHolder => self.on_lock_renew_holder(now, result, out),
            S3For::Reconfig => self.on_reconfig_put(now, result, replica, out),
            S3For::ReconfigReread => self.on_reconfig_reread(now, result, replica, out),
            S3For::TakeoverGet => self.on_takeover_get(now, result, replica, out),
            S3For::MarkGranting => self.on_mark_granting_put(now, result, replica, out),
            S3For::MarkGrantingReread => self.on_mark_granting_reread(now, result, replica, out),
            S3For::ClearMark => self.on_clear_mark_put(now, op, result, out),
            S3For::ClearMarkReread => self.on_clear_mark_reread(now, op, result, replica, out),
            S3For::Heartbeats => self.on_heartbeats(now, result, replica, out),
            S3For::KeptSlack => self.on_kept_heartbeats(now, result, replica, out),
            S3For::HeartbeatPut(slack) => self.on_heartbeat_put(slack, result),
            S3For::PromiseWatch => {
                if let S3Result::LeaseGet(Ok(Some((lease, _)))) = result {
                    self.lease.note_object(&lease);
                }
            }
            S3For::DeadRoot => self.on_dead_root_get(now, result, replica, out),
            S3For::RenewProbe => self.on_renew_probe(now, result, replica, out),
        }
    }

    fn on_timer(&mut self, now: Now, id: TimerId, replica: &dyn Replica, out: &mut Vec<Action>) {
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
            Timer::WithdrawRetry(rid) => self.on_withdraw_timer(now, rid, replica, out),
            Timer::OwnRecordWait(rid) => self.on_own_record_wait(now, rid, out),
            Timer::ObservedUpload => self.on_observed_upload(now, replica, out),
            Timer::AcquireRetry(rid) => self.on_acquire_retry(now, rid, replica, out),
            Timer::ClientDeadline(rid) => self.on_client_deadline_timer(now, rid, replica, out),
            Timer::ReplayDrain => {
                self.drain_timer = None;
                self.on_drain_tick(now, replica, out);
                self.arm_drain(now, out);
            }
            Timer::JobRequestTimeout(req) => {
                // Plan 43 C1-3: the timer that ends an attempt of a claim to
                // fetch its group's tail.
                if !self.on_tail_fetch_timer(now, req, replica, out) {
                    self.on_job_peer_failed(now, req, 0, replica, out);
                }
            }
            Timer::FlushFloor => {
                self.flush_floor_timer = None;
                self.service_pending_flushes(now, replica, out);
            }
            Timer::InboxPoll => {
                self.inbox.poll_timer = None;
                self.inbox_holder_tick(now, replica, out);
            }
            Timer::InboxRecheck => {
                self.inbox.recheck_timer = None;
                self.on_inbox_recheck_tick(now, replica, out);
            }
            Timer::InboxTombstoneBg => {
                self.inbox.bg_timer = None;
                self.on_bg_tombstone_tick(now, out);
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
            Timer::HeldReply(park) => self.on_held_reply_timer(now, park, replica, out),
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
            Timer::HoldResend => self.on_hold_resend_timer(now, replica, out),
            Timer::BackupWatch => {
                self.bk.watch_timer = None;
                self.on_backup_watch(now, out);
            }
            Timer::PromiseWait => self.on_promise_wait(now, replica, out),
            Timer::PromiseWatch => {
                self.pr.watch_timer = None;
                self.on_promise_watch(now, out);
            }
            Timer::DelegExpiry(gen) => self.on_deleg_expiry(now, gen, replica, out),
            Timer::ReleaseResend(gen) => self.on_release_resend(now, gen, out),
            Timer::DelegReleaseSeal(gen) => self.on_deleg_release_seal(now, gen, replica, out),
            Timer::DelegRenew(gen) => self.on_deleg_renew_timer(now, gen, out),
            Timer::DelegLapse(gen) => self.on_deleg_lapse(now, gen, replica, out),
            Timer::LockRequestTimeout(req) => self.on_lock_request_timeout(now, req, replica, out),
            Timer::LockRetry(op) => self.on_lock_retry(now, op, replica, out),
            Timer::LockHeldReply(w) => self.on_lock_held_reply_timer(w, out),
            Timer::LockGrantExpiry(id) => self.on_lock_grant_expiry(now, id, replica, out),
            Timer::LockRenewTick => self.on_lock_renew_tick(now, replica, out),
            Timer::LockReleaseWait(ino, grant) => {
                self.on_lock_flushed(now, ino, grant, true, replica, out)
            }
            Timer::LockWaiterTick => self.on_lock_waiter_tick(now, replica, out),
            Timer::LockGraceEnd => self.on_lock_grace_end(now, replica, out),
            Timer::LockFinal => {
                self.lk.final_timer = None;
                self.lock_final_step(now, replica, out, true)
            }
            Timer::LockRenewTimeout(req) => self.on_lock_renew_timeout(req, replica, out),
            Timer::LockTestTimeout(req) => self.on_lock_test_timeout(req, out),
            Timer::DelegStream => self.on_deleg_stream_timer(now, replica, out),
            Timer::DelegRestartQuarantine(gen) => {
                self.on_deleg_restart_quarantine(now, gen, replica, out)
            }
            Timer::DelegInstallRetry => self.on_deleg_install_retry(now, replica, out),
            Timer::Placement => self.on_placement_timer(now, replica, out),
            Timer::ReleaseRecallBound => self.on_release_recall_bound(now, replica, out),
            Timer::LeaveWait => self.on_leave_wait_timer(now, replica, out),
            Timer::StickyRelease => self.on_sticky_release_timer(now, replica, out),
            Timer::AbandonWait => self.on_abandon_timer(now, replica, out),
            Timer::GateDrainRenew => {
                self.gate_drain_timer = None;
                self.on_gate_drain_renew(now, replica, out);
            }
        }
    }

    fn on_control(
        &mut self,
        now: Now,
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
            Control::Barrier { .. } if self.lease.lost => {
                // Deposed: the recovery round is about to strand the
                // unshipped rows, emptying the journal without shipping
                // them, so no position taken now could be honoured.
                out.push(Action::ControlDone {
                    op,
                    result: Err(Self::barrier_refused_deposed()),
                });
            }
            Control::Barrier { .. } => {
                // Its position is taken here, as it is admitted: every row
                // journaled before it — every write acknowledged before
                // the caller asked — is at or below the journal's tip.
                let upto = replica.journal_tip();
                self.barriers.push(jobs::BarrierWait::new(op, upto));
                self.nudge(now, out);
            }
            Control::PublishNow => {
                self.publish_forced = true;
                self.round_waiters.push((op, req));
                self.nudge(now, out);
            }
            Control::Abandon { target, force } => {
                self.on_control_abandon(now, op, target, force, replica, out)
            }
            Control::DropHeld {
                ino,
                remote,
                sessions,
            } => self.on_control_drop_held(now, op, ino, remote, sessions, replica, out),
            Control::Reintegrate => {
                if replica.lost_persisted() && !self.lease.lost {
                    self.lease.force_lost();
                    // As every other end of the holder's tenure (plan 43
                    // C2f review, D5): the fast path is told first that this
                    // node may not answer its own clients alone, and the
                    // replica stops being the holder's.
                    self.clear_holder_epoch(replica);
                    self.fail_barriers(out);
                    self.drop_unexecuted_parks(now, replica, out);
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
            Control::ReadIndex {
                ino,
                dir,
                name,
                ahead,
            } => self.on_read_index_control(now, op, ino, dir, name, ahead, replica, out),
            Control::Recall { inos } => self.on_recall_control(now, op, inos, replica, out),
            Control::Lock {
                ino,
                mode,
                blocking,
                waited_ms,
            } => self.on_lock_control(now, op, ino, mode, blocking, waited_ms, replica, out),
            Control::LockIdle { ino } => self.on_lock_idle_control(now, op, ino, replica, out),
            Control::LockReleaseWake { ino } => {
                self.on_lock_release_wake(now, op, ino, replica, out)
            }
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
                self.pr.epoch_members.clone_from(&members);
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
            Control::Delegate { dir, node, sticky } => {
                self.on_control_delegate(now, op, dir, node, sticky, replica, out)
            }
            Control::Undelegate { dir } => self.on_control_undelegate(now, op, dir, replica, out),
            Control::ReleaseGrants => self.on_control_release_grants(now, op, replica, out),
            Control::StickyRelease { dirs, wait } => {
                self.on_control_sticky_release(now, op, dirs, wait, replica, out)
            }
            Control::LeaveFailed => {
                if self.dl.leave_self {
                    self.leave_self_abort(now, out);
                }
                out.push(Action::ControlDone {
                    op,
                    result: Ok(ControlOk::Done),
                });
            }
            Control::EndGrantsOf { node } => {
                self.on_control_end_grants_of(now, op, node, replica, out)
            }
            Control::Retire => {
                self.on_retire(now, replica, out);
                out.push(Action::ControlDone {
                    op,
                    result: Ok(ControlOk::Done),
                });
            }
            Control::Authority {
                forward_only,
                suspended,
            } => {
                self.set_authority_mode(
                    now,
                    AuthorityMode {
                        forward_only,
                        suspended,
                    },
                    out,
                );
                out.push(Action::ControlDone {
                    op,
                    result: Ok(ControlOk::Done),
                });
            }
        }
    }

    /// Plan 31 C8 (`mode.rs`): a forward-only holder gives the lease back
    /// once idle (the round's release check, `LeaseState::wants_handoff`);
    /// a round soon lets it do so without waiting for the next poll.
    fn set_authority_mode(&mut self, now: Now, mode: AuthorityMode, out: &mut Vec<Action>) {
        if mode == self.mode {
            return;
        }
        tracing::info!(
            node = self.cfg.node_id,
            forward_only = mode.forward_only,
            suspended = mode.suspended,
            "authority mode"
        );
        self.mode = mode;
        self.lease.release_when_idle = mode.forwards();
        if mode.forwards() && self.lease.held.is_some() {
            self.nudge(now, out);
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
        now: Now,
        state: EpochState,
        members: &[NodeId],
        stale_below: Epoch,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let before = self.epoch;
        self.epoch = state;
        // Chunk epoch-liveness-gap: the member took its carrier's fresh
        // proposal in place of the active epoch (`epoch_replacing`).
        let replaced = state.open && !state.active && !state.frozen && before.active;
        let was_replacing = self.epoch_replacing;
        if replaced {
            self.epoch_replacing = true;
        } else if state.active || !state.open {
            self.epoch_replacing = false;
        }
        if state.open != before.open {
            self.epoch_probes = 0;
            self.epoch_s3_down = false;
        }
        tracing::debug!(
            node = self.cfg.node_id,
            open = state.open,
            active = state.active,
            frozen = state.frozen,
            flushing = state.flushing,
            carried = ?self.pr.carried,
            held = ?self.lease.held.as_ref().map(|(l, _)| (l.epoch, l.expires_unix_ms)),
            hold = ?self.lease.epoch_hold(),
            lost = self.lease.lost,
            "epoch state"
        );
        if !replaced {
            // Replaced, the delegates stay stopped through the transient
            // state: the next activation finds them so.
            self.deleg_on_epoch(now, state.active && !state.frozen, replica, out);
        }
        if state.active && !before.active {
            let closed_carried = self.epoch_closed_carried();
            let alone = members.iter().all(|m| *m == self.cfg.node_id);
            if closed_carried.is_some() && !alone && self.epoch_closed_claim_in_doubt() {
                // The activation carries the lease this node's close let
                // go, but a CAS of its own replacing it is out (or wrote
                // another lease epoch): it may land and move the object
                // past the carried one, and the other members close once
                // it has (`EpochClaimView::claim`; chunk
                // epoch-liveness-gap, `locks-blips-tight-faults` seed
                // 409: the CAS landed after the hold was taken, the
                // members closed and took the lease over at its expiry,
                // and the hold owner, crashed meanwhile, re-adopted its
                // persisted hold). No hold: this node owes the epoch's
                // close and the lease's re-claim instead (`owes_move`),
                // as for a lease it was releasing at the activation.
                self.owe_epoch_move(replica);
            } else if let Some((lease, tag)) = closed_carried {
                // The activation carries the lease this node's last
                // epoch close let go, claimed while its re-claim was
                // pending (`epoch_closed_claim`): it stood in S3 all
                // along, this node's, and nobody else held it since (it
                // is the claim at the highest epoch, and taking it needs
                // a CAS on that very object, or this epoch's members'
                // promises). Held again as before the close, the epoch
                // carries it below as it carries any holder's, and the
                // hold continues the tenure and its lock grants
                // (`epoch_tenure_resumed`). Claiming nothing, the epoch
                // carried no lease and refused every write (`EROFS`).
                tracing::info!(
                    node = self.cfg.node_id,
                    epoch = lease.epoch,
                    "the continuation epoch carries the lease its predecessor's close let go: \
                     holding it again"
                );
                if self.hold_ended_is(lease.epoch, lease.expires_unix_ms)
                    && replica.persist_epoch_hold_ended(None).is_ok()
                {
                    self.pr.hold_ended = None;
                }
                // The tenure continues under the lease it held: whoever
                // wrote its mark is not known here, so it is its own.
                self.ack.mark_own = lease.granted_delegations;
                self.ack.mark_inherited = false;
                self.lease.adopt(now, lease, tag, None);
                self.sync_cfg_spans(replica);
                self.stats.epoch_closed_leases_reheld += 1;
            }
            self.skip_ship = true;
            // Plan 30 §M10's claim resolution: a member whose lease claim
            // is below an epoch another member knows of was taken over
            // and has not heard yet — deposed, not carried.
            if !self.lease.epoch_held() && self.epoch_claim_stale(stale_below) {
                self.stats.epoch_stale_claims += 1;
                let mine = self.lease.epoch().unwrap_or(0);
                self.deposed(now, 0, stale_below, mine, replica, out);
            } else if !self.lease.lost && self.epoch_carries_let_go() {
                // Flex-crash seed 16364: the activation carries the lease
                // this node is RELEASING (it acked while holding it, then
                // its idle release began before the activation arrived:
                // a paused node, its events late), or has released or
                // given up in doubt since. The release CAS may land (or
                // has), and a released lease is anyone's without
                // promises, so no hold may stand on it. Adopting it
                // anyway, the release's in-doubt give-up wiped the hold,
                // nobody held the epoch, and its members waited for the
                // carried lease to move for good. This node owes that
                // move instead (`owes_move`): it closes its epoch once S3
                // is back, and re-claims the lease if the release did not
                // land (`epoch_carrier_checked`, `epoch_reclaim_due`); or
                // it holds the epoch after all if the release ends with
                // the lease kept (`epoch_hold_after_kept_release`).
                //
                // Not the lease an epoch's close let go, its re-claim
                // pending (the block above): that lease was never
                // released (no release CAS), stands in S3 as this node's,
                // and is held again here as before the close.
                self.owe_epoch_move(replica);
            } else if !self.lease.lost && self.carries_mine() && self.epoch_may_carry(members) {
                // The activation carries exactly this node's lease (the
                // claim it acked: same epoch and expiry, `carries_mine`).
                // Adopted whether or not the lease is still inside its
                // usable window *now*: the claim was usable when acked,
                // and the activation can arrive after the expiry margin
                // (`epoch-member-lost`: 40 ms late, the hold was never
                // adopted, the epoch carried a lease nobody held, and
                // every write in it failed). Safe for the reason the
                // restart path below gives: nobody else can hold the
                // carried lease — the members promise nothing while the
                // epoch is open, so no taker gets past the promise check.
                let epoch = self.lease.epoch().unwrap_or(1);
                self.adopt_epoch_hold(now, epoch, replica);
            } else if self
                .epoch_closed_claim(now)
                .is_some_and(|(l, _)| l.epoch < stale_below)
            {
                // A member knows a later epoch: the lease the close let go
                // was taken over meanwhile, and its tenure is over.
                self.epoch_tenure_resumed(None, false, replica);
            }
        } else if state.active
            && self.lease.usable(now, &self.cfg)
            && !self.lease.releasing
            && !self.lease.epoch_held()
            && self.carries_mine()
            && self.epoch_may_carry(members)
        {
            // Re-affirming (a holder carrying S3 authority through an
            // epoch that was already active when it acquired).
            let epoch = self.lease.epoch().unwrap_or(1);
            self.adopt_epoch_hold(now, epoch, replica);
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
            // Not a hold this node already owned and let go. Flex-crash
            // seed 166: it gave its journal-less hold to node 1, which
            // journaled under it and crashed; the freeze re-reported the
            // epoch and this path adopted the carried hold a second time,
            // flushed nothing, closed the epoch and released the lease,
            // and node 1's acknowledged journal met a newer epoch. Seed
            // 2236: a report after its own close adopted the hold again
            // beside the next S3 holder.
            let ended = |c: &crate::event::Carrier| self.hold_ended_is(c.epoch, c.expires_unix_ms);
            // Nor one whose lease this node released (or is releasing,
            // or issued the release CAS for before a restart): that
            // hold's move is owed instead (`owes_move`).
            let let_go = |c: &crate::event::Carrier| {
                let it = Some((c.epoch, c.expires_unix_ms));
                self.pr.owes_move == it || self.pr.released == it
            };
            let carried = self
                .pr
                .carried
                .filter(|c| c.node == self.cfg.node_id && !ended(c));
            if carried.is_some_and(|c| let_go(&c)) {
                self.owe_epoch_move(replica);
            } else if let Some(c) = carried {
                tracing::info!(
                    node = self.cfg.node_id,
                    epoch = c.epoch,
                    "adopting the epoch hold the activation carried for this node's lease \
                     (restarted into the open epoch before the hold was persisted)"
                );
                self.stats.epoch_holds_adopted_late += 1;
                self.skip_ship = true;
                self.adopt_epoch_hold(now, c.epoch, replica);
            }
        }
        self.restore_epoch_hold(now, replica);
        if !state.open {
            // The epoch closed: no move is owed any more (this node's own
            // close made it a re-claim, `end_epoch_hold`; anyone else's
            // means the carried lease moved).
            self.settle_owed_move(replica);
        }
        if !state.open && before.open {
            // Plan 30 §M9/§M10 rule (b): a member backup ran no seal
            // watch while its epoch was open; it resumes now, with a
            // fresh silence window for the holder's re-acquisition.
            self.backup_watch_after_epoch(now, out);
        }
        if (!state.active && before.active && !state.frozen && !replaced)
            || (!state.open && was_replacing)
        {
            self.skip_ship = false;
            self.epoch_close_release(now, replica, out);
            self.nudge(now, out);
        }
        if !state.open && !state.flushing {
            self.skip_ship = false;
        }
        if self.epoch_refuses_writes() {
            // A frozen epoch, or one whose claim resolution carried no
            // lease (no hold owner: chunk close-stall-metered's review
            // must-fix 2), refuses writes (EROFS): every op waiting for
            // the lease hears it now rather than at its 2 × TTL deadline.
            // Idempotent: a repeated report finds nothing waiting.
            self.refuse_waiting_for_lease(now, Code::ReadOnly, replica, out);
        }
    }

    // ---- ids, timers, the poll ----

    pub(crate) fn op_id(&mut self) -> OpId {
        let id = OpId(self.next_op);
        self.next_op += 1;
        id
    }

    /// Plan 43 C7-H1: a timer is relative. The core computes the time
    /// left on the clock its deadline is on, against the same clock's
    /// reading of this step, and the driver sleeps that long on the
    /// monotonic clock: a wall step between now and the firing moves
    /// neither a wait nor an honour window. A handler re-checks its
    /// deadline against the step's reading and re-arms (the sleep ends at
    /// or after the deadline up to the rate error, never by a step).
    fn arm_timer(
        &mut self,
        at: TimerAt,
        remaining_ms: i64,
        timer: Timer,
        out: &mut Vec<Action>,
    ) -> TimerId {
        let id = TimerId(self.next_timer);
        self.next_timer += 1;
        self.timers.insert(id, (timer, at));
        out.push(Action::SetTimer {
            id,
            after_ms: remaining_ms.max(0) as u64,
            kind: timer.kind(),
        });
        id
    }

    /// A timer at a WAIT deadline: a wait-side or liveness stamp's end.
    fn set_timer_wait(&mut self, at: WaitMs, timer: Timer, out: &mut Vec<Action>) -> TimerId {
        let remaining = at.since(self.clk.wait);
        self.arm_timer(TimerAt::Wait(at), remaining, timer, out)
    }

    /// A timer at a HONOUR deadline: a holder's own window (a renewal
    /// tick, a delegation's lapse). It can fire late after a forward step
    /// or a suspend, which costs liveness only: every honour check is made
    /// where the right is used, not by the timer.
    fn set_timer_honour(&mut self, at: HonourMs, timer: Timer, out: &mut Vec<Action>) -> TimerId {
        let remaining = at.since(self.clk.honour);
        self.arm_timer(TimerAt::Honour(at), remaining, timer, out)
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
        self.ack.s3_sent.insert(op, self.clk.honour);
        out.push(Action::S3 { op, req });
        op
    }

    /// (Re)arm the periodic poll `delay_ms` from now, replacing a pending
    /// one (`poll.as_mut().reset(..)`).
    fn arm_poll(&mut self, now: Now, delay_ms: u64, out: &mut Vec<Action>) {
        if let Some(id) = self.poll_timer.take() {
            self.cancel_timer(id, out);
        }
        let id = self.set_timer_wait(now.wait.plus(delay_ms), Timer::Poll, out);
        self.poll_timer = Some(id);
    }

    fn arm_drain(&mut self, now: Now, out: &mut Vec<Action>) {
        if self.drain_timer.is_some() {
            return;
        }
        let id = self.set_timer_wait(
            now.wait.plus(self.cfg.replay_drain_ms),
            Timer::ReplayDrain,
            out,
        );
        self.drain_timer = Some(id);
    }

    /// Run a round as soon as possible (`SyncRequest::Nudge`). A round
    /// already in flight is not cancelled — the M2b rule — it is followed
    /// by another.
    pub(crate) fn nudge(&mut self, now: Now, out: &mut Vec<Action>) {
        if self.job.is_some() {
            self.nudged = true;
            return;
        }
        self.idle_rounds = 0;
        self.arm_poll(now, 0, out);
    }

    /// The periodic poll fired: run a round unless a job holds the slot,
    /// in which case the round follows it.
    fn on_poll(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.epoch.open {
            self.epoch_probes = self.epoch_probes.saturating_add(1);
        }
        if self.job.is_some() {
            self.nudged = true;
            return;
        }
        self.start_round(now, replica, out);
    }

    /// `next_poll_ms`: doubling idle backoff, capped by the ceiling and,
    /// while holding, by a quarter of the TTL so renewal is never slept
    /// through (`lease_poll_cap_ms`); M13: by the hot tail interval
    /// while an op waits on its inbox outcome. While a continuation epoch
    /// is open its rounds are the probes that close it once S3 is back:
    /// the sync interval at first, doubling to at most four times it
    /// (500 ms → 2 s by default), and the sync interval again once S3
    /// answers after a failure (`note_epoch_probe`). Backed off as idle
    /// rounds, a member with nothing to ship (no hold, or an epoch that
    /// carries no lease and refuses writes with `EROFS`) probed only
    /// every `idle_max_ms` (10 s), still refusing writes that long after
    /// S3 returned (`stress-ng-fs-faults`: a create after the run failed
    /// `EROFS`); never backed off, a frozen epoch whose dead member
    /// waits for an admin probed twice a second for hours. A held
    /// lease's poll also fires when its renewal falls due
    /// (`renew_due_in_ms`): with only the quarter-TTL cap the renewal
    /// went out as late as a quarter TTL before the expiry, and the root
    /// capped every delegation, and through it every delegated lock
    /// grant, by that short remainder (`locks-delegated` seed 1432: a
    /// grant renewed for 501 ms with a 500 ms margin lapsed under its
    /// holder's I/O).
    pub(crate) fn next_poll_ms(&self, now: Now) -> u64 {
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
        if self.epoch.open {
            let probe = base
                .saturating_mul(1u64 << self.epoch_probes.saturating_sub(1).min(2))
                .min(base.saturating_mul(4));
            next = probe;
            if self.lease.ship_epoch(now, &self.cfg).is_some() {
                next = next.min((self.cfg.ttl_ms / 4).max(1));
            }
            if !self.inbox.pending.is_empty() {
                next = next.min(self.cfg.inbox_tail_ms.max(1));
            }
        }
        if let Some(due) = self.renew_due_in_ms(now) {
            next = next.min(due.max(1));
        }
        next
    }

    /// An S3 result while a continuation epoch is open: a failure marks
    /// S3 away; the first success after one is the evidence that S3 is
    /// back, and the probes return to the sync interval for the close.
    fn note_epoch_probe(&mut self, result: &S3Result) {
        if !self.epoch.open {
            return;
        }
        let failed = match result {
            S3Result::LeaseGet(r) => r.is_err(),
            S3Result::LeasePut(r) => matches!(r, Err(CasFailure::Failed(_))),
            S3Result::SegmentPut(r) | S3Result::InboxPut(r) => {
                matches!(r, Err(CasFailure::Failed(_)))
            }
            S3Result::SegmentRun(r) => r.is_err(),
            S3Result::SegmentGap(r) => r.is_err(),
            S3Result::InboxRun(r) | S3Result::InboxDrain(r) => r.is_err(),
            S3Result::InboxDelete(r) | S3Result::InboxTombstone(r) => r.is_err(),
            S3Result::InboxLastN(r) => r.is_err(),
            S3Result::Heartbeats(r) => r.is_err(),
            S3Result::HeartbeatPut(r) => r.is_err(),
        };
        if failed {
            self.epoch_s3_down = true;
        } else if self.epoch_s3_down {
            self.epoch_s3_down = false;
            self.epoch_probes = 0;
        }
    }
}
