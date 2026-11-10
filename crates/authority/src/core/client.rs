//! A local client op from `Event::Submit` to `Action::Reply`: the local
//! fast path, the forward with its same-rid retries and one redirect,
//! M13's inbox when there is no P2P path, the lease path with its in-doubt
//! resolution, the read-your-refusal wait and the deadline (what
//! `view::View::mutate_op_rebasable` + `node_runtime::dispatch_forward` +
//! `forward::request_mutate_with` + `inbox::forward_via_inbox` did).
//!
//! A stranded op's replay (`recovery::drain_pending_replays`) rides the
//! same machine under `Origin::Replay`: it reaches the holder the same way
//! and gets the same retries, but its outcome goes to the replay drain,
//! never to a client.

use super::backup::is_answer;
use super::holder::{Fence, TOUCHED_UNKNOWN};
use super::{ack_counter, Core, S3For, Timer};
use crate::action::{Action, AnswerSource, ClientReply, S3Op};
use crate::event::{PeerMsg, Policy, S3Result};
use crate::ids::{Epoch, HonourMs, NodeId, Now, OpId, WaitMs};
use crate::replica::Replica;
use constellation_fs_core::Ino;
use constellation_meta::delegation::Ownership;
use constellation_meta::locks::LockTag;
use constellation_meta::{
    CompletedOutcome, KeySet, LogRecord, MetaError, MutateOp, MutateOutcome, OwnChunks, Position,
    Rid, Streams, TouchSet,
};
use constellation_types::Code;
use std::collections::BTreeSet;

/// How many `InDoubt` rids the core remembers for their resubmission.
/// Known I1 limit: a rid evicted past this bound is resubmitted without the
/// marks it carried (`sent_to_delegate`, `redirected_to_designee`), so a
/// refusal of an op a designee ran is no longer held (ADR-45); the same
/// eviction already allows a second execution of an ordinary in-doubt rid.
pub(super) const MAX_IN_DOUBT_RIDS: usize = 10_000;

/// Who submitted the op and therefore where its outcome goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Origin {
    Client,
    /// A queued stranded op (`pending_replay` row `queue_seq`).
    Replay {
        queue_seq: u64,
        /// Plan 30 §M11: the delegate-stream rows the stranded
        /// transaction was ordered after (`StrandedOp::deps`).
        deps: Streams,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Phase {
    /// Behind an earlier op of this node that touches a common inode
    /// (plan 29 M5's `KeyGate`): issued only once that op's outcome has
    /// landed here, so two replies can never install out of order.
    Gated,
    /// Reading the lease object to learn the holder.
    LearnHolder,
    /// Forwarded to `holder`; waiting for the reply or the timeout.
    Forwarded { req: OpId, holder: NodeId },
    /// Between two forward attempts.
    Backoff,
    /// M13: queued for the submitter's next batch under `epoch`.
    InboxQueued { epoch: Epoch },
    /// M13: durable in the holder's inbox; the outcome arrives through
    /// the log.
    InboxWaiting { epoch: Epoch },
    /// M13: the op left the inbox path with its batch still durable and
    /// is about to be forwarded to `holder`; the batch is being deleted
    /// first, so no later takeover drain can execute it a second time
    /// (a P2P refusal leaves no `completed` witness to dedup against).
    InboxWithdraw { holder: NodeId, gen: u64 },
    /// Waiting for the acquire job to give this node the lease.
    WaitingLease,
    /// The acquire job answered busy; retrying after a backoff.
    AcquireRetry,
    /// Accepted by the holder on a base this replica has not applied
    /// (`PeerMsg::MutateReply::base`): the records arrive through the
    /// log; answered when the rid's completion is applied. `position` is
    /// the reply's (plan 30 §M6), raised into `observed` then.
    AwaitingLog { epoch: Epoch, position: Position },
    /// Plan 30 §M8: executed here as the sequencer; the reply waits for
    /// read delegations on what it touched to be recalled.
    Recalling,
}

/// The externally visible phase, for tests and `status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientPhase {
    Gated,
    LearnHolder,
    Forwarded,
    Backoff,
    InboxQueued,
    InboxWaiting,
    InboxWithdraw,
    WaitingLease,
    AcquireRetry,
    AwaitingLog,
    Recalling,
    /// Plan 43 C2 (§5.3 item 5): its own attempts reached their deadline;
    /// a settle of its rid is on its way (whatever route it takes).
    Settling,
}

#[derive(Debug)]
pub(crate) struct ClientOp {
    pub op: MutateOp,
    pub origin: Origin,
    pub policy: Policy,
    pub phase: Phase,
    /// Submission order, for the gate.
    pub order: u64,
    /// The inodes the op reads or writes (`forward::conflict_keys`).
    pub keys: TouchSet,
    pub attempts: u32,
    /// `NotHolder` redirects followed (plan 30 §M11: two, since a
    /// delegate's names the root and the root's a delegate).
    pub redirected: u32,
    /// The op was sent to a node that holds a delegation (the table named it
    /// the owner of the op's keys, or the root redirected the op to it): it
    /// may have run there, whatever the root says later.
    pub sent_to_delegate: bool,
    /// The root redirected the op to a node this node's table does not list
    /// as a delegate (the table lags the root's journal): the op was sent to
    /// a designee all the same, whatever the keys resolve to here.
    pub redirected_to_designee: bool,
    /// A non-stealable holder's refusal was held back for this op (it was
    /// sent on, so it may have run): its in-doubt end is I4's exception for
    /// an unreachable non-stealable holder (`unsettled_nonstealable_holder`).
    pub designation_held: bool,
    /// A delegate executed the op and said so (`Accepted` with its
    /// generation): what the op still lacks is the commit of the delegate's
    /// rows through the root (it awaits the log). The sim judges its
    /// unsettled `EIO` against a root, not against the delegate it already
    /// heard (`Core::take_unsettled_accepted`).
    pub accepted_by_delegate: bool,
    /// Plan 46 M0.3 (B): the root that last answered this op `Held` (it is
    /// alive and holds the op by design): the backoff retry goes back to
    /// it when this node's cached holder was dropped meanwhile.
    /// (ADR-47: with the generation to route the retry by; `0`: the lease
    /// holder.)
    pub held_by: Option<(NodeId, u64)>,
    /// ADR-47: the highest non-zero generation this rid was ever sent under
    /// or offered to locally (`0`: never; `TOUCHED_UNKNOWN`: cannot know),
    /// monotone over the rid's life on this node; raised before the send or
    /// the admission it describes, carried on every forward and into the
    /// inbox batch.
    pub touched_gen: u64,
    /// ADR-47: the rid was queued for an inbox batch at some point (or a
    /// resubmission of a rid that was).
    #[allow(dead_code)]
    pub inbox_ever: bool,
    /// ADR-47: `Fenced` answers followed with a resend routed by the
    /// answering generation, since the last reset of `attempts`; past two
    /// the op is routed afresh.
    pub fence_bounces: u32,
    /// Sent to a holder at least once (or into an inbox): it may have
    /// taken effect there, so every later local execution first checks
    /// `completed`.
    pub forwarded: bool,
    pub acquire_retries: u32,
    pub deadline: crate::ids::TimerId,
    /// Plan 43 C7b: the deadline was moved once to cover this node's own
    /// claim of the lease ([`Core::on_client_deadline_timer`]).
    pub deadline_extended: bool,
    pub timer: Option<crate::ids::TimerId>,
    /// M13: when the op entered the inbox path (its in-doubt deadline
    /// counts from here) and when its batch became durable.
    pub inbox_since: Option<WaitMs>,
    pub inbox_durable_at: Option<WaitMs>,
    /// Every durable batch this rid was submitted in and may still sit
    /// in (plan 30 M5 round 4; seed 10476 of the long configuration,
    /// found by the M8 coder and bisected by the M7 coder). One op can
    /// be submitted more than once under the same epoch — its inbox
    /// deadline sends it down the lease path, the acquisition loses to
    /// the live holder, and `on_acquire_retry` routes it back through
    /// the inbox — and each submission is its own batch. All of them
    /// must be withdrawn before a P2P forward: a forgotten one is
    /// drained by the next takeover after the client was already
    /// answered from the P2P reply.
    pub inbox_keys: Vec<constellation_store_s3::inbox::InboxKey>,
    /// Plan 30 §M11: what this node had observed when the op was
    /// submitted — the op's causal dependencies, carried on every forward.
    pub deps: Position,
    /// A replay (`Origin::Replay`) whose rid a client resubmitted while
    /// it ran (after hearing `InDoubt`): the replay's outcome answers
    /// that client too (long-acks3 seed 50557: the resubmission was
    /// dropped as a duplicate and never answered).
    pub also_client: bool,
    /// Held by `hold_for_lost_deps` at least once: its `deps` are taken
    /// afresh before it proceeds, so they order after the replays it
    /// waited for.
    pub deps_held: bool,
    /// Plan 30 §M14 phase 2: the op's fencing token, carried on every
    /// forward and inbox batch and checked by every local execution.
    pub tag: LockTag,
    /// When the op was submitted, and the states it went through since
    /// (`(ms after submission, state)`, at most [`HISTORY_CAP`]): a slow
    /// op is logged with them when it finishes (`finish`).
    pub submitted: WaitMs,
    pub history: Vec<(i64, String)>,
    /// Chunk close-stall-metered: the safety timer of
    /// `Core::await_own_records` (cancelled when the op finishes), the
    /// inodes whose pending chunks here the op's transaction waits for
    /// (as its sequencers named them), and whether this op already
    /// uploaded them all (`Action::UploadAwaited`).
    pub own_record_timer: Option<crate::ids::TimerId>,
    pub own_record_inos: BTreeSet<Ino>,
    pub own_record_uploaded: bool,
    /// Chunk metered-own-rows: the journal rows of the reply's position
    /// this op's observation does not owe (`Core::settle_own_rows`): an
    /// `AwaitingLog` op observes its position once the log answered it.
    pub excused: Vec<(u64, u64)>,
    /// Plan 43 C2a: who decided the outcome this op will be answered with,
    /// set by whatever decides it (an execution here, a reply passed on, a
    /// row read from the log) just before `finish`. Unset when `finish`
    /// emits a definitive outcome is a bug.
    pub answered_by: Option<AnswerSource>,
    /// Plan 43 C2 (§5.3 item 5): the op's own attempts reached their
    /// deadline and it is being *settled*: every forward, inbox batch and
    /// lease-path execution of it from now on asks for the rid's outcome,
    /// or for its refusal for good, instead of executing it.
    pub settle: bool,
    /// Plan 46 M1.2 step 13: while `settle`, when the settle's second
    /// deadline fires (the wait clock), and the latest it may be moved to
    /// (`settle_cap`: one reclaim horizon, a seal window and a settle window
    /// past the settle's start). Zero for a settle that started as one (a
    /// placeholder's): those are never moved.
    pub settle_until: WaitMs,
    pub settle_cap: WaitMs,
    /// ADR-44: this settle waits in `Backoff` for a recall this node's
    /// shippable gate sent on demand; the backoff routes it as a settle
    /// again (`settle_route`), where the gate answers it after the cut.
    pub gate_recall_wait: bool,
}

/// Chunk metered-own-rows: an observed position's pending upload
/// (`Core::repeat_observed_upload`).
///
/// `inos` accumulates across raises until the watermark is reached or
/// the repeats run out, and the timer is not cancelled on a role change:
/// both harmless, as a pass uploads only what is still pending here (an
/// index lookup per inode for the rest) and stops once `observed` is
/// reached.
#[derive(Debug, Default)]
pub(crate) struct ObservedUpload {
    pub inos: BTreeSet<Ino>,
    pub repeats: u32,
    pub timer: Option<crate::ids::TimerId>,
}

/// How many times an observed position's upload is repeated
/// (`own_record_wait_ms` apart) before it is left to the watermark's TTL.
pub(crate) const OBSERVED_UPLOAD_REPEATS: u32 = 6;

/// How many state changes a client op remembers for its slow-op log.
pub(crate) const HISTORY_CAP: usize = 24;

/// A client op slower than this is logged, with its states, when it
/// finishes (`CONSTELLATION_SLOW_OP_MS`, default 2000; 0 turns it off).
fn slow_op_ms() -> i64 {
    static MS: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *MS.get_or_init(|| {
        std::env::var("CONSTELLATION_SLOW_OP_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2000)
    })
}

impl ClientOp {
    pub fn phase_kind(&self) -> ClientPhase {
        if self.settle {
            return ClientPhase::Settling;
        }
        match self.phase {
            Phase::Gated => ClientPhase::Gated,
            Phase::LearnHolder => ClientPhase::LearnHolder,
            Phase::Forwarded { .. } => ClientPhase::Forwarded,
            Phase::Backoff => ClientPhase::Backoff,
            Phase::InboxQueued { .. } => ClientPhase::InboxQueued,
            Phase::InboxWaiting { .. } => ClientPhase::InboxWaiting,
            Phase::InboxWithdraw { .. } => ClientPhase::InboxWithdraw,
            Phase::WaitingLease => ClientPhase::WaitingLease,
            Phase::AcquireRetry => ClientPhase::AcquireRetry,
            Phase::AwaitingLog { .. } => ClientPhase::AwaitingLog,
            Phase::Recalling => ClientPhase::Recalling,
        }
    }
}

impl super::Core {
    /// Note every in-flight client op's state if it changed (run after
    /// each event): what [`Self::log_if_slow`] logs for a slow op.
    pub(crate) fn note_client_states(&mut self, now: Now) {
        if slow_op_ms() <= 0 || self.clients.is_empty() {
            return;
        }
        let pending_exec = &self.dl.pending_exec;
        let parked_local = &self.rd.parked_local;
        for (rid, c) in self.clients.iter_mut() {
            let mut state = format!("{:?}", c.phase_kind());
            if let Phase::Forwarded { holder, .. } = c.phase {
                state.push_str(&format!("->{holder}"));
            }
            if pending_exec.contains(rid) {
                state.push_str("+delegate-parked");
            }
            if parked_local.contains_key(rid) {
                state.push_str("+ack-parked");
            }
            if c.deps_held {
                state.push_str("+held-for-replays");
            }
            if c.history.last().is_none_or(|(_, s)| *s != state) && c.history.len() < HISTORY_CAP {
                c.history.push((now.wait.since(c.submitted), state));
            }
        }
    }

    /// A client op that took longer than `CONSTELLATION_SLOW_OP_MS` is
    /// logged with the states it went through.
    fn log_if_slow(&self, now: Now, rid: Rid, c: &ClientOp, outcome: &str, replica: &dyn Replica) {
        let took = now.wait.since(c.submitted);
        if slow_op_ms() <= 0 || took < slow_op_ms() {
            return;
        }
        tracing::warn!(
            node = self.cfg.node_id,
            ?rid,
            took_ms = took,
            op = ?c.op,
            outcome,
            attempts = c.attempts,
            redirected = c.redirected,
            deps = ?c.deps,
            applied = ?replica.applied_position(),
            history = ?c.history,
            "slow client op"
        );
    }
}

/// `forward::AckTracker`: the contiguous prefix of this incarnation's rid
/// seqs whose outcome the client has seen, sent as `acked_through` so the
/// holder can drop its `recent` entries.
#[derive(Debug, Default)]
pub(crate) struct AckTracker {
    floor: u64,
    done: BTreeSet<u64>,
}

impl AckTracker {
    pub fn mark_done(&mut self, seq: u64) {
        self.done.insert(seq);
        while self.done.remove(&(self.floor + 1)) {
            self.floor += 1;
        }
    }

    pub fn floor(&self) -> u64 {
        self.floor
    }
}

/// `forward::conflict_keys`: the keys `op` reads or writes, for the
/// ordering gate (plan 30 §M12: [`super::holder::keys_of_op_in`], with
/// an unlink's target exclusive too — a name that does not resolve
/// locally is left out; the dentry and the shared parent hold still
/// serialize two ops racing on an unresolved child).
pub(crate) fn conflict_keys(op: &MutateOp, replica: &dyn Replica) -> TouchSet {
    let mut set = super::holder::keys_of_op_in(op, replica);
    if let MutateOp::Unlink { parent, name } = op {
        if let Some(i) = replica.lookup_ino(*parent, name) {
            set.inos.insert(i);
        }
    }
    set
}

/// `forward::named_child`.
pub(crate) fn named_child(op: &MutateOp) -> Option<(Ino, &str)> {
    match op {
        MutateOp::Mkdir { parent, name, .. }
        | MutateOp::Create { parent, name, .. }
        | MutateOp::Symlink { parent, name, .. }
        | MutateOp::Mknod { parent, name, .. }
        | MutateOp::Link { parent, name, .. } => Some((*parent, name.as_str())),
        _ => None,
    }
}

/// The code a refusal for an ended lock grant (`LockLapsed`) is journaled
/// under on the P2P paths (`ENOLCK`, which no mutation refuses with
/// otherwise), so a dedup of the rid answers `LockLapsed` again: a client
/// re-sends under a fresh token while it still holds the grant, or keeps
/// its refused commit as a conflict copy (`refused_as_outcome`).
pub(crate) const LAPSED_CODE: Code = Code::NoLock;

/// Plan 43 C2 (§5.3 item 5, D2): the code a settled op's refusal is
/// journaled under (`Refused { rid, code: ABANDONED_CODE }`): `ECANCELED`,
/// which no mutation refuses with otherwise, so a dedup of the rid answers
/// [`MutateOutcome::Abandoned`] again (`refused_as_outcome`). Not a new
/// portable code: the errno table has no hole, and the client hears `EIO`.
pub const ABANDONED_CODE: Code = Code::Canceled;

/// The inert op of a settle of a rid `repair drop-held` dropped: the
/// refusal the settle journals if the rid has no outcome (plan 46 M0.2
/// follow-up, defect C), spelled as a record. The op travels with every
/// settle request, wherever it goes (the holder, a delegate, an inbox
/// batch); it executes nothing and touches no key.
pub(crate) fn dropped_settle_op(rid: Rid) -> MutateOp {
    MutateOp::Records {
        records: vec![LogRecord::Refused {
            rid,
            code: constellation_meta::DROPPED_CODE,
        }],
    }
}

/// The code a settle of `rid` journals when the rid has no outcome:
/// [`ABANDONED_CODE`] (a requester's op nobody heard of), or the dropped
/// code when the settle's op says so ([`dropped_settle_op`]: the op was
/// acknowledged and then dropped by an operator, and its conflict copy is
/// made once the refusal is the rid's outcome).
pub(crate) fn settle_code(rid: Rid, op: Option<&MutateOp>) -> Code {
    match op {
        Some(MutateOp::Records { records }) => match records.as_slice() {
            [LogRecord::Refused { rid: r, code }] if *r == rid => *code,
            _ => ABANDONED_CODE,
        },
        _ => ABANDONED_CODE,
    }
}

/// A journaled refusal (`Refused { rid, code }`) as the outcome the
/// sequencer gave: a stale base is the `Conflict` a client rebases on, an
/// ended grant `LockLapsed`, a settled op `Abandoned`, anything else its
/// errno.
pub(crate) fn refused_as_outcome(code: Code) -> MutateOutcome {
    match code {
        Code::Stale => MutateOutcome::Conflict { manifest: None },
        LAPSED_CODE => MutateOutcome::LockLapsed,
        ABANDONED_CODE => MutateOutcome::Abandoned,
        code => MutateOutcome::Errno(code),
    }
}

/// What the local log already says about `rid`, as the outcome a
/// forward would have produced (`inbox::local_outcome`).
pub(crate) fn completed_as_outcome(
    replica: &dyn Replica,
    rid: Rid,
    epoch: Epoch,
) -> Option<MutateOutcome> {
    match replica.completed_outcome(rid).ok().flatten()? {
        CompletedOutcome::Executed { .. } => Some(MutateOutcome::Accepted {
            epoch,
            records: Vec::new(),
        }),
        CompletedOutcome::Refused { code } => Some(refused_as_outcome(code)),
    }
}

/// Whether this node's unshipped journal holds `rid`'s outcome (its
/// `Completed` or `Refused` record).
fn journal_carries_outcome(replica: &dyn Replica, rid: Rid) -> bool {
    replica
        .journal_txs_from(0, usize::MAX)
        .iter()
        .flat_map(|tx| &tx.records)
        .any(|rec| {
            matches!(rec, LogRecord::Completed { rid: r } | LogRecord::Refused { rid: r, .. } if *r == rid)
        })
}

/// The rows of `streams` whose generation is live in the delegation table
/// this replica holds from the log. A generation the table does not list
/// has ended (its `DelegateNodeEnded` is applied: its rows are in the log below the
/// cut, or lost with it) or was never in the log (a deposed root's
/// delegation that did not ship): the rows named in it can only be met or
/// are never coming, so nobody waits for them.
fn live_rows(streams: &Streams, replica: &dyn Replica) -> Streams {
    let table = replica.delegation_table();
    let mut live = Streams::NONE;
    for (gen, idx) in streams.iter() {
        if idx > 0 && table.has_gen(gen) {
            live.raise(gen, idx);
        }
    }
    live
}

impl Core {
    // ---- entry ----

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_submit(
        &mut self,
        now: Now,
        rid: Rid,
        op: MutateOp,
        policy: Policy,
        tag: LockTag,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.submit(now, rid, op, policy, tag, Origin::Client, replica, out);
    }

    /// Plan 30 §M9 (`Event::SubmitInDoubt`): `rid` is a resubmission of
    /// an op this node may have executed. If the rid's op is live here (the
    /// deposition's queue entry for the same write), that op is marked in
    /// doubt: it reads `completed` before any local execution instead of
    /// running as one never sent. Otherwise the next `submit` of `rid`
    /// starts in doubt (see `in_doubt_rids`).
    ///
    /// `load-probe-takeover` seed 46: a fast-path write resubmitted when
    /// its acknowledgement wait ended `DurableWait::Lost` was also the
    /// queue entry; the takeover gate replayed that entry (`Completed`),
    /// and the waiting op, not in doubt, executed a second time: refused
    /// (`EEXIST`, `ENOENT`), journaled as the rid's second outcome and
    /// answered to the application.
    pub(crate) fn note_in_doubt(&mut self, rid: Rid) {
        // ADR-47: executed locally under no generation this node can name
        // (the fast path): its send history is unknown.
        if let Some(c) = self.clients.get_mut(&rid) {
            c.forwarded = true;
            c.touched_gen = TOUCHED_UNKNOWN;
            return;
        }
        self.in_doubt_rids.insert(rid);
        self.in_doubt_touched.insert(rid, TOUCHED_UNKNOWN);
        self.evict_in_doubt();
    }

    /// Keeps `in_doubt_rids` within `MAX_IN_DOUBT_RIDS`: the lowest rid goes,
    /// with what was remembered of it. An own rid evicted raises the
    /// watermark (ADR-47): every own rid at or below it is submitted with an
    /// unknown send history from then on.
    fn evict_in_doubt(&mut self) {
        while self.in_doubt_rids.len() > MAX_IN_DOUBT_RIDS {
            let first = *self.in_doubt_rids.iter().next().expect("non-empty");
            self.in_doubt_rids.remove(&first);
            self.in_doubt_batches.remove(&first);
            self.in_doubt_designee_rids.remove(&first);
            self.in_doubt_touched.remove(&first);
            if first.node == self.cfg.node_id {
                self.in_doubt_evicted_max = self.in_doubt_evicted_max.max(Some(first));
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn submit(
        &mut self,
        now: Now,
        rid: Rid,
        op: MutateOp,
        policy: Policy,
        tag: LockTag,
        origin: Origin,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.submit_as(now, rid, op, policy, tag, (origin, false), replica, out);
    }

    /// Plan 46 M0.2 (R5): settle the rid of the placeholder at `queue_seq`
    /// (`Core::settle_placeholder_if_due`): a client entry under the rid
    /// that is a *settle* from its first step — it asks for the rid's
    /// outcome or its refusal for good, wherever the op would have gone,
    /// and executes nothing (its op is inert and `settle` is set before
    /// anything routes it). Its outcome reaches `replay_outcome` as a
    /// replay's does.
    ///
    /// `op`: the inert op the settle carries: empty for a placeholder's
    /// (the refusal is the `ABANDONED_CODE` one), [`dropped_settle_op`] for
    /// a refused entry's (`Core::copy_after_settle`).
    pub(crate) fn submit_settle(
        &mut self,
        now: Now,
        (rid, queue_seq): (Rid, u64),
        op: MutateOp,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.submit_as(
            now,
            rid,
            op,
            Policy::Client,
            LockTag::NONE,
            (
                Origin::Replay {
                    queue_seq,
                    deps: Streams::NONE,
                },
                true,
            ),
            replica,
            out,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn submit_as(
        &mut self,
        now: Now,
        rid: Rid,
        op: MutateOp,
        policy: Policy,
        tag: LockTag,
        (origin, settle): (Origin, bool),
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.clients.contains_key(&rid) {
            let c = self.clients.get_mut(&rid).expect("present");
            if origin == Origin::Client && matches!(c.origin, Origin::Replay { .. }) {
                // The client retries a rid this node is replaying by rid
                // (it heard `InDoubt` for an earlier attempt, which the
                // takeover stranded): one execution, both answered.
                tracing::debug!(
                    node = self.cfg.node_id,
                    ?rid,
                    "a client resubmitted a rid under replay: the replay answers it"
                );
                c.also_client = true;
                return;
            }
            tracing::warn!(node = self.cfg.node_id, ?rid, "duplicate submit ignored");
            return;
        }
        // Plan 43 C2 (§5.3 item 5, Q9): a client op's own attempts end a
        // settle window before its deadline; the settle has the rest.
        let tries_for = if settle {
            // (A placeholder's settle runs for one settle window; unanswered,
            // it ends in doubt and the drain settles again.)
            self.dl_t().settle_window
        } else if Self::settles(origin, policy) {
            self.cfg.acquire_deadline_ms - self.dl_t().settle_window
        } else {
            self.cfg.acquire_deadline_ms
        };
        let deadline =
            self.set_timer_wait(now.wait.plus(tries_for), Timer::ClientDeadline(rid), out);
        let keys = conflict_keys(&op, replica);
        let mut deps = self.fresh_deps(now, replica);
        // Plan 30 §M11: a replay is ordered after this node's session
        // and after the delegate-stream rows its first execution was
        // (`StrandedOp::deps`). Those are kept when both do not fit.
        if let Origin::Replay { deps: queued, .. } = origin {
            deps.streams = queued.join(&deps.streams).unwrap_or(queued);
        }
        // A rid this core already answered `InDoubt` may have taken
        // effect somewhere: its resubmission is in doubt from the start.
        // So is a stranded op's replay (`Origin::Replay`): its first
        // execution may be in the log already, and the takeover gate
        // replays the queue locally while a drain-submitted copy of the
        // same op can be waiting for that very acquisition (M5 round 5,
        // long seed 11932: the gate's replay journaled it, then
        // `on_acquire_finished` executed the waiting copy a second time).
        //
        // The mark is the client's: a replay leaves it for the client's
        // own resubmission (`placement-hot+links+stalebase` seed 1397: a
        // deposed root's op went in doubt at its deadline, the drain then
        // submitted the rid's queue entry and took the mark, the replay
        // completed it, and the client's resubmission by rid, not in
        // doubt, ran it again: refused `EEXIST`, the rid's second outcome
        // in the log, and the answer the client heard).
        // A resubmission of a rid that went in doubt may have been sent to a
        // holder by the earlier record: the mark carries over. A replay's
        // starts unmarked: it is the queue's own entry, and its refusals are
        // held by the inbox hold (`on_holder_learned`), not by the reply
        // hold, which is the client's (`Origin::Client`); a replay that
        // `send_forward` sends to a designee is marked from then on, as any.
        let resubmitted =
            !matches!(origin, Origin::Replay { .. }) && self.in_doubt_rids.remove(&rid);
        let redirected_to_designee = resubmitted && self.in_doubt_designee_rids.remove(&rid);
        let in_doubt = matches!(origin, Origin::Replay { .. }) || resubmitted;
        let inbox_keys = self.in_doubt_batches.remove(&rid).unwrap_or_default();
        // ADR-47: the send history this node can vouch for. Whatever may
        // have lost it reads as unknown, which only ever holds a refusal.
        let touched_gen = if matches!(origin, Origin::Replay { .. }) {
            // The queue entry: its history is not known here.
            TOUCHED_UNKNOWN
        } else if resubmitted {
            // (A fast-path in-doubt resubmission, `Event::SubmitInDoubt`,
            // put `TOUCHED_UNKNOWN` there itself.)
            self.in_doubt_touched.remove(&rid).unwrap_or(0)
        } else if rid.node == self.cfg.node_id
            && (rid.incarnation < self.cfg.incarnation
                || self.in_doubt_evicted_max.is_some_and(|m| rid <= m))
        {
            TOUCHED_UNKNOWN
        } else {
            0
        };
        if touched_gen == TOUCHED_UNKNOWN {
            self.stats.touched_unknown_submits += 1;
        }
        let inbox_ever = !inbox_keys.is_empty();
        self.next_client_order += 1;
        self.clients.insert(
            rid,
            ClientOp {
                op,
                origin,
                policy,
                phase: Phase::WaitingLease,
                order: self.next_client_order,
                keys,
                attempts: 0,
                redirected: 0,
                held_by: None,
                touched_gen,
                inbox_ever,
                fence_bounces: 0,
                forwarded: in_doubt,
                acquire_retries: 0,
                deadline,
                deadline_extended: false,
                timer: None,
                inbox_since: None,
                inbox_durable_at: None,
                inbox_keys,
                deps,
                also_client: false,
                deps_held: false,
                submitted: now.wait,
                history: Vec::new(),
                own_record_timer: None,
                own_record_inos: BTreeSet::new(),
                own_record_uploaded: false,
                excused: Vec::new(),
                answered_by: None,
                settle: false,
                settle_until: WaitMs(0),
                settle_cap: WaitMs(0),
                gate_recall_wait: false,
                sent_to_delegate: resubmitted,
                redirected_to_designee,
                designation_held: false,
                accepted_by_delegate: false,
                tag,
            },
        );
        if settle {
            // Plan 46 M0.2 (R5): settled from the start; nothing of the
            // op's own is ever routed.
            // It gets the same window and cap as a settle that began at a
            // deadline, so the outwait hold applies to it too (I4).
            let window = self.dl_t().settle_window;
            let cap = now
                .wait
                .plus(2 * window + self.reclaim_horizon_ms() + 2 * self.cfg.forward_timeout_ms);
            if let Some(c) = self.clients.get_mut(&rid) {
                c.settle = true;
                c.settle_until = now.wait.plus(window);
                c.settle_cap = cap;
            }
            self.settle_route(now, rid, replica, out);
            return;
        }
        // Causal order after a generation ended with this node's writes
        // tentative (see `hold_for_lost_deps`).
        if self.hold_for_lost_deps(now, rid, replica, out) {
            return;
        }
        // Plan 30 §M11: a delegate executes its own subtree here; an op
        // under another node's delegation goes to that delegate (or to
        // the root, which recalls, when the observed stream table is
        // full or the delegate is unreachable).
        if self.cfg.delegation {
            let (op_ref, tag) = self
                .clients
                .get(&rid)
                .map(|c| (c.op.clone(), c.tag.clone()))
                .expect("present");
            if self.delegate_try_execute(
                now,
                0,
                None,
                rid,
                &op_ref,
                deps,
                0,
                &tag,
                false,
                Fence::default(),
                replica,
                out,
            ) {
                return;
            }
            if self.cfg.forwarding && self.cfg.p2p {
                if let Ownership::Delegated(d) =
                    replica.resolve_ownership(&super::holder::keys_of_op_in(&op_ref, replica))
                {
                    if d.node != self.cfg.node_id {
                        if deps.streams.is_full() && deps.streams.get(d.gen).is_none() {
                            self.stats.deps_overflow_to_root += 1;
                        } else if self.reaches(now, d.node) {
                            self.stats.deleg_forwarded += 1;
                            self.send_forward(now, rid, d.node, d.gen, replica, out);
                            return;
                        }
                    }
                }
            }
        }
        // The local fast path: the view is open, so execute here with no
        // await between the check and the write.
        if let Some(epoch) = self.lease.new_mutation_epoch(now, &self.cfg) {
            let outcome = self.resolve_in_doubt_then_execute(now, rid, epoch, replica, out);
            self.finish(now, rid, outcome, replica, out);
            return;
        }
        if self.lease.lost && policy != Policy::Client {
            // As the deadline would (plan 43 C2g's review: a system op is
            // settled too, never answered in doubt unsettled at once).
            self.client_deadline(now, rid, false, replica, out);
            return;
        }
        if self.gated(rid) {
            self.clients.get_mut(&rid).expect("present").phase = Phase::Gated;
            return;
        }
        self.route(now, rid, replica, out);
    }

    /// ADR-44: this node's own op or settle, routed while this node is the
    /// root in a shippable takeover gate (its view closed: it waits for the
    /// open). An open root would execute it here and recall, first, every
    /// generation its keys fall under (`execute_local_outcome` plans the
    /// recall without a redirect: a live, reachable delegate was tried
    /// before the route, and one that answered `NotHolder` naming this node
    /// has disowned the generation). The gate does the same now
    /// (`gate_recall_on_demand` with no redirect), not when it opens.
    /// ADR-44: this node's own settle waits in `Backoff` for the recall its
    /// gate sent on demand, and is routed as a settle again then
    /// (`on_forward_backoff` -> `settle_route`).
    fn gate_hold_settle(&mut self, now: Now, rid: Rid, out: &mut Vec<Action>) {
        let delay = self.cfg.recall_hold_ms.max(50);
        let timer = self.set_timer_wait(now.wait.plus(delay), Timer::ForwardBackoff(rid), out);
        if let Some(c) = self.clients.get_mut(&rid) {
            c.phase = Phase::Backoff;
            c.gate_recall_wait = true;
            if let Some(t) = c.timer.replace(timer) {
                self.cancel_timer(t, out);
            }
        }
    }

    /// `true`: a recall the op needs is in flight in the gate.
    fn gate_recall_for_own(
        &mut self,
        now: Now,
        rid: Rid,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        if !self.cfg.delegation || self.lease.gate.is_none() {
            return false;
        }
        let Some(op) = self.clients.get(&rid).map(|c| c.op.clone()) else {
            return false;
        };
        let keys = super::holder::keys_of_op_in(&op, replica);
        self.gate_recall_on_demand(now, None, &keys, replica, out)
    }

    /// Whether an earlier op of this node touching a common inode is
    /// still in flight. A replay of a stranded op goes ahead of every other
    /// op that has not left this node yet, whenever it was submitted: it
    /// was acknowledged before anything in flight on its keys was
    /// submitted (the gate kept them behind it then), and those may depend
    /// on it. (An op already sent to the new holder is not held back: it
    /// can still execute before the replay.) Ordered by submission,
    /// a replay waited behind an op that waited for the log to carry its
    /// acceptance by the dead holder — which never came: both waited for
    /// the client deadline (40 s), and the op was answered in doubt
    /// (`fuse-inval-storm` on uring: an unlink behind its own rename).
    fn gated(&self, rid: Rid) -> bool {
        let Some(me) = self.clients.get(&rid) else {
            return false;
        };
        let replay = |c: &ClientOp| matches!(c.origin, Origin::Replay { .. });
        self.clients.iter().any(|(other, c)| {
            *other != rid
                && match (replay(c), replay(me)) {
                    (true, false) => true,
                    (false, true) => false,
                    _ => c.order < me.order,
                }
                && !matches!(
                    c.phase,
                    Phase::InboxQueued { .. } | Phase::InboxWaiting { .. }
                )
                && c.keys.overlaps(&me.keys)
        })
    }

    /// An op finished: release the gated ops it was holding back, in
    /// submission order.
    fn release_gated(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        let mut ready: Vec<(u64, Rid)> = self
            .clients
            .iter()
            .filter(|(_, c)| c.phase == Phase::Gated)
            .map(|(rid, c)| (c.order, *rid))
            .collect();
        ready.sort_unstable();
        for (_, rid) in ready {
            // `finish` below re-enters this method for the ops still
            // gated at that point, so an op later in `ready` may already
            // have been released and finished by that nested pass (M5
            // round 5, long seed 11932: two gated ops behind one that a
            // takeover's acquisition released; the nested pass finished
            // the second and the outer loop indexed a removed entry).
            let Some(c) = self.clients.get_mut(&rid) else {
                continue;
            };
            if c.phase != Phase::Gated || self.gated(rid) {
                continue;
            }
            if let Some(epoch) = self.lease.new_mutation_epoch(now, &self.cfg) {
                // A gated resubmission is in doubt like any other: the
                // op it waited behind may have been the takeover gate's
                // own drain of this very rid.
                let outcome = self.resolve_in_doubt_then_execute(now, rid, epoch, replica, out);
                self.finish(now, rid, outcome, replica, out);
                continue;
            }
            if let Some(c) = self.clients.get_mut(&rid) {
                c.phase = Phase::WaitingLease;
            }
            self.route(now, rid, replica, out);
        }
    }

    /// The `deps` a write submitted now carries: the session's (see
    /// `SessionState::deps`), plus, on the root, its own journal.
    pub(crate) fn fresh_deps(&mut self, now: Now, replica: &dyn Replica) -> Position {
        // Plan 30 §M11: `None` (the streams overflow) is a root-only op:
        // the root orders after everything it appended.
        let (mut deps, overflow) = match replica.deps() {
            Some(d) => (d, false),
            None => (
                Position {
                    streams: Default::default(),
                    ..replica.observed()
                },
                true,
            ),
        };
        if overflow {
            self.stats.deps_overflow_to_root += 1;
        }
        // The root's own journal (its local executions, the streams it
        // appended) is ahead of what it shipped: a delegate orders after
        // all of it.
        if let Some(epoch) = self.lease.ship_epoch(now, &self.cfg) {
            if let Some(jp) = replica.journal_position(epoch) {
                if Some(jp) > deps.pending {
                    deps.pending = Some(jp);
                }
            }
        }
        deps
    }

    /// Causal order across a generation's end. This node's own
    /// tentatively acknowledged ops (a delegate's acceptance whose
    /// generation ended before the root appended it) are stranded and
    /// replayed by rid; a later write of this node must not overtake
    /// those replays — above all one whose `deps` name such a
    /// transaction (`Replica::deps_lost`), which the void rule alone
    /// would let run first (a `marker-order` marker ahead of its data:
    /// long-delegated seeds 74189, 76967, long-delegated-backup 77901).
    ///
    /// So a client op is held (a backoff, re-checked on its timer) while
    /// this node has replays to settle *ahead of its own entry, if it has
    /// one* (a fast-path row a deposition stranded is resubmitted through
    /// the core by `DurableWait::Lost` and is also queued; held for the
    /// entries behind it, the op and the drain — which waits for a live
    /// client op at the head of the queue — waited for each other until
    /// the client deadline: `load-probe-takeover` under
    /// `pause:lock-lapse+500:10000@b`, 32 of 40 cells); then, if its
    /// `deps` were lost, they are taken afresh — ordering after wherever the replays
    /// landed — and it proceeds. `true`: held. A replay itself is never
    /// held (the queue is what the others wait for); its own lost `deps`
    /// are refreshed, and so are the recorded rows (`StrandedOp::deps`) of
    /// a generation the log has ended (see `live_rows`).
    pub(crate) fn hold_for_lost_deps(
        &mut self,
        now: Now,
        rid: Rid,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        if !self.cfg.delegation {
            return false;
        }
        let Some(c) = self.clients.get(&rid) else {
            return false;
        };
        let lost = replica.deps_lost(&c.deps);
        let replay = matches!(c.origin, Origin::Replay { .. });
        let first_hold = !c.deps_held;
        let unsettled = !replay && self.own_replays_unsettled_ahead_of(replica, Some(rid));
        // A client op that is also a queue entry (a deposition stranded its
        // row after the op was resubmitted in doubt) owes the rows its
        // entry recorded of the generations that are still live, as its
        // replay would (`live_rows`): the executor waits for them.
        let queued_owed = if replay {
            Streams::NONE
        } else {
            self.queued_rows_owed(rid, replica)
        };
        let owing = c
            .deps
            .streams
            .join(&queued_owed)
            .filter(|j| *j != c.deps.streams);
        // A replay's recorded rows (`StrandedOp::deps`) of a generation
        // the log has ended are not waited for either: see below.
        let ended = match &c.origin {
            Origin::Replay { deps, .. } => live_rows(deps, replica) != *deps,
            _ => false,
        };
        if !lost && !unsettled && !c.deps_held && !ended {
            if let Some(joined) = owing {
                if let Some(c) = self.clients.get_mut(&rid) {
                    c.deps.streams = joined;
                }
            }
            return false;
        }
        if unsettled {
            self.stats.deps_lost_holds += 1;
            if first_hold && tracing::enabled!(tracing::Level::DEBUG) {
                // A silent hold hid a 40 s deadlock once: say what the op
                // waits for (it is also in the op's history, as
                // `+held-for-replays`), by the hold's own rule.
                let queue = replica.pending_replays().unwrap_or_default();
                let own = queue.iter().find(|s| s.rid == rid).map(|s| s.queue_seq);
                let table = replica.delegation_table();
                let waiting_for: Vec<_> = queue
                    .iter()
                    .filter(|s| self.is_unsettled_replay_ahead(s, own, &table))
                    .take(4)
                    .map(|s| (s.rid, s.queue_seq, s.gen))
                    .collect();
                tracing::debug!(
                    node = self.cfg.node_id,
                    ?rid,
                    in_flight = ?self.replay.in_flight,
                    ?waiting_for,
                    "an op is held for this node's replays queued ahead of it"
                );
            }
            let old = self.clients.get_mut(&rid).and_then(|c| c.timer.take());
            if let Some(t) = old {
                self.cancel_timer(t, out);
            }
            let timer = self.set_timer_wait(
                now.wait.plus(self.cfg.delegation_stream_tick_ms.max(10)),
                Timer::ForwardBackoff(rid),
                out,
            );
            let c = self.clients.get_mut(&rid).expect("present");
            c.phase = Phase::Backoff;
            c.timer = Some(timer);
            c.deps_held = true;
            return true;
        }
        let mut deps = self.fresh_deps(now, replica);
        // A replay's recorded rows of the generations that are still live
        // stay owed (the delegate re-streams them to the log's root, and
        // the executor waits for them): only the rows of an ended
        // generation are replaced by the fresh session. Which generation
        // ended is the log's word, not this replica's own memory of it:
        // a deposed root's session still holds the cut at which it ended
        // the generation itself (`end_generation`, a `DelegateNodeEnded` that was
        // journaled and never shipped), above the one the successor's
        // `DelegateNodeEnded` made of it in the log, and its `deps_lost` calls the
        // rows between them present while the executor refuses them as
        // lost (long-delegated-backup seed 3585: `Held`, re-sent, forever).
        let mut owed = None;
        if let Some(Origin::Replay { deps: recorded, .. }) =
            self.clients.get(&rid).map(|c| &c.origin)
        {
            let live = live_rows(recorded, replica);
            deps.streams = live.join(&deps.streams).unwrap_or(live);
            owed = Some(live);
        } else if let Some(joined) = deps.streams.join(&queued_owed) {
            deps.streams = joined;
        }
        tracing::debug!(
            node = self.cfg.node_id,
            ?rid,
            ?deps,
            "an op held for this node's replays (or with deps lost with their generation) proceeds with fresh deps"
        );
        self.stats.deps_refreshed += 1;
        if let Some(c) = self.clients.get_mut(&rid) {
            c.deps = deps;
            c.deps_held = false;
            // What the replay still owes is what stays recorded: the next
            // call asks again only when another generation ends.
            if let (Origin::Replay { deps, .. }, Some(live)) = (&mut c.origin, owed) {
                *deps = live;
            }
        }
        false
    }

    /// Replays this node still has to run (its own stranded ops, not a
    /// refused one waiting for its conflict copy, not another node's
    /// streamed op, not one held while its generation lives — the
    /// delegate re-streams it in order — and not one whose rid is already
    /// in flight as a client op).
    pub(super) fn own_replays_unsettled(&self, replica: &dyn Replica) -> bool {
        self.own_replays_unsettled_ahead_of(replica, None)
    }

    /// [`Self::own_replays_unsettled`], for an op of the given rid: only
    /// the entries the drain replays *before the op's own entry* count
    /// (those with a smaller `queue_seq`; the drain walks the queue in
    /// that order), and so none do for an op at the head of the queue.
    /// An op whose rid is not queued (or `None`: a barrier has no rid)
    /// waits for every entry.
    ///
    /// The distinction is one of liveness. A fast-path row a deposition
    /// strands is a live client op (the `DurableWait::Lost` resubmission)
    /// and a queue entry at once; the drain stops at an entry that is in
    /// flight as a client op, so it never reaches the entries behind it,
    /// and an op held for them is held until its deadline.
    pub(super) fn own_replays_unsettled_ahead_of(
        &self,
        replica: &dyn Replica,
        rid: Option<Rid>,
    ) -> bool {
        // A barrier (no rid) waits for everything, a replay in flight
        // included, without reading the queue.
        if rid.is_none() && self.replay.in_flight.is_some() {
            return true;
        }
        let Ok(queue) = replica.pending_replays() else {
            return self.replay.in_flight.is_some();
        };
        let own = rid.and_then(|rid| queue.iter().find(|s| s.rid == rid).map(|s| s.queue_seq));
        // Plan 46 M0.2 review (MUST 1 b): an entry the drain holds behind a
        // placeholder (R2) can wait for the placeholder's bound; an op
        // waits for it only if it shares a key with it.
        let held = super::replay::held_behind_placeholders(&queue, replica);
        let keys = rid.and_then(|rid| self.clients.get(&rid)).map(|c| &c.keys);
        let held_unrelated = |s: &constellation_meta::StrandedOp| {
            held.contains(&s.queue_seq)
                && keys.is_some_and(|k| !super::replay::entry_keys(s, replica).overlaps(k))
        };
        if self
            .replay
            .in_flight
            .is_some_and(|q| own.is_none_or(|own| q < own))
        {
            return true;
        }
        if queue.is_empty() {
            return false;
        }
        let table = replica.delegation_table();
        queue
            .iter()
            .any(|s| self.is_unsettled_replay_ahead(s, own, &table) && !held_unrelated(s))
    }

    /// The hold's rule for one queue entry: an op whose own entry has
    /// `queue_seq` `own` (none: it has none) waits for `entry` when the
    /// drain replays it first and it is this node's own, not refused, not
    /// held while its generation lives, and not a live client op itself.
    fn is_unsettled_replay_ahead(
        &self,
        entry: &constellation_meta::StrandedOp,
        own: Option<u64>,
        table: &constellation_meta::delegation::DelegationTable,
    ) -> bool {
        own.is_none_or(|own| entry.queue_seq < own)
            && entry.refused.is_none()
            // (A placeholder is another node's row awaiting its requester's
            // retry, not a replay of this node's.)
            && !entry.is_await_retry()
            && !entry.foreign
            && !self.clients.contains_key(&entry.rid)
            && (entry.gen == 0 || !table.has_gen(entry.gen))
    }

    /// The rows of live generations the queue entry of `rid` recorded (its
    /// `StrandedOp::deps`, `live_rows`); none when the rid is not queued.
    fn queued_rows_owed(&self, rid: Rid, replica: &dyn Replica) -> Streams {
        let Ok(queue) = replica.pending_replays() else {
            return Streams::NONE;
        };
        queue
            .iter()
            .find(|s| s.rid == rid)
            .map_or(Streams::NONE, |s| live_rows(&s.deps, replica))
    }

    /// Ops of this node waiting in an S3 inbox with lost `deps`: the root
    /// leaves them there (it will not execute them), so they are withdrawn
    /// and held here (`send_forward` to node 0 = withdraw, then back off).
    pub(crate) fn withdraw_lost_deps_inbox_ops(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let lost: Vec<Rid> = self
            .clients
            .iter()
            .filter(|(_, c)| {
                matches!(c.phase, Phase::InboxWaiting { .. })
                    && !c.inbox_keys.is_empty()
                    && !matches!(c.origin, Origin::Replay { .. })
            })
            .filter(|(_, c)| replica.deps_lost(&c.deps))
            .map(|(r, _)| *r)
            .collect();
        for rid in lost {
            tracing::debug!(
                node = self.cfg.node_id,
                ?rid,
                "inbox op's dependency was lost with its generation: withdrawing it"
            );
            if let Some(t) = self.clients.get_mut(&rid).and_then(|c| c.timer.take()) {
                self.cancel_timer(t, out);
            }
            self.send_forward(now, rid, 0, 0, replica, out);
        }
    }

    /// Not the holder: forward if a holder is known and reachable, learn
    /// it if not, submit through its inbox when there is no P2P path, or
    /// take the lease path when forwarding is off.
    pub(crate) fn route(
        &mut self,
        now: Now,
        rid: Rid,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some((policy, settle)) = self.clients.get(&rid).map(|c| (c.policy, c.settle)) else {
            return;
        };
        // ADR-44: this node is the root in a shippable takeover gate; the
        // generations the op needs ended are recalled now. A settle whose
        // recall is in flight is asked again here (`on_forward_backoff` ->
        // `settle_route`) once the cut may be journaled, not sent down the
        // lease path, which waits for the open.
        if self.gate_recall_for_own(now, rid, replica, out) && settle {
            self.gate_hold_settle(now, rid, out);
            return;
        }
        // Plan 31 C8: a forward-only node forwards whatever
        // `CONSTELLATION_FORWARD` says.
        if !self.cfg.forwarding && !self.mode.forwards() {
            self.lease_path(now, rid, replica, out);
            return;
        }
        // Plan 30 M5 round 3: an op from a node whose own journal is not
        // shipped yet (an ephemeral clone made at mount, a deposed node's
        // recovery, a rebuild) may depend on records no holder has; a
        // holder would refuse it (`ENOENT`) for state that exists here.
        // Only this node can execute it, after its journal ships — the
        // lease path, which the `ship-pending-journal` acquisition is
        // already on. A non-holder with nothing journaled never pays this.
        // Plan 30 §M11: a delegate's own stream rows are not such work
        // (the root ships them); only undelegated rows count.
        if replica.journal_len().unwrap_or(0) > 0 && replica.journal_has_undelegated() {
            tracing::debug!(
                node = self.cfg.node_id,
                ?rid,
                "unshipped local journal: the op takes the lease path, not a forward"
            );
            self.lease_path(now, rid, replica, out);
            return;
        }
        if policy == Policy::BestEffort && self.lease.cached_holder.is_none() {
            self.finish_in_doubt(now, rid, replica, out);
            return;
        }
        let cached = self.lease.cached_holder.filter(|h| *h != self.cfg.node_id);
        match cached {
            Some(holder) if self.reaches(now, holder) => {
                self.send_forward(now, rid, holder, 0, replica, out)
            }
            // A cache naming this node is not trustworthy on its own
            // (nothing invalidates it when the lease is lost); re-read.
            // An unreachable holder needs the lease object too: the
            // inbox is addressed by its epoch.
            _ => {
                if policy == Policy::BestEffort {
                    self.finish_in_doubt(now, rid, replica, out);
                    return;
                }
                if self.epoch_refuses_writes() {
                    // The lease read leads to a forward to nobody or to
                    // the lease path, which cannot end in this epoch.
                    self.refuse_on_lease_path(now, rid, Code::ReadOnly, replica, out);
                    return;
                }
                if let Some(c) = self.clients.get_mut(&rid) {
                    c.phase = Phase::LearnHolder;
                }
                self.issue_s3(S3Op::LeaseGet, S3For::LearnHolder(rid), out);
            }
        }
    }

    /// Whether `rid`'s op was sent to a node holding a non-stealable grant
    /// (a sticky grant or a designation) of its keys: it may have run there,
    /// and a final refusal by anyone else would contradict it.
    fn sent_to_designation(&self, rid: Rid, replica: &dyn Replica) -> bool {
        let Some(c) = self.clients.get(&rid).filter(|c| c.sent_to_delegate) else {
            return false;
        };
        let keys = super::holder::keys_of_op_in(&c.op, replica);
        match replica.resolve_ownership(&keys) {
            Ownership::Delegated(d) => d.kind != constellation_meta::SubtreeKind::Placed,
            Ownership::CrossSubtree { involved, .. } => involved
                .iter()
                .any(|d| d.kind != constellation_meta::SubtreeKind::Placed),
            // A table that lags the root's journal resolves the keys to the
            // root after a redirect to the designee: still sent to it.
            Ownership::Root => c.redirected_to_designee,
        }
    }

    /// Whether `node` owns (holds a grant of) any of `rid`'s keys in this
    /// replica's table.
    fn owns_keys_of(&self, rid: Rid, node: NodeId, replica: &dyn Replica) -> bool {
        let Some(c) = self.clients.get(&rid) else {
            return false;
        };
        let keys = super::holder::keys_of_op_in(&c.op, replica);
        match replica.resolve_ownership(&keys) {
            Ownership::Delegated(d) => d.node == node,
            Ownership::CrossSubtree { involved, .. } => involved.iter().any(|d| d.node == node),
            Ownership::Root => false,
        }
    }

    pub(crate) fn on_holder_learned(
        &mut self,
        now: Now,
        rid: Rid,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // Only an op still waiting for this read is routed by it. The read
        // may come back long after it was sent (the response held up on
        // its way), the op meanwhile answered in doubt at its deadline and
        // resubmitted by rid, executed here and parked for its answer's
        // durability: routed again by the late read, it took the lease path
        // and executed a second time, not in doubt (`completed` was not
        // asked), and the client heard the first outcome while the log
        // carried the second (`locks-core-stall-backup-long-slow-lease`
        // seed 671: refused `EEXIST`, then created once the name was gone).
        if !self
            .clients
            .get(&rid)
            .is_some_and(|c| c.phase == Phase::LearnHolder)
        {
            return;
        }
        let object = match result {
            S3Result::LeaseGet(Ok(Some((lease, _)))) => {
                self.lease.note_object(&lease);
                Some(lease)
            }
            _ => None,
        };
        let live = object
            .as_ref()
            .filter(|lease| lease.holder != 0 && !self.lease.claimable_observed(now, &self.cfg));
        match live {
            Some(lease) if lease.holder != self.cfg.node_id => {
                let holder = lease.holder;
                let epoch = lease.epoch;
                let reaches = self.reaches(now, holder);
                tracing::debug!(
                    node = self.cfg.node_id,
                    ?rid,
                    holder,
                    reaches,
                    inbox = self.cfg.inbox,
                    "holder learned from the lease"
                );
                if reaches {
                    self.send_forward(now, rid, holder, 0, replica, out);
                } else if self.cfg.inbox
                    && !self.cfg.refuse_forwarded_designation_ops
                    && self.sent_to_designation(rid, replica)
                {
                    // The inbox drain refuses an op under a designation for
                    // good (journaled, `Refused`), and an op sent to the
                    // designee may have run there: the refusal would
                    // contradict the designee's answer (I1). It waits, as
                    // for the root's refusal (`on_mutate_reply`), until the
                    // holder's word or its deadlines.
                    self.stats.designation_refusals_held += 1;
                    let timer = self.set_timer_wait(
                        now.wait.plus(self.cfg.forward_timeout_ms.max(10)),
                        Timer::ForwardBackoff(rid),
                        out,
                    );
                    if let Some(c) = self.clients.get_mut(&rid) {
                        c.phase = Phase::Backoff;
                        c.timer = Some(timer);
                        c.designation_held = true;
                    }
                } else if self.cfg.inbox {
                    self.inbox_enqueue(now, rid, epoch, replica, out);
                } else {
                    self.lease_path(now, rid, replica, out);
                }
            }
            // Nobody to forward to (no lease, claimable, or it names us
            // while our view is closed): the lease path.
            _ => {
                self.lease.cached_holder = None;
                self.lease_path(now, rid, replica, out);
            }
        }
    }

    pub(crate) fn send_forward(
        &mut self,
        now: Now,
        rid: Rid,
        holder: NodeId,
        routed_gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let req = self.op_id();
        let timer = self.set_timer_wait(
            now.wait.plus(self.cfg.forward_timeout_ms),
            Timer::ForwardTimeout(rid),
            out,
        );
        let acked_through = self.acked.floor();
        // A node other than the holder the holder signals last named
        // (`LeaseState::named_holder`: lease objects, depositions, stream
        // frames, the holder's own replies; not only the last lease object
        // read, which they leave behind) may be a designee: the table here
        // lags the root's, and any cache setter may have named one. The op
        // is marked, so its refusal by the root is held, not final (I1). A
        // structural guard: it does not depend on how the target came to be
        // cached. (Without a signal there is nothing to compare with.)
        let to_delegate = replica.delegation_table().iter().any(|e| e.node == holder)
            || (holder != 0 && self.lease.named_holder.is_some_and(|named| named != holder));
        let Some(c) = self.clients.get_mut(&rid) else {
            return;
        };
        // (A send routed by a generation is to a delegate, whatever the
        // table here lists.)
        c.sent_to_delegate |= to_delegate || routed_gen != 0;
        // ADR-47: the rid is touched by the generation this send is routed
        // by from here on, whether the send leaves now or after the batch
        // withdrawal below; the value travels with it.
        c.touched_gen = c.touched_gen.max(routed_gen);
        if let Some(&key) = c.inbox_keys.first() {
            // Withdraw the durable batches (one at a time, every one the
            // rid was submitted in) before the holder can see the op
            // twice: a drain that read one later would find no
            // `completed` witness for a P2P refusal and execute it again.
            if c.inbox_keys.len() > 1 && !matches!(c.phase, Phase::InboxWithdraw { .. }) {
                self.stats.inbox_multi_batch_withdrawals += 1;
            }
            // Withdrawn by overwriting the batch with a tombstone, not by
            // DELETE: the key stays taken, so the holder's GET-next reads
            // past it to this node's later batches instead of stopping at
            // a hole (co-batched ops are re-submitted by
            // `on_inbox_withdrawn`).
            c.phase = Phase::InboxWithdraw {
                holder,
                gen: routed_gen,
            };
            self.cancel_timer(timer, out);
            self.inbox.pending.remove(&rid);
            let batch = constellation_store_s3::inbox::InboxBatch::tombstone(
                key,
                self.cfg.incarnation,
                now.wall.0,
            );
            self.issue_s3(
                S3Op::InboxTombstone { batch },
                super::S3For::InboxWithdraw(rid),
                out,
            );
            return;
        }
        if holder == 0 {
            // `withdraw_lost_deps_inbox_ops`: withdrawn; back off, where
            // `hold_for_lost_deps` decides.
            c.phase = Phase::Backoff;
            self.cancel_timer(timer, out);
            let t = self.set_timer_wait(
                now.wait.plus(self.cfg.delegation_stream_tick_ms.max(10)),
                Timer::ForwardBackoff(rid),
                out,
            );
            self.clients.get_mut(&rid).expect("present").timer = Some(t);
            return;
        }
        c.phase = Phase::Forwarded { req, holder };
        c.timer = Some(timer);
        c.forwarded = true;
        let deps = c.deps;
        self.by_req.insert(req, rid);
        out.push(Action::Send {
            to: holder,
            msg: PeerMsg::MutateRequest {
                req,
                rid,
                op: c.op.clone(),
                acked_through,
                deps,
                applied: replica.applied_seq().unwrap_or(0),
                tag: c.tag.clone(),
                settle: c.settle,
                routed_gen,
                touched_gen: c.touched_gen,
            },
        });
    }

    // ---- the forward's outcomes ----

    /// Plan 43 C2 (§5.3): a shadow or hint installed for an answer its
    /// sequencer did not say is committed. A non-stealable sequencer's — an
    /// open continuation epoch's hold owner (this node is in the epoch), or a
    /// designee (`gen` names a designation) — is one by design (its answers
    /// stay uncommitted, the maintainer's decision on C2f) and counted
    /// apart; any other is an answer outside the commit rule: counted and
    /// warned once, so a sequencer that still gives one is seen.
    /// Plan 46 M0.2 (R7): whether an answer (not `committed`) came from a
    /// non-stealable sequencer: an open continuation epoch's hold owner
    /// (this node is in the epoch), or a designee (`gen` names a
    /// designation).
    fn answered_non_stealable(&self, committed: bool, gen: u64, replica: &dyn Replica) -> bool {
        !committed
            && (self.epoch.open
                || (gen != 0
                    && replica
                        .delegation_table()
                        .iter()
                        .any(|d| d.gen == gen && d.non_stealable())))
    }

    fn note_ungated_shadow(
        &mut self,
        from: NodeId,
        rid: Rid,
        committed: bool,
        gen: u64,
        replica: &dyn Replica,
    ) {
        if committed {
            return;
        }
        if self.answered_non_stealable(committed, gen, replica) {
            self.stats.ungated_shadows_non_stealable += 1;
            return;
        }
        self.stats.ungated_shadows += 1;
        if self.stats.ungated_shadows == 1 {
            tracing::warn!(
                node = self.cfg.node_id,
                from,
                ?rid,
                "installed a shadow for an answer its sequencer did not say is committed"
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_mutate_reply(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        outcome: MutateOutcome,
        (base, position, gen, own_chunks, own_rows, committed): (
            Option<crate::ids::Seq>,
            Position,
            u64,
            OwnChunks,
            Option<constellation_meta::OwnRows>,
            bool,
        ),
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(rid) = self.by_req.remove(&req) else {
            return;
        };
        let Some(c) = self.clients.get_mut(&rid) else {
            return;
        };
        if !matches!(c.phase, Phase::Forwarded { req: r, .. } if r == req) {
            return;
        }
        if let Some(t) = c.timer.take() {
            self.cancel_timer(t, out);
        }
        let own_chunks = self.settle_own_rows(rid, own_chunks, own_rows.as_ref(), replica);
        self.stats.forwards_ok += 1;
        self.note_p2p_result(now, from, true);
        // Plan 30 §M11: whatever the reply installs here, this client's
        // next write orders after the answering sequencer's position.
        replica.note_frontier(&position);
        if gen == 0
            && !matches!(
                outcome,
                MutateOutcome::NotHolder { .. } | MutateOutcome::Busy
            )
        {
            // Plan 30 §M11: a delegate's reply says nothing about the
            // lease; only the root's names the holder. Nor does a
            // refusal to execute: a recalled delegate answers its parked
            // ops `NotHolder` with no generation, and cached as the
            // holder, it took this node's lock renewals for the rest of
            // an S3 cut (`locks-blips-tight-delegated` seed 3117: two
            // former delegates each sent the other their renewals until
            // the grant lapsed under its I/O; chunk delegate-fenced-io).
            self.lease.name_holder(from);
        }
        let applied = replica.applied_seq().unwrap_or(0);
        let base_ok = self.cfg.speculate_on_stale_base || base.is_some_and(|b| applied >= b);
        tracing::debug!(
            node = self.cfg.node_id,
            ?rid,
            from,
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            ?base,
            ?position,
            applied,
            base_ok,
            ?outcome,
            "forward reply"
        );
        // A designation's refusal (the root's `EROFS`/`EXDEV`: nothing is
        // journaled for it, no generation named) of an op this node sent to
        // the designee (its table named it, or the root redirected there)
        // before: the root cannot know that, and
        // the designee may have run the op, so the refusal would tell the
        // application it did not (I1). It is asked again, like a `Held`: if
        // the designee's row reached the root the answer is the rid's
        // outcome, else the op's deadlines end it in doubt (the settle's
        // unsettled `EIO`, bounded by `settle_cap`: I4's exception for an
        // unreachable non-stealable holder).
        let outcome = if gen == 0
            && !self.cfg.refuse_forwarded_designation_ops
            && !committed
            && matches!(
                &outcome,
                MutateOutcome::Errno(Code::ReadOnly | Code::CrossDevice)
            )
            && self
                .clients
                .get(&rid)
                .is_some_and(|c| c.sent_to_delegate && c.origin == Origin::Client)
            // (The refusal is the root's, not the designee's own: the root
            // may be listed as a delegate of other keys, so the table's
            // listing says nothing; whose grant the op's keys are does.)
            && !self.owns_keys_of(rid, from, replica)
        {
            self.stats.designation_refusals_held += 1;
            if let Some(c) = self.clients.get_mut(&rid) {
                c.designation_held = true;
            }
            MutateOutcome::Held {
                retry_ms: self.cfg.forward_timeout_ms.max(10),
                outwait_ms: None,
            }
        } else {
            outcome
        };
        if matches!(&outcome, MutateOutcome::Accepted { .. }) {
            // The latest acceptance decides: a root's after a delegate's
            // leaves the op awaiting the root's log, not a delegate's rows.
            if let Some(c) = self.clients.get_mut(&rid) {
                c.accepted_by_delegate = gen != 0;
            }
        }
        match outcome {
            MutateOutcome::Accepted { epoch, records } if !base_ok || records.is_empty() => {
                // The holder evaluated the op against state this replica
                // has not applied: installing the records now would put
                // them ahead of records they follow. The log delivers
                // them in order; answer when it has.
                //
                // No records: the holder's `completed` dedup answer (an
                // op it executed before). There is nothing to install,
                // and a shadow of nothing would stand for the op's
                // effect: the holder's stream then adopts it in place by
                // rid and the transaction is never applied here
                // (`delegated-backup+links+stalebase` seed 138: a
                // deposed root's replay of a delegate's create was
                // answered from dedup, and the create was lost on that
                // replica). Wait for the log, or the stream, instead.
                if replica.completed_position(rid).ok().flatten().is_some() {
                    // Delivered by the log already; the rest of what the
                    // holder evaluated against normally came with it.
                    self.observe_reply(now, rid, position, &own_chunks, replica, out);
                    self.finish_as(
                        now,
                        rid,
                        MutateOutcome::Accepted { epoch, records },
                        AnswerSource::Relayed { from },
                        replica,
                        out,
                    );
                    return;
                }
                self.stats.awaited_log += 1;
                // Plan 30 §M9 (the OVH run's finding 4): the holder's
                // pre-S3 stream carried the transaction here ahead of
                // this reply — installed already, in the holder's order.
                if self.adopt_streamed(rid, &position, replica) {
                    replica.note_covering(KeySet::from_records(&records), position);
                    self.finish_as(
                        now,
                        rid,
                        MutateOutcome::Accepted { epoch, records },
                        AnswerSource::Relayed { from },
                        replica,
                        out,
                    );
                    return;
                }
                let c = self.clients.get_mut(&rid).expect("present");
                c.phase = Phase::AwaitingLog { epoch, position };
                // What the pre-S3 stream brings (`answer_awaiting_streamed`)
                // is this sequencer's own transaction, its answer passed on;
                // an outcome read from the log is the log's
                // (`answer_awaiting_log`).
                c.answered_by = Some(AnswerSource::Relayed { from });
                // Chunk metered-own-rows (OVH run 31): a replay of a
                // stranded op waits for a transaction the sequencer's
                // stream may have carried here already — before this
                // node's deposition was recovered (a deposed holder drops
                // the new holder's stream, and its recovery rolls back
                // what it installed), so it is gone here and the stream
                // does not resend it. Answered `Streamed`, each replay
                // waited out the safety timer (10 s) before its upload,
                // one after another (the queue replays one op at a time),
                // and a close queued behind seven of them hit its 120 s
                // deadline. Replays are rare (a takeover's stranded ops):
                // upload at once.
                let own_chunks = match own_chunks {
                    OwnChunks::Streamed(inos) if matches!(c.origin, Origin::Replay { .. }) => {
                        OwnChunks::Upload(inos)
                    }
                    own => own,
                };
                self.await_own_records(now, rid, own_chunks, out);
                self.nudge(now, out);
            }
            MutateOutcome::Accepted { epoch, records } => {
                let Some((op, tag)) = self
                    .clients
                    .get(&rid)
                    .map(|c| (c.op.clone(), c.tag.clone()))
                else {
                    return;
                };
                let answer = (
                    committed,
                    self.answered_non_stealable(committed, gen, replica)
                        .then_some(now.wall.0),
                );
                match replica.install_shadow(rid, epoch, gen, &op, &tag, &records, answer) {
                    Ok(true) => {
                        tracing::debug!(node = self.cfg.node_id, ?rid, epoch, "shadow installed");
                        self.stats.shadows_installed += 1;
                        self.note_ungated_shadow(from, rid, committed, gen, replica);
                        // Plan 30 §M6 (coordinator decision 2): installed
                        // effects do not raise `observed`; the shadow
                        // covers its keys (the parent directory's
                        // attributes included) up to the reply's position.
                        replica.note_covering(KeySet::from_records(&records), position);
                        self.finish_as(
                            now,
                            rid,
                            MutateOutcome::Accepted { epoch, records },
                            AnswerSource::Relayed { from },
                            replica,
                            out,
                        );
                    }
                    Ok(false) => {
                        // Not installed: the completion is already in the
                        // log prefix (fine), or this node took the lease
                        // at a higher epoch and queued the op for replay
                        // (plan 30 §M3b) — then the lease path executes it
                        // here at once by the same rid.
                        let queued = replica.completed_position(rid).ok().flatten().is_none();
                        if queued {
                            self.stats.queued_behind_takeover += 1;
                            self.lease_path(now, rid, replica, out);
                        } else {
                            self.observe_reply(now, rid, position, &own_chunks, replica, out);
                            self.finish_as(
                                now,
                                rid,
                                MutateOutcome::Accepted { epoch, records },
                                AnswerSource::Relayed { from },
                                replica,
                                out,
                            );
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, node = self.cfg.node_id, ?rid, "failed to install shadow");
                        // This node's own error, not an answer of the
                        // sequencer's.
                        self.finish_as(
                            now,
                            rid,
                            MutateOutcome::Errno(Code::Io),
                            AnswerSource::None,
                            replica,
                            out,
                        );
                    }
                }
            }
            MutateOutcome::Busy => self.retry_or_lease(now, rid, replica, out),
            // ADR-47: the answering node holds the keys under `fenced` and
            // was not routed by it; nothing ran. Not an attempt, and not a
            // holder to cache. Sent again routed by that generation, at most
            // twice; past that the op is routed afresh (table, cache, lease).
            MutateOutcome::Fenced { gen: fenced } => {
                let bounced = self.clients.get_mut(&rid).map(|c| {
                    c.fence_bounces += 1;
                    c.fence_bounces
                });
                if bounced.is_some_and(|n| n <= 2) {
                    self.stats.fenced_resends += 1;
                    self.send_forward(now, rid, from, fenced, replica, out);
                } else {
                    self.stats.fence_bounce_limit += 1;
                    self.route(now, rid, replica, out);
                }
            }
            // ADR-47: the root holds an op that touched a non-stealable
            // generation whose holder it does not reach (nothing journaled).
            // Held like a `Held`: asked again, no attempt spent; the op ends
            // by the root's later answer or in doubt at its deadlines.
            MutateOutcome::HolderUnreachable { retry_ms, .. } => {
                self.stats.designation_refusals_held += 1;
                if self
                    .clients
                    .get(&rid)
                    .is_some_and(|c| matches!(c.origin, Origin::Replay { .. }))
                {
                    self.replay.stuck_since = None;
                }
                let timer =
                    self.set_timer_wait(now.wait.plus(retry_ms), Timer::ForwardBackoff(rid), out);
                if let Some(c) = self.clients.get_mut(&rid) {
                    c.phase = Phase::Backoff;
                    c.timer = Some(timer);
                    c.designation_held = true;
                    c.held_by = Some((from, 0));
                }
            }
            MutateOutcome::Held {
                retry_ms,
                outwait_ms,
            } => {
                // Plan 30 §M8: executed; the holder is recalling read
                // delegations before it acknowledges. Ask again (same
                // rid, no attempt spent) — the holder answers from its
                // dedup, or holds the retry until the recalls are done.
                self.stats.held_retries += 1;
                // Plan 46 M1.2 step 13: the sequencer holds this settle for
                // the end of a generation it outwaits: its rows land when
                // that ends and the settle then finds them. The second
                // deadline moves past it (never earlier), at most to the
                // settle's cap.
                if let Some(d) = outwait_ms {
                    let window = self.dl_t().settle_window;
                    let want = self
                        .clients
                        .get(&rid)
                        .filter(|c| c.settle && c.settle_cap > WaitMs(0))
                        .map(|c| now.wait.plus(d).plus(window).min(c.settle_cap));
                    if let Some(want) = want {
                        if self.extend_settle_deadline(rid, want, None, out) {
                            self.stats.settles_extended_for_outwait += 1;
                        }
                    }
                }
                // Plan 46 M0.2 review: a holder answering `Held` is alive
                // and holding the op by design (behind its replay queue, up
                // to the placeholder's bound, or for a recall): a replay it
                // holds is not stuck, and the lease fallback
                // (`replay_lease_fallback_ms`) does not count the time.
                if self
                    .clients
                    .get(&rid)
                    .is_some_and(|c| matches!(c.origin, Origin::Replay { .. }))
                {
                    self.replay.stuck_since = None;
                }
                let timer =
                    self.set_timer_wait(now.wait.plus(retry_ms), Timer::ForwardBackoff(rid), out);
                if let Some(c) = self.clients.get_mut(&rid) {
                    c.phase = Phase::Backoff;
                    c.timer = Some(timer);
                    // (A delegate's `Held` names its generation: the retry
                    // goes back to it routed by that.)
                    c.held_by = Some((from, gen));
                }
                // Only an acknowledgement waiting for durability that
                // needs this node's chunks says `Upload`; a recall's, a
                // lost dependency's or a redirect's hold never does.
                if matches!(own_chunks, OwnChunks::Upload(_)) {
                    self.await_own_records(now, rid, own_chunks, out);
                }
            }
            MutateOutcome::NotHolder { holder } => {
                // The root's redirect to a designee carries the designee's
                // generation (`gen`); a lease holder's redirect (a deposed
                // root naming the new one, a delegate naming the root) does
                // not. Only the first marks the op as sent to a designee, even
                // when this node's table does not list it yet.
                let to_designee =
                    gen != 0 && holder != 0 && holder != from && holder != self.cfg.node_id;
                // Plan 30 §M11: the root's `NotHolder` names a delegate
                // (the op is wholly the delegate's); that is a route, not
                // the lease holder, so the cache keeps the root. A redirect that
                // carries a generation is the root naming a designee even when
                // this node's table lags: caching it would route other rids
                // to the designee unmarked (I1).
                let names_delegate = holder != 0
                    && (gen != 0 || replica.delegation_table().iter().any(|e| e.node == holder));
                if holder != 0 && !names_delegate {
                    self.lease.name_holder(holder);
                }
                // M12 round 2: this node's table names `from` the owner of
                // the op's keys (a subtree just delegated to it) but `from` answered `NotHolder` — its own table has
                // not applied the grant yet. Executing here now would
                // recall the grant this node just gave (the root's own
                // create 3 ms after a grant); ask again in a
                // stream tick instead, like a `Held` (the backoff retries
                // the delegate path first, then the forward).
                let grant_lags = self.cfg.delegation
                    && from != self.cfg.node_id
                    && self.clients.get(&rid).is_some_and(|c| {
                        matches!(
                            replica.resolve_ownership(&super::holder::keys_of_op_in(&c.op, replica)),
                            Ownership::Delegated(d) if d.node == from
                        )
                    });
                if grant_lags {
                    self.stats.deleg_grant_lag_retries += 1;
                    let timer = self.set_timer_wait(
                        now.wait.plus(self.cfg.delegation_stream_tick_ms.max(10)),
                        Timer::ForwardBackoff(rid),
                        out,
                    );
                    if let Some(c) = self.clients.get_mut(&rid) {
                        c.phase = Phase::Backoff;
                        c.timer = Some(timer);
                    }
                    return;
                }
                let c = self.clients.get_mut(&rid).expect("present");
                if holder != 0 && holder != from && holder != self.cfg.node_id && c.redirected < 2 {
                    c.redirected += 1;
                    if to_designee {
                        c.sent_to_delegate = true;
                        c.redirected_to_designee = true;
                    }
                    self.stats.forward_redirects += 1;
                    self.send_forward(now, rid, holder, gen, replica, out);
                } else {
                    self.lease_path(now, rid, replica, out);
                }
            }
            MutateOutcome::Exists { ref records, epoch } => {
                // Plan 30 §M6: `Exists` is the general rule plus a hint.
                // Same base rule as a shadow: the entry was read on the
                // holder's state, which this replica must have applied.
                // Installed, the hint covers the entry's keys up to the
                // reply's position; otherwise the refusal observed state
                // this replica lacks, and reads wait for it.
                let floor = position.hint_floor();
                let mut hinted = false;
                if base_ok && applied < floor {
                    // Not installed (`Ok(false)`): the refusal is here
                    // already — streamed ahead of the log, with what the
                    // holder did after it — so the hint is stale (backup
                    // sim seed 600396); the refusal is uncovered, as below.
                    match replica.install_hint(rid, records, position, epoch, gen, committed) {
                        Ok(true) => {
                            self.stats.hints_installed += 1;
                            self.note_ungated_shadow(from, rid, committed, gen, replica);
                            replica.note_covering(KeySet::from_records(records), position);
                            hinted = true;
                        }
                        Ok(false) => {}
                        Err(error) => tracing::warn!(%error, "failed to install hint"),
                    }
                }
                if !hinted {
                    self.observe_reply(now, rid, position, &own_chunks, replica, out);
                }
                self.finish_as(
                    now,
                    rid,
                    outcome,
                    AnswerSource::Relayed { from },
                    replica,
                    out,
                );
            }
            MutateOutcome::LockLapsed => {
                // Plan 30 §M14 phase 2: a grant the op was issued under
                // ended before it reached the executor. Nothing ran;
                // final (`EIO`), never retried by rid.
                self.stats.lock_lapsed_refusals += 1;
                self.finish_as(
                    now,
                    rid,
                    outcome,
                    AnswerSource::Relayed { from },
                    replica,
                    out,
                )
            }
            outcome @ (MutateOutcome::Errno(_)
            | MutateOutcome::Conflict { .. }
            | MutateOutcome::Abandoned) => {
                // Plan 30 §M6: a refusal observed the holder's state
                // without installing anything here (this replaces plan 29
                // M6's per-name causal wait: every later read waits).
                self.observe_reply(now, rid, position, &own_chunks, replica, out);
                self.finish_as(
                    now,
                    rid,
                    outcome,
                    AnswerSource::Relayed { from },
                    replica,
                    out,
                )
            }
        }
    }

    pub(crate) fn on_forward_timeout(
        &mut self,
        now: Now,
        rid: Rid,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(c) = self.clients.get_mut(&rid) else {
            return;
        };
        if let Phase::Forwarded { req, .. } = c.phase {
            self.by_req.remove(&req);
            c.timer = None;
            self.stats.forwards_err += 1;
            self.retry_or_lease(now, rid, replica, out);
        }
    }

    pub(crate) fn on_forward_failed(
        &mut self,
        now: Now,
        rid: Rid,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(c) = self.clients.get_mut(&rid) else {
            return;
        };
        if let Phase::Forwarded { .. } = c.phase {
            if let Some(t) = c.timer.take() {
                self.cancel_timer(t, out);
            }
            self.retry_or_lease(now, rid, replica, out);
        }
    }

    /// Plan 30 §M2: a timeout, transport failure or `Busy` leaves the op
    /// in doubt; retry the same rid a few times before the lease path
    /// (a system op gets no retries; a best-effort one is dropped).
    pub(crate) fn retry_or_lease(
        &mut self,
        now: Now,
        rid: Rid,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // EC2 campaign 8 A-1: with this node's own S3 stalled the lease
        // path waits on S3 requests that cannot complete (30 s each, then
        // the acquisition retries); while the holder's P2P link is up the
        // op keeps forwarding instead, the backoff capped, until the
        // holder answers or `s3_less_deadline_ms` ends it (`on_own_s3`).
        let keep_forwarding = self.s3_less()
            && self
                .lease
                .cached_holder
                .filter(|h| *h != self.cfg.node_id)
                .is_some_and(|h| self.reaches(now, h));
        let Some(c) = self.clients.get_mut(&rid) else {
            return;
        };
        match c.policy {
            Policy::BestEffort => {
                self.finish_in_doubt(now, rid, replica, out);
            }
            Policy::Client if c.attempts < self.cfg.forward_retries || keep_forwarding => {
                c.attempts += 1;
                self.stats.forward_retries += 1;
                if c.attempts > self.cfg.forward_retries {
                    self.stats.s3_less_retries += 1;
                }
                let delay = (self.cfg.forward_backoff_ms * u64::from(c.attempts))
                    .min(self.cfg.s3_less_retry_ms.max(self.cfg.forward_backoff_ms));
                c.phase = Phase::Backoff;
                let timer =
                    self.set_timer_wait(now.wait.plus(delay), Timer::ForwardBackoff(rid), out);
                self.clients.get_mut(&rid).expect("present").timer = Some(timer);
            }
            _ => {
                tracing::debug!(
                    node = self.cfg.node_id,
                    ?rid,
                    attempts = c.attempts,
                    "forward retries exhausted: the lease path"
                );
                self.lease_path(now, rid, replica, out)
            }
        }
    }

    pub(crate) fn on_forward_backoff(
        &mut self,
        now: Now,
        rid: Rid,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(c) = self.clients.get_mut(&rid) else {
            return;
        };
        if c.phase != Phase::Backoff {
            return;
        }
        c.timer = None;
        let held_by = c.held_by.take();
        // (A settle has no effect to order after this node's replays.)
        let settle = c.settle;
        if !settle && self.hold_for_lost_deps(now, rid, replica, out) {
            return;
        }
        // The view may have opened meanwhile (another op's acquisition
        // landed the lease here, tailed to head). The op was forwarded,
        // so it is in doubt: the coverage rule applies before executing.
        if let Some(epoch) = self.lease.new_mutation_epoch(now, &self.cfg) {
            let outcome = self.resolve_in_doubt_then_execute(now, rid, epoch, replica, out);
            self.finish(now, rid, outcome, replica, out);
            return;
        }
        // ADR-44: this node's own settle, held for a recall its shippable
        // gate sent on demand: routed as a settle again (the gate answers
        // it once the cut is journaled, S1).
        let gate_recall_wait = self
            .clients
            .get_mut(&rid)
            .is_some_and(|c| std::mem::take(&mut c.gate_recall_wait));
        if settle && gate_recall_wait {
            if let Some(c) = self.clients.get_mut(&rid) {
                c.held_by = held_by;
            }
            self.settle_route(now, rid, replica, out);
            return;
        }
        // ADR-44: an open root would execute the op here (above), after
        // recalling what its keys fall under: its delegate answered
        // `NotHolder` naming this node, or `Held`, and the backoff tries it
        // again. In a shippable gate the recall goes out now; a settle
        // waits for it here.
        if self.gate_recall_for_own(now, rid, replica, out) && settle {
            self.gate_hold_settle(now, rid, out);
            if let Some(c) = self.clients.get_mut(&rid) {
                c.held_by = held_by;
            }
            return;
        }
        // Plan 30 §M12: the op may be this node's own to execute by now
        // (a delegation of its subtree installed
        // while the forward was held: the root answers `Held` to a
        // delegate's own op until the delegate's table carries the
        // grant). The delegate path dedups by rid.
        if self.cfg.delegation {
            let own = self
                .clients
                .get(&rid)
                .map(|c| (c.op.clone(), c.deps, c.settle));
            if let Some((op, deps, settle)) = own {
                let tag = self
                    .clients
                    .get(&rid)
                    .map(|c| c.tag.clone())
                    .unwrap_or_default();
                if self.delegate_try_execute(
                    now,
                    0,
                    None,
                    rid,
                    &op,
                    deps,
                    0,
                    &tag,
                    settle,
                    Fence::default(),
                    replica,
                    out,
                ) {
                    self.stats.deleg_retry_executed += 1;
                    return;
                }
            }
        }
        // Plan 46 M0.3 (B): an op the root answered `Held` is asked again
        // there, also when this node dropped its cached holder meanwhile
        // (another request's refusal, such as a read or lock request the
        // re-adopting root answered naming no holder). The route instead read the lease (slow), sent
        // the op to the lease path and its settle outlasted its window while
        // the root, reachable all along, answered every other node's
        // (`backup-strict` 1551 with slow lease reads). The rid is the same:
        // the root answers it from its dedup, or holds it again.
        let cached = self
            .lease
            .cached_holder
            .map(|h| (h, 0))
            .or(held_by)
            .filter(|(h, _)| *h != self.cfg.node_id);
        match cached {
            Some((holder, routed)) if self.reaches(now, holder) => {
                self.send_forward(now, rid, holder, routed, replica, out)
            }
            _ => self.route(now, rid, replica, out),
        }
    }

    // ---- this node without S3 (EC2 campaign 8 A-1) ----

    /// This node's own S3 path is stalled and no continuation epoch is in
    /// force: an S3 request it makes (a lease read, an acquisition, an
    /// inbox batch) will not complete, so its ops must not wait on one.
    /// (In a continuation epoch authority moves by P2P handoff and the
    /// lease path needs no S3.)
    pub(crate) fn s3_less(&self) -> bool {
        self.own_s3.stalled && !self.epoch.open && !self.epoch.active && !self.lease.epoch_held()
    }

    /// `Event::OwnS3`, every second. While this node's S3 is stalled:
    ///
    /// - an op waiting on the lease path (it entered it before the stall
    ///   was known, or the holder answered `NotHolder`) is forwarded again
    ///   to the known holder whenever the P2P link to it is up — the
    ///   acquisition it waits on needs S3 and would hold it for one S3
    ///   retry budget (30 s) per attempt;
    /// - a client op not answered `s3_less_deadline_ms` after its
    ///   submission fails (`InDoubt`: `EIO`, retryable by rid) instead of
    ///   waiting out `acquire_deadline_ms` (2 × TTL, 120 s) — unless the
    ///   peers answered that none of them reaches S3 either (a bucket
    ///   outage, which the continuation epoch serves).
    pub(crate) fn on_own_s3(
        &mut self,
        now: Now,
        stalled: bool,
        peers_reach_s3: Option<bool>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if stalled != self.own_s3.stalled {
            if stalled {
                tracing::warn!(
                    node = self.cfg.node_id,
                    ?peers_reach_s3,
                    "this node's S3 path is stalled: ops forward over P2P and do not wait on S3"
                );
            } else {
                tracing::info!(node = self.cfg.node_id, "this node's S3 path answers again");
            }
        }
        self.own_s3 = super::OwnS3 {
            stalled,
            peers_reach_s3: if stalled { peers_reach_s3 } else { None },
            since: if stalled {
                self.own_s3.since.or(Some(now.wait))
            } else {
                None
            },
        };
        if !self.s3_less() || !self.cfg.forwarding {
            return;
        }
        if peers_reach_s3 != Some(false) {
            let over: Vec<Rid> = self
                .clients
                .iter()
                .filter(|(_, c)| {
                    c.origin == Origin::Client
                        && c.policy == Policy::Client
                        && !c.settle
                        && now.wait.since(c.submitted) >= self.cfg.s3_less_deadline_ms as i64
                })
                .map(|(rid, _)| *rid)
                .collect();
            for rid in over {
                if let Some(c) = self.clients.get(&rid) {
                    tracing::warn!(
                        node = self.cfg.node_id,
                        ?rid,
                        op = ?c.op,
                        phase = ?c.phase_kind(),
                        age_ms = now.wait.since(c.submitted),
                        "no answer while this node's S3 is stalled: the op fails (in doubt)"
                    );
                }
                self.stats.s3_less_deadlines += 1;
                self.client_deadline(now, rid, true, replica, out);
            }
        }
        if !self.cfg.p2p {
            return;
        }
        let Some(holder) = self.lease.cached_holder.filter(|h| *h != self.cfg.node_id) else {
            return;
        };
        if !self.reaches(now, holder) {
            return;
        }
        // `route`'s rule: an op of a node with unshipped journal of its
        // own may depend on records no holder has.
        if replica.journal_len().unwrap_or(0) > 0 && replica.journal_has_undelegated() {
            return;
        }
        let mut waiting: Vec<(u64, Rid)> = self
            .clients
            .iter()
            .filter(|(_, c)| {
                c.policy == Policy::Client
                    && matches!(c.phase, Phase::WaitingLease | Phase::AcquireRetry)
            })
            .map(|(rid, c)| (c.order, *rid))
            .collect();
        waiting.sort_unstable();
        for (_, rid) in waiting {
            let Some(c) = self.clients.get_mut(&rid) else {
                continue;
            };
            if !matches!(c.phase, Phase::WaitingLease | Phase::AcquireRetry) {
                continue;
            }
            // Not ahead of this node's own stranded writes (a deposed
            // node's recovery needs S3 too): held, it forwards from its
            // backoff once they have landed.
            if self.hold_for_lost_deps(now, rid, replica, out) {
                continue;
            }
            let Some(c) = self.clients.get_mut(&rid) else {
                continue;
            };
            if let Some(t) = c.timer.take() {
                self.cancel_timer(t, out);
            }
            tracing::debug!(
                node = self.cfg.node_id,
                ?rid,
                holder,
                "S3 stalled: forwarding the op again instead of waiting on the lease path"
            );
            self.stats.s3_less_forwards += 1;
            self.send_forward(now, rid, holder, 0, replica, out);
        }
    }

    // ---- the lease path ----

    /// `mutate_op_rebasable`'s `Busy | NotHolder` arm: ask for the lease
    /// and, once held, execute here — unless the rid already completed.
    pub(crate) fn lease_path(
        &mut self,
        now: Now,
        rid: Rid,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(c) = self.clients.get_mut(&rid) else {
            return;
        };
        if c.policy == Policy::BestEffort {
            self.finish_in_doubt(now, rid, replica, out);
            return;
        }
        if self.epoch_refuses_writes() {
            // Admitted before the engine's gate closed, or routed here
            // after: the lease path cannot end in this epoch.
            self.refuse_on_lease_path(now, rid, Code::ReadOnly, replica, out);
            return;
        }
        let c = self.clients.get_mut(&rid).expect("present");
        if !matches!(c.phase, Phase::WaitingLease) {
            self.stats.lease_path_taken += 1;
        }
        let from_inbox = matches!(
            c.phase,
            Phase::InboxQueued { .. } | Phase::InboxWaiting { .. }
        );
        c.phase = Phase::WaitingLease;
        let timer = c.timer.take();
        let old_deadline = c.deadline;
        if let Some(t) = timer {
            self.cancel_timer(t, out);
        }
        if from_inbox {
            // The inbox wait had its own deadline; the lease path gets
            // the ordinary one from here.
            self.cancel_timer(old_deadline, out);
            let deadline = self.set_timer_wait(
                now.wait.plus(self.cfg.acquire_deadline_ms),
                Timer::ClientDeadline(rid),
                out,
            );
            self.clients.get_mut(&rid).expect("present").deadline = deadline;
        }
        self.enqueue_job(
            now,
            super::jobs::JobReq::Acquire {
                reason: "fuse-acquire",
                ask_handoff: self.cfg.p2p,
            },
            replica,
            out,
        );
    }

    /// The acquire job ended: every op waiting for the lease either
    /// executes here (or finds itself already completed), or backs off
    /// and asks again (a system op gives up instead).
    pub(crate) fn on_acquire_finished(
        &mut self,
        now: Now,
        acquired: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let waiting: Vec<Rid> = self
            .clients
            .iter()
            .filter(|(_, c)| matches!(c.phase, Phase::WaitingLease))
            .map(|(rid, _)| *rid)
            .collect();
        let retry_hint = self.acquire_retry_hint.take();
        for rid in waiting {
            // An earlier iteration's `finish` may have released and
            // finished this one through the key gate.
            let Some(policy) = self.clients.get(&rid).map(|c| c.policy) else {
                continue;
            };
            if acquired {
                if let Some(epoch) = self.lease.new_mutation_epoch(now, &self.cfg) {
                    let outcome = self.resolve_in_doubt_then_execute(now, rid, epoch, replica, out);
                    self.finish(now, rid, outcome, replica, out);
                    continue;
                }
            }
            if policy == Policy::System {
                // One acquisition and no wait for a live holder: the op's own
                // attempts end here, and it is settled (plan 43 C2g's review)
                // — or, already settling, answered in doubt, unsettled.
                self.client_deadline(now, rid, false, replica, out);
                continue;
            }
            // Plan 46 M1.3 step 3 (bucket A): a settle whose lease was won
            // while the op still waited is the gate's to answer now.
            if self.settled_by_gate(now, rid, replica, out) {
                continue;
            }
            self.acquire_retry_hint = retry_hint;
            self.schedule_acquire_retry(now, rid, out);
        }
        self.acquire_retry_hint = None;
    }

    /// Plan 46 M1.3 step 3 (bucket A, `delegated-holder-cut` 252): this
    /// node's own settle, its lease won while the op was on the lease path or
    /// in the acquire backoff: the takeover gate (read quarantine, drain)
    /// keeps the view closed for 12 to 20 s, and the op looped "the holder is
    /// unreachable, routing again" with this node as the holder until its
    /// deadline, long after the gate's journal work was done. `settle_route`
    /// (the settle's own routing, S1) answers it on the keys the gate owns,
    /// as it does for a settle that began while the gate stood. Only where the
    /// gate answers: any other settle goes on as it did.
    fn settled_by_gate(
        &mut self,
        now: Now,
        rid: Rid,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        let Some(c) = self.clients.get(&rid) else {
            return false;
        };
        if !c.settle || c.policy != Policy::Client {
            return false;
        }
        if !matches!(
            self.gate_settle(now, self.cfg.node_id, &c.op, replica),
            Some(super::holder::GateSettle::Answer(_))
        ) {
            return false;
        }
        // (A rid that took effect here is not answered before the
        // quarantine: `settle_route` would go on to the lease path, whose
        // acquisition ends at once under the gate and calls this again.)
        if super::holder::took_effect_here(replica, rid) {
            return false;
        }
        if let Some(t) = self.clients.get_mut(&rid).and_then(|c| c.timer.take()) {
            self.cancel_timer(t, out);
        }
        self.stats.gate_settles_after_win += 1;
        self.settle_route(now, rid, replica, out);
        true
    }

    /// Answer every op on the lease path with `code` (a frozen epoch's,
    /// or a carrier-less one's, `EROFS`): an op never sent to a holder is
    /// refused; one that was sent gets the log's answer, else in doubt
    /// ([`Self::refuse_on_lease_path`]). `LearnHolder` counts: its lease
    /// read leads only to the lease path or to a forward, and in such an
    /// epoch the lease path cannot end (no write executes under it) — the
    /// read's late answer finds no op.
    pub(crate) fn refuse_waiting_for_lease(
        &mut self,
        now: Now,
        code: Code,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let waiting: Vec<Rid> = self
            .clients
            .iter()
            .filter(|(_, c)| {
                matches!(
                    c.phase,
                    Phase::LearnHolder | Phase::WaitingLease | Phase::AcquireRetry
                )
            })
            .map(|(rid, _)| *rid)
            .collect();
        for rid in waiting {
            self.refuse_on_lease_path(now, rid, code, replica, out);
        }
    }

    /// Chunk close-stall-metered (review must-fix 2): whether this node
    /// is in a continuation epoch under which no write can execute — one
    /// that froze, or one whose claim resolution carried no lease (no hold
    /// owner): the engine's write gate refuses new writes with `EROFS`
    /// (`EpochManager::writes_refused`), and an op on the lease path would
    /// otherwise wait for its 2 × TTL deadline (120 s) and end in doubt.
    ///
    /// Also one carrying this node's lease that it gave up with its
    /// release in doubt (`PromiseState::given_up`): nobody holds that
    /// epoch, nor can until it closes (this node owes the move,
    /// `owes_move`), so a write would only wait out its deadline.
    pub(crate) fn epoch_refuses_writes(&self) -> bool {
        self.epoch.frozen
            || (self.epoch.active && (self.pr.carried.is_none() || self.epoch_carried_given_up()))
    }

    /// The active epoch carries the lease this node gave up in doubt, and
    /// it holds no epoch.
    fn epoch_carried_given_up(&self) -> bool {
        !self.lease.epoch_held()
            && self.lease.held.is_none()
            && self.pr.carried.is_some_and(|c| {
                c.node == self.cfg.node_id && self.pr.given_up == Some((c.epoch, c.expires_unix_ms))
            })
    }

    /// `rid` is on the lease path in an epoch that refuses writes.
    /// Flex-crash seed 10248: an op sent anywhere before may have taken
    /// effect (a forward whose reply was lost), so "refused" would be a
    /// lie, and its resubmission would not check `completed`: it executed
    /// a second time and answered `EEXIST` for its own create. It gets the
    /// log's answer if this node has applied it (a node holding nothing
    /// has `completed` rows from applied segments only, as at the
    /// deadline); otherwise in doubt (`EIO`, retryable by rid; the
    /// resubmission is in doubt too). One never sent gets `code` — but
    /// never a stranded op's replay: refused, it would become a conflict
    /// copy of a write that was acknowledged; in doubt, it stays queued
    /// and the drain resends it once the epoch is over.
    fn refuse_on_lease_path(
        &mut self,
        now: Now,
        rid: Rid,
        code: Code,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(c) = self.clients.get(&rid) else {
            return;
        };
        let sent = c.forwarded || c.attempts > 0;
        if matches!(c.origin, Origin::Replay { .. }) {
            self.finish_in_doubt(now, rid, replica, out);
            return;
        }
        if !sent {
            // This node's own refusal (nothing journaled).
            // (Nothing journaled, nothing to commit: this node's own answer,
            // as a lone node's. Not `root_commit`, which since the C2f
            // review is `Archive` for a node that holds no lease.)
            let source = AnswerSource::Sequenced(constellation_meta::RowCommit::LONE);
            self.finish_as(now, rid, MutateOutcome::Errno(code), source, replica, out);
            return;
        }
        if let Some(outcome) = self.settled_outcome(rid, replica) {
            self.stats.forward_indoubt_resolved += 1;
            let (outcome, source) = Self::eio_unless_recorded(
                outcome,
                self.completed_source(replica, rid),
                AnswerSource::Sequenced(constellation_meta::RowCommit::LONE),
            );
            self.park_settled_acceptance(now, rid, &outcome, replica, out);
            self.finish_as(now, rid, outcome, source, replica, out);
            return;
        }
        // Sent, and no sequencer can settle it in this epoch: the unsettled
        // `EIO` (counted here unless `finish_in_doubt` counts a settle's).
        if self.clients.get(&rid).is_some_and(|c| !c.settle) {
            self.stats.client_unsettled_eio += 1;
        }
        self.finish_in_doubt(now, rid, replica, out);
    }

    fn schedule_acquire_retry(&mut self, now: Now, rid: Rid, out: &mut Vec<Action>) {
        let Some(c) = self.clients.get_mut(&rid) else {
            return;
        };
        c.acquire_retries += 1;
        let mut delay = (self.cfg.acquire_retry_min_ms << c.acquire_retries.min(10))
            .min(self.cfg.acquire_retry_max_ms);
        // Plan 43 C7b: a retry also at the instant this node's observation
        // lets it claim, when that is before the backoff's. The backoff
        // (capped at `acquire_retry_max_ms`) still bounds the polling of a
        // cold claimant, and a retry is never later than it was: the lease
        // may change hands or be released before the observation passes, and
        // the read that finds out must not wait for it.
        if let Some(at) = self.acquire_retry_hint {
            let to_claim = at.since(now.wait).max(0) as u64 + 1;
            delay = delay.min(to_claim.max(self.cfg.acquire_retry_min_ms));
        }
        c.phase = Phase::AcquireRetry;
        let timer = self.set_timer_wait(now.wait.plus(delay), Timer::AcquireRetry(rid), out);
        self.clients.get_mut(&rid).expect("present").timer = Some(timer);
    }

    pub(crate) fn on_acquire_retry(
        &mut self,
        now: Now,
        rid: Rid,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(c) = self.clients.get_mut(&rid) else {
            return;
        };
        if c.phase != Phase::AcquireRetry {
            return;
        }
        c.timer = None;
        if let Some(epoch) = self.lease.new_mutation_epoch(now, &self.cfg) {
            let outcome = self.resolve_in_doubt_then_execute(now, rid, epoch, replica, out);
            self.finish(now, rid, outcome, replica, out);
            return;
        }
        // Plan 46 M1.3 step 3 (bucket A): a settle the gate answers.
        if self.settled_by_gate(now, rid, replica, out) {
            return;
        }
        // A node's later write must not overtake its own earlier ones: the
        // replays this node has to settle first (a deposition's, queued by
        // rid) hold the op here, as they hold one that was forwarded
        // (`on_forward_backoff`); and while the deposition's journal is
        // still unshipped (the recovery round has not run) only the lease
        // path is open to it, which the deposed node's own re-acquisition
        // refusal turns into this retry. A deposed node's mutations are not
        // refused at the write gate: this is where they wait.
        if self.hold_for_lost_deps(now, rid, replica, out) {
            return;
        }
        if self.lease.lost
            && replica.journal_len().unwrap_or(0) > 0
            && replica.journal_has_undelegated()
        {
            self.lease_path(now, rid, replica, out);
            return;
        }
        // Before re-asking S3, one more look at forwarding: the holder may
        // simply have changed (the lease read told us who). Not to a holder
        // whose lease this node has itself watched go unrenewed for its TTL
        // (plan 43 C7b's claim is due): that holder is claimed, and the
        // lease read finds out if it renewed meanwhile. Forwarding to it
        // again cost an op on a dead holder's lease its forward retries
        // after the claim fell due (3.4 s of a 6 s lease's 12 s deadline),
        // and the op went in doubt. Not while this node's own S3 is stalled
        // either: the claim it would make needs S3 (the lease read would
        // wait on a dead path), and a live holder it can reach is the way
        // out (`s3_less`).
        let claim_due = !self.mode.forwards()
            && !self.cfg.single_writer
            && !self.s3_less()
            && self
                .lease
                .claim_due(&self.cfg)
                .is_some_and(|due| due <= now.wait)
            && self
                .lease
                .obs
                .as_ref()
                .is_some_and(|o| Some(o.body.holder) == self.lease.cached_holder);
        if (self.cfg.forwarding || self.mode.forwards()) && !claim_due {
            if let Some(holder) = self.lease.cached_holder {
                if holder != self.cfg.node_id && self.reaches(now, holder) {
                    let c = self.clients.get_mut(&rid).expect("present");
                    c.attempts = 0;
                    c.fence_bounces = 0;
                    c.redirected = 0;
                    self.send_forward(now, rid, holder, 0, replica, out);
                    return;
                }
            }
            // M13/M5: with a live holder this node cannot reach, the op
            // goes back through the holder's inbox (a lease re-read says
            // who and under which epoch) rather than spinning on
            // acquisitions until that holder lets go; the escalation
            // asks for the lease on the op's behalf meanwhile.
            //
            // Plan 46 M1.3 step 3 (bucket O, `delegated-holder-cut` 128): not
            // for a settle whose holder is silent and whose lease this
            // node's own observation lets it claim by TTL (`claim_due`)
            // before the op's deadline: the route read the lease (a second,
            // slow one before the job's own) and put the settle in the inbox
            // of a holder nobody reaches, a restarted one that only
            // re-adopts at a forward or its own claim; the settle waited
            // there while the claim, which is all that can end it, was a
            // read behind. It claims. (A claim that falls due after the
            // deadline helps nothing, and a live holder cut off by a
            // partition drains the inbox: `delegated-long-delegation`.)
            let until = self
                .clients
                .get(&rid)
                .filter(|c| c.settle)
                .map(|c| c.submitted.plus(self.cfg.acquire_deadline_ms));
            let settle_claims = until.is_some_and(|until| {
                self.lease
                    .claim_due(&self.cfg)
                    .is_some_and(|due| due <= until)
            }) && !self.mode.forwards()
                && !self.cfg.single_writer
                && !self.s3_less()
                && self
                    .lease
                    .obs
                    .as_ref()
                    .is_some_and(|o| Some(o.body.holder) == self.lease.cached_holder);
            if settle_claims {
                self.stats.settle_claims_not_routes += 1;
            }
            if self.cfg.inbox
                && !settle_claims
                && self
                    .clients
                    .get(&rid)
                    .is_some_and(|c| c.policy == Policy::Client)
            {
                tracing::debug!(
                    node = self.cfg.node_id,
                    ?rid,
                    holder = ?self.lease.cached_holder,
                    "acquire retry: the holder is unreachable, routing again (the inbox)"
                );
                self.route(now, rid, replica, out);
                return;
            }
        }
        self.lease_path(now, rid, replica, out);
    }

    /// The coverage rule: this node holds the lease and has tailed to the
    /// departing holder's head, so `completed` is exact — found means an
    /// earlier attempt took effect (or was refused through an inbox) and
    /// nothing runs again. An op that was never sent anywhere cannot have
    /// taken effect and skips the read (the fast path's cost).
    pub(crate) fn resolve_in_doubt_then_execute(
        &mut self,
        now: Now,
        rid: Rid,
        epoch: Epoch,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> MutateOutcome {
        let (in_doubt, settle) = self.clients.get(&rid).map_or((false, false), |c| {
            (c.forwarded || c.attempts > 0 || c.settle, c.settle)
        });
        if in_doubt {
            if let Some(outcome) =
                completed_as_outcome(replica, rid, epoch).filter(|o| *o == MutateOutcome::Abandoned)
            {
                // The rid's settle refusal (an earlier settle of it here): it
                // is answered once committed, as when it was journaled — never
                // re-tagged `Lone` for this node's own client
                // (`Core::settle_refusal`).
                self.stats.forward_indoubt_resolved += 1;
                let position = Position {
                    seq: self.ship.head_seq,
                    pending: replica.journal_position(epoch),
                    streams: Default::default(),
                };
                let class = if replica.outcome_from_log(rid) {
                    Ok(constellation_meta::RowCommit::ARCHIVE)
                } else {
                    self.journaled_answer_class_for(replica, rid, super::Asker::Remote)
                };
                let (outcome, class) =
                    Self::eio_unless_recorded(outcome, class, constellation_meta::RowCommit::LONE);
                if !replica.outcome_from_log(rid) {
                    let shipped = replica.journal_acked_seq().unwrap_or(0);
                    if let Some(need) =
                        self.answer_need(&position, class, super::Asker::Remote, shipped)
                    {
                        self.rd
                            .parked_local
                            .insert(rid, (Default::default(), Some(need)));
                    }
                }
                if let Some(c) = self.clients.get_mut(&rid) {
                    c.answered_by = Some(AnswerSource::Sequenced(class));
                }
                return outcome;
            }
            if let Some(outcome) = completed_as_outcome(replica, rid, epoch) {
                self.stats.forward_indoubt_resolved += 1;
                // Plan 30 §M9: a completion found in this node's own
                // unshipped journal is not durable yet under a
                // `Backup`/`S3` policy; the answer waits like a fresh
                // execution's would.
                let position = Position {
                    seq: self.ship.head_seq,
                    pending: replica.journal_position(epoch),
                    streams: Default::default(),
                };
                let durable = self.ack_need_of(rid, &position, replica);
                // Plan 41 P41-01: an acceptance found completed waits for the
                // read-delegation quarantine as a fresh execution's does
                // (`dedup_acceptance_wait`).
                let wait = matches!(outcome, MutateOutcome::Accepted { .. })
                    .then(|| self.dedup_acceptance_wait(now, rid, replica, out))
                    .flatten();
                if wait.is_some() || durable.is_some() {
                    self.rd
                        .parked_local
                        .insert(rid, (wait.unwrap_or_default(), durable));
                }
                let (outcome, source) = Self::eio_unless_recorded(
                    outcome,
                    self.completed_source(replica, rid),
                    AnswerSource::Sequenced(constellation_meta::RowCommit::LONE),
                );
                if let Some(c) = self.clients.get_mut(&rid) {
                    c.answered_by = Some(source);
                }
                return outcome;
            }
        }
        // (A settle has no effect to order after this node's replays.)
        if !settle && self.hold_for_lost_deps(now, rid, replica, out) {
            // Held (`finish` leaves a held op alone).
            return MutateOutcome::Held {
                retry_ms: 0,
                outwait_ms: None,
            };
        }
        self.execute_local(now, rid, epoch, replica, out)
    }

    /// Execute as holder, on this node's own behalf (`dispatch_forward`'s
    /// local branch). The op is answered as this node's own sequencing,
    /// of the class the root's answers have now (`answered_by`).
    pub(crate) fn execute_local(
        &mut self,
        now: Now,
        rid: Rid,
        epoch: Epoch,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> MutateOutcome {
        constellation_meta::clear_last_journaled();
        let outcome = self.execute_local_outcome(now, rid, epoch, replica, out);
        let (outcome, class) = Self::eio_unless_recorded(
            outcome,
            self.executed_answer_class(replica, rid),
            constellation_meta::RowCommit::LONE,
        );
        if let Some(c) = self.clients.get_mut(&rid) {
            c.answered_by = Some(AnswerSource::Sequenced(class));
        }
        outcome
    }

    fn execute_local_outcome(
        &mut self,
        now: Now,
        rid: Rid,
        epoch: Epoch,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> MutateOutcome {
        let Some((op, policy, deps, tag, settle)) = self
            .clients
            .get(&rid)
            .map(|c| (c.op.clone(), c.policy, c.deps, c.tag.clone(), c.settle))
        else {
            // Already finished by a nested pass (see `release_gated`);
            // the outcome goes to `finish`, which drops it.
            return MutateOutcome::Errno(Code::Io);
        };
        // Plan 46 M0.2 (review MUST 1): this node's own op stands behind the
        // replay queue on its keys as another node's does (R3): what the
        // queue holds ran before it, and a placeholder's requester is
        // retrying a row that stood ahead of it. A replay is the queue's
        // own and a settle executes nothing; both go on.
        if !settle
            && !matches!(
                self.clients.get(&rid).map(|c| c.origin),
                Some(Origin::Replay { .. })
            )
            && self.queue_gate_blocks(rid, &op, replica)
        {
            self.stats.ops_parked_behind_queue += 1;
            self.stats.deleg_exec_parked += 1;
            self.park_exec_local(now, Default::default(), None, rid, true);
            return MutateOutcome::Busy;
        }
        // Plan 30 §M11: as the root, recall the write delegations the
        // op's keys fall under first, and wait for its `deps`; `finish`
        // skips this rid until the parked continuation executes it.
        if self.cfg.delegation {
            self.deleg_root_sync_due(now, replica, out);
            let keys = super::holder::keys_of_op_in(&op, replica);
            tracing::debug!(
                node = self.cfg.node_id,
                ?rid,
                gens = self.dl.gens.len(),
                ownership = ?replica.resolve_ownership(&keys),
                "root executes locally"
            );
            // Plan 46 M0.3 (S5, ADR-42): a settle executes nothing, and the
            // takeover gate ends its own generations without a recall
            // (`delegation_sync` skips their rows until it opens): the plan
            // would wait on a generation `start_recall` does not know until
            // the gate opens. A gated root's settle is answered by
            // `gate_settle` on keys no generation of another node owns.
            let wait = if !self.deleg_known() || (settle && self.lease.gate.is_some()) {
                Default::default()
            } else {
                match self.deleg_recall_plan(now, &keys, replica, out) {
                    super::delegate::RecallPlan::None => Default::default(),
                    super::delegate::RecallPlan::Wait(w) => w,
                    super::delegate::RecallPlan::Refuse(code) => {
                        // An op already sent on (a forward to the holder
                        // whose answer did not come) may have run there: a
                        // refusal for good would tell the application it did
                        // not. It waits for the holder's word instead: its row
                        // (the rid's completion, which ends the wait), its
                        // release or the stream's end; else the client's
                        // deadline answers it in doubt.
                        if !self.cfg.refuse_forwarded_designation_ops
                            && self
                                .clients
                                .get(&rid)
                                .is_some_and(|c| c.forwarded || c.attempts > 0)
                        {
                            let gens: Vec<u64> = match replica.resolve_ownership(&keys) {
                                constellation_meta::delegation::Ownership::Delegated(d) => {
                                    vec![d.gen]
                                }
                                constellation_meta::delegation::Ownership::CrossSubtree {
                                    involved,
                                    ..
                                } => involved.iter().map(|d| d.gen).collect(),
                                constellation_meta::delegation::Ownership::Root => Vec::new(),
                            };
                            let wait: BTreeSet<u64> =
                                gens.into_iter().map(super::delegate::wait_id).collect();
                            if !wait.is_empty() {
                                if let Some(c) = self.clients.get_mut(&rid) {
                                    c.designation_held = true;
                                }
                                self.park_exec_local(now, (wait, None), None, rid, false);
                                return MutateOutcome::Busy;
                            }
                        }
                        self.note_final_nonholder_refusal(rid, code, false);
                        return MutateOutcome::Errno(code);
                    }
                    // The gate recalls nothing (ADR-42): retried once it opens.
                    super::delegate::RecallPlan::Gated => return MutateOutcome::Busy,
                }
            };
            let deps_wait = (!settle && !replica.reaches_streams(&deps)).then_some(deps);
            if !wait.is_empty() || deps_wait.is_some() {
                if deps_wait.is_some() {
                    self.stats.deleg_deps_waits += 1;
                }
                tracing::debug!(
                    node = self.cfg.node_id,
                    ?rid,
                    ?wait,
                    ?deps_wait,
                    applied = ?replica.applied_position(),
                    "root parks a local execution"
                );
                self.stats.deleg_exec_parked += 1;
                self.park_exec_local(now, (wait, None), deps_wait, rid, false);
                return MutateOutcome::Busy;
            }
        }
        // Plan 30 §M8, option (c) (review S1): the root's own op recalls
        // every read delegation that misses it *before* it executes, as a
        // forwarded one does (`holder.rs`, `recall_before_execute`): a root
        // deposed while its honour still runs (a seal of a live holder)
        // must not answer its own client from a row that reached the log
        // while a grant of this root still serves the old state. Not for a
        // settle (it executes nothing). A retry the dedup answers never gets
        // here (`resolve_in_doubt_then_execute`). Parked as a delegation
        // recall's execution is (`ExecuteLocal`): re-run once the recall is
        // acked or the grant outwaited, or the lease path if it is lost.
        // Only the grants that miss it, not the quarantine: the op's own
        // acknowledgement waits for that after the execution, and no other
        // node answers this node's rid from the log (the backup-strict
        // seeds 892, 894 of `+unsettled` settled `EIO` otherwise: parked
        // through a long quarantine, past the client's settle window).
        let touched = if !settle && self.cfg.recall_before_ack {
            replica.recall_inos_of_op(&op)
        } else {
            Vec::new()
        };
        if !touched.is_empty() {
            let me = self.cfg.node_id;
            if let Some(wait) = self.recall_before_execute(now, me, &touched, false, replica, out) {
                self.stats.local_recalls_first += 1;
                self.stats.deleg_exec_parked += 1;
                self.park_exec_local(now, wait, None, rid, false);
                return MutateOutcome::Busy;
            }
        }
        if self.cfg.placement && self.cfg.delegation {
            let dirs = Core::dirs_of_keys(&super::holder::keys_of_op(&op), replica);
            self.place_note(self.cfg.node_id, dirs);
        }
        let _class = self.journal_class();
        // Plan 43 C2 (§5.3 item 5): this node settles its own op as the root
        // (`resolve_in_doubt_then_execute` found no outcome): refused for
        // good, answered once the refusal's row is committed (below).
        let outcome = if settle {
            match self.settle_refusal(rid, Some(&op), super::Asker::Own, replica) {
                // (Not journaled: the requester retries the settle.)
                MutateOutcome::Busy => return MutateOutcome::Busy,
                settled => {
                    // Its answer waits for the log: start a round now, not
                    // at the idle backoff's next one.
                    self.nudge(now, out);
                    (settled, None)
                }
            }
        } else {
            let executed = constellation_meta::locks::TokenPath::Local
                .scope(|| replica.execute(&op, Some(rid), &tag, now));
            // The op ran (or was refused): grants on its inodes may resume.
            if !touched.is_empty() {
                self.recall_unblock(now, &touched);
            }
            match executed {
                Ok(records) => {
                    if policy == Policy::Client {
                        self.lease.touch(now);
                    }
                    self.nudge(now, out);
                    // Plan 30 §M8: the sequencer's own writes recall read
                    // delegations too; `finish` parks the reply until done.
                    // (`_executed`: plus what an unlink or rename changed
                    // that its records do not name.)
                    let inos = constellation_meta::recall_inos_executed(&records);
                    let wait = self.recall_needed(now, &inos, None, replica, out);
                    (MutateOutcome::Accepted { epoch, records }, wait)
                }
                // A stale base or an ended grant is a definitive answer (a
                // client rebases or reissues under a fresh rid; a replay makes
                // its conflict copy): the rid's outcome, journaled like any
                // refusal (`Code::Stale`: a dedup answers it as the conflict it
                // was), as the inbox drain journals it. Unrecorded, a copy of
                // the op queued by the same rid on another node (as `Records`,
                // which apply without the base check or the token) could still
                // run it.
                Err(MetaError::Conflict) => {
                    if matches!(op, MutateOp::SetManifest { .. }) {
                        self.record_refusal(
                            rid,
                            Code::Stale,
                            Some(&op),
                            super::Asker::Own,
                            replica,
                        );
                    }
                    (MutateOutcome::Conflict { manifest: None }, None)
                }
                // Plan 30 §M14 phase 2: the op's lock grant ended; nothing ran
                // but the refusal's row. That row is the rid's outcome, so the
                // answer waits for its commit as every refusal's does (below;
                // plan 46 I1: answered at once, it claimed a commit nothing
                // backed).
                Err(MetaError::LockLapsed) => {
                    self.stats.lock_lapsed_refusals += 1;
                    self.record_refusal(rid, LAPSED_CODE, Some(&op), super::Asker::Own, replica);
                    (MutateOutcome::LockLapsed, None)
                }
                Err(error) => {
                    // Plan 30 §M9: a definitive refusal is journaled as an
                    // outcome (`record_refusal`), for the same reason as a
                    // forwarded op's — and so that the refusal's position is
                    // never "nothing unshipped": under `Backup`/`S3` the
                    // answer then waits for that row to reach the backup /
                    // the log, which is what proves this node still holds.
                    // Without it a holder taken over fast (`ack=s3`, a seal)
                    // could answer a refusal from its stale replica at once
                    // (long-acks3 seed 50277: EEXIST for a name a newer
                    // holder had already renamed away).
                    let code = error.code();
                    self.record_refusal(rid, code, Some(&op), super::Asker::Own, replica);
                    (MutateOutcome::Errno(code), None)
                }
            }
        };
        // Plan 30 §M9: accepted or refused, the reply observed the
        // journal as it stands; under a `Backup`/`S3` policy it leaves
        // only once that is durable.
        let position = Position {
            seq: self.ship.head_seq,
            pending: replica.journal_position(epoch),
            streams: Default::default(),
        };
        // (A settle's refusal waits for its commit like another node's
        // answer: never the lone shortcut, `settle_refusal`.)
        let durable = if settle {
            let shipped = replica.journal_acked_seq().unwrap_or(0);
            let class = self.root_commit(super::Asker::Remote);
            self.answer_need(&position, class, super::Asker::Remote, shipped)
        } else {
            self.ack_need(&position)
        };
        let (outcome, wait) = outcome;
        if wait.is_some() || durable.is_some() {
            self.rd
                .parked_local
                .insert(rid, (wait.unwrap_or_default(), durable));
        }
        outcome
    }

    // ---- finishing ----

    /// Chunk metered-own-rows: what of the reply's position this node
    /// already has. The sequencer named its own transactions still
    /// unshipped through the position (`OwnRows`): a `back` close answered
    /// at once and deferred there behind its chunk, held here on a metered
    /// network. Those whose effects this replica carries (or that have
    /// none: refusals) are excused from the watermark the reply raises
    /// (kept with the op: `observe` uses them, now or once the log answers
    /// an `AwaitingLog` op), and nothing is uploaded for them; what the
    /// rest wait for — this op's own transaction, an earlier incarnation's,
    /// any other node's op deferred behind this node's chunks — still is.
    /// Returns `own_chunks` narrowed to that (`None` when nothing is left).
    ///
    /// Safe because every row through the position is either at most the
    /// log position a read waits for, or one of this node's own whose
    /// effect is in this replica already, in the sequencer's order: the
    /// sequencer evaluated the reply on its journal, this node's rows
    /// included, and installed them here on the same state (an accepted
    /// reply is installed only on its `base`, and only a transaction the
    /// sequencer ships later than the rows below it can be deferred). Every
    /// other node's row is still waited for; one deferred behind this
    /// node's chunks makes this node upload them (`OwnRows::others`). A
    /// shadow rolled back later (a deposition) queues its replay, which
    /// every read of its keys waits for.
    fn settle_own_rows(
        &mut self,
        rid: Rid,
        own_chunks: OwnChunks,
        own_rows: Option<&constellation_meta::OwnRows>,
        replica: &dyn Replica,
    ) -> OwnChunks {
        let Some(rows) = own_rows else {
            if let Some(c) = self.clients.get_mut(&rid) {
                c.excused.clear();
            }
            return own_chunks;
        };
        let rids: Vec<Rid> = rows
            .txs
            .iter()
            .filter(|t| t.effect)
            .map(|t| t.rid)
            .collect();
        let installed = replica.effects_installed(&rids);
        let (excused, upload) = rows.settle(|r| installed.contains(&r));
        if !excused.is_empty() {
            self.stats.own_rows_excused += 1;
        }
        if let Some(c) = self.clients.get_mut(&rid) {
            c.excused = excused;
        }
        match own_chunks {
            OwnChunks::None => OwnChunks::None,
            _ if upload.is_empty() => OwnChunks::None,
            OwnChunks::Streamed(_) => OwnChunks::Streamed(upload),
            OwnChunks::Upload(_) => OwnChunks::Upload(upload),
        }
    }

    /// [`Self::observe`] a reply's position, for an op that finishes
    /// with it. Chunk close-stall-followup: every later read here waits
    /// until the log reaches that position, and the sequencer said whether
    /// that waits for this node's own held chunks (`OwnChunks`, through
    /// the position: a `back` close of this node's answered at once and
    /// still deferred there keeps every segment's `through` below it).
    /// `Upload`: upload them now, as for an op's own record; nothing else
    /// would on a metered network, and the reads would wait out the
    /// session budget until the watermark's TTL drops it. `Streamed`: the
    /// stream brings the position here. Chunk metered-own-rows: not when
    /// the watermark is reached already (the rows are here, streamed or
    /// excused), and repeated while it stays unreached
    /// (`Core::on_observed_upload`: a pass that failed).
    fn observe_reply(
        &mut self,
        now: Now,
        rid: Rid,
        position: Position,
        own_chunks: &OwnChunks,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let client = self
            .clients
            .get(&rid)
            .is_some_and(|c| c.origin == Origin::Client);
        self.observe(rid, position, replica);
        if let (true, OwnChunks::Upload(inos)) = (client, own_chunks) {
            if self.cfg.own_record_wait_ms > 0 && !inos.is_empty() && !replica.reaches_observed() {
                self.stats.own_record_uploads += 1;
                tracing::debug!(
                    node = self.cfg.node_id,
                    ?rid,
                    ?inos,
                    "an observed position waits for this node's pending chunks: uploading them"
                );
                out.push(Action::UploadAwaited { inos: inos.clone() });
                self.repeat_observed_upload(now, inos, out);
            }
        }
    }

    /// Chunk metered-own-rows (review nit): repeat an observed position's
    /// upload every `own_record_wait_ms` while the watermark stays
    /// unreached, as an op's own-record upload is: one pass that failed
    /// (S3 away a moment) left the reads waiting out the watermark's TTL.
    /// A pass with nothing left to upload costs an index lookup per
    /// inode. At most [`OBSERVED_UPLOAD_REPEATS`] times per raise.
    fn repeat_observed_upload(&mut self, now: Now, inos: &[Ino], out: &mut Vec<Action>) {
        let u = &mut self.observed_upload;
        u.inos.extend(inos.iter().copied());
        u.repeats = 0;
        if u.timer.is_none() {
            let t = self.set_timer_wait(
                now.wait.plus(self.cfg.own_record_wait_ms),
                Timer::ObservedUpload,
                out,
            );
            self.observed_upload.timer = Some(t);
        }
    }

    /// `Timer::ObservedUpload` (see [`Self::repeat_observed_upload`]).
    pub(crate) fn on_observed_upload(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.observed_upload.timer = None;
        let u = &mut self.observed_upload;
        if u.inos.is_empty() || replica.reaches_observed() || u.repeats >= OBSERVED_UPLOAD_REPEATS {
            *u = ObservedUpload::default();
            return;
        }
        u.repeats += 1;
        let inos: Vec<Ino> = u.inos.iter().copied().collect();
        self.stats.own_record_uploads += 1;
        tracing::debug!(
            node = self.cfg.node_id,
            ?inos,
            "an observed position still waits for this node's pending chunks: uploading again"
        );
        out.push(Action::UploadAwaited { inos });
        let t = self.set_timer_wait(
            now.wait.plus(self.cfg.own_record_wait_ms),
            Timer::ObservedUpload,
            out,
        );
        self.observed_upload.timer = Some(t);
    }

    /// Plan 30 §M6: a client-visible reply to `rid` observed `position`
    /// without installing its effects here: raise `observed`, less the
    /// rows of this node's own its reply excused (`settle_own_rows`). A
    /// replay's outcome is not client-visible and raises nothing.
    fn observe(&self, rid: Rid, position: Position, replica: &dyn Replica) {
        if let Some(c) = self
            .clients
            .get(&rid)
            .filter(|c| c.origin == Origin::Client)
        {
            replica.raise_observed_excusing(position, &c.excused);
        }
    }

    /// Plan 31 C8 × write-back (chunk close-stall-metered): this node's
    /// forwarded op waits for its own transaction — accepted on state
    /// this replica has not applied (`AwaitingLog`), or answered `Held`
    /// while the holder's acknowledgement waits for durability — and the
    /// sequencer said what it waits for from here (`OwnChunks`,
    /// `Core::own_chunks_for`). A `--write-mode back` close forwards its
    /// manifest with chunks still pending here (`meta::store::remote`);
    /// the sequencer ships that transaction, and every later one that
    /// depends on it (a `chmod` or a `rename` of the same file, plan 30
    /// §M4's key dependence), only once this node reports the chunks up,
    /// and with the upload hold on (a metered network) this node's rounds
    /// upload nothing, so the op used to wait out the whole forward
    /// deadline and end in doubt (chunk 39b on real S3; the review's
    /// `chmod` right after the close). Any op, not only the close: the
    /// answer names the inodes whose chunks it waits for.
    ///
    /// - `Upload`: nothing but this node's upload brings the transaction
    ///   back (no backup holds it, so no pre-S3 stream carries it;
    ///   `ack=s3`; the stream stops before it at a chunk this node lacks;
    ///   a delegate's execution): upload the inodes' pending chunks now
    ///   (`Action::UploadAwaited`). That is a durability need of this op,
    ///   as an `fsync`'s is, and the hold exempts it.
    /// - `Streamed`: the sequencer's stream carries it here past those
    ///   chunks, which stay held. Only a stream that stalls (a backup
    ///   lost after the reply) leaves the op waiting: past
    ///   `own_record_wait_ms` the safety timer uploads them anyway.
    /// - `None`: nothing of this node's is pending in what it waits for.
    ///
    /// After an upload the timer repeats it every `own_record_wait_ms`
    /// while the op still exists (a pass that failed). The op's phase is
    /// not consulted: whatever it waits in after this (a retry, a new
    /// holder, the lease path) its transaction still needs the same
    /// chunks in S3 before any sequencer ships it, and a pass with
    /// nothing left to upload costs one index lookup per inode in the
    /// driver. The timer is cancelled when the op finishes.
    pub(crate) fn await_own_records(
        &mut self,
        now: Now,
        rid: Rid,
        own_chunks: OwnChunks,
        out: &mut Vec<Action>,
    ) {
        let wait = self.cfg.own_record_wait_ms;
        // `0` turns the whole mechanism off (the stall comes back: for
        // reproducing it).
        if wait == 0 {
            return;
        }
        let Some(c) = self.clients.get_mut(&rid) else {
            return;
        };
        let (inos, upload) = match own_chunks {
            OwnChunks::None => return,
            OwnChunks::Streamed(inos) => (inos, false),
            OwnChunks::Upload(inos) => (inos, true),
        };
        let fresh = inos.iter().any(|ino| !c.own_record_inos.contains(ino));
        c.own_record_inos.extend(inos);
        if fresh {
            c.own_record_uploaded = false;
        }
        if upload {
            if !c.own_record_uploaded {
                self.upload_own_records(now, rid, out);
            }
        } else if c.own_record_timer.is_none() {
            let t = self.set_timer_wait(now.wait.plus(wait), Timer::OwnRecordWait(rid), out);
            self.clients
                .get_mut(&rid)
                .expect("present")
                .own_record_timer = Some(t);
        }
    }

    /// `Timer::OwnRecordWait`: the op still waits (a stale fire finds no
    /// op: the timer is cancelled when it finishes).
    pub(crate) fn on_own_record_wait(&mut self, now: Now, rid: Rid, out: &mut Vec<Action>) {
        let Some(c) = self.clients.get_mut(&rid) else {
            return;
        };
        c.own_record_timer = None;
        self.upload_own_records(now, rid, out);
    }

    fn upload_own_records(&mut self, now: Now, rid: Rid, out: &mut Vec<Action>) {
        let Some(c) = self.clients.get_mut(&rid) else {
            return;
        };
        if c.own_record_inos.is_empty() {
            return;
        }
        let inos: Vec<Ino> = c.own_record_inos.iter().copied().collect();
        c.own_record_uploaded = true;
        let old = c.own_record_timer.take();
        if let Some(old) = old {
            self.cancel_timer(old, out);
        }
        self.stats.own_record_uploads += 1;
        tracing::debug!(
            node = self.cfg.node_id,
            ?rid,
            ?inos,
            "forwarded op waits for its own transaction: uploading the pending chunks it needs"
        );
        out.push(Action::UploadAwaited { inos });
        let wait = self.cfg.own_record_wait_ms;
        if wait > 0 {
            let t = self.set_timer_wait(now.wait.plus(wait), Timer::OwnRecordWait(rid), out);
            self.clients
                .get_mut(&rid)
                .expect("present")
                .own_record_timer = Some(t);
        }
    }

    /// Whether the pre-S3 stream installed `rid`'s transaction here
    /// (adopting it as this node's op): then an accepted reply this
    /// replica could not install as a shadow is answered at once.
    ///
    /// A delegate's reply too (its position names the delegation stream):
    /// the root appends a generation's stream in index order, after the
    /// `deps` of each transaction and after everything it had streamed to
    /// the delegate, so the root's journal through the transaction
    /// completing `rid` holds everything the delegate evaluated it
    /// against — the delegate's own earlier transactions, root rows it
    /// held only as streamed speculation, the log it had applied. The
    /// pre-S3 stream installs that journal here contiguously from the
    /// applied log, in order, so the op's effect sits exactly where the
    /// log will put it. (A first version of this rule, with the root's
    /// stream alone, failed `delegated-holder-cut` seed 1607: a takeover
    /// re-shipped the predecessor's appends of delegate streams without
    /// their origin and behind the successor's own delegate rows, so the
    /// log's order differed from the journal's the stream had carried;
    /// both are fixed in the takeover, see `apply_backup_tail` and
    /// `Meta::strand_for_takeover`.)
    fn adopt_streamed(&mut self, rid: Rid, position: &Position, replica: &dyn Replica) -> bool {
        let Some((op, tag)) = self
            .clients
            .get(&rid)
            .map(|c| (c.op.clone(), c.tag.clone()))
        else {
            return false;
        };
        match replica.adopt_streamed(rid, &op, &tag) {
            Ok(true) => {
                tracing::debug!(
                    node = self.cfg.node_id,
                    ?rid,
                    "accepted forward answered from the pre-S3 stream"
                );
                self.stats.awaited_log_streamed += 1;
                if self.epoch.open {
                    self.stats.epoch_forwards_streamed += 1;
                }
                if !position.streams.is_empty() {
                    self.stats.awaited_log_streamed_deleg += 1;
                }
                true
            }
            Ok(false) => false,
            Err(error) => {
                tracing::warn!(%error, node = self.cfg.node_id, ?rid, "failed to adopt a streamed transaction");
                false
            }
        }
    }

    /// Plan 30 §M9 (the OVH run's findings 4 and 6): the holder's pre-S3
    /// stream just installed the transactions completing `installed`
    /// (with their keys): every op of this node waiting for the log
    /// among them — a forward accepted behind unshipped work on its keys,
    /// as a close after a create or a `utimensat` after a close from a
    /// non-owning node is — has its effect here now, in the holder's
    /// order: answer it, as an installed shadow would be. Before, it
    /// waited for the segment to reach S3 and come back, one S3 round
    /// trip per such op although the stream had delivered it.
    pub(crate) fn answer_awaiting_streamed(
        &mut self,
        now: Now,
        installed: &[(Rid, KeySet)],
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        for (rid, keys) in installed {
            let Some((epoch, position)) = self.clients.get(rid).and_then(|c| match c.phase {
                Phase::AwaitingLog { epoch, position } => Some((epoch, position)),
                _ => None,
            }) else {
                continue;
            };
            if !self.adopt_streamed(*rid, &position, replica) {
                continue;
            }
            replica.note_covering(keys.clone(), position);
            self.finish(
                now,
                *rid,
                MutateOutcome::Accepted {
                    epoch,
                    records: Vec::new(),
                },
                replica,
                out,
            );
        }
    }

    /// After segments were applied: answer every op waiting for the log
    /// whose completion is now here — a forward accepted on a stale base,
    /// or an inbox op whose outcome the holder shipped.
    pub(crate) fn answer_awaiting_log(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let waiting: Vec<(Rid, Epoch, Option<Position>)> = self
            .clients
            .iter()
            .filter_map(|(rid, c)| match c.phase {
                Phase::AwaitingLog { epoch, position } => Some((*rid, epoch, Some(position))),
                Phase::InboxWaiting { epoch } | Phase::InboxQueued { epoch } => {
                    Some((*rid, epoch, None))
                }
                _ => None,
            })
            .collect();
        // Plan 46 M0.3 (S5, ADR-42): inside a takeover gate the replica's
        // `completed` also holds what the gate re-appended to its journal
        // (the predecessor's tail, a delegate's rows): the journal's, not
        // the log's yet (I1: an inbox op answered from the re-shipped tail
        // before it shipped). The node's own ship answers it (`ship_landed`
        // calls this once rows are in the log), or the open
        // (`resolve_moot_waits`).
        let gated_journal: Option<std::collections::BTreeSet<Rid>> =
            (self.lease.gate.is_some() && !waiting.is_empty()).then(|| {
                replica
                    .journal_txs_from(0, usize::MAX)
                    .iter()
                    .flat_map(|tx| &tx.records)
                    .filter_map(|rec| match rec {
                        LogRecord::Completed { rid } | LogRecord::Refused { rid, .. } => Some(*rid),
                        _ => None,
                    })
                    .collect()
            });
        for (rid, epoch, position) in waiting {
            if gated_journal.as_ref().is_some_and(|j| j.contains(&rid)) {
                continue;
            }
            let Some(outcome) = completed_as_outcome(replica, rid, epoch) else {
                continue;
            };
            // Not from this delegate's own uncommitted row (D6): the log's
            // answer is still to come.
            if !self.completed_row_settled(rid, replica) {
                continue;
            }
            match position {
                // Plan 30 §M6: normally already dominated by the applied
                // position (the segment carrying the completion ships
                // everything before it), unless an M4 held row keeps the
                // shipped-through position below it.
                Some(position) => {
                    self.observe(rid, position, replica);
                    // The outcome is the log's, which need not be what the
                    // sequencer said (long-delegated seed 1297: a delegate's
                    // tentative acceptance, its generation ended, the rid
                    // run again and refused): the client hears what the log
                    // carries.
                    if let Some(c) = self.clients.get_mut(&rid) {
                        c.answered_by = Some(AnswerSource::Sequenced(
                            constellation_meta::RowCommit::ARCHIVE,
                        ));
                    }
                }
                // M13: an inbox outcome rides the log behind every row it
                // was evaluated against; nothing to raise. Read from the
                // log: the archive's (the sequencer that drained the batch
                // was judged where it journaled the row, but its answer
                // reached this node only through the log).
                None => {
                    self.inbox_answered(now, rid);
                    if let Some(c) = self.clients.get_mut(&rid) {
                        c.answered_by = Some(AnswerSource::Sequenced(
                            constellation_meta::RowCommit::ARCHIVE,
                        ));
                    }
                }
            }
            self.finish(now, rid, outcome, replica, out);
        }
    }

    /// After every event: once this node holds an S3 lease with its view
    /// open, the waits a takeover makes moot end at once instead of at
    /// their own timers — above all an op of this node that some earlier
    /// holder accepted on a base this replica had not applied
    /// (`AwaitingLog`). The acquisition tailed the log to head and
    /// applied the predecessor's backup tail before the view opened, so
    /// `completed` is exact for every epoch below this one: the rid's
    /// outcome is there (answered from it, parked for durability like a
    /// fresh execution's), or it never landed and cannot any more (the
    /// predecessor's later segments are fenced by the marker), so it
    /// executes here by rid. Before, such an op waited for a log that
    /// would never carry it, up to the client deadline, and the client
    /// heard in doubt (`EIO`) for a write that had landed.
    ///
    /// Also: an inbox op durable under an older epoch (the gate drained
    /// that epoch's inbox, so the same rule holds; before, it waited for
    /// the next recheck), and an op backing off between acquisitions.
    /// Not: a continuation-epoch hold (S3 away, `completed` not exact);
    /// an op awaiting the log under this very epoch (a delegate's
    /// stream: the log carries it); a forward in flight or backing off
    /// (its own short timer checks the view first).
    pub(crate) fn resolve_moot_waits(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.clients.is_empty() || self.lease.epoch_held() {
            return;
        }
        let Some(held) = self.lease.held.as_ref().map(|(l, _)| l.epoch) else {
            return;
        };
        if self.lease.new_mutation_epoch(now, &self.cfg) != Some(held) {
            return;
        }
        let epoch = held;
        let moot = |phase: &Phase| match *phase {
            Phase::AwaitingLog { epoch: e, .. } | Phase::InboxWaiting { epoch: e } => e < epoch,
            Phase::AcquireRetry => true,
            _ => false,
        };
        let mut rids: Vec<(u64, Rid)> = self
            .clients
            .iter()
            .filter(|(_, c)| moot(&c.phase))
            .map(|(rid, c)| (c.order, *rid))
            .collect();
        if rids.is_empty() {
            return;
        }
        rids.sort_unstable();
        for (_, rid) in rids {
            // An earlier iteration's `finish` may have released or
            // finished this one; the view may have closed.
            let Some(c) = self.clients.get_mut(&rid) else {
                continue;
            };
            if !moot(&c.phase) {
                continue;
            }
            if self.lease.new_mutation_epoch(now, &self.cfg) != Some(epoch) {
                return;
            }
            let c = self.clients.get_mut(&rid).expect("present");
            let kind = c.phase_kind();
            if let Some(t) = c.timer.take() {
                self.cancel_timer(t, out);
            }
            self.inbox.pending.remove(&rid);
            match kind {
                ClientPhase::AwaitingLog => self.stats.awaiting_log_resolved += 1,
                ClientPhase::InboxWaiting => self.stats.inbox_waits_resolved += 1,
                _ => {}
            }
            tracing::debug!(
                node = self.cfg.node_id,
                ?rid,
                ?kind,
                epoch,
                "this node holds: resolving a wait its acquisition made moot"
            );
            let outcome = self.resolve_in_doubt_then_execute(now, rid, epoch, replica, out);
            self.finish(now, rid, outcome, replica, out);
        }
    }

    /// After every event, on a node that does not hold the lease: an op
    /// accepted by an earlier holder on a base this replica had not
    /// applied (`AwaitingLog`) whose epoch the log has since moved past
    /// without its completion (`answer_awaiting_log` answers those that
    /// came). The log is applied in order, so the dead holder's epoch is
    /// all here and the acceptance never shipped — unless a sealed
    /// backup's tail re-ships it under the new epoch, which the new
    /// holder's gate applies before it answers. Either way it is the new
    /// holder's to settle: the op goes there again by rid (in doubt:
    /// answered from `completed`, or executed once). Before, it waited for
    /// the client deadline (40 s) and its client heard in doubt (`EIO`);
    /// only a node that took the lease itself resolved it
    /// (`resolve_moot_waits`). Not before this node's own stranded ops
    /// are replayed: those came first, and the op may depend on them.
    pub(crate) fn reroute_ended_epoch_waits(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.clients.is_empty() || self.lease.held.is_some() || self.lease.epoch_held() {
            return;
        }
        let max = self.ship.max_epoch;
        let ended =
            |c: &ClientOp| matches!(c.phase, Phase::AwaitingLog { epoch, .. } if epoch < max);
        if !self.clients.values().any(ended) {
            return;
        }
        let mut rids: Vec<(u64, Rid, Epoch)> = self
            .clients
            .iter()
            .filter_map(|(rid, c)| match c.phase {
                Phase::AwaitingLog { epoch, .. } if epoch < max => Some((c.order, *rid, epoch)),
                _ => None,
            })
            .collect();
        rids.sort_unstable();
        for (_, rid, epoch) in rids {
            if !self.clients.get(&rid).is_some_and(&ended) {
                continue;
            }
            // Per op, as `hold_for_lost_deps` holds it: for the replays
            // queued ahead of its own entry (an op that is itself the head
            // of the queue is not held for the entries behind it, which
            // the drain cannot reach while it is in flight).
            if self.own_replays_unsettled_ahead_of(replica, Some(rid)) {
                continue;
            }
            // Not from this delegate's own uncommitted row (D6): the op
            // asks the new holder, as one the log does not carry.
            if let Some(outcome) = completed_as_outcome(replica, rid, epoch)
                .filter(|_| self.completed_row_settled(rid, replica))
            {
                // Read from the log, as `answer_awaiting_log` does.
                self.finish_as(
                    now,
                    rid,
                    outcome,
                    AnswerSource::Sequenced(constellation_meta::RowCommit::ARCHIVE),
                    replica,
                    out,
                );
                continue;
            }
            self.stats.awaiting_log_rerouted += 1;
            tracing::debug!(
                node = self.cfg.node_id,
                ?rid,
                epoch,
                max,
                "the log moved past the epoch that accepted an op without it: asking the new holder"
            );
            let c = self.clients.get_mut(&rid).expect("present");
            c.phase = Phase::WaitingLease;
            // No longer awaiting the accepting delegate's rows: a live
            // delegate may settle the op afresh, without a root.
            c.accepted_by_delegate = false;
            if let Some(t) = c.timer.take() {
                self.cancel_timer(t, out);
            }
            if self.gated(rid) {
                self.clients.get_mut(&rid).expect("present").phase = Phase::Gated;
                continue;
            }
            self.route(now, rid, replica, out);
        }
    }

    /// `Timer::ClientDeadline`. Plan 43 C7b: an op still waiting for the
    /// lease whose node may claim it only after the deadline — a cold claim
    /// waits one *holder's* TTL from its first read (plus the backups' claim
    /// grace for a non-backup of a `Backup` lease), and `acquire_deadline_ms`
    /// is two of this node's own TTLs: a holder with a longer TTL, or a short
    /// TTL under a long grace, outlasts it — gets until then (plus one
    /// retry backoff), once, instead of failing in doubt before its claim
    /// was ever possible. (`on_own_s3`'s early deadline does not come here.)
    pub(crate) fn on_client_deadline_timer(
        &mut self,
        now: Now,
        rid: Rid,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let waiting = self.clients.get(&rid).is_some_and(|c| {
            matches!(c.phase, Phase::WaitingLease | Phase::AcquireRetry) && !c.deadline_extended
        });
        if waiting {
            if let Some(due) = self
                .lease
                .claim_due(&self.cfg)
                .filter(|due| *due > now.wait)
            {
                let until = due.plus(self.cfg.acquire_retry_max_ms);
                let timer = self.set_timer_wait(until, Timer::ClientDeadline(rid), out);
                let c = self.clients.get_mut(&rid).expect("present");
                c.deadline = timer;
                c.deadline_extended = true;
                self.stats.client_deadlines_extended += 1;
                return;
            }
        }
        self.on_client_deadline(now, rid, replica, out);
    }

    pub(crate) fn on_client_deadline(
        &mut self,
        now: Now,
        rid: Rid,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.client_deadline(now, rid, false, replica, out);
    }

    /// Whether an op of `origin` submitted under `policy` is settled at its
    /// deadline (plan 43 C2, §5.3 item 5): a client's (FUSE or system) op.
    /// A replay of a stranded op is the lone branch's (C5), and a
    /// best-effort batch (atime) is dropped in doubt as ever.
    pub(crate) fn settles(origin: Origin, policy: Policy) -> bool {
        origin == Origin::Client && policy != Policy::BestEffort
    }

    /// The op's deadline (`Timer::ClientDeadline`, or `s3_less` A-1's early
    /// one while this node's S3 is stalled).
    ///
    /// Plan 43 C2 (§5.3 item 5): the first deadline ends the op's own
    /// attempts. What the log or this node already says of the rid answers
    /// it; an op that never left this node, with nothing of it executed
    /// here, has no copy anywhere and is answered `Abandoned` at once;
    /// otherwise the op is *settled*: under the same rid and the same client
    /// entry (one submission, I2(b)), a settle goes the ordinary route
    /// (`settle_route`: this node as the sequencer, a delegate, a forward,
    /// the inbox, the lease path), and the sequencer in force answers the
    /// rid's outcome — the late success, a refusal — or journals `Refused
    /// { rid, ABANDONED_CODE }` and answers `Abandoned` once that row is
    /// committed. The client hears that outcome (`Abandoned` is `EIO`). The
    /// second deadline, a settle window later, finds no sequencer answered:
    /// the client hears the unsettled `EIO` (in doubt, retryable by rid:
    /// the rid stays in `in_doubt_rids`; `client_unsettled_eio`).
    ///
    /// Plan 46 M1.2 step 13: "a settle window later" is the least. Later
    /// only while a live sequencer says it holds the settle for the end of a
    /// generation it outwaits (`Held::outwait_ms`: a silent delegate's window,
    /// which the lease argument forbids cutting short), or while a lapsed
    /// generation of this node's holding the op's row is ended by the root
    /// (`own_gen_end_due`): the deadline then moves to that end and a settle
    /// window, never earlier, and never past `settle_cap`: the settle's start
    /// plus two settle windows, one reclaim horizon (no renewal is granted
    /// once a recall starts, so every outwait ends within it) and two forward
    /// timeouts (a seal window). With the defaults (lease TTL 60 s: a 30 s
    /// window; 5 s grant TTL, 1 s margin, 0.5 s forward timeout) the worst
    /// case adds one settle window, the horizon and a seal window: 37 s more to
    /// the unsettled `EIO`, and only for a settle a live sequencer reports
    /// holding. An isolated requester, which hears no `Held`, keeps the
    /// plain window.
    ///
    /// Why a settled `EIO` means "did not, and will not, happen": the
    /// refusal is a committed row of the sequencer in force for the op's
    /// keys, and every other copy of the op can execute only at a sequencer
    /// in force for those keys later, which holds the refusal first and
    /// dedups the copy to it. Two cases make that the whole of "no
    /// execution of the rid exists anywhere afterwards":
    ///
    /// - *A copy in flight across a routing change.* The op was forwarded
    ///   to root R and delayed in the network; its directory was delegated
    ///   to D meanwhile; the settle goes to D, and D journals the refusal;
    ///   the delayed op arrives at R. R does not execute in a delegated
    ///   subtree without recalling it first, the recall drains D's stream
    ///   (which carries the refusal), and `completed` then dedups the op at
    ///   R. (The other way round, a generation's end drains the delegate's
    ///   committed rows before anything executes at the root.)
    /// - *A takeover.* A successor's takeover gate re-ships the sealed tail
    ///   (the committed rows, the refusal among them) before it executes
    ///   anything, so a copy executed there after the gate dedups too.
    ///
    /// A parked execution of the rid at a sequencer is left to dedup on the
    /// refusal when it resumes, or is dropped by the settle itself
    /// (`drop_unexecuted_park`, the delegate's own park). The unsettled
    /// `EIO` keeps only the older guarantee (retry by rid; an isolated
    /// requester's op may still land).
    pub(crate) fn client_deadline(
        &mut self,
        now: Now,
        rid: Rid,
        s3_less: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(c) = self.clients.get(&rid) else {
            return;
        };
        let second = c.settle;
        let (recalling, settle_cap) = (c.phase == Phase::Recalling, c.settle_cap);
        // Plan 46 M1.2 step 13 (review): a settle of this node's own op, parked
        // here as the root for the recall of a generation the root outwaits
        // (a silent delegate, an inherited generation), is kept until the
        // outwait ends: the generation's rows, the op's among them, land when
        // it does, and the settle then finds them; a client told `EIO`
        // before would see the row land after (I4). A `Sealing` generation
        // resets its end at every expiry pass, so the hold is capped at the
        // settle's cap: a silent delegate and backups end it in doubt there.
        if second && recalling && now.wait < settle_cap {
            if let Some(end) = self.exec_local_park_outwait(rid) {
                if now.wait <= end {
                    let at = end.plus(self.deleg_request_timeout_ms()).min(settle_cap);
                    if self.extend_settle_deadline(rid, at, Some(now.wait), out) {
                        self.stats.settles_held_for_outwait += 1;
                        return;
                    }
                }
            }
        }
        // The generation of this node's an op in doubt sits in lapsed after
        // the first deadline (an acquire deadline short against the grant's
        // ttl): the root ends it by `deleg_end_due` and the row lands then.
        if second && settle_cap > WaitMs(0) {
            if let Some(due) = self.own_gen_end_due(rid, replica) {
                if due > now.honour {
                    let rest = due.0.saturating_sub(now.honour.0).max(0) as u64;
                    let want = now
                        .wait
                        .plus(rest + self.dl_t().settle_window)
                        .min(settle_cap);
                    if self.extend_settle_deadline(rid, want, Some(now.wait), out) {
                        self.stats.settles_extended_for_lapsed_gen += 1;
                        return;
                    }
                }
            }
        }
        // Phase 2b round 2: parked *before* executing (the root waits
        // for a delegation's recall, or this node, the op's delegate,
        // for its grant's renewal or the op's deps): nothing ran, the
        // park is dropped and the client hears in doubt — never held past
        // its deadline. A delegate's park kept the op, deadline spent,
        // and let it go to the lease path with none (the delegate-restart
        // wedge's op was re-sent for 540 s, never in doubt).
        let unexecuted =
            recalling && (self.abort_exec_local_park(rid) || self.abort_deleg_own_park(rid));
        let Some(c) = self.clients.get(&rid) else {
            return;
        };
        if c.phase == Phase::Recalling && !unexecuted {
            // Executed; only the acknowledgement waits. A recall ends by
            // the grant's TTL: not in doubt. A durability wait (plan 30
            // §M9) can outlast anything (S3 away under `ack=s3`): the
            // client hears in doubt and retries by rid, which `completed`
            // answers once the row is durable.
            if !self.abort_durable_park_of(rid, out) {
                return;
            }
            self.stats.acks_aborted += 1;
        }
        let Some(c) = self.clients.get_mut(&rid) else {
            return;
        };
        if c.phase == Phase::Recalling && !unexecuted {
            // Executed here, never acknowledged: the resubmission is in
            // doubt (it must find the completion, not run again).
            c.forwarded = true;
        }
        if let Phase::Forwarded { req, .. } = c.phase {
            self.by_req.remove(&req);
        }
        if let Some(t) = c.timer.take() {
            self.cancel_timer(t, out);
        }
        if let Some(outcome) = self.settled_outcome(rid, replica) {
            self.stats.forward_indoubt_resolved += 1;
            tracing::debug!(
                node = self.cfg.node_id,
                ?rid,
                "deadline: the log answered the op; not in doubt"
            );
            let (outcome, source) = Self::eio_unless_recorded(
                outcome,
                self.completed_source(replica, rid),
                AnswerSource::Sequenced(constellation_meta::RowCommit::LONE),
            );
            self.park_settled_acceptance(now, rid, &outcome, replica, out);
            self.finish_as(now, rid, outcome, source, replica, out);
            return;
        }
        let c = self.clients.get(&rid).expect("present");
        if second || !Self::settles(c.origin, c.policy) {
            self.stats.in_doubt += 1;
            tracing::debug!(
                node = self.cfg.node_id,
                ?rid,
                settle = second,
                "op goes in doubt"
            );
            self.finish_in_doubt(now, rid, replica, out);
            return;
        }
        // (Nothing of it executed here either: no row of the rid in this
        // node's journal or `recent`, no `completed` row — a belt over
        // `forwarded`, which every path that executes the op here sets.)
        if !c.forwarded
            && c.attempts == 0
            && c.inbox_keys.is_empty()
            && replica.recent_outcome(rid).is_none()
            && replica.completed_outcome(rid).ok().flatten().is_none()
            && !journal_carries_outcome(replica, rid)
        {
            // Never sent anywhere, and nothing of it executed here: no copy
            // of it exists, so nothing can ever execute it. Settled here, by
            // no sequencer (no row records it: nothing to commit).
            self.stats.settled_local += 1;
            tracing::debug!(
                node = self.cfg.node_id,
                ?rid,
                "deadline: never left this node; abandoned"
            );
            self.finish_as(
                now,
                rid,
                MutateOutcome::Abandoned,
                AnswerSource::None,
                replica,
                out,
            );
            return;
        }
        // Settle: until the client's deadline (twice the lease TTL from the
        // submission), and at least a settle window from here — a deadline
        // extended for a cold claim keeps its window — but no later than the
        // full deadline when the early S3-less one started it. (An inbox
        // op's own attempts end early by the inbox's deadline: its settle
        // still has the rest of the client's.)
        let window = self.dl_t().settle_window;
        let full = c.submitted.plus(self.cfg.acquire_deadline_ms);
        let mut until = now.wait.plus(window);
        if s3_less {
            if full > now.wait {
                until = until.min(full);
            }
        } else if full > until {
            until = full;
        }
        // An op in doubt in a generation of this node's that lapsed: its row
        // is journaled here and lands when the root ends the generation and
        // fetches the tail, which it does by `deleg_end_due` and a recall
        // later. A settle that expired before would end the op unsettled
        // and its row land after (I4).
        if let Some(due) = self.own_gen_end_due(rid, replica) {
            let rest = due.0.saturating_sub(now.honour.0).max(0) as u64;
            until = until.max(now.wait.plus(rest + window));
        }
        // The settle may be moved later (the sequencer holding it for a
        // generation it outwaits, a lapsed generation of this node's), but
        // never past one reclaim horizon (no renewal after a recall starts),
        // a seal window and a settle window from here.
        let cap =
            until
                .max(now.wait.plus(
                    2 * window + self.reclaim_horizon_ms() + 2 * self.cfg.forward_timeout_ms,
                ));
        let old = c.deadline;
        self.cancel_timer(old, out);
        let deadline = self.set_timer_wait(until, Timer::ClientDeadline(rid), out);
        let c = self.clients.get_mut(&rid).expect("present");
        c.deadline = deadline;
        c.settle_until = until;
        c.settle_cap = cap;
        c.settle = true;
        c.attempts = 0;
        c.fence_bounces = 0;
        c.redirected = 0;
        self.stats.settles_sent += 1;
        tracing::debug!(
            node = self.cfg.node_id,
            ?rid,
            window,
            "deadline: settling the op"
        );
        self.settle_route(now, rid, replica, out);
    }

    /// Move a settle's second deadline to `at` (the wait clock): cancels the
    /// armed one and records the new one in `settle_until`. `false` (nothing changed) if the op is
    /// gone or `at` is not later than the deadline in force: a deadline is
    /// never shortened.
    ///
    /// `fired`: the deadline in force has just fired (at that reading of
    /// the wait clock), so the new one only has to lie after that; `None`:
    /// it is still armed, and the new one has to lie after it.
    fn extend_settle_deadline(
        &mut self,
        rid: Rid,
        at: WaitMs,
        fired: Option<WaitMs>,
        out: &mut Vec<Action>,
    ) -> bool {
        let Some(c) = self.clients.get(&rid) else {
            return false;
        };
        if at <= fired.unwrap_or(c.settle_until) {
            return false;
        }
        let old = c.deadline;
        self.cancel_timer(old, out);
        let deadline = self.set_timer_wait(at, Timer::ClientDeadline(rid), out);
        let c = self.clients.get_mut(&rid).expect("present");
        c.deadline = deadline;
        c.settle_until = at;
        true
    }

    /// When the root ends the lapsed generations of this node's that may
    /// hold `rid`'s row (journaled here, not acknowledged): the latest
    /// [`Core::deleg_end_due`] of them; `None` if the journal carries no
    /// row of the rid or no generation of this node's has lapsed.
    fn own_gen_end_due(&self, rid: Rid, replica: &dyn Replica) -> Option<HonourMs> {
        let due = self
            .dl
            .mine
            .values()
            .filter(|d| d.lapsed && !d.stopped)
            .map(|d| self.deleg_end_due(d))
            .max()?;
        journal_carries_outcome(replica, rid).then_some(due)
    }

    /// Plan 43 C2 (§5.3 item 5): send a settle of `rid` the way the op itself
    /// would go now, from wherever it stood — executed here if this node is
    /// the sequencer for it (the root with its view open, or its delegate),
    /// forwarded to a live delegate of its keys or to the known holder,
    /// else the ordinary route (`route`: the lease read, the inbox, the
    /// lease path). Retries (`Busy`, a timeout, `NotHolder`) go on as the
    /// op's did until the second deadline.
    fn settle_route(&mut self, now: Now, rid: Rid, replica: &dyn Replica, out: &mut Vec<Action>) {
        // (In `submit`'s order: this node as the op's delegate, a live
        // delegate of its keys, this node as the root, the route.)
        let Some((op, deps, tag)) = self
            .clients
            .get(&rid)
            .map(|c| (c.op.clone(), c.deps, c.tag.clone()))
        else {
            return;
        };
        if self.cfg.delegation {
            if self.delegate_try_execute(
                now,
                0,
                None,
                rid,
                &op,
                deps,
                0,
                &tag,
                true,
                Fence::default(),
                replica,
                out,
            ) {
                return;
            }
            if self.cfg.forwarding && self.cfg.p2p {
                if let Ownership::Delegated(d) =
                    replica.resolve_ownership(&super::holder::keys_of_op_in(&op, replica))
                {
                    // A generation this root is recalling has a stopped
                    // delegate: it answers `NotHolder` naming this node, one
                    // bounced round trip per hold (ADR-44: the gated root's
                    // own held settle). `on_forward` applies the same rule
                    // to a forwarded settle.
                    let recalling = self
                        .dl
                        .gens
                        .get(&d.gen)
                        .is_some_and(|g| g.recall != super::delegate::RecallPhase::None);
                    if d.node != self.cfg.node_id && self.reaches(now, d.node) && !recalling {
                        self.send_forward(now, rid, d.node, d.gen, replica, out);
                        return;
                    }
                }
            }
        }
        if let Some(epoch) = self.lease.new_mutation_epoch(now, &self.cfg) {
            let outcome = self.resolve_in_doubt_then_execute(now, rid, epoch, replica, out);
            self.finish(now, rid, outcome, replica, out);
            return;
        }
        // Plan 46 M0.3 (S1): this node's own settle, the root in a takeover
        // gate whose journal work is done: the gate keeps the view closed
        // (`new_mutation_epoch`) but answers a settle on the keys it owns
        // (`Core::gate_settle`) — as it answers another node's — instead of
        // routing it in a loop with this node as the holder. A rid that took
        // effect is not answered here before the quarantine: it goes on
        // retrying.
        if let Some(super::holder::GateSettle::Answer(epoch)) =
            self.gate_settle(now, self.cfg.node_id, &op, replica)
        {
            if !super::holder::took_effect_here(replica, rid) {
                // (The gate's keys are the root's and this node's own
                // generations', which the gate ends when it opens: a
                // settle on them recalls nothing, whether or not the gate
                // has taken on generations of other nodes, ADR-42; and
                // `execute_local_outcome` plans no recall for it.)
                self.stats.gate_settles_answered += 1;
                let outcome = self.resolve_in_doubt_then_execute(now, rid, epoch, replica, out);
                self.finish(now, rid, outcome, replica, out);
                return;
            }
            self.stats.gate_settles_held_executed += 1;
        }
        if let Some(c) = self.clients.get_mut(&rid) {
            c.phase = Phase::Backoff;
        }
        let cached = self.lease.cached_holder.filter(|h| *h != self.cfg.node_id);
        match cached {
            Some(holder) if self.reaches(now, holder) && self.cfg.forwarding => {
                self.send_forward(now, rid, holder, 0, replica, out)
            }
            _ => self.route(now, rid, replica, out),
        }
    }

    /// Plan 43 C2 (D6): whether `rid`'s `completed` row here is an outcome
    /// its client may hear. Not while it is this node's own row of a
    /// delegated generation that waits for its group (gated) and the
    /// group does not hold it: the generation may end without it (the
    /// backup sealed it short of the row, the root's drain drops what is
    /// past the cut), and the rid then runs again at the generation's new
    /// owner. Settled once the group or the log holds it, or when it is
    /// this node's own client's `Lone` row.
    ///
    /// Also not while it is this node's own *append* of a delegate's row
    /// (this node is the root) that its tenure has not committed
    /// ([`Core::holder_append_uncommitted`]), whatever the class the row
    /// carries: a root answers its own forwarded op from the row the
    /// delegate streamed, and that row, tagged `Lone`, is held by this
    /// journal alone until it ships or the backups hold it (`placement-hot`
    /// 1659, `long-delegated-late-answers` 542, 992: answered `Archive`,
    /// the root then sealed by its backup).
    pub(crate) fn completed_row_settled(&self, rid: Rid, replica: &dyn Replica) -> bool {
        replica
            .outcome_row(rid)
            .filter(|r| r.gen != 0)
            .is_none_or(|r| {
                (!r.class.gated() || self.deleg_stream_durable(r.gen, r.idx, replica))
                    && !self.holder_append_uncommitted(r.gen, r.idx, replica)
            })
    }

    /// What this replica can answer for `rid` without running it, at a
    /// point where the op would otherwise go in doubt: its `completed`
    /// row, if that row is an outcome a client may hear now.
    ///
    /// Plan 30 §M9: a node that holds no lease has `completed` rows only
    /// from applied segments (speculation records none), so one here is
    /// the rid's outcome in the log (acks3 seed 700087: the deposed
    /// holder's own write had landed in its last segment before a pause;
    /// its durability wait was aborted, the retries timed out, and the
    /// client heard `EIO` for a write the log carried). A holder's row may
    /// be in its own unshipped journal instead: answered only when that
    /// journal is durable under the lease's policy (flex-crash seed 2140:
    /// the deadline fired between a takeover's CAS and its gate, and the
    /// client heard `EIO` for a rename the log carried at seq 7). A
    /// deposed holder's journal will be rolled back: never answered —
    /// and a deposed node holds no lease either (`mark_lost`), so until
    /// its recovery has rolled the journal back, a row the journal still
    /// carries is not the log's (long_backup seed 56774: the deposed
    /// holder's own `Refused` row, never acknowledged, answered `ENOENT`
    /// at the deadline; the op then ran by rid under the next holder and
    /// succeeded). A holder's row that is not in its journal is in an
    /// applied segment — the log (a holder applies its own segments once
    /// they landed): answered as a non-holder's is, whatever its journal
    /// still waits for (long-delegated-late-answers seed 1381: a backup
    /// took over 120 ms before its own forwarded create's deadline; the
    /// create was at log seq 15, the new holder's first segments were not
    /// durable yet, and the client heard `EIO`). The journal decides only
    /// while the `completed` row is its own: once an applied segment
    /// carried the rid's outcome, the row is the log's (position 0, as
    /// `store::spec`'s stranding tells them apart), whatever the deposed
    /// journal still holds (long-delegated-late-answers seed 9287: a
    /// deposed root's journal kept its delegate's appended row of the
    /// root's own create — a generation's row, which a deposition does
    /// not strand — while the successor's segment carrying the create was
    /// applied; the client heard `EIO` for a write the log carried).
    pub(crate) fn settled_outcome(&self, rid: Rid, replica: &dyn Replica) -> Option<MutateOutcome> {
        let holds = self.lease.held.is_some() || self.lease.epoch_held();
        if !holds {
            // Plan 46 M1.4 step 2: nor a row an ended hold left uncommitted
            // (the member's copy was never acknowledged, the archive has not
            // carried it): it is the journal's, not the log's.
            let held_back = self.lease.lost
                || replica
                    .journal_class_of_rid(rid)
                    .is_some_and(|(first, _)| self.left_by_hold(first));
            if held_back && journal_carries_outcome(replica, rid) && !replica.outcome_from_log(rid)
            {
                return None;
            }
            if !self.completed_row_settled(rid, replica) {
                return None;
            }
            return completed_as_outcome(replica, rid, self.ship.max_epoch);
        }
        if self.lease.lost {
            return None;
        }
        let epoch = self.lease.epoch().unwrap_or(self.ship.max_epoch);
        let outcome = completed_as_outcome(replica, rid, epoch)?;
        if !journal_carries_outcome(replica, rid) {
            return Some(outcome);
        }
        // A delegation row in this journal (this node's own row of its
        // generation, or this tenure's append of it) commits by its group
        // or the log, not by the root journal's position below: a lone
        // root's `ack_need` gates nothing (`long-delegated` seeds 255 and
        // 865 under slow lease reads: a delegate took the root over, and
        // its own client's row, which the restarted root never
        // acknowledged, was answered at the deadline inside the takeover
        // gate on this disk alone).
        if !self.completed_row_settled(rid, replica) {
            return None;
        }
        let position = Position {
            seq: self.ship.head_seq,
            pending: replica.journal_position(epoch),
            streams: Default::default(),
        };
        if outcome == MutateOutcome::Abandoned {
            // A settle's refusal: answered once committed, as another
            // node's answer (never the lone shortcut, `Core::settle_refusal`).
            let shipped = replica.journal_acked_seq().unwrap_or(0);
            let class = self.root_commit(super::Asker::Remote);
            return self
                .answer_need(&position, class, super::Asker::Remote, shipped)
                .is_none()
                .then_some(outcome);
        }
        self.ack_need_of(rid, &position, replica)
            .is_none()
            .then_some(outcome)
    }

    /// Plan 46 M1.4 step 2 fix: [`Self::ack_need`] for the answer to `rid`'s
    /// row. A row an ended hold left uncommitted waits as `park_durable`
    /// makes it wait (the archive or the group, a lone node's disk
    /// included); `ack_need` reads the group's coverage of the tip only and
    /// ignores `hold_end_tip`: a deadline's or a retry's answer left on it
    /// while the parked one waited, with the row labelled for the archive.
    pub(crate) fn ack_need_of(
        &self,
        rid: Rid,
        position: &Position,
        replica: &dyn Replica,
    ) -> Option<u64> {
        if let Some((first, _)) = replica
            .journal_class_of_rid(rid)
            .filter(|(first, _)| self.left_by_hold(*first))
        {
            let shipped = replica.journal_acked_seq().unwrap_or(0);
            return (!self.park_durable(first, false, shipped)).then_some(first);
        }
        self.ack_need(position)
    }

    /// Plan 41 P41-01: an acceptance `settled_outcome` found at a lease
    /// holder (a deadline, a lease-path refusal) is a dedup answer like
    /// `resolve_in_doubt_then_execute`'s: it waits for the read-delegation
    /// quarantine (`dedup_acceptance_wait`), parked as `finish` parks a
    /// local answer. A non-holder knows no successor's quarantine (open).
    fn park_settled_acceptance(
        &mut self,
        now: Now,
        rid: Rid,
        outcome: &MutateOutcome,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !matches!(outcome, MutateOutcome::Accepted { .. })
            || !(self.lease.held.is_some() || self.lease.epoch_held())
        {
            return;
        }
        if let Some(wait) = self.dedup_acceptance_wait(now, rid, replica, out) {
            let durable = self.rd.parked_local.remove(&rid).and_then(|(_, d)| d);
            self.rd.parked_local.insert(rid, (wait, durable));
        }
    }

    /// Starts recording the rids of unsettled `EIO`s whose refusal was held
    /// back for a designee, for `take_unsettled_designation` (the sim driver).
    pub fn track_unsettled_designation(&mut self) {
        self.track_unsettled_designation = true;
    }

    /// ADR-47: starts recording the root's decisions to refuse an op under a
    /// non-stealable grant and the executions of this node's non-stealable
    /// generations (`take_final_nonholder_refusals`,
    /// `take_nonstealable_executions`; the sim driver).
    pub fn track_fence_audit(&mut self) {
        self.track_fence_audit = true;
    }

    /// The refusals decided since the last call: `(rid, code, drain)`, the
    /// last flag set for the inbox drain's journaled `Refused`.
    pub fn take_final_nonholder_refusals(&mut self) -> Vec<(Rid, Code, bool)> {
        std::mem::take(&mut self.final_nonholder_refusals)
    }

    /// The `(rid, gen)` executed under a non-stealable generation of this
    /// node since the last call.
    pub fn take_nonstealable_executions(&mut self) -> Vec<(Rid, u64)> {
        std::mem::take(&mut self.nonstealable_executions)
    }

    pub(crate) fn note_final_nonholder_refusal(&mut self, rid: Rid, code: Code, drain: bool) {
        if self.track_fence_audit {
            self.final_nonholder_refusals.push((rid, code, drain));
        }
    }

    /// Whether `rid`'s unsettled `EIO` was one whose refusal was held back
    /// for a designee (`Stats::unsettled_nonstealable_holder`); taken once.
    pub fn take_unsettled_designation(&mut self, rid: Rid) -> bool {
        self.unsettled_designation_rids.remove(&rid)
    }

    /// Whether `rid`'s unsettled `EIO` was one of an op a delegate had
    /// executed and answered (`ClientOp::accepted_by_delegate`); taken once.
    pub fn take_unsettled_accepted(&mut self, rid: Rid) -> bool {
        self.unsettled_accepted_rids.remove(&rid)
    }

    /// The op is neither executed here nor answered by anyone: the client
    /// hears `InDoubt` (`EIO`; retryable by the same rid), the replay
    /// drain leaves it queued.
    pub(crate) fn finish_in_doubt(
        &mut self,
        now: Now,
        rid: Rid,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(c) = self.clients.remove(&rid) else {
            return;
        };
        if c.settle && matches!(c.origin, Origin::Client) {
            // Plan 43 C2: no sequencer answered the settle (I4's one
            // exception: an isolated requester).
            self.stats.client_unsettled_eio += 1;
            // Of those, the op whose refusal was held back for a designee
            // (the op was sent to it, or its keys are waited on): only an
            // unsettled `EIO` is I4's exception, by rid.
            if c.accepted_by_delegate && self.track_unsettled_designation {
                self.unsettled_accepted_rids.insert(rid);
                while self.unsettled_accepted_rids.len() > MAX_IN_DOUBT_RIDS {
                    let first = *self
                        .unsettled_accepted_rids
                        .iter()
                        .next()
                        .expect("non-empty");
                    self.unsettled_accepted_rids.remove(&first);
                }
            }
            if c.designation_held {
                self.stats.unsettled_nonstealable_holder += 1;
                // (Recorded only when a consumer takes them: the sim
                // driver. A production core would never drain the set.)
                if self.track_unsettled_designation {
                    self.unsettled_designation_rids.insert(rid);
                }
                while self.unsettled_designation_rids.len() > MAX_IN_DOUBT_RIDS {
                    let first = *self
                        .unsettled_designation_rids
                        .iter()
                        .next()
                        .expect("non-empty");
                    self.unsettled_designation_rids.remove(&first);
                }
            }
        }
        self.log_if_slow(now, rid, &c, "in doubt", replica);
        self.cancel_timer(c.deadline, out);
        if let Some(t) = c.timer {
            self.cancel_timer(t, out);
        }
        if let Some(t) = c.own_record_timer {
            self.cancel_timer(t, out);
        }
        self.inbox_forget(rid);
        match c.origin {
            Origin::Client => {
                if rid.node == self.cfg.node_id && rid.incarnation == self.cfg.incarnation {
                    self.acked.mark_done(rid.seq);
                }
                if c.forwarded || c.attempts > 0 {
                    self.in_doubt_rids.insert(rid);
                    if c.touched_gen != 0 {
                        self.in_doubt_touched.insert(rid, c.touched_gen);
                    }
                    self.evict_in_doubt();
                    if c.redirected_to_designee {
                        self.in_doubt_designee_rids.insert(rid);
                    }
                    if !c.inbox_keys.is_empty() {
                        self.in_doubt_batches.insert(rid, c.inbox_keys);
                    }
                }
                out.push(Action::Reply {
                    rid,
                    reply: ClientReply::InDoubt,
                    source: AnswerSource::None,
                });
            }
            Origin::Replay { queue_seq, .. } => {
                if c.also_client {
                    out.push(Action::Reply {
                        rid,
                        reply: ClientReply::InDoubt,
                        source: AnswerSource::None,
                    });
                }
                self.on_replay_outcome(now, queue_seq, rid, None, replica, out)
            }
        }
        self.release_gated(now, replica, out);
    }

    /// [`Self::finish`] an outcome whose source is `source` (plan 43 C2a:
    /// who decided it).
    pub(crate) fn finish_as(
        &mut self,
        now: Now,
        rid: Rid,
        outcome: MutateOutcome,
        source: AnswerSource,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if let Some(c) = self.clients.get_mut(&rid) {
            c.answered_by = Some(source);
        }
        self.finish(now, rid, outcome, replica, out);
    }

    /// The source `finish` names for `outcome`, which `answered_by` says
    /// (a definitive outcome nobody named is a bug, and counts as the
    /// archive's), counted when it leaves as a sequencer's answer.
    fn answer_source(
        &mut self,
        answered_by: Option<AnswerSource>,
        outcome: &MutateOutcome,
    ) -> AnswerSource {
        let source = match answered_by {
            Some(source) => source,
            None if is_answer(outcome) => {
                debug_assert!(false, "an outcome with no source: {outcome:?}");
                AnswerSource::Sequenced(constellation_meta::RowCommit::ARCHIVE)
            }
            None => AnswerSource::None,
        };
        if let (AnswerSource::Sequenced(class), true) = (source, is_answer(outcome)) {
            self.stats.ack_classes[ack_counter(class)] += 1;
        }
        source
    }

    /// Deliver the op's outcome to whoever submitted it.
    pub(crate) fn finish(
        &mut self,
        now: Now,
        rid: Rid,
        outcome: MutateOutcome,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // Plan 30 §M11: the execution itself is parked (a recall, `deps`);
        // the parked continuation finishes it.
        if self.dl.pending_exec.contains(&rid) {
            return;
        }
        // Held for a lost dependency (`hold_for_lost_deps`): its backoff
        // timer drives it on.
        if matches!(outcome, MutateOutcome::Held { .. })
            && self
                .clients
                .get(&rid)
                .is_some_and(|c| c.phase == Phase::Backoff && c.timer.is_some())
        {
            return;
        }
        if let Some((wait, durable)) = self.rd.parked_local.remove(&rid) {
            if let Some(c) = self.clients.get(&rid) {
                let class = match c.answered_by {
                    Some(AnswerSource::Sequenced(class)) => class,
                    other => {
                        debug_assert!(false, "a parked local answer with source {other:?}");
                        constellation_meta::RowCommit::ARCHIVE
                    }
                };
                self.park_finish(now, rid, wait, durable, outcome, class);
                return;
            }
        }
        let Some(c) = self.clients.remove(&rid) else {
            return;
        };
        self.log_if_slow(now, rid, &c, &format!("{outcome:?}"), replica);
        self.cancel_timer(c.deadline, out);
        if let Some(t) = c.timer {
            self.cancel_timer(t, out);
        }
        if let Some(t) = c.own_record_timer {
            self.cancel_timer(t, out);
        }
        self.inbox_forget(rid);
        if c.settle && is_answer(&outcome) && matches!(c.origin, Origin::Client) {
            if outcome == MutateOutcome::Abandoned {
                self.stats.settled_abandoned += 1;
            } else {
                self.stats.settled_late_success += 1;
            }
        }
        match c.origin {
            Origin::Client => {
                // Plan 30 §M2 GC: every completion path marks the rid
                // acked, not just the forward reply.
                if rid.node == self.cfg.node_id && rid.incarnation == self.cfg.incarnation {
                    self.acked.mark_done(rid.seq);
                }
                let source = self.answer_source(c.answered_by, &outcome);
                out.push(Action::Reply {
                    rid,
                    reply: ClientReply::Outcome(outcome),
                    source,
                });
            }
            Origin::Replay { queue_seq, .. } => {
                if c.also_client {
                    if rid.node == self.cfg.node_id && rid.incarnation == self.cfg.incarnation {
                        self.acked.mark_done(rid.seq);
                    }
                    let source = self.answer_source(c.answered_by, &outcome);
                    out.push(Action::Reply {
                        rid,
                        reply: ClientReply::Outcome(outcome.clone()),
                        source,
                    });
                }
                self.on_replay_outcome(now, queue_seq, rid, Some(outcome), replica, out)
            }
        }
        self.release_gated(now, replica, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_tracker_advances_a_contiguous_floor() {
        let mut t = AckTracker::default();
        t.mark_done(2);
        assert_eq!(t.floor(), 0);
        t.mark_done(1);
        assert_eq!(t.floor(), 2);
        t.mark_done(4);
        t.mark_done(3);
        assert_eq!(t.floor(), 4);
    }
}
