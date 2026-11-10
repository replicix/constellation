//! The S3 inbox (plan 30 §M13), as core state: the requester's queue,
//! group-commit submitter and outcome waiters; the holder's per-requester
//! poll schedule, batch execution and GC; the takeover drain; round 3b's
//! escalation; and round 3a's P2P reachability record. What
//! `cli::inbox::InboxRuntime` did with tasks, sleeps and locks is here a
//! few timers and the results of the S3 actions the core issues.
//!
//! The requester half: an op with no P2P path to a live holder is queued
//! under that holder's epoch; one PUT is in flight at a time (so batch
//! numbers are sequential and GET-next never sees a gap), and everything
//! queued meanwhile shares the next batch. Once durable the op waits for
//! its outcome to arrive through the log — `Completed { rid }` or
//! `Refused { rid, errno }` — which `answer_awaiting_log` finds after
//! every applied segment. While anything waits, the lease is re-read on a
//! timer: a new epoch strands the batch (re-submitted by rid under the new
//! one), a claimable lease or this node itself as holder sends the op to
//! the lease path (whose coverage rule resolves it against `completed`
//! once the takeover gate has drained the batch), and the in-doubt
//! deadline does the same.
//!
//! The holder half: while holding, every requester in the roster and the
//! peer directory that this node is not P2P-connected to is polled on
//! its own schedule (`PollBackoff`: hot after a hit, doubling to the warm
//! ceiling, cold after a minute of silence); each batch executes in order
//! through the same dedup a P2P forward gets plus the position watermark;
//! a batch whose requester says `wants_lease` is the same as finding that
//! requester in `wanted_by` at a renewal (M5: learned at the next poll,
//! not the next half-TTL); batches whose rows have shipped are deleted,
//! all but each requester's newest. A takeover drains every older epoch's
//! batches inside its gate, before the new holder's view opens.

use super::client::Phase;
use super::{Core, S3For, Timer, TimerAt};
use crate::action::{Action, S3Op};
use crate::event::{CasFailure, S3Result};
use crate::ids::{Epoch, NodeId, Now, OpId, TimerId, WaitMs};
use crate::replica::Replica;
use constellation_meta::{InboxAck, MetaError, MutateOp, Rid};
use constellation_store_s3::inbox::{
    gc_keep_newest, InboxBatch, InboxKey, InboxOp, InboxRid, PollBackoff, MAX_OPS_PER_BATCH,
};
use constellation_types::Code;
use std::collections::{BTreeMap, VecDeque};

/// PUT attempts per batch before the waiters are told the inbox is
/// unavailable (they then take the lease path).
const SUBMIT_ATTEMPTS: u32 = 6;
/// Hysteresis: de-escalate only once the window has fallen below half
/// of both thresholds.
const DEESCALATE_FRACTION: f64 = 0.5;
/// Round 4: the wait term needs this many ops in the window, and its
/// single largest sample is left out (one slow first contact is not
/// demand).
const ESCALATE_WAIT_MIN_OPS: u64 = 5;
/// The escalator's tick while demand is sustained.
const ESCALATE_TICK_MS: u64 = 250;

/// A batch this tenure executed, paired with the journal seq its last
/// row landed at (deletable once that seq has shipped).
type ExecutedBatch = (InboxKey, u64);

#[derive(Debug)]
struct Submitting {
    #[allow(dead_code)]
    op: OpId,
    epoch: Epoch,
    n: u64,
    rids: Vec<Rid>,
    attempt: u32,
}

#[derive(Debug)]
struct RequesterPoll {
    backoff: PollBackoff,
    next_n: u64,
    due_at: WaitMs,
    /// The poll in flight and when it was issued (ADR-47: a rid this root
    /// redirected after that moment may still be in the run it returns).
    in_flight: Option<(OpId, WaitMs)>,
    /// The width of the poll in flight (or of the last one).
    width: usize,
    /// The last poll found batches: the next one fetches a full-width
    /// run. Otherwise a poll is one GET of the next batch — nothing is
    /// expected, and a full-width probe cost `inbox_poll_width` GET-404s
    /// per idle poll (the log tail's EC2 finding R2-2 lesson, again).
    full: bool,
    /// Polled now: in the roster or the peer directory, and not
    /// P2P-connected. A requester that stops being polled (its link came
    /// back, it left the roster) keeps its entry for the rest of the
    /// tenure — cursor, backoff and due time — so a link that flaps
    /// resumes its schedule where it was instead of starting over: an
    /// idle requester's polls never outrun its backoff, however often
    /// the directory flags it down and up.
    active: bool,
}

#[derive(Debug)]
pub(crate) struct HolderPoll {
    epoch: Epoch,
    requesters: BTreeMap<NodeId, RequesterPoll>,
    executed: Vec<ExecutedBatch>,
}

/// Late-landing U1(a): tries of a background tombstone before it is dropped.
const BG_TOMBSTONE_TRIES: u32 = 6;

#[derive(Debug, Default)]
pub(crate) struct InboxState {
    // ---- requester ----
    /// Ops waiting for the submitter, in submission order.
    queue: VecDeque<(Rid, Epoch)>,
    submitting: Option<Submitting>,
    /// The batch numbering this node uses under an epoch; `None` until
    /// resynced from the bucket (a previous incarnation may have written).
    next_n: Option<(Epoch, u64)>,
    resync: Option<OpId>,
    /// Ops durable in a holder's inbox, waiting on the log.
    pub(crate) pending: BTreeMap<Rid, InboxKey>,
    /// EC2 finding 2: when `keeps_lease_for_p2p_side` last logged.
    kept_logged_at: Option<WaitMs>,
    pub(crate) recheck_timer: Option<TimerId>,
    pub(crate) retry_timer: Option<TimerId>,
    /// Late-landing U1(a): the tombstones a settle gave up on, with the
    /// tries made; a timer writes them again, a bounded number of times.
    bg_tombstones: BTreeMap<InboxKey, u32>,
    pub(crate) bg_timer: Option<TimerId>,
    // ---- escalation (round 3b) ----
    /// `(answered_at, round_trip_ms)` of inbox-answered ops, oldest
    /// first, pruned to the window.
    window: VecDeque<(WaitMs, u64)>,
    escalated_since: Option<WaitMs>,
    escalate_attempts: u32,
    escalate_last_request: Option<WaitMs>,
    pub(crate) escalate_timer: Option<TimerId>,
    // ---- holder ----
    pub(crate) holder: Option<HolderPoll>,
    pub(crate) poll_timer: Option<TimerId>,
    /// Older epochs' batches the takeover drain could not execute (an op
    /// under a live delegation: the recall runs first), in drain order.
    /// The holder only polls its own epoch, so these are run from the
    /// holder tick until done; a requester with batches here is not
    /// polled under the new epoch meanwhile (its ops stay in order).
    ///
    /// ADR-47: kept as a record of what to re-read, not as bytes. A batch
    /// read before its requester's tombstone and run after it would be
    /// refused with the `touched_gen` it was enqueued with, while the
    /// designee runs the withdrawn op; the leftovers are listed and read
    /// again from storage after the gate (`inbox_run_leftovers`).
    pub(crate) leftover: Option<Leftover>,
    /// Requesters whose inbox op halted on a write-delegation recall (a
    /// cross-subtree op), since when. While one waits, a generation that
    /// recall ended is not re-delegated (`deleg_redelegate_after_cross`):
    /// the recalled op runs at the next poll or tick, and a re-delegation
    /// in between would make it recall again, forever (found by
    /// long-delegated seed 70117 once M16's drain leftovers existed; the
    /// poll path had the same window).
    pub(crate) recall_waiters: BTreeMap<NodeId, WaitMs>,
    // ---- reachability (round 3a) ----
    /// Per holder, when its P2P path first failed since it last answered.
    down_since: BTreeMap<NodeId, WaitMs>,
}

/// The takeover drain's batches that wait for the holder tick (see
/// `InboxState::leftover`), per requester.
#[derive(Debug)]
pub(crate) struct Leftover {
    waiting: BTreeMap<NodeId, LeftoverNode>,
    /// When the gate left them (wait clock): the holder keeps its lease for
    /// them `Deadlines::leftover_hold` from then, not longer.
    pub(crate) since: WaitMs,
}

impl Leftover {
    /// Whether the holder still keeps its lease for these.
    pub(crate) fn holds_lease(&self, now: WaitMs, hold_ms: u64) -> bool {
        now < self.since.plus(hold_ms)
    }
}

/// One requester's halted batches.
#[derive(Debug)]
pub(crate) struct LeftoverNode {
    /// Its halted batches, as the gate saw them, in key order (their number
    /// is what the status counts; the bytes are never run). The re-read
    /// starts at the first.
    keys: Vec<InboxKey>,
    /// The re-read in flight, and when it was issued (wait clock).
    read: Option<(OpId, WaitMs)>,
    /// The earliest the next re-read may be issued (a halted batch is not
    /// read again at every round).
    next_read_at: WaitMs,
}

impl InboxState {
    /// The number of takeover-drain batches still waiting to be re-read.
    #[cfg(test)]
    pub(crate) fn leftover_batches(&self) -> usize {
        self.leftover
            .as_ref()
            .map_or(0, |l| l.waiting.values().map(|n| n.keys.len()).sum())
    }
}

/// What `status` shows of the inbox.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InboxView {
    pub pending_ops: u64,
    pub next_n: u64,
    pub tracked_requesters: u64,
    pub escalated: bool,
    /// The core's current write-eligible roster (`Event::Roster`).
    pub roster: Vec<NodeId>,
}

impl InboxState {
    pub(crate) fn view(&self, now: Now, cfg: &super::Config, roster: &[NodeId]) -> InboxView {
        let _ = now;
        InboxView {
            roster: roster.to_vec(),
            pending_ops: self.pending.len() as u64 + self.queue.len() as u64,
            next_n: self.next_n.map(|(_, n)| n).unwrap_or(0),
            tracked_requesters: self
                .holder
                .as_ref()
                .map(|h| h.requesters.values().filter(|r| r.active).count() as u64)
                .unwrap_or(0),
            escalated: cfg.escalation && self.escalated_since.is_some(),
        }
    }

    fn prune_window(&mut self, now: Now, window_ms: u64) {
        while self
            .window
            .front()
            .is_some_and(|(at, _)| now.wait.since(*at) > window_ms as i64)
        {
            self.window.pop_front();
        }
    }

    /// `(ops, wait_ms)` over the window: the op count, and the time this
    /// node's ops spent waiting on the inbox path with the single longest
    /// wait left out. Overlapping waits count once (M12 round 2): ops
    /// submitted together wait out the same delay — the holder's recall
    /// of a delegation on what they touch, say — and summing them made
    /// one three-second incident look like sustained demand (harness
    /// `delegate-partition`: the root handed its lease to the requester
    /// it had just recalled, on a demand of two ops).
    fn demand(&self) -> (u64, u64) {
        let mut spans: Vec<(i64, i64)> = self
            .window
            .iter()
            .map(|(at, w)| (at.0 - *w as i64, at.0))
            .collect();
        spans.sort_unstable();
        let mut union: i64 = 0;
        let mut cur: Option<(i64, i64)> = None;
        for (from, to) in spans {
            match cur {
                Some((f, t)) if from <= t => cur = Some((f, t.max(to))),
                Some((f, t)) => {
                    union += t - f;
                    cur = Some((from, to));
                }
                None => cur = Some((from, to)),
            }
        }
        if let Some((f, t)) = cur {
            union += t - f;
        }
        let largest = self.window.iter().map(|(_, w)| *w).max().unwrap_or(0);
        (
            self.window.len() as u64,
            (union.max(0) as u64).saturating_sub(largest),
        )
    }
}

/// Why an inbox op could not be executed right now.
enum Halt {
    /// The lease view is closed (releasing, or the takeover gate is
    /// pending): stop at this batch and re-fetch it next round.
    Fenced,
    /// Plan 30 §M8: the next op touches inodes other nodes hold read
    /// delegations on. Its acknowledgement is the log, which cannot be
    /// held back per op, so it is not executed until they are recalled;
    /// the requester is polled again then.
    Recall,
    /// The bytes were read at least `Deadlines::inbox_read_fresh` ago on this
    /// node's wait clock: nothing is executed or journaled from them, the key
    /// is read again (late-landing S1: the I4-safety rule of the inbox path).
    Stale,
    Meta(MetaError),
}

impl From<MetaError> for Halt {
    fn from(e: MetaError) -> Self {
        Halt::Meta(e)
    }
}

fn rid_of(op: &InboxOp) -> Rid {
    Rid {
        node: op.rid.node,
        incarnation: op.rid.incarnation,
        seq: op.rid.seq,
    }
}

impl Core {
    // ---------------------------------------------------- reachability

    /// Plan 30 M13's path-selection rule (round 3a, tightened in round 4).
    /// With P2P enabled, a holder that is in the peer directory is
    /// reachable unless an *outage* to it — a failed dial, or a transport
    /// error that evicted its connection — has lasted longer than the
    /// grace with nothing heard from it since. "Not yet talked to" means
    /// reachable: the first forward dials. A slow or timing-out reply on
    /// an open connection is never an outage.
    pub(crate) fn reaches(&mut self, now: Now, holder: NodeId) -> bool {
        if !self.cfg.p2p {
            return false;
        }
        let Some(link) = self.roster.links().get(&holder).copied() else {
            return false;
        };
        let Some(since) = self.inbox.down_since.get(&holder).copied() else {
            return true;
        };
        if link.connected || link.last_seen.is_some_and(|at| at > since) {
            tracing::debug!(
                node = self.cfg.node_id,
                holder,
                connected = link.connected,
                last_seen = ?link.last_seen,
                since = ?since,
                "reaches: the link is up again"
            );
            self.inbox.down_since.remove(&holder);
            return true;
        }
        now.wait.since(since) < self.cfg.inbox_p2p_grace_ms as i64
    }

    /// EC2 finding 2: a mutation of `node`'s executed here, over P2P
    /// (`p2p`) or through the S3 inbox.
    pub(crate) fn note_demand(&mut self, now: Now, node: NodeId, p2p: bool) {
        let map = if p2p {
            &mut self.demand_p2p
        } else {
            &mut self.demand_inbox
        };
        map.insert(node, now.wait);
    }

    /// EC2 finding 2: whether this holder keeps the lease its wanters
    /// asked for because they are on the far side of a P2P partition from
    /// the nodes using it.
    ///
    /// M13's escalation hands the lease to sustained S3-inbox demand —
    /// right when nobody has a P2P path (the `lease-handover` shape), but
    /// in a partition of an otherwise connected cluster it moved the
    /// lease *to the isolated node*: the EC2 run's holder released to it
    /// after the wanted grace, and every other node's writes then went
    /// through the inbox (or waited) until it idle-released 37 s later —
    /// a 44 s stall on the majority side while S3 and a backup were fine
    /// everywhere. So: when every wanter is one this holder cannot reach
    /// over P2P, and nodes it can reach have used it over P2P within the
    /// escalation window, the lease stays unless the far side is bigger
    /// (in nodes that used the lease in the window; the holder counts for
    /// its own side). A holder nobody reaches over P2P (an isolated
    /// holder) has no P2P demand and still hands over. Availability only:
    /// who holds the lease is still decided by CAS and TTL alone.
    pub(crate) fn keeps_lease_for_p2p_side(&mut self, now: Now) -> bool {
        if !self.cfg.p2p || self.lease.wanted.is_empty() {
            return false;
        }
        let window = self.cfg.escalate_window_ms.max(self.cfg.wanted_grace_ms) as i64;
        let wanted = self.lease.wanted.clone();
        // The same notion of "connected" the inbox's poll schedule uses:
        // a wanter still linked over P2P is served by a handoff as ever.
        let connected =
            |core: &Self, id: &NodeId| core.roster.links().get(id).is_some_and(|l| l.connected);
        if wanted.iter().any(|w| connected(self, w)) {
            return false;
        }
        let me = self.cfg.node_id;
        self.demand_p2p
            .retain(|_, at| now.wait.since(*at) <= window);
        self.demand_inbox
            .retain(|_, at| now.wait.since(*at) <= window);
        let recent: Vec<NodeId> = self.demand_p2p.keys().copied().collect();
        let mut near = 1usize;
        for node in recent {
            if node != me && !wanted.contains(&node) && connected(self, &node) {
                near += 1;
            }
        }
        if near == 1 {
            return false;
        }
        let mut far: std::collections::BTreeSet<NodeId> = wanted.iter().copied().collect();
        far.extend(self.demand_inbox.keys().copied().filter(|n| *n != me));
        let keep = near >= far.len();
        if keep {
            self.stats.leases_kept_for_p2p_side += 1;
        }
        if keep && self.demand_note_due(now) {
            tracing::info!(
                node = me,
                wanted = ?wanted,
                near,
                far = far.len(),
                "keeping the lease: its wanters are across a P2P partition from the nodes using it"
            );
        }
        keep
    }

    /// Rate-limits `keeps_lease_for_p2p_side`'s log line.
    fn demand_note_due(&mut self, now: Now) -> bool {
        let due = self
            .inbox
            .kept_logged_at
            .is_none_or(|at| now.wait.since(at) >= 10_000);
        if due {
            self.inbox.kept_logged_at = Some(now.wait);
        }
        due
    }

    /// What a P2P exchange with `holder` just said: anything heard ends
    /// an outage; a transport failure starts the grace, if none is
    /// running.
    pub(crate) fn note_p2p_result(&mut self, now: Now, holder: NodeId, heard: bool) {
        if heard {
            self.inbox.down_since.remove(&holder);
        } else {
            self.inbox.down_since.entry(holder).or_insert(now.wait);
        }
    }

    // ------------------------------------------------------ requester

    /// Queue `rid` for the holder's inbox under `epoch`.
    pub(crate) fn inbox_enqueue(
        &mut self,
        now: Now,
        rid: Rid,
        epoch: Epoch,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // An earlier attempt (P2P, or a previous epoch's batch) may have
        // executed it already — not this delegate's own uncommitted row of
        // it (D6): its generation may end without the row.
        if let Some(outcome) = super::client::completed_as_outcome(replica, rid, epoch)
            .filter(|_| self.completed_row_settled(rid, replica))
        {
            let (outcome, source) = Self::eio_unless_recorded(
                outcome,
                self.completed_source(replica, rid),
                crate::action::AnswerSource::Sequenced(constellation_meta::RowCommit::LONE),
            );
            self.finish_as(now, rid, outcome, source, replica, out);
            return;
        }
        let Some(c) = self.clients.get_mut(&rid) else {
            return;
        };
        c.phase = Phase::InboxQueued { epoch };
        c.forwarded = true;
        c.inbox_ever = true;
        // (A settle keeps the deadline it was given: the settle window's
        // end, plan 43 C2.)
        if c.inbox_since.is_none() && !c.settle {
            c.inbox_since = Some(now.wait);
            // The inbox wait has its own deadline (`min(2×TTL,
            // retention/2)`); the lease path afterwards gets the
            // ordinary one.
            let old = c.deadline;
            self.cancel_timer(old, out);
            // Plan 43 C2: the op's own attempts end a settle window early.
            let tries_for = self.dl_t().inbox_attempts;
            let deadline =
                self.set_timer_wait(now.wait.plus(tries_for), Timer::ClientDeadline(rid), out);
            self.clients.get_mut(&rid).expect("present").deadline = deadline;
        }
        if let Some(t) = self.clients.get_mut(&rid).expect("present").timer.take() {
            self.cancel_timer(t, out);
        }
        self.inbox.queue.push_back((rid, epoch));
        self.inbox_kick(now, replica, out);
    }

    /// Start the next batch PUT if none is in flight: everything queued
    /// under the highest queued epoch goes into it (ops queued under an
    /// older epoch re-read the lease).
    pub(crate) fn inbox_kick(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.inbox.submitting.is_some() || self.inbox.resync.is_some() {
            return;
        }
        let Some(epoch) = self.inbox.queue.iter().map(|(_, e)| *e).max() else {
            return;
        };
        // Stale-epoch items: the holder changed under them.
        let stale: Vec<Rid> = self
            .inbox
            .queue
            .iter()
            .filter(|(_, e)| *e < epoch)
            .map(|(rid, _)| *rid)
            .collect();
        self.inbox.queue.retain(|(_, e)| *e == epoch);
        for rid in stale {
            self.route(now, rid, replica, out);
        }
        match self.inbox.next_n {
            Some((e, _)) if e == epoch => {}
            Some((e, _)) if e < epoch => self.inbox.next_n = Some((epoch, 0)),
            _ => {
                // First submission of this mount (or an older epoch than
                // the last numbering, which cannot happen for a queued
                // op): a previous incarnation may have written under this
                // epoch — LIST-last before numbering.
                let op = self.issue_s3(
                    S3Op::InboxLastN {
                        epoch,
                        node: self.cfg.node_id,
                    },
                    S3For::InboxLastN,
                    out,
                );
                self.inbox.resync = Some(op);
                return;
            }
        }
        let n = self.inbox.next_n.expect("set above").1;
        let rids: Vec<Rid> = self
            .inbox
            .queue
            .iter()
            .take(MAX_OPS_PER_BATCH)
            .map(|(rid, _)| *rid)
            .collect();
        if rids.is_empty() {
            return;
        }
        self.inbox.queue.drain(..rids.len());
        let ops: Vec<InboxOp> = rids
            .iter()
            .filter_map(|rid| {
                let c = self.clients.get(rid)?;
                let op = c.op.to_postcard().ok()?;
                Some(InboxOp {
                    rid: InboxRid {
                        node: rid.node,
                        incarnation: rid.incarnation,
                        seq: rid.seq,
                    },
                    op,
                    deps: c.deps.to_postcard(),
                    lock_tag: c.tag.to_wire(now.honour),
                    settle: c.settle,
                    touched_gen: c.touched_gen,
                })
            })
            .collect();
        let wants_lease = self.escalated(now);
        let batch = InboxBatch {
            epoch,
            node: self.cfg.node_id,
            incarnation: self.cfg.incarnation,
            n,
            submitted_unix_ms: now.wall.0,
            ops,
            wants_lease,
        };
        let op = self.issue_s3(S3Op::InboxPut { batch }, S3For::InboxPut, out);
        // Late-landing (MUST-2): the batch may become durable at any moment
        // from here on, whatever its acknowledgement says (or whether one
        // ever comes: a timed-out PUT can land), so each op's settle or
        // forward withdraws the key from now. A tombstone over a key never
        // written is harmless (`InboxStore::put_tombstone`).
        let key = InboxKey {
            epoch,
            node: self.cfg.node_id,
            n,
        };
        for rid in &rids {
            if let Some(c) = self.clients.get_mut(rid) {
                if !c.inbox_keys.contains(&key) {
                    c.inbox_keys.push(key);
                }
            }
        }
        self.inbox.submitting = Some(Submitting {
            op,
            epoch,
            n,
            rids,
            attempt: 1,
        });
    }

    pub(crate) fn on_inbox_last_n(
        &mut self,
        now: Now,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.inbox.resync = None;
        let epoch = self.inbox.queue.iter().map(|(_, e)| *e).max().unwrap_or(0);
        match result {
            S3Result::InboxLastN(Ok(last)) => {
                self.inbox.next_n = Some((epoch, last.map(|n| n + 1).unwrap_or(0)));
                self.inbox_kick(now, replica, out);
            }
            other => {
                tracing::warn!(
                    node = self.cfg.node_id,
                    ?other,
                    "inbox: resuming batch numbering failed"
                );
                self.stats.inbox_unavailable += 1;
                let queued: Vec<Rid> = self.inbox.queue.drain(..).map(|(rid, _)| rid).collect();
                for rid in queued {
                    self.lease_path(now, rid, replica, out);
                }
            }
        }
    }

    pub(crate) fn on_inbox_put(
        &mut self,
        now: Now,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(sub) = self.inbox.submitting.take() else {
            return;
        };
        match result {
            S3Result::InboxPut(Ok(())) => {
                let key = InboxKey {
                    epoch: sub.epoch,
                    node: self.cfg.node_id,
                    n: sub.n,
                };
                self.inbox.next_n = Some((sub.epoch, sub.n + 1));
                self.stats.inbox_submitted_batches += 1;
                self.stats.inbox_submitted_ops += sub.rids.len() as u64;
                self.stats.inbox_largest_batch_ops = self
                    .stats
                    .inbox_largest_batch_ops
                    .max(sub.rids.len() as u64);
                tracing::debug!(
                    node = self.cfg.node_id,
                    epoch = sub.epoch,
                    n = sub.n,
                    ops = sub.rids.len(),
                    "inbox: submitted a batch"
                );
                for rid in sub.rids {
                    if let Some(c) = self.clients.get_mut(&rid) {
                        if matches!(c.phase, Phase::InboxQueued { .. }) {
                            c.phase = Phase::InboxWaiting { epoch: sub.epoch };
                            c.inbox_durable_at = Some(now.wait);
                            if !c.inbox_keys.contains(&key) {
                                c.inbox_keys.push(key);
                            }
                            self.inbox.pending.insert(rid, key);
                        }
                    }
                }
                self.arm_inbox_recheck(now, out);
                // Tail at the hot interval for the outcome.
                self.nudge(now, out);
                self.inbox_kick(now, replica, out);
            }
            S3Result::InboxPut(Err(CasFailure::Conflict)) => {
                // The key is taken by something that is not this batch
                // (a stale numbering): resync and retry. Not ours, so not
                // ours to withdraw either.
                let key = InboxKey {
                    epoch: sub.epoch,
                    node: self.cfg.node_id,
                    n: sub.n,
                };
                for rid in &sub.rids {
                    if let Some(c) = self.clients.get_mut(rid) {
                        c.inbox_keys.retain(|k| *k != key);
                    }
                }
                self.inbox.next_n = None;
                self.inbox_requeue_front(&sub.rids, sub.epoch);
                self.inbox_retry(now, sub.attempt, replica, out);
            }
            other => {
                tracing::warn!(
                    node = self.cfg.node_id,
                    attempt = sub.attempt,
                    epoch = sub.epoch,
                    ?other,
                    "inbox: batch PUT failed"
                );
                if sub.attempt >= SUBMIT_ATTEMPTS {
                    self.stats.inbox_unavailable += 1;
                    for rid in sub.rids {
                        self.lease_path(now, rid, replica, out);
                    }
                    self.inbox_kick(now, replica, out);
                    return;
                }
                self.inbox_requeue_front(&sub.rids, sub.epoch);
                self.inbox_retry(now, sub.attempt, replica, out);
            }
        }
    }

    fn inbox_requeue_front(&mut self, rids: &[Rid], epoch: Epoch) {
        for rid in rids.iter().rev() {
            if self.clients.contains_key(rid) {
                self.inbox.queue.push_front((*rid, epoch));
            }
        }
    }

    fn inbox_retry(
        &mut self,
        now: Now,
        attempt: u32,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let _ = replica;
        if self.inbox.retry_timer.is_some() {
            return;
        }
        let delay = 200 * u64::from(attempt);
        let id = self.set_timer_wait(now.wait.plus(delay), Timer::InboxSubmitRetry, out);
        self.inbox.retry_timer = Some(id);
        // The attempt count carries into the next PUT.
        if let Some(sub) = self.inbox.submitting.as_mut() {
            sub.attempt = attempt + 1;
        }
    }

    fn arm_inbox_recheck(&mut self, now: Now, out: &mut Vec<Action>) {
        if self.inbox.recheck_timer.is_some() || self.inbox.pending.is_empty() {
            return;
        }
        let id = self.set_timer_wait(
            now.wait.plus(self.cfg.inbox_recheck_ms.max(100)),
            Timer::InboxRecheck,
            out,
        );
        self.inbox.recheck_timer = Some(id);
    }

    /// The recheck timer: re-read the lease for the waiting ops.
    pub(crate) fn on_inbox_recheck_tick(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let _ = replica;
        if self.inbox.pending.is_empty() {
            return;
        }
        self.issue_s3(S3Op::LeaseGet, S3For::InboxRecheck, out);
        let _ = now;
    }

    pub(crate) fn on_inbox_recheck(
        &mut self,
        now: Now,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let lease = match result {
            S3Result::LeaseGet(Ok(Some((lease, _)))) => {
                self.lease.note_object(&lease);
                Some(lease)
            }
            _ => None,
        };
        let waiting: Vec<(Rid, InboxKey)> =
            self.inbox.pending.iter().map(|(r, k)| (*r, *k)).collect();
        for (rid, key) in waiting {
            let Some(c) = self.clients.get(&rid) else {
                self.inbox.pending.remove(&rid);
                continue;
            };
            let Phase::InboxWaiting { epoch } = c.phase else {
                continue;
            };
            let since = c.inbox_since.unwrap_or(now.wait);
            match &lease {
                Some(l) if l.holder == self.cfg.node_id => {
                    // Round 3b: this node took the lease (an escalation
                    // landed). Its takeover gate drains the batch and
                    // writes the outcome locally; the lease path resolves
                    // the rid against `completed` once the gate is done.
                    self.inbox.pending.remove(&rid);
                    self.lease_path(now, rid, replica, out);
                }
                Some(l) if l.epoch > epoch => {
                    // Stranded under an older epoch: delete the stale
                    // batch (a drain that already read it is unaffected)
                    // and re-submit the same rid under the new one.
                    self.inbox.pending.remove(&rid);
                    if let Some(c) = self.clients.get_mut(&rid) {
                        c.inbox_keys.retain(|k| *k != key);
                    }
                    self.issue_s3(S3Op::InboxDelete { key }, S3For::InboxGc(key), out);
                    self.stats.inbox_resubmitted_ops += 1;
                    tracing::info!(
                        node = self.cfg.node_id,
                        ?rid,
                        old_epoch = epoch,
                        epoch = l.epoch,
                        "inbox: a takeover stranded a submitted op; re-submitting by rid"
                    );
                    let new_epoch = l.epoch;
                    if self.lease.claimable_observed(now, &self.cfg) {
                        self.stats.inbox_unavailable += 1;
                        self.lease_path(now, rid, replica, out);
                    } else {
                        // No hold for a designee here (`on_holder_learned`
                        // has one): an op is `InboxWaiting` only after it
                        // passed that hold, and nothing marks it sent to a
                        // designee while it stays in the inbox (`send_forward`
                        // withdraws the batch first, then the mark is set and
                        // the op leaves this phase). A drain may have read
                        // the old-epoch batch already, which a guard here
                        // could not undo either.
                        self.inbox_enqueue(now, rid, new_epoch, replica, out);
                    }
                }
                Some(l)
                    if self.cfg.forwarding
                        && l.holder != self.cfg.node_id
                        && l.holder != 0
                        && l.epoch == epoch
                        && !self.lease.claimable_observed(now, &self.cfg)
                        && self
                            .roster
                            .links()
                            .get(&l.holder)
                            .is_some_and(|k| k.connected)
                        && self.reaches(now, l.holder) =>
                {
                    // EC2 follow-up: the op went to the inbox while the
                    // holder was not (yet) P2P-reachable — a node mounting
                    // together with others often learns the holder from
                    // the lease before its link to it is up. The link is
                    // up now, and a holder polls only the inboxes of
                    // requesters it is *not* connected to, so the batch
                    // would sit until the inbox deadline (2×TTL), then the
                    // op went in doubt (EIO). Forward it by rid instead;
                    // `send_forward` withdraws the batch first, and a
                    // holder that executed it already answers from
                    // `completed` (exactly once either way).
                    self.inbox.pending.remove(&rid);
                    self.stats.inbox_rerouted_to_p2p += 1;
                    tracing::info!(
                        node = self.cfg.node_id,
                        ?rid,
                        holder = l.holder,
                        "inbox: the holder is reachable over P2P now; forwarding the waiting op"
                    );
                    if let Some(c) = self.clients.get_mut(&rid) {
                        c.attempts = 0;
                        c.fence_bounces = 0;
                        c.redirected = 0;
                    }
                    self.send_forward(now, rid, l.holder, 0, replica, out);
                }
                Some(_) if self.lease.claimable_observed(now, &self.cfg) => {
                    // The holder is gone; whoever takes over drains the
                    // batch. The lease path resolves the rid.
                    self.inbox.pending.remove(&rid);
                    self.stats.inbox_unavailable += 1;
                    self.lease_path(now, rid, replica, out);
                }
                _ => {
                    if now.wait.since(since) >= self.cfg.inbox_deadline_ms as i64 {
                        tracing::warn!(
                            node = self.cfg.node_id,
                            ?rid,
                            epoch,
                            "inbox: no outcome before the deadline; the op is in doubt and takes \
                             the lease path"
                        );
                        self.inbox.pending.remove(&rid);
                        self.stats.inbox_unavailable += 1;
                        self.lease_path(now, rid, replica, out);
                    }
                }
            }
        }
        self.arm_inbox_recheck(now, out);
    }

    /// An inbox op's outcome arrived through the log: feed the demand
    /// window (`finish` follows).
    pub(crate) fn inbox_answered(&mut self, now: Now, rid: Rid) {
        let Some(c) = self.clients.get(&rid) else {
            return;
        };
        let queued_at = c.inbox_since.unwrap_or(now.wait);
        let durable_at = c.inbox_durable_at.unwrap_or(now.wait);
        let queue_wait = durable_at.since(queued_at).max(0) as u64;
        let outcome_wait = now.wait.since(durable_at).max(0) as u64;
        self.stats.inbox_queue_wait_ms_total += queue_wait;
        self.stats.inbox_outcome_wait_ms_total += outcome_wait;
        self.stats.inbox_round_trip_ms_total += queue_wait + outcome_wait;
        self.stats.inbox_answered += 1;
        tracing::debug!(
            node = self.cfg.node_id,
            ?rid,
            queue_wait_ms = queue_wait,
            outcome_wait_ms = outcome_wait,
            "inbox: op answered through the log"
        );
        self.note_inbox_op(now, queue_wait + outcome_wait);
    }

    /// The op left the machine (answered, in doubt, or gone): drop it
    /// from the queue and the waiters.
    pub(crate) fn inbox_forget(&mut self, rid: Rid) {
        self.inbox.pending.remove(&rid);
        self.inbox.queue.retain(|(r, _)| *r != rid);
    }

    // ----------------------------------------------------- escalation

    fn note_inbox_op(&mut self, now: Now, round_trip_ms: u64) {
        self.inbox.window.push_back((now.wait, round_trip_ms));
        self.evaluate_escalation(now);
    }

    fn evaluate_escalation(&mut self, now: Now) {
        self.inbox.prune_window(now, self.cfg.escalate_window_ms);
        // Single-writer mode: this node can never claim another node's
        // lease (`Config::single_writer`), so it never asks for it.
        if !self.cfg.escalation || self.cfg.single_writer {
            self.inbox.escalated_since = None;
            return;
        }
        let (ops, wait_ms) = self.inbox.demand();
        let sustained = ops >= self.cfg.escalate_ops
            || (ops >= ESCALATE_WAIT_MIN_OPS && wait_ms >= self.cfg.escalate_wait_ms);
        let quiet = (ops as f64) < self.cfg.escalate_ops as f64 * DEESCALATE_FRACTION
            && (wait_ms as f64) < self.cfg.escalate_wait_ms as f64 * DEESCALATE_FRACTION;
        match (self.inbox.escalated_since, sustained, quiet) {
            (None, true, _) => {
                self.inbox.escalated_since = Some(now.wait);
                self.inbox.escalate_attempts = 0;
                self.inbox.escalate_last_request = None;
                self.stats.inbox_escalations += 1;
                tracing::info!(
                    node = self.cfg.node_id,
                    ops,
                    wait_ms,
                    window_ms = self.cfg.escalate_window_ms,
                    "inbox: sustained demand; asking for the lease"
                );
            }
            (Some(since), _, true) => {
                tracing::info!(
                    node = self.cfg.node_id,
                    escalated_for_ms = now.wait.since(since),
                    "inbox: demand fell off; no longer asking for the lease"
                );
                self.inbox.escalated_since = None;
            }
            _ => {}
        }
    }

    /// Whether this node's inbox demand is sustained (it is, or should
    /// be, asking for the lease). Re-evaluated on read so an escalation
    /// with no further ops still expires with its window. While
    /// escalated the tick that asks is armed.
    pub(crate) fn escalated(&mut self, now: Now) -> bool {
        self.evaluate_escalation(now);
        // Plan 31 C8: a forward-only node never asks for the lease.
        self.inbox.escalated_since.is_some() && !self.mode.forwards()
    }

    /// Arm the escalator's tick (from wherever an escalation can begin).
    fn arm_escalate_tick(&mut self, now: Now, out: &mut Vec<Action>) {
        if self.inbox.escalate_timer.is_some() || self.inbox.escalated_since.is_none() {
            return;
        }
        let id = self.set_timer_wait(now.wait.plus(ESCALATE_TICK_MS), Timer::EscalateTick, out);
        self.inbox.escalate_timer = Some(id);
    }

    /// While escalated and not holding, ask for the lease through the
    /// ordinary acquisition (which registers `wanted_by`), backing off
    /// from 100 ms doubling to `escalate_retry_ms`; ops keep going
    /// through the inbox meanwhile, and the batches carry `wants_lease`.
    pub(crate) fn on_escalate_tick(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.escalated(now) {
            return;
        }
        // M12 round 2: the inbox was a detour — the holder was briefly
        // unreachable over P2P (harness `delegate-partition` and
        // `shared-dir-multi-writer` under load: a link marked down for
        // a few ops) — and is reachable again: the forwards resume, so
        // the demand is over. Asking for the lease now would move it off
        // a root the operator pinned (the wanted grace binds only the
        // S3 path; a P2P handoff is served at once), for nothing. A
        // node with no P2P path (M13) is unaffected.
        if let Some(holder) = self.lease.cached_holder.filter(|h| *h != self.cfg.node_id) {
            if self.cfg.p2p && self.reaches(now, holder) {
                tracing::info!(
                    node = self.cfg.node_id,
                    holder,
                    "inbox: the holder is reachable over P2P again; no longer asking for the lease"
                );
                self.inbox.escalated_since = None;
                self.inbox.window.clear();
                return;
            }
        }
        let held = self.lease.ship_epoch(now, &self.cfg).is_some();
        if !held {
            let backoff =
                (100u64 << self.inbox.escalate_attempts.min(10)).min(self.cfg.escalate_retry_ms);
            let due = self
                .inbox
                .escalate_last_request
                .is_none_or(|at| now.wait.since(at) >= backoff as i64);
            if due {
                self.inbox.escalate_last_request = Some(now.wait);
                self.inbox.escalate_attempts = self.inbox.escalate_attempts.saturating_add(1);
                self.stats.inbox_lease_requests += 1;
                self.enqueue_job(
                    now,
                    super::jobs::JobReq::Acquire {
                        reason: "inbox-escalation",
                        ask_handoff: self.cfg.p2p,
                    },
                    replica,
                    out,
                );
            }
        }
        self.arm_escalate_tick(now, out);
    }

    /// Called after every event that could have started an escalation.
    pub(crate) fn inbox_after_event(&mut self, now: Now, out: &mut Vec<Action>) {
        if self.inbox.escalated_since.is_some() {
            self.arm_escalate_tick(now, out);
        }
    }

    // --------------------------------------------------------- holder

    /// Who this holder polls: the roster and the peer directory, minus
    /// itself and the peers it is P2P-connected to.
    fn requesters_to_poll(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self.roster.write_eligible().to_vec();
        let connected =
            |id: &NodeId| self.cfg.p2p && self.roster.links().get(id).is_some_and(|l| l.connected);
        for node in self.roster.links().keys() {
            if !connected(node) {
                ids.push(*node);
            }
        }
        ids.retain(|id| *id != self.cfg.node_id && *id != 0 && !connected(id));
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Whether `node` has shown demand recently enough that a holder
    /// starting to poll it should poll it warm: a mutation of its seen
    /// here over P2P or through the inbox within the warm span (the warm
    /// ceiling times `COLD_AFTER_ROUNDS`, when a silent requester would
    /// have gone cold anyway), or it wants the lease. With P2P off the
    /// inbox is every requester's only path and each is tracked once per
    /// tenure, so all start warm as before.
    fn inbox_recent_demand(&self, now: Now, node: NodeId) -> bool {
        if !self.cfg.p2p {
            return true;
        }
        let span = self
            .cfg
            .inbox_warm_max_ms
            .saturating_mul(constellation_store_s3::inbox::COLD_AFTER_ROUNDS as u64)
            as i64;
        let recent = |m: &BTreeMap<NodeId, WaitMs>| {
            m.get(&node).is_some_and(|at| now.wait.since(*at) <= span)
        };
        recent(&self.demand_p2p) || recent(&self.demand_inbox) || self.lease.wanted.contains(&node)
    }

    /// The holder's inbox work, from the end of every round and from the
    /// poll timer: GC what shipped, then GET-next every due requester.
    pub(crate) fn inbox_holder_tick(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.cfg.inbox {
            return;
        }
        let epoch = match self.lease.ship_epoch(now, &self.cfg) {
            Some(e) if !self.lease.fenced() && !self.lease.epoch_held() => e,
            _ => {
                self.inbox.holder = None;
                // Still in the bucket: the next holder's gate drains them.
                self.inbox.leftover = None;
                self.inbox.recall_waiters.clear();
                return;
            }
        };
        if self.inbox.holder.as_ref().is_none_or(|h| h.epoch != epoch) {
            self.inbox.holder = Some(HolderPoll {
                epoch,
                requesters: BTreeMap::new(),
                executed: Vec::new(),
            });
            out.push(Action::RefreshRoster);
        }
        let tracked = self.requesters_to_poll();
        let (base, warm, cold, hot, grace, width) = (
            self.cfg.sync_interval_ms.max(1),
            self.cfg.inbox_warm_max_ms,
            self.cfg.inbox_cold_max_ms,
            self.cfg.inbox_hot_ms,
            self.cfg.inbox_hot_grace,
            self.cfg.inbox_poll_width.max(1),
        );
        let fresh: Vec<(NodeId, bool)> = {
            let known = self.inbox.holder.as_ref().expect("set above");
            tracked
                .iter()
                .filter(|id| !known.requesters.contains_key(id))
                .map(|id| (*id, self.inbox_recent_demand(now, *id)))
                .collect()
        };
        let holder = self.inbox.holder.as_mut().expect("set above");
        for (id, rp) in holder.requesters.iter_mut() {
            rp.active = tracked.contains(id);
        }
        for (id, demand) in fresh {
            tracing::debug!(
                node = self.cfg.node_id,
                requester = id,
                epoch,
                demand,
                "inbox: tracking a requester"
            );
            // Polled at once, then never hot — only a hit (a requester
            // writing right now) earns that — and warm only on a sign of
            // recent demand; otherwise cold, what an idle requester
            // costs. Nothing but demand ever moves it back up.
            let backoff = PollBackoff::two_tier(base, warm, cold).with_hot(hot, grace);
            holder.requesters.insert(
                id,
                RequesterPoll {
                    backoff: if demand {
                        backoff.past_hot()
                    } else {
                        backoff.gone_cold()
                    },
                    next_n: 0,
                    due_at: now.wait,
                    in_flight: None,
                    width: 1,
                    full: false,
                    active: true,
                },
            );
        }

        // GC: batches whose rows have shipped, all but each requester's
        // newest executed one (the LIST-last high-water mark).
        let acked = replica.journal_acked_seq().unwrap_or(0);
        let (ready, waiting): (Vec<ExecutedBatch>, Vec<ExecutedBatch>) = holder
            .executed
            .drain(..)
            .partition(|(_, seq)| *seq <= acked);
        let ready_keys: Vec<InboxKey> = ready.iter().map(|(k, _)| *k).collect();
        let victims = gc_keep_newest(&ready_keys);
        holder.executed = waiting;
        let mut deletes = Vec::new();
        for (key, seq) in ready {
            if victims.contains(&key) {
                deletes.push(key);
            } else {
                holder.executed.push((key, seq));
            }
        }
        for key in deletes {
            self.issue_s3(S3Op::InboxDelete { key }, S3For::InboxGc(key), out);
        }
        // The takeover drain's leftovers first: a requester's older
        // batches run before anything it submitted under this epoch.
        let waiting = self.inbox_run_leftovers(now, out);
        let Some(holder) = self.inbox.holder.as_mut() else {
            return;
        };
        // Polls due now.
        let mut polls = Vec::new();
        let mut next_due: Option<WaitMs> = None;
        if self.inbox.leftover.is_some() {
            next_due = Some(now.wait.plus(base));
        }
        for (node, rp) in holder.requesters.iter_mut() {
            if !rp.active || rp.in_flight.is_some() || waiting.contains(node) {
                continue;
            }
            if rp.due_at <= now.wait {
                rp.width = if rp.full || rp.backoff.is_hot() {
                    width
                } else {
                    1
                };
                polls.push((*node, rp.next_n, rp.width));
            } else {
                next_due = Some(next_due.map_or(rp.due_at, |d: WaitMs| d.min(rp.due_at)));
            }
        }
        for (node, from, width) in polls {
            let op = self.issue_s3(
                S3Op::InboxRun {
                    epoch,
                    node,
                    from,
                    width,
                },
                S3For::InboxPoll(node),
                out,
            );
            self.stats.inbox_polls += 1;
            if let Some(rp) = self
                .inbox
                .holder
                .as_mut()
                .and_then(|h| h.requesters.get_mut(&node))
            {
                rp.in_flight = Some((op, now.wait));
            }
        }
        if let Some(at) = next_due {
            self.arm_inbox_poll(at, out);
        }
    }

    fn arm_inbox_poll(&mut self, at: WaitMs, out: &mut Vec<Action>) {
        if let Some(id) = self.inbox.poll_timer.take() {
            self.cancel_timer(id, out);
        }
        let id = self.set_timer_wait(at, Timer::InboxPoll, out);
        self.inbox.poll_timer = Some(id);
    }

    pub(crate) fn on_inbox_polled(
        &mut self,
        now: Now,
        node: NodeId,
        op: OpId,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.inbox_poll_result(now, node, op, result, replica, out);
    }

    fn inbox_poll_result(
        &mut self,
        now: Now,
        node: NodeId,
        op: OpId,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(holder) = self.inbox.holder.as_mut() else {
            return;
        };
        let Some(rp) = holder.requesters.get_mut(&node) else {
            return;
        };
        // Late-landing S1: the freshness check needs this read's own issue
        // time. A poll of an earlier holder state (the requester was
        // rebuilt, or a newer poll is in flight) has none: drop it.
        let Some((flying, read_at)) = rp.in_flight else {
            return;
        };
        if flying != op {
            return;
        }
        rp.in_flight = None;
        let run = match result {
            S3Result::InboxRun(Ok(run)) => run,
            other => {
                tracing::warn!(
                    node = self.cfg.node_id,
                    requester = node,
                    ?other,
                    "inbox poll failed"
                );
                rp.backoff.miss();
                rp.due_at = now.wait.plus(rp.backoff.delay_ms());
                let at = rp.due_at;
                self.arm_inbox_poll_min(at, out);
                return;
            }
        };
        // Saturated against the width actually fetched: a one-GET probe
        // that hits is followed at once by a full-width run.
        let width = rp.width.max(1);
        if run.is_empty() {
            rp.full = false;
            rp.backoff.miss();
            rp.due_at = now.wait.plus(rp.backoff.delay_ms());
            let at = rp.due_at;
            self.arm_inbox_poll_min(at, out);
            return;
        }
        rp.backoff.hit();
        rp.full = true;
        self.stats.inbox_poll_hits += 1;
        tracing::debug!(
            node = self.cfg.node_id,
            requester = node,
            batches = run.len(),
            oldest_ms = run
                .iter()
                .map(|b| now.wall.0.saturating_sub(b.submitted_unix_ms))
                .max()
                .unwrap_or(0),
            idle_rounds_before = rp.backoff.idle_rounds(),
            "inbox: poll found batches"
        );
        for batch in &run {
            self.stats.inbox_pickup_ms_total +=
                now.wall.0.saturating_sub(batch.submitted_unix_ms).max(0) as u64;
            self.stats.inbox_pickup_samples += 1;
        }
        let saturated = run.len() >= width;
        let mut halted = false;
        let mut stale = false;
        // Plan 30 §M8: another node's ops — the lone-node kernel latch.
        self.note_foreign(now, replica, out);
        for batch in &run {
            if batch.is_tombstone() {
                // A withdrawn batch: nothing to run, and no demand (its
                // ops went over P2P or back into a later batch).
                self.stats.inbox_tombstones_read += 1;
            } else {
                self.note_demand(now, batch.node, false);
            }
            if batch.wants_lease {
                self.lease.note_wanted(now, batch.node);
            }
            match self.execute_inbox_batch(now, batch, read_at, true, replica, out) {
                Ok(seq) => {
                    let holder = self.inbox.holder.as_mut().expect("holding");
                    holder.executed.push((batch.key(), seq));
                    if let Some(rp) = holder.requesters.get_mut(&node) {
                        rp.next_n = batch.n + 1;
                    }
                }
                Err(Halt::Fenced) => {
                    halted = true;
                    break;
                }
                Err(Halt::Recall) => {
                    // Re-polled when the recalls are done (or at the
                    // ordinary halted cadence, whichever is first).
                    halted = true;
                    break;
                }
                Err(Halt::Stale) => {
                    // The rest of the run is as old: discarded, read again
                    // at once.
                    halted = true;
                    stale = true;
                    break;
                }
                Err(Halt::Meta(error)) => {
                    tracing::warn!(node = self.cfg.node_id, %error, "executing an inbox batch");
                    halted = true;
                    break;
                }
            }
        }
        self.nudge(now, out);
        let base = self.cfg.sync_interval_ms.max(1);
        if let Some(rp) = self
            .inbox
            .holder
            .as_mut()
            .and_then(|h| h.requesters.get_mut(&node))
        {
            rp.due_at = if stale {
                now.wait
            } else if halted {
                now.wait.plus(base)
            } else if saturated {
                now.wait
            } else {
                now.wait.plus(rp.backoff.delay_ms())
            };
            let at = rp.due_at;
            self.arm_inbox_poll_min(at, out);
        }
    }

    /// Plan 30 §M8: the recalls an inbox op waited for are done: poll its
    /// requester again now.
    pub(crate) fn inbox_repoll(&mut self, now: Now, node: NodeId, out: &mut Vec<Action>) {
        let Some(rp) = self
            .inbox
            .holder
            .as_mut()
            .and_then(|h| h.requesters.get_mut(&node))
        else {
            return;
        };
        rp.due_at = now.wait;
        self.arm_inbox_poll_min(now.wait, out);
    }

    fn arm_inbox_poll_min(&mut self, at: WaitMs, out: &mut Vec<Action>) {
        let sooner = match self.inbox.poll_timer {
            Some(id) => match self.timer_at(id) {
                None => true,
                Some(TimerAt::Wait(t)) => at < t,
                Some(_) => {
                    debug_assert!(false, "the inbox poll timer left the wait clock");
                    false
                }
            },
            None => true,
        };
        if sooner {
            self.arm_inbox_poll(at, out);
        }
    }

    /// The requester's own batch withdrawal before a P2P forward
    /// (`Phase::InboxWithdraw`): the batch is a tombstone now, so the
    /// forward proceeds; a failed overwrite leaves the batch drainable,
    /// so the op takes the lease path instead, where this node's own gate
    /// drains it and `completed` is exact.
    ///
    /// The tombstone withdrew every op the batch carried. The others
    /// still waiting on it (co-batched ops that are not being forwarded
    /// themselves) are re-submitted by rid, at the front of the queue in
    /// their submission order: a holder that read the batch before the
    /// overwrite executed them already and deduplicates the copies by
    /// rid; one that reads the tombstone runs them from the new batch.
    pub(crate) fn on_inbox_withdrawn(
        &mut self,
        now: Now,
        rid: Rid,
        key: InboxKey,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(c) = self.clients.get_mut(&rid) else {
            return;
        };
        // A settle's own tombstones (`ClientOp::withdraw`) run beside it,
        // whatever its phase is.
        if c.withdraw.is_some() {
            self.on_settle_withdrawn(now, rid, key, result, replica, out);
            return;
        }
        let Phase::InboxWithdraw { holder, gen } = c.phase else {
            return;
        };
        // An answer for a key this rid no longer holds is a duplicate of one
        // already handled (a tombstone written twice): it moves nothing.
        if !c.inbox_keys.contains(&key) {
            return;
        }
        match result {
            S3Result::InboxTombstone(Ok(())) => {
                // `send_forward` withdraws the next batch, if any, before
                // it forwards.
                c.inbox_keys.retain(|k| *k != key);
                self.stats.inbox_withdrawn_ops += 1;
                self.inbox_forget_leftover(key);
                self.inbox_resubmit_cobatched(now, key, replica, out);
                self.send_forward(now, rid, holder, gen, replica, out);
            }
            other => {
                tracing::debug!(
                    node = self.cfg.node_id,
                    ?rid,
                    ?other,
                    "inbox: withdrawing the batch failed; the op takes the lease path"
                );
                self.lease_path(now, rid, replica, out);
            }
        }
    }

    /// Late-landing U1(a): the settle gave up on these batches' tombstones
    /// (its client hears the unsettled `EIO`); keep writing them in the
    /// background, `BG_TOMBSTONE_TRIES` times, so a batch no drain read yet
    /// is not left to be executed later.
    pub(crate) fn bg_tombstones_add(
        &mut self,
        now: Now,
        keys: Vec<InboxKey>,
        out: &mut Vec<Action>,
    ) {
        for key in keys {
            self.inbox.bg_tombstones.entry(key).or_insert(0);
        }
        self.arm_bg_tombstone(now, 0, out);
    }

    fn arm_bg_tombstone(&mut self, now: Now, tries: u32, out: &mut Vec<Action>) {
        if self.inbox.bg_timer.is_some() || self.inbox.bg_tombstones.is_empty() {
            return;
        }
        let base = self.dl_t().settle_window.max(100);
        let delay = base << tries.min(4);
        let id = self.set_timer_wait(now.wait.plus(delay), Timer::InboxTombstoneBg, out);
        self.inbox.bg_timer = Some(id);
    }

    pub(crate) fn on_bg_tombstone_tick(&mut self, now: Now, out: &mut Vec<Action>) {
        let keys: Vec<InboxKey> = self.inbox.bg_tombstones.keys().copied().collect();
        for key in keys {
            let batch = constellation_store_s3::inbox::InboxBatch::tombstone(
                key,
                self.cfg.incarnation,
                now.wall.0,
            );
            self.issue_s3(
                S3Op::InboxTombstone { batch },
                S3For::InboxTombstoneBg(key),
                out,
            );
        }
    }

    pub(crate) fn on_bg_tombstone(
        &mut self,
        now: Now,
        key: InboxKey,
        result: S3Result,
        out: &mut Vec<Action>,
    ) {
        if matches!(result, S3Result::InboxTombstone(Ok(()))) {
            if self.inbox.bg_tombstones.remove(&key).is_some() {
                self.stats.bg_tombstones_written += 1;
            }
            return;
        }
        let Some(tries) = self.inbox.bg_tombstones.get_mut(&key) else {
            return;
        };
        *tries += 1;
        let tries = *tries;
        if tries >= BG_TOMBSTONE_TRIES {
            self.inbox.bg_tombstones.remove(&key);
            self.stats.bg_tombstones_dropped += 1;
            return;
        }
        self.arm_bg_tombstone(now, tries, out);
    }

    /// Late-landing F1 (the own purge): `key` is a tombstone now, so a batch
    /// of this node's own that the takeover drain left for a re-read is not
    /// read again. A shortcut: the freshness rule (S1) is what keeps an
    /// older read from running it.
    pub(crate) fn inbox_forget_leftover(&mut self, key: InboxKey) {
        let Some(l) = self.inbox.leftover.as_mut() else {
            return;
        };
        if let Some(n) = l.waiting.get_mut(&key.node) {
            n.keys.retain(|k| *k != key);
        }
        l.waiting.retain(|_, n| !n.keys.is_empty());
        if l.waiting.is_empty() {
            self.inbox.leftover = None;
        }
    }

    /// `key` is a tombstone now: every op still waiting on it alone goes
    /// back to the front of the submission queue (see
    /// `on_inbox_withdrawn`). An op being withdrawn itself, or waiting on
    /// a later batch too, only forgets the key.
    pub(crate) fn inbox_resubmit_cobatched(
        &mut self,
        now: Now,
        key: InboxKey,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let mut again: Vec<(u64, Rid)> = Vec::new();
        for (rid, c) in self.clients.iter_mut() {
            if !c.inbox_keys.contains(&key)
                || c.withdraw.is_some()
                || matches!(c.phase, Phase::InboxWithdraw { .. })
            {
                continue;
            }
            c.inbox_keys.retain(|k| *k != key);
            if matches!(c.phase, Phase::InboxWaiting { .. })
                && self.inbox.pending.get(rid) == Some(&key)
            {
                c.phase = Phase::InboxQueued { epoch: key.epoch };
                again.push((c.order, *rid));
            }
        }
        if again.is_empty() {
            return;
        }
        again.sort_unstable();
        let rids: Vec<Rid> = again.into_iter().map(|(_, rid)| rid).collect();
        for rid in &rids {
            self.inbox.pending.remove(rid);
        }
        self.stats.inbox_resubmitted_ops += rids.len() as u64;
        tracing::debug!(
            node = self.cfg.node_id,
            ?key,
            ops = rids.len(),
            "inbox: re-submitting the ops a withdrawn batch also carried"
        );
        self.inbox_requeue_front(&rids, key.epoch);
        self.inbox_kick(now, replica, out);
    }

    pub(crate) fn on_inbox_gc(&mut self, now: Now, key: InboxKey, result: S3Result) {
        let _ = now;
        match result {
            S3Result::InboxDelete(Ok(())) => self.stats.inbox_gc_deleted += 1,
            other => {
                tracing::debug!(
                    node = self.cfg.node_id,
                    ?key,
                    ?other,
                    "inbox: delete failed; retrying next round"
                );
                if let Some(holder) = self.inbox.holder.as_mut() {
                    holder.executed.push((key, 0));
                }
            }
        }
    }

    /// Execute every op of `batch` in order (`read_at`: when the GET that
    /// returned it was issued, on the wait clock); returns the journal seq its
    /// last row landed at (for GC), or where it had to stop. `admitted`
    /// is the poll path (through the lease view like every other local
    /// mutation); the takeover gate passes `false` (the view is
    /// deliberately closed and the gate itself is the authority).
    fn execute_inbox_batch(
        &mut self,
        now: Now,
        batch: &InboxBatch,
        read_at: WaitMs,
        admitted: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Result<u64, Halt> {
        // The executor's freshness rule (late-landing S1): an inbox op runs
        // only from a GET this node issued less than `f` ago on its own wait
        // clock. This is synchronous, so a batch that passes here runs
        // wholly inside the window. A requester that settled has its batch
        // tombstoned a full window before the client may hear an unsettled
        // `EIO`; a read older than half that window is the only one that
        // could have seen the batch before the tombstone.
        if now.wait.since(read_at) >= self.dl_t().inbox_read_fresh as i64 {
            self.stats.inbox_stale_reads_discarded += 1;
            return Err(Halt::Stale);
        }
        // Plan 43 C1-2: every row of the batch is journaled under the class
        // the answers to the batch's node wait for now. Plan 43 C2 (§2.1):
        // another node's — its requester learns the outcome from the log, and
        // a lone root's row of it is `Archive`, so its deposition keeps the
        // row as a member copy instead of queueing a second copy of the op
        // beside the batch the next holder drains.
        self.note_heard(now, batch.node, replica);
        let _class = self.journal_class_for(self.asker_of(batch.node));
        for (i, op) in batch.ops.iter().enumerate() {
            let rid = rid_of(op);
            let ack = InboxAck {
                epoch: batch.epoch,
                node: batch.node,
                n: batch.n,
                i: i as u32,
            };
            // The position watermark first: answered by an earlier
            // tenure, past whatever `completed` still remembers.
            if replica.inbox_ack_covers(ack) {
                self.stats.inbox_deduped_ops += 1;
                continue;
            }
            // Admitted before anything is journaled, the acknowledgement of
            // a duplicate below included: a poll that landed while the lease
            // was being released (a handoff's CAS in flight, the journal
            // drained) journaled that ack after the drain, and the release
            // left it behind unshipped: a `Group{Root}` row of a tenure that
            // ended with no tail, kept as a member copy for ever
            // (long-backup seed 1421). The batch stays in the inbox for the
            // next holder's drain.
            if admitted && self.lease.new_mutation_epoch(now, &self.cfg).is_none() {
                return Err(Halt::Fenced);
            }
            // Then the rid: executed or refused already (this tenure's
            // `recent` or journal, or the log), by any path.
            if replica.recent_outcome(rid).is_some() || replica.completed_outcome(rid)?.is_some() {
                replica.journal_inbox_ack(ack)?;
                self.stats.inbox_deduped_ops += 1;
                continue;
            }
            tracing::debug!(
                node = self.cfg.node_id,
                ?rid,
                completed = ?replica.completed_outcome(rid),
                "inbox: executing a batch op"
            );
            let decoded = MutateOp::from_postcard(&op.op);
            let op_touched_gen = op.touched_gen;
            // Plan 30 §M8: recall before executing (see `Halt::Recall`).
            let touched = decoded
                .as_ref()
                .map(|op| replica.recall_inos_of_op(op))
                .unwrap_or_default();
            if admitted
                && !op.settle
                && self.inbox_recall_first(now, batch.node, &touched, replica, out)
            {
                return Err(Halt::Recall);
            }
            // Plan 46 M0.2 (R3): behind the replay queue on its keys, as a
            // forwarded op is (`Core::queue_gate_blocks`): the batch halts
            // here and is polled again at the halted cadence. Every op of
            // the batch behind this one waits with it, up to the
            // placeholder's bound: a requester's batch is executed in
            // order (the order is the requester's program order, and the
            // batch is acknowledged by position), so it is not reordered
            // to let the later ops past. Counted
            // (`status.ack.inbox_halted`).
            if !op.settle {
                if let Ok(decoded_op) = &decoded {
                    if self.queue_gate_blocks(rid, decoded_op, replica) {
                        self.stats.ops_parked_behind_queue += 1;
                        self.stats.inbox_halted += 1;
                        return Err(Halt::Recall);
                    }
                }
            }
            // M16: the op's `deps`, as a forward's: a delegate's stream
            // it depends on (a marker written after data the delegate
            // acknowledged) must be here before the op executes, or the
            // log would carry the marker ahead of the data (harness and
            // sim `marker-order`; long-delegated seed 70491 once the drain
            // no longer stopped at the first delegated op). Inside the
            // takeover gate the delegates have not re-streamed to this
            // root yet: the requester's batches are deferred; after it,
            // the batch waits (`Halt::Recall`: retried at the halted
            // cadence) — the delegate re-streams, or the root reclaims the
            // generation and voids its stream.
            // (Plan 43 C2: a settle has no effect to order: it never waits
            // for its `deps`.)
            if self.cfg.delegation && !op.settle && !op.deps.is_empty() {
                let deps = constellation_meta::Position::from_postcard(&op.deps);
                if !replica.reaches_streams(&deps) {
                    self.stats.deleg_deps_waits += 1;
                    return Err(Halt::Recall);
                }
                // A dependency its generation ended without: the op stays
                // in the inbox; its requester withdraws it and re-sends it
                // after its own replay of the cause
                // (`withdraw_lost_deps_inbox_ops`).
                if replica.deps_lost(&deps) {
                    self.stats.deps_lost_refused += 1;
                    tracing::debug!(
                        node = self.cfg.node_id,
                        ?rid,
                        ?deps,
                        "an execution whose deps were lost with their generation is refused"
                    );
                    return Err(Halt::Recall);
                }
            }
            // Plan 30 §M11: the same for write delegations — an inbox op
            // in a delegated subtree waits for the recall (the delegate's
            // stream is appended first), like a forwarded one. Inside the
            // takeover gate too (phase 2b): a successor's drain must not
            // run ahead of a live delegate's stream.
            if !admitted && self.cfg.delegation {
                // Inside the takeover gate the root's generation state is
                // not built yet (`delegation_sync` follows the gate): the
                // table decides — an op under any live delegation stays
                // in the inbox for the poll that follows the gate.
                if let Ok(op) = &decoded {
                    let keys = super::holder::keys_of_op_in(op, replica);
                    if !matches!(
                        replica.resolve_ownership(&keys),
                        constellation_meta::delegation::Ownership::Root
                    ) {
                        // ADR-44: once the gate's journal work is done it
                        // recalls what the op needs, on demand, while the op
                        // waits for the poll after the open.
                        self.gate_recall_on_demand(now, None, &keys, replica, out);
                        return Err(Halt::Recall);
                    }
                }
            }
            if self.cfg.delegation && self.deleg_known() {
                if let Ok(op) = &decoded {
                    let keys = super::holder::keys_of_op_in(op, replica);
                    match self.deleg_recall_plan(now, &keys, replica, out) {
                        super::delegate::RecallPlan::None => {}
                        super::delegate::RecallPlan::Wait(wait) => {
                            self.inbox.recall_waiters.insert(batch.node, now.wait);
                            self.park(
                                now,
                                (wait, None),
                                None,
                                super::readindex::ParkedWhat::InboxRepoll { node: batch.node },
                            );
                            return Err(Halt::Recall);
                        }
                        // Unreachable in the takeover gate (the `!admitted`
                        // halt above recalls on demand, ADR-44); kept for
                        // the plan's exhaustiveness.
                        super::delegate::RecallPlan::Gated => {
                            return Err(Halt::Recall);
                        }
                        super::delegate::RecallPlan::Refuse(code)
                            if self.inbox_halts_touched(
                                batch.node,
                                code,
                                op_touched_gen,
                                &keys,
                                replica,
                            ) =>
                        {
                            return Err(Halt::Recall);
                        }
                        super::delegate::RecallPlan::Refuse(code) => {
                            // Phase 2b: a designation is involved.
                            replica.journal_inbox_refusal(rid, code, ack, decoded.as_ref().ok())?;
                            self.note_final_nonholder_refusal(rid, code, true);
                            self.stats.inbox_refused_ops += 1;
                            continue;
                        }
                    }
                }
            }
            // Plan 43 C2 (§5.3 item 5): a settle of the rid, which has no
            // outcome here (above): refused for good, like any refusal of an
            // inbox op (its requester learns it from the log).
            if op.settle {
                // Never `Lone`, even for this node's own batch (its takeover
                // gate drains it): `Core::settle_refusal`.
                let _settle_class = self.journal_class_for(super::Asker::Remote);
                replica.journal_inbox_refusal(
                    rid,
                    super::client::settle_code(rid, decoded.as_ref().ok()),
                    ack,
                    decoded.as_ref().ok(),
                )?;
                self.recall_unblock(now, &touched);
                self.stats.settles_refused += 1;
                self.stats.inbox_refused_ops += 1;
                continue;
            }
            let executed = match &decoded {
                Ok(decoded_op) => constellation_meta::locks::TokenPath::Inbox.scope(|| {
                    replica.execute_inbox(
                        ack,
                        decoded_op,
                        rid,
                        &constellation_meta::locks::LockTag::from_wire(&op.lock_tag, None),
                        now,
                    )
                }),
                Err(_) => Err(MetaError::Invalid("undecodable inbox op".into())),
            };
            self.recall_unblock(now, &touched);
            match executed {
                Ok(records) => {
                    replica.remember_outcome(rid, &records);
                    if rid.node != self.cfg.node_id {
                        replica.note_foreign_executed(&records);
                    }
                    if admitted {
                        self.lease.touch(now);
                    }
                    if admitted && self.cfg.placement && self.cfg.delegation {
                        let keys = constellation_meta::TouchSet::from_records(records.iter());
                        let dirs = Core::dirs_of_keys(&keys, replica);
                        self.place_note(batch.node, dirs);
                    }
                    self.stats.inbox_executed_ops += 1;
                }
                Err(MetaError::Conflict) => {
                    // A stale manifest base: `ESTALE` on the log, and the
                    // requester rebases from its own replica.
                    replica.journal_inbox_refusal(rid, Code::Stale, ack, decoded.as_ref().ok())?;
                    self.stats.inbox_refused_ops += 1;
                }
                // Plan 30 §M14 phase 2: the op's lock grant ended. Journaled
                // as a P2P refusal is (`LAPSED_CODE`): the requester learns
                // inbox outcomes from the log, and the position must be
                // answered for the drain to move on. Nothing else ran. Not
                // `EIO`: that is a dropped op's code (`DROPPED_CODE`), and
                // read back as one (`completed_source`) the lone root's own
                // op drained in its takeover gate was answered as another
                // node's, claiming the archive for its singleton row.
                Err(error) => {
                    let code = if matches!(error, MetaError::LockLapsed) {
                        self.stats.lock_lapsed_refusals += 1;
                        super::client::LAPSED_CODE
                    } else {
                        error.code()
                    };
                    replica.journal_inbox_refusal(rid, code, ack, decoded.as_ref().ok())?;
                    self.stats.inbox_refused_ops += 1;
                }
            }
        }
        self.inbox.recall_waiters.remove(&batch.node);
        Ok(replica.journal_next_seq()?.saturating_sub(1))
    }

    /// Whether an inbox op still waits to run after a recall it caused
    /// (see `InboxState::recall_waiters`); a waiter older than the
    /// requester's own in-doubt deadline no longer counts (it has taken
    /// the lease path, or is gone).
    pub(crate) fn inbox_holds_redelegation(&mut self, now: Now) -> bool {
        let horizon = self.cfg.inbox_deadline_ms as i64;
        self.inbox
            .recall_waiters
            .retain(|_, since| now.wait.since(*since) < horizon);
        !self.inbox.recall_waiters.is_empty()
    }

    /// Inside the takeover gate, after this node's own stranded ops were
    /// replayed and before its view opens: execute every batch of every
    /// epoch below the new one (dedup and watermark make re-reads exact)
    /// and delete them.
    ///
    /// A batch with an op under a live delegation cannot run inside the
    /// gate (the recall runs first). It and the same requester's later
    /// batches are left for the holder tick once the view is open
    /// (`inbox_run_leftovers`), which lists and reads them again from
    /// storage (ADR-47): the holder never polls an older epoch, so a batch
    /// left in the bucket would otherwise wait for the next takeover (a
    /// crashed requester's forever, then executed long after its
    /// incarnation ended). Every other requester's batches still drain.
    pub(crate) fn inbox_drain(
        &mut self,
        now: Now,
        batches: Vec<InboxBatch>,
        read_at: WaitMs,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Result<(), MetaError> {
        let mut ops = 0u64;
        let mut drained = 0u64;
        let mut stopped: std::collections::BTreeSet<NodeId> = std::collections::BTreeSet::new();
        let mut keys = Vec::new();
        for batch in batches {
            if stopped.contains(&batch.node) {
                keys.push(batch.key());
                continue;
            }
            match self.execute_inbox_batch(now, &batch, read_at, false, replica, out) {
                Ok(_) => {}
                Err(Halt::Fenced) => unreachable!("no view admission inside the gate"),
                Err(Halt::Stale) => {
                    // The drain's read is too old to run from: this
                    // requester's batches (and every later one of the
                    // round) go to the re-read, which lists them again.
                    self.stats.inbox_stale_relists += 1;
                    stopped.insert(batch.node);
                    keys.push(batch.key());
                    continue;
                }
                Err(Halt::Recall) => {
                    // Phase 2b: an op under an inherited delegation: the
                    // recall runs first; this requester's batches wait
                    // for the holder tick after the gate.
                    tracing::info!(
                        node = self.cfg.node_id,
                        requester = batch.node,
                        epoch = batch.epoch,
                        n = batch.n,
                        "inbox: drain defers a requester at a delegated subtree; \
                         the recall runs first"
                    );
                    stopped.insert(batch.node);
                    keys.push(batch.key());
                    continue;
                }
                Err(Halt::Meta(error)) => return Err(error),
            }
            ops += batch.ops.len() as u64;
            drained += 1;
            let key = batch.key();
            self.issue_s3(S3Op::InboxDelete { key }, S3For::InboxGc(key), out);
        }
        if drained > 0 || !keys.is_empty() {
            tracing::info!(
                node = self.cfg.node_id,
                batches = drained,
                ops,
                deferred = keys.len(),
                "inbox: drained older epochs' batches inside the takeover gate"
            );
        }
        self.stats.inbox_drained_batches += drained;
        self.stats.inbox_drained_ops += ops;
        // The poll state, if any, belongs to the previous tenure.
        self.inbox.holder = None;
        let mut waiting: BTreeMap<NodeId, LeftoverNode> = BTreeMap::new();
        for key in keys {
            waiting
                .entry(key.node)
                .or_insert_with(|| LeftoverNode {
                    keys: Vec::new(),
                    read: None,
                    next_read_at: now.wait,
                })
                .keys
                .push(key);
        }
        self.inbox.leftover = (!waiting.is_empty()).then_some(Leftover {
            waiting,
            since: now.wait,
        });
        Ok(())
    }

    /// ADR-47: the takeover drain's leftovers, from the holder tick: per
    /// requester that still waits, read its batches again from storage
    /// (`InboxRun` from its first remembered key, at the poll width: a
    /// batch its requester withdrew since is gone, and nothing else is
    /// listed or read) and run what is read through the admitted path (the
    /// recalls a delegated op needs are started and waited for there; see
    /// `on_inbox_leftover_read`). Returns the requesters that still have
    /// leftovers: they are not polled under the new epoch until these ran.
    fn inbox_run_leftovers(
        &mut self,
        now: Now,
        out: &mut Vec<Action>,
    ) -> std::collections::BTreeSet<NodeId> {
        let width = self.cfg.inbox_poll_width.max(1);
        let Some(l) = self.inbox.leftover.as_ref() else {
            return Default::default();
        };
        let due: Vec<(NodeId, InboxKey)> = l
            .waiting
            .iter()
            .filter(|(_, n)| n.read.is_none() && now.wait >= n.next_read_at)
            .filter_map(|(node, n)| n.keys.first().map(|k| (*node, *k)))
            .collect();
        let nodes = l.waiting.keys().copied().collect();
        for (node, first) in due {
            let op = self.issue_s3(
                S3Op::InboxRun {
                    epoch: first.epoch,
                    node,
                    from: first.n,
                    width,
                },
                S3For::InboxLeftover(node),
                out,
            );
            if let Some(n) = self
                .inbox
                .leftover
                .as_mut()
                .and_then(|l| l.waiting.get_mut(&node))
            {
                n.read = Some((op, now.wait));
            }
        }
        nodes
    }

    /// The re-read of one requester's leftover batches came back: run them
    /// in order. A batch that is gone (withdrawn, deleted) or ran is done;
    /// what halts again stays for the next tick's re-read. A requester
    /// whose batches are all done is released.
    pub(crate) fn on_inbox_leftover_read(
        &mut self,
        now: Now,
        node: NodeId,
        op: OpId,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let hold = self.cfg.recall_hold_ms.max(50);
        let me = self.cfg.node_id;
        let Some(n) = self
            .inbox
            .leftover
            .as_mut()
            .and_then(|l| l.waiting.get_mut(&node))
        else {
            return;
        };
        let Some(read_at) = n.read.filter(|(id, _)| *id == op).map(|(_, at)| at) else {
            return;
        };
        n.read = None;
        n.next_read_at = now.wait.plus(hold);
        let run = match result {
            S3Result::InboxRun(Ok(run)) => run,
            other => {
                tracing::warn!(
                    node = me,
                    requester = node,
                    ?other,
                    "inbox: re-reading the takeover drain's leftovers failed; retried"
                );
                return;
            }
        };
        let mut executed = false;
        if run.is_empty() {
            // Its first remembered batch is gone: the requester withdrew it
            // or another drain removed it.
            n.keys.remove(0);
            n.next_read_at = now.wait;
        }
        for batch in run {
            match self.execute_inbox_batch(now, &batch, read_at, true, replica, out) {
                Ok(_) => {
                    executed = true;
                    self.stats.inbox_drained_batches += 1;
                    self.stats.inbox_drained_ops += batch.ops.len() as u64;
                    let key = batch.key();
                    if let Some(n) = self
                        .inbox
                        .leftover
                        .as_mut()
                        .and_then(|l| l.waiting.get_mut(&node))
                    {
                        n.keys.retain(|k| *k != key);
                        n.next_read_at = now.wait;
                    }
                    self.issue_s3(S3Op::InboxDelete { key }, S3For::InboxGc(key), out);
                }
                Err(Halt::Fenced) | Err(Halt::Recall) => break,
                Err(Halt::Stale) => {
                    // Read again at once (the keys are still remembered).
                    self.stats.inbox_stale_relists += 1;
                    if let Some(n) = self
                        .inbox
                        .leftover
                        .as_mut()
                        .and_then(|l| l.waiting.get_mut(&node))
                    {
                        n.next_read_at = now.wait;
                    }
                    break;
                }
                Err(Halt::Meta(error)) => {
                    tracing::warn!(
                        node = self.cfg.node_id,
                        %error,
                        "executing a leftover inbox batch"
                    );
                    break;
                }
            }
        }
        if let Some(l) = self.inbox.leftover.as_mut() {
            l.waiting.retain(|_, n| !n.keys.is_empty());
            if l.waiting.is_empty() {
                self.inbox.leftover = None;
            }
        }
        if executed {
            self.nudge(now, out);
        }
    }

    /// ADR-47 (the drain's halt, rule 3): whether an inbox op the drain
    /// would refuse (`code`: a non-stealable grant it does not reach) is
    /// held instead, its batch kept and polled again (it runs once the
    /// grant ends, with dedup). `EROFS` of an op
    /// - whose requester recorded a generation at enqueue (`touched_gen`,
    ///   `UNKNOWN` included): it may have run there; or
    /// - whose keys are a non-stealable grant of its own requester: that node
    ///   may run the op locally at any moment (a batch it withdrew may still
    ///   be in a read in flight), so the grant's holder is not refused.
    ///
    /// `EXDEV` stays a journaled refusal. A rid that was ever in an inbox is
    /// never run by a designee otherwise (the inbox rule: the designee
    /// fences it, the root does not redirect it), so these are the only
    /// ways a drain's refusal could contradict an execution.
    fn inbox_halts_touched(
        &mut self,
        requester: NodeId,
        code: Code,
        touched_gen: u64,
        keys: &constellation_meta::TouchSet,
        replica: &dyn Replica,
    ) -> bool {
        if self.cfg.root_ignores_touched || code != Code::ReadOnly {
            return false;
        }
        if touched_gen != 0 {
            self.stats.inbox_touched_halts += 1;
            return true;
        }
        let own_grant = matches!(
            replica.resolve_ownership(keys),
            constellation_meta::delegation::Ownership::Delegated(d) if d.node == requester
        );
        if own_grant {
            self.stats.inbox_own_grant_halts += 1;
        }
        own_grant
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Config, Core};
    use crate::ids::Now;

    /// Single-writer mode: a requester whose inbox demand is sustained
    /// does not escalate — it could never claim the holder's lease
    /// (`Config::single_writer`), and asking made the holder release it
    /// to nobody.
    #[test]
    fn a_single_writer_requester_never_asks_for_the_lease() {
        for single_writer in [false, true] {
            let mut cfg = Config::defaults(2, 1);
            cfg.single_writer = single_writer;
            let mut core = Core::new(cfg);
            let t0 = Now::same(1_000_000);
            let ops = core.cfg.escalate_ops;
            for i in 0..ops {
                core.note_inbox_op(t0.plus(i * 100), 2_000);
            }
            assert_eq!(
                core.escalated(t0.plus(ops * 100)),
                !single_writer,
                "sustained inbox demand (single writer: {single_writer})"
            );
        }
    }
}
