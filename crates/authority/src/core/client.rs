//! A local client op from `Event::Submit` to `Action::Reply`: the local
//! fast path, the forward with its same-rid retries and one redirect,
//! M13's inbox when there is no P2P path, the lease path with its in-doubt
//! resolution, the read-your-refusal wait and the deadline (what
//! `fusefs::mutate_op_rebasable` + `node_runtime::dispatch_forward` +
//! `forward::request_mutate_with` + `inbox::forward_via_inbox` did).
//!
//! A stranded op's replay (`recovery::drain_pending_replays`) rides the
//! same machine under `Origin::Replay`: it reaches the holder the same way
//! and gets the same retries, but its outcome goes to the replay drain,
//! never to a client.

use super::{Core, S3For, Timer};
use crate::action::{Action, ClientReply, S3Op};
use crate::event::{PeerMsg, Policy, S3Result};
use crate::ids::{Epoch, Ms, NodeId, OpId};
use crate::replica::Replica;
use constellation_fs_core::Ino;
use constellation_meta::delegation::Ownership;
use constellation_meta::{
    CompletedOutcome, KeySet, MetaError, MutateOp, MutateOutcome, Position, Rid,
};
use std::collections::BTreeSet;

/// How many `InDoubt` rids the core remembers for their resubmission.
const MAX_IN_DOUBT_RIDS: usize = 10_000;

/// Who submitted the op and therefore where its outcome goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Origin {
    Client,
    /// A queued stranded op (`pending_replay` row `queue_seq`).
    Replay {
        queue_seq: u64,
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
    InboxWithdraw { holder: NodeId },
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
    pub keys: Vec<Ino>,
    pub attempts: u32,
    /// `NotHolder` redirects followed (plan 30 §M11: two, since a
    /// delegate's names the root and the root's a delegate).
    pub redirected: u32,
    /// Sent to a holder at least once (or into an inbox): it may have
    /// taken effect there, so every later local execution first checks
    /// `completed`.
    pub forwarded: bool,
    pub acquire_retries: u32,
    pub deadline: crate::ids::TimerId,
    pub timer: Option<crate::ids::TimerId>,
    /// M13: when the op entered the inbox path (its in-doubt deadline
    /// counts from here) and when its batch became durable.
    pub inbox_since: Option<Ms>,
    pub inbox_durable_at: Option<Ms>,
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
}

impl ClientOp {
    pub fn phase_kind(&self) -> ClientPhase {
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

/// `forward::meta_errno`.
pub fn meta_errno(e: &MetaError) -> i32 {
    use MetaError::*;
    match e {
        NoEnt(_) | NoEntry => libc::ENOENT,
        Exists => libc::EEXIST,
        NotDir => libc::ENOTDIR,
        IsDir => libc::EISDIR,
        NotEmpty => libc::ENOTEMPTY,
        NoData => libc::ENODATA,
        Invalid(_) => libc::EINVAL,
        Conflict => libc::EAGAIN,
        Fjall(_) | Io(_) | Record(_) | Key(_) | Json(_) | Postcard(_) => libc::EIO,
    }
}

/// `forward::conflict_keys`: the inodes `op` reads or writes, for the
/// ordering gate. A name that does not resolve locally is left out; every
/// case includes the parent, so two ops racing on an unresolved child
/// still serialize on it.
pub(crate) fn conflict_keys(op: &MutateOp, replica: &dyn Replica) -> Vec<Ino> {
    let lookup = |parent: Ino, name: &str| replica.lookup_ino(parent, name);
    let mut keys = match op {
        MutateOp::Mkdir { parent, .. }
        | MutateOp::Create { parent, .. }
        | MutateOp::Symlink { parent, .. }
        | MutateOp::Mknod { parent, .. } => vec![*parent],
        MutateOp::Link { ino, parent, .. } => vec![*parent, *ino],
        MutateOp::Unlink { parent, name } | MutateOp::Rmdir { parent, name } => {
            let mut k = vec![*parent];
            k.extend(lookup(*parent, name));
            k
        }
        MutateOp::Rename {
            parent,
            name,
            new_parent,
            new_name,
        } => {
            let mut k = vec![*parent, *new_parent];
            k.extend(lookup(*parent, name));
            k.extend(lookup(*new_parent, new_name));
            k
        }
        MutateOp::Setattr { ino, .. }
        | MutateOp::SetManifest { ino, .. }
        | MutateOp::SetXattr { ino, .. }
        | MutateOp::RemoveXattr { ino, .. } => vec![*ino],
        MutateOp::Publish { ino, parent, .. } => vec![*parent, *ino],
        MutateOp::AtimeBatch { entries } => entries.iter().map(|(i, _, _)| *i).collect(),
        MutateOp::Records { .. } => Vec::new(),
    };
    keys.sort_unstable();
    keys.dedup();
    keys
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
        CompletedOutcome::Refused { errno } if errno == libc::ESTALE => {
            Some(MutateOutcome::Conflict { manifest: None })
        }
        CompletedOutcome::Refused { errno } => Some(MutateOutcome::Errno(errno)),
    }
}

impl Core {
    // ---- entry ----

    pub(crate) fn on_submit(
        &mut self,
        now: Ms,
        rid: Rid,
        op: MutateOp,
        policy: Policy,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.submit(now, rid, op, policy, Origin::Client, replica, out);
    }

    /// Plan 30 §M9 (`Control::InDoubt`): the next `submit` of `rid`
    /// starts in doubt (see `in_doubt_rids`).
    pub(crate) fn note_in_doubt(&mut self, rid: Rid) {
        self.in_doubt_rids.insert(rid);
        while self.in_doubt_rids.len() > MAX_IN_DOUBT_RIDS {
            let first = *self.in_doubt_rids.iter().next().expect("non-empty");
            self.in_doubt_rids.remove(&first);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn submit(
        &mut self,
        now: Ms,
        rid: Rid,
        op: MutateOp,
        policy: Policy,
        origin: Origin,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.clients.contains_key(&rid) {
            tracing::warn!(node = self.cfg.node_id, ?rid, "duplicate submit ignored");
            return;
        }
        let deadline = self.set_timer(
            now.plus(self.cfg.acquire_deadline_ms),
            Timer::ClientDeadline(rid),
            out,
        );
        let keys = conflict_keys(&op, replica);
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
        // A rid this core already answered `InDoubt` may have taken
        // effect somewhere: its resubmission is in doubt from the start.
        // So is a stranded op's replay (`Origin::Replay`): its first
        // execution may be in the log already, and the takeover gate
        // replays the queue locally while a drain-submitted copy of the
        // same op can be waiting for that very acquisition (M5 round 5,
        // long seed 11932: the gate's replay journaled it, then
        // `on_acquire_finished` executed the waiting copy a second time).
        let in_doubt = self.in_doubt_rids.remove(&rid) || matches!(origin, Origin::Replay { .. });
        let inbox_keys = self.in_doubt_batches.remove(&rid).unwrap_or_default();
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
                forwarded: in_doubt,
                acquire_retries: 0,
                deadline,
                timer: None,
                inbox_since: None,
                inbox_durable_at: None,
                inbox_keys,
                deps,
            },
        );
        // Plan 30 §M11: a delegate executes its own subtree here; an op
        // under another node's delegation goes to that delegate (or to
        // the root, which recalls, when the observed stream table is
        // full or the delegate is unreachable).
        if self.cfg.delegation {
            let op_ref = self
                .clients
                .get(&rid)
                .map(|c| c.op.clone())
                .expect("present");
            if self.delegate_try_execute(now, 0, None, rid, &op_ref, deps, 0, replica, out) {
                return;
            }
            if self.cfg.forwarding && self.cfg.p2p {
                if let Ownership::Delegated(d) =
                    replica.resolve_ownership(&super::holder::keys_of_op(&op_ref))
                {
                    if d.node != self.cfg.node_id {
                        if deps.streams.is_full() && deps.streams.get(d.gen).is_none() {
                            self.stats.deps_overflow_to_root += 1;
                        } else if self.reaches(now, d.node) {
                            self.stats.deleg_forwarded += 1;
                            self.send_forward(now, rid, d.node, out);
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
            self.finish_in_doubt(now, rid, replica, out);
            return;
        }
        if self.gated(rid) {
            self.clients.get_mut(&rid).expect("present").phase = Phase::Gated;
            return;
        }
        self.route(now, rid, replica, out);
    }

    /// Whether an earlier op of this node touching a common inode is
    /// still in flight.
    fn gated(&self, rid: Rid) -> bool {
        let Some(me) = self.clients.get(&rid) else {
            return false;
        };
        self.clients.iter().any(|(other, c)| {
            *other != rid
                && c.order < me.order
                && !matches!(
                    c.phase,
                    Phase::InboxQueued { .. } | Phase::InboxWaiting { .. }
                )
                && c.keys.iter().any(|k| me.keys.contains(k))
        })
    }

    /// An op finished: release the gated ops it was holding back, in
    /// submission order.
    fn release_gated(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
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

    /// Not the holder: forward if a holder is known and reachable, learn
    /// it if not, submit through its inbox when there is no P2P path, or
    /// take the lease path when forwarding is off.
    pub(crate) fn route(
        &mut self,
        now: Ms,
        rid: Rid,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(policy) = self.clients.get(&rid).map(|c| c.policy) else {
            return;
        };
        if !self.cfg.forwarding {
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
            Some(holder) if self.reaches(now, holder) => self.send_forward(now, rid, holder, out),
            // A cache naming this node is not trustworthy on its own
            // (nothing invalidates it when the lease is lost); re-read.
            // An unreachable holder needs the lease object too: the
            // inbox is addressed by its epoch.
            _ => {
                if policy == Policy::BestEffort {
                    self.finish_in_doubt(now, rid, replica, out);
                    return;
                }
                if let Some(c) = self.clients.get_mut(&rid) {
                    c.phase = Phase::LearnHolder;
                }
                self.issue_s3(S3Op::LeaseGet, S3For::LearnHolder(rid), out);
            }
        }
    }

    pub(crate) fn on_holder_learned(
        &mut self,
        now: Ms,
        rid: Rid,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.clients.contains_key(&rid) {
            return;
        }
        let object = match result {
            S3Result::LeaseGet(Ok(Some((lease, _)))) => {
                self.lease.note_object(now, &lease);
                Some(lease)
            }
            _ => None,
        };
        let live = object
            .as_ref()
            .filter(|lease| lease.holder != 0 && !lease.is_claimable(now.0));
        match live {
            Some(lease) if lease.holder != self.cfg.node_id => {
                let holder = lease.holder;
                let epoch = lease.epoch;
                if self.reaches(now, holder) {
                    self.send_forward(now, rid, holder, out);
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
        now: Ms,
        rid: Rid,
        holder: NodeId,
        out: &mut Vec<Action>,
    ) {
        let req = self.op_id();
        let timer = self.set_timer(
            now.plus(self.cfg.forward_timeout_ms),
            Timer::ForwardTimeout(rid),
            out,
        );
        let acked_through = self.acked.floor();
        let Some(c) = self.clients.get_mut(&rid) else {
            return;
        };
        if let Some(&key) = c.inbox_keys.first() {
            // Withdraw the durable batches (one at a time, every one the
            // rid was submitted in) before the holder can see the op
            // twice: a drain that read one later would find no
            // `completed` witness for a P2P refusal and execute it again.
            if c.inbox_keys.len() > 1 && !matches!(c.phase, Phase::InboxWithdraw { .. }) {
                self.stats.inbox_multi_batch_withdrawals += 1;
            }
            c.phase = Phase::InboxWithdraw { holder };
            self.cancel_timer(timer, out);
            self.inbox.pending.remove(&rid);
            self.issue_s3(
                S3Op::InboxDelete { key },
                super::S3For::InboxWithdraw(rid),
                out,
            );
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
            },
        });
    }

    // ---- the forward's outcomes ----

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_mutate_reply(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        outcome: MutateOutcome,
        (base, position, gen): (Option<crate::ids::Seq>, Position, u64),
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
        self.stats.forwards_ok += 1;
        self.note_p2p_result(now, from, true);
        // Plan 30 §M11: whatever the reply installs here, this client's
        // next write orders after the answering sequencer's position.
        replica.note_frontier(&position);
        if gen == 0 {
            // Plan 30 §M11: a delegate's reply says nothing about the
            // lease; only the root's names the holder.
            self.lease.cached_holder = Some(from);
        }
        let applied = replica.applied_seq().unwrap_or(0);
        let base_ok = self.cfg.speculate_on_stale_base || base.is_some_and(|b| applied >= b);
        tracing::debug!(
            node = self.cfg.node_id,
            ?rid,
            ?base,
            ?position,
            applied,
            base_ok,
            ?outcome,
            "forward reply"
        );
        match outcome {
            MutateOutcome::Accepted { epoch, records } if !base_ok => {
                // The holder evaluated the op against state this replica
                // has not applied: installing the records now would put
                // them ahead of records they follow. The log delivers
                // them in order; answer when it has.
                if replica.completed_position(rid).ok().flatten().is_some() {
                    // Delivered by the log already; the rest of what the
                    // holder evaluated against normally came with it.
                    self.observe(rid, position, replica);
                    self.finish(
                        now,
                        rid,
                        MutateOutcome::Accepted { epoch, records },
                        replica,
                        out,
                    );
                    return;
                }
                self.stats.awaited_log += 1;
                let c = self.clients.get_mut(&rid).expect("present");
                c.phase = Phase::AwaitingLog { epoch, position };
                self.nudge(now, out);
            }
            MutateOutcome::Accepted { epoch, records } => {
                let Some(op) = self.clients.get(&rid).map(|c| c.op.clone()) else {
                    return;
                };
                match replica.install_shadow(rid, epoch, gen, &op, &records) {
                    Ok(true) => {
                        tracing::debug!(node = self.cfg.node_id, ?rid, epoch, "shadow installed");
                        self.stats.shadows_installed += 1;
                        // Plan 30 §M6 (coordinator decision 2): installed
                        // effects do not raise `observed`; the shadow
                        // covers its keys (the parent directory's
                        // attributes included) up to the reply's position.
                        replica.note_covering(KeySet::from_records(&records), position);
                        self.finish(
                            now,
                            rid,
                            MutateOutcome::Accepted { epoch, records },
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
                            self.observe(rid, position, replica);
                            self.finish(
                                now,
                                rid,
                                MutateOutcome::Accepted { epoch, records },
                                replica,
                                out,
                            );
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, node = self.cfg.node_id, ?rid, "failed to install shadow");
                        self.finish(now, rid, MutateOutcome::Errno(libc::EIO), replica, out);
                    }
                }
            }
            MutateOutcome::Busy => self.retry_or_lease(now, rid, replica, out),
            MutateOutcome::Held { retry_ms } => {
                // Plan 30 §M8: executed; the holder is recalling read
                // delegations before it acknowledges. Ask again (same
                // rid, no attempt spent) — the holder answers from its
                // dedup, or holds the retry until the recalls are done.
                self.stats.held_retries += 1;
                let timer = self.set_timer(now.plus(retry_ms), Timer::ForwardBackoff(rid), out);
                if let Some(c) = self.clients.get_mut(&rid) {
                    c.phase = Phase::Backoff;
                    c.timer = Some(timer);
                }
            }
            MutateOutcome::NotHolder { holder } => {
                // Plan 30 §M11: the root's `NotHolder` names a delegate
                // (the op is wholly the delegate's); that is a route, not
                // the lease holder, so the cache keeps the root.
                let names_delegate =
                    holder != 0 && replica.delegation_table().iter().any(|e| e.node == holder);
                if holder != 0 && !names_delegate {
                    self.lease.cached_holder = Some(holder);
                }
                let c = self.clients.get_mut(&rid).expect("present");
                if holder != 0 && holder != from && holder != self.cfg.node_id && c.redirected < 2 {
                    c.redirected += 1;
                    self.stats.forward_redirects += 1;
                    self.send_forward(now, rid, holder, out);
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
                    match replica.install_hint(records, floor, epoch, gen) {
                        Ok(()) => {
                            self.stats.hints_installed += 1;
                            replica.note_covering(KeySet::from_records(records), position);
                            hinted = true;
                        }
                        Err(error) => tracing::warn!(%error, "failed to install hint"),
                    }
                }
                if !hinted {
                    self.observe(rid, position, replica);
                }
                self.finish(now, rid, outcome, replica, out);
            }
            outcome @ (MutateOutcome::Errno(_) | MutateOutcome::Conflict { .. }) => {
                // Plan 30 §M6: a refusal observed the holder's state
                // without installing anything here (this replaces plan 29
                // M6's per-name causal wait: every later read waits).
                self.observe(rid, position, replica);
                self.finish(now, rid, outcome, replica, out)
            }
        }
    }

    pub(crate) fn on_forward_timeout(
        &mut self,
        now: Ms,
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
        now: Ms,
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
        now: Ms,
        rid: Rid,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(c) = self.clients.get_mut(&rid) else {
            return;
        };
        match c.policy {
            Policy::BestEffort => {
                self.finish_in_doubt(now, rid, replica, out);
            }
            Policy::Client if c.attempts < self.cfg.forward_retries => {
                c.attempts += 1;
                self.stats.forward_retries += 1;
                let delay = self.cfg.forward_backoff_ms * u64::from(c.attempts);
                c.phase = Phase::Backoff;
                let timer = self.set_timer(now.plus(delay), Timer::ForwardBackoff(rid), out);
                self.clients.get_mut(&rid).expect("present").timer = Some(timer);
            }
            _ => self.lease_path(now, rid, replica, out),
        }
    }

    pub(crate) fn on_forward_backoff(
        &mut self,
        now: Ms,
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
        // The view may have opened meanwhile (another op's acquisition
        // landed the lease here, tailed to head). The op was forwarded,
        // so it is in doubt: the coverage rule applies before executing.
        if let Some(epoch) = self.lease.new_mutation_epoch(now, &self.cfg) {
            let outcome = self.resolve_in_doubt_then_execute(now, rid, epoch, replica, out);
            self.finish(now, rid, outcome, replica, out);
            return;
        }
        let cached = self.lease.cached_holder.filter(|h| *h != self.cfg.node_id);
        match cached {
            Some(holder) if self.reaches(now, holder) => self.send_forward(now, rid, holder, out),
            _ => self.route(now, rid, replica, out),
        }
    }

    // ---- the lease path ----

    /// `mutate_op_rebasable`'s `Busy | NotHolder` arm: ask for the lease
    /// and, once held, execute here — unless the rid already completed.
    pub(crate) fn lease_path(
        &mut self,
        now: Ms,
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
            let deadline = self.set_timer(
                now.plus(self.cfg.acquire_deadline_ms),
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
        now: Ms,
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
                self.finish_in_doubt(now, rid, replica, out);
                continue;
            }
            self.schedule_acquire_retry(now, rid, out);
        }
    }

    /// Answer every op on the lease path with `errno` (a frozen
    /// continuation epoch).
    pub(crate) fn refuse_waiting_for_lease(
        &mut self,
        now: Ms,
        errno: i32,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let waiting: Vec<Rid> = self
            .clients
            .iter()
            .filter(|(_, c)| matches!(c.phase, Phase::WaitingLease | Phase::AcquireRetry))
            .map(|(rid, _)| *rid)
            .collect();
        for rid in waiting {
            self.finish(now, rid, MutateOutcome::Errno(errno), replica, out);
        }
    }

    fn schedule_acquire_retry(&mut self, now: Ms, rid: Rid, out: &mut Vec<Action>) {
        let Some(c) = self.clients.get_mut(&rid) else {
            return;
        };
        c.acquire_retries += 1;
        let delay = (self.cfg.acquire_retry_min_ms << c.acquire_retries.min(10))
            .min(self.cfg.acquire_retry_max_ms);
        c.phase = Phase::AcquireRetry;
        let timer = self.set_timer(now.plus(delay), Timer::AcquireRetry(rid), out);
        self.clients.get_mut(&rid).expect("present").timer = Some(timer);
    }

    pub(crate) fn on_acquire_retry(
        &mut self,
        now: Ms,
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
        // Before re-asking S3, one more look at forwarding: the holder may
        // simply have changed (the lease read told us who).
        if self.cfg.forwarding {
            if let Some(holder) = self.lease.cached_holder {
                if holder != self.cfg.node_id && self.reaches(now, holder) {
                    let c = self.clients.get_mut(&rid).expect("present");
                    c.attempts = 0;
                    c.redirected = 0;
                    self.send_forward(now, rid, holder, out);
                    return;
                }
            }
            // M13/M5: with a live holder this node cannot reach, the op
            // goes back through the holder's inbox (a lease re-read says
            // who and under which epoch) rather than spinning on
            // acquisitions until that holder lets go; the escalation
            // asks for the lease on the op's behalf meanwhile.
            if self.cfg.inbox
                && self
                    .clients
                    .get(&rid)
                    .is_some_and(|c| c.policy == Policy::Client)
            {
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
        now: Ms,
        rid: Rid,
        epoch: Epoch,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> MutateOutcome {
        let in_doubt = self
            .clients
            .get(&rid)
            .is_some_and(|c| c.forwarded || c.attempts > 0);
        if in_doubt {
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
                if let Some(need) = self.ack_need(&position) {
                    self.rd
                        .parked_local
                        .insert(rid, (Default::default(), Some(need)));
                }
                return outcome;
            }
        }
        self.execute_local(now, rid, epoch, replica, out)
    }

    /// Execute as holder, on this node's own behalf (`dispatch_forward`'s
    /// local branch).
    pub(crate) fn execute_local(
        &mut self,
        now: Ms,
        rid: Rid,
        epoch: Epoch,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> MutateOutcome {
        let Some((op, policy, deps)) = self
            .clients
            .get(&rid)
            .map(|c| (c.op.clone(), c.policy, c.deps))
        else {
            // Already finished by a nested pass (see `release_gated`);
            // the outcome goes to `finish`, which drops it.
            return MutateOutcome::Errno(libc::EIO);
        };
        // Plan 30 §M11: as the root, recall the write delegations the
        // op's keys fall under first, and wait for its `deps`; `finish`
        // skips this rid until the parked continuation executes it.
        if self.cfg.delegation {
            let keys = super::holder::keys_of_op(&op);
            let wait = if self.dl.gens.is_empty() {
                Default::default()
            } else {
                self.deleg_recall_needed(now, &keys, replica, out)
                    .unwrap_or_default()
            };
            let deps_wait = (!replica.reaches_streams(&deps)).then_some(deps);
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
                self.park_exec_local(now, wait, deps_wait, rid);
                return MutateOutcome::Busy;
            }
        }
        let outcome = match replica.execute(&op, Some(rid)) {
            Ok(records) => {
                if policy == Policy::Client {
                    self.lease.touch(now);
                }
                self.nudge(now, out);
                // Plan 30 §M8: the sequencer's own writes recall read
                // delegations too; `finish` parks the reply until done.
                let inos = constellation_meta::recall_inos(&records);
                let wait = self.recall_needed(now, &inos, None, replica, out);
                (MutateOutcome::Accepted { epoch, records }, wait)
            }
            Err(MetaError::Conflict) => (MutateOutcome::Conflict { manifest: None }, None),
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
                let errno = meta_errno(&error);
                self.record_refusal(rid, errno, replica);
                (MutateOutcome::Errno(errno), None)
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
        let durable = self.ack_need(&position);
        let (outcome, wait) = outcome;
        if wait.is_some() || durable.is_some() {
            self.rd
                .parked_local
                .insert(rid, (wait.unwrap_or_default(), durable));
        }
        outcome
    }

    // ---- finishing ----

    /// Plan 30 §M6: a client-visible reply to `rid` observed `position`
    /// without installing its effects here: raise `observed`. A replay's
    /// outcome is not client-visible and raises nothing.
    fn observe(&self, rid: Rid, position: Position, replica: &dyn Replica) {
        if self
            .clients
            .get(&rid)
            .is_some_and(|c| c.origin == Origin::Client)
        {
            replica.raise_observed(position);
        }
    }

    /// After segments were applied: answer every op waiting for the log
    /// whose completion is now here — a forward accepted on a stale base,
    /// or an inbox op whose outcome the holder shipped.
    pub(crate) fn answer_awaiting_log(
        &mut self,
        now: Ms,
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
        for (rid, epoch, position) in waiting {
            let Some(outcome) = completed_as_outcome(replica, rid, epoch) else {
                continue;
            };
            match position {
                // Plan 30 §M6: normally already dominated by the applied
                // position (the segment carrying the completion ships
                // everything before it), unless an M4 held row keeps the
                // shipped-through position below it.
                Some(position) => self.observe(rid, position, replica),
                // M13: an inbox outcome rides the log behind every row it
                // was evaluated against; nothing to raise.
                None => self.inbox_answered(now, rid),
            }
            self.finish(now, rid, outcome, replica, out);
        }
    }

    pub(crate) fn on_client_deadline(
        &mut self,
        now: Ms,
        rid: Rid,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(c) = self.clients.get(&rid) else {
            return;
        };
        if c.phase == Phase::Recalling {
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
        if c.phase == Phase::Recalling {
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
        self.stats.in_doubt += 1;
        tracing::debug!(node = self.cfg.node_id, ?rid, "op goes in doubt");
        self.finish_in_doubt(now, rid, replica, out);
    }

    /// The op is neither executed here nor answered by anyone: the client
    /// hears `InDoubt` (`EIO`; retryable by the same rid), the replay
    /// drain leaves it queued.
    pub(crate) fn finish_in_doubt(
        &mut self,
        now: Ms,
        rid: Rid,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(c) = self.clients.remove(&rid) else {
            return;
        };
        self.cancel_timer(c.deadline, out);
        if let Some(t) = c.timer {
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
                    while self.in_doubt_rids.len() > MAX_IN_DOUBT_RIDS {
                        let first = *self.in_doubt_rids.iter().next().expect("non-empty");
                        self.in_doubt_rids.remove(&first);
                        self.in_doubt_batches.remove(&first);
                    }
                    if !c.inbox_keys.is_empty() {
                        self.in_doubt_batches.insert(rid, c.inbox_keys);
                    }
                }
                out.push(Action::Reply {
                    rid,
                    reply: ClientReply::InDoubt,
                });
            }
            Origin::Replay { queue_seq } => {
                self.on_replay_outcome(now, queue_seq, rid, None, replica, out)
            }
        }
        self.release_gated(now, replica, out);
    }

    /// Deliver the op's outcome to whoever submitted it.
    pub(crate) fn finish(
        &mut self,
        now: Ms,
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
        if let Some((wait, durable)) = self.rd.parked_local.remove(&rid) {
            if self.clients.contains_key(&rid) {
                self.park_finish(now, rid, wait, durable, outcome);
                return;
            }
        }
        let Some(c) = self.clients.remove(&rid) else {
            return;
        };
        self.cancel_timer(c.deadline, out);
        if let Some(t) = c.timer {
            self.cancel_timer(t, out);
        }
        self.inbox_forget(rid);
        match c.origin {
            Origin::Client => {
                // Plan 30 §M2 GC: every completion path marks the rid
                // acked, not just the forward reply.
                if rid.node == self.cfg.node_id && rid.incarnation == self.cfg.incarnation {
                    self.acked.mark_done(rid.seq);
                }
                out.push(Action::Reply {
                    rid,
                    reply: ClientReply::Outcome(outcome),
                });
            }
            Origin::Replay { queue_seq } => {
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
