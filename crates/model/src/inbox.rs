//! Plan 30 §M13: forwarding through S3 when P2P is unavailable.
//!
//! With `AuthorityModel::inbox` on, no P2P message is ever sent. A
//! non-holder with an op writes it, with its rid, as a CAS-created
//! *batch object* `inbox/<epoch>/<node>/<n>` ([`InboxBatch`], one op per
//! batch here — see the simplifications in the crate doc) and waits for
//! the outcome to arrive through the log it tails anyway: `Completed
//! { rid }` riding with the op's records means success, a `Refused { rid,
//! errno }` entry means the holder refused it. The holder polls each
//! requester's next batch (`PollInbox`), executes it in order with the
//! same rid dedup a P2P forward gets, journals the outcome, and deletes
//! the batch once the segment carrying its outcome has shipped
//! (`GcInbox`).
//!
//! The two rules that make this exactly-once across epochs, and the two
//! naive variants the tests show to be wrong:
//!
//! - **A takeover drains every older epoch's batches inside its gate**,
//!   before the new holder's view opens, with rid dedup against the
//!   log-derived `completed` table (`AuthorityModel::inbox_drain_dedup`
//!   off is the naive variant: the drain re-executes what the old holder
//!   already shipped).
//! - **Refusals are outcomes too**: a `Refused { rid }` in the log is
//!   deduplicated exactly like a `Completed { rid }`, so a batch the old
//!   holder refused is never re-evaluated by a successor against a
//!   namespace that has since changed (`AuthorityModel::
//!   inbox_record_refusals` off is the naive variant: plan 30 §M2's
//!   "refusals are not recorded" carried over unchanged to a path where
//!   the *holder*, not the requester, decides to retry).
//!
//! A requester whose batch is overtaken by a takeover — it tails a
//! segment from a higher epoch than the one it submitted under, with no
//! outcome for its rid — treats the op as in doubt, deletes its stale
//! batch and re-submits the same rid under the new epoch
//! (`ResubmitInbox`); the drain and the re-submission race, and dedup
//! makes the winner irrelevant. In this M3a-based model a takeover's
//! first segment always carries the drain's outcomes (the drain and the
//! new holder's own op journal into one segment), so re-submission is
//! only reachable once plan 30 §M3b's takeover marker ships an empty
//! higher-epoch segment ahead of them; it is modeled now so that phase
//! is a test, not a change.
//!
//! Nothing here speculates: an inbox-submitted op installs no shadow, so
//! the requester's replica stays a log prefix (plus whatever P2P shadows
//! it has, none in inbox mode) and the plan 30 §M3a machinery is
//! untouched by this path.

use crate::namespace::{eval, Errno, NsOp, NsRet};
use crate::protocol::{
    authority, log_head, node_replica, rid_completed_record, AuthorityModel, Epoch, HistEvt,
    NodeId, Phase, Rid, Segment, State,
};

/// A batch number within one `(epoch, node)` prefix.
pub type BatchNo = u8;

/// One batch object in the bucket (`store_s3::inbox::InboxBatch`,
/// reduced to one op). `executed` is the holder's own bookkeeping for
/// GC — a successor never trusts it, and a drain treats every object it
/// lists as unknown, exactly as the real code has no such flag at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct InboxBatch {
    pub epoch: Epoch,
    pub node: NodeId,
    pub n: BatchNo,
    pub rid: Rid,
    pub op: NsOp,
    pub executed: bool,
}

/// The requester's LIST-last: the next free `n` under `(epoch, node)`.
fn next_n(s: &State, epoch: Epoch, node: NodeId) -> BatchNo {
    s.inbox()
        .iter()
        .filter(|b| b.epoch == epoch && b.node == node)
        .map(|b| b.n + 1)
        .max()
        .unwrap_or(0)
}

fn insert_sorted(s: &mut State, batch: InboxBatch) {
    let key = (batch.epoch, batch.node, batch.n);
    let inbox = s.inbox_mut();
    let at = inbox.partition_point(|b| (b.epoch, b.node, b.n) < key);
    inbox.insert(at, batch);
}

/// `InboxSubmitter::submit`: CAS-create the next batch under the epoch
/// the lease register currently shows, and wait for the outcome in the
/// log. Used both for a fresh op (`ClientInvoke`) and a re-submission.
pub(crate) fn submit(s: &mut State, id: NodeId, op: NsOp, rid: Rid) {
    let epoch = s.lease.epoch;
    let n = next_n(s, epoch, id);
    insert_sorted(
        s,
        InboxBatch {
            epoch,
            node: id,
            n,
            rid,
            op,
            executed: false,
        },
    );
    if let Some(cop) = &mut s.nodes[id as usize].client_op {
        cop.phase = Phase::InboxPending { epoch, n };
    }
}

/// Whether `rid` was refused as far as `id`'s applied log (and, for a
/// holder, its own unshipped refusals) can tell, and with what errno.
/// The `completed` table's other half. With `record_refusals` off (the
/// naive variant) refusals are never consulted, only carried to the
/// requester.
pub(crate) fn rid_refused(s: &State, id: NodeId, rid: Rid, record_refusals: bool) -> Option<Errno> {
    if !record_refusals {
        return None;
    }
    refused_in_applied_log(s, id, rid).or_else(|| {
        s.nodes[id as usize]
            .refusals()
            .iter()
            .find(|(r, _, _)| *r == rid)
            .map(|(_, e, _)| *e)
    })
}

/// The errno a non-fenced segment in `id`'s applied prefix refused `rid`
/// with, if any.
fn refused_in_applied_log(s: &State, id: NodeId, rid: Rid) -> Option<Errno> {
    let node = &s.nodes[id as usize];
    let mut max_epoch: Epoch = 0;
    for seq in 1..=node.applied_seq {
        if let Some(seg) = &s.log[seq as usize] {
            let fenced = seg.epoch > 0 && seg.epoch < max_epoch;
            if !fenced {
                if let Some((_, e)) = seg.refused().iter().find(|(r, _)| *r == rid) {
                    return Some(*e);
                }
                max_epoch = max_epoch.max(seg.epoch);
            }
        }
    }
    None
}

/// The outcome `id`'s applied log already holds for `rid`, if any: the
/// requester-side resolution of an in-doubt op against `completed`
/// (plan 30 §M2's coverage rule, which holds here because a requester
/// only consults this after tailing every segment below the one that
/// stranded it).
fn outcome_in_applied_log(m: &AuthorityModel, s: &State, id: NodeId, rid: Rid) -> Option<NsRet> {
    if crate::protocol::rid_in_applied_log(s, id, rid) {
        return Some(NsRet::Ok);
    }
    rid_refused(s, id, rid, m.inbox_record_refusals).map(NsRet::Err)
}

/// Execute one batch's op as holder `h` (`forward::holder_execute`
/// driven by the poller instead of a request): skipped if the rid's
/// outcome is already known (when `dedup`), otherwise evaluated against
/// the replica, journaled with its rid on success or recorded as a
/// refusal otherwise. No reply is produced; the outcome ships.
fn execute_one(m: &AuthorityModel, s: &mut State, h: NodeId, batch: InboxBatch, dedup: bool) {
    if dedup
        && (rid_completed_record(s, h, batch.rid, m.protocol).is_some()
            || rid_refused(s, h, batch.rid, m.inbox_record_refusals).is_some())
    {
        return;
    }
    let base = node_replica(s, h);
    let (ret, _) = eval(base, batch.op);
    let epoch = s.nodes[h as usize].held_epoch.unwrap_or(0);
    match ret {
        NsRet::Ok => {
            // Journaled with holder capture like any other executed op
            // (plan 30 §M3b), so a deposition rolls it back and replays it.
            m.journal_push(s, h, batch.rid, batch.op, epoch);
            s.nodes[h as usize]
                .recent
                .push((batch.rid, batch.op, epoch));
        }
        NsRet::Err(e) => s.nodes[h as usize]
            .inbox_mut()
            .refusals
            .push((batch.rid, e, epoch)),
        NsRet::Tentative | NsRet::Conflicted => unreachable!(
            "eval() never returns Tentative/Conflicted; only mark_tentative/mark_conflicted \
             rewrite an already-recorded Return to them"
        ),
    }
}

/// The batch `PollInbox(h, r)` would fetch: `r`'s next batch under the
/// epoch `h` holds, at `h`'s cursor for `r`.
fn next_batch_for(s: &State, h: NodeId, r: NodeId, epoch: Epoch) -> Option<InboxBatch> {
    let n = s.nodes[h as usize].cursor(r);
    s.inbox()
        .iter()
        .find(|b| b.epoch == epoch && b.node == r && b.n == n)
        .copied()
}

/// `InboxPoller::poll` hitting: execute the batch and advance the
/// cursor. `None` if not enabled (the caller offered it from a state
/// where it was).
pub(crate) fn poll(m: &AuthorityModel, s: &mut State, h: NodeId, r: NodeId) -> Option<()> {
    let epoch = authority(s, h, m.protocol)?;
    let batch = next_batch_for(s, h, r, epoch)?;
    execute_one(m, s, h, batch, true);
    s.nodes[h as usize].inbox_mut().cursor[r as usize] += 1;
    if let Some(b) = s
        .inbox_mut()
        .iter_mut()
        .find(|b| (b.epoch, b.node, b.n) == (batch.epoch, batch.node, batch.n))
    {
        b.executed = true;
    }
    Some(())
}

/// Whether `h`'s applied log already carries an outcome for `rid` — the
/// condition under which the holder may delete a batch it executed:
/// the segment with the outcome has shipped (and, being the holder's
/// own, is applied). Independent of the dedup knobs: GC eligibility is
/// about durability, not about what a successor will consult.
fn outcome_shipped(s: &State, h: NodeId, rid: Rid) -> bool {
    crate::protocol::rid_in_applied_log(s, h, rid) || refused_in_applied_log(s, h, rid).is_some()
}

/// The batches `h` may GC right now: executed by someone, outcome in
/// `h`'s applied log, and — for the epoch `h` holds — not the
/// requester's newest executed batch (`store_s3::inbox::gc_keep_newest`:
/// the high-water mark a restarted requester's LIST-last resumes from).
/// Batches of older epochs a drain executed or deduplicated go
/// unconditionally.
fn gc_candidates(s: &State, h: NodeId) -> Vec<InboxBatch> {
    let held = s.nodes[h as usize].held_epoch;
    s.inbox()
        .iter()
        .filter(|b| b.executed && outcome_shipped(s, h, b.rid))
        .filter(|b| {
            Some(b.epoch) != held
                || s.inbox()
                    .iter()
                    .any(|o| o.executed && o.epoch == b.epoch && o.node == b.node && o.n > b.n)
        })
        .copied()
        .collect()
}

/// `GcInbox(h)`: one unconditional DELETE per eligible batch. All at
/// once rather than one per action: the order and grouping of deletes
/// is not observable by anything else in the model, so one action per
/// batch would only multiply interleavings.
pub(crate) fn gc(m: &AuthorityModel, s: &mut State, h: NodeId) -> Option<()> {
    authority(s, h, m.protocol)?;
    let victims = gc_candidates(s, h);
    if victims.is_empty() {
        return None;
    }
    s.inbox_mut().retain(|b| !victims.contains(b));
    s.normalize_inbox();
    Some(())
}

/// The takeover drain (`InboxStore::drain_below` executed inside the
/// gate): every batch below `new_epoch`, in `(epoch, node, n)` order,
/// executed with rid dedup (`inbox_drain_dedup`; off is the naive
/// variant) and marked executed so it is GC'd once its outcome ships.
/// Cursors for the new epoch start at zero: nothing can have been
/// written under an epoch that did not exist a moment ago.
pub(crate) fn drain_below(
    m: &AuthorityModel,
    s: &mut State,
    h: NodeId,
    prev_held: Option<Epoch>,
    new_epoch: Epoch,
) {
    if prev_held != Some(new_epoch) {
        if let Some(inbox) = s.nodes[h as usize].inbox.as_mut() {
            inbox.cursor = [0; crate::protocol::MAX_NODES];
        }
        s.nodes[h as usize].normalize_inbox();
    }
    let old: Vec<InboxBatch> = s
        .inbox()
        .iter()
        .filter(|b| b.epoch < new_epoch)
        .copied()
        .collect();
    for batch in old {
        execute_one(m, s, h, batch, m.inbox_drain_dedup);
        if let Some(b) = s
            .inbox_mut()
            .iter_mut()
            .find(|b| (b.epoch, b.node, b.n) == (batch.epoch, batch.node, batch.n))
        {
            b.executed = true;
        }
    }
}

/// `ResubmitInbox(id)`: a requester whose op was stranded (a higher-epoch
/// segment arrived without its outcome) or whose lease-path attempt was
/// overtaken by another holder. First the M2 rule — if its own applied
/// log already answers the rid, that is the answer; otherwise delete the
/// stale batch (an object nobody will poll again) and submit the same
/// rid under the epoch the register now shows.
pub(crate) fn resubmit(m: &AuthorityModel, s: &mut State, id: NodeId) -> Option<()> {
    let cop = s.nodes[id as usize].client_op.clone()?;
    if !matches!(cop.phase, Phase::NeedsLease) {
        return None;
    }
    if let Some(ret) = outcome_in_applied_log(m, s, id, cop.rid) {
        s.history.push(HistEvt::Return(id, ret, cop.rid));
        s.nodes[id as usize].client_op = None;
        return Some(());
    }
    let epoch = s.lease.epoch;
    s.inbox_mut()
        .retain(|b| !(b.node == id && b.rid == cop.rid && b.epoch < epoch));
    submit(s, id, cop.op, cop.rid);
    Some(())
}

/// The requester's half of `Meta::apply_segment` for a pending inbox
/// op: the segment either carries the outcome (return it to the FUSE
/// caller — its records are applied, so read-your-write holds from here
/// on), or proves the submitting epoch over (`seg.epoch` higher, no
/// outcome), which strands the op: it is in doubt and goes back through
/// `NeedsLease`, from where it is re-submitted or resolved on the lease
/// path. Called only for a segment that was not fenced out: a fenced
/// segment's contents never took effect, outcome included.
pub(crate) fn on_tailed(s: &mut State, id: NodeId, seg: &Segment) {
    let Some(cop) = s.nodes[id as usize].client_op.clone() else {
        return;
    };
    let Phase::InboxPending { epoch, .. } = cop.phase else {
        return;
    };
    let completed = seg.records.iter().any(|(r, _)| *r == Some(cop.rid));
    let refused = seg
        .refused()
        .iter()
        .find(|(r, _)| *r == cop.rid)
        .map(|(_, e)| *e);
    let ret = if completed {
        Some(NsRet::Ok)
    } else {
        refused.map(NsRet::Err)
    };
    if let Some(ret) = ret {
        s.history.push(HistEvt::Return(id, ret, cop.rid));
        s.nodes[id as usize].client_op = None;
    } else if seg.epoch > epoch {
        s.nodes[id as usize]
            .client_op
            .as_mut()
            .expect("cloned above")
            .phase = Phase::NeedsLease;
    }
}

/// The holder-side actions: `PollInbox(h, r)` for every requester whose
/// next batch exists (a poll that would miss is a no-op and not
/// offered), and `GcInbox(h)` when something is deletable.
pub(crate) fn holder_actions(
    m: &AuthorityModel,
    s: &State,
    h: NodeId,
    actions: &mut Vec<crate::protocol::Action>,
) {
    let Some(epoch) = authority(s, h, m.protocol) else {
        return;
    };
    for r in 0..s.nodes.len() as NodeId {
        if r != h && next_batch_for(s, h, r, epoch).is_some() {
            actions.push(crate::protocol::Action::PollInbox(h, r));
        }
    }
    if !gc_candidates(s, h).is_empty() {
        actions.push(crate::protocol::Action::GcInbox(h));
    }
}

/// Whether the requester `id` with a pending inbox op may take the lease
/// path right now: the register is claimable (the holder it submitted to
/// released or expired) and, for a genuine takeover, it has tailed to
/// head — the same rule `NeedsLease` follows.
pub(crate) fn pending_may_acquire(m: &AuthorityModel, s: &State, id: NodeId) -> bool {
    match crate::protocol::claim_kind(s, id) {
        Some(needs_tail) => {
            // A takeover ships its epoch marker (plan 30 §M3b), so, like
            // the `NeedsLease` arm, it needs a free log slot.
            let marker_fits = !needs_tail || log_head(&s.log) < m.max_seq;
            (!needs_tail || s.nodes[id as usize].applied_seq == log_head(&s.log)) && marker_fits
        }
        None => false,
    }
}

/// "No rid executes twice": across the durable log (fencing applied)
/// and the unshipped journals of every node that currently has
/// authority, each rid has at most one outcome — one execution or one
/// refusal, never two of either or one of each. Journals of nodes
/// without authority are excluded: a deposed holder's journal never
/// ships under its lost epoch (M3b rolls it back and replays by rid),
/// and a dead node's is stranded by definition.
///
/// Registered for every protocol variant. `Today` violates it on bug A's
/// path (the same double execution `linearizable` catches); the dedup
/// variants keep it, and the inbox path keeps it across epochs, which is
/// what plan 30 §M13 asks this model to show.
pub(crate) fn prop_no_rid_executes_twice(m: &AuthorityModel, s: &State) -> bool {
    // A handful of rids per state: a linear scan beats a map, and this
    // runs once per explored state of every configuration.
    let mut seen: Vec<Rid> = Vec::new();
    let mut note = |rid: Rid| -> bool {
        if seen.contains(&rid) {
            return false;
        }
        seen.push(rid);
        true
    };
    let mut max_epoch: Epoch = 0;
    for seg in s.log.iter().flatten() {
        let fenced = seg.epoch > 0 && seg.epoch < max_epoch;
        if fenced {
            continue;
        }
        max_epoch = max_epoch.max(seg.epoch);
        let executed = seg.records.iter().filter_map(|(rid, _)| *rid);
        let refused = seg.refused().iter().map(|(rid, _)| *rid);
        for rid in executed.chain(refused) {
            if !note(rid) {
                return false;
            }
        }
    }
    for (i, node) in s.nodes.iter().enumerate() {
        if !node.alive || authority(s, i as NodeId, m.protocol).is_none() {
            continue;
        }
        let executed = node.journal.iter().filter_map(|e| e.rid);
        let refused = node.refusals().iter().map(|(rid, _, _)| *rid);
        for rid in executed.chain(refused) {
            if !note(rid) {
                return false;
            }
        }
    }
    true
}
