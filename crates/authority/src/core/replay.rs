//! The stranded-op replay drain (`recovery::drain_pending_replays`), the
//! local replay the takeover gate runs (`recovery::replay_locally`), and
//! the conflict-copy retries of refused replays (plan 30 §M4 round 2).

use super::client::{completed_as_outcome, Origin};
use super::Core;
use crate::action::{Action, ControlOk};
use crate::event::Policy;
use crate::ids::{Now, OpId, WaitMs};
use crate::replica::Replica;
use constellation_fs_core::Ino;
use constellation_meta::{MutateOp, MutateOutcome, Position, Rid, StrandedOp};
use constellation_types::Code;
use std::collections::BTreeMap;

/// First and largest pause between attempts at a refused replay's
/// conflict copy that could not be made yet.
const COPY_BACKOFF_MIN_MS: u64 = 250;
const COPY_BACKOFF_MAX_MS: u64 = 10_000;

#[derive(Debug, Clone, Copy)]
struct CopyRetry {
    attempts: u32,
    /// When the first attempt failed: how long the copy has been stalled.
    since: WaitMs,
    /// Not before this.
    next_at: WaitMs,
}

#[derive(Debug, Default)]
pub(crate) struct ReplayState {
    /// The queued op currently in flight through the forward path.
    pub in_flight: Option<u64>,
    /// When the head of the queue last stopped making progress.
    pub stuck_since: Option<WaitMs>,
    /// Refused replays whose conflict copy failed at least once.
    copies: BTreeMap<u64, CopyRetry>,
    /// Defect C: a drop's refused entries whose rid was settled (or asked
    /// to be) with no outcome back yet, by `queue_seq`: no sequencer
    /// reachable, or its answer not in the log here. Retried with the
    /// copies' backoff (250 ms doubling to 10 s), never once per tick.
    settles: BTreeMap<u64, CopyRetry>,
    /// The conflict copy the driver is making right now.
    copy_in_flight: Option<u64>,
    /// When a stalled copy last asked for the lease.
    copy_lease_asked_at: Option<WaitMs>,
    /// Plan 30 §M9: rids executed here whose reply was still waiting for
    /// durability when the tenure ended (`abort_durable_parks`). The
    /// recovery that follows queues their rows for replay by rid; those
    /// entries are marked unacked (`Replica::mark_replay_unacked`) — a
    /// refused replay of one is an outcome for its client, not an effect
    /// to preserve as a conflict copy.
    pub unacked: std::collections::BTreeSet<Rid>,
    /// Plan 46 M0.2 (R5): how long each placeholder (by `queue_seq`) has
    /// waited for its requester's retry, on this node's wait clock and
    /// not while the epoch refuses writes, and the tick that last counted.
    awaiting: BTreeMap<u64, u64>,
    awaiting_tick: Option<WaitMs>,
    /// Late-landing S2a (SHOULD-4): when the drain first met each foreign
    /// entry (by `queue_seq`) on this node's wait clock. Its settle waits
    /// out the requester's own attempts, or it would make the sequencer
    /// journal `Refused { ABANDONED }` for an op the remote requester's
    /// retry would still have executed.
    foreign_seen: BTreeMap<u64, WaitMs>,
}

impl ReplayState {
    /// `(pending, stalled)`: refused entries still waiting for their rid's
    /// settle, and those waiting for at least the lease-fallback interval.
    pub fn settle_counts(&self, now: Now, fallback_ms: u64) -> (u64, u64) {
        let stalled = self
            .settles
            .values()
            .filter(|c| now.wait.since(c.since) >= fallback_ms as i64)
            .count();
        (self.settles.len() as u64, stalled as u64)
    }

    /// `(pending, stalled)`: copies that failed at least once, and those
    /// failing for at least the lease-fallback interval.
    pub fn copy_counts(&self, now: Now, fallback_ms: u64) -> (u64, u64) {
        let stalled = self
            .copies
            .values()
            .filter(|c| now.wait.since(c.since) >= fallback_ms as i64)
            .count();
        (self.copies.len() as u64, stalled as u64)
    }
}

fn copy_backoff_ms(attempts: u32) -> u64 {
    COPY_BACKOFF_MIN_MS
        .saturating_mul(1u64 << attempts.saturating_sub(1).min(16))
        .min(COPY_BACKOFF_MAX_MS)
}

/// `recovery::refusal_is_satisfied`: removing a name that is already gone
/// is what the op wanted.
fn refusal_is_satisfied(op: &MutateOp, code: Code) -> bool {
    matches!(op, MutateOp::Unlink { .. } | MutateOp::Rmdir { .. }) && code == Code::NotFound
}

/// Whether `op` is a size-only `setattr` (the FUSE truncate path) with a
/// manifest commit for the same inode queued after it: the commit carries
/// the final size, so the truncate is folded into it.
fn folded_into_later_manifest(op: &StrandedOp, queue: &[StrandedOp]) -> bool {
    let MutateOp::Setattr {
        ino,
        mode: None,
        uid: None,
        gid: None,
        size: Some(_),
        atime_ns: None,
        mtime_ns: None,
    } = &op.op
    else {
        return false;
    };
    op.refused.is_none()
        && queue.iter().any(|later| {
            later.queue_seq > op.queue_seq
                // A refused manifest (a conflict copy to make, e.g. a
                // close refused for its lock grant) replaces nothing.
                && later.refused.is_none()
                && matches!(&later.op, MutateOp::SetManifest { ino: m, .. } if m == ino)
        })
}

/// What a truncate [`folded_into_later_manifest`] replays as, by its own
/// rid, in its turn ahead of the commit. Folded, its effect is the
/// commit's to carry (truncating on its own would cut whatever another
/// node wrote since), but its rid must be settled all the same — in the
/// log, by the sequencer: a copy of the truncate may be queued elsewhere
/// by the same rid (a deposed root's append of this node's delegate
/// stream row, a subscriber's streamed row), as `Records` that apply
/// unconditionally, and only an outcome for the rid keeps it from running
/// the truncate on its own after the commit (load campaign seeds 42, 61:
/// the file came back empty on every node).
///
/// With the inode's manifest from before the truncate at hand (the
/// stranded row's before-image), it replays as the base-checked commit
/// of the cut ([`StrandedOp::conditional_truncate`]): applied where the
/// file is still what it cut, or already the cut (a predecessor's
/// re-journaled backup tail carried the truncate: load campaign seed
/// 63), refused where another node wrote the file since. The commit then
/// replays on the base it was composed on. Without one (no before-image,
/// or a truncate up, which cut nothing) it replays as the no-op of its
/// rid (an empty `MutateOp::Records`, no token), which journals the rid's
/// completion and nothing else. Either way a refusal of it is no conflict
/// of its own: the commit after it decides (see `replay_locally`).
///
/// Both forms are built from `..op.clone()` on purpose: whatever else the
/// stranded row carries (its origin, source, generation, and any ordering
/// the replay is held for, such as replay deps) is the truncate's own and
/// the folded replay inherits it, so it is held and released exactly as
/// the truncate would be. Do not build the replay from defaults.
fn as_folded(op: &StrandedOp) -> StrandedOp {
    match op.conditional_truncate() {
        Some(conditional) => StrandedOp {
            op: conditional,
            ..op.clone()
        },
        None => StrandedOp {
            op: MutateOp::Records {
                records: Vec::new(),
            },
            lock_tag: Default::default(),
            ..op.clone()
        },
    }
}

impl Core {
    /// One drain tick: refused ops get their conflict copy (with backoff,
    /// never holding up the queue); the oldest unresolved op goes down
    /// the forward path (holder-local, or forwarded with M2's retries);
    /// after `replay_lease_fallback_ms` without progress this node asks
    /// for the lease itself, whose gate replays locally.
    pub(crate) fn on_drain_tick(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        self.drain_tick(now, replica, out);
        // (Whatever the tick forgot, the ops parked behind the queue look
        // again: plan 46 M0.2, R3.)
        self.wake_queue_parks(now, replica, out);
    }

    fn drain_tick(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        // A holder never tails the later-epoch segment that would strand
        // speculation from an older tenure left on it: strand it here.
        if let Some(epoch) = self.lease.ship_epoch(now, &self.cfg) {
            if replica.has_outstanding_speculation() {
                if let Ok(stranded) = replica.strand_below_epoch(epoch) {
                    self.stats.speculation_rolled_back +=
                        (stranded.shadows + stranded.hints) as u64;
                    self.stats.local_rolled_back +=
                        (stranded.locals + stranded.member_copied) as u64;
                    self.note_committed_stranded(&stranded);
                    self.lock_note_replay_pins(now, replica);
                    if stranded.any() {
                        replica.clear_streamed();
                    }
                }
            }
        }
        // Plan 46 M0.2 review (R7): a requester's shadow of a non-stealable
        // answer (a designee's or a sticky holder's) lives until the rid's outcome reaches the log; if its
        // owner's row never does, it is rolled back and replayed after the
        // bound (`drop_held_await_ms`).
        if replica.has_outstanding_speculation() {
            let cutoff = now.wall.0 - self.dl_t().stale_shadow_after as i64;
            if let Ok(stranded) = replica.strand_stale_shadows(cutoff) {
                if stranded.any() {
                    self.stats.speculation_rolled_back +=
                        (stranded.shadows + stranded.hints) as u64;
                    self.stats.stale_non_stealable_shadows += stranded.shadows as u64;
                    self.note_committed_stranded(&stranded);
                    replica.clear_streamed();
                }
            }
        }
        let queued = match replica.pending_replays() {
            Ok(q) => q,
            Err(error) => {
                tracing::warn!(%error, "reading the replay queue");
                return;
            }
        };
        self.replay
            .copies
            .retain(|seq, _| queued.iter().any(|q| q.queue_seq == *seq));
        self.replay
            .settles
            .retain(|seq, _| queued.iter().any(|q| q.queue_seq == *seq));
        if queued.is_empty() {
            self.replay.stuck_since = None;
            return;
        }
        self.account_placeholders(now, &queued);
        // The keys of the entries left behind so far (plan 46 M0.2, R2): a
        // placeholder whose requester's retry has not executed, and every
        // entry held behind one. The queue runs in order, but only on a
        // shared key: an entry on other keys commutes with them.
        let mut behind = constellation_meta::TouchSet::default();
        for head in &queued {
            if self.drop_retired_origin(head, replica) {
                continue;
            }
            // Plan 46 M0.2 (R1, R2): a placeholder (`repair drop-held`
            // rolled back another node's unanswered row) is no op to run:
            // its requester's retry is the one copy. Done once its rid has
            // an outcome here; until then it holds the later entries on its
            // keys.
            if let Some(placeholder) = &head.await_retry {
                if !self.resolve_placeholder(head, replica) {
                    absorb_keys(&mut behind, &placeholder.keys);
                    // (R4: an answer parked for the dropped row is aborted
                    // now, and its requester retries.)
                    self.abort_parked_answer_of(now, head.rid, replica, out);
                    self.settle_placeholder_if_due(now, head, replica, out);
                }
                continue;
            }
            // Behind a placeholder on a shared key: the dependents of a
            // row wait for its retry, so they run on the state it leaves
            // (not on one without it, ahead of it). An entry the log has
            // settled is not held: it is forgotten below.
            if head.refused.is_none() && overlaps_queue_keys(&behind, head, replica) {
                absorb_keys(&mut behind, &entry_keys(head, replica));
                self.stats.replays_held_for_placeholder += 1;
                continue;
            }
            // The fold: a size-only truncate with a manifest commit of the
            // inode queued after it goes out as `as_folded` (computed here,
            // from the queue, every tick: the row keeps the truncate and its
            // before-image for a gate of this node's own).
            let folded = folded_into_later_manifest(head, &queued);
            let replayed;
            let head = if folded {
                replayed = as_folded(head);
                &replayed
            } else {
                head
            };
            if let Some(refusal) = &head.refused {
                if head.dropped {
                    self.copy_after_settle(now, head, replica, out);
                } else {
                    // (A sequencer's refusal: its row is journaled where
                    // it was given.)
                    let reason = refusal.reason.clone();
                    self.try_copy(now, head, reason, replica, out);
                }
                continue;
            }
            if self.replay.in_flight.is_some() {
                return;
            }
            // Not settled by this delegate's own uncommitted row of the rid
            // (D6): its generation may end without it, and the entry is
            // then the only thing that carries the op (a restart lost the
            // replay in flight that journaled the row). The drain submits
            // it again; the dedup answers it when the row commits.
            if let Some(outcome) = completed_as_outcome(replica, head.rid, 0)
                .filter(|_| self.completed_row_settled(head.rid, replica))
            {
                self.replay.stuck_since = None;
                if folded {
                    // Settled; a refusal of it is no conflict of its own.
                    if !matches!(outcome, MutateOutcome::Accepted { .. }) {
                        self.note_unacked_refused(head);
                        self.stats.stranded_replayed += 1;
                    }
                    let _ = replica.forget_replay(head.queue_seq);
                    continue;
                }
                match outcome {
                    MutateOutcome::Accepted { .. } => {
                        let _ = replica.forget_replay(head.queue_seq);
                    }
                    // Plan 43 C2 (§5.3 item 5): the rid was settled — its
                    // requester asked for its outcome and the sequencer in
                    // force refused it for good. Nothing of it may run: no
                    // re-execution, no conflict copy (nobody heard it took
                    // effect).
                    MutateOutcome::Abandoned => {
                        self.note_unacked_refused(head);
                        self.stats.stranded_replayed += 1;
                        let _ = replica.forget_replay(head.queue_seq);
                    }
                    // Refused through an inbox (or, plan 30 §M9, by a
                    // holder that journals refusals) before this node saw
                    // it: an outcome, not a re-evaluation. An unacked
                    // entry's refusal is its client's answer.
                    MutateOutcome::Errno(code)
                        if head.foreign || refusal_is_satisfied(&head.op, code) =>
                    {
                        self.note_unacked_refused(head);
                        self.stats.stranded_replayed += 1;
                        let _ = replica.forget_replay(head.queue_seq);
                    }
                    MutateOutcome::Errno(code) => {
                        let reason = format!("refused with {code} (inbox)");
                        self.refuse_replay(now, head, reason, replica, out);
                    }
                    MutateOutcome::Conflict { .. } => {
                        let reason = "stale manifest base (inbox)".to_string();
                        self.refuse_replay(now, head, reason, replica, out);
                    }
                    MutateOutcome::LockLapsed => {
                        let reason = "the lock grant it was issued under has ended".to_string();
                        self.refuse_replay(now, head, reason, replica, out);
                    }
                    _ => {}
                }
                continue;
            }
            // Plan 30 §M11 phase 2b round 2: a shadow a delegate accepted
            // is not re-executed while its generation is live — the
            // delegate re-streams it to the (successor) root, in its
            // stream's order, or the generation ends and the void rule
            // frees it (long-delegated seed 70075: the successor's replay
            // ran ahead of the delegate's earlier transactions).
            if head.gen != 0 && replica.delegation_table().has_gen(head.gen) {
                self.stats.replays_held_for_stream += 1;
                return;
            }
            if self.clients.contains_key(&head.rid) {
                // Still in flight as a client op (its reply is what will
                // resolve it); nothing to do this tick. (Counted: an op
                // held for the replays behind this entry would wait for
                // this very drain; `hold_for_lost_deps` holds an op only
                // for the entries ahead of its own.)
                self.stats.replays_waiting_on_client += 1;
                return;
            }
            // Chunk close-stall-followup: in a continuation epoch that
            // refuses writes (frozen, or carrying no lease) nothing can
            // execute the replay: sent, it is answered in doubt and stays
            // queued (never refused: it was acknowledged), so every tick
            // resent it to the same answer, and the fallback below asked
            // for a lease the epoch cannot grant. Wait for the epoch to
            // end; the time spent waiting does not count as stuck.
            if self.epoch_refuses_writes() {
                self.replay.stuck_since = None;
                return;
            }
            // Late-landing S2a: an entry nobody acknowledged (an own row of a
            // deposed tenure whose reply never left, marked `unacked`) or
            // that another node's op left here (`foreign`) is never
            // executed by the drain. Its client may have heard `EIO`; the
            // settle finds the rid's outcome or makes the sequencer in force
            // journal `Refused { ABANDONED }`, after which every copy
            // dedups. With no sequencer reachable it stays queued. (It is
            // not stuck: it asks for no lease, whose gate would run it.) An
            // acknowledged own row is executed below, as ever (I5).
            if head.foreign || self.replay.unacked.contains(&head.rid) {
                self.settle_unexecuted_entry(now, head, replica, out);
                absorb_keys(&mut behind, &entry_keys(head, replica));
                continue;
            }
            let stuck_for = self.replay.stuck_since.get_or_insert(now.wait).to_owned();
            if now.wait.since(stuck_for) >= self.cfg.replay_lease_fallback_ms as i64
                && self.lease.ship_epoch(now, &self.cfg).is_none()
            {
                tracing::warn!(node = self.cfg.node_id, rid = ?head.rid, "replay stuck; asking for the lease");
                self.replay.stuck_since = Some(now.wait);
                // The cached holder may be the dead one that stranded the
                // op: make the next attempt read the lease object.
                self.lease.cached_holder = None;
                self.enqueue_job(
                    now,
                    super::jobs::JobReq::Acquire {
                        reason: "replay-fallback",
                        ask_handoff: self.cfg.p2p,
                    },
                    replica,
                    out,
                );
                return;
            }
            self.replay.in_flight = Some(head.queue_seq);
            self.submit(
                now,
                head.rid,
                head.op.clone(),
                Policy::Client,
                head.lock_tag.clone(),
                Origin::Replay {
                    queue_seq: head.queue_seq,
                    deps: head.deps,
                },
                replica,
                out,
            );
            return;
        }
    }

    /// A refused queue entry of `repair drop-held` (`StrandedOp::dropped`:
    /// an acknowledged op whose effect is kept aside as a conflict copy;
    /// the refusals a sequencer gave are journaled where it gave them)
    /// makes its copy only once its rid's refusal is *in the log* (plan 46 M0.2 follow-up, defect C). A node `repair drop-held`
    /// ran on after it had been deposed (suspended past its seal) rolled
    /// the rows back and journaled their `Refused` rows, which are fenced
    /// and never ship: the entry would make its copy and the acknowledged
    /// op would have no outcome in the log at all, so a requester's retry
    /// of the rid, or a successor re-shipping the original, would run it
    /// beside the copy. So:
    ///
    /// - the rid has an outcome here that is a refusal (and, when it is
    ///   this node's own journal row, committed): the copy, as before;
    /// - an outcome that accepted it: a successor completed the original
    ///   from its tail, the write survived in the log, and the entry is
    ///   forgotten with no copy;
    /// - none: the rid is settled through the sequencer in force, the way
    ///   a placeholder's is (`submit_settle`, C2g's `Core::settle_refusal`),
    ///   with the dropped code in place of the abandoned one; the answer
    ///   (`replay_outcome`) decides between the two above. Not while the
    ///   epoch refuses writes (nothing could answer), nor while the rid is
    ///   in flight.
    fn copy_after_settle(
        &mut self,
        now: Now,
        entry: &StrandedOp,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let reason = entry
            .refused
            .as_ref()
            .map(|r| r.reason.clone())
            .unwrap_or_default();
        if let Some(outcome) = completed_as_outcome(replica, entry.rid, 0)
            .filter(|_| self.completed_row_settled(entry.rid, replica))
        {
            if !self.copy_row_committed(entry.rid, replica) {
                return;
            }
            match outcome {
                MutateOutcome::Accepted { .. } => {
                    self.stats.refused_entries_survived += 1;
                    let _ = replica.forget_replay(entry.queue_seq);
                    self.wake_queue_parks(now, replica, out);
                }
                _ => self.try_copy(now, entry, reason, replica, out),
            }
            return;
        }
        if self.clients.contains_key(&entry.rid) || self.epoch_refuses_writes() {
            return;
        }
        if !self.settle_backed_off(
            now,
            entry,
            "settling the rid of a dropped row before its conflict copy; no outcome yet",
        ) {
            return;
        }
        self.stats.refused_entry_settles_sent += 1;
        self.submit_settle(
            now,
            (entry.rid, entry.queue_seq),
            super::client::dropped_settle_op(entry.rid),
            replica,
            out,
        );
    }

    /// Late-landing S2a: settle the rid of a queue entry that must not
    /// execute (see `drain_tick`).
    fn settle_unexecuted_entry(
        &mut self,
        now: Now,
        entry: &StrandedOp,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.clients.contains_key(&entry.rid) || self.epoch_refuses_writes() {
            return;
        }
        // A foreign entry's requester is another node's client: it retries
        // by rid for as long as its attempts last (`Deadlines::attempts`), and
        // that retry is the executing copy. The settle comes after.
        if entry.foreign && !self.replay.unacked.contains(&entry.rid) {
            let seen = *self
                .replay
                .foreign_seen
                .entry(entry.queue_seq)
                .or_insert(now.wait);
            if now.wait.since(seen) < self.dl_t().attempts as i64 {
                self.stats.foreign_settles_deferred += 1;
                return;
            }
        }
        if !self.settle_backed_off(
            now,
            entry,
            "settling the rid of an unacknowledged stranded row, which is never executed; no outcome yet",
        ) {
            return;
        }
        self.stats.unacked_replay_settles_sent += 1;
        let inert = MutateOp::Records {
            records: Vec::new(),
        };
        self.submit_settle(now, (entry.rid, entry.queue_seq), inert, replica, out);
    }

    /// Whether a settle of `entry`'s rid may go out now, recording the
    /// attempt: backed off, because with no sequencer reachable a settle ends
    /// in doubt after its window, and asking again once per tick is a storm.
    fn settle_backed_off(&mut self, now: Now, entry: &StrandedOp, warn: &str) -> bool {
        if self
            .replay
            .settles
            .get(&entry.queue_seq)
            .is_some_and(|retry| now.wait < retry.next_at)
        {
            return false;
        }
        let retry = self
            .replay
            .settles
            .entry(entry.queue_seq)
            .or_insert(CopyRetry {
                attempts: 0,
                since: now.wait,
                next_at: now.wait,
            });
        retry.attempts += 1;
        retry.next_at = now.wait.plus(copy_backoff_ms(retry.attempts));
        if retry.attempts == 1 || retry.attempts.is_multiple_of(10) {
            tracing::warn!(
                node = self.cfg.node_id,
                rid = ?entry.rid,
                attempts = retry.attempts,
                "{}",
                warn
            );
        }
        true
    }

    /// Plan 46 M0.2 (R3): whether an op arriving at this sequencer from
    /// another node (a forward, an inbox op) must wait for the replay
    /// queue: some entry *ahead of it* touches a key it touches.
    ///
    /// The queue is a total order (`StrandedOp::queue_seq`, the journal's
    /// own order of the rolled-back rows). An op whose rid has an entry —
    /// its requester's retry of a dropped row, whose entry is a placeholder
    /// — stands at that entry's place: it waits for the entries before it
    /// and for nothing after it, so it executes exactly where the dropped
    /// row stood, on the state the rows before it leave. An op with no
    /// entry (a new op) stands behind the whole queue: whatever was
    /// acknowledged and is queued ran before it.
    ///
    /// **This cannot deadlock.** An op waits only for entries ahead of its
    /// place (smaller `queue_seq`), and nothing in the queue waits for an
    /// op arriving here except a placeholder, for the retry of its *own*
    /// rid — which stands at its own place and so waits only for entries
    /// before it again. The drain holds an entry only behind placeholders
    /// ahead of it (R2). Every wait goes to a strictly smaller place or to
    /// the retry of a smaller place's own placeholder, so the waits-for
    /// relation has no cycle. The head of the queue waits for nothing:
    /// its placeholder is resolved by its requester's retry (which waits
    /// for nothing ahead of it), by the bound (R5), or by the drain
    /// executing it. A refused entry (a conflict copy to make) never
    /// executes on its keys and holds nothing; an entry whose rid the log
    /// has settled is about to be forgotten and holds nothing.
    ///
    /// An op whose rid already has an outcome here is not held (the dedup
    /// answers it), nor is a settle (it executes nothing). One counter read
    /// when the queue is empty.
    pub(crate) fn queue_gate_blocks(&self, rid: Rid, op: &MutateOp, replica: &dyn Replica) -> bool {
        let Ok(snap) = replica.queue_snapshot() else {
            return false;
        };
        if snap.is_empty() {
            return false;
        }
        if replica.recent_outcome(rid).is_some() || completed_as_outcome(replica, rid, 0).is_some()
        {
            return false;
        }
        let own = snap
            .entries
            .iter()
            .find(|q| q.rid == rid)
            .map(|q| q.queue_seq);
        let lookup = |p, n: &str| replica.lookup_ino(p, n);
        let mut keys = super::client::conflict_keys(op, replica);
        keys.inos.extend(created_ino(op));
        snap.entries.iter().enumerate().any(|(i, ahead)| {
            own.is_none_or(|own| ahead.queue_seq < own)
                && ahead.rid != rid
                && ahead.refused.is_none()
                && snap.keys_of(i, &lookup).overlaps(&keys)
                && completed_as_outcome(replica, ahead.rid, 0).is_none()
        })
    }

    /// Plan 46 M0.2: whether an op that reached this sequencer from `from`
    /// (0: this node's own client) waits behind the replay queue
    /// ([`Self::queue_gate_blocks`]). This node's own replays are the
    /// queue's own and go on.
    pub(crate) fn gated_by_queue(
        &self,
        from: crate::ids::NodeId,
        rid: Rid,
        op: &MutateOp,
        replica: &dyn Replica,
    ) -> bool {
        if from == 0
            && self
                .clients
                .get(&rid)
                .is_some_and(|c| matches!(c.origin, super::client::Origin::Replay { .. }))
        {
            return false;
        }
        self.queue_gate_blocks(rid, op, replica)
    }

    /// Plan 46 M0.2 (R3): the queue changed (an entry was forgotten, or
    /// executed): the ops parked behind it look again.
    pub(crate) fn wake_queue_parks(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.queue_parks() {
            self.complete_ready(now, replica, out);
        }
    }

    /// Plan 46 M0.2 (R2): the retry of a dropped row executed here just
    /// now (or was refused, or was settled): its placeholder is resolved
    /// at once, not at the drain's next tick, and whatever waited behind it
    /// looks again.
    pub(crate) fn rid_executed_here(
        &mut self,
        now: Now,
        rid: Rid,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Ok(queue) = replica.pending_replays() else {
            return;
        };
        if let Some(entry) = queue.iter().find(|q| q.rid == rid && q.is_await_retry()) {
            self.resolve_placeholder(entry, replica);
            self.wake_queue_parks(now, replica, out);
        }
    }

    /// Plan 46 M0.2 (R2): a placeholder (`StrandedOp::await_retry`) whose
    /// rid has an outcome here is done — its requester's retry executed
    /// (or was refused), or the rid was settled — and is forgotten.
    /// `true`: forgotten. A settle this node journaled itself
    /// (`Refused { rid, ABANDONED }`) is an outcome once its row is
    /// committed, like any refusal's answer: until then the placeholder
    /// keeps its place (a deposition that rolled the row back would leave
    /// the rid unowned with its dependents already run).
    pub(crate) fn resolve_placeholder(
        &mut self,
        entry: &StrandedOp,
        replica: &dyn Replica,
    ) -> bool {
        let Some(outcome) = completed_as_outcome(replica, entry.rid, 0)
            .filter(|_| self.completed_row_settled(entry.rid, replica))
        else {
            return false;
        };
        if matches!(outcome, MutateOutcome::Abandoned)
            && self.outcome_uncommitted(entry.rid, replica)
        {
            return false;
        }
        tracing::info!(
            node = self.cfg.node_id,
            rid = ?entry.rid,
            ?outcome,
            "a dropped row's placeholder is resolved: its rid has an outcome"
        );
        self.stats.await_retry_resolved += 1;
        let _ = replica.forget_replay(entry.queue_seq);
        true
    }

    /// `Control::DropHeld` (plan 46 M0.2 review): `repair drop-held` run by
    /// the core, which alone knows which answers of the dropped rows have
    /// not left — those still parked for durability. Another node's row of
    /// the group classes is a placeholder only for those (after a restart
    /// nothing is parked and nothing is known: the op stays, the only copy
    /// that can execute if its answer left); the parked answer of each
    /// placeholder is aborted in the same step (R4), so the requester's
    /// retry arrives at once and meets the placeholder's place.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_control_drop_held(
        &mut self,
        now: Now,
        op: OpId,
        ino: Ino,
        remote: bool,
        sessions: constellation_meta::LiveSessions,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let unanswered = |rid: Rid| self.answer_parked(rid).is_some();
        let live = self.ops_naming_manifests_of(ino);
        // An op this drop rolls back keeps nothing held only while this node
        // stays the sequencer (its retry meets the shipped refusal): a
        // deposed dropper's refusals are fenced (`drop_held_knowing`).
        let sequencer = self.lease.epoch_held() || self.lease.ship_epoch(now, &self.cfg).is_some();
        let live_ops = |hash: &replicix_chunkfmt::ChunkHash| -> Vec<Rid> {
            live.iter()
                .filter(|(_, manifest)| constellation_meta::manifest_names(manifest, hash))
                .map(|(rid, _)| *rid)
                .collect()
        };
        // The `Refused` rows the drop journals are the archive's, at the
        // configuration this tenure's journal is in (plan 46 M1.1 step 4).
        let _class = constellation_meta::PendingClass::set(constellation_meta::RowCommit::group(
            self.root_config_version(),
            constellation_meta::GroupOn::Archive,
        ));
        // ADR-59 part B (D1): without the lease, the holder of the stream the
        // held rows are rows of decides (a deposed root's own sticky stream
        // included): the record goes into that stream.
        let dropped = if sequencer || remote {
            replica.drop_held(
                ino,
                now.wall.0 / 1000,
                remote,
                &unanswered,
                &live_ops,
                &sessions,
                sequencer,
            )
        } else {
            let holds = |gen: u64| self.dl.mine.get(&gen).is_some_and(|d| !d.stopped);
            replica.drop_held_in_stream(
                ino,
                now.wall.0 / 1000,
                &unanswered,
                &live_ops,
                &sessions,
                &holds,
            )
        };
        match dropped {
            Ok(dropped) => match dropped.pending {
                // ADR-59 part B (D1, D4): decided, journaled; the effect (and
                // the control op's answer) waits for the archive to carry the
                // record.
                Some(jseq) => {
                    self.stats.drops_decided += 1;
                    self.park(
                        now,
                        (std::collections::BTreeSet::new(), None),
                        Some(jseq),
                        super::readindex::ParkedWhat::DropApply {
                            op: Some(op),
                            ino,
                            epoch: dropped.pending_epoch,
                            sessions,
                        },
                    );
                    // (The record ships at the next round.)
                    self.nudge(now, out);
                }
                None => {
                    for rid in dropped.placeholders.clone() {
                        self.abort_parked_answer_of(now, rid, replica, out);
                    }
                    out.push(Action::ControlDone {
                        op,
                        result: Ok(ControlOk::DroppedHeld(dropped)),
                    });
                }
            },
            Err(e) => out.push(Action::ControlDone {
                op,
                result: Err(e.to_string()),
            }),
        }
    }

    /// The ops of this node that have not journaled yet and would write a
    /// manifest of `ino`, with those manifests: `repair drop-held` keeps
    /// the lost chunks they name held (`Meta::drop_held_knowing`).
    ///
    /// - this node's client ops that are not finished (an op in
    ///   `AwaitingLog` was executed by another sequencer: its row comes
    ///   through the log, whatever it names is not this node's to hold);
    /// - the forwards parked here unexecuted (`ExecuteReply`);
    /// - the ops a delegate parked here.
    ///
    /// An op the drop rolls back itself (a journaled row, a seed) is
    /// excluded by the drop. Other nodes' inbox batches are not listed: a
    /// foreign op names this node's pending chunks only through the
    /// forwarded-pending list (`--remote`'s case).
    pub(crate) fn ops_naming_manifests_of(&self, ino: Ino) -> Vec<(Rid, Vec<u8>)> {
        let mut out: Vec<(Rid, Vec<u8>)> = Vec::new();
        let mut add = |rid: Rid, op: &MutateOp| {
            for manifest in constellation_meta::op_manifests_of(op, ino) {
                out.push((rid, manifest.to_vec()));
            }
        };
        for (rid, client) in &self.clients {
            if !matches!(client.phase, super::client::Phase::AwaitingLog { .. }) {
                add(*rid, &client.op);
            }
        }
        for (rid, op) in self.parked_exec_ops() {
            add(rid, op);
        }
        for d in self.dl.mine.values() {
            for parked in &d.parked {
                add(parked.rid, &parked.op);
            }
        }
        out
    }

    /// Plan 46 M0.2 (R5): count the time each placeholder in `queue` has
    /// waited since the last tick, on the wait clock — except while the
    /// epoch refuses writes: nothing can execute the retry then, nor a
    /// settle, so the requester's wait is not the bound's. A placeholder
    /// first seen starts from the wall time since it was dropped
    /// (`AwaitRetry::since_ms`: a restart does not start the bound over).
    fn account_placeholders(&mut self, now: Now, queue: &[StrandedOp]) {
        let elapsed = self
            .replay
            .awaiting_tick
            .map_or(0, |last| now.wait.since(last).max(0) as u64);
        self.replay.awaiting_tick = Some(now.wait);
        let running = !self.epoch_refuses_writes();
        let wall_ms = now.wall.0;
        let live: std::collections::BTreeSet<u64> = queue
            .iter()
            .filter(|q| q.is_await_retry())
            .map(|q| q.queue_seq)
            .collect();
        self.replay.awaiting.retain(|seq, _| live.contains(seq));
        let queued: std::collections::BTreeSet<u64> = queue.iter().map(|q| q.queue_seq).collect();
        self.replay
            .foreign_seen
            .retain(|seq, _| queued.contains(seq));
        for q in queue {
            let Some(placeholder) = &q.await_retry else {
                continue;
            };
            match self.replay.awaiting.entry(q.queue_seq) {
                std::collections::btree_map::Entry::Occupied(mut e) => {
                    if running {
                        *e.get_mut() += elapsed;
                    }
                }
                std::collections::btree_map::Entry::Vacant(e) => {
                    e.insert((wall_ms - placeholder.since_ms).max(0) as u64);
                }
            }
        }
    }

    /// Plan 46 M0.2 (R5): a placeholder that has waited the bound for its
    /// requester's retry is settled by the sequencer in force — the rid's
    /// outcome if it has one by now, else `Refused { rid, ABANDONED }`
    /// journaled under the remote asker's class (`Core::settle_refusal`,
    /// C2g's record) and answered once committed, like any refusal — and
    /// forgotten when the settle's outcome comes back
    /// (`replay_outcome`). The settle is a client entry of this node under
    /// the rid (an op nobody will execute: `settle` from the start, the
    /// op inert), so it goes the way the op itself would: executed here
    /// when this node is the sequencer, else to the holder, a delegate, or
    /// the inbox. Not while the epoch refuses writes, and not while one is
    /// in flight.
    fn settle_placeholder_if_due(
        &mut self,
        now: Now,
        entry: &StrandedOp,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let waited = self
            .replay
            .awaiting
            .get(&entry.queue_seq)
            .copied()
            .unwrap_or(0);
        if waited < self.dl_t().drop_held_await
            || self.epoch_refuses_writes()
            || self.clients.contains_key(&entry.rid)
        {
            return;
        }
        tracing::warn!(
            node = self.cfg.node_id,
            rid = ?entry.rid,
            waited_ms = waited,
            "a dropped row's requester never retried: settling the rid as abandoned"
        );
        self.stats.await_retry_settles_sent += 1;
        let inert = MutateOp::Records {
            records: Vec::new(),
        };
        self.submit_settle(now, (entry.rid, entry.queue_seq), inert, replica, out);
    }

    /// Whether the refusal (or completion) of `rid` a refused entry's copy
    /// rests on is committed: not a row of this node's own journal the log
    /// or the backup group does not hold. Stricter than
    /// [`Self::outcome_uncommitted`], which trusts `park_durable`: that is
    /// true whenever this node holds a continuation epoch (an answer needs
    /// no S3 there), and a deposed hold owner that has not learnt of it
    /// would make its copy at once and forget the entry, with the row it
    /// rested on rolled back at the deposition. Here an own row counts once
    /// the archive carries it, or the group does outside an epoch hold; a
    /// lone tenure's row, once shipped.
    fn copy_row_committed(&self, rid: Rid, replica: &dyn Replica) -> bool {
        match replica.outcome_row(rid) {
            Some(row) if row.gen == 0 => {
                let shipped = replica.journal_acked_seq().unwrap_or(0);
                self.archive_covers(row.jseq, shipped)
                    || (!self.lease.epoch_held()
                        && self.durable_jseq() != u64::MAX
                        && self.durable_covers(row.jseq))
            }
            _ => true,
        }
    }

    /// Whether `rid`'s `completed` row is this node's own journal row
    /// that its commit point (the group or the archive, as for a remote
    /// asker) does not hold yet.
    fn outcome_uncommitted(&self, rid: Rid, replica: &dyn Replica) -> bool {
        match replica.outcome_row(rid) {
            Some(row) if row.gen == 0 => {
                let shipped = replica.journal_acked_seq().unwrap_or(0);
                !self.park_durable(row.jseq, true, shipped)
            }
            _ => false,
        }
    }

    /// A stranded foreign entry — a transaction this node held as streamed
    /// speculation — whose *source* (the holder, or an epoch's hold owner,
    /// whose stream carried it: `StrandedOp::source`) an admin retired
    /// (`leave --node-id`, which fences its leases with its id in
    /// `Lease::retired`): forgotten, not replayed. Streamed rows are the
    /// source's unshipped journal, which died with it and which the
    /// operator gave up by retiring it.
    ///
    /// Only the source's speculation, though: a committed row (`gated`: its
    /// answer waited for a group or the archive, so this node holds it as
    /// the group's copy; plan 46 M1.4 step 4, an epoch hold's row answered
    /// after this member's `StreamAheadAck`) was acknowledged to a client
    /// and is not the retired node's to take with it. It replays (ADR-51:
    /// the fallback for a copy that no claim carried; the flex model's
    /// `hold_answered_rows_survive_retire`). Keyed on the source, not the
    /// requester (`rid.node`): an op a retired node forwarded to a live
    /// holder is that holder's journal and replays, and an op a live node
    /// forwarded to the retired holder went down with the rest of its
    /// journal. The node's own ops are never foreign, and a successor's
    /// backup tail (`Backup`-acknowledged rows) is not a replay.
    fn drop_retired_origin(&mut self, queued: &StrandedOp, replica: &dyn Replica) -> bool {
        let Some(source) = queued.source.filter(|_| queued.foreign) else {
            return false;
        };
        if queued.class.gated() || source == self.cfg.node_id {
            return false;
        }
        let retired = self
            .lease
            .held
            .as_ref()
            .map(|(l, _)| l)
            .into_iter()
            .chain(self.lease.last_seen.as_ref())
            .any(|l| l.retired.contains(&source));
        if !retired {
            return false;
        }
        tracing::info!(
            node = self.cfg.node_id,
            source,
            rid = ?queued.rid,
            "a retired node's stranded journal is not replayed"
        );
        self.note_unacked_refused(queued);
        self.stats.replays_of_retired_dropped += 1;
        let _ = replica.forget_replay(queued.queue_seq);
        true
    }

    /// A refused replay: record the refusal and start its conflict copy.
    /// Plan 30 §M9: bookkeeping for a refused replay of a never
    /// acknowledged op (an unacked entry is `foreign`-marked).
    fn note_unacked_refused(&mut self, queued: &StrandedOp) {
        if self.replay.unacked.remove(&queued.rid) {
            self.stats.unacked_replays_refused += 1;
        }
    }

    /// Plan 30 §M9: after a deposition recovery queued the stranded rows
    /// for replay by rid, the entries of ops whose reply never left
    /// (`abort_durable_parks`) are marked unacked, persistently.
    pub(crate) fn mark_unacked_replays(&mut self, replica: &dyn Replica) {
        if self.replay.unacked.is_empty() {
            return;
        }
        let Ok(queued) = replica.pending_replays() else {
            return;
        };
        let queued_rids: std::collections::BTreeSet<Rid> = queued.iter().map(|q| q.rid).collect();
        for q in &queued {
            if self.replay.unacked.contains(&q.rid) && !q.foreign {
                let _ = replica.mark_replay_unacked(q.queue_seq);
            }
        }
        // Rids the recovery did not queue (rows already shipped, or
        // nothing of the op journaled) need no marker.
        self.replay.unacked.retain(|rid| queued_rids.contains(rid));
    }

    fn refuse_replay(
        &mut self,
        now: Now,
        queued: &StrandedOp,
        reason: String,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let _ = replica.mark_replay_refused(queued.queue_seq, reason.clone());
        tracing::error!(
            node = self.cfg.node_id,
            rid = ?queued.rid,
            op = ?queued.op,
            reason,
            "stranded op replay refused; materializing a conflict copy"
        );
        self.stats.replay_conflicts += 1;
        self.try_copy(now, queued, reason, replica, out);
    }

    /// One attempt, subject to backoff, at a refused replay's conflict
    /// copy: at most one copy in flight; a stalled copy asks for the
    /// lease once per fallback interval (the node still named in the
    /// lease object with its view closed is what stalls it).
    fn try_copy(
        &mut self,
        now: Now,
        queued: &StrandedOp,
        reason: String,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.replay.copy_in_flight.is_some() {
            return;
        }
        if self
            .replay
            .copies
            .get(&queued.queue_seq)
            .is_some_and(|retry| now.wait < retry.next_at)
        {
            let stalled_for = now.wait.since(self.replay.copies[&queued.queue_seq].since);
            let asked_recently = self
                .replay
                .copy_lease_asked_at
                .is_some_and(|at| now.wait.since(at) < self.cfg.replay_lease_fallback_ms as i64);
            // Not in an epoch that refuses writes: no lease can be had
            // there (see `on_drain_tick`).
            if stalled_for >= self.cfg.replay_lease_fallback_ms as i64
                && !asked_recently
                && self.lease.ship_epoch(now, &self.cfg).is_none()
                && !self.epoch_refuses_writes()
            {
                tracing::warn!(
                    node = self.cfg.node_id,
                    rid = ?queued.rid,
                    stalled_ms = stalled_for,
                    "a conflict copy has been stalled; acquiring the lease to make it locally"
                );
                self.replay.copy_lease_asked_at = Some(now.wait);
                self.enqueue_job(
                    now,
                    super::jobs::JobReq::Acquire {
                        reason: "copy-fallback",
                        ask_handoff: self.cfg.p2p,
                    },
                    replica,
                    out,
                );
            }
            return;
        }
        self.replay.copy_in_flight = Some(queued.queue_seq);
        out.push(Action::ConflictCopy {
            queue_seq: queued.queue_seq,
            rid: queued.rid,
            op: queued.op.clone(),
            reason,
            locked: !queued.lock_tag.is_empty(),
        });
    }

    pub(crate) fn on_conflict_copy_done(
        &mut self,
        now: Now,
        queue_seq: u64,
        ok: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.replay.copy_in_flight == Some(queue_seq) {
            self.replay.copy_in_flight = None;
        }
        if ok {
            let _ = replica.forget_replay(queue_seq);
            self.replay.copies.remove(&queue_seq);
            self.wake_queue_parks(now, replica, out);
            return;
        }
        let retry = self.replay.copies.entry(queue_seq).or_insert(CopyRetry {
            attempts: 0,
            since: now.wait,
            next_at: now.wait,
        });
        retry.attempts += 1;
        retry.next_at = now.wait.plus(copy_backoff_ms(retry.attempts));
        if retry.attempts == 1 || retry.attempts.is_multiple_of(10) {
            tracing::warn!(
                node = self.cfg.node_id,
                queue_seq,
                attempts = retry.attempts,
                "a refused replay's conflict copy could not be made yet; backing off"
            );
        }
    }

    /// The forward path answered (or gave up on) a replayed op.
    pub(crate) fn on_replay_outcome(
        &mut self,
        now: Now,
        queue_seq: u64,
        rid: Rid,
        outcome: Option<MutateOutcome>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.replay_outcome(now, queue_seq, rid, outcome, replica, out);
        self.wake_queue_parks(now, replica, out);
    }

    fn replay_outcome(
        &mut self,
        now: Now,
        queue_seq: u64,
        rid: Rid,
        outcome: Option<MutateOutcome>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.replay.in_flight == Some(queue_seq) {
            self.replay.in_flight = None;
        }
        let queue = replica.pending_replays().unwrap_or_default();
        let Some(queued) = queue.iter().find(|s| s.queue_seq == queue_seq).cloned() else {
            return;
        };
        // Plan 46 M0.2 (R5): the answer to a placeholder's settle. The rid
        // has an outcome — the settle's refusal, or one that came first —
        // and the placeholder is done; no answer (in doubt, busy, not the
        // holder) leaves it for a later tick to settle again.
        if queued.is_await_retry() {
            match outcome {
                None
                | Some(MutateOutcome::Busy)
                | Some(MutateOutcome::NotHolder { .. })
                | Some(MutateOutcome::Held { .. })
                | Some(MutateOutcome::Fenced { .. })
                | Some(MutateOutcome::HolderUnreachable { .. }) => {}
                Some(outcome) => {
                    if outcome == MutateOutcome::Abandoned {
                        self.stats.await_retry_settled += 1;
                    } else {
                        self.stats.await_retry_resolved += 1;
                    }
                    let _ = replica.forget_replay(queue_seq);
                }
            }
            return;
        }
        // Defect C: the answer to a refused entry's settle
        // (`copy_after_settle`): the original was completed in the log (the
        // write survived: no copy), or a refusal answered it, whose row the
        // tick makes the copy of once the log shows it; no answer leaves it
        // for a later tick.
        if queued.dropped && queued.refused.is_some() {
            match outcome {
                None
                | Some(MutateOutcome::Busy)
                | Some(MutateOutcome::NotHolder { .. })
                | Some(MutateOutcome::Held { .. })
                | Some(MutateOutcome::Fenced { .. })
                | Some(MutateOutcome::HolderUnreachable { .. }) => {}
                Some(MutateOutcome::Accepted { .. }) => {
                    self.stats.refused_entries_survived += 1;
                    let _ = replica.forget_replay(queue_seq);
                    self.replay.stuck_since = None;
                }
                // Any other answer (a refusal of the rid, whatever its code):
                // the copy waits for the rid's refusal to be *visible in the
                // log here* (`copy_after_settle`'s first rule), not for the
                // answer's say-so: an answer is a message, and only the
                // log's row is the outcome the rid keeps. The next tick
                // finds the row (or asks again).
                Some(_) => {
                    self.stats.refused_entry_answers_awaiting_log += 1;
                }
            }
            return;
        }
        // Plan 30 §M9: a foreign entry (another node's op, stranded here
        // as streamed speculation) only ever waits for the successor's
        // verdict; refused, it is simply forgotten — its own requester
        // makes any conflict copy. So is a folded truncate's replay
        // (`as_folded`): the sequencer journaled its refusal, and the
        // commit after it decides.
        if (queued.foreign || folded_into_later_manifest(&queued, &queue))
            && matches!(
                outcome,
                Some(
                    MutateOutcome::Errno(_)
                        | MutateOutcome::Exists { .. }
                        | MutateOutcome::Conflict { .. }
                        | MutateOutcome::LockLapsed
                        | MutateOutcome::Abandoned
                )
            )
        {
            self.note_unacked_refused(&queued);
            let _ = replica.forget_replay(queue_seq);
            self.replay.stuck_since = None;
            return;
        }
        let reason = match outcome {
            Some(MutateOutcome::Accepted { .. }) => {
                tracing::info!(node = self.cfg.node_id, ?rid, "stranded op replayed by rid");
                self.stats.stranded_replayed += 1;
                let _ = replica.forget_replay(queue_seq);
                self.replay.stuck_since = None;
                return;
            }
            // In doubt: leave it queued; the next tick finds it completed
            // or resends it.
            None
            | Some(MutateOutcome::Busy)
            | Some(MutateOutcome::NotHolder { .. })
            | Some(MutateOutcome::Held { .. })
            | Some(MutateOutcome::Fenced { .. })
            | Some(MutateOutcome::HolderUnreachable { .. }) => return,
            Some(MutateOutcome::Errno(code)) if refusal_is_satisfied(&queued.op, code) => {
                self.stats.stranded_replayed += 1;
                let _ = replica.forget_replay(queue_seq);
                self.replay.stuck_since = None;
                return;
            }
            Some(MutateOutcome::Errno(code)) => format!("refused with {code}"),
            Some(MutateOutcome::Exists { .. }) => "the name now exists".to_string(),
            Some(MutateOutcome::Conflict { .. }) => "stale manifest base".to_string(),
            // Plan 30 §M14 phase 2: the replay carried the fencing token
            // of a grant that has ended since. Final: what the op would
            // have written is kept aside (the conflict copy), never
            // applied over the next lock holder's writes.
            Some(MutateOutcome::LockLapsed) => {
                "the lock grant it was issued under has ended".to_string()
            }
            // Plan 43 C2 (§5.3 item 5): the rid was settled by its
            // requester: nothing of it may run, and nobody heard it took
            // effect (an acknowledged op is never settled).
            Some(MutateOutcome::Abandoned) => {
                self.note_unacked_refused(&queued);
                self.stats.stranded_replayed += 1;
                let _ = replica.forget_replay(queue_seq);
                self.replay.stuck_since = None;
                return;
            }
        };
        self.replay.stuck_since = None;
        self.refuse_replay(now, &queued, reason, replica, out);
    }

    /// `recovery::takeover_gate`'s replay half: execute every queued op
    /// locally, in order, as the holder, before the view opens. A refused
    /// one stays queued for its conflict copy (the drain makes it). One
    /// left queued for the drain (see `replay_locally`) holds every later
    /// entry on its keys too: the drain runs the queue in order, and an
    /// entry executed here ahead of an earlier one on the same keys
    /// would not be (an unlink of `x` held for a file another node
    /// sequences, then the create of a new `x`: refused `EEXIST`, or
    /// created only for the drain's unlink to remove it).
    pub(crate) fn replay_queue_locally(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Result<(), constellation_meta::MetaError> {
        let queue = replica.pending_replays()?;
        // Inodes whose folded truncate's rid this replica already holds
        // completed while the file is still what the truncate would cut:
        // another node settled it as the no-op (it had no before-image of
        // the inode), so the truncate never took effect and the commit
        // after it must stand for it — checked against the uncut manifest
        // the truncate started from, not the cut it was composed on.
        let mut settled_uncut: std::collections::HashMap<
            constellation_fs_core::Ino,
            (Vec<u8>, Vec<u8>),
        > = Default::default();
        // The keys of the entries left queued so far, as one set (an
        // overlap with a union is an overlap with one of its sets).
        let mut held_keys = constellation_meta::TouchSet::default();
        for queued in &queue {
            // Plan 46 M0.2 (R1, R2): a placeholder is no op to run here
            // either; until its rid has an outcome it holds the later
            // entries on its keys, which the drain runs after it.
            if let Some(placeholder) = &queued.await_retry {
                if !self.resolve_placeholder(queued, replica) {
                    absorb_keys(&mut held_keys, &placeholder.keys);
                }
                continue;
            }
            // The entry's own keys (a folded truncate's too, whatever it
            // replays as), with the inode it creates: a later entry in a
            // directory, or on a file, that a held entry creates runs after
            // it. (Only here: an inode not created yet resolves to the root,
            // so the ownership keys leave it out.)
            let mut keys = super::client::conflict_keys(&queued.op, replica);
            keys.inos.extend(created_ino(&queued.op));
            let held = &mut Held {
                keys,
                held: &mut held_keys,
            };
            // The drain tick's rule, here too (it was the pre-M5
            // `recovery::takeover_gate`'s, lost when the gate moved into
            // the core): a size-only truncate with a manifest commit for
            // the same inode queued after it is folded into that commit,
            // which carries the final size *and* the base check. Executed
            // on its own it has no base to check — a takeover gate that
            // replayed it truncated the file another node had written
            // since (harness `deposed-reintegration`: B's `same` became
            // empty while A's manifest was, correctly, refused). It replays
            // as `as_folded`, by its own rid.
            if folded_into_later_manifest(queued, &queue) {
                if let (
                    Some(MutateOutcome::Accepted { .. }),
                    Some(MutateOp::SetManifest {
                        ino,
                        base_manifest: Some(pre),
                        manifest: cut,
                        ..
                    }),
                ) = (
                    completed_as_outcome(replica, queued.rid, 0),
                    queued.conditional_truncate(),
                ) {
                    if replica.manifest(ino).as_ref() == Some(&pre) {
                        settled_uncut.insert(ino, (cut, pre));
                    }
                }
                self.replay_locally(now, &as_folded(queued), true, held, replica, out)?;
                continue;
            }
            if let MutateOp::SetManifest {
                ino,
                base_manifest: Some(base),
                ..
            } = &queued.op
            {
                if let Some((cut, pre)) = settled_uncut.remove(ino) {
                    if *base == cut {
                        let mut on_uncut = queued.clone();
                        if let MutateOp::SetManifest { base_manifest, .. } = &mut on_uncut.op {
                            *base_manifest = Some(pre);
                        }
                        self.replay_locally(now, &on_uncut, false, held, replica, out)?;
                        continue;
                    }
                }
            }
            self.replay_locally(now, queued, false, held, replica, out)?;
        }
        self.replay.in_flight = None;
        Ok(())
    }

    /// `folded`: `queued` is a truncate folded into the commit after it
    /// (`as_folded`): whatever its outcome, it leaves the queue — its
    /// refusal is journaled as its rid's outcome but makes no conflict copy
    /// of its own (the commit after it decides). An entry it leaves queued
    /// for the drain (unexecuted, unrefused) adds its keys to `held`.
    fn replay_locally(
        &mut self,
        now: Now,
        queued: &StrandedOp,
        folded: bool,
        held: &mut Held<'_>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Result<(), constellation_meta::MetaError> {
        if queued.refused.is_some() || self.drop_retired_origin(queued, replica) {
            return Ok(());
        }
        // Late-landing S2a, at the gate too: an entry nobody acknowledged
        // (`unacked`) or another node's (`foreign`) is never executed, its
        // client may have heard `EIO`. It stays queued, holding the later
        // entries on its keys, for the drain to settle (an entry the log
        // already settled is forgotten below).
        if (queued.foreign || self.replay.unacked.contains(&queued.rid))
            && completed_as_outcome(replica, queued.rid, 0).is_none()
        {
            held.hold();
            self.stats.replays_held_for_stream += 1;
            return Ok(());
        }
        // Per-key order: behind an earlier entry left to the drain, the
        // drain runs this one too, after it (an entry the log already
        // settled is not executed again: it is forgotten below).
        if held.overlaps() && completed_as_outcome(replica, queued.rid, 0).is_none() {
            held.hold();
            self.stats.replays_held_for_stream += 1;
            return Ok(());
        }
        // Plan 30 §M11 phase 2b round 2: a delegate-accepted shadow stays
        // queued while its generation is live (the delegate re-streams it
        // to this successor, in stream order; the drain tick frees it
        // once the generation ended) — never executed here ahead of the
        // delegate's earlier transactions (long-delegated seed 70075).
        if queued.gen != 0 && replica.delegation_table().has_gen(queued.gen) {
            held.hold();
            self.stats.replays_held_for_stream += 1;
            return Ok(());
        }
        // Plan 30 §M11: executed here, the replay bypasses the ownership
        // every other path respects. An op on keys another node sequences
        // (a live delegation) is left for the drain tick, whose submit
        // routes it like any client op: to the delegate, or through the
        // root's recall of the generation (long-delegated seed 73964: the
        // successor's gate replayed a create in `d1` while `d1` was
        // delegated to another node; the log put it ahead of that
        // delegate's earlier, acknowledged unlink). The delegations this
        // node holds itself are its own to execute (its stranded rows
        // are in this very queue, in order).
        // Plan 30 §M11: nor ahead of the delegate-stream rows it was
        // ordered after (`StrandedOp::deps`) while they are still to be
        // appended here: the drain tick submits it, and the root's
        // execution waits for them as for any op's `deps`.
        let deps = Position {
            streams: queued.deps,
            ..Position::ZERO
        };
        if !replica.reaches_streams(&deps)
            && !replica.deps_lost(&deps)
            && completed_as_outcome(replica, queued.rid, 0).is_none()
        {
            held.hold();
            self.stats.replays_held_for_stream += 1;
            return Ok(());
        }
        let keys = super::holder::keys_of_op_in(&queued.op, replica);
        let foreign_owner = match replica.resolve_ownership(&keys) {
            constellation_meta::delegation::Ownership::Root => false,
            constellation_meta::delegation::Ownership::Delegated(d) => d.node != self.cfg.node_id,
            constellation_meta::delegation::Ownership::CrossSubtree { involved, .. } => {
                involved.iter().any(|d| d.node != self.cfg.node_id)
            }
        };
        if foreign_owner && completed_as_outcome(replica, queued.rid, 0).is_none() {
            held.hold();
            self.stats.replays_held_for_stream += 1;
            return Ok(());
        }
        if let Some(outcome) = completed_as_outcome(replica, queued.rid, 0) {
            // (Forgotten also when the rid's row is a delegation row not
            // settled yet. Keeping the entry, with or without a hold of its
            // keys, replays an op the delegate's group had acknowledged as a
            // conflict copy once the row is stranded, I5,
            // `c2_a_delegates_committed_answer_is_never_rolled_back` seed 5.
            // A deposition does not strand a generation's rows: the delegate
            // streams them again; a tenure deposed before its own row ships
            // strands it and queues its op by rid again,
            // `Core::finish_recovery`. Generations that end orphaned at cut 0
            // are the known L2b gap, `generation_epochs.rs`.)
            if folded {
                if !matches!(outcome, MutateOutcome::Accepted { .. }) {
                    self.note_unacked_refused(queued);
                    self.stats.stranded_replayed += 1;
                }
                replica.forget_replay(queued.queue_seq)?;
                return Ok(());
            }
            match outcome {
                MutateOutcome::Accepted { .. } => {
                    replica.forget_replay(queued.queue_seq)?;
                }
                // Settled (see `on_drain_tick`): nothing of it may run.
                MutateOutcome::Abandoned => {
                    self.note_unacked_refused(queued);
                    self.stats.stranded_replayed += 1;
                    replica.forget_replay(queued.queue_seq)?;
                }
                MutateOutcome::Errno(code)
                    if queued.foreign || refusal_is_satisfied(&queued.op, code) =>
                {
                    self.note_unacked_refused(queued);
                    self.stats.stranded_replayed += 1;
                    replica.forget_replay(queued.queue_seq)?;
                }
                MutateOutcome::Errno(code) => {
                    let reason = format!("refused with {code} (inbox)");
                    self.refuse_replay(now, queued, reason, replica, out);
                }
                MutateOutcome::LockLapsed => {
                    let reason = "the lock grant it was issued under has ended".to_string();
                    self.refuse_replay(now, queued, reason, replica, out);
                }
                _ => {
                    let reason = "stale manifest base (inbox)".to_string();
                    self.refuse_replay(now, queued, reason, replica, out);
                }
            }
            return Ok(());
        }
        // Plan 30 §M14 phase 2: with the op's fencing token — a replay
        // under a grant that has ended is refused (`LockLapsed`) and
        // becomes a conflict copy below, as any refusal does.
        // (Plan 43 C2f review: another node's rid is journaled with that
        // node's class: a lone root's row of it is `Archive`.)
        let asker = self.asker_of(queued.rid.node);
        let _class = self.journal_class_for(asker);
        match constellation_meta::locks::TokenPath::Replay
            .scope(|| replica.execute(&queued.op, Some(queued.rid), &queued.lock_tag, now))
        {
            Ok(_) => {
                tracing::info!(node = self.cfg.node_id, rid = ?queued.rid, "stranded op replayed locally");
                self.stats.stranded_replayed += 1;
                replica.forget_replay(queued.queue_seq)?;
            }
            Err(error) => {
                // A refusal here is final (the replay is satisfied, or
                // becomes a conflict copy), so it is the rid's outcome,
                // journaled as the forwarded path journals one
                // (`record_refusal`) — a stale base or a lapsed grant
                // included, which are final for a replay. Left unrecorded,
                // a copy of the op queued by the same rid on another node
                // (a deposed root's append of this node's delegate row,
                // queued there as `Records`, which apply without the base
                // check) was executed later and overwrote what this refusal
                // kept out (load campaign seed 61: the conflict-copied
                // commit landed in the file anyway).
                let code = match error {
                    constellation_meta::MetaError::Conflict => Code::Stale,
                    constellation_meta::MetaError::LockLapsed => super::client::LAPSED_CODE,
                    ref e => e.code(),
                };
                // (`NotAdmitted` is no outcome: nothing ran, and it never
                // answers anyone.)
                if !matches!(error, constellation_meta::MetaError::NotAdmitted) {
                    self.record_refusal(queued.rid, code, Some(&queued.op), asker, replica);
                }
                if folded || queued.foreign || refusal_is_satisfied(&queued.op, error.code()) {
                    self.note_unacked_refused(queued);
                    self.stats.stranded_replayed += 1;
                    replica.forget_replay(queued.queue_seq)?;
                } else {
                    self.refuse_replay(now, queued, format!("{error}"), replica, out);
                }
            }
        }
        Ok(())
    }
}

/// `replay_queue_locally`'s per-key order: the keys of the entry being
/// replayed (`keys`) and of the entries left queued for the drain before
/// it (`held`).
struct Held<'a> {
    keys: constellation_meta::TouchSet,
    held: &'a mut constellation_meta::TouchSet,
}

impl Held<'_> {
    /// Whether an earlier entry left to the drain shares a key with this one.
    fn overlaps(&self) -> bool {
        self.held.overlaps(&self.keys)
    }

    /// This entry is left to the drain: the later ones on its keys wait
    /// for it.
    fn hold(&mut self) {
        let keys = std::mem::take(&mut self.keys);
        absorb_keys(self.held, &keys);
    }
}

/// `into` plus `keys` (an overlap with the union is an overlap with one
/// of its sets).
fn absorb_keys(into: &mut constellation_meta::TouchSet, keys: &constellation_meta::TouchSet) {
    into.dentries.extend(keys.dentries.iter().cloned());
    into.inos.extend(keys.inos.iter().copied());
    into.shared.extend(keys.shared.iter().copied());
}

/// The keys a queue entry touches: a placeholder's own, else its op's
/// (with the inode it creates: a later entry in a directory, or on a file,
/// that a queued entry creates runs after it).
pub(super) fn entry_keys(
    entry: &StrandedOp,
    replica: &dyn Replica,
) -> constellation_meta::TouchSet {
    if let Some(placeholder) = &entry.await_retry {
        return placeholder.keys.clone();
    }
    let mut keys = super::client::conflict_keys(&entry.op, replica);
    keys.inos.extend(created_ino(&entry.op));
    keys
}

/// The `queue_seq`s of the entries the drain holds behind a placeholder
/// (R2): not refused, not settled in the log, on a key a placeholder ahead
/// of them (or an entry held for one) touches.
pub(super) fn held_behind_placeholders(
    queue: &[StrandedOp],
    replica: &dyn Replica,
) -> std::collections::BTreeSet<u64> {
    let mut held = std::collections::BTreeSet::new();
    if !queue.iter().any(|q| q.is_await_retry()) {
        return held;
    }
    let mut behind = constellation_meta::TouchSet::default();
    for e in queue {
        if completed_as_outcome(replica, e.rid, 0).is_some() {
            continue;
        }
        if let Some(p) = &e.await_retry {
            absorb_keys(&mut behind, &p.keys);
        } else if e.refused.is_none() && overlaps_queue_keys(&behind, e, replica) {
            absorb_keys(&mut behind, &entry_keys(e, replica));
            held.insert(e.queue_seq);
        }
    }
    held
}

/// Whether `entry` shares a key with `behind` and is not already settled
/// in the log (a settled entry is forgotten, not held).
fn overlaps_queue_keys(
    behind: &constellation_meta::TouchSet,
    entry: &StrandedOp,
    replica: &dyn Replica,
) -> bool {
    if behind.dentries.is_empty() && behind.inos.is_empty() && behind.shared.is_empty() {
        return false;
    }
    behind.overlaps(&entry_keys(entry, replica))
        && completed_as_outcome(replica, entry.rid, 0).is_none()
}

/// The inode `op` creates (its new name's): the op form names it, but its
/// keys leave it out (`TouchSet::from_op`; the `Records` form counts it).
pub(super) fn created_ino(op: &MutateOp) -> Option<constellation_fs_core::Ino> {
    match op {
        MutateOp::Create { ino, .. }
        | MutateOp::Mkdir { ino, .. }
        | MutateOp::Symlink { ino, .. }
        | MutateOp::Mknod { ino, .. } => Some(*ino),
        _ => None,
    }
}
