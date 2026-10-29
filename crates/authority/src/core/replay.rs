//! The stranded-op replay drain (`recovery::drain_pending_replays`), the
//! local replay the takeover gate runs (`recovery::replay_locally`), and
//! the conflict-copy retries of refused replays (plan 30 §M4 round 2).

use super::client::{completed_as_outcome, Origin};
use super::Core;
use crate::action::Action;
use crate::event::Policy;
use crate::ids::{Now, WaitMs};
use crate::replica::Replica;
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
}

impl ReplayState {
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
        if queued.is_empty() {
            self.replay.stuck_since = None;
            return;
        }
        for head in &queued {
            if self.drop_retired_origin(head, replica) {
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
                let reason = refusal.reason.clone();
                self.try_copy(now, head, reason, replica, out);
                continue;
            }
            if self.replay.in_flight.is_some() {
                return;
            }
            if let Some(outcome) = completed_as_outcome(replica, head.rid, 0) {
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
            if head.gen != 0 && replica.delegation_table().iter().any(|d| d.gen == head.gen) {
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

    /// A stranded foreign entry — a transaction this node held as streamed
    /// speculation — whose *source* (the holder, or an epoch's hold owner,
    /// whose stream carried it: `StrandedOp::source`) an admin retired
    /// (`leave --node-id`, which fences its leases with its id in
    /// `Lease::retired`): forgotten, not replayed. Streamed rows are the
    /// source's unshipped journal, which died with it (an epoch's writes
    /// are acknowledged on the hold owner's disk alone) and which the
    /// operator gave up by retiring it; replayed, whatever part of it this
    /// node happened to have streamed surfaced through the successor
    /// (`epoch-holder-retired`, about one run in four: the hold owner's
    /// write reached this member's stream before its `kill -9`). Keyed on
    /// the source, not the requester (`rid.node`): an op a retired node
    /// forwarded to a live holder is that holder's journal and replays,
    /// and an op a live node forwarded to the retired holder went down
    /// with the rest of its journal. The node's own ops are never foreign,
    /// and a successor's backup tail (`Backup`-acknowledged rows) is not a
    /// replay.
    fn drop_retired_origin(&mut self, queued: &StrandedOp, replica: &dyn Replica) -> bool {
        let Some(source) = queued.source.filter(|_| queued.foreign) else {
            return false;
        };
        if source == self.cfg.node_id {
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
        let _ = out;
        if self.replay.copy_in_flight == Some(queue_seq) {
            self.replay.copy_in_flight = None;
        }
        if ok {
            let _ = replica.forget_replay(queue_seq);
            self.replay.copies.remove(&queue_seq);
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
        if self.replay.in_flight == Some(queue_seq) {
            self.replay.in_flight = None;
        }
        let queue = replica.pending_replays().unwrap_or_default();
        let Some(queued) = queue.iter().find(|s| s.queue_seq == queue_seq).cloned() else {
            return;
        };
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
            | Some(MutateOutcome::Held { .. }) => return,
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
        if queued.gen != 0
            && replica
                .delegation_table()
                .iter()
                .any(|d| d.gen == queued.gen)
        {
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
        let _class = self.journal_class();
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
                    self.record_refusal(queued.rid, code, Some(&queued.op), replica);
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
        self.held.dentries.extend(keys.dentries);
        self.held.inos.extend(keys.inos);
        self.held.shared.extend(keys.shared);
    }
}

/// The inode `op` creates (its new name's): the op form names it, but its
/// keys leave it out (`TouchSet::from_op`; the `Records` form counts it).
fn created_ino(op: &MutateOp) -> Option<constellation_fs_core::Ino> {
    match op {
        MutateOp::Create { ino, .. }
        | MutateOp::Mkdir { ino, .. }
        | MutateOp::Symlink { ino, .. }
        | MutateOp::Mknod { ino, .. } => Some(*ino),
        _ => None,
    }
}
