//! The stranded-op replay drain (`recovery::drain_pending_replays`), the
//! local replay the takeover gate runs (`recovery::replay_locally`), and
//! the conflict-copy retries of refused replays (plan 30 §M4 round 2).

use super::client::{completed_as_outcome, meta_errno, Origin};
use super::Core;
use crate::action::Action;
use crate::event::Policy;
use crate::ids::Ms;
use crate::replica::Replica;
use constellation_meta::{MutateOp, MutateOutcome, Rid, StrandedOp};
use std::collections::BTreeMap;

/// First and largest pause between attempts at a refused replay's
/// conflict copy that could not be made yet.
const COPY_BACKOFF_MIN_MS: u64 = 250;
const COPY_BACKOFF_MAX_MS: u64 = 10_000;

#[derive(Debug, Clone, Copy)]
struct CopyRetry {
    attempts: u32,
    /// When the first attempt failed: how long the copy has been stalled.
    since: Ms,
    /// Not before this.
    next_at: Ms,
}

#[derive(Debug, Default)]
pub(crate) struct ReplayState {
    /// The queued op currently in flight through the forward path.
    pub in_flight: Option<u64>,
    /// When the head of the queue last stopped making progress.
    pub stuck_since: Option<Ms>,
    /// Refused replays whose conflict copy failed at least once.
    copies: BTreeMap<u64, CopyRetry>,
    /// The conflict copy the driver is making right now.
    copy_in_flight: Option<u64>,
    /// When a stalled copy last asked for the lease.
    copy_lease_asked_at: Option<Ms>,
}

impl ReplayState {
    /// `(pending, stalled)`: copies that failed at least once, and those
    /// failing for at least the lease-fallback interval.
    pub fn copy_counts(&self, now: Ms, fallback_ms: u64) -> (u64, u64) {
        let stalled = self
            .copies
            .values()
            .filter(|c| now.since(c.since) >= fallback_ms as i64)
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
fn refusal_is_satisfied(op: &MutateOp, errno: i32) -> bool {
    matches!(op, MutateOp::Unlink { .. } | MutateOp::Rmdir { .. }) && errno == libc::ENOENT
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
                && matches!(&later.op, MutateOp::SetManifest { ino: m, .. } if m == ino)
        })
}

impl Core {
    /// One drain tick: refused ops get their conflict copy (with backoff,
    /// never holding up the queue); the oldest unresolved op goes down
    /// the forward path (holder-local, or forwarded with M2's retries);
    /// after `replay_lease_fallback_ms` without progress this node asks
    /// for the lease itself, whose gate replays locally.
    pub(crate) fn on_drain_tick(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        // A holder never tails the later-epoch segment that would strand
        // speculation from an older tenure left on it: strand it here.
        if let Some(epoch) = self.lease.ship_epoch(now, &self.cfg) {
            if replica.has_outstanding_speculation() {
                if let Ok(stranded) = replica.strand_below_epoch(epoch) {
                    self.stats.speculation_rolled_back +=
                        (stranded.shadows + stranded.hints) as u64;
                    self.stats.local_rolled_back += stranded.locals as u64;
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
            if folded_into_later_manifest(head, &queued) {
                let _ = replica.forget_replay(head.queue_seq);
                self.stats.stranded_replayed += 1;
                continue;
            }
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
                match outcome {
                    MutateOutcome::Accepted { .. } => {
                        let _ = replica.forget_replay(head.queue_seq);
                    }
                    // Refused through an inbox before this node saw it:
                    // an outcome, not a re-evaluation.
                    MutateOutcome::Errno(errno) if refusal_is_satisfied(&head.op, errno) => {
                        self.stats.stranded_replayed += 1;
                        let _ = replica.forget_replay(head.queue_seq);
                    }
                    MutateOutcome::Errno(errno) => {
                        let reason = format!("refused with errno {errno} (inbox)");
                        self.refuse_replay(now, head, reason, replica, out);
                    }
                    MutateOutcome::Conflict { .. } => {
                        let reason = "stale manifest base (inbox)".to_string();
                        self.refuse_replay(now, head, reason, replica, out);
                    }
                    _ => {}
                }
                continue;
            }
            if self.clients.contains_key(&head.rid) {
                // Still in flight as a client op (its reply is what will
                // resolve it); nothing to do this tick.
                return;
            }
            let stuck_for = self.replay.stuck_since.get_or_insert(now).to_owned();
            if now.since(stuck_for) >= self.cfg.replay_lease_fallback_ms as i64
                && self.lease.ship_epoch(now, &self.cfg).is_none()
            {
                tracing::warn!(node = self.cfg.node_id, rid = ?head.rid, "replay stuck; asking for the lease");
                self.replay.stuck_since = Some(now);
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
                Origin::Replay {
                    queue_seq: head.queue_seq,
                },
                replica,
                out,
            );
            return;
        }
    }

    /// A refused replay: record the refusal and start its conflict copy.
    fn refuse_replay(
        &mut self,
        now: Ms,
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
        now: Ms,
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
            .is_some_and(|retry| now < retry.next_at)
        {
            let stalled_for = now.since(self.replay.copies[&queued.queue_seq].since);
            let asked_recently = self
                .replay
                .copy_lease_asked_at
                .is_some_and(|at| now.since(at) < self.cfg.replay_lease_fallback_ms as i64);
            if stalled_for >= self.cfg.replay_lease_fallback_ms as i64
                && !asked_recently
                && self.lease.ship_epoch(now, &self.cfg).is_none()
            {
                tracing::warn!(
                    node = self.cfg.node_id,
                    rid = ?queued.rid,
                    stalled_ms = stalled_for,
                    "a conflict copy has been stalled; acquiring the lease to make it locally"
                );
                self.replay.copy_lease_asked_at = Some(now);
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
        });
    }

    pub(crate) fn on_conflict_copy_done(
        &mut self,
        now: Ms,
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
            since: now,
            next_at: now,
        });
        retry.attempts += 1;
        retry.next_at = now.plus(copy_backoff_ms(retry.attempts));
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
        now: Ms,
        queue_seq: u64,
        rid: Rid,
        outcome: Option<MutateOutcome>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.replay.in_flight == Some(queue_seq) {
            self.replay.in_flight = None;
        }
        let queued = match replica.pending_replays() {
            Ok(q) => q.into_iter().find(|s| s.queue_seq == queue_seq),
            Err(_) => None,
        };
        let Some(queued) = queued else {
            return;
        };
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
            Some(MutateOutcome::Errno(errno)) if refusal_is_satisfied(&queued.op, errno) => {
                self.stats.stranded_replayed += 1;
                let _ = replica.forget_replay(queue_seq);
                self.replay.stuck_since = None;
                return;
            }
            Some(MutateOutcome::Errno(errno)) => format!("refused with errno {errno}"),
            Some(MutateOutcome::Exists { .. }) => "the name now exists".to_string(),
            Some(MutateOutcome::Conflict { .. }) => "stale manifest base".to_string(),
        };
        self.replay.stuck_since = None;
        self.refuse_replay(now, &queued, reason, replica, out);
    }

    /// `recovery::takeover_gate`'s replay half: execute every queued op
    /// locally, in order, as the holder, before the view opens. A refused
    /// one stays queued for its conflict copy (the drain makes it).
    pub(crate) fn replay_queue_locally(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Result<(), constellation_meta::MetaError> {
        let queued = replica.pending_replays()?;
        for queued in &queued {
            self.replay_locally(now, queued, replica, out)?;
        }
        self.replay.in_flight = None;
        Ok(())
    }

    fn replay_locally(
        &mut self,
        now: Ms,
        queued: &StrandedOp,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Result<(), constellation_meta::MetaError> {
        if queued.refused.is_some() {
            return Ok(());
        }
        if let Some(outcome) = completed_as_outcome(replica, queued.rid, 0) {
            match outcome {
                MutateOutcome::Accepted { .. } => {
                    replica.forget_replay(queued.queue_seq)?;
                }
                MutateOutcome::Errno(errno) if refusal_is_satisfied(&queued.op, errno) => {
                    self.stats.stranded_replayed += 1;
                    replica.forget_replay(queued.queue_seq)?;
                }
                MutateOutcome::Errno(errno) => {
                    let reason = format!("refused with errno {errno} (inbox)");
                    self.refuse_replay(now, queued, reason, replica, out);
                }
                _ => {
                    let reason = "stale manifest base (inbox)".to_string();
                    self.refuse_replay(now, queued, reason, replica, out);
                }
            }
            return Ok(());
        }
        match replica.execute(&queued.op, Some(queued.rid)) {
            Ok(_) => {
                tracing::info!(node = self.cfg.node_id, rid = ?queued.rid, "stranded op replayed locally");
                self.stats.stranded_replayed += 1;
                replica.forget_replay(queued.queue_seq)?;
            }
            Err(error) => {
                let errno = meta_errno(&error);
                if refusal_is_satisfied(&queued.op, errno) {
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
