//! A release in doubt whose lease re-read failed too stays fenced until a
//! read of the lease resolves it.
//!
//! A release CAS that fails without an answer may have landed (56085d00), so
//! the node re-reads the lease: landed completes the release, ours keeps the
//! lease, another holder's deposes. When that re-read fails as well the node
//! used to give the lease up at once, which left the object naming it,
//! unreleased, with nobody holding it: a continuation epoch that formed in
//! the cut carried a lease no node claimed, and the peers' settles found no
//! sequencer. It now does not give up. It keeps everything a release in
//! flight holds (`releasing`, the claim view, no grant renewed, no hold
//! adopted) and keeps looking, until the first conclusive read:
//!
//! - its own re-read, issued again on a backoff
//!   ([`crate::deadlines::release_reread_retry_ms`]);
//! - a reachable peer's, asked for with [`PeerMsg::LeaseProbe`]. The peer
//!   issues a new `LeaseGet` after the probe arrived, so the read is causally
//!   after the failed CAS, as the node's own re-read is: no assumption
//!   beyond the one 56085d00 already rests on.
//!
//! The decision table is the re-read's ([`Core::resolve_release_doubt`]).
//! A peer cannot send a `LeaseTag`, so a lease a peer read shows standing
//! keeps the (lease, tag) the node holds: a stale tag only makes the next
//! CAS conflict and re-read.
//!
//! The wait is bounded by the held lease's honour end: past it the old
//! give-up runs unchanged ([`Core::give_up_release_in_doubt`]). A shutdown
//! flush gives up at once, as does a release that runs under an epoch hold.
//! Forwards are answered `Held` meanwhile (`holder.rs`), never executed.

use super::jobs::JobKind;
use super::{Core, S3For, Timer};
use crate::action::{Action, S3Op};
use crate::event::{PeerMsg, ProbedLease, S3Result};
use crate::ids::{Epoch, HonourMs, NodeId, Now, OpId, TimerId, WaitMs};
use crate::replica::Replica;
use constellation_types::Code;
use replicix_objcas::{Lease, LeaseTag};
use std::collections::BTreeMap;

/// The state of a release in doubt that is waiting for a read to resolve it
/// (`Core::release_unresolved`). The job stays in `Phase::ReleaseReread {
/// in_doubt: true }`: its `op` is the node's own outstanding re-read, if any.
#[derive(Debug)]
pub(crate) struct ReleaseUnresolved {
    /// The lease the release CAS replaced: epoch, `expires_unix_ms` and
    /// `holder_expires_ms`. A peer read shows it kept when it carries these.
    mine: (Epoch, i64, i64),
    /// When the wait began.
    since: WaitMs,
    /// Looks made so far (the backoff's exponent).
    tries: u32,
    timer: Option<TimerId>,
    /// Outstanding probes: the peer and the honour time the probe was sent.
    probes: BTreeMap<OpId, (NodeId, HonourMs)>,
}

/// A read of the lease object offered to the decision table.
#[derive(Debug)]
pub(crate) enum DoubtRead {
    /// This node's own re-read, with the tag it returned.
    Own(Option<(Lease, LeaseTag)>),
    /// A peer's read, requested at `sent` (this node's honour clock).
    Peer {
        object: Option<Lease>,
        sent: HonourMs,
    },
}

/// What the table decided.
enum Decision {
    Landed,
    KeptOwn(Lease, LeaseTag),
    KeptByPeer(Lease, HonourMs),
    Deposed(NodeId, Epoch),
    Inconclusive,
}

impl Core {
    /// The job tail every exit of the release re-read shares: the release is
    /// over (landed, kept, deposed or given up), the job ends.
    pub(super) fn release_job_tail(
        &mut self,
        now: Now,
        kind: JobKind,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.lease.releasing = false;
        self.epoch_hold_after_kept_release(now, replica, out);
        match kind {
            JobKind::Round => self.finish_round(now, None, replica, out),
            _ => self.finish_flush_job(now, false, replica, out),
        }
    }

    /// The `LeaseGet` result of `Phase::ReleaseReread`, the node's own.
    pub(super) fn on_release_reread(
        &mut self,
        now: Now,
        kind: JobKind,
        in_doubt: bool,
        result: Result<Option<(Lease, LeaseTag)>, crate::event::S3Failure>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some((mine, _)) = self.lease.held.clone() else {
            self.end_release_unresolved(out);
            self.lease.releasing = false;
            self.lease_gone_mid_job(now, kind, replica, out);
            return;
        };
        match result {
            Ok(object) => {
                self.resolve_release_doubt(now, kind, DoubtRead::Own(object), replica, out)
            }
            // Waiting already: the timer looks again, nothing gives up here.
            Err(e) if self.release_unresolved.is_some() => {
                tracing::debug!(
                    node = self.cfg.node_id,
                    error = %e.0,
                    "lease re-read failed while a release in doubt is unresolved"
                );
                self.set_release_reread_phase(None);
            }
            Err(e) if in_doubt => {
                if self.may_wait_unresolved(now) {
                    tracing::warn!(
                        node = self.cfg.node_id,
                        error = %e.0,
                        "lease re-read failed after a release in doubt; staying fenced until a read resolves it"
                    );
                    self.enter_release_unresolved(now, &mine, out);
                    return;
                }
                tracing::warn!(
                    node = self.cfg.node_id,
                    error = %e.0,
                    "lease re-read failed after a release in doubt; giving the lease up"
                );
                self.give_up_release_in_doubt(now, &mine, replica, out);
                self.release_job_tail(now, kind, replica, out);
            }
            Err(e) => {
                tracing::warn!(node = self.cfg.node_id, error = %e.0, "lease re-read failed");
                self.release_job_tail(now, kind, replica, out);
            }
        }
    }

    /// Whether a release in doubt whose re-read failed may wait for a read:
    /// not a shutdown (the process ends; the record on disk keeps a restart
    /// conservative), not under an epoch hold, and with honour left on the
    /// held lease.
    fn may_wait_unresolved(&self, now: Now) -> bool {
        !self.job_is_stop_flush()
            && !self.lease.epoch_held()
            && self
                .lease
                .honour_left_ms(now, &self.cfg)
                .is_some_and(|left| left > self.cfg.expiry_margin_ms as i64)
    }

    fn enter_release_unresolved(&mut self, now: Now, mine: &Lease, out: &mut Vec<Action>) {
        self.stats.release_unresolved += 1;
        // The job holds no own read now; `lease.held`, `releasing`, the
        // parks, delegations and the inbox are not touched.
        self.set_release_reread_phase(None);
        self.release_unresolved = Some(ReleaseUnresolved {
            mine: (mine.epoch, mine.expires_unix_ms, mine.holder_expires_ms),
            since: now.wait,
            tries: 0,
            timer: None,
            probes: BTreeMap::new(),
        });
        self.send_release_probes(now, out);
        self.arm_release_reread(now, out);
    }

    /// Drop the unresolved state: its timer, and its probes (a late reply
    /// is then ignored).
    fn end_release_unresolved(&mut self, out: &mut Vec<Action>) {
        if let Some(st) = self.release_unresolved.take() {
            if let Some(id) = st.timer {
                self.cancel_timer(id, out);
            }
        }
    }

    fn arm_release_reread(&mut self, now: Now, out: &mut Vec<Action>) {
        let Some(st) = self.release_unresolved.as_ref() else {
            return;
        };
        let mut delay = crate::deadlines::release_reread_retry_ms(&self.cfg, st.tries);
        // Fire no later than the bound the wait ends at.
        if let Some(left) = self.lease.honour_left_ms(now, &self.cfg) {
            let to_bound = (left - self.cfg.expiry_margin_ms as i64).max(1) as u64;
            delay = delay.min(to_bound);
        }
        let id = self.set_timer_wait(now.wait.plus(delay), Timer::ReleaseReread, out);
        if let Some(st) = self.release_unresolved.as_mut() {
            st.timer = Some(id);
        }
    }

    /// Ask every reachable roster peer that has no probe outstanding to read
    /// the lease. A probe unanswered for a forward timeout is dead (as in
    /// `settle_probe`) and asked again.
    fn send_release_probes(&mut self, now: Now, out: &mut Vec<Action>) {
        let timeout = self.cfg.forward_timeout_ms as i64;
        let me = self.cfg.node_id;
        let Some(st) = self.release_unresolved.as_mut() else {
            return;
        };
        st.probes
            .retain(|_, (_, sent)| now.honour.since(*sent) < timeout);
        let asked: Vec<NodeId> = st.probes.values().map(|(n, _)| *n).collect();
        let peers: Vec<NodeId> = self.roster.links().keys().copied().collect();
        for node in peers {
            if node == me || asked.contains(&node) || !self.reaches(now, node) {
                continue;
            }
            let req = self.op_id();
            if let Some(st) = self.release_unresolved.as_mut() {
                st.probes.insert(req, (node, now.honour));
            }
            out.push(Action::Send {
                to: node,
                msg: PeerMsg::LeaseProbe { req },
            });
        }
    }

    /// A shutdown arrived while the release waited: give the lease up at
    /// once, as a shutdown flush does when its own re-read fails.
    pub(super) fn give_up_unresolved_release(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(kind) = self.job.as_ref().map(|j| j.kind()) else {
            self.end_release_unresolved(out);
            return;
        };
        self.end_release_unresolved(out);
        self.stats.release_unresolved_gave_up += 1;
        match self.lease.held.clone() {
            Some((mine, _)) => {
                self.give_up_release_in_doubt(now, &mine, replica, out);
                self.release_job_tail(now, kind, replica, out);
            }
            None => {
                self.lease.releasing = false;
                self.lease_gone_mid_job(now, kind, replica, out);
            }
        }
    }

    /// `Timer::ReleaseReread`.
    pub(crate) fn on_release_reread_timer(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(st) = self.release_unresolved.as_mut() else {
            return;
        };
        st.timer = None;
        let Some(kind) = self.job.as_ref().map(|j| j.kind()) else {
            self.end_release_unresolved(out);
            return;
        };
        let Some((mine, _)) = self.lease.held.clone() else {
            // Deposed (or the lease dropped) while it waited.
            self.end_release_unresolved(out);
            self.lease.releasing = false;
            self.lease_gone_mid_job(now, kind, replica, out);
            return;
        };
        let honour_left = self.lease.honour_left_ms(now, &self.cfg);
        if honour_left.is_none_or(|left| left <= self.cfg.expiry_margin_ms as i64) {
            // Nothing resolved it by the lease's honour end: the old give-up.
            tracing::warn!(
                node = self.cfg.node_id,
                waited_ms = self.release_unresolved.as_ref().map(|st| now.wait.since(st.since)),
                "no read resolved a release in doubt before the lease's honour ended; giving the lease up"
            );
            self.end_release_unresolved(out);
            self.stats.release_unresolved_gave_up += 1;
            self.give_up_release_in_doubt(now, &mine, replica, out);
            self.release_job_tail(now, kind, replica, out);
            return;
        }
        if self.job_has_no_op() {
            let op = self.issue_s3(S3Op::LeaseGet, S3For::Job, out);
            self.set_release_reread_phase(Some(op));
        }
        self.send_release_probes(now, out);
        if let Some(st) = self.release_unresolved.as_mut() {
            st.tries = st.tries.saturating_add(1);
        }
        self.arm_release_reread(now, out);
    }

    /// A `LeaseProbeReply` from `from`.
    pub(crate) fn on_lease_probe_reply(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        read: ProbedLease,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(st) = self.release_unresolved.as_mut() else {
            return;
        };
        match st.probes.get(&req) {
            Some((node, _)) if *node == from => {}
            _ => return,
        }
        let Some((_, sent)) = st.probes.remove(&req) else {
            return;
        };
        let object = match read {
            ProbedLease::Failed => return,
            ProbedLease::Absent => None,
            ProbedLease::Object(lease) => Some(lease),
        };
        let Some(kind) = self.job.as_ref().map(|j| j.kind()) else {
            self.end_release_unresolved(out);
            return;
        };
        self.resolve_release_doubt(now, kind, DoubtRead::Peer { object, sent }, replica, out);
    }

    /// A probe that could not be sent (or whose answer never came): the next
    /// look asks that peer again.
    pub(crate) fn on_release_probe_failed(&mut self, req: OpId) -> bool {
        self.release_unresolved
            .as_mut()
            .is_some_and(|st| st.probes.remove(&req).is_some())
    }

    /// The decision table for a read of the lease while a release is in
    /// doubt: the node's own re-read (first or retried) and a peer's.
    ///
    /// | read | decision |
    /// |---|---|
    /// | this node, the held epoch, released | the release landed |
    /// | this node, the held epoch, standing, own read | kept (`renewed`) |
    /// | the same, a peer's read with the held expiry fields | kept; the held (lease, tag) stays |
    /// | the same, a peer's read with other expiry fields | inconclusive |
    /// | this node, an older epoch, a peer's read | inconclusive |
    /// | another holder, a later epoch, or no object | deposed |
    pub(crate) fn resolve_release_doubt(
        &mut self,
        now: Now,
        kind: JobKind,
        read: DoubtRead,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some((mine, _)) = self.lease.held.clone() else {
            self.end_release_unresolved(out);
            self.lease.releasing = false;
            self.lease_gone_mid_job(now, kind, replica, out);
            return;
        };
        let me = self.cfg.node_id;
        let waited_ms = self
            .release_unresolved
            .as_ref()
            .map(|st| now.wait.since(st.since));
        let held_key = self
            .release_unresolved
            .as_ref()
            .map(|st| st.mine)
            .unwrap_or((mine.epoch, mine.expires_unix_ms, mine.holder_expires_ms));
        let decision = match read {
            DoubtRead::Own(Some((cur, _)))
                if cur.holder == me && cur.epoch == mine.epoch && cur.released =>
            {
                Decision::Landed
            }
            DoubtRead::Own(Some((cur, tag))) if cur.holder == me && cur.epoch == mine.epoch => {
                Decision::KeptOwn(cur, tag)
            }
            DoubtRead::Own(Some((cur, _))) => Decision::Deposed(cur.holder, cur.epoch),
            DoubtRead::Own(None) => Decision::Deposed(0, 0),
            DoubtRead::Peer {
                object: Some(cur), ..
            } if cur.holder == me && cur.epoch == mine.epoch && cur.released => Decision::Landed,
            DoubtRead::Peer {
                object: Some(cur),
                sent,
            } if cur.holder == me && cur.epoch == mine.epoch => {
                if (cur.epoch, cur.expires_unix_ms, cur.holder_expires_ms) == held_key {
                    Decision::KeptByPeer(cur, sent)
                } else {
                    self.stats.release_unresolved_inconclusive += 1;
                    Decision::Inconclusive
                }
            }
            // An older read proves nothing (`root_probe::judge`).
            DoubtRead::Peer {
                object: Some(cur), ..
            } if cur.holder == me && cur.epoch < mine.epoch => {
                self.stats.release_unresolved_inconclusive += 1;
                Decision::Inconclusive
            }
            DoubtRead::Peer {
                object: Some(cur), ..
            } => Decision::Deposed(cur.holder, cur.epoch),
            DoubtRead::Peer { object: None, .. } => Decision::Deposed(0, 0),
        };
        if matches!(decision, Decision::Inconclusive) {
            return;
        }
        // Conclusive: whatever follows acts on a node that is no longer
        // waiting.
        self.end_release_unresolved(out);
        match decision {
            Decision::Landed => {
                self.release_landed(now, kind, replica, out);
                return;
            }
            Decision::KeptOwn(cur, tag) => self.lease.renewed(now, cur, tag),
            Decision::KeptByPeer(cur, sent) => {
                self.stats.release_unresolved_kept_by_peer += 1;
                tracing::info!(
                    node = self.cfg.node_id,
                    epoch = cur.epoch,
                    ?waited_ms,
                    "a peer's read shows the lease standing: the release did not happen"
                );
                // The held (lease, tag) is unchanged. The read proves the
                // tenure live as of the probe's send, never its arrival.
                self.lease.note_object(&cur);
                if sent > self.ack.last_s3_fresh {
                    self.ack.last_s3_fresh = sent;
                }
            }
            Decision::Deposed(holder, epoch) => {
                self.deposed(now, holder, epoch, mine.epoch, replica, out)
            }
            Decision::Inconclusive => return,
        }
        self.release_job_tail(now, kind, replica, out);
    }

    /// The release in doubt is given up: it may or may not have landed and
    /// no read said which. Had it not, the object names this node,
    /// unreleased, until it expires; the next acquisition re-adopts it
    /// through the gate.
    pub(super) fn give_up_release_in_doubt(
        &mut self,
        now: Now,
        mine: &Lease,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let carried_mine = self.epoch_carries(mine);
        self.pr.released = Some((mine.epoch, mine.expires_unix_ms));
        self.pr.given_up = self.pr.released;
        self.lease.released();
        self.owe_given_up_move(carried_mine, replica);
        self.drop_unexecuted_parks(now, replica, out);
        self.deleg_on_lease_gone(now, replica, out);
        self.clear_holder_epoch(replica);
        self.inbox.holder = None;
        if self.epoch_refuses_writes() {
            // Nobody holds the epoch now: what waits for the lease hears
            // `EROFS` at once, not at its deadline.
            self.refuse_waiting_for_lease(now, Code::ReadOnly, replica, out);
        }
    }

    /// Peer side of [`PeerMsg::LeaseProbe`]: a new read of the lease object,
    /// issued after the probe arrived. Never answered from `last_seen` or a
    /// read already in flight: the read must be causally after the probing
    /// node's failed CAS.
    pub(crate) fn on_lease_probe(&mut self, from: NodeId, req: OpId, out: &mut Vec<Action>) {
        self.issue_s3(S3Op::LeaseGet, S3For::LeaseProbe { from, req }, out);
    }

    /// The fresh read of a probe: answered to the prober, with no other
    /// effect on this node.
    pub(crate) fn on_lease_probe_read(
        &mut self,
        from: NodeId,
        req: OpId,
        result: S3Result,
        out: &mut Vec<Action>,
    ) {
        self.stats.lease_probes_answered += 1;
        let read = match result {
            S3Result::LeaseGet(Ok(Some((lease, _)))) => ProbedLease::Object(lease),
            S3Result::LeaseGet(Ok(None)) => ProbedLease::Absent,
            _ => ProbedLease::Failed,
        };
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::LeaseProbeReply { req, read },
        });
    }
}
