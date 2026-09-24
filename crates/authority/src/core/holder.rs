//! The holder's side of forwarding and handoff: a peer's
//! `MutateRequest` (what `node_runtime::dispatch_mutate` + `forward::
//! holder_execute` did), a peer's `LeaseRequest` (`SyncRequest::HandOff`),
//! and a pushed segment (`SyncRequest::ApplyPushed`).

use super::client::{meta_errno, named_child};
use super::{Core, S3For};
use crate::action::{Action, S3Op};
use crate::event::PeerMsg;
use crate::ids::{Epoch, Ms, NodeId, OpId, Seq};
use crate::replica::Replica;
use constellation_meta::{MetaError, MutateOp, MutateOutcome, Rid, TouchSet};

/// The keys `op` reads or writes, before it runs (a refusal touches
/// nothing but is evaluated against them), as the replica's unshipped
/// set keeps them.
/// Whether two touch sets share a dentry or an inode.
pub(crate) fn touch_sets_overlap(a: &TouchSet, b: &TouchSet) -> bool {
    a.dentries.iter().any(|d| b.dentries.contains(d)) || a.inos.iter().any(|i| b.inos.contains(i))
}

pub(crate) fn keys_of_op(op: &MutateOp) -> TouchSet {
    let mut set = TouchSet::default();
    let mut dentry = |p: u64, n: &str| {
        set.dentries.insert((p, n.to_string()));
    };
    match op {
        MutateOp::Mkdir { parent, name, .. }
        | MutateOp::Create { parent, name, .. }
        | MutateOp::Symlink { parent, name, .. }
        | MutateOp::Mknod { parent, name, .. }
        | MutateOp::Unlink { parent, name }
        | MutateOp::Rmdir { parent, name } => dentry(*parent, name),
        MutateOp::Link { ino, parent, name } => {
            dentry(*parent, name);
            set.inos.insert(*ino);
        }
        MutateOp::Rename {
            parent,
            name,
            new_parent,
            new_name,
        } => {
            dentry(*parent, name);
            dentry(*new_parent, new_name);
        }
        MutateOp::Setattr { ino, .. }
        | MutateOp::SetManifest { ino, .. }
        | MutateOp::SetXattr { ino, .. }
        | MutateOp::RemoveXattr { ino, .. } => {
            set.inos.insert(*ino);
        }
        MutateOp::Publish {
            ino, parent, name, ..
        } => {
            dentry(*parent, name);
            set.inos.insert(*ino);
        }
        MutateOp::AtimeBatch { entries } => {
            for (i, _, _) in entries {
                set.inos.insert(*i);
            }
        }
        MutateOp::Records { records } => {
            set = TouchSet::from_records(records.iter());
        }
    }
    set
}

impl Core {
    /// `dispatch_mutate`: execute a peer's forwarded op if this node holds
    /// the lease and its view is not fenced; answer `Busy` inside a
    /// release/handoff/gate window, `NotHolder` otherwise.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_mutate_request(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        rid: Rid,
        op: MutateOp,
        acked_through: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        replica.forget_acked_through(rid.node, rid.incarnation, acked_through);
        let mut base = None;
        let outcome = if self.lease.fenced() {
            MutateOutcome::Busy
        } else if let Some(epoch) = self.lease.ship_epoch(now, &self.cfg) {
            // Deliberately `ship_epoch`, not `new_mutation_epoch`: the
            // handoff pause closes this node's *own* new writes so a
            // waiter can claim, not a peer's forwarded ones.
            base = self.reply_base(&op, replica);
            self.holder_execute(now, epoch, rid, &op, replica, out)
        } else if self.lease.lost {
            MutateOutcome::Busy
        } else {
            let holder = self.lease.cached_holder.unwrap_or(0);
            if holder == 0 || holder == self.cfg.node_id {
                self.issue_s3(S3Op::LeaseGet, S3For::RefreshHolder, out);
            }
            MutateOutcome::NotHolder {
                holder: if holder == self.cfg.node_id {
                    0
                } else {
                    holder
                },
            }
        };
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::MutateReply { req, outcome, base },
        });
    }

    /// The base a requester needs for this op's reply (see
    /// `PeerMsg::MutateReply::base`): `None` when the unshipped journal
    /// already touched one of the op's keys (only the log can order the
    /// reply); otherwise the last *shipped* position that touched one of
    /// them, or the window's floor when none did within it
    /// (`Core::shipped_touches`) — never the head itself, which under
    /// load every requester trails and which would send every reply to
    /// `AwaitingLog` (M5 round 3: `holder-ships-under-forward-load` at 8×
    /// main's time).
    fn reply_base(&self, op: &MutateOp, replica: &dyn Replica) -> Option<Seq> {
        let keys = keys_of_op(op);
        if replica.unshipped_overlaps(&keys) {
            return None;
        }
        let touched = self
            .shipped_touches
            .iter()
            .rev()
            .find(|(_, touches)| touch_sets_overlap(touches, &keys))
            .map(|(seq, _)| *seq);
        Some(touched.unwrap_or(self.shipped_floor))
    }

    /// `forward::holder_execute`: dedup by `recent`, then by `completed`,
    /// then execute; refusals carry what the requester needs to rebase or
    /// to install the existing entry.
    pub(crate) fn holder_execute(
        &mut self,
        now: Ms,
        epoch: Epoch,
        rid: Rid,
        op: &MutateOp,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> MutateOutcome {
        if let Some(records) = replica.recent_outcome(rid) {
            self.stats.forward_dedup_hits += 1;
            return MutateOutcome::Accepted { epoch, records };
        }
        match replica.completed_outcome(rid).ok().flatten() {
            Some(constellation_meta::CompletedOutcome::Executed { .. }) => {
                self.stats.forward_dedup_hits += 1;
                return MutateOutcome::Accepted {
                    epoch,
                    records: Vec::new(),
                };
            }
            Some(constellation_meta::CompletedOutcome::Refused { errno }) => {
                self.stats.forward_dedup_hits += 1;
                return if errno == libc::ESTALE {
                    MutateOutcome::Conflict { manifest: None }
                } else {
                    MutateOutcome::Errno(errno)
                };
            }
            None => {}
        }
        match replica.execute(op, Some(rid)) {
            Ok(records) => {
                replica.remember_outcome(rid, &records);
                self.lease.touch(now);
                self.nudge(now, out);
                MutateOutcome::Accepted { epoch, records }
            }
            Err(MetaError::Conflict) => match op {
                MutateOp::SetManifest { ino, .. } => MutateOutcome::Conflict {
                    manifest: replica.manifest(*ino),
                },
                _ => MutateOutcome::Errno(libc::EAGAIN),
            },
            Err(MetaError::Exists) => match named_child(op) {
                Some((parent, name)) => match replica.entry_as_record(parent, name) {
                    Some(record) => MutateOutcome::Exists {
                        records: vec![record],
                        ship_floor: self.ship_floor(),
                        epoch,
                    },
                    None => MutateOutcome::Errno(libc::EEXIST),
                },
                None => MutateOutcome::Errno(libc::EEXIST),
            },
            Err(e) => MutateOutcome::Errno(meta_errno(&e)),
        }
    }

    /// `SyncRequest::HandOff`: a peer wants the lease. If this node holds
    /// it (view not fenced, no gate pending), flush and release through
    /// the handoff job; in an active continuation epoch the hold is
    /// simply let go (nothing ships during an epoch); otherwise decline —
    /// the requester waits the lease out through S3, which is always
    /// correct.
    pub(crate) fn on_lease_request(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let can_serve = self.lease.ship_epoch(now, &self.cfg).is_some() && !self.lease.fenced();
        if !can_serve {
            self.stats.handoffs_declined += 1;
            out.push(Action::Send {
                to: from,
                msg: PeerMsg::LeaseHandoff {
                    req,
                    released: false,
                    epoch: self.lease.epoch().unwrap_or(0),
                    head_seq: None,
                },
            });
            return;
        }
        if self.epoch.active && !self.epoch.frozen && self.lease.epoch_held() {
            let epoch = self.lease.epoch().unwrap_or(1);
            self.lease.release_local();
            replica.set_holder_epoch(0);
            self.stats.handoffs_served += 1;
            out.push(Action::Send {
                to: from,
                msg: PeerMsg::LeaseHandoff {
                    req,
                    released: true,
                    epoch,
                    head_seq: Some(self.ship.head_seq),
                },
            });
            return;
        }
        self.enqueue_job(
            now,
            super::jobs::JobReq::Handoff { req, from },
            replica,
            out,
        );
    }

    /// `SyncRequest::ApplyPushed`: a gossiped segment. Applied at once when
    /// it is exactly the next one and carries its payload; otherwise a
    /// round is nudged to tail it.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_segment_pushed(
        &mut self,
        now: Ms,
        from: NodeId,
        seq: Seq,
        epoch: Epoch,
        payload: Option<Vec<u8>>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if from != 0 && epoch >= self.ship.max_epoch {
            self.lease.cached_holder = Some(from);
        }
        if seq < self.ship.next_seq {
            return;
        }
        // A job mid-tail or mid-ship owns the cursor; let it find the
        // segment (its next probe or its CAS collision will).
        if seq == self.ship.next_seq && self.job.is_none() {
            if let Some(payload) = payload {
                match self.apply_incoming(now, seq, &payload, replica, out) {
                    Ok(()) => {
                        self.stats.segments_applied += 1;
                        self.stats.pushed_applied += 1;
                        self.answer_awaiting_log(now, replica, out);
                        return;
                    }
                    Err(error) => {
                        tracing::warn!(%error, node = self.cfg.node_id, seq, "pushed segment not applied")
                    }
                }
            }
        }
        self.nudge(now, out);
    }
}
