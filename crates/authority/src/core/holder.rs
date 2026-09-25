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
use constellation_meta::delegation::Ownership;
use constellation_meta::{MetaError, MutateOp, MutateOutcome, Position, Rid, TouchSet};

/// The keys `op` reads or writes, before it runs (a refusal touches
/// nothing but is evaluated against them), as the replica's unshipped
/// set keeps them.
/// Whether two touch sets share a dentry or an inode.
pub(crate) fn touch_sets_overlap(a: &TouchSet, b: &TouchSet) -> bool {
    a.overlaps(b)
}

/// The keys `op` reads or writes, before it runs (plan 30 §M12: a
/// dentry plus a *shared* hold on its directory for a create, unlink,
/// link or rename; an *exclusive* hold on an inode whose own attributes
/// change).
pub(crate) fn keys_of_op(op: &MutateOp) -> TouchSet {
    TouchSet::from_op(op)
}

/// [`keys_of_op`] plus the inodes the op takes away or moves, looked up
/// on `replica` (plan 30 §M12: an rmdir or a rename holds the directory
/// it removes or moves *exclusively* — against the creates inside it,
/// and against a delegation of it).
pub(crate) fn keys_of_op_in(op: &MutateOp, replica: &dyn Replica) -> TouchSet {
    TouchSet::from_op_in(op, &|p, n| replica.lookup_ino(p, n))
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
        deps: Position,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        replica.forget_acked_through(rid.node, rid.incarnation, acked_through);
        self.note_foreign(now, replica, out);
        if self.cfg.placement && self.cfg.delegation && self.root_usable(now) {
            let dirs = Core::dirs_of_keys(&keys_of_op(&op), replica);
            self.place_note(from, dirs);
        }
        // Plan 30 §M8: a retry of an op whose reply waits for recalls
        // re-attaches to that wait (its outcome is final; only the
        // acknowledgement is held).
        if req != OpId(0) && self.reattach_parked(now, from, req, rid, out) {
            return;
        }
        // Plan 30 §M11: a delegate executes what is its own; the root
        // recalls what a live delegation owns before it executes, and
        // redirects what is wholly a delegate's.
        if self.cfg.delegation
            && self.delegate_try_execute(
                now,
                from,
                Some(req),
                rid,
                &op,
                deps,
                acked_through,
                replica,
                out,
            )
        {
            return;
        }
        // Plan 30 §M12: the table names this node the owner (a range or
        // a subtree just delegated) but the grant is not installed yet:
        // the requester retries here in a moment rather than bouncing
        // between the root and this node until its redirects run out.
        if self.cfg.delegation && req != OpId(0) && self.lease.ship_epoch(now, &self.cfg).is_none()
        {
            if let Ownership::Delegated(d) = replica.resolve_ownership(&keys_of_op_in(&op, replica))
            {
                if d.node == self.cfg.node_id
                    && !self.dl.mine.get(&d.gen).is_some_and(|m| m.stopped)
                {
                    self.stats.deleg_not_owner += 1;
                    out.push(Action::Send {
                        to: from,
                        msg: PeerMsg::MutateReply {
                            req,
                            outcome: MutateOutcome::Held {
                                retry_ms: self.cfg.delegation_stream_tick_ms.max(10),
                            },
                            base: None,
                            position: Position::ZERO,
                            gen: 0,
                        },
                    });
                    return;
                }
            }
        }
        let mut base = None;
        let mut position = Position::ZERO;
        let mut fresh = None;
        let outcome = if self.lease.fenced() {
            MutateOutcome::Busy
        } else if let Some(epoch) = self.lease.ship_epoch(now, &self.cfg) {
            // Deliberately `ship_epoch`, not `new_mutation_epoch`: the
            // handoff pause closes this node's *own* new writes so a
            // waiter can claim, not a peer's forwarded ones.
            if self.cfg.delegation && !self.dl.gens.is_empty() {
                let keys = keys_of_op_in(&op, replica);
                if let Ownership::Delegated(d) = replica.resolve_ownership(&keys) {
                    if d.node != self.cfg.node_id
                        && self
                            .dl
                            .gens
                            .get(&d.gen)
                            .is_some_and(|g| !g.ended && now < g.until)
                        && self.links.get(&d.node).is_some_and(|l| l.connected)
                    {
                        // Wholly a live delegate's: the requester's table
                        // was stale; send it there. (Phase 2b: only while
                        // the delegate renews and the root reaches it; a
                        // silent one is recalled — or, for a designation,
                        // the op refused — instead of bouncing the
                        // requester between the two.) The delegate's own
                        // op, sent before its table carried the grant, is
                        // held for a retry instead of a redirect to itself.
                        self.stats.deleg_not_owner += 1;
                        let outcome = if from == d.node {
                            MutateOutcome::Held {
                                retry_ms: self.cfg.delegation_stream_tick_ms.max(10),
                            }
                        } else {
                            MutateOutcome::NotHolder { holder: d.node }
                        };
                        out.push(Action::Send {
                            to: from,
                            msg: PeerMsg::MutateReply {
                                req,
                                outcome,
                                base: None,
                                position: Position::ZERO,
                                gen: 0,
                            },
                        });
                        return;
                    }
                }
                let wait = match self.deleg_recall_plan(now, &keys, replica, out) {
                    super::delegate::RecallPlan::None => Default::default(),
                    super::delegate::RecallPlan::Wait(w) => w,
                    super::delegate::RecallPlan::Refuse(errno) => {
                        if req != OpId(0) {
                            out.push(Action::Send {
                                to: from,
                                msg: PeerMsg::MutateReply {
                                    req,
                                    outcome: MutateOutcome::Errno(errno),
                                    base: None,
                                    position: Position::ZERO,
                                    gen: 0,
                                },
                            });
                        }
                        return;
                    }
                };
                let deps_wait = (!replica.reaches_streams(&deps)).then_some(deps);
                if !wait.is_empty() || deps_wait.is_some() {
                    if deps_wait.is_some() {
                        self.stats.deleg_deps_waits += 1;
                    }
                    if req != OpId(0) {
                        self.park_exec_reply(
                            now,
                            wait,
                            deps_wait,
                            from,
                            req,
                            rid,
                            op,
                            acked_through,
                            out,
                        );
                    }
                    return;
                }
            } else if self.cfg.delegation && !replica.reaches_streams(&deps) {
                self.stats.deleg_deps_waits += 1;
                if req != OpId(0) {
                    self.park_exec_reply(
                        now,
                        Default::default(),
                        Some(deps),
                        from,
                        req,
                        rid,
                        op,
                        acked_through,
                        out,
                    );
                }
                return;
            }
            base = self.reply_base(&op, replica);
            let (outcome, executed) = self.holder_execute(now, epoch, rid, &op, replica, out);
            fresh = executed;
            // Plan 30 §M6: the state the op was evaluated against, its
            // own rows included — everything shipped through `head_seq`,
            // plus the unshipped journal through its last row. Read right
            // after the execution; a local write committing in between
            // only makes it larger.
            position = Position {
                seq: self.ship.head_seq,
                pending: replica.journal_position(epoch),
                streams: Default::default(),
            };
            outcome
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
        // Plan 30 §M8: the op took effect here just now; its reply may
        // leave only once no other node still honours a read delegation
        // on what it touched. The requester's own delegation is not
        // recalled: its reads of its own write are read-your-writes (M6).
        // Plan 30 §M9: and, accepted or refused, only once the journal
        // position it was evaluated at is durable under the lease's
        // acknowledgement policy.
        let wait = match fresh {
            Some(inos) => self.recall_needed(now, &inos, Some(from), replica, out),
            None => None,
        };
        let durable = self.ack_need(&position);
        if wait.is_some() || durable.is_some() {
            self.park_reply(
                now,
                wait.unwrap_or_default(),
                durable,
                from,
                req,
                rid,
                outcome,
                base,
                position,
                0,
                out,
            );
            return;
        }
        if req == OpId(0) {
            // A parked execution whose requester was answered `Held`: its
            // retry is answered from the dedup.
            return;
        }
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::MutateReply {
                req,
                outcome,
                base,
                position,
                gen: 0,
            },
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
    pub(crate) fn reply_base(&self, op: &MutateOp, replica: &dyn Replica) -> Option<Seq> {
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
    /// to install the existing entry. The second value is set when the op
    /// executed just now: the inodes its records touched (plan 30 §M8's
    /// recall set).
    pub(crate) fn holder_execute(
        &mut self,
        now: Ms,
        epoch: Epoch,
        rid: Rid,
        op: &MutateOp,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> (MutateOutcome, Option<Vec<u64>>) {
        if let Some(records) = replica.recent_outcome(rid) {
            self.stats.forward_dedup_hits += 1;
            return (MutateOutcome::Accepted { epoch, records }, None);
        }
        match replica.completed_outcome(rid).ok().flatten() {
            Some(constellation_meta::CompletedOutcome::Executed { .. }) => {
                self.stats.forward_dedup_hits += 1;
                return (
                    MutateOutcome::Accepted {
                        epoch,
                        records: Vec::new(),
                    },
                    None,
                );
            }
            Some(constellation_meta::CompletedOutcome::Refused { errno }) => {
                self.stats.forward_dedup_hits += 1;
                return (
                    if errno == libc::ESTALE {
                        MutateOutcome::Conflict { manifest: None }
                    } else {
                        MutateOutcome::Errno(errno)
                    },
                    None,
                );
            }
            None => {}
        }
        tracing::debug!(
            node = self.cfg.node_id,
            ?rid,
            rseq = rid.seq,
            "holder: executing a forwarded op"
        );
        let outcome = match replica.execute(op, Some(rid)) {
            Ok(records) => {
                replica.remember_outcome(rid, &records);
                self.lease.touch(now);
                self.nudge(now, out);
                let inos = constellation_meta::recall_inos(&records);
                return (MutateOutcome::Accepted { epoch, records }, Some(inos));
            }
            Err(MetaError::Conflict) => match op {
                MutateOp::SetManifest { ino, .. } => MutateOutcome::Conflict {
                    manifest: replica.manifest(*ino),
                },
                _ => MutateOutcome::Errno(libc::EAGAIN),
            },
            Err(MetaError::Exists) => {
                self.record_refusal(rid, libc::EEXIST, replica);
                match named_child(op) {
                    Some((parent, name)) => match replica.entry_as_record(parent, name) {
                        Some(record) => MutateOutcome::Exists {
                            records: vec![record],
                            epoch,
                        },
                        None => MutateOutcome::Errno(libc::EEXIST),
                    },
                    None => MutateOutcome::Errno(libc::EEXIST),
                }
            }
            Err(e) => {
                let errno = meta_errno(&e);
                self.record_refusal(rid, errno, replica);
                MutateOutcome::Errno(errno)
            }
        };
        (outcome, None)
    }

    /// Plan 30 §M9: a definitive refusal of an op executed by rid is an
    /// outcome, journaled as `Refused { rid, errno }` so that any second
    /// execution of the rid — the requester's retry after a `Busy` or a
    /// lost reply, the deposed holder's replay by rid, an inbox batch
    /// drained later — dedups to the same errno rather than re-evaluating
    /// the op against a state that may have changed meanwhile. (A
    /// transient refusal — `Conflict`/`EAGAIN`, a stale manifest base —
    /// is not an outcome: the requester rebases and retries.) The row is
    /// unshipped journal work like any other: the reply's position
    /// carries it, and under `Backup`/`S3` the acknowledgement waits for
    /// it.
    pub(crate) fn record_refusal(&mut self, rid: Rid, errno: i32, replica: &dyn Replica) {
        if let Err(error) = replica.journal_refusal(rid, errno) {
            tracing::warn!(node = self.cfg.node_id, ?rid, errno, %error, "could not journal a refusal");
            return;
        }
        self.stats.refusals_journaled += 1;
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
        self.note_foreign(now, replica, out);
        // Plan 30 §M11: no handoff while a delegation is live (the root
        // stays; see `round_release`).
        let can_serve = self.lease.ship_epoch(now, &self.cfg).is_some()
            && !self.lease.fenced()
            && self.deleg_live_generations() == 0;
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
            // Plan 30 §M10 (found by the M10 simulation's first epoch
            // runs, pre-M10 behaviour): a hold with an unshipped journal is
            // not handed over. Nothing ships during an epoch, so the
            // successor would execute on a replica missing this journal,
            // and the two journals would ship in either order after the
            // epoch (acknowledged effects re-evaluated and reordered). The
            // requester forwards to this node instead; its file writes'
            // manifests go forwarded too, their chunks uploaded when S3
            // returns (`fusefs::commit_manifest_forwarded`).
            if replica.journal_len().unwrap_or(1) > 0 {
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
            let epoch = self.lease.epoch().unwrap_or(1);
            self.lease.release_local();
            self.deleg_on_lease_gone(now, replica, out);
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

    /// A gossiped "segment `seq` landed" hint (plan 30 §M7: hints carry
    /// no payload). The stream this node follows delivers it; otherwise
    /// a round tails it now rather than at the next poll.
    pub(crate) fn on_segment_pushed(
        &mut self,
        now: Ms,
        from: NodeId,
        seq: Seq,
        epoch: Epoch,
        out: &mut Vec<Action>,
    ) {
        if from != 0 && epoch >= self.ship.max_epoch {
            self.lease.cached_holder = Some(from);
        }
        if seq < self.ship.next_seq || self.stream_delivers_from(from) {
            return;
        }
        self.stream_note_hint(seq);
        self.nudge(now, out);
    }
}
