//! The holder's side of forwarding and handoff: a peer's
//! `MutateRequest` (what `node_runtime::dispatch_mutate` + `forward::
//! holder_execute` did), a peer's `LeaseRequest` (`SyncRequest::HandOff`),
//! and a pushed segment (`SyncRequest::ApplyPushed`).

use super::client::named_child;
use super::{Core, S3For};
use crate::action::{Action, S3Op};
use crate::event::PeerMsg;
use crate::ids::{Epoch, Ms, NodeId, OpId, Seq};
use crate::replica::Replica;
use constellation_meta::delegation::Ownership;
use constellation_meta::locks::LockTag;
use constellation_meta::{
    LogRecord, MetaError, MutateOp, MutateOutcome, OwnChunks, Position, RemoteBlockers, Rid,
    TouchSet,
};
use constellation_types::Code;

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
        (deps, applied): (Position, Seq),
        tag: LockTag,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        replica.forget_acked_through(rid.node, rid.incarnation, acked_through);
        self.note_foreign(now, replica, out);
        if from != self.cfg.node_id {
            self.note_demand(now, from, true);
        }
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
                &tag,
                replica,
                out,
            )
        {
            return;
        }
        // A dependency its generation ended without (see
        // `Replica::deps_lost`): not executed here; the requester re-sends
        // once its own replay of the cause has landed. A retry of an op
        // already done is answered from the dedup below as ever.
        if self.cfg.delegation
            && replica.deps_lost(&deps)
            && replica.recent_outcome(rid).is_none()
            && replica.completed_outcome(rid).ok().flatten().is_none()
        {
            self.stats.deps_lost_refused += 1;
            tracing::debug!(
                node = self.cfg.node_id,
                ?rid,
                ?deps,
                "an execution whose deps were lost with their generation is refused"
            );
            if req != OpId(0) {
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
                        own_chunks: OwnChunks::None,
                        own_rows: None,
                    },
                });
            }
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
                            own_chunks: OwnChunks::None,
                            own_rows: None,
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
            if !self.lease.releasing && req != OpId(0) && self.readopting() {
                // A-1: the gate of a re-adoption started for a forward;
                // the requester keeps retrying (a `Busy` would spend its
                // retries and send it to a lease path it may not have).
                self.stats.readopt_for_forward += 1;
                MutateOutcome::Held {
                    retry_ms: self.cfg.recall_hold_ms.max(50),
                }
            } else {
                MutateOutcome::Busy
            }
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
                                own_chunks: OwnChunks::None,
                                own_rows: None,
                            },
                        });
                        return;
                    }
                }
                let wait = match self.deleg_recall_plan(now, &keys, replica, out) {
                    super::delegate::RecallPlan::None => Default::default(),
                    super::delegate::RecallPlan::Wait(w) => w,
                    super::delegate::RecallPlan::Refuse(code) => {
                        if req != OpId(0) {
                            out.push(Action::Send {
                                to: from,
                                msg: PeerMsg::MutateReply {
                                    req,
                                    outcome: MutateOutcome::Errno(code),
                                    base: None,
                                    position: Position::ZERO,
                                    gen: 0,
                                    own_chunks: OwnChunks::None,
                                    own_rows: None,
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
                            tag,
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
                        tag,
                        out,
                    );
                }
                return;
            }
            base = self.reply_base(&op, replica);
            let (outcome, executed) = self.holder_execute(now, epoch, rid, &op, &tag, replica, out);
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
        } else if req != OpId(0) && self.readopt_own_lease_for_forward(now, replica, out) {
            // EC2 campaign 8 A-1: the lease names this node — a previous
            // incarnation's, left behind by a crash — and nobody else will
            // take it before it expires (a TTL) unless a backup seals it.
            // `NotHolder { 0 }` sent the requester down its own lease path
            // (S3, and with its S3 cut, nowhere); this node re-adopts the
            // lease instead and the requester retries by rid meanwhile.
            self.stats.readopt_for_forward += 1;
            MutateOutcome::Held {
                retry_ms: self.cfg.recall_hold_ms.max(50),
            }
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
        let executed = fresh.is_some();
        let wait = match fresh {
            Some(inos) => self.recall_needed(now, &inos, Some(from), replica, out),
            None => None,
        };
        let durable = self.ack_need(&position);
        if wait.is_some() || durable.is_some() {
            // Only an acknowledgement's wait for durability says now what
            // it waits for (`held_for_upload`); the rest is worked out
            // when the reply leaves.
            let upto = position.pending.map(|p| p.jseq);
            let blockers = (durable.is_some() && req != OpId(0))
                .then(|| self.own_record_blockers(from, rid, &outcome, 0, upto, replica));
            let own_chunks = match &blockers {
                Some(b) => self.own_chunks_given(from, (0, &position), b.clone(), replica),
                None => OwnChunks::None,
            };
            let upload = matches!(own_chunks, OwnChunks::Upload(_));
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
            if let Some(b) = blockers {
                self.keep_parked_blockers(now, rid, b);
            }
            if durable.is_some() && upload && req != OpId(0) {
                self.held_for_upload(rid, own_chunks, out);
            }
            return;
        }
        if req == OpId(0) {
            // A parked execution whose requester was answered `Held`: its
            // retry is answered from the dedup.
            return;
        }
        // Chunk close-stall-followup: a requester that has the reply's
        // base installs an acceptance at once (it neither waits for its
        // own record nor observes the reply's position), so what those
        // wait for goes unread: skip the work. Not a refusal: it is
        // observed whatever the base. Chunk metered-own-rows: and only for
        // a fresh execution. An answer from the dedup (a retry after a
        // lost reply) may find its transaction shipped ahead of a
        // deferred close of the requester's, already applied there: the
        // requester then installs nothing and observes the position, and
        // reads its `own_chunks`.
        let installs = executed
            && matches!(outcome, MutateOutcome::Accepted { .. })
            && base.is_some_and(|b| applied >= b);
        let (own_chunks, own_rows) = if installs {
            (OwnChunks::None, None)
        } else {
            self.own_reply_parts(from, rid, &outcome, (0, &position), replica)
        };
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::MutateReply {
                req,
                outcome,
                base,
                position,
                gen: 0,
                own_chunks,
                own_rows,
            },
        });
    }

    /// EC2 campaign 8 A-1: a peer forwarded an op to this node, which does
    /// not hold the lease, while the lease object last read names this
    /// node, unreleased: a lease a previous incarnation held when it
    /// died (a restart with nothing of its own to ship does not acquire
    /// on its own). Re-adopt it — the acquisition claims it only if it
    /// still names this node, unreleased (`jobs::READOPT_REASON`) — and
    /// report whether that is under way. Never after this incarnation
    /// released or handed the lease off (the object says `released`),
    /// never while deposed, releasing, gated or in a continuation epoch,
    /// and not for a TTL after a re-adoption that did not land (then the
    /// requester is answered `NotHolder` and may take the lease itself).
    fn readopt_own_lease_for_forward(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        if !self.cfg.p2p
            || self.lease.held.is_some()
            || self.lease.gate.is_some()
            || self.lease.releasing
            || self.lease.lost
            || self.lease.epoch_held()
            || self.epoch.open
            || self.retired()
            || self.mode.forwards()
            || now < self.readopt_refused_until
        {
            return false;
        }
        let names_us = match &self.lease.last_seen {
            Some(lease) => lease.holder == self.cfg.node_id && !lease.released,
            None => self.lease.cached_holder == Some(self.cfg.node_id),
        };
        if !names_us {
            return false;
        }
        if self.acquiring() {
            return true;
        }
        tracing::info!(
            node = self.cfg.node_id,
            "a peer forwarded an op while the lease still names this node \
             (a previous incarnation's): re-adopting it"
        );
        self.enqueue_job(
            now,
            super::jobs::JobReq::Acquire {
                reason: super::jobs::READOPT_REASON,
                ask_handoff: false,
            },
            replica,
            out,
        );
        self.acquiring()
    }

    /// Chunk close-stall-metered: what `rid`'s transaction (`outcome`'s
    /// records, evaluated at `position`) waits for from `to`, the node
    /// that forwarded the op (`OwnChunks`, carried on the reply). Exact as
    /// of now: the chunks `to` forwarded as still pending and has not
    /// reported up that the transaction cannot leave this node before —
    /// its own manifests', and those of every unshipped transaction it
    /// depends on (`Meta::remote_blockers`: a `chmod` right after a `back`
    /// close of the same file waits for the close's chunks too). They
    /// reach `to` before those chunks are in S3 only through this node's
    /// pre-S3 stream, which carries a forwarder's own transactions past
    /// its pending chunks (`Core::stream_ahead`) but streams only rows a
    /// backup holds — a backed root, or a continuation epoch's hold owner
    /// — only to a subscriber, and only up to the first transaction naming
    /// a chunk that subscriber lacks (this node's own write-back, another
    /// node's forwarded close: `Core::stream_reaches`). Otherwise the ship
    /// plan defers them until `to`'s report — under `Local` with no backup
    /// (none in budget, the first seconds of a tenure,
    /// `CONSTELLATION_BACKUPS=0`), under `S3`, or behind a chunk the
    /// stream stops at — and `to`'s upload is one of the things the
    /// segment carrying them waits for: `Upload`. Whatever else it waits
    /// for (this node's own uploads, another forwarder's) comes with that
    /// node's own rounds. A delegate's execution (`gen != 0`) streams and
    /// backs up nothing naming a pending chunk (`Meta::delegate_txs_from`):
    /// only the upload releases it. A later change (a backup lost, or
    /// committed) is the requester's safety timer's to cover
    /// (`Core::await_own_records`).
    pub(crate) fn own_chunks_for(
        &self,
        to: NodeId,
        rid: Rid,
        outcome: &MutateOutcome,
        (gen, position): (u64, &Position),
        replica: &dyn Replica,
    ) -> OwnChunks {
        self.own_reply_parts(to, rid, outcome, (gen, position), replica)
            .0
    }

    /// [`Self::own_chunks_for`], and which of the unshipped transactions
    /// through the reply's position are `to`'s own (chunk
    /// metered-own-rows: `PeerMsg::MutateReply::own_rows`).
    pub(crate) fn own_reply_parts(
        &self,
        to: NodeId,
        rid: Rid,
        outcome: &MutateOutcome,
        (gen, position): (u64, &Position),
        replica: &dyn Replica,
    ) -> (OwnChunks, Option<constellation_meta::OwnRows>) {
        let upto = position.pending.map(|p| p.jseq);
        let mut blockers = self.own_record_blockers(to, rid, outcome, gen, upto, replica);
        let own = blockers.own.take();
        (
            self.own_chunks_given(to, (gen, position), blockers, replica),
            own,
        )
    }

    /// [`Self::own_chunks_for`]'s costly half: what the transaction waits
    /// for from `to` (`Meta::remote_blockers`), through journal seq `upto`
    /// (the reply's position). One point read when `to` has nothing
    /// pending here; otherwise the ship plan's walk of the unshipped
    /// journal, resumed from where the last answer to `to` left it
    /// (`held::blame_walk`), which a reply parked for its acknowledgement
    /// also keeps rather than redoing every hold interval
    /// (`Core::on_held_reply_timer`).
    pub(crate) fn own_record_blockers(
        &self,
        to: NodeId,
        rid: Rid,
        outcome: &MutateOutcome,
        gen: u64,
        upto: Option<u64>,
        replica: &dyn Replica,
    ) -> RemoteBlockers {
        // The outcomes a requester waits on: an acceptance (its
        // transaction, or the position it observes once the log brought
        // it), and a refusal (it observes the position: chunk
        // close-stall-followup).
        let records: &[LogRecord] = match outcome {
            MutateOutcome::Accepted { records, .. } | MutateOutcome::Exists { records, .. } => {
                records
            }
            MutateOutcome::Errno(_) | MutateOutcome::Conflict { .. } => &[],
            _ => return RemoteBlockers::default(),
        };
        if to == self.cfg.node_id {
            return RemoteBlockers::default();
        }
        replica.remote_blockers(rid, gen, records, to, upto)
    }

    /// [`Self::own_chunks_for`] from `blockers`: whether this node's
    /// stream carries the records to `to` past them.
    pub(crate) fn own_chunks_given(
        &self,
        to: NodeId,
        (gen, position): (u64, &Position),
        blockers: RemoteBlockers,
        replica: &dyn Replica,
    ) -> OwnChunks {
        if blockers.inos.is_empty() {
            return OwnChunks::None;
        }
        // Through the op's transaction and the position it was evaluated
        // at (a local write committing in between only makes that later).
        let through = blockers
            .through
            .max(position.pending.map(|p| p.jseq))
            .unwrap_or(0);
        let streams = gen == 0
            && self.cfg.pre_s3_streaming
            && (self.epoch_streams_ahead() || !self.lease.backups().is_empty())
            && self.stream_subscribers().contains(&to)
            && self.stream_reaches(to, through, replica);
        if streams {
            OwnChunks::Streamed(blockers.inos)
        } else {
            OwnChunks::Upload(blockers.inos)
        }
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
        // Never below the floor, even when an older window entry touched
        // the keys: a delegate's window only records segments applied
        // while it delegates, and its grant raised the floor over the
        // ones applied before (a root's floor is below every entry it
        // holds, so this is its old rule). Long-delegated seed 70162: the
        // segment that renamed `d2/f1` away and granted the new
        // generation was applied between two generations, missing from
        // the window; the create of `d2/f1` answered base 46 (an older
        // touch) instead of 75, and the requester installed it under
        // the rename it had not applied yet.
        Some(touched.unwrap_or(0).max(self.shipped_floor))
    }

    /// `forward::holder_execute`: dedup by `recent`, then by `completed`,
    /// then execute; refusals carry what the requester needs to rebase or
    /// to install the existing entry. The second value is set when the op
    /// executed just now: the inodes its records touched (plan 30 §M8's
    /// recall set).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn holder_execute(
        &mut self,
        now: Ms,
        epoch: Epoch,
        rid: Rid,
        op: &MutateOp,
        tag: &LockTag,
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
            Some(constellation_meta::CompletedOutcome::Refused { code }) => {
                self.stats.forward_dedup_hits += 1;
                return (
                    if code == Code::Stale {
                        MutateOutcome::Conflict { manifest: None }
                    } else {
                        MutateOutcome::Errno(code)
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
        let outcome = match replica.execute(op, Some(rid), tag, now.0) {
            Ok(records) => {
                replica.remember_outcome(rid, &records);
                if rid.node != self.cfg.node_id {
                    replica.note_foreign_executed(&records);
                }
                self.lease.touch(now);
                self.nudge(now, out);
                // (`_executed`: plus what an unlink or rename changed
                // that its records do not name.)
                let inos = constellation_meta::recall_inos_executed(&records);
                return (MutateOutcome::Accepted { epoch, records }, Some(inos));
            }
            Err(MetaError::Conflict) => match op {
                MutateOp::SetManifest { ino, .. } => MutateOutcome::Conflict {
                    manifest: replica.manifest(*ino),
                },
                _ => MutateOutcome::Errno(Code::Again),
            },
            // Plan 30 §M14 phase 2, the fencing token: a grant the op was
            // issued under is no longer live here. Not journaled (nothing
            // ran, and a retry of the rid is refused the same way: a
            // token never comes back to life), never executed.
            Err(MetaError::LockLapsed) => {
                self.stats.lock_lapsed_refusals += 1;
                tracing::info!(
                    node = self.cfg.node_id,
                    ?rid,
                    ?tag,
                    "refused a forwarded op: the lock grant it was issued under is no longer live"
                );
                MutateOutcome::LockLapsed
            }
            Err(MetaError::Exists) => {
                self.record_refusal(rid, Code::Exists, Some(op), replica);
                match named_child(op) {
                    Some((parent, name)) => match replica.entry_as_record(parent, name) {
                        Some(record) => MutateOutcome::Exists {
                            records: vec![record],
                            epoch,
                        },
                        None => MutateOutcome::Errno(Code::Exists),
                    },
                    None => MutateOutcome::Errno(Code::Exists),
                }
            }
            Err(e) => {
                let code = e.code();
                self.record_refusal(rid, code, Some(op), replica);
                MutateOutcome::Errno(code)
            }
        };
        (outcome, None)
    }

    /// Plan 30 §M9: a definitive refusal of an op executed by rid is an
    /// outcome, journaled as `Refused { rid, code }` so that any second
    /// execution of the rid — the requester's retry after a `Busy` or a
    /// lost reply, the deposed holder's replay by rid, an inbox batch
    /// drained later — dedups to the same code rather than re-evaluating
    /// the op against a state that may have changed meanwhile. (A
    /// transient refusal — `Conflict`/`EAGAIN`, a stale manifest base —
    /// is not an outcome: the requester rebases and retries.) The row is
    /// unshipped journal work like any other: the reply's position
    /// carries it, and under `Backup`/`S3` the acknowledgement waits for
    /// it.
    /// `op`: the refused op, so the row records what it observed
    /// (`JournalTx::observed`) and the ship plan keeps the refusal behind
    /// a deferred transaction that produced that state.
    pub(crate) fn record_refusal(
        &mut self,
        rid: Rid,
        code: Code,
        op: Option<&MutateOp>,
        replica: &dyn Replica,
    ) {
        if let Err(error) = replica.journal_refusal(rid, code, op) {
            tracing::warn!(node = self.cfg.node_id, ?rid, %code, %error, "could not journal a refusal");
            return;
        }
        self.stats.refusals_journaled += 1;
    }

    /// `SyncRequest::HandOff`: a peer wants the lease. If this node holds
    /// it (view not fenced, no gate pending), flush and release through
    /// the handoff job; in an active continuation epoch the hold is
    /// simply let go (nothing ships during an epoch); otherwise decline —
    /// the requester waits the lease out through S3, which is always
    /// correct. The two kinds are never crossed: an epoch request
    /// (`epoch_applied: Some`) is answered only by the hold transfer, an
    /// S3 request only by the handoff job.
    pub(crate) fn on_lease_request(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        epoch_applied: Option<Seq>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.note_foreign(now, replica, out);
        let decline = |core: &mut Self, out: &mut Vec<Action>| {
            core.stats.handoffs_declined += 1;
            out.push(Action::Send {
                to: from,
                msg: PeerMsg::LeaseHandoff {
                    req,
                    released: false,
                    epoch: core.lease.epoch().unwrap_or(0),
                    head_seq: None,
                },
            });
        };
        // Plan 30 §M11: no handoff while a delegation is live (the root
        // stays; see `round_release`).
        let can_serve = self.lease.ship_epoch(now, &self.cfg).is_some()
            && !self.lease.fenced()
            && self.deleg_live_generations() == 0
            && replica.locks().grants_len() == 0;
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
        let epoch_transfer = self.epoch.active && !self.epoch.frozen && self.lease.epoch_held();
        if epoch_transfer != epoch_applied.is_some() {
            // Flex-crash seed 2236: the requester's epoch was active, this
            // node's still frozen; the handoff job flushed and released
            // the S3 lease, the requester adopted a local hold on the
            // strength of the reply, and a third node claimed the freed
            // lease beside it. (And an S3 requester — no member of this
            // epoch — cannot take its hold.)
            decline(self, out);
            return;
        }
        if epoch_transfer {
            let applied = epoch_applied.unwrap_or(0);
            // Plan 30 §M10 (found by the M10 simulation's first epoch
            // runs, pre-M10 behaviour): a hold with an unshipped journal is
            // not handed over. Nothing ships during an epoch, so the
            // successor would execute on a replica missing this journal,
            // and the two journals would ship in either order after the
            // epoch (acknowledged effects re-evaluated and reordered). The
            // requester forwards to this node instead; its file writes'
            // manifests go forwarded too, their chunks uploaded when S3
            // returns (`view::View::commit_manifest_forwarded`).
            // `flex-crash` seed 30702: nor to a requester that has not
            // applied this node's whole log. The successor catches up to
            // `head_seq` from S3 outside an epoch; inside one it cannot,
            // and it would execute against a state missing segments this
            // node shipped before the outage (seq 14–18 there, streamed
            // while the requester was partitioned from this node): an
            // unlink of a file the log had already removed took effect.
            // It forwards to this node instead.
            let behind = applied < self.ship.head_seq;
            if behind {
                self.stats.epoch_handoffs_behind += 1;
            }
            if behind || replica.journal_len().unwrap_or(1) > 0 {
                decline(self, out);
                return;
            }
            // The hold leaves for good: persisted before the reply, so no
            // path (the restart adoption above all) takes it back while
            // the successor journals under it.
            if !self.end_epoch_hold(replica, false) {
                decline(self, out);
                return;
            }
            let epoch = self.lease.epoch().unwrap_or(1);
            self.lease.release_local();
            // The hold owner now: this node follows its log stream.
            self.lease.cached_holder = Some(from);
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
            super::jobs::JobReq::Handoff { req, from, at: now },
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
