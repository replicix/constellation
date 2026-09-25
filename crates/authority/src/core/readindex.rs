//! Plan 30 §M8: `cto=strict` — ReadIndex, read delegations and recalls.
//!
//! # The reader
//!
//! A strict open or lookup on a node that is not the sequencer
//! (`Control::ReadIndex`, from a FUSE thread that found no delegation to
//! read under) asks the sequencer for a **position**: the state that
//! covers every mutation of what it reads that was acknowledged before
//! the question arrived. The FUSE thread then waits (M6's session wait,
//! with the position as a per-read floor) and reads its own replica. The
//! answer may carry a **read delegation** on the inode; the core installs
//! it in `Meta`'s table (`constellation_meta::readdeleg`), and later opens
//! and lookups under it are answered locally by the FUSE thread, with no
//! event and no round trip, until it expires or is recalled. No live
//! sequencer, or no P2P path to ask one: the core tails the log to its
//! head in S3 instead (`ReadAnswer::Tailed`; M13's P2P-off mode — see the
//! module doc's last section). No answer within the budget: `Degraded`,
//! and the read is bounded, as M6's timed-out wait is.
//!
//! # The sequencer
//!
//! It answers a ReadIndex only while its lease is usable and its view is
//! open (not in the takeover gate, not releasing) — the lease-read rule:
//! a usable lease is exclusive, so its replica is the authority. The
//! position is `(head_seq, pending)`, where `pending` — its unshipped
//! journal position — is included only when the unshipped journal
//! touched what the reader reads (`Meta::unshipped_touches_read`): a
//! file nobody else writes costs the reader one round trip and no wait,
//! however busy the sequencer is with other files. A grant is made
//! **before** the position is read (the ordering the FUSE fast path
//! relies on, see `readdeleg`'s module doc), is persisted as a horizon
//! before it is answered (a restart inside the lease must not forget it),
//! and is capped so it never outlives the lease that backs it.
//!
//! Before the acknowledgement of any mutation that touched a delegated
//! inode leaves this node — a forwarded op's reply, a local op's reply,
//! the FUSE fast path's return (`Control::Recall`), an inbox op's
//! execution (whose acknowledgement is the log), a release (which lets
//! another node take over at once) — every grant on it held by a node
//! other than the writer is recalled over P2P, or outwaited: the grant is
//! live until `granted + ttl + margin` by this node's clock. The wait is
//! a *parked* continuation, never a blocked handler: unrelated keys, the
//! ship round and other requesters proceed. A forwarded op whose reply is
//! parked longer than `recall_hold_ms` is answered `Held`, and the
//! requester retries the same rid (the reply then comes from dedup, or
//! re-attaches to the park).
//!
//! # The margins
//!
//! The delegate honours a grant until `sent + ttl − margin` (its clock,
//! from when it sent the request), the sequencer outwaits it until
//! `granted + ttl + margin` (its clock), and a grant's `ttl` is capped at
//! `lease.expires − margin − now` — `margin` being the lease's own
//! `expiry_margin_ms` everywhere. `crates/model/src/cto.rs` has the
//! argument (every condition reduces to the lease's own `margin > 2D`
//! for clocks within `D` of real time) and the model that checks it.
//!
//! # P2P off (M13's inbox)
//!
//! With no P2P there is no ReadIndex, no delegation and no recall: a
//! strict open tails S3 to head and reads. That makes every close whose
//! records are *in the log* when the open starts visible — which covers
//! every write that went through the inbox (its outcome, hence its
//! close, comes from the log). The sequencer's *own* writes are
//! acknowledged before they ship, so under P2P off they become visible
//! to other nodes' strict opens only at its next ship: strict degrades
//! to "the log", as bounded as the ship interval. The inbox path with
//! P2P *on* elsewhere (one requester cut off) is handled: the sequencer
//! recalls before it executes an inbox op on a delegated inode, and
//! blocks new grants on it until it has.

use super::client::Phase as ClientPhase;
use super::{Core, S3For, Timer};
use crate::action::{Action, ControlOk, ReadAnswer, S3Op};
use crate::event::{PeerMsg, ReadGrantMsg, ReadIndexOutcome, S3Result};
use crate::ids::{Ms, NodeId, OpId, Seq, TimerId};
use crate::replica::Replica;
use constellation_fs_core::Ino;
use constellation_meta::{HeldDelegation, MutateOp, MutateOutcome, Position, RecallNeed, Rid};
use std::collections::{BTreeMap, BTreeSet};

/// A strict read waiting for its answer.
#[derive(Debug)]
struct ReadReq {
    ino: Ino,
    dir: bool,
    name: Option<String>,
    /// The peer request in flight, if any.
    req: Option<OpId>,
    holder: NodeId,
    /// When the request was sent (the delegation's lifetime counts from
    /// here) and the recall generation then.
    sent_at: Ms,
    gen: u64,
    attempts: u32,
    redirected: bool,
    deadline: TimerId,
}

/// A grant being recalled.
#[derive(Debug)]
struct Recalling {
    req: OpId,
    timer: TimerId,
}

/// What a parked acknowledgement will do once its recalls are done.
#[derive(Debug)]
pub(crate) enum ParkedWhat {
    /// A forwarded op's reply. `req: None` once it was answered `Held`
    /// (the requester's retry re-attaches).
    Reply {
        to: NodeId,
        req: Option<OpId>,
        rid: Rid,
        outcome: MutateOutcome,
        base: Option<Seq>,
        position: Position,
        gen: u64,
        held_timer: Option<TimerId>,
    },
    /// A local client op's `finish`.
    Finish { rid: Rid, outcome: MutateOutcome },
    /// A FUSE fast-path write (`Control::Recall`).
    Control { op: OpId },
    /// A release CAS.
    Release,
    /// An inbox op halted before executing: poll its requester again.
    InboxRepoll { node: NodeId },
    /// Plan 30 §M11: a forwarded op the root executes once the write
    /// delegations its keys fall under are recalled and its `deps` are
    /// here (`req: None` once answered `Held`; the retry re-attaches).
    ExecuteReply {
        from: NodeId,
        req: Option<OpId>,
        rid: Rid,
        op: MutateOp,
        acked_through: u64,
        deps: Position,
        held_timer: Option<TimerId>,
    },
    /// Plan 30 §M11: a local op of this root, executed (and finished)
    /// once the recall is done and its `deps` are here.
    ExecuteLocal { rid: Rid },
    /// Phase 2b: a delegate's answer to the root's recall, sent once the
    /// read delegations it granted are recalled.
    DelegRecalled {
        to: NodeId,
        req: OpId,
        gen: u64,
        through: u64,
        /// Plan 30 §M14: the lock grants handed back with it.
        locks: Vec<constellation_meta::locks::Grant>,
    },
}

#[derive(Debug)]
struct Parked {
    waiting: BTreeSet<u64>,
    quarantine: Option<Ms>,
    /// Plan 30 §M9: the journal seq that must be durable (on every
    /// backup, or shipped) before this acknowledgement leaves.
    durable: Option<u64>,
    /// Plan 30 §M11: the position the replica must reach first.
    deps: Option<Position>,
    /// Phase 2b: a delegate's `(gen, idx)` that must be on its backup or
    /// in an applied segment first.
    stream_need: Option<(u64, u64)>,
    since: Ms,
    what: ParkedWhat,
}

/// What a parked acknowledgement waits for besides durability.
pub(crate) type RecallWait = (BTreeSet<u64>, Option<Ms>);

impl ReadState {
    pub(crate) fn container_sizes(&self) -> Vec<(&'static str, usize)> {
        vec![
            ("rd_parked", self.parked.len()),
            ("rd_reads", self.reads.len()),
            ("rd_by_req", self.by_req.len()),
            ("rd_recalls", self.recalls.len()),
            ("rd_parked_rids", self.parked_rids.len()),
        ]
    }
}

#[derive(Debug, Default)]
pub(crate) struct ReadState {
    reads: BTreeMap<OpId, ReadReq>,
    by_req: BTreeMap<OpId, OpId>,
    recalls: BTreeMap<u64, Recalling>,
    recall_by_req: BTreeMap<OpId, u64>,
    parked: BTreeMap<u64, Parked>,
    next_park: u64,
    parked_rids: BTreeMap<Rid, u64>,
    /// `execute_local` found recalls (or a durability wait) needed;
    /// `finish` parks.
    pub(crate) parked_local: BTreeMap<Rid, (RecallWait, Option<u64>)>,
    /// Inodes an inbox op waits to execute on: no new grants until then.
    pub(crate) blocked: BTreeMap<Ino, Ms>,
    /// Strict reads answered by a tail to head (no live sequencer, or no
    /// P2P): the leader's job, not yet started, that later reads join —
    /// a tail that starts after a read began covers it — and each
    /// leader's followers.
    pub(crate) read_tail_open: Option<OpId>,
    pub(crate) read_tails: BTreeMap<OpId, Vec<OpId>>,
    quarantine_timer: Option<TimerId>,
}

/// Plan 30 §M8's view for `status`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadView {
    pub reads_in_flight: usize,
    pub recalls_in_flight: usize,
    pub parked_acks: usize,
}

impl Core {
    pub fn read_view(&self) -> ReadView {
        ReadView {
            reads_in_flight: self.rd.reads.len(),
            recalls_in_flight: self.rd.recalls.len(),
            parked_acks: self.rd.parked.len(),
        }
    }

    /// At start: a previous incarnation's grants may still be honoured.
    pub(crate) fn read_start(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        if !self.cfg.read_delegations {
            return;
        }
        if let Some(until) = replica.load_grant_quarantine(now.0) {
            tracing::info!(
                node = self.cfg.node_id,
                wait_ms = until - now.0,
                "read delegations granted before this restart may still be honoured; \
                 acknowledgements wait until they have expired"
            );
            let id = self.set_timer(Ms(until), Timer::GrantQuarantine, out);
            self.rd.quarantine_timer = Some(id);
        }
    }

    // ------------------------------------------------------------ holder

    /// Another node showed itself. A lone strict node's kernel cache must
    /// drain before anything another node started is acknowledged (see
    /// `Config::kernel_cache_ttl_ms`): the latch flips once, and the drain
    /// deadline joins the quarantine every acknowledgement waits on.
    pub(crate) fn note_foreign(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.cfg.kernel_cache_ttl_ms == 0 {
            return;
        }
        if let Some(until) = replica
            .read_delegations()
            .leave_alone(now.0, self.cfg.kernel_cache_ttl_ms)
        {
            tracing::info!(
                node = self.cfg.node_id,
                drain_ms = self.cfg.kernel_cache_ttl_ms,
                "another node showed itself: strict mounts stop caching in the kernel; \
                 acknowledgements wait once for the entries cached until now to expire"
            );
            if self.rd.quarantine_timer.is_none() {
                let id = self.set_timer(Ms(until), Timer::GrantQuarantine, out);
                self.rd.quarantine_timer = Some(id);
            }
        }
    }

    /// The sequencer answers where the state a strict reader reads is.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_read_index(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        ino: Ino,
        dir: bool,
        name: Option<String>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.note_foreign(now, replica, out);
        // Phase 2b: this node as the delegate of the subtree.
        if let Some(outcome) = self.deleg_read_index(now, from, ino, name.as_deref(), replica) {
            out.push(Action::Send {
                to: from,
                msg: PeerMsg::ReadIndexReply { req, outcome },
            });
            return;
        }
        // Phase 2b: keys under another node's live delegation have that
        // delegate's index, not the holder's (a stale requester's table):
        // send it there, and never grant a read delegation over them.
        if let Some(n) = self.deleg_read_owner(now, ino, name.as_deref(), replica) {
            if n != self.cfg.node_id {
                out.push(Action::Send {
                    to: from,
                    msg: PeerMsg::ReadIndexReply {
                        req,
                        outcome: ReadIndexOutcome::NotHolder { holder: n },
                    },
                });
                return;
            }
        }
        let outcome = if !self.lease.usable(now, &self.cfg) {
            self.stats.read_index_refused += 1;
            let holder = self
                .lease
                .cached_holder
                .filter(|h| *h != self.cfg.node_id)
                .unwrap_or(0);
            ReadIndexOutcome::NotHolder { holder }
        } else if self.lease.fenced() {
            self.stats.read_index_refused += 1;
            ReadIndexOutcome::Busy
        } else if !self.strict_answer_allowed(now, out) || !self.ensure_granting_marked(out) {
            // Plan 30 §M9: a tenure that may be taken over before its
            // lease expires answers strict reads only with fresh S3
            // liveness, and only once the lease says it does (its
            // successor then waits the horizon out).
            self.stats.read_index_refused += 1;
            ReadIndexOutcome::Busy
        } else {
            let epoch = self.lease.epoch().unwrap_or(0);
            let grant = self.maybe_grant(now, from, ino, epoch, replica);
            // After the grant (see `readdeleg`'s ordering argument).
            let child = name.as_deref().map(|n| (n, replica.lookup_ino(ino, n)));
            let touched = replica.unshipped_touches_read(ino, dir, child);
            let pending = if touched {
                replica.journal_position(epoch)
            } else {
                None
            };
            if pending.is_some() {
                // The reader waits for the ship that carries it: soon.
                self.nudge(now, out);
            }
            self.stats.read_index_served += 1;
            tracing::debug!(
                node = self.cfg.node_id,
                from,
                ino,
                dir,
                ?name,
                touched,
                head = self.ship.head_seq,
                ?pending,
                granted = grant.is_some(),
                "answered a ReadIndex"
            );
            ReadIndexOutcome::Ok {
                position: Position {
                    seq: self.ship.head_seq,
                    pending,
                    streams: Default::default(),
                },
                grant,
            }
        };
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::ReadIndexReply { req, outcome },
        });
    }

    fn maybe_grant(
        &mut self,
        now: Ms,
        to: NodeId,
        ino: Ino,
        epoch: u64,
        replica: &dyn Replica,
    ) -> Option<ReadGrantMsg> {
        if !self.cfg.read_delegations || self.lease.epoch_held() || to == self.cfg.node_id {
            return None;
        }
        if self.rd.blocked.get(&ino).is_some_and(|until| *until > now) {
            return None;
        }
        let (lease, _) = self.lease.held.as_ref()?;
        let margin = self.cfg.expiry_margin_ms as i64;
        // Never outlive the lease that backs the promise.
        let cap = lease.expires_unix_ms - margin - now.0;
        let ttl = (self.cfg.read_delegation_ttl_ms as i64).min(cap);
        if ttl <= 0 {
            return None;
        }
        let until = now.0 + ttl + margin;
        if !replica.note_grant_horizon(until) {
            return None;
        }
        let id = replica.read_delegations().grant(to, ino, until);
        self.stats.read_grants += 1;
        Some(ReadGrantMsg {
            id,
            ttl_ms: ttl as u64,
            epoch,
        })
    }

    /// What acknowledging a mutation with these touched inodes must wait
    /// for, the recalls started. `None`: nothing (the common case, one
    /// lock and an empty map).
    pub(crate) fn recall_needed(
        &mut self,
        now: Ms,
        inos: &[Ino],
        except: Option<NodeId>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Option<(BTreeSet<u64>, Option<Ms>)> {
        if !self.cfg.recall_before_ack {
            return None;
        }
        let need = replica.read_delegations().touching(inos, except, now.0);
        self.start_recalls(now, need, out)
    }

    pub(crate) fn start_recalls(
        &mut self,
        now: Ms,
        need: RecallNeed,
        out: &mut Vec<Action>,
    ) -> Option<(BTreeSet<u64>, Option<Ms>)> {
        if need.is_empty() {
            return None;
        }
        let mut waiting = BTreeSet::new();
        for g in need.grants {
            waiting.insert(g.id);
            if self.rd.recalls.contains_key(&g.id) {
                continue;
            }
            let req = self.op_id();
            let timer = self.set_timer(Ms(g.until_ms), Timer::GrantExpiry(g.id), out);
            self.rd.recalls.insert(g.id, Recalling { req, timer });
            self.rd.recall_by_req.insert(req, g.id);
            self.stats.recalls_sent += 1;
            tracing::debug!(
                node = self.cfg.node_id,
                delegate = g.node,
                ino = g.ino,
                grant = g.id,
                "recalling a read delegation"
            );
            out.push(Action::Send {
                to: g.node,
                msg: PeerMsg::DelegationRecall {
                    req,
                    ino: g.ino,
                    grant: g.id,
                },
            });
        }
        let quarantine = need.quarantine_until.map(Ms);
        if let (Some(at), None) = (quarantine, self.rd.quarantine_timer) {
            let id = self.set_timer(at, Timer::GrantQuarantine, out);
            self.rd.quarantine_timer = Some(id);
        }
        let _ = now;
        Some((waiting, quarantine))
    }

    pub(crate) fn park(
        &mut self,
        now: Ms,
        wait: RecallWait,
        durable: Option<u64>,
        what: ParkedWhat,
    ) -> u64 {
        self.park_with_deps(now, wait, durable, None, what)
    }

    pub(crate) fn park_with_deps(
        &mut self,
        now: Ms,
        wait: RecallWait,
        durable: Option<u64>,
        deps: Option<Position>,
        what: ParkedWhat,
    ) -> u64 {
        self.rd.next_park += 1;
        let id = self.rd.next_park;
        if !wait.0.is_empty() || wait.1.is_some() {
            self.stats.recall_waits += 1;
        }
        if durable.is_some() {
            self.stats.acks_waited += 1;
        }
        self.rd.parked.insert(
            id,
            Parked {
                waiting: wait.0,
                quarantine: wait.1,
                durable,
                deps,
                stream_need: None,
                since: now,
                what,
            },
        );
        id
    }

    /// Phase 2b: park an acknowledgement until stream transaction
    /// `(gen, idx)` is durable (`Core::deleg_stream_durable`).
    pub(crate) fn park_stream_need(
        &mut self,
        now: Ms,
        gen: u64,
        idx: u64,
        what: ParkedWhat,
    ) -> u64 {
        self.rd.next_park += 1;
        let id = self.rd.next_park;
        self.stats.acks_waited += 1;
        if let ParkedWhat::Reply { rid, .. } = &what {
            self.rd.parked_rids.insert(*rid, id);
        }
        self.rd.parked.insert(
            id,
            Parked {
                waiting: BTreeSet::new(),
                quarantine: None,
                durable: None,
                deps: None,
                stream_need: Some((gen, idx)),
                since: now,
                what,
            },
        );
        id
    }

    /// Phase 2b: the parks waiting on generation `gen`'s durability.
    pub(crate) fn stream_parks_of(&self, gen: u64) -> Vec<u64> {
        self.rd
            .parked
            .iter()
            .filter(|(_, p)| p.stream_need.is_some_and(|(g, _)| g == gen))
            .map(|(id, _)| *id)
            .collect()
    }

    pub(crate) fn has_stream_parks(&self) -> bool {
        self.rd.parked.values().any(|p| p.stream_need.is_some())
    }

    /// Phase 2b: answer a parked acknowledgement `Busy` (the requester,
    /// or this node's client, retries by rid).
    pub(crate) fn abort_park_busy(
        &mut self,
        now: Ms,
        id: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(p) = self.rd.parked.remove(&id) else {
            return;
        };
        match p.what {
            ParkedWhat::Reply {
                to,
                req,
                rid,
                held_timer,
                ..
            } => {
                self.rd.parked_rids.remove(&rid);
                if let Some(t) = held_timer {
                    self.cancel_timer(t, out);
                }
                if let Some(req) = req {
                    out.push(Action::Send {
                        to,
                        msg: PeerMsg::MutateReply {
                            req,
                            outcome: MutateOutcome::Busy,
                            base: None,
                            position: Position::ZERO,
                            gen: 0,
                        },
                    });
                }
            }
            ParkedWhat::Finish { rid, .. } => {
                if let Some(c) = self.clients.get_mut(&rid) {
                    c.phase = ClientPhase::WaitingLease;
                }
                self.retry_or_lease(now, rid, replica, out);
            }
            // Phase 2b: the root's execution parks (a lease lost with a
            // recall in flight): the requester is answered `Busy` and
            // re-sends; a local op takes the lease path; an inbox batch
            // is polled again by whoever holds the lease next.
            ParkedWhat::ExecuteReply {
                from,
                req,
                rid,
                held_timer,
                ..
            } => {
                self.rd.parked_rids.remove(&rid);
                if let Some(t) = held_timer {
                    self.cancel_timer(t, out);
                }
                if let Some(req) = req {
                    out.push(Action::Send {
                        to: from,
                        msg: PeerMsg::MutateReply {
                            req,
                            outcome: MutateOutcome::Busy,
                            base: None,
                            position: Position::ZERO,
                            gen: 0,
                        },
                    });
                }
            }
            ParkedWhat::ExecuteLocal { rid } => {
                self.dl.pending_exec.remove(&rid);
                if let Some(c) = self.clients.get_mut(&rid) {
                    c.phase = ClientPhase::WaitingLease;
                }
                self.retry_or_lease(now, rid, replica, out);
            }
            ParkedWhat::InboxRepoll { .. } => {}
            other => {
                self.rd.parked.insert(id, Parked { what: other, ..p });
            }
        }
    }

    /// Plan 30 §M11: park a forwarded op's *execution* (the root recalls
    /// the write delegations first, or waits for `deps`); the requester
    /// is answered `Held` before its RPC times out and re-attaches.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn park_exec_reply(
        &mut self,
        now: Ms,
        wait: BTreeSet<u64>,
        deps: Option<Position>,
        from: NodeId,
        req: OpId,
        rid: Rid,
        op: MutateOp,
        acked_through: u64,
        out: &mut Vec<Action>,
    ) {
        self.stats.deleg_exec_parked += 1;
        let id = self.park_with_deps(
            now,
            (wait, None),
            None,
            deps,
            ParkedWhat::ExecuteReply {
                from,
                req: Some(req),
                rid,
                op,
                acked_through,
                deps: deps.unwrap_or(Position::ZERO),
                held_timer: None,
            },
        );
        self.rd.parked_rids.insert(rid, id);
        let timer = self.set_timer(now.plus(self.cfg.recall_hold_ms), Timer::HeldReply(id), out);
        if let Some(Parked {
            what: ParkedWhat::ExecuteReply { held_timer, .. },
            ..
        }) = self.rd.parked.get_mut(&id)
        {
            *held_timer = Some(timer);
        }
    }

    /// Plan 30 §M11: park a local op's execution.
    pub(crate) fn park_exec_local(
        &mut self,
        now: Ms,
        wait: BTreeSet<u64>,
        deps: Option<Position>,
        rid: Rid,
    ) {
        self.stats.deleg_exec_parked += 1;
        self.dl.pending_exec.insert(rid);
        if let Some(c) = self.clients.get_mut(&rid) {
            c.phase = ClientPhase::Recalling;
        }
        self.park_with_deps(
            now,
            (wait, None),
            None,
            deps,
            ParkedWhat::ExecuteLocal { rid },
        );
    }

    /// Whether any parked continuation waits for a position.
    pub(crate) fn has_deps_parks(&self) -> bool {
        self.rd.parked.values().any(|p| p.deps.is_some())
    }

    /// Phase 2b round 2: a local op parked on a recall (`ExecuteLocal`)
    /// reached its client deadline: the park is dropped (the op never
    /// ran; the client hears in doubt and retries by rid).
    pub(crate) fn abort_exec_local_park(&mut self, rid: Rid) -> bool {
        let id = self
            .rd
            .parked
            .iter()
            .find(|(_, p)| matches!(p.what, ParkedWhat::ExecuteLocal { rid: r } if r == rid))
            .map(|(id, _)| *id);
        let Some(id) = id else {
            return false;
        };
        self.rd.parked.remove(&id);
        self.dl.pending_exec.remove(&rid);
        true
    }

    /// Phase 2b: the root lost its lease with executions parked on its
    /// generations' recalls — abort them (`Busy` to a requester, the
    /// lease path for a local op).
    pub(crate) fn abort_parks_waiting_on(
        &mut self,
        now: Ms,
        ids: &BTreeSet<u64>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let parked: Vec<u64> = self
            .rd
            .parked
            .iter()
            .filter(|(_, p)| p.waiting.iter().any(|w| ids.contains(w)))
            .map(|(id, _)| *id)
            .collect();
        for id in parked {
            self.abort_park_busy(now, id, replica, out);
        }
    }

    /// Plan 30 §M11: a write delegation's wait id is done (its generation
    /// ended): release what waited on it.
    pub(crate) fn deleg_wait_done(
        &mut self,
        now: Ms,
        id: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        for p in self.rd.parked.values_mut() {
            p.waiting.remove(&id);
        }
        self.complete_ready(now, replica, out);
        // Phase 2b: the generations a cross-subtree op ended are granted
        // again once it ran.
        self.deleg_redelegate_after_cross(now, replica, out);
    }

    /// Phase 2b: whether any parked continuation still waits on the
    /// recall of generation `gen`.
    pub(crate) fn rd_has_recall_wait(&self, gen: u64) -> bool {
        let id = super::delegate::wait_id(gen);
        self.rd.parked.values().any(|p| p.waiting.contains(&id))
    }

    /// Phase 2b: whether any execution (a forwarded op, a local op, an
    /// inbox batch) is parked on the root at all — a re-delegation
    /// before it ran would only make it recall again.
    pub(crate) fn has_exec_parks(&self) -> bool {
        self.rd.parked.values().any(|p| {
            matches!(
                p.what,
                ParkedWhat::ExecuteReply { .. }
                    | ParkedWhat::ExecuteLocal { .. }
                    | ParkedWhat::InboxRepoll { .. }
            )
        })
    }

    /// Acknowledgements parked for durability (plan 30 §M9, `status`).
    pub(crate) fn parked_durable_count(&self) -> usize {
        self.rd
            .parked
            .values()
            .filter(|p| p.durable.is_some())
            .count()
    }

    /// Arm the quarantine timer at `at` (plan 30 §M9's successor floor
    /// joins M8's restart quarantine).
    pub(crate) fn arm_grant_quarantine(&mut self, at: Ms, out: &mut Vec<Action>) {
        if self.rd.quarantine_timer.is_none() {
            let id = self.set_timer(at, Timer::GrantQuarantine, out);
            self.rd.quarantine_timer = Some(id);
        }
    }

    /// Plan 30 §M9: a local op parked for durability reached its deadline:
    /// answer it in doubt (its row is journaled here and ships when it
    /// can; the client's retry by rid finds it completed). `false` when
    /// `rid` is not parked for durability (a recall park: bounded by the
    /// grant's TTL, so the deadline lets it be).
    pub(crate) fn abort_durable_park_of(&mut self, rid: Rid, out: &mut Vec<Action>) -> bool {
        let id = self
            .rd
            .parked
            .iter()
            .find(|(_, p)| {
                p.durable.is_some()
                    && matches!(p.what, ParkedWhat::Finish { rid: r, .. } if r == rid)
            })
            .map(|(id, _)| *id);
        let Some(id) = id else {
            return false;
        };
        self.rd.parked.remove(&id);
        let _ = out;
        true
    }

    /// Plan 30 §M9: the lease is gone; every acknowledgement parked for
    /// durability is answered as not given (see `backup.rs`). Returns how
    /// many.
    pub(crate) fn abort_durable_parks(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> u64 {
        let ids: Vec<u64> = self
            .rd
            .parked
            .iter()
            .filter(|(_, p)| p.durable.is_some())
            .map(|(id, _)| *id)
            .collect();
        let mut n = 0;
        for id in ids {
            // Re-entrancy tolerance as `complete_ready` (M5's rule).
            let Some(p) = self.rd.parked.remove(&id) else {
                continue;
            };
            n += 1;
            match p.what {
                ParkedWhat::Reply {
                    to,
                    req,
                    rid,
                    held_timer,
                    ..
                } => {
                    self.rd.parked_rids.remove(&rid);
                    if let Some(t) = held_timer {
                        self.cancel_timer(t, out);
                    }
                    if let Some(req) = req {
                        out.push(Action::Send {
                            to,
                            msg: PeerMsg::MutateReply {
                                req,
                                outcome: MutateOutcome::Busy,
                                base: None,
                                position: Position::ZERO,
                                gen: 0,
                            },
                        });
                    }
                }
                ParkedWhat::Finish { rid, .. } => {
                    // Executed here but never acknowledged: in doubt, and
                    // retried by the same rid (the row is stranded and
                    // replayed by the recovery; `completed` dedups — a
                    // refusal too, since the holder journals refusals —
                    // and the replay entry is marked unacked so that a
                    // refusal of it is this client's answer, not a
                    // conflict copy).
                    self.replay.unacked.insert(rid);
                    if let Some(c) = self.clients.get_mut(&rid) {
                        c.forwarded = true;
                        c.phase = ClientPhase::WaitingLease;
                    }
                    self.retry_or_lease(now, rid, replica, out);
                }
                ParkedWhat::Control { op } => out.push(Action::ControlDone {
                    op,
                    result: Err("the lease was lost before the write was durable".into()),
                }),
                ParkedWhat::Release => {}
                ParkedWhat::InboxRepoll { .. } => {}
                ParkedWhat::ExecuteReply {
                    rid, held_timer, ..
                } => {
                    self.rd.parked_rids.remove(&rid);
                    if let Some(t) = held_timer {
                        self.cancel_timer(t, out);
                    }
                }
                ParkedWhat::ExecuteLocal { rid } => {
                    self.dl.pending_exec.remove(&rid);
                    if let Some(c) = self.clients.get_mut(&rid) {
                        c.phase = ClientPhase::WaitingLease;
                    }
                    self.retry_or_lease(now, rid, replica, out);
                }
                ParkedWhat::DelegRecalled {
                    to,
                    req,
                    gen,
                    through,
                    locks,
                } => {
                    // Answered as it stands: the root outwaits the read
                    // grants by its horizon anyway.
                    out.push(Action::Send {
                        to,
                        msg: PeerMsg::DelegRecalled {
                            req,
                            gen,
                            through,
                            locks,
                        },
                    });
                }
            }
        }
        n
    }

    /// A forwarded op's reply must wait: park it, and answer `Held` if it
    /// is still parked shortly before the requester's RPC gives up.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn park_reply(
        &mut self,
        now: Ms,
        wait: RecallWait,
        durable: Option<u64>,
        to: NodeId,
        req: OpId,
        rid: Rid,
        outcome: MutateOutcome,
        base: Option<Seq>,
        position: Position,
        gen: u64,
        out: &mut Vec<Action>,
    ) {
        let id = self.park(
            now,
            wait,
            durable,
            ParkedWhat::Reply {
                to,
                req: Some(req),
                rid,
                outcome,
                base,
                position,
                gen,
                held_timer: None,
            },
        );
        self.rd.parked_rids.insert(rid, id);
        let timer = self.set_timer(now.plus(self.cfg.recall_hold_ms), Timer::HeldReply(id), out);
        if let Some(Parked {
            what: ParkedWhat::Reply { held_timer, .. },
            ..
        }) = self.rd.parked.get_mut(&id)
        {
            *held_timer = Some(timer);
        }
    }

    /// A retry of a forwarded op whose reply is parked: attach it.
    pub(crate) fn reattach_parked(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        rid: Rid,
        out: &mut Vec<Action>,
    ) -> bool {
        let Some(&id) = self.rd.parked_rids.get(&rid) else {
            return false;
        };
        let hold = self.cfg.recall_hold_ms;
        let timer = self.set_timer(now.plus(hold), Timer::HeldReply(id), out);
        let (to, r, held_timer) = match self.rd.parked.get_mut(&id) {
            Some(Parked {
                what:
                    ParkedWhat::Reply {
                        to,
                        req: r,
                        held_timer,
                        ..
                    },
                ..
            }) => (to, r, held_timer),
            Some(Parked {
                what:
                    ParkedWhat::ExecuteReply {
                        from,
                        req: r,
                        held_timer,
                        ..
                    },
                ..
            }) => (from, r, held_timer),
            _ => return false,
        };
        *to = from;
        *r = Some(req);
        let old = held_timer.replace(timer);
        if let Some(old) = old {
            self.cancel_timer(old, out);
        }
        true
    }

    pub(crate) fn on_held_reply_timer(&mut self, id: u64, out: &mut Vec<Action>) {
        let (to, req, held_timer, durable) = match self.rd.parked.get_mut(&id) {
            Some(Parked {
                what:
                    ParkedWhat::Reply {
                        to,
                        req,
                        held_timer,
                        ..
                    },
                durable,
                ..
            }) => (to, req, held_timer, durable),
            Some(Parked {
                what:
                    ParkedWhat::ExecuteReply {
                        from,
                        req,
                        held_timer,
                        ..
                    },
                durable,
                ..
            }) => (from, req, held_timer, durable),
            _ => return,
        };
        // A durability wait (an S3 round trip) is longer than a LAN
        // recall: the retry comes back at the hold interval, not every
        // few ms.
        let retry_ms = if durable.is_some() {
            self.cfg.recall_hold_ms
        } else {
            self.cfg.held_retry_ms
        };
        *held_timer = None;
        if let Some(req) = req.take() {
            self.stats.held_replies += 1;
            out.push(Action::Send {
                to: *to,
                msg: PeerMsg::MutateReply {
                    req,
                    outcome: MutateOutcome::Held { retry_ms },
                    base: None,
                    position: Position::ZERO,
                    gen: 0,
                },
            });
        }
    }

    /// The FUSE fast path wrote inodes that may carry delegations.
    pub(crate) fn on_recall_control(
        &mut self,
        now: Ms,
        op: OpId,
        inos: Vec<Ino>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        match self.recall_needed(now, &inos, None, replica, out) {
            None => out.push(Action::ControlDone {
                op,
                result: Ok(ControlOk::Done),
            }),
            Some(wait) => {
                self.park(now, wait, None, ParkedWhat::Control { op });
            }
        }
    }

    /// A release lets another node take over at once: every live grant
    /// must be gone first. `true`: parked; the release is re-issued when
    /// the recalls are done.
    pub(crate) fn park_release_for_recalls(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        if !self.cfg.read_delegations && self.cfg.kernel_cache_ttl_ms == 0 {
            return false;
        }
        // Releasing lets another node take over at once: that is another
        // node for the kernel-cache latch too.
        self.note_foreign(now, replica, out);
        // `all_live` carries the kernel drain, not the restart quarantine.
        let need = replica.read_delegations().all_live(now.0);
        match self.start_recalls(now, need, out) {
            None => false,
            Some(wait) => {
                tracing::debug!(
                    node = self.cfg.node_id,
                    grants = wait.0.len(),
                    "release waits for read delegations to be recalled"
                );
                self.park(now, wait, None, ParkedWhat::Release);
                true
            }
        }
    }

    /// An inbox op would touch delegated inodes: recall, block new grants
    /// on them, and have the requester polled again when done.
    pub(crate) fn inbox_recall_first(
        &mut self,
        now: Ms,
        node: NodeId,
        inos: &[Ino],
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        let Some(wait) = self.recall_needed(now, inos, Some(node), replica, out) else {
            return false;
        };
        let until = now.plus(2 * (self.cfg.read_delegation_ttl_ms + self.cfg.expiry_margin_ms));
        for ino in inos {
            self.rd.blocked.insert(*ino, until);
        }
        self.park(now, wait, None, ParkedWhat::InboxRepoll { node });
        true
    }

    /// The inbox op executed: grants on its inodes may resume.
    pub(crate) fn inbox_unblock(&mut self, now: Ms, inos: &[Ino]) {
        for ino in inos {
            self.rd.blocked.remove(ino);
        }
        self.rd.blocked.retain(|_, until| *until > now);
    }

    pub(crate) fn on_delegation_recalled(
        &mut self,
        now: Ms,
        req: OpId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(id) = self.rd.recall_by_req.remove(&req) else {
            return;
        };
        self.stats.recalls_acked += 1;
        self.grant_done(now, id, replica, out);
    }

    pub(crate) fn on_grant_expiry(
        &mut self,
        now: Ms,
        id: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if let Some(r) = self.rd.recalls.get(&id) {
            self.rd.recall_by_req.remove(&r.req);
            self.stats.recalls_expired += 1;
            tracing::info!(
                node = self.cfg.node_id,
                grant = id,
                "a read delegation's recall went unanswered; outwaited it (TTL + margin)"
            );
        }
        self.grant_done(now, id, replica, out);
    }

    pub(crate) fn on_grant_quarantine(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.rd.quarantine_timer = None;
        let q = replica.read_delegations().quarantine_until();
        if q > now.0 {
            let id = self.set_timer(Ms(q), Timer::GrantQuarantine, out);
            self.rd.quarantine_timer = Some(id);
            return;
        }
        self.complete_ready(now, replica, out);
        // A takeover gate that waited for the quarantine can finish now.
        self.nudge(now, out);
    }

    fn grant_done(&mut self, now: Ms, id: u64, replica: &dyn Replica, out: &mut Vec<Action>) {
        replica.read_delegations().forget(id);
        if let Some(r) = self.rd.recalls.remove(&id) {
            self.cancel_timer(r.timer, out);
            self.rd.recall_by_req.remove(&r.req);
        }
        for p in self.rd.parked.values_mut() {
            p.waiting.remove(&id);
        }
        self.complete_ready(now, replica, out);
    }

    pub(crate) fn complete_ready(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        // Plan 30 §M9: a row at or below the journal's shipped watermark
        // is in the log — durable whatever the lease says now (long-acks3
        // seed 801715: the holder's segment landed while it was paused
        // past its lease; the acknowledgement it was parked for was
        // never released, and its client heard `EIO` at the deadline).
        let shipped = replica.journal_acked_seq().unwrap_or(0);
        let ready: Vec<u64> = self
            .rd
            .parked
            .iter()
            .filter(|(_, p)| {
                p.waiting.is_empty()
                    && p.quarantine.is_none_or(|q| q <= now)
                    && p.durable
                        .is_none_or(|j| j <= shipped || self.durable_covers(j))
                    && p.deps.as_ref().is_none_or(|d| replica.reaches_streams(d))
                    && p.stream_need
                        .is_none_or(|(g, i)| self.deleg_stream_durable(g, i, replica))
            })
            .map(|(id, _)| *id)
            .collect();
        for id in ready {
            // Tolerate re-entrancy the way M5's `release_gated` does: a
            // continuation run earlier in this loop (a `finish`, which
            // re-enters `release_gated`) may have completed or replaced
            // later entries.
            let Some(p) = self.rd.parked.remove(&id) else {
                continue;
            };
            let waited = now.since(p.since).max(0) as u64;
            if p.durable.is_some() {
                self.stats.ack_wait_ms_total += waited;
                let req = match &p.what {
                    ParkedWhat::Reply { req, .. } => req.map(|r| r.0),
                    _ => None,
                };
                tracing::trace!(
                    target: "constellation_authority::ack_wait",
                    node = self.cfg.node_id,
                    waited_ms = waited,
                    need = p.durable,
                    durable = self.durable_jseq(),
                    req,
                    "parked acknowledgement released"
                );
            } else {
                self.stats.recall_wait_ms_total += waited;
            }
            match p.what {
                ParkedWhat::Reply {
                    to,
                    req,
                    rid,
                    outcome,
                    base,
                    position,
                    gen,
                    held_timer,
                } => {
                    self.rd.parked_rids.remove(&rid);
                    if let Some(t) = held_timer {
                        self.cancel_timer(t, out);
                    }
                    if let Some(req) = req {
                        out.push(Action::Send {
                            to,
                            msg: PeerMsg::MutateReply {
                                req,
                                outcome,
                                base,
                                position,
                                gen,
                            },
                        });
                    }
                    // Answered `Held` already: the retry is answered from
                    // the holder's dedup.
                }
                ParkedWhat::Finish { rid, outcome } => self.finish(now, rid, outcome, replica, out),
                ParkedWhat::Control { op } => out.push(Action::ControlDone {
                    op,
                    result: Ok(ControlOk::Done),
                }),
                ParkedWhat::Release => self.release_after_recalls(now, replica, out),
                ParkedWhat::InboxRepoll { node } => self.inbox_repoll(now, node, out),
                ParkedWhat::ExecuteReply {
                    from,
                    req,
                    rid,
                    op,
                    acked_through,
                    deps,
                    held_timer,
                } => {
                    self.rd.parked_rids.remove(&rid);
                    if let Some(t) = held_timer {
                        self.cancel_timer(t, out);
                    }
                    // Executes now (the recall ended, `deps` are here); a
                    // retry answered `Held` meanwhile is answered from the
                    // dedup when it comes back.
                    self.on_mutate_request(
                        now,
                        from,
                        req.unwrap_or(OpId(0)),
                        rid,
                        op,
                        acked_through,
                        deps,
                        replica,
                        out,
                    );
                }
                ParkedWhat::DelegRecalled {
                    to,
                    req,
                    gen,
                    through,
                    locks,
                } => {
                    out.push(Action::Send {
                        to,
                        msg: PeerMsg::DelegRecalled {
                            req,
                            gen,
                            through,
                            locks,
                        },
                    });
                }
                ParkedWhat::ExecuteLocal { rid } => {
                    self.dl.pending_exec.remove(&rid);
                    if !self.clients.contains_key(&rid) {
                        continue;
                    }
                    if let Some(epoch) = self.lease.new_mutation_epoch(now, &self.cfg) {
                        let outcome =
                            self.resolve_in_doubt_then_execute(now, rid, epoch, replica, out);
                        tracing::debug!(
                            node = self.cfg.node_id,
                            ?rid,
                            ?outcome,
                            "parked local execution ran"
                        );
                        self.finish(now, rid, outcome, replica, out);
                    } else {
                        self.lease_path(now, rid, replica, out);
                    }
                }
            }
        }
    }

    /// `finish` found this op's local execution needs recalls (or
    /// durability) first.
    pub(crate) fn park_finish(
        &mut self,
        now: Ms,
        rid: Rid,
        wait: RecallWait,
        durable: Option<u64>,
        outcome: MutateOutcome,
    ) {
        if let Some(c) = self.clients.get_mut(&rid) {
            c.phase = ClientPhase::Recalling;
        }
        self.park(now, wait, durable, ParkedWhat::Finish { rid, outcome });
    }

    // ------------------------------------------------------------ delegate

    /// The sequencer recalls a delegation: stop honouring it, then ack.
    pub(crate) fn on_delegation_recall(
        &mut self,
        from: NodeId,
        req: OpId,
        ino: Ino,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        replica.read_delegations().recall(Some(ino));
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::DelegationRecalled { req },
        });
    }

    // ------------------------------------------------------------ reader

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_read_index_control(
        &mut self,
        now: Ms,
        op: OpId,
        ino: Ino,
        dir: bool,
        name: Option<String>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // Phase 2b (M8 under M11): the index of a delegated subtree is
        // its delegate's, the holder's included — the root's replica
        // trails the delegate's stream.
        let delegate = self
            .deleg_read_owner(now, ino, name.as_deref(), replica)
            .filter(|n| *n != self.cfg.node_id);
        if delegate.is_none() && self.lease.usable(now, &self.cfg) && !self.lease.fenced() {
            // Plan 30 §M9: a holder of a tenure that may be taken over
            // before its lease expires reads its own replica only with
            // fresh S3 liveness; otherwise this read probes first (a tail
            // to head reveals a deposition), then reads.
            if self.strict_answer_allowed(now, out) {
                self.read_answer(op, ReadAnswer::Holder, out);
            } else {
                self.read_tail(now, op, replica, out);
            }
            return;
        }
        if !self.cfg.p2p {
            // No ReadIndex without P2P: the log in S3 is the index.
            self.read_tail(now, op, replica, out);
            return;
        }
        let deadline = self.set_timer(
            now.plus(self.cfg.read_index_deadline_ms),
            Timer::ReadIndexDeadline(op),
            out,
        );
        self.rd.reads.insert(
            op,
            ReadReq {
                ino,
                dir,
                name,
                req: None,
                holder: 0,
                sent_at: now,
                gen: 0,
                attempts: 0,
                redirected: false,
                deadline,
            },
        );
        self.read_route(now, op, replica, out);
    }

    fn read_route(&mut self, now: Ms, op: OpId, replica: &dyn Replica, out: &mut Vec<Action>) {
        // Phase 2b (M8 under M11): a read under a delegated subtree asks
        // the delegate, not the holder; this node as that delegate reads
        // its own replica.
        let owner = self
            .rd
            .reads
            .get(&op)
            .and_then(|r| self.deleg_read_owner(now, r.ino, r.name.as_deref(), replica));
        match owner {
            Some(n) if n == self.cfg.node_id => {
                if let Some(r) = self.rd.reads.remove(&op) {
                    self.cancel_timer(r.deadline, out);
                }
                self.stats.deleg_read_index_served += 1;
                self.read_answer(op, ReadAnswer::Holder, out);
                return;
            }
            Some(n) => {
                self.send_read_index(now, op, n, replica, out);
                return;
            }
            None => {}
        }
        match self.lease.cached_holder.filter(|h| *h != self.cfg.node_id) {
            Some(holder) => self.send_read_index(now, op, holder, replica, out),
            None => {
                self.issue_s3(S3Op::LeaseGet, S3For::ReadHolder(op), out);
            }
        }
    }

    fn send_read_index(
        &mut self,
        now: Ms,
        op: OpId,
        holder: NodeId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let req = self.op_id();
        let timer_at = now.plus(self.cfg.forward_timeout_ms);
        let Some(r) = self.rd.reads.get_mut(&op) else {
            return;
        };
        r.req = Some(req);
        r.holder = holder;
        r.sent_at = now;
        r.gen = replica.read_delegations().recall_gen();
        let msg = PeerMsg::ReadIndex {
            req,
            ino: r.ino,
            dir: r.dir,
            name: r.name.clone(),
        };
        self.rd.by_req.insert(req, op);
        self.set_timer(timer_at, Timer::ReadIndexTimeout(req), out);
        self.stats.read_index_sent += 1;
        out.push(Action::Send { to: holder, msg });
    }

    pub(crate) fn on_read_holder_learned(
        &mut self,
        now: Ms,
        op: OpId,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.rd.reads.contains_key(&op) {
            return;
        }
        let object = match result {
            S3Result::LeaseGet(Ok(Some((lease, _)))) => {
                self.lease.note_object(now, &lease);
                Some(lease)
            }
            S3Result::LeaseGet(Ok(None)) => None,
            _ => {
                self.read_retry(now, op, out);
                return;
            }
        };
        let live = object.filter(|l| l.holder != 0 && !l.is_claimable(now.0));
        match live {
            Some(lease) if lease.holder != self.cfg.node_id => {
                self.send_read_index(now, op, lease.holder, replica, out)
            }
            // Our own live lease while our view is closed (a gate, a
            // release): ask again shortly.
            Some(_) => self.read_retry(now, op, out),
            // Nobody holds it: every acknowledged write is in the log
            // (or was lost with an unshipped tenure, which nothing may be
            // owed). Tail to head and read.
            None => {
                let Some(r) = self.rd.reads.remove(&op) else {
                    return;
                };
                self.cancel_timer(r.deadline, out);
                self.read_tail(now, op, replica, out);
            }
        }
    }

    pub(crate) fn on_read_index_reply(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        outcome: ReadIndexOutcome,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(op) = self.rd.by_req.remove(&req) else {
            return;
        };
        if self.rd.reads.get(&op).is_none_or(|r| r.req != Some(req)) {
            return;
        }
        self.note_p2p_result(now, from, true);
        let Some(r) = self.rd.reads.get_mut(&op) else {
            return;
        };
        r.req = None;
        match outcome {
            ReadIndexOutcome::Ok { position, grant } => {
                // Phase 2b: a delegate's answer (its position names its
                // stream) says nothing about the lease.
                if position.streams.is_empty() {
                    self.lease.cached_holder = Some(from);
                }
                let Some(r) = self.rd.reads.remove(&op) else {
                    return;
                };
                self.cancel_timer(r.deadline, out);
                let mut delegated = false;
                if let Some(g) = grant {
                    let margin = self.cfg.expiry_margin_ms as i64;
                    let ttl = g.ttl_ms as i64;
                    let held = HeldDelegation {
                        until_ms: r.sent_at.0 + ttl - margin,
                        position,
                        epoch: g.epoch,
                        holder: from,
                        grant: g.id,
                        renew_at_ms: r.sent_at.0 + ttl / 2,
                    };
                    if held.until_ms > now.0 {
                        delegated = replica.read_delegations().install(r.ino, held, r.gen);
                    }
                }
                if !delegated {
                    replica.read_delegations().renewal_done(r.ino);
                }
                self.stats.read_index_answered += 1;
                self.read_answer(
                    op,
                    ReadAnswer::Position {
                        position,
                        delegated,
                    },
                    out,
                );
            }
            ReadIndexOutcome::NotHolder { holder } => {
                let redirect = holder != 0 && holder != from && holder != self.cfg.node_id;
                // Phase 2b: a redirect to a delegate is a route, not the
                // lease holder.
                let names_delegate =
                    holder != 0 && replica.delegation_table().iter().any(|e| e.node == holder);
                if redirect && !r.redirected {
                    r.redirected = true;
                    if !names_delegate {
                        self.lease.cached_holder = Some(holder);
                    }
                    self.send_read_index(now, op, holder, replica, out);
                } else {
                    self.lease.cached_holder = None;
                    self.read_retry(now, op, out);
                }
            }
            ReadIndexOutcome::Busy => self.read_retry(now, op, out),
        }
    }

    /// A read request failed at the transport, or its reply timed out.
    pub(crate) fn on_read_request_failed(
        &mut self,
        now: Ms,
        req: OpId,
        to: NodeId,
        outage: bool,
        out: &mut Vec<Action>,
    ) -> bool {
        if let Some(id) = self.rd.recall_by_req.get(&req).copied() {
            // A recall that did not arrive: the grant's expiry answers it.
            tracing::debug!(
                node = self.cfg.node_id,
                delegate = to,
                grant = id,
                "a read-delegation recall failed at the transport; outwaiting the grant"
            );
            return true;
        }
        let Some(op) = self.rd.by_req.remove(&req) else {
            return false;
        };
        self.note_p2p_result(now, to, !outage);
        if let Some(r) = self.rd.reads.get_mut(&op) {
            if r.req == Some(req) {
                r.req = None;
                self.lease.cached_holder = None;
                self.read_retry(now, op, out);
            }
        }
        true
    }

    pub(crate) fn on_read_index_timeout(&mut self, now: Ms, req: OpId, out: &mut Vec<Action>) {
        let Some(op) = self.rd.by_req.remove(&req) else {
            return;
        };
        if let Some(r) = self.rd.reads.get_mut(&op) {
            if r.req == Some(req) {
                r.req = None;
                self.read_retry(now, op, out);
            }
        }
    }

    fn read_retry(&mut self, now: Ms, op: OpId, out: &mut Vec<Action>) {
        let Some(r) = self.rd.reads.get_mut(&op) else {
            return;
        };
        r.attempts += 1;
        let delay = (self.cfg.forward_backoff_ms / 4).max(10) * u64::from(r.attempts.min(8));
        self.set_timer(now.plus(delay), Timer::ReadIndexRetry(op), out);
    }

    pub(crate) fn on_read_index_retry(
        &mut self,
        now: Ms,
        op: OpId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.rd.reads.get(&op).is_some_and(|r| r.req.is_none()) {
            return;
        }
        if self.lease.usable(now, &self.cfg) && !self.lease.fenced() {
            let Some(r) = self.rd.reads.remove(&op) else {
                return;
            };
            self.cancel_timer(r.deadline, out);
            if self.strict_answer_allowed(now, out) {
                self.read_answer(op, ReadAnswer::Holder, out);
            } else {
                self.read_tail(now, op, replica, out);
            }
            return;
        }
        self.read_route(now, op, replica, out);
    }

    pub(crate) fn on_read_index_deadline(
        &mut self,
        op: OpId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(r) = self.rd.reads.remove(&op) else {
            return;
        };
        if let Some(req) = r.req {
            self.rd.by_req.remove(&req);
        }
        replica.read_delegations().renewal_done(r.ino);
        self.stats.read_index_degraded += 1;
        tracing::debug!(
            node = self.cfg.node_id,
            ino = r.ino,
            "strict read: no sequencer answer within the budget; reading the replica (degraded)"
        );
        self.read_answer(op, ReadAnswer::Degraded, out);
    }

    /// Answer strict read `op` with a tail to head: joins a queued tail
    /// that has not started yet (it will read everything the read is
    /// owed), else queues one. One S3 tail answers every strict read
    /// that arrived while the previous one ran.
    fn read_tail(&mut self, now: Ms, op: OpId, replica: &dyn Replica, out: &mut Vec<Action>) {
        self.stats.read_index_tailed += 1;
        if let Some(leader) = self.rd.read_tail_open {
            self.rd.read_tails.entry(leader).or_default().push(op);
            return;
        }
        self.rd.read_tail_open = Some(op);
        self.rd.read_tails.insert(op, Vec::new());
        self.enqueue_job(
            now,
            super::jobs::JobReq::TailToHead { control: op },
            replica,
            out,
        );
    }

    /// The tail job `control` ended: answer the reads that joined it.
    pub(crate) fn read_tail_done(
        &mut self,
        control: OpId,
        result: &Result<ControlOk, String>,
        out: &mut Vec<Action>,
    ) {
        if self.rd.read_tail_open == Some(control) {
            self.rd.read_tail_open = None;
        }
        for op in self.rd.read_tails.remove(&control).unwrap_or_default() {
            out.push(Action::ControlDone {
                op,
                result: result.clone(),
            });
        }
    }

    fn read_answer(&mut self, op: OpId, answer: ReadAnswer, out: &mut Vec<Action>) {
        out.push(Action::ControlDone {
            op,
            result: Ok(ControlOk::ReadIndex(answer)),
        });
    }
}
