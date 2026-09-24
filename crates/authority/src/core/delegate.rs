//! Plan 30 §M11: delegated sub-sequencers over one log.
//!
//! Two sides live here. **A delegate** (a node the log's
//! `Delegate{dir, node, gen}` record names) executes every mutation whose
//! keys fall under `dir` on its own replica — authoritative for the
//! subtree, because every write there goes through it — journals the
//! transaction with the generation's next stream index and the
//! requester's `deps`, acknowledges, and streams the transactions to the
//! root in stream order (`DelegateStream`, one batch in flight per
//! generation). It honours its grant only while its clock is below
//! `sent + ttl − margin` measured from its own renewal request (M8's
//! discipline), renews at half the ttl, and stops the moment the root
//! recalls the generation. Its journal rows are speculation like a
//! holder's: `Meta` retires them when a segment carries their origin
//! and strands them when the log recalls the generation (rolled back,
//! replayed by rid through the new owner).
//!
//! **The root** (the lease holder) appends a delegate's stream in index
//! order once the generation is live and the batch's `deps` are in its
//! replica — no re-validation — and journals it with the delegate's
//! origin, so the segment carries it and every replica (the delegate
//! included) learns its per-generation applied index. Before it executes
//! anything whose keys fall under a live delegation (a cross-subtree
//! rename, an op forwarded by a node with a stale table, an
//! `Undelegate`) it recalls the generation: `DelegRecall`, then the
//! delegate's `DelegRecalled{through}` and the stream catching up to
//! `through`, or the grant's expiry on the root's clock (granted at
//! `g`: `g + ttl + margin`) — after which the generation *ends*: a
//! `Recall{dir, gen}` record journaled, every later stream record of it
//! refused. An unrenewed grant is reclaimed the same way (a crashed
//! delegate is never recalled by an op alone). No renewal is granted
//! once a recall or a reclaim began.
//!
//! The rules the model made load-bearing (`crates/model/src/delegation.rs`,
//! "Rules the model found") are each a line below: the delegate waits
//! for `deps` too (its own readers see its speculation); a dependency on
//! an ended generation is void (`Replica::reaches`); an acknowledgement
//! from an ended stream is tentative (`Meta` strands the row; the
//! requester replays by rid); the initial grant is capped like every
//! renewal by the root lease, so an inherited grant is dead at a takeover
//! and the successor may reclaim at once; the root reclaims an unrenewed
//! grant on its own.
//!
//! Positions: a delegate's acknowledgement carries `(gen, idx)` in the
//! position's `streams` (M6's watermark with a per-stream pending part);
//! the root's carries every generation's cursor. A requester whose
//! observed streams are full sends its op to the root instead
//! (`deps_overflow_to_root`, coordinator decision 4).
//!
//! Continuation epochs (M10): no delegation inside an epoch. When the
//! driver reports an active epoch the root recalls every live generation
//! (over P2P, which an epoch has by definition) and refuses `Delegate`;
//! a delegate that learns of an epoch stops executing. Delegation resumes
//! by an operator's `delegate` after the epoch closed.

use super::{Core, Timer};
use crate::action::{Action, ControlOk};
use crate::event::PeerMsg;
use crate::ids::{Ms, NodeId, OpId, TimerId};
use crate::replica::Replica;
use constellation_fs_core::Ino;
use constellation_meta::delegation::Ownership;
use constellation_meta::{LogRecord, MetaError, MutateOp, MutateOutcome, Position, Rid, TouchSet};
use std::collections::{BTreeMap, BTreeSet};

/// Parked-wait ids for generations (above every read-delegation grant
/// id; `Parked::waiting` holds both kinds).
pub(crate) const DELEG_WAIT_BASE: u64 = 1 << 62;

fn wait_id(gen: u64) -> u64 {
    DELEG_WAIT_BASE + gen
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecallPhase {
    None,
    Sent,
    /// `DelegRecalled { through }` received: ends once the cursor is there.
    Drained(u64),
}

/// The root's view of one generation.
#[derive(Debug, Clone)]
pub(crate) struct GenState {
    pub dir: Ino,
    pub node: NodeId,
    /// Appended through this stream index.
    pub cursor: u64,
    /// The root outwaits the delegate once its clock reaches this.
    pub until: Ms,
    pub recall: RecallPhase,
    pub ended: bool,
    pub expiry: Option<TimerId>,
    pub recall_req: Option<OpId>,
    /// `Undelegate` controls answered when the generation ends.
    pub controls: Vec<OpId>,
}

/// An op a delegate holds until its `deps` are here or its grant is
/// renewed.
#[derive(Debug, Clone)]
pub(crate) struct ParkedDeleg {
    /// `0`: this node's own client op (its `ClientOp` is in `clients`).
    pub from: NodeId,
    pub req: Option<OpId>,
    pub rid: Rid,
    pub op: MutateOp,
    pub deps: Position,
    /// The requester's M2 receipt, forwarded on when the op re-enters
    /// the holder path (`answer_not_owner` → the requester re-sends).
    #[allow(dead_code)]
    pub acked_through: u64,
}

/// This node as the delegate of one generation.
#[derive(Debug, Clone)]
pub(crate) struct DelegateState {
    pub dir: Ino,
    pub gen: u64,
    /// Honoured while this node's clock is below this (`Ms(0)`: not
    /// renewed yet).
    pub until: Ms,
    pub stopped: bool,
    /// The stream's next attempt is not before this (a failed send backs
    /// off; the root unreachable must not mean a send per event).
    pub stream_after: Option<Ms>,
    /// The current backoff, doubled per failure up to a bound.
    pub stream_backoff_ms: u64,
    /// The root refused the stream (the generation is ending or unknown
    /// to it): nothing more is streamed until the log says.
    pub refused: bool,
    /// The root acknowledged the stream through this index.
    pub streamed_through: u64,
    /// The batch in flight: its request id and last index.
    pub inflight: Option<(OpId, u64)>,
    /// A renewal in flight: its request id and this node's clock when
    /// it was sent.
    pub renew: Option<(OpId, Ms)>,
    pub renew_timer: Option<TimerId>,
    pub parked: Vec<ParkedDeleg>,
    /// Executed under this generation here (for `status`).
    pub executed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReqKind {
    Stream,
    Renew,
    Recall,
}

#[derive(Debug, Default)]
pub(crate) struct DelegationState {
    /// The root's generations (this node holds the lease).
    pub gens: BTreeMap<u64, GenState>,
    /// The generations this node is the delegate of.
    pub mine: BTreeMap<u64, DelegateState>,
    by_req: BTreeMap<OpId, (u64, ReqKind)>,
    /// Local ops whose execution waits for a recall or for `deps` (their
    /// `finish` is deferred to the parked continuation).
    pub pending_exec: BTreeSet<Rid>,
    stream_timer: Option<TimerId>,
    /// Plan 30 §M10: an active continuation epoch — no delegation.
    epoch_active: bool,
}

/// Plan 30 §M11's view for `status`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DelegView {
    /// `(dir, gen, until_ms, stopped, streamed_through, executed, parked)`.
    pub mine: Vec<(Ino, u64, i64, bool, u64, u64, usize)>,
    /// `(dir, node, gen, cursor, until_ms, recall, ended)`.
    pub gens: Vec<(Ino, NodeId, u64, u64, i64, String, bool)>,
    pub pending_exec: usize,
}

/// The keys `op` touches, as ownership is resolved over them.
pub(crate) fn op_keys(op: &MutateOp) -> TouchSet {
    super::holder::keys_of_op(op)
}

impl Core {
    pub fn deleg_view(&self) -> DelegView {
        DelegView {
            mine: self
                .dl
                .mine
                .values()
                .map(|d| {
                    (
                        d.dir,
                        d.gen,
                        d.until.0,
                        d.stopped,
                        d.streamed_through,
                        d.executed,
                        d.parked.len(),
                    )
                })
                .collect(),
            gens: self
                .dl
                .gens
                .values()
                .map(|g| {
                    (
                        g.dir,
                        g.node,
                        gen_of(g, &self.dl.gens),
                        g.cursor,
                        g.until.0,
                        format!("{:?}", g.recall),
                        g.ended,
                    )
                })
                .collect(),
            pending_exec: self.dl.pending_exec.len(),
        }
    }

    fn me(&self) -> NodeId {
        self.cfg.node_id
    }

    /// Generations this root has not ended.
    pub(crate) fn deleg_live_generations(&self) -> usize {
        self.dl.gens.values().filter(|g| !g.ended).count()
    }

    /// The root this node streams to and renews with: the lease holder
    /// as last learned.
    fn root_node(&self) -> Option<NodeId> {
        self.lease
            .cached_holder
            .or(self.lease.last_seen.as_ref().map(|l| l.holder))
            .filter(|h| *h != 0 && *h != self.cfg.node_id)
    }

    /// This node holds the root lease usably (may append, recall, grant).
    fn root_usable(&self, now: Ms) -> bool {
        self.lease.ship_epoch(now, &self.cfg).is_some() && !self.lease.fenced()
    }

    // ----------------------------------------------------- the table

    /// The table changed (a segment applied, a record journaled here, the
    /// lease changed hands): install the delegations that name this node,
    /// drop the ones the log ended, and — as the root — learn the
    /// generations a predecessor left live.
    pub(crate) fn delegation_sync(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.cfg.delegation {
            return;
        }
        let table = replica.delegation_table();
        let me = self.me();
        // The delegate side.
        let live_mine: BTreeSet<u64> = table
            .iter()
            .filter(|d| d.node == me)
            .map(|d| d.gen)
            .collect();
        let ended: Vec<u64> = self
            .dl
            .mine
            .keys()
            .copied()
            .filter(|g| !live_mine.contains(g))
            .collect();
        for gen in ended {
            self.drop_delegate_state(now, gen, replica, out);
        }
        for d in table.iter().filter(|d| d.node == me) {
            if self.dl.mine.contains_key(&d.gen) || self.dl.epoch_active {
                continue;
            }
            tracing::info!(node = me, dir = d.dir, gen = d.gen, "delegation installed");
            self.stats.deleg_installed += 1;
            self.dl.mine.insert(
                d.gen,
                DelegateState {
                    dir: d.dir,
                    gen: d.gen,
                    until: Ms(0),
                    stopped: false,
                    stream_after: None,
                    stream_backoff_ms: 0,
                    refused: false,
                    streamed_through: replica.stream_applied(d.gen),
                    inflight: None,
                    renew: None,
                    renew_timer: None,
                    parked: Vec::new(),
                    executed: 0,
                },
            );
            self.deleg_renew_now(now, d.gen, out);
        }
        // The root side.
        if self.root_usable(now) {
            let live: BTreeSet<u64> = table.iter().map(|d| d.gen).collect();
            for d in table.iter() {
                if self.dl.gens.contains_key(&d.gen) {
                    continue;
                }
                // Inherited from a predecessor root. Its grant was capped
                // by that lease (the model's `cap_by_lease`), which a TTL
                // takeover has outwaited; a seal-based takeover of an
                // unexpired lease (M9) has not, so the conservative
                // horizon is waited before a reclaim — a renewal from the
                // delegate ends the wait sooner.
                let cursor = replica.stream_applied(d.gen);
                self.dl.gens.insert(
                    d.gen,
                    GenState {
                        dir: d.dir,
                        node: d.node,
                        cursor,
                        until: now.plus(self.cfg.delegation_ttl_ms + self.cfg.expiry_margin_ms),
                        recall: RecallPhase::None,
                        ended: false,
                        expiry: None,
                        recall_req: None,
                        controls: Vec::new(),
                    },
                );
                self.arm_gen_expiry(now, d.gen, out);
            }
            let gone: Vec<u64> = self
                .dl
                .gens
                .iter()
                .filter(|(g, s)| !s.ended && !live.contains(g))
                .map(|(g, _)| *g)
                .collect();
            for gen in gone {
                // Ended by a record this node did not write (a
                // predecessor's, applied from the log).
                self.mark_ended(now, gen, replica, out);
            }
            if self.dl.epoch_active {
                self.recall_all(now, out);
            }
        } else if !self.dl.gens.is_empty() {
            for (_, g) in std::mem::take(&mut self.dl.gens) {
                if let Some(t) = g.expiry {
                    self.cancel_timer(t, out);
                }
                for c in g.controls {
                    out.push(Action::ControlDone {
                        op: c,
                        result: Err("the lease was lost before the recall ended".into()),
                    });
                }
            }
        }
        self.deleg_after_event(now, replica, out);
    }

    fn drop_delegate_state(
        &mut self,
        now: Ms,
        gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(d) = self.dl.mine.remove(&gen) else {
            return;
        };
        tracing::info!(node = self.me(), dir = d.dir, gen, "delegation ended here");
        if let Some(t) = d.renew_timer {
            self.cancel_timer(t, out);
        }
        if let Some((req, _)) = d.inflight {
            self.dl.by_req.remove(&req);
        }
        if let Some((req, _)) = d.renew {
            self.dl.by_req.remove(&req);
        }
        let root = self.root_node().unwrap_or(0);
        for p in d.parked {
            self.answer_not_owner(now, p, root, replica, out);
        }
    }

    /// A parked op cannot execute here any more: the requester
    /// re-resolves (a redirect to the root, which recalls if it must).
    fn answer_not_owner(
        &mut self,
        now: Ms,
        p: ParkedDeleg,
        holder: NodeId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.stats.deleg_not_owner += 1;
        if p.from == 0 {
            // Own client op: back through the ordinary route.
            if self.clients.contains_key(&p.rid) {
                self.route(now, p.rid, replica, out);
            }
            return;
        }
        if let Some(req) = p.req {
            out.push(Action::Send {
                to: p.from,
                msg: PeerMsg::MutateReply {
                    req,
                    outcome: MutateOutcome::NotHolder { holder },
                    base: None,
                    position: Position::ZERO,
                    gen: 0,
                },
            });
        }
    }

    // ----------------------------------------------------- the delegate

    /// Whether `op` is this node's to execute as a delegate right now:
    /// the generation that owns every key, if it is one of this node's,
    /// not stopped, and its grant honoured.
    fn my_generation_for(&self, keys: &TouchSet, replica: &dyn Replica) -> Option<u64> {
        if self.dl.mine.is_empty() {
            return None;
        }
        match replica.resolve_ownership(keys) {
            Ownership::Delegated(d) if d.node == self.me() => {
                let s = self.dl.mine.get(&d.gen)?;
                (!s.stopped).then_some(d.gen)
            }
            _ => None,
        }
    }

    /// Plan 30 §M9's journaled refusal, as a delegate: a transaction of
    /// the stream (the root appends it; a retry by rid anywhere finds it).
    fn record_delegate_refusal(
        &mut self,
        rid: Rid,
        errno: i32,
        gen: u64,
        deps: Position,
        replica: &dyn Replica,
    ) {
        if let Err(error) = replica.delegate_refusal(rid, errno, gen, deps) {
            tracing::warn!(node = self.me(), ?rid, errno, %error, "could not journal a delegate refusal");
            return;
        }
        self.stats.refusals_journaled += 1;
        if let Some(d) = self.dl.mine.get_mut(&gen) {
            d.executed += 1;
        }
    }

    /// Execute `op` here as a delegate if it is this node's to execute
    /// (`true`), parking it while its `deps` are missing or the grant is
    /// not honoured. `from == 0` is this node's own client op.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn delegate_try_execute(
        &mut self,
        now: Ms,
        from: NodeId,
        req: Option<OpId>,
        rid: Rid,
        op: &MutateOp,
        deps: Position,
        acked_through: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        let keys = op_keys(op);
        let Some(gen) = self.my_generation_for(&keys, replica) else {
            return false;
        };
        let honoured = self.dl.mine.get(&gen).is_some_and(|d| now < d.until);
        // The model's rule 1: the delegate waits for `deps` too.
        let reaches = replica.reaches(&deps);
        if !honoured || !reaches {
            tracing::debug!(
                node = self.me(),
                gen,
                ?rid,
                honoured,
                reaches,
                ?deps,
                applied = ?replica.applied_position(),
                "delegate parks an op"
            );
            if !reaches {
                self.stats.deleg_deps_waits += 1;
            } else {
                self.stats.deleg_parked_expired += 1;
            }
            let d = self.dl.mine.get_mut(&gen).expect("present");
            d.parked.push(ParkedDeleg {
                from,
                req,
                rid,
                op: op.clone(),
                deps,
                acked_through,
            });
            if from == 0 {
                if let Some(c) = self.clients.get_mut(&rid) {
                    c.phase = super::client::Phase::Recalling;
                }
            }
            if !honoured {
                self.deleg_renew_now(now, gen, out);
            }
            return true;
        }
        self.delegate_execute_now(now, gen, from, req, rid, op, deps, replica, out);
        true
    }

    #[allow(clippy::too_many_arguments)]
    fn delegate_execute_now(
        &mut self,
        now: Ms,
        gen: u64,
        from: NodeId,
        req: Option<OpId>,
        rid: Rid,
        op: &MutateOp,
        deps: Position,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let base = self.reply_base(op, replica);
        // The requester installs an accepted reply as a shadow under
        // this epoch: the root's, as this delegate knows it, so that the
        // shadow strands at a root takeover (conservative: the record
        // re-streams and the replay dedups) and never before.
        let epoch = self.ship.max_epoch.max(1);
        let outcome = if let Some(records) = replica.recent_outcome(rid) {
            self.stats.forward_dedup_hits += 1;
            MutateOutcome::Accepted { epoch, records }
        } else if let Some(o) = replica.completed_outcome(rid).ok().flatten() {
            self.stats.forward_dedup_hits += 1;
            match o {
                constellation_meta::CompletedOutcome::Executed { .. } => MutateOutcome::Accepted {
                    epoch,
                    records: Vec::new(),
                },
                constellation_meta::CompletedOutcome::Refused { errno } => {
                    MutateOutcome::Errno(errno)
                }
            }
        } else {
            match replica.delegate_execute(op, Some(rid), gen, deps) {
                Ok((records, _idx)) => {
                    replica.remember_outcome(rid, &records);
                    self.stats.deleg_executed += 1;
                    if let Some(d) = self.dl.mine.get_mut(&gen) {
                        d.executed += 1;
                    }
                    MutateOutcome::Accepted { epoch, records }
                }
                Err(MetaError::Conflict) => match op {
                    MutateOp::SetManifest { ino, .. } => MutateOutcome::Conflict {
                        manifest: replica.manifest(*ino),
                    },
                    _ => MutateOutcome::Errno(libc::EAGAIN),
                },
                Err(MetaError::Exists) => {
                    let errno = libc::EEXIST;
                    self.record_delegate_refusal(rid, errno, gen, deps, replica);
                    match super::client::named_child(op) {
                        Some((parent, name)) => match replica.entry_as_record(parent, name) {
                            Some(record) => MutateOutcome::Exists {
                                records: vec![record],
                                epoch,
                            },
                            None => MutateOutcome::Errno(errno),
                        },
                        None => MutateOutcome::Errno(errno),
                    }
                }
                Err(e) => {
                    let errno = super::client::meta_errno(&e);
                    self.record_delegate_refusal(rid, errno, gen, deps, replica);
                    MutateOutcome::Errno(errno)
                }
            }
        };
        let position = Position {
            seq: replica.applied_seq().unwrap_or(0),
            pending: None,
            streams: {
                let mut s = constellation_meta::Streams::NONE;
                s.raise(gen, replica.stream_applied(gen));
                s
            },
        };
        self.arm_stream_tick(now, out);
        if from == 0 {
            self.finish(now, rid, outcome, replica, out);
            return;
        }
        if let Some(req) = req {
            out.push(Action::Send {
                to: from,
                msg: PeerMsg::MutateReply {
                    req,
                    outcome,
                    base,
                    position,
                    gen,
                },
            });
        }
    }

    /// Retry every parked op whose wait is over, then stream and renew.
    pub(crate) fn deleg_after_event(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.dl.mine.is_empty() && self.dl.gens.is_empty() {
            return;
        }
        // Parked delegate ops.
        let gens: Vec<u64> = self.dl.mine.keys().copied().collect();
        for gen in gens {
            let Some(d) = self.dl.mine.get(&gen) else {
                continue;
            };
            if d.parked.is_empty() {
                continue;
            }
            let honoured = now < d.until && !d.stopped;
            let ready: Vec<usize> = d
                .parked
                .iter()
                .enumerate()
                .filter(|(_, p)| honoured && replica.reaches(&p.deps))
                .map(|(i, _)| i)
                .collect();
            if ready.is_empty() {
                continue;
            }
            let mut taken = Vec::new();
            let d = self.dl.mine.get_mut(&gen).expect("present");
            for i in ready.into_iter().rev() {
                taken.push(d.parked.remove(i));
            }
            taken.reverse();
            for p in taken {
                if p.from == 0 && !self.clients.contains_key(&p.rid) {
                    continue;
                }
                self.delegate_execute_now(
                    now, gen, p.from, p.req, p.rid, &p.op, p.deps, replica, out,
                );
            }
        }
        self.deleg_stream(now, replica, out);
        // Root-side parked executions waiting for `deps`.
        if self.has_deps_parks() {
            self.complete_ready(now, replica, out);
        }
    }

    fn arm_stream_tick(&mut self, now: Ms, out: &mut Vec<Action>) {
        if self.dl.stream_timer.is_none() {
            let t = self.set_timer(
                now.plus(self.cfg.delegation_stream_tick_ms),
                Timer::DelegStream,
                out,
            );
            self.dl.stream_timer = Some(t);
        }
    }

    pub(crate) fn on_deleg_stream_timer(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.dl.stream_timer = None;
        self.deleg_stream(now, replica, out);
    }

    /// Send the next batch of every generation without one in flight.
    fn deleg_stream(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.dl.mine.is_empty() {
            return;
        }
        let Some(root) = self.root_node() else {
            // Learn the holder; the next event streams.
            self.issue_s3(
                crate::action::S3Op::LeaseGet,
                super::S3For::RefreshHolder,
                out,
            );
            self.arm_stream_tick(now, out);
            return;
        };
        let gens: Vec<u64> = self.dl.mine.keys().copied().collect();
        for gen in gens {
            let d = self.dl.mine.get(&gen).expect("present");
            if d.inflight.is_some() || d.refused || d.stream_after.is_some_and(|t| now < t) {
                continue;
            }
            let from = d.streamed_through + 1;
            let txs = replica.delegate_txs_from(gen, from, self.cfg.delegation_stream_rows);
            if txs.is_empty() {
                continue;
            }
            let last = txs.last().map(|t| t.idx).unwrap_or(from);
            let req = self.op_id();
            self.dl.by_req.insert(req, (gen, ReqKind::Stream));
            self.stats.deleg_streamed_txs += txs.len() as u64;
            self.dl.mine.get_mut(&gen).expect("present").inflight = Some((req, last));
            tracing::debug!(
                node = self.me(),
                gen,
                from,
                last,
                root,
                "delegate stream batch"
            );
            out.push(Action::Send {
                to: root,
                msg: PeerMsg::DelegateStream { req, gen, txs },
            });
        }
        // A lost ack (the transport reports it), a batch that could not
        // be sent, or a generation backing off: the tick retries.
        if self
            .dl
            .mine
            .values()
            .any(|d| d.inflight.is_some() || d.stream_after.is_some_and(|t| now < t))
        {
            self.arm_stream_tick(now, out);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_delegate_stream_ack(
        &mut self,
        now: Ms,
        req: OpId,
        gen: u64,
        through: u64,
        refused: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some((g, ReqKind::Stream)) = self.dl.by_req.remove(&req) else {
            return;
        };
        if g != gen {
            return;
        }
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return;
        };
        d.inflight = None;
        if refused {
            // The generation is ending (or the root has not learned it
            // yet): stop streaming; the log says what happened.
            self.stats.deleg_stream_refused += 1;
            d.refused = true;
            return;
        }
        d.streamed_through = d.streamed_through.max(through);
        d.stream_after = None;
        d.stream_backoff_ms = 0;
        self.deleg_stream(now, replica, out);
    }

    /// A stream, renewal or recall request failed at the transport: let
    /// the tick retry it.
    pub(crate) fn on_deleg_request_failed(
        &mut self,
        now: Ms,
        req: OpId,
        out: &mut Vec<Action>,
    ) -> bool {
        let Some((gen, kind)) = self.dl.by_req.remove(&req) else {
            return false;
        };
        match kind {
            ReqKind::Stream => {
                let tick = self.cfg.delegation_stream_tick_ms.max(1);
                if let Some(d) = self.dl.mine.get_mut(&gen) {
                    d.inflight = None;
                    // Back off: the root is unreachable or slow, and the
                    // stream is retried on the tick, not per event
                    // (harness `delegate-partition`: 440k sends in a cut).
                    d.stream_backoff_ms = (d.stream_backoff_ms * 2).clamp(tick * 4, 2_000);
                    d.stream_after = Some(now.plus(d.stream_backoff_ms));
                }
                self.arm_stream_tick(now, out);
            }
            ReqKind::Renew => {
                if let Some(d) = self.dl.mine.get_mut(&gen) {
                    d.renew = None;
                    if d.renew_timer.is_none() {
                        let t = self.set_timer(
                            now.plus(self.cfg.delegation_stream_tick_ms),
                            Timer::DelegRenew(gen),
                            out,
                        );
                        self.dl.mine.get_mut(&gen).expect("present").renew_timer = Some(t);
                    }
                }
            }
            ReqKind::Recall => {
                // Outwaited by the expiry timer.
                if let Some(g) = self.dl.gens.get_mut(&gen) {
                    g.recall_req = None;
                }
            }
        }
        true
    }

    // ----------------------------------------------------- renewals

    fn deleg_renew_now(&mut self, now: Ms, gen: u64, out: &mut Vec<Action>) {
        let Some(root) = self.root_node() else {
            self.issue_s3(
                crate::action::S3Op::LeaseGet,
                super::S3For::RefreshHolder,
                out,
            );
            return;
        };
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return;
        };
        if d.renew.is_some() || d.stopped {
            return;
        }
        let req = self.op_id();
        let d = self.dl.mine.get_mut(&gen).expect("present");
        d.renew = Some((req, now));
        if let Some(t) = d.renew_timer.take() {
            self.cancel_timer(t, out);
        }
        self.dl.by_req.insert(req, (gen, ReqKind::Renew));
        out.push(Action::Send {
            to: root,
            msg: PeerMsg::DelegRenew { req, gen },
        });
    }

    pub(crate) fn on_deleg_renew_timer(&mut self, now: Ms, gen: u64, out: &mut Vec<Action>) {
        if let Some(d) = self.dl.mine.get_mut(&gen) {
            d.renew_timer = None;
        }
        self.deleg_renew_now(now, gen, out);
    }

    /// Root side: grant (or refuse) a renewal.
    pub(crate) fn on_deleg_renew(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        gen: u64,
        out: &mut Vec<Action>,
    ) {
        let mut ttl_ms = 0u64;
        if self.root_usable(now) && !self.dl.epoch_active {
            let cap = self.grant_cap_ms(now);
            if let Some(g) = self.dl.gens.get_mut(&gen) {
                if g.node == from && !g.ended && g.recall == RecallPhase::None {
                    ttl_ms = self.cfg.delegation_ttl_ms.min(cap);
                    if ttl_ms > 0 {
                        let until = now.plus(ttl_ms + self.cfg.expiry_margin_ms);
                        if until > g.until {
                            g.until = until;
                        }
                    }
                }
            }
            if ttl_ms > 0 {
                self.stats.deleg_renewals += 1;
                self.lease.touch(now);
                self.arm_gen_expiry(now, gen, out);
            } else {
                self.stats.deleg_renewals_refused += 1;
            }
        } else {
            self.stats.deleg_renewals_refused += 1;
        }
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::DelegRenewed { req, gen, ttl_ms },
        });
    }

    /// A grant never outlives the root lease's usable end (the cap).
    fn grant_cap_ms(&self, now: Ms) -> u64 {
        let Some((lease, _)) = &self.lease.held else {
            return 0;
        };
        if self.lease.epoch_held() {
            return self.cfg.delegation_ttl_ms;
        }
        let end = lease.expires_unix_ms - self.cfg.expiry_margin_ms as i64;
        (end - now.0).max(0) as u64
    }

    pub(crate) fn on_deleg_renewed(
        &mut self,
        now: Ms,
        req: OpId,
        gen: u64,
        ttl_ms: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some((g, ReqKind::Renew)) = self.dl.by_req.remove(&req) else {
            return;
        };
        if g != gen {
            return;
        }
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return;
        };
        let Some((_, sent)) = d.renew.take() else {
            return;
        };
        if ttl_ms == 0 {
            // Refused: the generation is ending; stop executing (the log
            // ends it), keep the parked ops for `NotHolder` then.
            self.stats.deleg_renewals_refused += 1;
            d.stopped = true;
            let parked = std::mem::take(&mut d.parked);
            let root = self.root_node().unwrap_or(0);
            for p in parked {
                self.answer_not_owner(now, p, root, replica, out);
            }
            return;
        }
        // Measured from the send (M8's discipline): honoured until
        // `sent + ttl − margin` on this clock.
        let until = sent.plus(ttl_ms.saturating_sub(self.cfg.expiry_margin_ms));
        if until > d.until {
            d.until = until;
        }
        // Renew at half the ttl.
        let at = sent.plus(ttl_ms / 2);
        let t = self.set_timer(at.max(now.plus(1)), Timer::DelegRenew(gen), out);
        self.dl.mine.get_mut(&gen).expect("present").renew_timer = Some(t);
        self.deleg_after_event(now, replica, out);
    }

    // ----------------------------------------------------- recalls (delegate)

    pub(crate) fn on_deleg_recall(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let through = replica.delegate_idx(gen);
        if let Some(d) = self.dl.mine.get_mut(&gen) {
            d.stopped = true;
            self.stats.deleg_recalls_received += 1;
            let parked = std::mem::take(&mut d.parked);
            for p in parked {
                self.answer_not_owner(now, p, from, replica, out);
            }
            // Drain: everything executed streams as usual; the root
            // waits for the cursor to reach `through`.
            self.deleg_stream(now, replica, out);
        }
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::DelegRecalled { req, gen, through },
        });
    }

    // ----------------------------------------------------- the root

    /// The generations `keys` fall under that this root must end before
    /// it executes: started (recalled) here; the caller parks on their
    /// wait ids. `None`: nothing to recall.
    pub(crate) fn deleg_recall_needed(
        &mut self,
        now: Ms,
        keys: &TouchSet,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Option<BTreeSet<u64>> {
        if !self.cfg.delegation || self.dl.gens.is_empty() {
            return None;
        }
        let involved: Vec<u64> = match replica.resolve_ownership(keys) {
            Ownership::Root => return None,
            Ownership::Delegated(d) => vec![d.gen],
            Ownership::CrossSubtree { involved, .. } => {
                self.stats.deleg_cross_subtree += 1;
                involved.iter().map(|d| d.gen).collect()
            }
        };
        let mut waiting = BTreeSet::new();
        for gen in involved {
            if self.dl.gens.get(&gen).is_some_and(|g| g.ended) {
                continue;
            }
            self.start_recall(now, gen, out);
            waiting.insert(wait_id(gen));
        }
        (!waiting.is_empty()).then_some(waiting)
    }

    fn start_recall(&mut self, now: Ms, gen: u64, out: &mut Vec<Action>) {
        let Some(g) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        if g.ended || g.recall != RecallPhase::None {
            return;
        }
        g.recall = RecallPhase::Sent;
        let (to, dir) = (g.node, g.dir);
        let req = self.op_id();
        self.dl.gens.get_mut(&gen).expect("present").recall_req = Some(req);
        self.dl.by_req.insert(req, (gen, ReqKind::Recall));
        self.stats.deleg_recalls_sent += 1;
        tracing::info!(
            node = self.me(),
            gen,
            dir,
            delegate = to,
            "recalling a delegation"
        );
        out.push(Action::Send {
            to,
            msg: PeerMsg::DelegRecall { req, dir, gen },
        });
        self.arm_gen_expiry(now, gen, out);
    }

    fn recall_all(&mut self, now: Ms, out: &mut Vec<Action>) {
        let gens: Vec<u64> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| !g.ended && g.recall == RecallPhase::None)
            .map(|(g, _)| *g)
            .collect();
        for gen in gens {
            self.start_recall(now, gen, out);
        }
    }

    fn arm_gen_expiry(&mut self, now: Ms, gen: u64, out: &mut Vec<Action>) {
        let Some(g) = self.dl.gens.get(&gen) else {
            return;
        };
        if g.ended {
            return;
        }
        let at = g.until.max(now);
        if let Some(t) = g.expiry {
            self.cancel_timer(t, out);
        }
        let t = self.set_timer(at, Timer::DelegExpiry(gen), out);
        self.dl.gens.get_mut(&gen).expect("present").expiry = Some(t);
    }

    pub(crate) fn on_deleg_expiry(
        &mut self,
        now: Ms,
        gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(g) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        g.expiry = None;
        if g.ended || !self.root_usable(now) {
            return;
        }
        let g = self.dl.gens.get(&gen).expect("present");
        if now < g.until {
            self.arm_gen_expiry(now, gen, out);
            return;
        }
        let done = matches!(g.recall, RecallPhase::Drained(th) if g.cursor >= th);
        if done {
            return;
        }
        // The grant is not honoured any more (the margin argument): a
        // pending recall is outwaited, an unrenewed grant reclaimed.
        if g.recall == RecallPhase::None {
            if !self.cfg.delegation_reclaim_expired {
                return;
            }
            self.stats.deleg_reclaimed += 1;
        } else {
            self.stats.deleg_recalls_expired += 1;
        }
        self.end_generation(now, gen, replica, out);
    }

    pub(crate) fn on_deleg_recalled(
        &mut self,
        now: Ms,
        req: OpId,
        gen: u64,
        through: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some((g, ReqKind::Recall)) = self.dl.by_req.remove(&req) else {
            return;
        };
        if g != gen {
            return;
        }
        let Some(gs) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        gs.recall_req = None;
        if gs.recall == RecallPhase::Sent {
            gs.recall = RecallPhase::Drained(through);
        }
        self.root_progress(now, replica, out);
    }

    /// A drained recall whose stream caught up ends the generation.
    fn root_progress(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        let done: Vec<u64> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| {
                !g.ended && matches!(g.recall, RecallPhase::Drained(th) if g.cursor >= th)
            })
            .map(|(g, _)| *g)
            .collect();
        for gen in done {
            self.stats.deleg_recalls_drained += 1;
            self.end_generation(now, gen, replica, out);
        }
    }

    /// End `gen` here and now: the `Recall` record journaled (the table
    /// row changes with it), later stream records of it refused, every
    /// parked execution waiting on it released.
    fn end_generation(&mut self, now: Ms, gen: u64, replica: &dyn Replica, out: &mut Vec<Action>) {
        let Some(g) = self.dl.gens.get(&gen) else {
            return;
        };
        if g.ended {
            return;
        }
        let dir = g.dir;
        let cursor = g.cursor;
        if let Err(error) = replica.apply_records_journaled(&[LogRecord::Recall { dir, gen }], None)
        {
            tracing::warn!(node = self.me(), gen, %error, "could not journal the recall record");
            self.arm_gen_expiry(now.plus(500), gen, out);
            return;
        }
        replica.void_stream(gen, cursor);
        self.lease.touch(now);
        self.nudge(now, out);
        tracing::info!(
            node = self.me(),
            gen,
            dir,
            cursor,
            "delegation generation ended"
        );
        self.mark_ended(now, gen, replica, out);
    }

    fn mark_ended(&mut self, now: Ms, gen: u64, replica: &dyn Replica, out: &mut Vec<Action>) {
        let Some(g) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        g.ended = true;
        let expiry = g.expiry.take();
        let recall_req = g.recall_req.take();
        let controls = std::mem::take(&mut g.controls);
        if let Some(t) = expiry {
            self.cancel_timer(t, out);
        }
        if let Some(req) = recall_req {
            self.dl.by_req.remove(&req);
        }
        self.stats.deleg_ended += 1;
        for c in controls {
            out.push(Action::ControlDone {
                op: c,
                result: Ok(ControlOk::Text(format!("generation {gen} ended"))),
            });
        }
        // Parked executions waiting on this generation.
        self.deleg_wait_done(now, wait_id(gen), replica, out);
    }

    /// The root appends a delegate's batch.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_delegate_stream(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        gen: u64,
        txs: Vec<constellation_meta::DelegateTx>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let usable = self.root_usable(now) && self.cfg.delegation;
        let refuse = |out: &mut Vec<Action>, through: u64| {
            out.push(Action::Send {
                to: from,
                msg: PeerMsg::DelegateStreamAck {
                    req,
                    gen,
                    through,
                    refused: true,
                },
            });
        };
        let Some(g) = self.dl.gens.get(&gen) else {
            self.stats.deleg_stream_refusals += 1;
            refuse(out, 0);
            return;
        };
        if !usable || g.ended || g.node != from {
            self.stats.deleg_stream_refusals += 1;
            refuse(out, g.cursor);
            return;
        }
        let mut cursor = g.cursor;
        let mut appended = 0u64;
        for tx in txs {
            if tx.idx <= cursor {
                continue;
            }
            if tx.idx != cursor + 1 {
                // A gap: the delegate re-sends from the cursor.
                break;
            }
            // The model's causal-cut assertion at the root: a delegate
            // waited for these before executing, so they are in the log
            // (and in this journal) already. Not so: refuse the batch
            // and count it; the delegate re-sends after a tick.
            if !replica.reaches_streams(&tx.deps) {
                self.stats.deleg_deps_unsatisfied_at_append += 1;
                break;
            }
            match replica.apply_delegate_tx(&tx.records, tx.rid, gen, tx.idx, tx.deps) {
                Ok(_) => {
                    cursor = tx.idx;
                    appended += 1;
                }
                Err(error) => {
                    tracing::warn!(node = self.me(), gen, idx = tx.idx, %error, "could not append a delegate transaction");
                    break;
                }
            }
        }
        if let Some(g) = self.dl.gens.get_mut(&gen) {
            g.cursor = cursor;
        }
        if appended > 0 {
            self.stats.deleg_appended_txs += appended;
            // The root's own ops forwarded to this delegate may await
            // the log for exactly these completions.
            self.answer_awaiting_log(now, replica, out);
            self.lease.touch(now);
            self.nudge(now, out);
        }
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::DelegateStreamAck {
                req,
                gen,
                through: cursor,
                refused: false,
            },
        });
        self.root_progress(now, replica, out);
        // Executions parked on these rows' deps.
        if appended > 0 && self.has_deps_parks() {
            self.complete_ready(now, replica, out);
        }
    }

    // ----------------------------------------------------- controls

    pub(crate) fn on_control_delegate(
        &mut self,
        now: Ms,
        op: OpId,
        dir: Ino,
        node: NodeId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let fail = |out: &mut Vec<Action>, msg: String| {
            out.push(Action::ControlDone {
                op,
                result: Err(msg),
            });
        };
        if !self.cfg.delegation {
            return fail(out, "delegation is off (CONSTELLATION_DELEGATION)".into());
        }
        if !self.cfg.p2p {
            return fail(out, "delegation needs P2P".into());
        }
        if !self.root_usable(now) {
            return fail(out, "this node does not hold the lease".into());
        }
        if self.dl.epoch_active || self.epoch.open {
            return fail(
                out,
                "no delegation while a continuation epoch is open".into(),
            );
        }
        if node == self.me() {
            return fail(out, "cannot delegate to the root itself".into());
        }
        let table = replica.delegation_table();
        if let Some(d) = table.owner_of_dir(replica.namespace(), dir) {
            return fail(
                out,
                format!(
                    "directory {dir} is under delegation {} (gen {})",
                    d.dir, d.gen
                ),
            );
        }
        if table.iter().any(|d| d.node == node) {
            return fail(out, format!("node {node} already holds a delegation"));
        }
        let highest = table.iter().map(|d| d.gen).max().unwrap_or(0);
        let gen = match replica.next_delegation_gen(highest + 1) {
            Ok(g) => g,
            Err(e) => return fail(out, e.to_string()),
        };
        if let Err(e) =
            replica.apply_records_journaled(&[LogRecord::Delegate { dir, node, gen }], None)
        {
            return fail(out, e.to_string());
        }
        self.dl.gens.insert(
            gen,
            GenState {
                dir,
                node,
                cursor: 0,
                until: now.plus(self.cfg.delegation_ttl_ms + self.cfg.expiry_margin_ms),
                recall: RecallPhase::None,
                ended: false,
                expiry: None,
                recall_req: None,
                controls: Vec::new(),
            },
        );
        self.arm_gen_expiry(now, gen, out);
        self.stats.deleg_delegated += 1;
        self.lease.touch(now);
        self.nudge(now, out);
        tracing::info!(node = self.me(), dir, delegate = node, gen, "delegated");
        out.push(Action::ControlDone {
            op,
            result: Ok(ControlOk::Text(format!(
                "delegated dir {dir} to node {node} (gen {gen})"
            ))),
        });
    }

    pub(crate) fn on_control_undelegate(
        &mut self,
        now: Ms,
        op: OpId,
        dir: Ino,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.root_usable(now) {
            out.push(Action::ControlDone {
                op,
                result: Err("this node does not hold the lease".into()),
            });
            return;
        }
        let gen = replica.delegation_table().get(dir).map(|d| d.gen);
        let Some(gen) = gen.filter(|g| self.dl.gens.contains_key(g)) else {
            out.push(Action::ControlDone {
                op,
                result: Err(format!("directory {dir} is not delegated")),
            });
            return;
        };
        self.dl
            .gens
            .get_mut(&gen)
            .expect("present")
            .controls
            .push(op);
        self.start_recall(now, gen, out);
    }

    /// Plan 30 §M10: an epoch became active (or closed).
    pub(crate) fn deleg_on_epoch(
        &mut self,
        now: Ms,
        active: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.dl.epoch_active == active {
            return;
        }
        self.dl.epoch_active = active;
        if !active {
            return;
        }
        // Delegates stop; the root recalls everything.
        let mine: Vec<u64> = self.dl.mine.keys().copied().collect();
        let root = self.root_node().unwrap_or(0);
        for gen in mine {
            if let Some(d) = self.dl.mine.get_mut(&gen) {
                d.stopped = true;
                let parked = std::mem::take(&mut d.parked);
                for p in parked {
                    self.answer_not_owner(now, p, root, replica, out);
                }
            }
        }
        if self.root_usable(now) {
            self.recall_all(now, out);
        }
    }

    /// The lease was lost or released: no generation is this node's to
    /// end any more (a successor learns them from the log).
    pub(crate) fn deleg_on_lease_gone(&mut self, out: &mut Vec<Action>) {
        for (_, g) in std::mem::take(&mut self.dl.gens) {
            if let Some(t) = g.expiry {
                self.cancel_timer(t, out);
            }
            if let Some(req) = g.recall_req {
                self.dl.by_req.remove(&req);
            }
            for c in g.controls {
                out.push(Action::ControlDone {
                    op: c,
                    result: Err("the lease was lost".into()),
                });
            }
        }
    }
}

fn gen_of(g: &GenState, gens: &BTreeMap<u64, GenState>) -> u64 {
    gens.iter()
        .find(|(_, s)| std::ptr::eq(*s, g))
        .map(|(k, _)| *k)
        .unwrap_or(0)
}
