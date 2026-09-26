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
use constellation_meta::delegation::{Ownership, Range};
use constellation_meta::{LogRecord, MetaError, MutateOp, MutateOutcome, Position, Rid, TouchSet};
use std::collections::{BTreeMap, BTreeSet};

/// Parked-wait ids for generations (above every read-delegation grant
/// id; `Parked::waiting` holds both kinds).
pub(crate) const DELEG_WAIT_BASE: u64 = 1 << 62;

pub(crate) fn wait_id(gen: u64) -> u64 {
    DELEG_WAIT_BASE + gen
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecallPhase {
    None,
    Sent,
    /// `DelegRecalled { through }` received: ends once the cursor is there.
    Drained(u64),
    /// Phase 2b: the delegate is silent; its backup was asked to seal
    /// and drain.
    Sealing,
}

/// Why a generation exists (phase 2b).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelegKind {
    /// `constellation delegate`: until `undelegate` or a cross-subtree op.
    Manual,
    /// The placement (`core::placement`): recalled by it too.
    Placed,
    /// An offline designation (plans 03–05): recalled only by `online`;
    /// never expires; a cross-subtree op touching it is refused.
    Designated,
}

/// The root's view of one generation.
#[derive(Debug, Clone)]
pub(crate) struct GenState {
    pub dir: Ino,
    pub node: NodeId,
    /// Plan 30 §M12: the part of `dir` (a hash range, or the whole).
    pub range: Range,
    /// Why it exists (phase 2b): an operator, the placement, a
    /// designation.
    pub kind: DelegKind,
    /// Phase 2b: the delegate's backup peer, as its renewals report it;
    /// the root drains it (seal) when the delegate dies.
    pub backup: Option<NodeId>,
    /// Phase 2b: ended by a cross-subtree op: delegate the directory to
    /// the same node again once the op executed.
    pub redelegate: bool,
    /// When the root granted it (placement's dwell counts from here).
    pub granted: Ms,
    /// Placement: since when the delegate's share has been below the
    /// leave threshold (`None`: it is not).
    pub below_since: Option<Ms>,
    /// Phase 2b: seals asked of the backup (one; then the plain reclaim).
    pub seal_attempts: u8,
    /// Appended through this stream index.
    pub cursor: u64,
    /// The root outwaits the delegate once its clock reaches this.
    pub until: Ms,
    pub recall: RecallPhase,
    /// The phase a seal of the backup replaced: what an unanswered seal
    /// goes back to.
    pub recall_before_seal: RecallPhase,
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
    /// Phase 2b: a designation — honoured whatever the clock says.
    pub designated: bool,
    /// Phase 2b: the root the stream last went to; a different one
    /// (a failover) gets every unretired transaction again.
    pub last_root: Option<NodeId>,
    /// Phase 2b: the backup peer this delegate appends to before it
    /// acknowledges, and what it has acknowledged.
    pub backup: Option<NodeId>,
    pub backup_acked: u64,
    pub backup_sent_through: u64,
    pub backup_inflight: Option<(OpId, u64)>,
    pub backup_sealed: bool,
    pub backup_failures: u32,
    /// The backup the in-flight renewal named (the root must learn a
    /// newly chosen backup before the next timer).
    pub renew_backup: Option<NodeId>,
    /// When the in-flight stream batch / backup append was sent: one
    /// lost in a partition (no transport failure) is re-sent after
    /// `deleg_request_timeout_ms`, not waited for forever.
    pub inflight_at: Ms,
    pub backup_inflight_at: Ms,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReqKind {
    Stream,
    Renew,
    Recall,
    /// Phase 2b: a delegate's append to its backup.
    BackupAppend,
    /// Phase 2b: the root's seal request to a dead delegate's backup.
    Seal,
}

impl DelegationState {
    pub(crate) fn container_sizes(&self) -> Vec<(&'static str, usize)> {
        vec![
            ("dl_gens", self.gens.len()),
            ("dl_mine", self.mine.len()),
            ("dl_by_req", self.by_req.len()),
            ("dl_pending_exec", self.pending_exec.len()),
            (
                "dl_parked",
                self.mine.values().map(|d| d.parked.len()).sum(),
            ),
            ("dl_backing", self.backing.len()),
            ("dl_sealed", self.sealed.len()),
        ]
    }
}

#[derive(Debug, Default)]
pub(crate) struct DelegationState {
    /// The root's generations (this node holds the lease).
    pub gens: BTreeMap<u64, GenState>,
    /// The generations this node is the delegate of.
    pub mine: BTreeMap<u64, DelegateState>,
    /// Phase 2b: the generations this node backs, `gen -> (delegate,
    /// acked)`, and the ones it sealed.
    pub backing: BTreeMap<u64, (NodeId, u64)>,
    pub sealed: BTreeSet<u64>,
    by_req: BTreeMap<OpId, (u64, ReqKind)>,
    /// Local ops whose execution waits for a recall or for `deps` (their
    /// `finish` is deferred to the parked continuation).
    pub pending_exec: BTreeSet<Rid>,
    stream_timer: Option<TimerId>,
    /// Plan 30 §M10: an active continuation epoch — no delegation.
    pub(crate) epoch_active: bool,
}

/// Plan 30 §M11's view for `status`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DelegView {
    /// `(dir, gen, until_ms, stopped, streamed_through, executed, parked)`.
    pub mine: Vec<(Ino, u64, i64, bool, u64, u64, usize)>,
    /// `(dir, node, gen, cursor, until_ms, recall, ended)`.
    pub gens: Vec<(Ino, NodeId, u64, u64, i64, String, bool)>,
    pub pending_exec: usize,
    /// Phase 2b: per generation held here, `(gen, backup, backup_acked)`.
    pub backups: Vec<(u64, NodeId, u64)>,
    /// Phase 2b: per generation on the root, `(gen, kind, backup)`.
    pub kinds: Vec<(u64, String, NodeId)>,
}

/// The keys `op` touches, as ownership is resolved over them.
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
            backups: self
                .dl
                .mine
                .values()
                .filter_map(|d| d.backup.map(|b| (d.gen, b, d.backup_acked)))
                .collect(),
            kinds: self
                .dl
                .gens
                .iter()
                .map(|(g, s)| (*g, format!("{:?}", s.kind), s.backup.unwrap_or(0)))
                .collect(),
        }
    }

    /// Phase 2b: the delegate's fast path is gated (a backup acknowledges
    /// first, or `ack=s3`): its writes go through the core and park.
    pub fn deleg_fast_path_gated(&self) -> bool {
        self.cfg.ack_s3 || self.dl.mine.values().any(|d| d.backup.is_some())
    }

    /// Phase 2b: the placement's busiest subtrees (`status`).
    pub fn placement_top(&self) -> Vec<(Ino, NodeId, u64, u64)> {
        self.pl.top.clone()
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
    pub(crate) fn root_node(&self) -> Option<NodeId> {
        self.lease
            .cached_holder
            .or(self.lease.last_seen.as_ref().map(|l| l.holder))
            .filter(|h| *h != 0 && *h != self.cfg.node_id)
    }

    /// This node holds the root lease usably (may append, recall, grant).
    pub(crate) fn root_usable(&self, now: Ms) -> bool {
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
            // A delegate's reply base is the last applied segment that
            // touched the op's keys (`reply_base`), and the window only
            // records segments applied while this node delegates: one
            // applied before this grant — the root's earlier writes in
            // the subtree — would be missing from it, and a reply naming
            // a lower base would let the requester install it on a
            // replica without them (chaos-soak-4 seed 42, `wf293`). The
            // floor covers everything applied so far.
            self.shipped_floor = self.shipped_floor.max(replica.applied_seq().unwrap_or(0));
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
                    streamed_through: replica.log_stream_idx(d.gen),
                    inflight: None,
                    renew: None,
                    renew_timer: None,
                    parked: Vec::new(),
                    executed: 0,
                    designated: d.designated,
                    last_root: self.root_node(),
                    backup: None,
                    backup_acked: 0,
                    backup_sent_through: 0,
                    backup_inflight: None,
                    backup_sealed: false,
                    backup_failures: 0,
                    renew_backup: None,
                    inflight_at: Ms(0),
                    backup_inflight_at: Ms(0),
                },
            );
            if d.designated {
                // Honoured whatever the root says or the clock does
                // (DESIGN.md §5.2: the designee writes while isolated).
                self.dl.mine.get_mut(&d.gen).expect("present").until = Ms(i64::MAX / 2);
            }
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
                // The log's index of the stream, never a shadow's (a
                // forwarded op's reply raised `stream_applied` past what
                // the predecessor appended; the delegate re-streams the
                // rest from here).
                let cursor = replica.log_stream_idx(d.gen);
                tracing::info!(
                    node = me,
                    gen = d.gen,
                    dir = d.dir,
                    delegate = d.node,
                    cursor,
                    stream_applied = replica.stream_applied(d.gen),
                    "inherited a delegation from a predecessor root"
                );
                self.dl.gens.insert(
                    d.gen,
                    GenState {
                        dir: d.dir,
                        node: d.node,
                        range: d.range,
                        kind: if d.designated {
                            DelegKind::Designated
                        } else {
                            DelegKind::Manual
                        },
                        backup: None,
                        redelegate: false,
                        granted: now,
                        below_since: None,
                        seal_attempts: 0,
                        cursor,
                        until: now.plus(self.reclaim_horizon_ms()),
                        recall: RecallPhase::None,
                        recall_before_seal: RecallPhase::None,
                        ended: false,
                        expiry: None,
                        recall_req: None,
                        controls: Vec::new(),
                    },
                );
                self.stats.deleg_inherited += 1;
                if d.node == me {
                    // A predecessor delegated this subtree to the node
                    // that is now the root: the root executes it itself.
                    self.end_generation(now, d.gen, replica, out);
                    continue;
                }
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
        } else if !self.dl.gens.is_empty() && (self.lease.held.is_none() || self.lease.lost) {
            // Only a lease that is gone ends this root's generations. One
            // held but unusable for the moment (a renewal or a gate in
            // flight, S3 away) keeps them, as `on_deleg_expiry` does: the
            // state dropped there was never learned again while the lease
            // stayed this node's, and with no generation known the root
            // executed ops under a live delegation itself — beside the
            // delegate (long-delegated seed 75808: an `EEXIST` evaluated
            // on the root's replica, which lacked the delegate's rename).
            self.deleg_on_lease_gone(now, replica, out);
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
        // Plan 30 §M14: whatever lock grants of the subtree are still
        // here (the log ended the generation before the recall message
        // did, or without one) are dropped — their holders keep
        // honouring them: the root reinstated its copies of what it
        // handed, and an outwaited generation's own grants were capped
        // by its tenure.
        let dropped = self.lock_take_generation(gen, replica);
        if !dropped.is_empty() {
            tracing::info!(
                node = self.me(),
                gen,
                n = dropped.len(),
                "delegation ended by the log with lock grants still here; dropped"
            );
        }
        self.lock_reserve_all(now, replica, out);
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
        let keys = super::holder::keys_of_op_in(op, replica);
        let Some(gen) = self.my_generation_for(&keys, replica) else {
            return false;
        };
        let honoured = self
            .dl
            .mine
            .get(&gen)
            .is_some_and(|d| now < d.until || d.designated);
        // The model's rule 1: the delegate waits for `deps` too.
        let reaches = replica.reaches(&deps);
        // Phase 2b (M8): the read delegations this delegate granted on
        // what the op touches are recalled first.
        let recalling =
            honoured && reaches && self.deleg_read_recall_pending(now, from, op, replica, out);
        if !honoured || !reaches || recalling {
            tracing::debug!(
                node = self.me(),
                gen,
                ?rid,
                honoured,
                reaches,
                recalling,
                ?deps,
                applied = ?replica.applied_position(),
                "delegate parks an op"
            );
            if !reaches {
                self.stats.deleg_deps_waits += 1;
            } else if !honoured {
                self.stats.deleg_parked_expired += 1;
            } else {
                self.stats.recall_waits += 1;
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

    /// Phase 2b (M8 under M11): a write this delegate executes must not
    /// leave a read delegation it granted on what the write touches
    /// live (the holder's rule, `recall_needed`, for the delegate's
    /// subtree; the requester's own grant excepted). `true` while a
    /// recall is in flight (started here, once).
    fn deleg_read_recall_pending(
        &mut self,
        now: Ms,
        from: NodeId,
        op: &MutateOp,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        if !self.cfg.read_delegations {
            return false;
        }
        let inos = replica.recall_inos_of_op(op);
        if inos.is_empty() {
            return false;
        }
        let except = Some(if from == 0 { self.me() } else { from });
        self.recall_needed(now, &inos, except, replica, out)
            .is_some()
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
        // A dependency its generation ended without (a tentative
        // acknowledgement, replayed by rid after the end): executing now
        // would put this op in the log ahead of its cause (`marker-order`,
        // long-delegated seed 74189). Not a retry of an op already done.
        if replica.deps_lost(&deps)
            && replica.recent_outcome(rid).is_none()
            && replica.completed_outcome(rid).ok().flatten().is_none()
        {
            if from == 0 {
                if self.hold_for_lost_deps(now, rid, replica, out) {
                    return;
                }
                let fresh = self.clients.get(&rid).map(|c| c.deps);
                if let Some(fresh) = fresh.filter(|d| !replica.deps_lost(d)) {
                    self.delegate_try_execute(now, 0, None, rid, op, fresh, 0, replica, out);
                }
                return;
            }
            self.stats.deps_lost_refused += 1;
            tracing::debug!(
                node = self.cfg.node_id,
                ?rid,
                ?deps,
                "an execution whose deps were lost with their generation is refused"
            );
            if let Some(req) = req {
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
            }
            return;
        }
        // chaos-soak-4 seed 42 (`wf293`): what this replica holds ahead of
        // its applied log (the root's pre-S3 stream, a shadow of its own)
        // is nowhere in the log the requester's `base` check reads, so
        // an op touching it is answered without a base: the requester
        // waits for the log instead of installing the reply on a replica
        // that may lack the earlier write (see
        // `a_delegate_reply_base_covers_what_it_applied_before_the_grant_and_streamed_state`).
        let base = if replica.speculation_touches(&super::holder::keys_of_op(op)) {
            None
        } else {
            self.reply_base(op, replica)
        };
        // The requester installs an accepted reply as a shadow under
        // this epoch: the root's, as this delegate knows it, so that the
        // shadow strands at a root takeover (conservative: the record
        // re-streams and the replay dedups) and never before.
        let epoch = self.ship.max_epoch.max(1);
        let mut exec_idx: Option<u64> = None;
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
                Ok((records, idx)) => {
                    replica.remember_outcome(rid, &records);
                    if rid.node != self.cfg.node_id {
                        replica.note_foreign_executed(&records);
                    }
                    self.stats.deleg_executed += 1;
                    if let Some(d) = self.dl.mine.get_mut(&gen) {
                        d.executed += 1;
                    }
                    exec_idx = Some(idx);
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
                    exec_idx = Some(replica.delegate_idx(gen));
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
                    exec_idx = Some(replica.delegate_idx(gen));
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
        // Phase 2b: under a backup (or `ack=s3`) the acknowledgement
        // waits until the transaction is on the backup (or in a segment
        // this replica applied); the row streams meanwhile.
        if let Some(idx) = exec_idx {
            if self.deleg_ack_gated(gen) && !self.deleg_stream_durable(gen, idx, replica) {
                self.stats.deleg_acks_parked += 1;
                let what = if from == 0 {
                    if let Some(c) = self.clients.get_mut(&rid) {
                        c.phase = super::client::Phase::Recalling;
                    }
                    super::readindex::ParkedWhat::Finish { rid, outcome }
                } else {
                    super::readindex::ParkedWhat::Reply {
                        to: from,
                        req,
                        rid,
                        outcome,
                        base,
                        position,
                        gen,
                        held_timer: None,
                    }
                };
                self.park_stream_need(now, gen, idx, what);
                self.deleg_backup_stream(now, replica, out);
                return;
            }
        }
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

    /// Phase 2b: whether this delegate's acknowledgements of `gen` wait
    /// for durability beyond its own disk.
    pub(crate) fn deleg_ack_gated(&self, gen: u64) -> bool {
        self.cfg.ack_s3 || self.dl.mine.get(&gen).is_some_and(|d| d.backup.is_some())
    }

    /// Phase 2b: transaction `(gen, idx)` is durable enough to
    /// acknowledge: on the backup, or in a segment this replica applied.
    pub(crate) fn deleg_stream_durable(&self, gen: u64, idx: u64, replica: &dyn Replica) -> bool {
        let Some(d) = self.dl.mine.get(&gen) else {
            return true;
        };
        if self.cfg.ack_s3 {
            return !replica.delegate_tx_pending(gen, idx);
        }
        match d.backup {
            None => true,
            Some(_) => d.backup_acked >= idx || !replica.delegate_tx_pending(gen, idx),
        }
    }

    /// Phase 2b: pick a backup for each generation held here (M9's
    /// candidate rule: a write-eligible peer within the RTT budget,
    /// connected the longest), never the root itself.
    fn deleg_backup_select(&mut self, now: Ms, out: &mut Vec<Action>) {
        if self.cfg.backup_rtt_budget_ms == 0 {
            return;
        }
        let root = self.root_node();
        let candidates = self.backup_candidates(now);
        let mut chosen = Vec::new();
        for d in self.dl.mine.values_mut() {
            if d.backup.is_some() || d.backup_sealed || d.stopped {
                continue;
            }
            if let Some(b) = candidates.iter().find(|n| Some(**n) != root).copied() {
                d.backup = Some(b);
                d.backup_acked = 0;
                d.backup_sent_through = 0;
                chosen.push(d.gen);
                tracing::info!(
                    node = self.cfg.node_id,
                    gen = d.gen,
                    backup = b,
                    "delegate chose a backup"
                );
            }
        }
        for gen in chosen {
            // The root learns the backup from a renewal (it seals it when
            // this delegate falls silent): send one now.
            self.deleg_renew_now(now, gen, out);
        }
    }

    /// Phase 2b: append what the backup lacks, one batch in flight.
    pub(crate) fn deleg_backup_stream(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let gens: Vec<u64> = self.dl.mine.keys().copied().collect();
        for gen in gens {
            let d = self.dl.mine.get(&gen).expect("present");
            let Some(backup) = d.backup else { continue };
            if d.backup_inflight.is_some() {
                continue;
            }
            let from = d.backup_sent_through + 1;
            let txs = replica.delegate_txs_from(gen, from, self.cfg.delegation_stream_rows);
            if txs.is_empty() {
                continue;
            }
            let last = txs.last().map(|t| t.idx).unwrap_or(from);
            let req = self.op_id();
            self.dl.by_req.insert(req, (gen, ReqKind::BackupAppend));
            self.stats.deleg_backup_appends += 1;
            let d = self.dl.mine.get_mut(&gen).expect("present");
            d.backup_inflight = Some((req, last));
            d.backup_inflight_at = now;
            out.push(Action::Send {
                to: backup,
                msg: PeerMsg::DelegBackupAppend { req, gen, txs },
            });
        }
        let _ = now;
    }

    /// Phase 2b: the backup answered an append.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_deleg_backup_ack(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        gen: u64,
        acked: u64,
        sealed: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some((g, ReqKind::BackupAppend)) = self.dl.by_req.remove(&req) else {
            return;
        };
        if g != gen {
            return;
        }
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return;
        };
        if d.backup != Some(from) {
            return;
        }
        self.stats.deleg_backup_acks += 1;
        let inflight = d.backup_inflight.take();
        if sealed {
            // The backup sealed the generation: the root is draining it
            // and will end it. Nothing more is acknowledged here; what
            // waited retries by rid (the root has it, or refuses it).
            tracing::warn!(
                node = self.cfg.node_id,
                gen,
                backup = from,
                "the backup sealed this delegation"
            );
            d.backup_sealed = true;
            d.backup = None;
            d.stopped = true;
            self.deleg_abort_stream_parks(now, gen, replica, out);
            return;
        }
        d.backup_failures = 0;
        d.backup_acked = d.backup_acked.max(acked);
        if let Some((_, last)) = inflight {
            if acked < last {
                // Short: resend from what it holds.
                d.backup_sent_through = acked;
            } else {
                d.backup_sent_through = d.backup_sent_through.max(last);
            }
        }
        self.complete_ready(now, replica, out);
        self.deleg_backup_stream(now, replica, out);
    }

    /// Phase 2b: what waited for the backup of `gen` cannot be
    /// acknowledged here any more: the requester (or this node's own
    /// client) retries by rid, which the root answers from the drain.
    fn deleg_abort_stream_parks(
        &mut self,
        now: Ms,
        gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let ids = self.stream_parks_of(gen);
        for id in ids {
            self.abort_park_busy(now, id, replica, out);
        }
    }

    // ----------------------------------------------------- the backup

    /// Phase 2b: a delegate's append (this node is its backup).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_deleg_backup_append(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        gen: u64,
        txs: Vec<constellation_meta::DelegateTx>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.note_foreign(now, replica, out);
        let sealed = self.dl.sealed.contains(&gen) || replica.deleg_backup_sealed(gen);
        let mut acked = self.dl.backing.get(&gen).map(|(_, a)| *a).unwrap_or(0);
        if !sealed && !txs.is_empty() {
            acked = replica.deleg_backup_append(gen, &txs);
            self.stats.deleg_backup_persisted += txs.len() as u64;
        }
        if !sealed {
            self.dl.backing.insert(gen, (from, acked));
        }
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::DelegBackupAck {
                req,
                gen,
                acked,
                sealed,
            },
        });
    }

    /// Phase 2b: the root asks this backup to seal `gen` and hand over
    /// its tail (the delegate is silent).
    pub(crate) fn on_deleg_seal(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.note_foreign(now, replica, out);
        let backing = (self.dl.backing.contains_key(&gen) || replica.deleg_backup_acked_any(gen))
            // Persisted (and synced) before it is acknowledged; not
            // persisted: answered unsealed, and the root falls back to
            // its plain reclaim.
            && replica.deleg_backup_seal(gen);
        let txs = if backing {
            self.dl.sealed.insert(gen);
            self.stats.deleg_backup_seals += 1;
            replica.deleg_backup_tail(gen)
        } else {
            Vec::new()
        };
        tracing::info!(
            node = self.cfg.node_id,
            gen,
            root = from,
            backing,
            rows = txs.len(),
            "sealed a delegation for the root"
        );
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::DelegSealed {
                req,
                gen,
                sealed: backing,
                txs,
            },
        });
    }

    /// Phase 2b: the backup's answer to a seal: append its tail, end the
    /// generation, delegate the directory to the backup.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_deleg_sealed(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        gen: u64,
        sealed: bool,
        txs: Vec<constellation_meta::DelegateTx>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some((g, ReqKind::Seal)) = self.dl.by_req.remove(&req) else {
            return;
        };
        if g != gen {
            return;
        }
        let Some(gs) = self.dl.gens.get(&gen) else {
            return;
        };
        if gs.ended || gs.backup != Some(from) {
            return;
        }
        let (dir, kind, range) = (gs.dir, gs.kind, gs.range);
        if sealed && self.root_usable(now) {
            let mut cursor = gs.cursor;
            let mut drained = 0u64;
            for tx in txs {
                if tx.idx <= cursor {
                    continue;
                }
                if tx.idx != cursor + 1 {
                    break;
                }
                if self.deps_from_a_newer_tenure(&tx.deps) {
                    self.stats.deleg_append_newer_tenure += 1;
                    break;
                }
                if !replica.reaches_streams(&tx.deps) {
                    self.stats.deleg_deps_unsatisfied_at_append += 1;
                    break;
                }
                match replica.apply_delegate_tx(&tx.records, tx.rid, gen, tx.idx, tx.deps) {
                    Ok(_) => {
                        replica.note_foreign_executed(&tx.records);
                        cursor = tx.idx;
                        drained += 1;
                    }
                    Err(error) => {
                        tracing::warn!(node = self.me(), gen, idx = tx.idx, %error, "could not append a sealed transaction");
                        break;
                    }
                }
            }
            if let Some(gs) = self.dl.gens.get_mut(&gen) {
                gs.cursor = cursor;
            }
            self.stats.deleg_sealed_drained += drained;
            if drained > 0 {
                self.answer_awaiting_log(now, replica, out);
                self.release_exec_parks_completed(now, replica, out);
                self.nudge(now, out);
            }
            tracing::info!(
                node = self.me(),
                gen,
                backup = from,
                drained,
                cursor,
                "drained a sealed delegation"
            );
        }
        self.end_generation(now, gen, replica, out);
        if sealed && kind != DelegKind::Designated {
            match self.delegate_dir(now, dir, from, kind, range, replica, out) {
                Ok(new_gen) => {
                    tracing::info!(
                        node = self.me(),
                        dir,
                        delegate = from,
                        gen,
                        new_gen,
                        "re-delegated to the backup"
                    );
                }
                Err(why) => {
                    tracing::debug!(node = self.me(), dir, why, "not re-delegated to the backup")
                }
            }
        }
    }

    /// Retry every parked op whose wait is over, then stream and renew.
    pub(crate) fn deleg_after_event(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // Phase 2b: the placement runs on the root, delegations or not.
        if self.root_usable(now) {
            self.place_arm(now, out);
            // M12: a re-delegation held back (the op still parked, the
            // delegate away) is tried again on the next event.
            if self.dl.gens.values().any(|g| g.ended && g.redelegate) {
                self.deleg_redelegate_after_cross(now, replica, out);
            }
        }
        if self.dl.mine.is_empty() && self.dl.gens.is_empty() {
            return;
        }
        // Phase 2b: a new root (a failover) gets every unretired
        // transaction again; it deduplicates by cursor and rid.
        let root = self.root_node();
        let mut stale_reqs = Vec::new();
        for d in self.dl.mine.values_mut() {
            if d.last_root != root {
                if d.last_root.is_some() && root.is_some() {
                    d.streamed_through = 0;
                    // The old root's answer to the batch in flight must
                    // not count for the new one (see
                    // `on_delegate_stream_ack`).
                    if let Some((req, _)) = d.inflight.take() {
                        stale_reqs.push(req);
                    }
                    d.refused = false;
                    d.stream_after = None;
                    d.stream_backoff_ms = 0;
                    self.stats.deleg_restreams += 1;
                    tracing::info!(
                        node = self.cfg.node_id,
                        gen = d.gen,
                        ?root,
                        "re-streaming to a new root"
                    );
                }
                d.last_root = root;
            }
        }
        for req in stale_reqs {
            self.dl.by_req.remove(&req);
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
            if !honoured {
                continue;
            }
            let candidates: Vec<(usize, NodeId, MutateOp)> = d
                .parked
                .iter()
                .enumerate()
                .filter(|(_, p)| replica.reaches(&p.deps))
                .map(|(i, p)| (i, p.from, p.op.clone()))
                .collect();
            let mut ready = Vec::new();
            for (i, from, op) in candidates {
                if !self.deleg_read_recall_pending(now, from, &op, replica, out) {
                    ready.push(i);
                }
            }
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
        // Phase 2b: the delegate's backup.
        self.deleg_backup_select(now, out);
        self.deleg_backup_stream(now, replica, out);
        // Root-side parked executions waiting for `deps` (and, phase 2b,
        // a delegate's acknowledgements waiting for a segment).
        if self.has_deps_parks() || self.has_stream_parks() {
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

    /// How long a stream batch, backup append or renewal waits for its
    /// answer before it is sent again (a partition drops it silently).
    fn deleg_request_timeout_ms(&self) -> u64 {
        self.cfg
            .forward_timeout_ms
            .max(self.cfg.delegation_stream_tick_ms * 4)
            .max(1)
    }

    /// Drop the in-flight requests whose answers are overdue; the
    /// generations whose renewal was dropped (re-sent by the caller).
    fn deleg_expire_inflight(&mut self, now: Ms) -> Vec<u64> {
        let timeout = self.deleg_request_timeout_ms() as i64;
        let mut drop_reqs = Vec::new();
        let mut renew_again = Vec::new();
        for d in self.dl.mine.values_mut() {
            if let Some((req, _)) = d.inflight {
                if now.since(d.inflight_at) >= timeout {
                    drop_reqs.push(req);
                    d.inflight = None;
                    self.stats.deleg_stream_timeouts += 1;
                }
            }
            if let Some((req, _)) = d.backup_inflight {
                if now.since(d.backup_inflight_at) >= timeout {
                    drop_reqs.push(req);
                    d.backup_inflight = None;
                    self.stats.deleg_stream_timeouts += 1;
                }
            }
            if let Some((req, sent)) = d.renew {
                if now.since(sent) >= timeout {
                    drop_reqs.push(req);
                    d.renew = None;
                    if d.renew_timer.is_none() && !d.stopped {
                        renew_again.push(d.gen);
                    }
                }
            }
        }
        for req in drop_reqs {
            self.dl.by_req.remove(&req);
        }
        renew_again
    }

    /// Send the next batch of every generation without one in flight.
    fn deleg_stream(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.dl.mine.is_empty() {
            return;
        }
        // A renewal whose answer was lost (a dropped message: no
        // transport failure, no reply) is sent again, or its timer never
        // comes back (long-delegated seed 70067: reclaimed unrenewed).
        for gen in self.deleg_expire_inflight(now) {
            self.deleg_renew_now(now, gen, out);
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
            let mut txs = replica.delegate_txs_from(gen, from, self.cfg.delegation_stream_rows);
            // Plan 30 §M12: a node holds several generations (the ranges
            // of a directory, a subtree); a transaction of one may depend
            // on this node's own execution under another (it reached it
            // locally). The root must hold that first: the batch stops
            // before a transaction whose own-generation deps the root has
            // not acknowledged, and goes out once it has.
            let acked_here = |h: u64, i: u64, mine: &BTreeMap<u64, DelegateState>| -> bool {
                h == gen || mine.get(&h).is_none_or(|o| o.streamed_through >= i)
            };
            if let Some(stop) = txs.iter().position(|t| {
                t.deps
                    .streams
                    .iter()
                    .any(|(h, i)| !acked_here(h, i, &self.dl.mine))
            }) {
                txs.truncate(stop);
            }
            if txs.is_empty() {
                continue;
            }
            let last = txs.last().map(|t| t.idx).unwrap_or(from);
            let req = self.op_id();
            self.dl.by_req.insert(req, (gen, ReqKind::Stream));
            self.stats.deleg_streamed_txs += txs.len() as u64;
            let d = self.dl.mine.get_mut(&gen).expect("present");
            d.inflight = Some((req, last));
            d.inflight_at = now;
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
        // be sent, a generation backing off, or rows held back behind
        // another generation's acknowledgement: the tick retries.
        if self.dl.mine.values().any(|d| {
            d.inflight.is_some()
                || d.stream_after.is_some_and(|t| now < t)
                || (!d.stopped && replica.delegate_idx(d.gen) > d.streamed_through)
        }) {
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
        // Only the answer to the batch in flight counts: a late answer
        // from a root this generation no longer streams to (it was paused,
        // then deposed) would claim the new root holds rows it never got,
        // and the stream would stop short of them for good (long-
        // delegated-backup seed 75504: the old root's ack through 7
        // arrived after the re-stream to its successor, whose cursor was
        // 6; the delegate's last row never reached the log).
        if d.inflight.map(|(r, _)| r) != Some(req) {
            return;
        }
        d.inflight = None;
        if refused {
            // The generation is ending, or the root has not learned it
            // (a successor before its table, a holder inside its takeover
            // gate): the log ends a generation, a refusal does not — back
            // off and try again on the tick.
            self.stats.deleg_stream_refused += 1;
            let tick = self.cfg.delegation_stream_tick_ms.max(1);
            d.stream_backoff_ms = (d.stream_backoff_ms * 2).clamp(tick * 4, 2_000);
            d.stream_after = Some(now.plus(d.stream_backoff_ms));
            self.arm_stream_tick(now, out);
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
            ReqKind::BackupAppend => {
                // Phase 2b: the backup is unreachable; the tick resends,
                // and after three failures the delegate goes on without
                // it (M9: no peer in budget means the local policy).
                let mut dropped = None;
                if let Some(d) = self.dl.mine.get_mut(&gen) {
                    d.backup_inflight = None;
                    d.backup_failures += 1;
                    if d.backup_failures >= 3 {
                        dropped = d.backup.take();
                        d.backup_failures = 0;
                    }
                }
                if let Some(b) = dropped {
                    tracing::warn!(
                        node = self.cfg.node_id,
                        gen,
                        backup = b,
                        "delegate dropped an unreachable backup"
                    );
                }
                self.arm_stream_tick(now, out);
            }
            ReqKind::Seal => {
                // Phase 2b: the backup did not answer the seal: the
                // grant's expiry ends the generation without its tail.
                // Back to the phase the seal replaced: a recall under way
                // stays one, so no renewal revives the grant (long-
                // delegated-backup seed 71251: the backup had crashed,
                // the phase went back to `None`, the restarted delegate's
                // renewals were granted again and again, and the root's
                // own op parked on the recall waited past its deadline —
                // `EIO` for a write the log carried).
                if let Some(g) = self.dl.gens.get_mut(&gen) {
                    if g.recall == RecallPhase::Sealing {
                        g.recall = match g.recall_before_seal {
                            RecallPhase::Sealing => RecallPhase::None,
                            before => before,
                        };
                    }
                }
                self.arm_gen_expiry(now, gen, out);
            }
        }
        true
    }

    // ----------------------------------------------------- renewals

    pub(crate) fn deleg_renew_now(&mut self, now: Ms, gen: u64, out: &mut Vec<Action>) {
        let Some(root) = self.root_node() else {
            // Learn the holder, and try again after a tick: the first
            // renewal of a grant installed from an S3 tail (no P2P
            // exchange with the root yet) must not be the last
            // (long-delegated seed 70067: never renewed, reclaimed).
            self.issue_s3(
                crate::action::S3Op::LeaseGet,
                super::S3For::RefreshHolder,
                out,
            );
            if let Some(d) = self.dl.mine.get_mut(&gen) {
                if d.renew_timer.is_none() && !d.stopped {
                    let t = self.set_timer(
                        now.plus(self.cfg.delegation_stream_tick_ms.max(1)),
                        Timer::DelegRenew(gen),
                        out,
                    );
                    self.dl.mine.get_mut(&gen).expect("present").renew_timer = Some(t);
                }
            }
            return;
        };
        let _ = self.deleg_expire_inflight(now);
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
        let backup = self.dl.mine.get(&gen).and_then(|d| d.backup);
        self.dl.mine.get_mut(&gen).expect("present").renew_backup = backup;
        out.push(Action::Send {
            to: root,
            msg: PeerMsg::DelegRenew {
                req,
                gen,
                backup,
                stream_head: 0,
            },
        });
    }

    pub(crate) fn on_deleg_renew_timer(&mut self, now: Ms, gen: u64, out: &mut Vec<Action>) {
        if let Some(d) = self.dl.mine.get_mut(&gen) {
            d.renew_timer = None;
        }
        self.deleg_renew_now(now, gen, out);
    }

    /// Root side: grant (or refuse) a renewal.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_deleg_renew(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        gen: u64,
        backup: Option<NodeId>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let mut ttl_ms = 0u64;
        if self.root_usable(now) && !self.dl.epoch_active {
            let cap = self.grant_cap_ms(now);
            let extra = self.reclaim_horizon_ms() - self.cfg.delegation_ttl_ms;
            if let Some(g) = self.dl.gens.get_mut(&gen) {
                if g.node == from && !g.ended && g.recall == RecallPhase::None {
                    ttl_ms = self.cfg.delegation_ttl_ms.min(cap);
                    if ttl_ms > 0 {
                        let until = now.plus(ttl_ms + extra);
                        if until > g.until {
                            g.until = until;
                        }
                    }
                    if g.backup != backup {
                        g.backup = backup;
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
        // Plan 30 §M14: the lock grants under the subtree travel with the
        // first renewal that grants, and again with every later one while
        // they may be live (the reply can be lost).
        // So does what is left of a lock grace here that covers it.
        let (locks, lock_grace_ms) = if ttl_ms > 0 {
            (
                self.lock_take_handoff(now, gen),
                self.lock_grace_for_generation(now, gen, replica),
            )
        } else {
            (Vec::new(), 0)
        };
        let lock_floor = if ttl_ms > 0 {
            self.lock_floor_for_generation(gen)
        } else {
            Default::default()
        };
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::DelegRenewed {
                req,
                gen,
                ttl_ms,
                locks,
                lock_grace_ms,
                lock_floor,
            },
        });
    }

    /// Plan 30 §M14: this node's honoured end of generation `gen` as its
    /// delegate (`None`: not held, stopped, or never renewed).
    pub(crate) fn deleg_mine_until(&self, gen: u64) -> Option<Ms> {
        self.dl
            .mine
            .get(&gen)
            .filter(|d| !d.stopped && d.until.0 > 0)
            .map(|d| d.until)
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
        // Renew at half the ttl — at a quarter while this generation has
        // lock grants out: they are capped by what is left of it, and a
        // grant needs at least `2 × margin` of it (`lock_min_grant_ms`).
        let at = if replica.locks().has_grants_of_gen(gen) {
            sent.plus(ttl_ms / 4)
        } else {
            sent.plus(ttl_ms / 2)
        };
        let t = self.set_timer(at.max(now.plus(1)), Timer::DelegRenew(gen), out);
        let d = self.dl.mine.get_mut(&gen).expect("present");
        d.renew_timer = Some(t);
        if d.backup.is_some() && d.renew_backup != d.backup {
            // Phase 2b: the root seals the backup on a silent delegate
            // only once it knows it — tell it now, not at the next timer.
            self.deleg_renew_now(now, gen, out);
        }
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
        // Plan 30 §M14: the subtree's lock grants go back with the answer;
        // its waiters are told to ask the root.
        let locks = self.lock_hand_back(gen, replica);
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
        // Phase 2b (M8): the read delegations this delegate granted are
        // recalled first; the root executes under the subtree only once
        // they are gone (or, past the horizon, expired).
        if self.cfg.read_delegations {
            let need = replica.read_delegations().all_live(now.0);
            if let Some(wait) = self.start_recalls(now, need, out) {
                self.park(
                    now,
                    wait,
                    None,
                    super::readindex::ParkedWhat::DelegRecalled {
                        to: from,
                        req,
                        gen,
                        through,
                        locks,
                    },
                );
                self.lock_reserve_all(now, replica, out);
                return;
            }
        }
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::DelegRecalled {
                req,
                gen,
                through,
                locks,
            },
        });
        self.lock_reserve_all(now, replica, out);
    }

    // ----------------------------------------------------- reads (M8)

    /// Phase 2b: the keys a read of `ino` (and `name` under it) touches,
    /// for ownership.
    /// The keys a strict read touches, keyed like the writes that change
    /// them (`keys_of_op`): a lookup is its dentry (the directory's own
    /// inode resolves by *its* parent, which would make every lookup in
    /// a delegated directory cross-subtree); an inode read is the inode.
    pub(crate) fn read_keys(ino: Ino, name: Option<&str>) -> TouchSet {
        let mut keys = TouchSet::default();
        match name {
            Some(n) => {
                keys.dentries.insert((ino, n.to_string()));
            }
            None => {
                keys.inos.insert(ino);
            }
        }
        keys
    }

    /// Phase 2b: answer a strict read for this delegate's subtree, if it
    /// is: the position is this replica's plus the stream index, and a
    /// read delegation capped by the grant.
    pub(crate) fn deleg_read_index(
        &mut self,
        now: Ms,
        from: NodeId,
        ino: Ino,
        name: Option<&str>,
        replica: &dyn Replica,
    ) -> Option<crate::event::ReadIndexOutcome> {
        if !self.cfg.delegation || self.dl.mine.is_empty() {
            return None;
        }
        let keys = Self::read_keys(ino, name);
        let gen = self.my_generation_for(&keys, replica)?;
        let d = self.dl.mine.get(&gen)?;
        if now >= d.until {
            return None;
        }
        let epoch = super::DELEG_READ_EPOCH_BASE + gen;
        let grant = if self.cfg.read_delegations && from != self.cfg.node_id {
            let margin = self.cfg.expiry_margin_ms as i64;
            let cap = d.until.0 - margin - now.0;
            let ttl = (self.cfg.read_delegation_ttl_ms as i64).min(cap);
            if ttl > 0 && !self.rd.blocked.get(&ino).is_some_and(|until| *until > now) {
                let until = now.0 + ttl + margin;
                let id = replica.read_delegations().grant(from, ino, until);
                self.stats.deleg_read_grants += 1;
                Some(crate::event::ReadGrantMsg {
                    id,
                    ttl_ms: ttl as u64,
                    epoch,
                })
            } else {
                None
            }
        } else {
            None
        };
        self.stats.deleg_read_index_served += 1;
        Some(crate::event::ReadIndexOutcome::Ok {
            position: Position {
                seq: replica.applied_seq().unwrap_or(0),
                pending: None,
                streams: {
                    let mut s = constellation_meta::Streams::NONE;
                    s.raise(gen, replica.stream_applied(gen));
                    s
                },
            },
            grant,
        })
    }

    /// Phase 2b: where a strict read of `ino` goes — the delegate owning
    /// it (`Some(node)`), this node itself as that delegate (`Some(me)`),
    /// or the holder (`None`).
    pub(crate) fn deleg_read_owner(
        &self,
        now: Ms,
        ino: Ino,
        name: Option<&str>,
        replica: &dyn Replica,
    ) -> Option<NodeId> {
        if !self.cfg.delegation || replica.delegation_table().is_empty() {
            return None;
        }
        let keys = Self::read_keys(ino, name);
        match replica.resolve_ownership(&keys) {
            Ownership::Delegated(d) => {
                if d.node == self.cfg.node_id {
                    let honoured = self
                        .dl
                        .mine
                        .get(&d.gen)
                        .is_some_and(|m| now < m.until && !m.stopped);
                    honoured.then_some(d.node)
                } else {
                    Some(d.node)
                }
            }
            _ => None,
        }
    }

    // ----------------------------------------------------- the root

    /// The generations `keys` fall under that this root must end before
    /// it executes: started (recalled) here; the caller parks on their
    /// wait ids. `None`: nothing to recall.
    /// What executing an op touching `keys` on the root needs first
    /// (phase 2b: a designation is never recalled — the op is refused).
    pub(crate) fn deleg_recall_plan(
        &mut self,
        now: Ms,
        keys: &TouchSet,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> RecallPlan {
        if !self.cfg.delegation || self.dl.gens.is_empty() {
            return RecallPlan::None;
        }
        let (involved, cross): (Vec<u64>, bool) = match replica.resolve_ownership(keys) {
            Ownership::Root => return RecallPlan::None,
            Ownership::Delegated(d) => (vec![d.gen], false),
            Ownership::CrossSubtree { involved, .. } => {
                self.stats.deleg_cross_subtree += 1;
                (involved.iter().map(|d| d.gen).collect(), true)
            }
        };
        let designated = involved.iter().any(|g| {
            self.dl
                .gens
                .get(g)
                .is_some_and(|s| !s.ended && s.kind == DelegKind::Designated)
        });
        if designated {
            // Plans 03–05: only the designee writes under its path
            // (`EROFS` for anyone else once the designee is unreachable),
            // and nothing moves across its boundary (`EXDEV`).
            self.stats.deleg_refused_designated += 1;
            tracing::info!(
                node = self.me(),
                cross,
                "refusing an op under an unreachable designation (plans 03-05)"
            );
            return RecallPlan::Refuse(if cross { libc::EXDEV } else { libc::EROFS });
        }
        let mut waiting = BTreeSet::new();
        for gen in involved {
            if self.dl.gens.get(&gen).is_some_and(|g| g.ended) {
                continue;
            }
            if cross {
                if let Some(g) = self.dl.gens.get_mut(&gen) {
                    // Ended by an op, not a decision: delegate it again
                    // to the same node once the op ran (a known 2a gap).
                    g.redelegate = true;
                }
            }
            self.start_recall(now, gen, out);
            waiting.insert(wait_id(gen));
        }
        if waiting.is_empty() {
            RecallPlan::None
        } else {
            RecallPlan::Wait(waiting)
        }
    }

    /// A delegate transaction whose `deps` name a journal position of a
    /// newer root tenure than this one: this root has been superseded
    /// without knowing it yet (paused, cut off), and must not append it —
    /// its replica would hold the transaction without its cause (long-
    /// delegated-backup seed 74035: a deposed root drained a sealed
    /// backup's marker whose data only its successor had; the rows strand
    /// at the deposition, but the replica showed the marker meanwhile).
    pub(crate) fn deps_from_a_newer_tenure(&self, deps: &Position) -> bool {
        deps.pending
            .is_some_and(|p| self.lease.epoch().is_none_or(|mine| p.epoch > mine))
    }

    /// Phase 2b: after a cross-subtree op executed, the generations it
    /// ended are granted again to their nodes (a new generation each).
    pub(crate) fn deleg_redelegate_after_cross(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.root_usable(now) {
            return;
        }
        let again: Vec<(u64, Ino, NodeId, DelegKind, Range)> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| g.ended && g.redelegate)
            .map(|(gen, g)| (*gen, g.dir, g.node, g.kind, g.range))
            .collect();
        for (gen, dir, node, kind, range) in again {
            if let Some(g) = self.dl.gens.get_mut(&gen) {
                g.redelegate = false;
            }
            if !self.dl.pending_exec.is_empty()
                || self.has_exec_parks()
                || self.rd_has_recall_wait(gen)
                || self.inbox_holds_redelegation(now)
            {
                // The op that ended it has not run yet: next time.
                if let Some(g) = self.dl.gens.get_mut(&gen) {
                    g.redelegate = true;
                }
                continue;
            }
            if !self.links.get(&node).is_some_and(|l| l.connected) {
                // The delegate is away for the moment (M12: a range's
                // delegate reconnecting mid-recall): once it is back.
                if let Some(g) = self.dl.gens.get_mut(&gen) {
                    g.redelegate = true;
                }
                continue;
            }
            match self.delegate_dir(now, dir, node, kind, range, replica, out) {
                Ok(new_gen) => {
                    self.stats.deleg_redelegated += 1;
                    tracing::info!(
                        node = self.me(),
                        dir,
                        delegate = node,
                        gen,
                        new_gen,
                        "re-delegated after a cross-subtree op"
                    );
                }
                Err(why) => tracing::debug!(
                    node = self.me(),
                    dir,
                    delegate = node,
                    why,
                    "not re-delegated"
                ),
            }
        }
    }

    pub(crate) fn start_recall(&mut self, now: Ms, gen: u64, out: &mut Vec<Action>) {
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
        if g.ended || g.kind == DelegKind::Designated {
            // A designation never expires: `online` ends it (an
            // unreachable designee's stays until it returns, DESIGN.md
            // §5.2).
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
        if g.ended {
            return;
        }
        if !self.root_usable(now) {
            // Phase 2b round 2: the root is unusable for the moment (a
            // renewal in flight, S3 away) but not gone (`deleg_on_lease_gone`
            // drops the state when it is): the expiry is tried again
            // shortly, or a recall in flight is never outwaited — an
            // execution parked on it waits forever (long-delegated seed
            // 70051: 85 000 simulated seconds of timers).
            let again = now.plus(self.cfg.delegation_stream_tick_ms.max(100));
            let t = self.set_timer(again, Timer::DelegExpiry(gen), out);
            self.dl.gens.get_mut(&gen).expect("present").expiry = Some(t);
            return;
        }
        let g = self.dl.gens.get(&gen).expect("present").clone();
        if now < g.until {
            self.arm_gen_expiry(now, gen, out);
            return;
        }
        let done = matches!(g.recall, RecallPhase::Drained(th) if g.cursor >= th);
        if done {
            return;
        }
        // Plan 30 §M14: an outwaited delegate — the lock grants moved to
        // it may still be honoured with this root's windows: a grace on
        // the subtree (the seal path re-delegates; the grace stands).
        let outwaited_dir = (g.recall != RecallPhase::Sealing).then_some(g.dir);
        let seal = match (
            g.backup,
            g.seal_attempts,
            g.recall != RecallPhase::Drained(0),
        ) {
            (Some(backup), 0, true) => Some(backup),
            _ => None,
        };
        if let Some(dir) = outwaited_dir {
            self.lock_on_generation_outwaited(now, gen, dir);
        }
        // Phase 2b: a silent delegate with a backup — ask the backup to
        // seal and hand over what it holds first (once; the answer, or
        // its absence, ends the generation either way).
        if let Some(backup) = seal {
            let req = self.op_id();
            let g = self.dl.gens.get_mut(&gen).expect("present");
            g.seal_attempts = 1;
            g.recall_before_seal = g.recall;
            g.recall = RecallPhase::Sealing;
            g.until = now.plus(self.cfg.forward_timeout_ms * 2);
            self.dl.by_req.insert(req, (gen, ReqKind::Seal));
            self.stats.deleg_seals_sent += 1;
            tracing::info!(
                node = self.me(),
                gen,
                backup,
                "delegate silent: sealing its backup"
            );
            out.push(Action::Send {
                to: backup,
                msg: PeerMsg::DelegSeal { req, gen },
            });
            self.arm_gen_expiry(now, gen, out);
            return;
        }
        // The grant is not honoured any more (the margin argument): a
        // pending recall is outwaited, an unrenewed grant reclaimed.
        match g.recall {
            RecallPhase::None => {
                if !self.cfg.delegation_reclaim_expired {
                    return;
                }
                self.stats.deleg_reclaimed += 1;
                tracing::info!(
                    node = self.me(),
                    gen,
                    until = g.until.0,
                    now = now.0,
                    "reclaiming an unrenewed delegation"
                );
            }
            RecallPhase::Sealing => {
                // The seal was not answered in time.
                if let Some(req) = self
                    .dl
                    .by_req
                    .iter()
                    .find(|(_, (g, k))| *g == gen && *k == ReqKind::Seal)
                    .map(|(r, _)| *r)
                {
                    self.dl.by_req.remove(&req);
                }
                self.stats.deleg_reclaimed += 1;
            }
            _ => self.stats.deleg_recalls_expired += 1,
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
        let drained = matches!(g.recall, RecallPhase::Drained(_));
        if let Err(error) = replica.apply_records_journaled(&[LogRecord::Recall { dir, gen }], None)
        {
            tracing::warn!(node = self.me(), gen, %error, "could not journal the recall record");
            self.arm_gen_expiry(now.plus(500), gen, out);
            return;
        }
        replica.void_stream(gen, cursor);
        // This root was the generation's delegate (inherited at a
        // takeover): its own rows past the cursor stay in its journal and
        // ship after the `Recall`, so they are not lost.
        let own = self.dl.mine.contains_key(&gen);
        replica.note_void_cut(
            gen,
            if own {
                cursor.max(replica.stream_applied(gen))
            } else {
                cursor
            },
        );
        // What this root holds of the generation's replies (its own
        // forwarded ops' shadows and hints) is stranded like on any
        // other replica applying the `Recall` (long-delegated seed 72780).
        match replica.strand_recalled_speculation(gen) {
            Ok(s) if s.any() => {
                tracing::info!(
                    node = self.me(),
                    gen,
                    shadows = s.shadows,
                    hints = s.hints,
                    "the root's own speculation of an ended generation stranded; replaying"
                );
                self.stats.speculation_rolled_back += (s.shadows + s.hints) as u64;
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(node = self.me(), gen, %error, "could not strand the root's speculation of an ended generation");
            }
        }
        self.lease.touch(now);
        self.nudge(now, out);
        // Plan 30 §M14: grants handed to this generation and not handed
        // back (still waiting for its first renewal — sim seed 96027 —,
        // or sent with a renewal reply its recall overtook or that was
        // lost — seeds 196252, 196102) come back to this table, where
        // the subtree's next delegation takes them along. An outwaited
        // generation also leaves a grace on the subtree (sim seed 96046).
        self.lock_on_generation_ended(now, gen, replica);
        if !drained {
            self.lock_on_generation_outwaited(now, gen, dir);
        }
        tracing::info!(
            node = self.me(),
            gen,
            dir,
            cursor,
            "delegation generation ended"
        );
        self.mark_ended(now, gen, replica, out);
        // The record is journaled here, not applied from a segment: a
        // generation this node held itself (inherited by a successor
        // that was the delegate) ends on the delegate side too.
        if self.dl.mine.contains_key(&gen) {
            self.drop_delegate_state(now, gen, replica, out);
        }
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
            if self.deps_from_a_newer_tenure(&tx.deps) {
                self.stats.deleg_append_newer_tenure += 1;
                break;
            }
            if !replica.reaches_streams(&tx.deps) {
                self.stats.deleg_deps_unsatisfied_at_append += 1;
                tracing::debug!(
                    node = self.me(),
                    gen,
                    idx = tx.idx,
                    deps = ?tx.deps,
                    applied = ?replica.applied_position(),
                    "delegate batch refused: the root lacks its deps"
                );
                break;
            }
            match replica.apply_delegate_tx(&tx.records, tx.rid, gen, tx.idx, tx.deps) {
                Ok(_) => {
                    replica.note_foreign_executed(&tx.records);
                    cursor = tx.idx;
                    appended += 1;
                    if self.cfg.placement {
                        // The op's origin, not the delegate that executed
                        // it: a forwarded op counts for its requester.
                        let origin = tx.rid.map(|r| r.node).unwrap_or(from);
                        let keys = TouchSet::from_records(tx.records.iter());
                        let dirs = Core::dirs_of_keys(&keys, replica);
                        self.place_note(origin, dirs);
                    }
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
            // the log for exactly these completions, or be parked on a
            // recall of it.
            self.answer_awaiting_log(now, replica, out);
            self.release_exec_parks_completed(now, replica, out);
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_control_delegate(
        &mut self,
        now: Ms,
        op: OpId,
        dir: Ino,
        node: NodeId,
        range: Range,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let fail = |out: &mut Vec<Action>, msg: String| {
            out.push(Action::ControlDone {
                op,
                result: Err(msg),
            });
        };
        match self.delegate_dir(now, dir, node, DelegKind::Manual, range, replica, out) {
            Ok(gen) => out.push(Action::ControlDone {
                op,
                result: Ok(ControlOk::Text(format!(
                    "delegated dir {dir} to node {node} (gen {gen})"
                ))),
            }),
            Err(msg) => fail(out, msg),
        }
    }

    /// Delegate `dir` to `node` as `kind`: the `Delegate` record and the
    /// root's generation state. The checks every path shares (an
    /// operator's control, the placement, a designation, a
    /// re-delegation after a cross-subtree op).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn delegate_dir(
        &mut self,
        now: Ms,
        dir: Ino,
        node: NodeId,
        kind: DelegKind,
        range: Range,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Result<u64, String> {
        if !self.cfg.delegation {
            return Err("delegation is off (CONSTELLATION_DELEGATION)".into());
        }
        if !self.cfg.p2p {
            return Err("delegation needs P2P".into());
        }
        if !self.root_usable(now) {
            return Err("this node does not hold the lease".into());
        }
        if self.dl.epoch_active || self.epoch.open {
            return Err("no delegation while a continuation epoch is open".into());
        }
        if node == self.me() {
            return Err("cannot delegate to the root itself".into());
        }
        let table = replica.delegation_table();
        if let Some(d) = table.covering(replica.namespace(), dir) {
            return Err(format!(
                "directory {dir} is under delegation {} (gen {})",
                d.dir, d.gen
            ));
        }
        if range.is_whole() {
            if table.is_split(dir) {
                return Err(format!("directory {dir} is split into ranges"));
            }
            // A delegation on a descendant of `dir` would be shadowed.
            if table
                .iter()
                .any(|d| d.dir != dir && is_under(replica.namespace(), d.dir, dir))
            {
                return Err(format!("a directory under {dir} is already delegated"));
            }
            if kind != DelegKind::Designated
                && table
                    .iter()
                    .any(|d| d.node == node && !d.designated && d.range.is_whole())
            {
                return Err(format!("node {node} already holds a delegation"));
            }
        } else {
            // Plan 30 §M12: one split depth per directory; a range is
            // delegated once; never over a designation.
            if kind == DelegKind::Designated {
                return Err("a designation covers a whole directory".into());
            }
            let ranges = table.ranges_of(dir);
            if let Some(d) = ranges.iter().find(|d| d.range.is_whole()) {
                return Err(format!(
                    "directory {dir} is wholly delegated (gen {})",
                    d.gen
                ));
            }
            if let Some(d) = ranges.iter().find(|d| d.range.bits != range.bits) {
                return Err(format!(
                    "directory {dir} is split {} ways, not {}",
                    1u64 << d.range.bits,
                    1u64 << range.bits
                ));
            }
            if let Some(d) = ranges.iter().find(|d| d.range == range) {
                return Err(format!(
                    "range {} of {dir} is delegated (gen {})",
                    range.label(),
                    d.gen
                ));
            }
        }
        // Above every generation the log ever named (a predecessor's
        // ended ones included): a generation is a stream's identity.
        let highest = table.max_gen();
        let gen = replica
            .next_delegation_gen(highest + 1)
            .map_err(|e| e.to_string())?;
        let designated = kind == DelegKind::Designated;
        replica
            .apply_records_journaled(
                &[LogRecord::Delegate {
                    dir,
                    node,
                    gen,
                    designated,
                    range: (range.bits, range.idx),
                }],
                None,
            )
            .map_err(|e| e.to_string())?;
        self.dl.gens.insert(
            gen,
            GenState {
                dir,
                node,
                range,
                kind,
                backup: None,
                redelegate: false,
                granted: now,
                below_since: None,
                seal_attempts: 0,
                cursor: 0,
                until: now.plus(self.reclaim_horizon_ms()),
                recall: RecallPhase::None,
                recall_before_seal: RecallPhase::None,
                ended: false,
                expiry: None,
                recall_req: None,
                controls: Vec::new(),
            },
        );
        self.arm_gen_expiry(now, gen, out);
        // Plan 30 §M14: the subtree's lock grants go with it.
        self.lock_on_delegated(gen, replica);
        self.lock_reserve_all(now, replica, out);
        self.stats.deleg_delegated += 1;
        match kind {
            DelegKind::Placed => self.stats.place_delegated += 1,
            DelegKind::Designated => self.stats.deleg_designated += 1,
            DelegKind::Manual => {}
        }
        self.lease.touch(now);
        self.nudge(now, out);
        tracing::info!(
            node = self.me(),
            dir,
            delegate = node,
            gen,
            ?kind,
            "delegated"
        );
        Ok(gen)
    }

    /// How long the root honours a grant it cannot see renewed: the
    /// grant's ttl plus the margin. The read delegations a delegate
    /// grants under it (phase 2b, M8) need no extra term: each is capped
    /// by the delegate's own `until` less the margin, so every one of
    /// them lapses before the grant does.
    pub(crate) fn reclaim_horizon_ms(&self) -> u64 {
        self.cfg.delegation_ttl_ms + self.cfg.expiry_margin_ms
    }

    /// Phase 2b: keep the table in step with the offline designations
    /// (plans 03–05): a live write designation becomes a designated
    /// generation to its designee (unless the designee is this root:
    /// then the root sequences it, which is the same thing); a released
    /// one is recalled (drained by the designee). A directory under
    /// another delegation waits for that generation to end first.
    pub(crate) fn on_control_sync_designations(
        &mut self,
        now: Ms,
        entries: &[(Ino, NodeId)],
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.cfg.delegation || !self.root_usable(now) {
            return;
        }
        let table = replica.delegation_table();
        for &(dir, node) in entries {
            if node == self.me() {
                continue;
            }
            match table.get(dir) {
                Some(d) if d.designated && d.node == node => continue,
                Some(d) if !d.designated => {
                    // A placed or manual delegation on the very directory:
                    // it ends, the designation follows on the next sync.
                    if self.dl.gens.get(&d.gen).is_some_and(|g| !g.ended) {
                        self.start_recall(now, d.gen, out);
                    }
                    continue;
                }
                _ => {}
            }
            if let Some(d) = table.covering(replica.namespace(), dir) {
                if !d.designated && self.dl.gens.get(&d.gen).is_some_and(|g| !g.ended) {
                    self.start_recall(now, d.gen, out);
                }
                continue;
            }
            if table.is_split(dir) {
                // Plan 30 §M12: the ranges end first; the designation
                // follows on the next sync.
                for d in table.ranges_of(dir) {
                    if self.dl.gens.get(&d.gen).is_some_and(|g| !g.ended) {
                        self.start_recall(now, d.gen, out);
                    }
                }
                continue;
            }
            if let Err(why) = self.delegate_dir(
                now,
                dir,
                node,
                DelegKind::Designated,
                Range::WHOLE,
                replica,
                out,
            ) {
                tracing::debug!(
                    node = self.me(),
                    dir,
                    designee = node,
                    why,
                    "designation not delegated yet"
                );
            }
        }
        let released: Vec<u64> = table
            .iter()
            .filter(|d| {
                d.designated
                    && !entries
                        .iter()
                        .any(|(dir, node)| *dir == d.dir && *node == d.node)
            })
            .map(|d| d.gen)
            .collect();
        for gen in released {
            if self.dl.gens.get(&gen).is_some_and(|g| !g.ended) {
                tracing::info!(node = self.me(), gen, "designation released: recalling");
                self.start_recall(now, gen, out);
            }
        }
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
        let table = replica.delegation_table();
        let entries = table.ranges_of(dir);
        // A designation is the operator's offline authority (plans
        // 03–05): only `online` releases it. Recalling it here would just
        // see it re-delegated by the next designation sync, after
        // bouncing the designee's in-flight ops.
        if entries.iter().any(|d| d.designated)
            || entries.iter().any(|d| {
                self.dl
                    .gens
                    .get(&d.gen)
                    .is_some_and(|g| g.kind == DelegKind::Designated)
            })
        {
            out.push(Action::ControlDone {
                op,
                result: Err(format!(
                    "directory {dir} is designated (an offline designation); \
                     release it with `constellation online`, not `undelegate`"
                )),
            });
            return;
        }
        // Plan 30 §M12: every generation of the directory (one whole, or
        // its ranges); the control is answered when the last one ended.
        let gens: Vec<u64> = entries
            .iter()
            .map(|d| d.gen)
            .filter(|g| self.dl.gens.get(g).is_some_and(|s| !s.ended))
            .collect();
        let Some(&gen) = gens.last() else {
            out.push(Action::ControlDone {
                op,
                result: Err(format!("directory {dir} is not delegated")),
            });
            return;
        };
        for g in gens.iter().filter(|g| **g != gen) {
            self.start_recall(now, *g, out);
        }
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
    pub(crate) fn deleg_on_lease_gone(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // Phase 2b: what waited on these generations' recalls re-routes
        // (a local op to the new holder; a forwarded one is answered
        // `Busy` and re-sent).
        let ids: BTreeSet<u64> = self.dl.gens.keys().map(|g| wait_id(*g)).collect();
        if !ids.is_empty() {
            tracing::info!(
                node = self.me(),
                generations = ids.len(),
                "the root lease is gone: its delegation state is dropped"
            );
            self.abort_parks_waiting_on(now, &ids, replica, out);
        }
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
        // Plan 30 §M14: the tenure's lock grants go with it.
        self.lock_on_lease_gone(now, replica, out);
    }
}

/// What the root must do before executing an op (phase 2b).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecallPlan {
    None,
    /// Wait for these recalls (`wait_id`s).
    Wait(BTreeSet<u64>),
    /// Refuse with this errno (a designation is involved).
    Refuse(i32),
}

/// Whether `dir` is `ancestor` or under it.
fn is_under(
    ns: &dyn constellation_meta::delegation::Namespace,
    mut dir: Ino,
    ancestor: Ino,
) -> bool {
    for _ in 0..4096 {
        if dir == ancestor {
            return true;
        }
        match ns.parent_of(dir) {
            Some(p) => dir = p,
            None => return false,
        }
    }
    false
}

fn gen_of(g: &GenState, gens: &BTreeMap<u64, GenState>) -> u64 {
    gens.iter()
        .find(|(_, s)| std::ptr::eq(*s, g))
        .map(|(k, _)| *k)
        .unwrap_or(0)
}
