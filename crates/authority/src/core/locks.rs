//! Plan 30 §M14: strict mode — cross-node `flock`/`fcntl` as leased,
//! recallable, per-node file-lock grants at the owning sequencer.
//!
//! The tables live in `Meta` (`constellation_meta::locks`, shared with
//! the FUSE threads); this module is the protocol around them, on both
//! sides of every exchange. `crates/model/src/locks.rs` is the model of
//! it; its module doc has the argument, and its action → code table
//! names the functions here.
//!
//! # Owner side (the root holder, or an M11 delegate for its subtree)
//!
//! - [`Core::on_lock_request`]: route (`NotOwner` for another owner's
//!   subtree), the M9 liveness and marking guards (a tenure that grants
//!   locks is one its fast successor waits out), the grace (M9's
//!   acknowledgement floor, the restart quarantine, or the subtree grace
//!   after an outwaited delegate), then grant, or recall the conflicting
//!   grants and park the request (a waiter, FIFO per inode).
//! - A recalled node releases (`LockReleased`) or is outwaited by the
//!   grant's expiry (`Timer::LockGrantExpiry`); either serves the
//!   waiters. A waiter whose request was answered `Waiting` (so the RPC
//!   returns within `recall_hold_ms`) gets its grant pushed
//!   (`LockGranted`) or re-attaches with its next request.
//! - [`Core::on_lock_renew`]: extend, or *reclaim* an unknown grant
//!   during a grace period, else `Lost`. The acknowledgement carries the
//!   recalled flag, so a lost recall is repaired by the next renewal.
//! - The table follows the subtree: handed to a delegate with its first
//!   granting `DelegRenewed`, back with `DelegRecalled`. The root keeps
//!   its copy of what it handed ([`LockState::handed`]): every granting
//!   renewal re-sends the live copies (the reply can be lost), and a
//!   generation that ends without handing one back — its recall
//!   overtook the reply — leaves it reinstated in the root's table,
//!   where the subtree's next delegation takes it along. A delegate
//!   outwaited by TTL also leaves a grace on its subtree. An
//!   asynchronous mirror rides to the backups (`LockMirror`); a fast
//!   successor installs it, restamped, next to the floor.
//!
//! # Node side
//!
//! - [`Core::on_lock_control`]: the FUSE thread's request for a grant
//!   (`Control::Lock`), answered `Granted { position }` (the FUSE thread
//!   then session-waits and invalidates the kernel's cache of the file,
//!   which is what makes writes under the previous holder's lock visible
//!   under this one), `WouldBlock`, or `Unavailable` (no P2P path: the
//!   inbox is not a lock path, see PROGRESS.md).
//! - Renewals ride `Timer::LockRenewTick` while any grant is held,
//!   grouped by owner; a lapsed grant is never renewed (the FUSE thread
//!   fences I/O under it: `EIO`).
//! - A recall of a grant with local locks under it is honoured when the
//!   last one leaves (`Control::LockIdle`); one without waits for the
//!   flush of the file's dirty data (`Action::LockFlush` →
//!   `Event::LockFlushed`) before `LockReleased`.

use super::{Core, Timer};
use crate::action::{Action, ControlOk, LockAnswer, LockTestAnswer, S3Op};
use crate::event::{LockOutcome, LockRenewEntry, LockRenewResult, LockTestOutcome, PeerMsg};
use crate::ids::{Ms, NodeId, OpId, TimerId};
use crate::replica::Replica;
use constellation_fs_core::Ino;
use constellation_meta::locks::{Grant, GrantId, HeldGrant, Installed, LockMode};
use constellation_meta::Position;
use std::collections::BTreeMap;

/// A client-side lock request in progress.
#[derive(Debug)]
struct LockOp {
    ino: Ino,
    mode: LockMode,
    blocking: bool,
    /// The peer request in flight, if any.
    req: Option<OpId>,
    owner: NodeId,
    /// When the current request was sent (the grant's lifetime counts
    /// from here).
    sent_at: Ms,
    attempts: u32,
    since: Ms,
    /// Waiting in this node's own waiter list (the owner is here).
    local_wait: bool,
}

/// Owner side: a blocking request waiting for recalls.
#[derive(Debug)]
struct Waiter {
    id: u64,
    node: NodeId,
    ino: Ino,
    mode: LockMode,
    /// The RPC to answer, or none (answered `Waiting`; the grant is
    /// pushed, or the next request re-attaches).
    req: Option<OpId>,
    /// The requester's clock when it sent its request (echoed with a
    /// push: the grant's window counts from there, never later).
    sent: Ms,
    /// This owner's clock when that request (the park, or the latest
    /// re-send) arrived: a grant served to a remote waiter is live from
    /// here, not from when it is served. The requester's window counts
    /// from `sent`, which was before, so the owner's record still
    /// outlasts it by `2 × margin` — and a requester that died while
    /// parked costs at most `ttl + margin` from its last message, never
    /// that plus however long it sat in the queue (the EC2 lock run,
    /// PROGRESS.md "Fix: M14 follow-ups").
    recv: Ms,
    /// This node's own request (its `LockOp`).
    op: Option<OpId>,
    held_timer: Option<TimerId>,
    since: Ms,
}

#[derive(Debug)]
struct Recalling {
    ino: Ino,
    req: Option<OpId>,
    timer: TimerId,
}

#[derive(Debug)]
struct RenewInFlight {
    entries: Vec<(Ino, GrantId)>,
    sent: Ms,
    timer: TimerId,
}

#[derive(Debug, Default)]
pub(crate) struct LockState {
    // ---- node side ----
    ops: BTreeMap<OpId, LockOp>,
    by_req: BTreeMap<OpId, OpId>,
    /// `getlk` probes in flight (control op → peer request).
    tests: BTreeMap<OpId, OpId>,
    test_by_req: BTreeMap<OpId, OpId>,
    renews: BTreeMap<OpId, RenewInFlight>,
    renew_timer: Option<TimerId>,
    /// A lease read to relearn the owner for renewals is in flight.
    relearning: bool,
    /// Flush attempts per inode with a release in flight.
    flushing: BTreeMap<Ino, u32>,
    // ---- owner side ----
    waiters: Vec<Waiter>,
    next_waiter: u64,
    /// Owner side: while waiters are parked, they are re-served on a
    /// tick — a refusal for stale liveness, an unmarked lease or a grace
    /// period has no event of its own that ends it.
    waiter_tick: Option<TimerId>,
    /// Grants handed back by an outwaited generation's handoff, or
    /// otherwise returning to this table at the next event.
    pending_returns: Vec<Grant>,
    recalls: BTreeMap<GrantId, Recalling>,
    recall_by_req: BTreeMap<OpId, GrantId>,
    /// Subtree grace after an outwaited delegate: `(dir, until)`.
    pub(crate) grace: Vec<(Ino, Ms)>,
    mirror_dirty: bool,
    mirror_ver: u64,
    /// This node as a backup: the holder's last mirror.
    bk_mirror: Option<(u64, Vec<Grant>)>,
    /// Root: grants to hand to a delegate with its first renewal.
    handoff: BTreeMap<u64, Vec<Grant>>,
    /// Root: the grants handed to a generation, as this root recorded
    /// them (its clock), until they would have expired here. The handoff
    /// rides a renewal *reply*, which the generation's recall can
    /// overtake (the delegate answers the recall with nothing, then
    /// drops what arrives for a generation it no longer serves) or which
    /// can be lost (the delegate renews again and serves without them).
    /// Either way the holders still honour the root's windows, so:
    /// every granting renewal re-sends the live ones (the delegate
    /// installs an id once), and a generation that ends without handing
    /// one back leaves it reinstated here — where the next delegation of
    /// the subtree takes it along. A subtree grace here instead covered
    /// only this root's own grants: the next delegate of the subtree
    /// granted over them (sim `locks-delegated` seed 196102, two
    /// exclusive holders).
    handed: BTreeMap<u64, Vec<Grant>>,
    /// Delegate: the moved grant ids a generation installed (a re-sent
    /// handoff installs each once: a grant released or replaced here
    /// since is not revived).
    moved_seen: BTreeMap<u64, std::collections::BTreeSet<GrantId>>,
}

impl LockState {
    pub(crate) fn container_sizes(&self) -> Vec<(&'static str, usize)> {
        vec![
            ("lk_ops", self.ops.len()),
            ("lk_waiters", self.waiters.len()),
            ("lk_recalls", self.recalls.len()),
            ("lk_renews", self.renews.len()),
        ]
    }
}

/// Plan 30 §M14's view for `status`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LockView {
    pub requests_in_flight: usize,
    pub waiters: usize,
    pub recalls_in_flight: usize,
}

/// Where a lock request for `ino` goes.
enum Route {
    /// This node owns it: the usable end of its authority (its clock),
    /// and the generation (0: the root).
    Me {
        cap_ms: i64,
        gen: u64,
    },
    Node(NodeId),
    Unknown,
}

/// What serving a request produced.
enum Served {
    Outcome(LockOutcome),
    Parked,
}

impl Core {
    pub fn lock_view(&self) -> LockView {
        LockView {
            requests_in_flight: self.lk.ops.len(),
            waiters: self.lk.waiters.len(),
            recalls_in_flight: self.lk.recalls.len(),
        }
    }

    fn lock_ttl_ms(&self) -> i64 {
        self.cfg.lock_ttl_ms as i64
    }

    fn lock_margin_ms(&self) -> i64 {
        self.cfg.expiry_margin_ms as i64
    }

    /// A restamp for a grant that changed owner (a move, a reclaim, a
    /// mirror installed): as if renewed now, which is never sooner than
    /// what its holder measured.
    fn restamp(&self, now: Ms) -> i64 {
        now.0 + self.lock_ttl_ms() + self.lock_margin_ms()
    }

    // ------------------------------------------------------------ routing

    fn lock_route(&self, now: Ms, ino: Ino, replica: &dyn Replica) -> Route {
        self.lock_route_for(now, ino, replica, false)
    }

    /// `renewal`: a held lease whose takeover gate is still pending (the
    /// view fenced for M9's floor) still serves renewals and reclaims —
    /// that is what the floor is for; only new grants wait (the
    /// harness's lock-failover found the successor answering `NotOwner`
    /// until the locker's grant lapsed).
    fn lock_route_for(&self, now: Ms, ino: Ino, replica: &dyn Replica, renewal: bool) -> Route {
        if self.cfg.delegation && !replica.delegation_table().is_empty() {
            let keys = Self::read_keys(ino, None);
            if let constellation_meta::delegation::Ownership::Delegated(d) =
                replica.resolve_ownership(&keys)
            {
                if d.node == self.cfg.node_id {
                    return match self.deleg_mine_until(d.gen) {
                        Some(until) if now < until => Route::Me {
                            cap_ms: until.0 - self.lock_margin_ms() - now.0,
                            gen: d.gen,
                        },
                        // Not installed or renewed yet: a request here
                        // would be answered `NotOwner{me}`; wait a tick.
                        _ => Route::Unknown,
                    };
                }
                return Route::Node(d.node);
            }
        }
        if self.lease.usable(now, &self.cfg) && (renewal || !self.lease.fenced()) {
            return Route::Me {
                cap_ms: self.lock_cap_ms(now),
                gen: 0,
            };
        }
        match self
            .lease
            .cached_holder
            .filter(|h| *h != 0 && *h != self.cfg.node_id)
        {
            Some(h) => Route::Node(h),
            None => Route::Unknown,
        }
    }

    /// The root's grants never outlive its lease's usable end (under a
    /// continuation epoch the lease has no expiry; the ttl stands).
    fn lock_cap_ms(&self, now: Ms) -> i64 {
        let Some((lease, _)) = &self.lease.held else {
            return 0;
        };
        if self.lease.epoch_held() {
            return self.lock_ttl_ms();
        }
        lease.expires_unix_ms - self.lock_margin_ms() - now.0
    }

    /// The inode is under a subtree grace (an outwaited delegate).
    fn lock_in_grace(&mut self, now: Ms, ino: Ino, replica: &dyn Replica) -> bool {
        self.lk.grace.retain(|(_, until)| *until > now);
        if self.lk.grace.is_empty() {
            return false;
        }
        let dirs: Vec<Ino> = self.lk.grace.iter().map(|(d, _)| *d).collect();
        dirs.iter()
            .any(|d| *d == constellation_fs_core::types::ROOT_INO || replica.is_under(ino, *d))
    }

    /// Whether new grants are refused now (reclaims still accepted).
    fn lock_grace_active(&mut self, now: Ms, ino: Ino, replica: &dyn Replica) -> bool {
        replica.read_delegations().quarantine_until() > now.0
            || self.lock_in_grace(now, ino, replica)
    }

    // ------------------------------------------------------------ owner side

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_request(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        ino: Ino,
        mode: LockMode,
        blocking: bool,
        sent: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.note_foreign(now, replica, out);
        // A re-sent request of a parked node: re-attach (and answer at
        // once if its grant is ready).
        // A re-sent request of a parked node: re-attach (and answer at
        // once if its grant is ready). Two ops of one node on one inode
        // are two waiters (matched by mode; sim seed 93007).
        if let Some(i) = self
            .lk
            .waiters
            .iter()
            .position(|w| w.node == from && w.ino == ino && w.mode == mode && w.op.is_none())
        {
            let hold = self.cfg.recall_hold_ms;
            let wid = self.lk.waiters[i].id;
            let timer = self.set_timer(now.plus(hold), Timer::LockHeldReply(wid), out);
            let w = &mut self.lk.waiters[i];
            w.sent = sent;
            w.recv = now;
            w.req = Some(req);
            let old = w.held_timer.replace(timer);
            if let Some(old) = old {
                self.cancel_timer(old, out);
            }
            self.lock_serve_waiters(now, ino, replica, out);
            return;
        }
        match self.lock_serve(
            now,
            from,
            ino,
            mode,
            blocking,
            Some(req),
            None,
            sent,
            replica,
            out,
        ) {
            Served::Outcome(outcome) => out.push(Action::Send {
                to: from,
                msg: PeerMsg::LockReply { req, outcome },
            }),
            Served::Parked => {}
        }
    }

    /// Grant, or recall and park. `req`/`op`: the waiter's identity if it
    /// parks. Never called for a re-attach.
    #[allow(clippy::too_many_arguments)]
    fn lock_serve(
        &mut self,
        now: Ms,
        from: NodeId,
        ino: Ino,
        mode: LockMode,
        blocking: bool,
        req: Option<OpId>,
        op: Option<OpId>,
        sent: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Served {
        let (cap_ms, gen) = match self.lock_route(now, ino, replica) {
            Route::Me { cap_ms, gen } => (cap_ms, gen),
            Route::Node(n) => return Served::Outcome(LockOutcome::NotOwner { owner: n }),
            Route::Unknown => return Served::Outcome(LockOutcome::NotOwner { owner: 0 }),
        };
        // Plan 30 §M9: a tenure that may be taken over before its lease
        // expires grants only with fresh S3 liveness, and only once the
        // lease says it grants (its successor then waits the horizon
        // out). A delegate's authority is its renewed grant; the root's
        // is the lease.
        let root_owned = self.lock_route_is_root(now, ino, replica);
        if root_owned
            && (!self.strict_answer_allowed(now, out) || !self.ensure_granting_marked(out))
        {
            return Served::Outcome(LockOutcome::Busy);
        }
        if self.lock_grace_active(now, ino, replica) {
            self.stats.lock_grace_refusals += 1;
            return if blocking {
                self.lock_park(now, from, ino, mode, req, op, sent, out);
                Served::Parked
            } else {
                Served::Outcome(LockOutcome::WouldBlock)
            };
        }
        let conflicting =
            match self.lock_try_grant(now, now, from, ino, mode, cap_ms, gen, out, replica) {
                Ok(outcome) => return Served::Outcome(outcome),
                Err(conflicting) => conflicting,
            };
        for g in conflicting {
            self.lock_recall(now, g, replica, out);
        }
        if blocking {
            self.lock_park(now, from, ino, mode, req, op, sent, out);
            Served::Parked
        } else {
            self.stats.lock_would_block += 1;
            Served::Outcome(LockOutcome::WouldBlock)
        }
    }

    /// Grant `mode` to `from` — re-affirming or upgrading *in place* a
    /// live grant it already holds (an owner never downgrades: the
    /// node's local locks may be under the stronger mode; the sim found
    /// a shared request in flight behind an exclusive one) — or the
    /// conflicting grants to recall. `base`: when the request being
    /// answered arrived (`now` for one answered on arrival; see
    /// [`Waiter::recv`]).
    #[allow(clippy::too_many_arguments)]
    fn lock_try_grant(
        &mut self,
        now: Ms,
        base: Ms,
        from: NodeId,
        ino: Ino,
        mode: LockMode,
        cap_ms: i64,
        gen: u64,
        out: &mut Vec<Action>,
        replica: &dyn Replica,
    ) -> Result<LockOutcome, Vec<Grant>> {
        let own = replica.locks().own_grant(ino, from, now.0);
        if own.is_some_and(|g| g.recalled) {
            // Recalled: the node gives it up once its local locks are
            // gone; nothing is granted meanwhile (a re-affirmation kept
            // the id, and the release then dropped a grant with locks
            // under it — the harness's SQLite run found this). The
            // request parks (or would block) until the release.
            return Err(Vec::new());
        }
        let mode = own.map_or(mode, |g| g.mode.max(mode));
        let conflicting = if own.is_some_and(|g| g.mode == mode) {
            Vec::new()
        } else {
            replica.locks().conflicting(ino, from, mode, now.0)
        };
        if !conflicting.is_empty() {
            return Err(conflicting);
        }
        let ttl = self.lock_ttl_ms().min(cap_ms);
        if ttl <= 0 {
            return Ok(LockOutcome::Busy);
        }
        let until = base.0.min(now.0) + ttl + self.lock_margin_ms();
        if !replica.note_grant_horizon(until) {
            return Ok(LockOutcome::Busy);
        }
        // Every grant is a new id, an own grant re-asked for included:
        // the node may have dropped (lapsed, released) the id the owner
        // still holds, and it refuses to reinstall a released id (sim
        // seed 90017 looped on that). The old id dies with its recall
        // bookkeeping; the node replaces it under its local locks.
        let id = {
            {
                if let Some(old) = own {
                    if let Some(r) = self.lk.recalls.remove(&old.id) {
                        self.cancel_timer(r.timer, out);
                        if let Some(req) = r.req {
                            self.lk.recall_by_req.remove(&req);
                        }
                    }
                }
                replica
                    .locks()
                    .grant(self.cfg.node_id, from, ino, mode, until, gen)
            }
        };
        self.stats.lock_grants += 1;
        self.lk.mirror_dirty = true;
        let position = self.lock_position(ino, replica);
        tracing::debug!(
            node = self.cfg.node_id,
            to = from,
            ino,
            ?mode,
            ttl,
            upgraded = own.is_some(),
            "granted a lock"
        );
        Ok(LockOutcome::Granted {
            id,
            mode,
            ttl_ms: ttl as u64,
            position,
        })
    }

    fn lock_route_is_root(&self, now: Ms, ino: Ino, replica: &dyn Replica) -> bool {
        if self.cfg.delegation && !replica.delegation_table().is_empty() {
            let keys = Self::read_keys(ino, None);
            if let constellation_meta::delegation::Ownership::Delegated(_) =
                replica.resolve_ownership(&keys)
            {
                return false;
            }
        }
        let _ = now;
        true
    }

    /// The position a grant carries: everything acknowledged on `ino`
    /// before it (as a ReadIndex answer would say).
    fn lock_position(&self, ino: Ino, replica: &dyn Replica) -> Position {
        if self.cfg.delegation && !replica.delegation_table().is_empty() {
            let keys = Self::read_keys(ino, None);
            if let constellation_meta::delegation::Ownership::Delegated(d) =
                replica.resolve_ownership(&keys)
            {
                if d.node == self.cfg.node_id {
                    let mut s = constellation_meta::Streams::NONE;
                    let idx = replica.stream_applied(d.gen);
                    if idx > 0 {
                        s.raise(d.gen, idx);
                    }
                    return Position {
                        seq: replica.applied_seq().unwrap_or(0),
                        pending: None,
                        streams: s,
                    };
                }
            }
        }
        let epoch = self.lease.epoch().unwrap_or(0);
        let pending = if replica.unshipped_touches_read(ino, false, None) {
            replica.journal_position(epoch)
        } else {
            None
        };
        Position {
            seq: self.ship.head_seq,
            pending,
            streams: Default::default(),
        }
    }

    fn lock_recall(&mut self, now: Ms, g: Grant, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.lk.recalls.contains_key(&g.id) {
            return;
        }
        let timer = self.set_timer(Ms(g.until_ms), Timer::LockGrantExpiry(g.id), out);
        let req = if g.node == self.cfg.node_id {
            None
        } else {
            Some(self.op_id())
        };
        self.lk.recalls.insert(
            g.id,
            Recalling {
                ino: g.ino,
                req,
                timer,
            },
        );
        if let Some(req) = req {
            self.lk.recall_by_req.insert(req, g.id);
        }
        self.stats.lock_recalls_sent += 1;
        replica.locks().mark_recalled(g.id);
        tracing::debug!(
            node = self.cfg.node_id,
            holder = g.node,
            ino = g.ino,
            "recalling a lock grant"
        );
        match req {
            Some(req) => out.push(Action::Send {
                to: g.node,
                msg: PeerMsg::LockRecall {
                    req,
                    ino: g.ino,
                    grant: g.id,
                },
            }),
            // This node's own grant (the owner as a lock user): the same
            // path as a peer's recall, in place.
            None => self.lock_recall_here(now, g.ino, g.id, replica, out),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn lock_park(
        &mut self,
        now: Ms,
        from: NodeId,
        ino: Ino,
        mode: LockMode,
        req: Option<OpId>,
        op: Option<OpId>,
        sent: Ms,
        out: &mut Vec<Action>,
    ) {
        self.stats.lock_waiters_parked += 1;
        self.lk.next_waiter += 1;
        let id = self.lk.next_waiter;
        self.lock_arm_waiter_tick(now, out);
        let held_timer = req.map(|_| {
            self.set_timer(
                now.plus(self.cfg.recall_hold_ms),
                Timer::LockHeldReply(id),
                out,
            )
        });
        self.lk.waiters.push(Waiter {
            id,
            node: from,
            ino,
            mode,
            req,
            sent,
            recv: now,
            op,
            held_timer,
            since: now,
        });
        if let Some(op) = op {
            if let Some(o) = self.lk.ops.get_mut(&op) {
                o.local_wait = true;
            }
        }
    }

    fn lock_arm_waiter_tick(&mut self, now: Ms, out: &mut Vec<Action>) {
        if self.lk.waiter_tick.is_some() {
            return;
        }
        let at = now.plus(self.cfg.recall_hold_ms.max(50));
        let t = self.set_timer(at, Timer::LockWaiterTick, out);
        self.lk.waiter_tick = Some(t);
    }

    pub(crate) fn on_lock_waiter_tick(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.lk.waiter_tick = None;
        if self.lk.waiters.is_empty() {
            return;
        }
        self.lock_reserve_all(now, replica, out);
        if !self.lk.waiters.is_empty() {
            self.lock_arm_waiter_tick(now, out);
        }
    }

    /// A waiter's RPC has been held long enough: answer `Waiting` (the
    /// requester re-sends; the grant is pushed if it comes first).
    pub(crate) fn on_lock_held_reply_timer(&mut self, wid: u64, out: &mut Vec<Action>) {
        // Re-sent well inside the window a grant served from the re-send
        // would have (`ttl - margin` from its arrival): a waiter silent
        // for that long is skipped (`lock_serve_waiters`).
        let retry_ms = (self
            .cfg
            .lock_ttl_ms
            .saturating_sub(self.cfg.expiry_margin_ms)
            / 2)
        .max(50);
        let Some(w) = self.lk.waiters.iter_mut().find(|w| w.id == wid) else {
            return;
        };
        w.held_timer = None;
        if let Some(req) = w.req.take() {
            self.stats.lock_waiting_replies += 1;
            out.push(Action::Send {
                to: w.node,
                msg: PeerMsg::LockReply {
                    req,
                    outcome: LockOutcome::Waiting { retry_ms },
                },
            });
        }
    }

    /// Something on `ino` changed hands: serve its waiters in order.
    fn lock_serve_waiters(
        &mut self,
        now: Ms,
        ino: Ino,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let idx: Vec<usize> = (0..self.lk.waiters.len())
            .filter(|i| self.lk.waiters[*i].ino == ino)
            .collect();
        let ttl = self.lock_ttl_ms();
        let margin = self.lock_margin_ms();
        let mut done = Vec::new();
        let mut gone = Vec::new();
        for i in idx {
            let (node, mode, req, op, sent, recv) = {
                let w = &self.lk.waiters[i];
                (w.node, w.mode, w.req, w.op, w.sent, w.recv)
            };
            // A remote waiter not heard from within its window (a live
            // one re-sends every `(ttl - margin) / 2`) would find a grant lapsed on
            // arrival and ask again anyway — or it is gone (killed while
            // parked): granting it would make everyone behind it wait out
            // `ttl + margin` for nothing, once per such waiter. Skipped,
            // not granted; a re-send re-attaches it in place, and one
            // silent for long is dropped (it re-parks if it ever asks).
            if op.is_none() && req.is_none() {
                if now.0 >= recv.0 + 4 * ttl {
                    gone.push(i);
                    continue;
                }
                if now.0 >= recv.0 + ttl - margin {
                    continue;
                }
            }
            let base = if op.is_some() { now } else { recv };
            match self.lock_serve_again(now, base, node, ino, mode, replica, out) {
                Some(outcome) => {
                    done.push(i);
                    self.lock_deliver(now, node, ino, req, op, sent, outcome, replica, out);
                }
                None => {
                    // Still conflicting (or in grace): stays parked.
                }
            }
        }
        if !gone.is_empty() {
            self.stats.lock_waiters_dropped += gone.len() as u64;
            tracing::debug!(
                node = self.cfg.node_id,
                ino,
                n = gone.len(),
                "dropped lock waiters silent for 4 × ttl"
            );
        }
        done.extend(gone);
        done.sort_unstable();
        for i in done.into_iter().rev() {
            let w = self.lk.waiters.remove(i);
            if let Some(t) = w.held_timer {
                self.cancel_timer(t, out);
            }
            let waited = now.since(w.since).max(0) as u64;
            self.stats.lock_wait_ms_total += waited;
        }
    }

    /// Try a parked request again: `Some` once it can be answered (a
    /// grant, or a refusal that ends the wait).
    #[allow(clippy::too_many_arguments)]
    fn lock_serve_again(
        &mut self,
        now: Ms,
        base: Ms,
        from: NodeId,
        ino: Ino,
        mode: LockMode,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Option<LockOutcome> {
        let (cap_ms, gen) = match self.lock_route(now, ino, replica) {
            Route::Me { cap_ms, gen } => (cap_ms, gen),
            Route::Node(n) => return Some(LockOutcome::NotOwner { owner: n }),
            Route::Unknown => return Some(LockOutcome::NotOwner { owner: 0 }),
        };
        let root_owned = self.lock_route_is_root(now, ino, replica);
        if root_owned
            && (!self.strict_answer_allowed(now, out) || !self.ensure_granting_marked(out))
        {
            return None;
        }
        if self.lock_grace_active(now, ino, replica) {
            return None;
        }
        match self.lock_try_grant(now, base, from, ino, mode, cap_ms, gen, out, replica) {
            Ok(outcome) => Some(outcome),
            Err(conflicting) => {
                for g in conflicting {
                    self.lock_recall(now, g, replica, out);
                }
                None
            }
        }
    }

    /// Answer a (formerly) parked request: over its RPC, by a push, or
    /// to this node's own op.
    #[allow(clippy::too_many_arguments)]
    fn lock_deliver(
        &mut self,
        now: Ms,
        node: NodeId,
        ino: Ino,
        req: Option<OpId>,
        op: Option<OpId>,
        sent: Ms,
        outcome: LockOutcome,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if let Some(op) = op {
            // A local waiter: the owner's clock is this node's, so the
            // grant's window starts now (sim seed 90017: stamped from a
            // request made seconds ago, it arrived lapsed).
            if let Some(o) = self.lk.ops.get_mut(&op) {
                o.sent_at = now;
            }
            let me = self.cfg.node_id;
            self.lock_op_outcome(now, op, me, outcome, replica, out);
            return;
        }
        match req {
            Some(req) => out.push(Action::Send {
                to: node,
                msg: PeerMsg::LockReply { req, outcome },
            }),
            None => out.push(Action::Send {
                to: node,
                msg: PeerMsg::LockGranted { ino, sent, outcome },
            }),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_released(
        &mut self,
        now: Ms,
        from: NodeId,
        ino: Ino,
        grant: GrantId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if replica
            .locks()
            .get(grant)
            .is_some_and(|g| g.node == from && g.ino == ino)
        {
            self.stats.lock_recalls_released += 1;
            self.lock_grant_done(now, grant, ino, replica, out);
        }
    }

    pub(crate) fn on_lock_recalled_ack(&mut self, req: OpId) {
        // The recall arrived; the release (or the expiry) follows.
        if let Some(id) = self.lk.recall_by_req.remove(&req) {
            if let Some(r) = self.lk.recalls.get_mut(&id) {
                r.req = None;
            }
        }
    }

    pub(crate) fn on_lock_grant_expiry(
        &mut self,
        now: Ms,
        id: GrantId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(g) = replica.locks().get(id) else {
            // Replaced or released meanwhile: whatever waited on it is
            // served now (the sim found a waiter stranded here).
            if let Some(r) = self.lk.recalls.remove(&id) {
                if let Some(req) = r.req {
                    self.lk.recall_by_req.remove(&req);
                }
                self.lock_serve_waiters(now, r.ino, replica, out);
            }
            return;
        };
        if g.until_ms > now.0 {
            // Renewed since: wait again.
            let timer = self.set_timer(Ms(g.until_ms), Timer::LockGrantExpiry(id), out);
            if let Some(r) = self.lk.recalls.get_mut(&id) {
                r.timer = timer;
            }
            return;
        }
        self.stats.lock_recalls_expired += 1;
        tracing::info!(
            node = self.cfg.node_id,
            holder = g.node,
            ino = g.ino,
            "a lock grant's recall went unanswered; outwaited it (TTL + margin)"
        );
        self.lock_grant_done(now, id, g.ino, replica, out);
    }

    fn lock_grant_done(
        &mut self,
        now: Ms,
        id: GrantId,
        ino: Ino,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        replica.locks().forget(id);
        self.lk.mirror_dirty = true;
        if let Some(r) = self.lk.recalls.remove(&id) {
            self.cancel_timer(r.timer, out);
            if let Some(req) = r.req {
                self.lk.recall_by_req.remove(&req);
            }
        }
        self.lock_serve_waiters(now, ino, replica, out);
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_renew(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        entries: Vec<LockRenewEntry>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let mut results = Vec::with_capacity(entries.len());
        for e in entries {
            let r = self.lock_renew_one(now, from, e.ino, e.grant, e.mode, replica);
            results.push((e.ino, e.grant, r));
        }
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::LockRenewed { req, results },
        });
    }

    fn lock_renew_one(
        &mut self,
        now: Ms,
        from: NodeId,
        ino: Ino,
        id: GrantId,
        mode: LockMode,
        replica: &dyn Replica,
    ) -> LockRenewResult {
        let (cap_ms, gen) = match self.lock_route_for(now, ino, replica, true) {
            Route::Me { cap_ms, gen } => (cap_ms, gen),
            Route::Node(n) => return LockRenewResult::NotOwner { owner: n },
            Route::Unknown => return LockRenewResult::NotOwner { owner: 0 },
        };
        let ttl = self.lock_ttl_ms().min(cap_ms);
        if ttl <= 0 {
            return LockRenewResult::NotOwner { owner: 0 };
        }
        let until = now.0 + ttl + self.lock_margin_ms();
        if let Some((mode, recalled)) = replica.locks().extend(id, from, until) {
            self.stats.lock_renewals_served += 1;
            return LockRenewResult::Ok {
                ttl_ms: ttl as u64,
                recalled,
                id,
                mode,
            };
        }
        // Not that id, but this node holds a newer grant here (a reply
        // that never arrived replaced it): renew that one and say so
        // (sim seed 90013 fenced a healthy node otherwise).
        if let Some(g) = replica.locks().own_grant(ino, from, now.0) {
            if let Some((mode, recalled)) = replica.locks().extend(g.id, from, until) {
                self.stats.lock_renewals_served += 1;
                return LockRenewResult::Ok {
                    ttl_ms: ttl as u64,
                    recalled,
                    id: g.id,
                    mode,
                };
            }
        }
        // Unknown: a reclaim during a grace period, if nothing conflicts.
        let grace = self.lock_grace_active(now, ino, replica);
        let conflicting = replica.locks().conflicting(ino, from, mode, now.0);
        if grace && conflicting.is_empty() && replica.note_grant_horizon(until) {
            replica.locks().install(Grant {
                id,
                node: from,
                ino,
                mode,
                until_ms: until,
                recalled: false,
                gen,
            });
            self.stats.lock_reclaimed += 1;
            self.lk.mirror_dirty = true;
            tracing::info!(node = self.cfg.node_id, from, ino, "reclaimed a lock grant");
            return LockRenewResult::Ok {
                ttl_ms: ttl as u64,
                recalled: false,
                id,
                mode,
            };
        }
        LockRenewResult::Lost
    }

    // ------------------------------------------------------------ getlk

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_test(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        ino: Ino,
        mode: LockMode,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let outcome = match self.lock_route(now, ino, replica) {
            Route::Me { .. } => match replica.locks().first_conflicting(ino, from, mode, now.0) {
                Some(g) => LockTestOutcome::Held {
                    node: g.node,
                    mode: g.mode,
                },
                None => LockTestOutcome::Free,
            },
            Route::Node(n) => LockTestOutcome::NotOwner { owner: n },
            Route::Unknown => LockTestOutcome::NotOwner { owner: 0 },
        };
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::LockTestReply { req, outcome },
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_test_control(
        &mut self,
        now: Ms,
        op: OpId,
        ino: Ino,
        mode: LockMode,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        match self.lock_route(now, ino, replica) {
            Route::Me { .. } => {
                let answer =
                    match replica
                        .locks()
                        .first_conflicting(ino, self.cfg.node_id, mode, now.0)
                    {
                        Some(g) => LockTestAnswer::Held {
                            node: g.node,
                            mode: g.mode,
                        },
                        None => LockTestAnswer::Free,
                    };
                out.push(Action::ControlDone {
                    op,
                    result: Ok(ControlOk::LockTest(answer)),
                });
            }
            Route::Node(n) if self.cfg.p2p && self.reaches(now, n) => {
                let req = self.op_id();
                self.lk.tests.insert(op, req);
                self.lk.test_by_req.insert(req, op);
                self.set_timer(
                    now.plus(self.cfg.forward_timeout_ms),
                    Timer::LockTestTimeout(req),
                    out,
                );
                out.push(Action::Send {
                    to: n,
                    msg: PeerMsg::LockTest { req, ino, mode },
                });
            }
            _ => out.push(Action::ControlDone {
                op,
                result: Ok(ControlOk::LockTest(LockTestAnswer::Free)),
            }),
        }
    }

    pub(crate) fn on_lock_test_reply(
        &mut self,
        req: OpId,
        outcome: LockTestOutcome,
        out: &mut Vec<Action>,
    ) {
        let Some(op) = self.lk.test_by_req.remove(&req) else {
            return;
        };
        if self.lk.tests.remove(&op).is_none() {
            return;
        }
        let answer = match outcome {
            LockTestOutcome::Held { node, mode } => LockTestAnswer::Held { node, mode },
            // Best effort: `getlk` is advisory.
            LockTestOutcome::Free | LockTestOutcome::NotOwner { .. } => LockTestAnswer::Free,
        };
        out.push(Action::ControlDone {
            op,
            result: Ok(ControlOk::LockTest(answer)),
        });
    }

    pub(crate) fn on_lock_test_timeout(&mut self, req: OpId, out: &mut Vec<Action>) {
        self.on_lock_test_reply(req, LockTestOutcome::Free, out);
    }

    // ------------------------------------------------------------ node side

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_control(
        &mut self,
        now: Ms,
        op: OpId,
        ino: Ino,
        mode: LockMode,
        blocking: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.stats.lock_requests += 1;
        self.lk.ops.insert(
            op,
            LockOp {
                ino,
                mode,
                blocking,
                req: None,
                owner: 0,
                sent_at: now,
                attempts: 0,
                since: now,
                local_wait: false,
            },
        );
        self.lock_route_op(now, op, replica, out);
    }

    fn lock_route_op(&mut self, now: Ms, op: OpId, replica: &dyn Replica, out: &mut Vec<Action>) {
        let Some(o) = self.lk.ops.get(&op) else {
            return;
        };
        let (ino, mode, blocking) = (o.ino, o.mode, o.blocking);
        // A grant this node holds that is recalled or being released:
        // no request leaves until it is gone (a reply for it could cross
        // its own `LockReleased` and reinstall a forgotten grant — sim
        // seeds 90000/90004). A non-blocking request while local locks
        // still pin the recalled grant would block: say so.
        if let Some(h) = replica.locks().held(ino) {
            if h.until_ms <= now.0 {
                // Lapsed: it is fenced already and cannot be renewed;
                // drop it (the owner outwaits it) and ask afresh.
                replica.locks().drop_held(ino, h.id);
            } else if h.recalled || h.releasing {
                if !blocking && !replica.locks().local_idle(ino) {
                    self.stats.lock_would_block += 1;
                    self.lk.ops.remove(&op);
                    out.push(Action::ControlDone {
                        op,
                        result: Ok(ControlOk::Lock(LockAnswer::WouldBlock)),
                    });
                    return;
                }
                self.set_timer(now.plus(10), Timer::LockRetry(op), out);
                return;
            }
        }
        match self.lock_route(now, ino, replica) {
            Route::Me { .. } => {
                let me = self.cfg.node_id;
                if let Some(o) = self.lk.ops.get_mut(&op) {
                    o.sent_at = now;
                    o.owner = me;
                }
                match self.lock_serve(
                    now,
                    me,
                    ino,
                    mode,
                    blocking,
                    None,
                    Some(op),
                    now,
                    replica,
                    out,
                ) {
                    Served::Outcome(outcome) => {
                        self.lock_op_outcome(now, op, me, outcome, replica, out)
                    }
                    Served::Parked => {}
                }
            }
            Route::Node(n) => {
                if !self.cfg.p2p || !self.reaches(now, n) {
                    self.lock_op_unreachable(now, op, out);
                    return;
                }
                let req = self.op_id();
                let Some(o) = self.lk.ops.get_mut(&op) else {
                    return;
                };
                o.req = Some(req);
                o.owner = n;
                o.sent_at = now;
                o.attempts += 1;
                self.lk.by_req.insert(req, op);
                self.set_timer(
                    now.plus(self.cfg.forward_timeout_ms),
                    Timer::LockRequestTimeout(req),
                    out,
                );
                out.push(Action::Send {
                    to: n,
                    msg: PeerMsg::LockRequest {
                        req,
                        ino,
                        mode,
                        blocking,
                        sent: now,
                    },
                });
            }
            Route::Unknown => {
                if !self.cfg.p2p {
                    self.lock_op_unreachable(now, op, out);
                    return;
                }
                if let Some(o) = self.lk.ops.get_mut(&op) {
                    o.attempts += 1;
                }
                self.issue_s3(S3Op::LeaseGet, super::S3For::LockHolder(op), out);
            }
        }
    }

    /// No P2P path to the owner: a non-blocking request fails
    /// (`ENOLCK`); a blocking one keeps trying.
    fn lock_op_unreachable(&mut self, now: Ms, op: OpId, out: &mut Vec<Action>) {
        let Some(o) = self.lk.ops.get(&op) else {
            return;
        };
        if o.blocking && self.cfg.p2p {
            self.lock_retry(now, op, out);
            return;
        }
        self.stats.lock_unavailable += 1;
        self.lk.ops.remove(&op);
        out.push(Action::ControlDone {
            op,
            result: Ok(ControlOk::Lock(LockAnswer::Unavailable)),
        });
    }

    fn lock_retry(&mut self, now: Ms, op: OpId, out: &mut Vec<Action>) {
        let Some(o) = self.lk.ops.get(&op) else {
            return;
        };
        if !o.blocking && o.attempts >= self.cfg.forward_retries {
            self.stats.lock_unavailable += 1;
            self.lk.ops.remove(&op);
            out.push(Action::ControlDone {
                op,
                result: Ok(ControlOk::Lock(LockAnswer::Unavailable)),
            });
            return;
        }
        let delay = (self.cfg.forward_backoff_ms / 4).max(10) * u64::from(o.attempts.clamp(1, 8));
        self.set_timer(now.plus(delay), Timer::LockRetry(op), out);
    }

    pub(crate) fn on_lock_retry(
        &mut self,
        now: Ms,
        op: OpId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self
            .lk
            .ops
            .get(&op)
            .is_some_and(|o| o.req.is_none() && !o.local_wait)
        {
            return;
        }
        self.lock_route_op(now, op, replica, out);
    }

    pub(crate) fn on_lock_holder_learned(
        &mut self,
        now: Ms,
        op: OpId,
        result: crate::event::S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.lk.ops.contains_key(&op) {
            return;
        }
        if let crate::event::S3Result::LeaseGet(Ok(Some((lease, _)))) = &result {
            self.lease.note_object(now, lease);
            if lease.holder != 0 && !lease.is_claimable(now.0) && lease.holder != self.cfg.node_id {
                self.lease.cached_holder = Some(lease.holder);
                self.lock_route_op(now, op, replica, out);
                return;
            }
        }
        // Nobody usable holds it: a lock needs a sequencer as a write
        // does — acquire (the sim found blocked lockers with nobody
        // taking the lease). Non-blocking: unavailable after the retries.
        let claimable = matches!(&result, crate::event::S3Result::LeaseGet(Ok(l))
            if l.as_ref().is_none_or(|(l, _)| l.holder == 0 || l.is_claimable(now.0)));
        if claimable && !self.lease.lost {
            self.enqueue_job(
                now,
                super::jobs::JobReq::Acquire {
                    reason: "lock",
                    ask_handoff: self.cfg.p2p,
                },
                replica,
                out,
            );
        }
        self.lock_retry(now, op, out);
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_reply(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        outcome: LockOutcome,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(op) = self.lk.by_req.remove(&req) else {
            return;
        };
        if !self.lk.ops.get(&op).is_some_and(|o| o.req == Some(req)) {
            return;
        }
        self.note_p2p_result(now, from, true);
        if let Some(o) = self.lk.ops.get_mut(&op) {
            o.req = None;
        }
        self.lock_op_outcome(now, op, from, outcome, replica, out);
    }

    /// A grant pushed to a waiter whose request was answered `Waiting`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_granted_push(
        &mut self,
        now: Ms,
        from: NodeId,
        ino: Ino,
        sent: Ms,
        outcome: LockOutcome,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // The op is matched by inode and owner; the window counts from
        // the echoed send (this clock, the request the push answers) or
        // the op's current send, whichever is earlier — a push queued
        // behind a pause answers an older attempt (sim seed 191008).
        let granted_mode = match &outcome {
            LockOutcome::Granted { mode, .. } => Some(*mode),
            _ => None,
        };
        let op = self
            .lk
            .ops
            .iter()
            .filter(|(_, o)| {
                o.ino == ino && o.owner == from && granted_mode.is_none_or(|m| m.covers(o.mode))
            })
            .map(|(op, _)| *op)
            .next();
        let Some(op) = op else {
            // No op waits for it: the grant is unknown here; the owner's
            // recall will find no holder and outwait it, or its next
            // request replaces it.
            return;
        };
        if let Some(o) = self.lk.ops.get_mut(&op) {
            if let Some(req) = o.req.take() {
                self.lk.by_req.remove(&req);
            }
            if sent < o.sent_at {
                o.sent_at = sent;
            }
        }
        self.lock_op_outcome(now, op, from, outcome, replica, out);
    }

    fn lock_op_outcome(
        &mut self,
        now: Ms,
        op: OpId,
        from: NodeId,
        outcome: LockOutcome,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let margin = self.lock_margin_ms();
        let Some(o) = self.lk.ops.get_mut(&op) else {
            return;
        };
        o.local_wait = false;
        match outcome {
            LockOutcome::Granted {
                id,
                mode,
                ttl_ms,
                position,
            } => {
                let ttl = ttl_ms as i64;
                if o.sent_at.0 + ttl - margin <= now.0 {
                    // Lapsed on arrival (a request held longer than the
                    // window): ask again rather than install a fence.
                    self.lock_retry(now, op, out);
                    return;
                }
                let held = HeldGrant {
                    id,
                    mode,
                    until_ms: o.sent_at.0 + ttl - margin,
                    renew_at_ms: o.sent_at.0 + ttl / 2,
                    owner: from,
                    recalled: false,
                    position,
                    renewing: None,
                    releasing: false,
                    first_use: false,
                    idle_since_ms: None,
                };
                let ino = o.ino;
                let waited = now.since(o.since).max(0) as u64;
                let recalled = match replica.locks().install_held(ino, held) {
                    Installed::Ok { recalled } => recalled,
                    Installed::Released => {
                        // This node released that id already (the reply
                        // crossed the release): ask again.
                        self.stats.lock_released_replies += 1;
                        self.lock_retry(now, op, out);
                        return;
                    }
                };
                self.lk.ops.remove(&op);
                self.stats.lock_grant_ms_total += waited;
                let b = (64 - waited.max(1).leading_zeros()).min(13) as usize;
                self.stats.lock_grant_ms[b] += 1;
                if held.renew_at_ms <= now.0 {
                    // Served late in its window (a push to a request
                    // parked a while): renew now, not at the next tick,
                    // which may come after the window.
                    if let Some(t) = self.lk.renew_timer.take() {
                        self.cancel_timer(t, out);
                    }
                    let t = self.set_timer(now.plus(1), Timer::LockRenewTick, out);
                    self.lk.renew_timer = Some(t);
                } else {
                    self.lock_arm_renew_tick(now, out);
                }
                out.push(Action::ControlDone {
                    op,
                    result: Ok(ControlOk::Lock(LockAnswer::Granted { position })),
                });
                if recalled {
                    // Recalled before it arrived: the FUSE thread takes
                    // its local lock first (the grant covers it until the
                    // last local lock leaves), then `LockIdle` releases.
                    self.stats.lock_granted_recalled += 1;
                }
            }
            LockOutcome::Waiting { retry_ms } => {
                self.set_timer(now.plus(retry_ms), Timer::LockRetry(op), out);
            }
            LockOutcome::WouldBlock => {
                self.lk.ops.remove(&op);
                out.push(Action::ControlDone {
                    op,
                    result: Ok(ControlOk::Lock(LockAnswer::WouldBlock)),
                });
            }
            LockOutcome::NotOwner { owner } => {
                if owner != 0 && owner != from && owner != self.cfg.node_id {
                    let names_delegate = replica.delegation_table().iter().any(|e| e.node == owner);
                    if !names_delegate {
                        self.lease.cached_holder = Some(owner);
                    }
                } else if owner == 0 && from != self.cfg.node_id {
                    self.lease.cached_holder = None;
                }
                self.lock_retry(now, op, out);
            }
            LockOutcome::Busy => self.lock_retry(now, op, out),
        }
    }

    pub(crate) fn on_lock_request_timeout(&mut self, now: Ms, req: OpId, out: &mut Vec<Action>) {
        let Some(op) = self.lk.by_req.remove(&req) else {
            return;
        };
        if let Some(o) = self.lk.ops.get_mut(&op) {
            if o.req == Some(req) {
                o.req = None;
                self.lock_retry(now, op, out);
            }
        }
    }

    /// A lock request, test, recall or renewal failed at the transport.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_request_failed(
        &mut self,
        now: Ms,
        req: OpId,
        to: NodeId,
        outage: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        if let Some(id) = self.lk.recall_by_req.remove(&req) {
            // The grant's expiry answers an undelivered recall.
            if let Some(r) = self.lk.recalls.get_mut(&id) {
                r.req = None;
            }
            return true;
        }
        if let Some(op) = self.lk.by_req.remove(&req) {
            self.note_p2p_result(now, to, !outage);
            if let Some(o) = self.lk.ops.get_mut(&op) {
                if o.req == Some(req) {
                    o.req = None;
                }
                // Twice unanswered: the holder this node remembers may be
                // gone; the next attempt reads the lease.
                if o.attempts >= 2 && self.lease.cached_holder == Some(to) {
                    self.lease.cached_holder = None;
                }
            }
            self.lock_retry(now, op, out);
            return true;
        }
        if self.lk.test_by_req.contains_key(&req) {
            self.on_lock_test_timeout(req, out);
            return true;
        }
        if let Some(r) = self.lk.renews.remove(&req) {
            tracing::debug!(
                node = self.cfg.node_id,
                to,
                outage,
                "lock renewal failed at the transport"
            );
            self.cancel_timer(r.timer, out);
            self.note_p2p_result(now, to, !outage);
            for (ino, id) in r.entries {
                replica.locks().renewal_failed(ino, id);
            }
            self.lock_relearn_owner(out);
            return true;
        }
        false
    }

    // ------------------------------------------------------------ renewals

    fn lock_arm_renew_tick(&mut self, now: Ms, out: &mut Vec<Action>) {
        if self.lk.renew_timer.is_some() {
            return;
        }
        let at = now.plus((self.cfg.lock_ttl_ms / 4).max(50));
        let t = self.set_timer(at, Timer::LockRenewTick, out);
        self.lk.renew_timer = Some(t);
    }

    pub(crate) fn on_lock_renew_tick(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.lk.renew_timer = None;
        let mut relearn = false;
        // A cache nobody locked for a while is given back (its renewals
        // would otherwise go on forever).
        let idle = replica
            .locks()
            .idle_before(now.0 - self.cfg.lock_cache_idle_ms as i64);
        for (ino, _) in idle {
            self.stats.lock_idle_released += 1;
            self.lock_release_begin(now, ino, replica, out);
        }
        // A recalled grant with no local lock under it is released, not
        // renewed; a lapsed one is dropped (sim seed 90010: a grant that
        // answered an op it did not cover was neither used nor released).
        for (ino, h) in replica.locks().held_all() {
            if h.until_ms <= now.0 {
                replica.locks().drop_held(ino, h.id);
            } else if h.recalled && !h.releasing && replica.locks().local_idle(ino) {
                self.lock_release_begin(now, ino, replica, out);
            }
        }
        let due = replica.locks().due_renewals(now.0);
        if !due.is_empty() {
            tracing::debug!(
                node = self.cfg.node_id,
                due = due.len(),
                cached_holder = ?self.lease.cached_holder,
                "lock renewals due"
            );
        }
        let mut by_owner: BTreeMap<NodeId, Vec<(Ino, HeldGrant)>> = BTreeMap::new();
        for (ino, h) in due {
            match self.lock_route(now, ino, replica) {
                Route::Me { .. } => {
                    let me = self.cfg.node_id;
                    let r = self.lock_renew_one(now, me, ino, h.id, h.mode, replica);
                    self.lock_apply_renew_result(now, ino, h.id, now, r, replica, out);
                }
                Route::Node(n) if self.cfg.p2p && self.reaches(now, n) => {
                    by_owner.entry(n).or_default().push((ino, h));
                }
                route => {
                    // The owner is unreachable or unknown (a failover: the
                    // holder this node last heard of is gone): learn the
                    // current one from the lease and renew there (the
                    // harness's lock-failover found renewals sent to a
                    // dead holder until the grant was outwaited).
                    tracing::debug!(
                        node = self.cfg.node_id,
                        ino,
                        unreachable = matches!(route, Route::Node(_)),
                        "lock renewal: no reachable owner; relearning"
                    );
                    replica.locks().renewal_failed(ino, h.id);
                    relearn = true;
                }
            }
        }
        if relearn {
            self.lock_relearn_owner(out);
        }
        for (to, entries) in by_owner {
            let req = self.op_id();
            let timer = self.set_timer(
                now.plus(self.cfg.forward_timeout_ms),
                Timer::LockRenewTimeout(req),
                out,
            );
            self.lk.renews.insert(
                req,
                RenewInFlight {
                    entries: entries.iter().map(|(i, h)| (*i, h.id)).collect(),
                    sent: now,
                    timer,
                },
            );
            tracing::debug!(
                node = self.cfg.node_id,
                to,
                n = entries.len(),
                "sending lock renewals"
            );
            out.push(Action::Send {
                to,
                msg: PeerMsg::LockRenew {
                    req,
                    entries: entries
                        .into_iter()
                        .map(|(ino, h)| LockRenewEntry {
                            ino,
                            grant: h.id,
                            mode: h.mode,
                        })
                        .collect(),
                },
            });
        }
        if replica.locks().held_count() > 0 || !self.lk.renews.is_empty() {
            self.lock_arm_renew_tick(now, out);
        }
    }

    #[allow(clippy::too_many_arguments)]
    /// Re-read the lease to learn the current holder for renewals (once
    /// at a time). `reaches` is no guide here: a dead holder's link stays
    /// "connected" until the transport notices, and a renewal that fails
    /// must not wait for that (the harness's lock-failover outwaited a
    /// live locker this way).
    fn lock_relearn_owner(&mut self, out: &mut Vec<Action>) {
        if self.lk.relearning {
            return;
        }
        self.lk.relearning = true;
        self.issue_s3(S3Op::LeaseGet, super::S3For::LockRenewHolder, out);
    }

    /// The lease read a renewal tick asked for: the holder to renew with.
    pub(crate) fn on_lock_renew_holder(
        &mut self,
        now: Ms,
        result: crate::event::S3Result,
        out: &mut Vec<Action>,
    ) {
        self.lk.relearning = false;
        tracing::debug!(
            node = self.cfg.node_id,
            ok = matches!(&result, crate::event::S3Result::LeaseGet(Ok(Some(_)))),
            "lock renewal: lease read"
        );
        if let crate::event::S3Result::LeaseGet(Ok(Some((lease, _)))) = &result {
            self.lease.note_object(now, lease);
            if lease.holder != 0 && !lease.is_claimable(now.0) && lease.holder != self.cfg.node_id {
                self.lease.cached_holder = Some(lease.holder);
            }
        }
        // Renew at once with what was learned.
        if let Some(t) = self.lk.renew_timer.take() {
            self.cancel_timer(t, out);
        }
        let t = self.set_timer(now.plus(1), Timer::LockRenewTick, out);
        self.lk.renew_timer = Some(t);
    }

    pub(crate) fn on_lock_renewed(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        results: Vec<(Ino, GrantId, LockRenewResult)>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(r) = self.lk.renews.remove(&req) else {
            return;
        };
        self.cancel_timer(r.timer, out);
        self.note_p2p_result(now, from, true);
        let sent = r.sent;
        tracing::debug!(
            node = self.cfg.node_id,
            from,
            ?results,
            "lock renewals answered"
        );
        for (ino, id, result) in results {
            self.lock_apply_renew_result(now, ino, id, sent, result, replica, out);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn lock_apply_renew_result(
        &mut self,
        now: Ms,
        ino: Ino,
        id: GrantId,
        sent: Ms,
        result: LockRenewResult,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        match result {
            LockRenewResult::Ok {
                ttl_ms,
                recalled,
                id: now_id,
                mode,
            } => {
                replica.locks().renewed(
                    ino,
                    id,
                    now_id,
                    mode,
                    sent.0,
                    ttl_ms as i64,
                    self.lock_margin_ms(),
                    recalled,
                );
                self.stats.lock_renewals += 1;
                if recalled {
                    self.lock_release_begin(now, ino, replica, out);
                }
            }
            LockRenewResult::Lost => {
                if replica.locks().lost(ino, id) {
                    self.stats.lock_lost += 1;
                    tracing::warn!(
                        node = self.cfg.node_id,
                        ino,
                        "lock grant lost (the owner does not know it); I/O under local locks is fenced"
                    );
                }
            }
            LockRenewResult::NotOwner { owner } => {
                let names_delegate = replica.delegation_table().iter().any(|e| e.node == owner);
                if owner != 0 && owner != self.cfg.node_id && !names_delegate {
                    self.lease.cached_holder = Some(owner);
                }
                replica.locks().renewal_failed(ino, id);
            }
        }
    }

    pub(crate) fn on_lock_renew_timeout(
        &mut self,
        req: OpId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if let Some(r) = self.lk.renews.remove(&req) {
            self.cancel_timer(r.timer, out);
            for (ino, id) in r.entries {
                replica.locks().renewal_failed(ino, id);
            }
            self.lock_relearn_owner(out);
        }
    }

    // ------------------------------------------------------------ recalls (node)

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_recall(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        ino: Ino,
        grant: GrantId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::LockRecalled { req },
        });
        self.lock_recall_here(now, ino, grant, replica, out);
    }

    fn lock_recall_here(
        &mut self,
        now: Ms,
        ino: Ino,
        grant: GrantId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        match replica.locks().recall_held(ino, grant) {
            Some(false) => self.lock_release_begin(now, ino, replica, out),
            Some(true) => {
                // Local locks under it: released when the last one
                // leaves (`Control::LockIdle`).
            }
            None => {
                // Not held (yet): the reply that brings it installs it
                // recalled (the model's rule; answering "released" here
                // livelocks two contenders).
                replica.locks().note_pending_recall(ino, grant);
            }
        }
    }

    /// The last local lock under a recalled grant left.
    pub(crate) fn on_lock_idle_control(
        &mut self,
        now: Ms,
        op: OpId,
        ino: Ino,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.lock_release_begin(now, ino, replica, out);
        out.push(Action::ControlDone {
            op,
            result: Ok(ControlOk::Done),
        });
    }

    /// Start releasing `ino`'s grant: flush its dirty data first.
    fn lock_release_begin(
        &mut self,
        now: Ms,
        ino: Ino,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(h) = replica.locks().begin_release(ino) else {
            return;
        };
        self.lk.flushing.insert(ino, 0);
        let _ = now;
        out.push(Action::LockFlush { ino, grant: h.id });
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_flushed(
        &mut self,
        now: Ms,
        ino: Ino,
        grant: GrantId,
        ok: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !ok {
            let attempts = self.lk.flushing.entry(ino).or_insert(0);
            *attempts += 1;
            if *attempts < 5 {
                out.push(Action::LockFlush { ino, grant });
                return;
            }
            tracing::warn!(
                node = self.cfg.node_id,
                ino,
                "releasing a recalled lock grant without a successful flush (its expiry would drop it anyway)"
            );
        }
        self.lk.flushing.remove(&ino);
        if !replica.locks().end_release(ino, grant) {
            // Gone already, or a local lock (or the grant's first use)
            // appeared meanwhile: it stays, recalled; the last unlock
            // releases it again (sim seed 90106).
            return;
        }
        self.stats.lock_released += 1;
        match self.lock_route(now, ino, replica) {
            Route::Me { .. } => {
                let me = self.cfg.node_id;
                self.on_lock_released(now, me, ino, grant, replica, out);
            }
            Route::Node(n) => out.push(Action::Send {
                to: n,
                msg: PeerMsg::LockReleased { ino, grant },
            }),
            Route::Unknown => {
                // The owner (whoever it is) outwaits it.
            }
        }
    }

    // ------------------------------------------------------------ mirror

    pub(crate) fn on_lock_mirror(&mut self, from: NodeId, ver: u64, grants: Vec<Grant>) {
        if self.root_node() != Some(from) {
            return;
        }
        if self.lk.bk_mirror.as_ref().is_none_or(|(v, _)| ver > *v) {
            self.lk.bk_mirror = Some((ver, grants));
        }
    }

    /// A fast takeover (this node was the backup): the mirror is this
    /// tenure's table, restamped; M9's floor covers what it misses.
    pub(crate) fn lock_install_mirror(&mut self, now: Ms, replica: &dyn Replica) {
        let Some((_, grants)) = self.lk.bk_mirror.take() else {
            return;
        };
        let until = self.restamp(now);
        let n = grants.len();
        for g in grants {
            replica.locks().install(Grant {
                until_ms: until,
                recalled: false,
                ..g
            });
        }
        if n > 0 {
            tracing::info!(
                node = self.cfg.node_id,
                grants = n,
                "installed the lock mirror"
            );
        }
        self.lk.mirror_dirty = true;
    }

    // ------------------------------------------------------------ delegation moves

    /// Root: a delegation was granted; its subtree's grants go with it
    /// (handed over with the delegate's first renewal).
    pub(crate) fn lock_on_delegated(&mut self, gen: u64, replica: &dyn Replica) {
        let moved = replica.locks().take_where(|ino| {
            let keys = Self::read_keys(ino, None);
            matches!(
                replica.resolve_ownership(&keys),
                constellation_meta::delegation::Ownership::Delegated(d) if d.gen == gen
            )
        });
        if !moved.is_empty() {
            self.lk.mirror_dirty = true;
            self.lk.handoff.entry(gen).or_default().extend(moved);
        }
    }

    /// Root: what to hand a delegate with a granting renewal — what is
    /// waiting for its first one, and again every grant handed to it
    /// before that this root would still consider live (see
    /// [`LockState::handed`]; the delegate installs each id once).
    pub(crate) fn lock_take_handoff(&mut self, now: Ms, gen: u64) -> Vec<Grant> {
        let pending = self.lk.handoff.remove(&gen).unwrap_or_default();
        let handed = self.lk.handed.entry(gen).or_default();
        handed.extend(pending);
        handed.retain(|g| g.until_ms > now.0);
        let grants = handed.clone();
        if grants.is_empty() {
            self.lk.handed.remove(&gen);
        }
        grants
    }

    /// Root: generation `gen` ended. What was waiting for its first
    /// renewal, and what was handed to it and not handed back, returns to
    /// this table as this root recorded it (the holders measure their
    /// windows against the grants it made or renewed; nobody renewed a
    /// grant the delegate never installed, and one it did install and
    /// renew came back with its drained answer — or was capped by its
    /// tenure, which an outwait waited out). A grant here of the same node
    /// on the same inode, or a conflicting one, is newer and stays (see
    /// `LockTables::install_if_consistent`).
    pub(crate) fn lock_on_generation_ended(&mut self, now: Ms, gen: u64, replica: &dyn Replica) {
        let mut back = self.lk.handoff.remove(&gen).unwrap_or_default();
        back.extend(self.lk.handed.remove(&gen).unwrap_or_default());
        let mut n = 0;
        for g in back {
            if g.until_ms > now.0 && replica.locks().install_if_consistent(Grant { gen: 0, ..g }) {
                n += 1;
            }
        }
        if n > 0 {
            self.stats.lock_reinstated += n;
            self.lk.mirror_dirty = true;
            tracing::info!(
                node = self.cfg.node_id,
                gen,
                n,
                "a delegation ended without handing back grants this root handed it; reinstated"
            );
        }
    }

    /// Root: a delegate's drained answer handed `grants` back — installed
    /// restamped; the root's copies of them are done.
    pub(crate) fn lock_install_returned(
        &mut self,
        now: Ms,
        gen: u64,
        grants: Vec<Grant>,
        replica: &dyn Replica,
    ) {
        if let Some(handed) = self.lk.handed.get_mut(&gen) {
            handed.retain(|h| !grants.iter().any(|g| g.node == h.node && g.ino == h.ino));
        }
        if !grants.is_empty() {
            self.lock_install_moved(now, 0, grants, replica);
        }
    }

    /// Delegate: grants that came with a renewal or, root: with a recall
    /// answer — installed restamped.
    pub(crate) fn lock_install_moved(
        &mut self,
        now: Ms,
        gen: u64,
        grants: Vec<Grant>,
        replica: &dyn Replica,
    ) {
        if gen != 0 && self.deleg_mine_until(gen).is_none() {
            // A handoff for a generation this node no longer serves (its
            // recall overtook the renewal reply carrying it, and was
            // answered without it): the root reinstates what it handed
            // when the generation ends. Installed here, the grants would
            // be dropped at that end anyway.
            if !grants.is_empty() {
                tracing::debug!(
                    node = self.cfg.node_id,
                    gen,
                    n = grants.len(),
                    "a lock handoff for a generation not served here; left to the root"
                );
            }
            return;
        }
        let until = self.restamp(now);
        let mut n = 0;
        for g in grants {
            // Tagged with the generation the move names (0: the root's
            // table) — never resolved from the delegation table, which
            // may not say yet (a handoff arrives with the renewal that
            // installs the generation; sim seed 196252 tagged it 0 and
            // lost it at the next recall).
            let g = Grant {
                until_ms: until,
                gen,
                ..g
            };
            if gen == 0 {
                replica.locks().install(g);
            } else {
                // A re-sent handoff: each id once, and never over a
                // newer grant of the same node, or next to a conflicting
                // one, here.
                if !self.lk.moved_seen.entry(gen).or_default().insert(g.id) {
                    continue;
                }
                if !replica.locks().install_if_consistent(g) {
                    continue;
                }
            }
            self.stats.lock_moved += 1;
            n += 1;
        }
        if n > 0 {
            tracing::debug!(
                node = self.cfg.node_id,
                gen,
                n,
                "installed moved lock grants"
            );
        }
        self.lk.mirror_dirty = true;
    }

    /// Delegate: the generation is being recalled (or ended): its
    /// subtree's grants leave this table (handed back, or dropped).
    pub(crate) fn lock_take_generation(&mut self, gen: u64, replica: &dyn Replica) -> Vec<Grant> {
        // By tag, not by the table: a generation the log has already
        // ended resolves to nobody (sim seed 96046 left its grants
        // behind).
        self.lk.moved_seen.remove(&gen);
        let mut taken = replica.locks().take_by_gen(gen);
        let by_tag = taken.len();
        taken.extend(replica.locks().take_where(|ino| {
            let keys = Self::read_keys(ino, None);
            matches!(
                replica.resolve_ownership(&keys),
                constellation_meta::delegation::Ownership::Delegated(d) if d.gen == gen
            )
        }));
        if !taken.is_empty() {
            tracing::debug!(
                node = self.cfg.node_id,
                gen,
                by_tag,
                by_table = taken.len() - by_tag,
                "took a generation's lock grants"
            );
        }
        taken
    }

    /// The table changed hands for some inodes (a move, a lease change):
    /// every waiter is tried again, which answers `NotOwner` to those
    /// whose owner is elsewhere now.
    pub(crate) fn lock_reserve_all(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let mut inos: Vec<Ino> = self.lk.waiters.iter().map(|w| w.ino).collect();
        inos.sort_unstable();
        inos.dedup();
        for ino in inos {
            self.lock_serve_waiters(now, ino, replica, out);
        }
    }

    /// Root: a generation ended by TTL (an outwaited delegate): the
    /// grants moved to it may still be honoured with the windows this
    /// root gave them — a grace on the subtree until they lapse or
    /// reclaim.
    pub(crate) fn lock_on_generation_outwaited(&mut self, now: Ms, gen: u64, dir: Ino) {
        let until = Ms(self.restamp(now));
        self.lk.grace.push((dir, until));
        self.stats.lock_grace_periods += 1;
        // Anything not yet handed over returns to this table at the next
        // event (it never left this node).
        if let Some(back) = self.lk.handoff.remove(&gen) {
            self.lk.pending_returns.extend(back);
        }
    }

    /// A takeover of a lease its holder *released* (not expired): its
    /// grants were capped by a lease still live, so they may be honoured
    /// for up to `ttl` more — a grace on everything, reclaims admitted
    /// (the sim found a successor granting at once).
    pub(crate) fn lock_on_released_takeover(&mut self, now: Ms) {
        let until = Ms(self.restamp(now));
        self.lk
            .grace
            .push((constellation_fs_core::types::ROOT_INO, until));
        self.stats.lock_grace_periods += 1;
    }

    pub(crate) fn locks_start(&self, replica: &dyn Replica) {
        replica
            .locks()
            .seed_ids(u64::from(self.cfg.incarnation) << 40);
    }

    /// The lease is gone (deposed, released, an epoch closed): this
    /// tenure's grants are void (capped by the lease), its waiters retry
    /// elsewhere.
    pub(crate) fn lock_on_lease_gone(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let n = replica.locks().clear_grants();
        if n > 0 {
            tracing::info!(
                node = self.cfg.node_id,
                grants = n,
                "lease gone: lock grants dropped"
            );
        }
        for (_, r) in std::mem::take(&mut self.lk.recalls) {
            self.cancel_timer(r.timer, out);
        }
        self.lk.recall_by_req.clear();
        self.lk.handoff.clear();
        self.lk.handed.clear();
        self.lk.grace.clear();
        let waiters = std::mem::take(&mut self.lk.waiters);
        for w in waiters {
            if let Some(t) = w.held_timer {
                self.cancel_timer(t, out);
            }
            self.lock_deliver(
                now,
                w.node,
                w.ino,
                w.req,
                w.op,
                w.sent,
                LockOutcome::NotOwner { owner: 0 },
                replica,
                out,
            );
        }
    }

    // ------------------------------------------------------------ after every event

    pub(crate) fn locks_after_event(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let returns = std::mem::take(&mut self.lk.pending_returns);
        if !returns.is_empty() {
            self.lock_install_moved(now, 0, returns, replica);
        }
        if self.lk.mirror_dirty {
            self.lk.mirror_dirty = false;
            let backups = self.lease.backups();
            if !backups.is_empty() && self.root_usable(now) {
                self.lk.mirror_ver += 1;
                let grants = replica.locks().grants_snapshot();
                for b in backups {
                    out.push(Action::Send {
                        to: *b,
                        msg: PeerMsg::LockMirror {
                            ver: self.lk.mirror_ver,
                            grants: grants.clone(),
                        },
                    });
                }
            }
        }
    }
}
