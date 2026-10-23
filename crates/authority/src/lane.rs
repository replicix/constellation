//! Plan 41 L2: the lock renewal lane (sans-IO).
//!
//! Every renewal of a lock grant is a step of the node's authority core,
//! and a step that waits on the metadata store (a priority write behind a
//! FUSE writer's slow journal write) renews nothing: the grant lapses and
//! the node's owner is fenced (`EIO`) although nothing is wrong with the
//! node but a slow disk (the xcheckfs run of 2026-10-13, an 18 s and a 24 s
//! store stall). The lane is a small component beside the core that extends
//! windows from memory alone — the lock tables, shared with the FUSE
//! threads — under a **certificate** the core publishes at the end of every
//! step ([`LockLaneView`], `Core::lock_lane_view`, before the driver
//! dispatches the step's actions).
//!
//! It does exactly two things:
//!
//! 1. **Own renewals**: this node's own grant at its own owner table (it is
//!    the root and the holder both), the owner record and the held window in
//!    one `LockTables` critical section ([`LockTables::renew_own`]).
//! 2. **Remote renewals**: it sends `LockRenew` for the held grants that are
//!    due to their owner and applies the `Ok` answers
//!    ([`LockTables::renewed`]).
//!
//! It never grants, upgrades, reclaims, answers `Lost` or `NotOwner`,
//! releases, flushes, drops a held grant, installs a mirror, changes
//! routing, writes or reads the store, or logs (a unit test greps this
//! source for a tracing macro). What it cannot decide goes to the core,
//! which decides as before when its step gets there: every result, `Ok`
//! included, is reported to it ([`LaneAction::Report`]).
//!
//! # Why every extension is one the core could have made
//!
//! 1. *Same process, same clocks.* The lane reads the node's protocol clock;
//!    no new skew term.
//! 2. *Authority (own grants).* The core extends its own grant under
//!    `Route::Me` while the lease is usable and (plan 41 L0) fresh on a tenure
//!    that can be taken over early. The lane requires `now < cap_until` (the
//!    lease's usable end: no TTL takeover before it), freshness on a fast
//!    tenure within `backup_takeover_ms` of a proof of liveness (a seal's
//!    grace covers what was answered within it), a marked lease, no release
//!    in flight, no continuation epoch, no gate, no delegation (checked
//!    live). Each is time-bounded and enforced by the lane, or is revoked by
//!    the certificate of the step that changes it, published before that
//!    step's actions are dispatched (a release's CAS, an epoch's recall): so
//!    any extension precedes the change, and the successor's grace covers it.
//! 3. *The record.* The lane extends only a record that is live at `t` under
//!    the tables' mutex. Every core decision that ends a record (expiry,
//!    release, move, outwait) is one critical section (`LockTables`' atomic
//!    primitives), so it happens before the extension (which then finds
//!    nothing and defers) or after it (and sees it).
//! 4. *Node side.* The held window is extended from an owner's `Ok` with a
//!    `sent` no later than the real send, exactly as the core does; routing
//!    is liveness only (a wrong owner answers `NotOwner`, which goes to the
//!    core).
//! 5. *Bounds.* Own renewals end at the lease cap (the core renews no lease
//!    while stuck). Remote renewals stop once the driver has been stuck for
//!    `max_stall_ms` (default 120 s): a node whose core is hung, not slow,
//!    lets its grants lapse as it always did.

use crate::event::{LockRenewEntry, LockRenewResult};
use crate::ids::{HonourMs, NodeId, Now};
use constellation_fs_core::Ino;
use constellation_meta::locks::{GrantId, LockTables};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

/// Request ids of the lane's `LockRenew` carry this bit, so a core never
/// mistakes a lane answer for one of its own (its ids count from 1, control
/// ids from `1 << 48`).
pub const LOCK_LANE_REQ_BIT: u64 = 1 << 63;

/// How long an answer to a request the lane stopped waiting for is still
/// used (the core's `LATE_RENEW_MS`).
const LATE_MS: i64 = 60_000;

/// How long the authority core has made no progress before the lane treats a
/// mark on a grant as the stuck step's (`RenewLane::tick`).
pub const LANE_CORE_STUCK_MS: i64 = 100;

/// The default bound on remote renewals while the driver is stuck.
pub const DEFAULT_MAX_STALL_MS: i64 = 120_000;

/// What the core publishes for the lane after a step (the *certificate*):
/// the lease mirror the FUSE fast path already reads, plus the few facts
/// the lane needs. The state at the end of the last finished step, never
/// newer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LockLaneView {
    /// This node's id.
    pub me: NodeId,
    /// All of: cluster locks on; the root lease held, usable, not lost, not
    /// fenced (no gate, no release); no continuation epoch open, held or
    /// being re-claimed; this node delegates nothing; not resuming; no
    /// unmarked release in flight; no final mirror sent. Each excluded
    /// state is one whose core rule differs from "extend a live record up to
    /// the lease cap".
    pub root_ok: bool,
    /// The lease's usable end (`honour_until − margin`) as an honour
    /// instant: no grant is renewed past it.
    pub cap_until: HonourMs,
    /// The tenure can be taken over before its lease expires
    /// (`Core::may_be_taken_over_early`), the send of the latest S3 request
    /// that proved it live (honour clock), the freshness window
    /// (`backup_takeover_ms`) and whether the lease is marked
    /// (`Core::tenure_marked`): the freshness rule of `Core::renew_hold`.
    pub fast: bool,
    pub last_fresh: HonourMs,
    pub window_ms: i64,
    pub marked: bool,
    /// Where the renewals of held grants go when no route is known: the
    /// holder as last learned (`lease.cached_holder`, 0: unknown).
    pub owner: NodeId,
    /// This node's protocol clock id: an own renewal confirms a grant
    /// installed as a copy from its send on this clock (plan 43 C7b, row 29;
    /// `LockTables::renew_own`). 0 (no certificate): confirms nothing.
    pub clock: u64,
    /// `lock_ttl_ms`, `expiry_margin_ms`, `forward_timeout_ms`.
    pub lock_ttl_ms: i64,
    pub margin_ms: i64,
    pub forward_timeout_ms: u64,
}

impl LockLaneView {
    /// This node may extend its own grants now: the certificate stands and
    /// the freshness rule holds at `now` (honour clock), as
    /// `Core::renew_hold` requires.
    pub fn own_ok(&self, now: HonourMs) -> bool {
        self.own_ok_with(now, true)
    }

    /// [`Self::own_ok`], with the freshness rule optionally ignored (the
    /// simulation's non-vacuity switch: a lane that renews a fast tenure
    /// through its successor's grace).
    pub fn own_ok_with(&self, now: HonourMs, freshness: bool) -> bool {
        self.root_ok
            && now < self.cap_until
            && (!freshness
                || (crate::core::liveness_fresh(self.fast, now, self.last_fresh, self.window_ms)
                    && self.marked))
    }
}

/// The certificate's cell: the core writes it at the end of every step, the
/// lane (another thread, or the simulation's lane task) reads it.
#[derive(Debug, Default)]
pub struct LaneCell {
    view: Mutex<LockLaneView>,
}

impl LaneCell {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Publish `view` (the core, after a step).
    pub fn publish(&self, view: LockLaneView) {
        let mut g = self.view.lock().unwrap_or_else(|e| e.into_inner());
        if *g != view {
            *g = view;
        }
    }

    pub fn view(&self) -> LockLaneView {
        self.view.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// The read side of the certificate the lane judges by: the cell, and the
/// one fact that must be read live (a `Delegate` record flips it inside a
/// step's own store write).
pub trait LaneFacts {
    fn view(&self) -> LockLaneView;
    /// `Meta::delegations_any()`.
    fn delegations_any(&self) -> bool;
}

/// What drives the lane.
#[derive(Debug)]
pub enum LaneEvent {
    /// Time to look for due renewals. `stuck_ms`: how long the authority
    /// driver has made no progress (0: it is running).
    Tick { stuck_ms: i64 },
    /// The owner answered request `req`.
    Answered {
        req: u64,
        results: Vec<(Ino, GrantId, LockRenewResult)>,
    },
    /// Request `req` was not answered in time (or failed at the transport):
    /// its grants are due again, and a late answer is still used.
    Failed { req: u64 },
}

/// What the lane asks its driver to do.
#[derive(Debug, PartialEq)]
pub enum LaneAction {
    /// Send `LockRenew { req, entries }` to `to`.
    Send {
        to: NodeId,
        req: u64,
        entries: Vec<LockRenewEntry>,
    },
    /// Tell the core, on the urgent lane (`Event::LaneOwn`).
    OwnRenewed { renewed: u32 },
    /// Tell the core what `from` answered (`Event::LaneRenewed`).
    Renewed {
        from: NodeId,
        sent: HonourMs,
        results: Vec<(Ino, GrantId, LockRenewResult)>,
    },
    /// Tell the core that `to` did not answer a renewal in time
    /// (`Event::LaneUnanswered`).
    Unanswered { to: NodeId },
}

/// What the lane did (status).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LaneStats {
    pub own_renewals: u64,
    pub remote_sent: u64,
    pub remote_ok: u64,
    pub late_ok: u64,
    pub deferred: u64,
    pub stall_stops: u64,
    /// Ticks that found grants held, and those of them at which the core was
    /// stuck in a step ([`LANE_CORE_STUCK_MS`]); due own grants the
    /// certificate did not cover (`own_ok` false).
    pub ticks: u64,
    pub stuck_ticks: u64,
    pub uncovered: u64,
    /// The longest the core had been stuck at a tick (ms).
    pub max_stuck_ms: u64,
}

#[derive(Debug)]
struct Pending {
    to: NodeId,
    sent: HonourMs,
    entries: Vec<(Ino, GrantId)>,
}

/// The lane's state: requests in flight and the ones it gave up on.
#[derive(Debug)]
pub struct RenewLane {
    next: u64,
    max_stall_ms: i64,
    /// Test-only non-vacuity: the freshness rule of a fast tenure is not
    /// applied (`RenewLane::ignore_freshness_for_tests`).
    ignores_freshness: bool,
    inflight: HashMap<u64, Pending>,
    late: HashMap<u64, (NodeId, HonourMs)>,
    pub stats: LaneStats,
}

impl RenewLane {
    pub fn new(max_stall_ms: i64) -> Self {
        Self {
            next: 0,
            max_stall_ms,
            ignores_freshness: false,
            inflight: HashMap::new(),
            late: HashMap::new(),
            stats: LaneStats::default(),
        }
    }

    /// The simulation's non-vacuity switch (never set by the daemon): the lane
    /// ignores the freshness rule of a fast tenure.
    #[doc(hidden)]
    pub fn ignore_freshness_for_tests(&mut self, on: bool) {
        self.ignores_freshness = on;
    }

    /// Requests waiting for an answer.
    pub fn in_flight(&self) -> usize {
        self.inflight.len()
    }

    /// Handle one event at `now` (the driver's reading of the protocol clock,
    /// taken before the event is handled and before anything is sent).
    pub fn handle(
        &mut self,
        now: Now,
        event: LaneEvent,
        locks: &LockTables,
        facts: &dyn LaneFacts,
    ) -> Vec<LaneAction> {
        match event {
            LaneEvent::Tick { stuck_ms } => self.tick(now, stuck_ms, locks, facts),
            LaneEvent::Answered { req, results } => self.answered(now, req, results, locks, facts),
            LaneEvent::Failed { req } => {
                let Some(p) = self.inflight.remove(&req) else {
                    return Vec::new();
                };
                // The owner the renewal went to did not answer: it may be
                // gone (a failover). Its grants are due again, no longer
                // routed to it (the next renewal goes to the holder as the
                // core last learned it, `LockLaneView::owner`), and the core
                // learns the holder again from the lease, as after its own
                // renewal's timeout. Routed to the same node again, a dead
                // root's lockers renewed nowhere but there, every forward
                // timeout, holding each grant's `renewing` mark so that the
                // core's tick, which would have read the lease, never took
                // it, until the window ended (the harness's `lock-failover`
                // under a forward wall step, which ends the window early:
                // the late renewal at the successor is refused, EIO).
                for (ino, id) in &p.entries {
                    locks.renewal_failed(*ino, *id);
                    locks.clear_route(*ino, *id, p.to);
                }
                self.late
                    .retain(|_, (_, sent)| p.sent.since(*sent) < LATE_MS);
                self.late.insert(req, (p.to, p.sent));
                vec![LaneAction::Unanswered { to: p.to }]
            }
        }
    }

    fn tick(
        &mut self,
        now: Now,
        stuck_ms: i64,
        locks: &LockTables,
        facts: &dyn LaneFacts,
    ) -> Vec<LaneAction> {
        // The common case, nothing held: no lock taken.
        if locks.held_count() == 0 {
            return Vec::new();
        }
        let view = facts.view();
        // A core stuck in a step: the marks it left on grants it began to
        // renew are not waited for ([`LockTables::due_renewals_lane`]).
        let core_stuck = stuck_ms >= LANE_CORE_STUCK_MS;
        self.stats.ticks += 1;
        self.stats.stuck_ticks += core_stuck as u64;
        self.stats.max_stuck_ms = self.stats.max_stuck_ms.max(stuck_ms.max(0) as u64);
        let due = locks.due_renewals_lane(
            now,
            view.forward_timeout_ms as i64,
            core_stuck,
            |ino, id| {
                self.inflight
                    .values()
                    .any(|p| p.entries.contains(&(ino, id)))
            },
        );
        if due.is_empty() {
            return Vec::new();
        }
        let own_ok =
            view.own_ok_with(now.honour, !self.ignores_freshness) && !facts.delegations_any();
        if !own_ok {
            self.stats.uncovered += due.len() as u64;
        }
        let mut own = 0u32;
        let mut by_owner: BTreeMap<
            NodeId,
            Vec<(Ino, GrantId, constellation_meta::locks::LockMode)>,
        > = BTreeMap::new();
        let freshness = !self.ignores_freshness;
        for (ino, h) in due {
            if own_ok {
                let cap = view.cap_until;
                // The cap and the freshness rule are judged again at the
                // table's own reading, the instant the extension is made
                // from (`renew_own` reads it inside its guard): the tick's
                // reading is older, and a proof of liveness fresh then may
                // be stale by the time the guard is taken.
                // So is the certificate: a step that revoked it (a release,
                // an epoch) after the tick read it was published before its
                // actions went out, and an extension judged at `n` must not
                // rest on what that step revoked (the cell is a mutex the
                // core's publish takes alone: nothing slow under the guard).
                let view = &view;
                if locks.renew_own(
                    ino,
                    h.id,
                    view.me,
                    |n| {
                        let now_view = facts.view();
                        if now_view.own_ok_with(n.honour, freshness) {
                            now_view.cap_until.min(cap).since(n.honour)
                        } else {
                            0
                        }
                    },
                    view.lock_ttl_ms,
                    view.margin_ms,
                    now,
                    view.clock,
                ) {
                    own += 1;
                    continue;
                }
            }
            // A remote renewal: where this grant's last answer came from, else
            // the holder as last learned. Not this node itself (its own grant
            // is the core's when the certificate does not stand).
            let route = h
                .route
                .filter(|r| *r != view.me)
                .or(Some(view.owner).filter(|o| *o != 0 && *o != view.me));
            match route {
                Some(to) if stuck_ms <= self.max_stall_ms => {
                    by_owner.entry(to).or_default().push((ino, h.id, h.mode));
                }
                Some(_) => {
                    // The driver is hung, not slow: let it lapse as before.
                    self.stats.stall_stops += 1;
                    locks.renewal_failed(ino, h.id);
                }
                None => {
                    self.stats.deferred += 1;
                    locks.renewal_failed(ino, h.id);
                }
            }
        }
        let mut out = Vec::new();
        if own > 0 {
            self.stats.own_renewals += own as u64;
            out.push(LaneAction::OwnRenewed { renewed: own });
        }
        for (to, entries) in by_owner {
            self.next += 1;
            let req = LOCK_LANE_REQ_BIT | self.next;
            self.stats.remote_sent += entries.len() as u64;
            self.inflight.insert(
                req,
                Pending {
                    to,
                    sent: now.honour,
                    entries: entries.iter().map(|(i, id, _)| (*i, *id)).collect(),
                },
            );
            out.push(LaneAction::Send {
                to,
                req,
                entries: entries
                    .into_iter()
                    .map(|(ino, grant, mode)| LockRenewEntry {
                        ino,
                        grant,
                        mode,
                        late: false,
                    })
                    .collect(),
            });
        }
        out
    }

    fn answered(
        &mut self,
        now: Now,
        req: u64,
        results: Vec<(Ino, GrantId, LockRenewResult)>,
        locks: &LockTables,
        facts: &dyn LaneFacts,
    ) -> Vec<LaneAction> {
        let margin = facts.view().margin_ms;
        let (from, sent, late) = match (self.inflight.remove(&req), self.late.remove(&req)) {
            (Some(p), _) => {
                let answered: Vec<(Ino, GrantId)> =
                    results.iter().map(|(i, g, _)| (*i, *g)).collect();
                // An entry the answer does not name is due again.
                for (ino, id) in p.entries.iter().filter(|e| !answered.contains(e)) {
                    locks.renewal_failed(*ino, *id);
                }
                (p.to, p.sent, false)
            }
            (None, Some((to, sent))) => (to, sent, true),
            (None, None) => return Vec::new(),
        };
        // Only a granted renewal is applied here, at once (monotone: the
        // window is extended from the send, never shortened). Everything
        // else is the core's to decide.
        let mut reported = Vec::new();
        for (ino, id, result) in results {
            match result {
                LockRenewResult::Ok {
                    ttl_ms,
                    recalled,
                    id: now_id,
                    mode,
                } => {
                    locks.renewed(
                        ino,
                        id,
                        now_id,
                        mode,
                        sent,
                        ttl_ms as i64,
                        margin,
                        recalled,
                        from,
                        now,
                    );
                    if late {
                        self.stats.late_ok += 1;
                    } else {
                        self.stats.remote_ok += 1;
                    }
                    reported.push((ino, id, result));
                }
                other if !late => reported.push((ino, id, other)),
                _ => {}
            }
        }
        if reported.is_empty() {
            return Vec::new();
        }
        vec![LaneAction::Renewed {
            from,
            sent,
            results: reported,
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::WaitMs;
    use constellation_meta::locks::{HeldGrant, LockMode};
    use constellation_meta::Position;

    struct Facts {
        view: LockLaneView,
        delegations: bool,
    }

    impl LaneFacts for Facts {
        fn view(&self) -> LockLaneView {
            self.view.clone()
        }
        fn delegations_any(&self) -> bool {
            self.delegations
        }
    }

    fn view() -> LockLaneView {
        LockLaneView {
            me: 1,
            root_ok: true,
            cap_until: HonourMs(100_000),
            fast: false,
            last_fresh: HonourMs(0),
            window_ms: 1_500,
            marked: true,
            owner: 0,
            clock: 7,
            lock_ttl_ms: 4_000,
            margin_ms: 500,
            forward_timeout_ms: 400,
        }
    }

    fn facts(v: LockLaneView) -> Facts {
        Facts {
            view: v,
            delegations: false,
        }
    }

    fn held(owner: u64, seq: u64, until: i64, renew_at: i64) -> HeldGrant {
        HeldGrant {
            id: GrantId { node: owner, seq },
            mode: LockMode::Exclusive,
            until: HonourMs(until),
            renew_at: HonourMs(renew_at),
            owner,
            recalled: false,
            position: Position::ZERO,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since: None,
            installed: HonourMs(0),
            lapsing: None,
            written: false,
            route: None,
        }
    }

    /// The root (node 1) holds its own grant on inode 7: the owner's record
    /// and the held window, due for renewal at 2 000.
    fn own_grant(t: &LockTables) -> GrantId {
        let id = t.grant(1, 1, 7, LockMode::Exclusive, WaitMs(4_500), 0);
        t.install_held(7, held(1, id.seq, 3_500, 2_000));
        id
    }

    fn at(ms: i64) -> Now {
        Now::same(ms)
    }

    fn tick(lane: &mut RenewLane, t: &LockTables, f: &Facts, ms: i64) -> Vec<LaneAction> {
        lane.handle(at(ms), LaneEvent::Tick { stuck_ms: 0 }, t, f)
    }

    #[test]
    fn an_own_grant_is_renewed_under_the_certificate_in_one_section() {
        let t = LockTables::default();
        let id = own_grant(&t);
        let mut lane = RenewLane::new(DEFAULT_MAX_STALL_MS);
        let f = facts(view());
        // Not due yet.
        assert!(tick(&mut lane, &t, &f, 1_999).is_empty());
        let out = tick(&mut lane, &t, &f, 2_000);
        assert_eq!(out, vec![LaneAction::OwnRenewed { renewed: 1 }]);
        // The owner record: now + ttl + margin, on the wait clock; the held
        // window: sent + ttl - margin.
        assert_eq!(t.get(id).unwrap().until, WaitMs(2_000 + 4_000 + 500));
        let h = t.held(7).unwrap();
        assert_eq!(h.until, HonourMs(2_000 + 4_000 - 500));
        assert_eq!(h.renew_at, HonourMs(2_000 + (4_000 - 500) / 2));
        assert_eq!(h.route, Some(1));
        assert_eq!(lane.stats.own_renewals, 1);
    }

    /// Fail-before (plan 41 L3, the stalls of `lock-stall-lone-node`): the
    /// core's tick marked the own grant a moment before its step stuck; the
    /// lane did not touch a marked grant for the forward timeout and the
    /// grant lapsed (4 of 5 stalls). With the core stuck it renews it.
    #[test]
    fn an_own_grant_the_stuck_cores_tick_marked_is_renewed_by_the_lane() {
        let t = LockTables::default();
        own_grant(&t);
        let mut lane = RenewLane::new(DEFAULT_MAX_STALL_MS);
        let f = facts(view());
        // The core's renewal tick, then its step sticks.
        assert_eq!(t.due_renewals(at(2_000)).len(), 1);
        // A running core is left to finish what it began.
        assert!(tick(&mut lane, &t, &f, 2_050).is_empty());
        // A stuck one is not waited for.
        let out = lane.handle(
            at(2_150),
            LaneEvent::Tick {
                stuck_ms: LANE_CORE_STUCK_MS,
            },
            &t,
            &f,
        );
        assert_eq!(out, vec![LaneAction::OwnRenewed { renewed: 1 }]);
        assert_eq!(t.held(7).unwrap().until, HonourMs(2_150 + 4_000 - 500));
    }

    /// The freshness rule is judged at the instant the extension is made
    /// from: the table's reading inside its guard, not the tick's older
    /// one. Fail-before (plan 41 review): a fast tenure's proof of liveness
    /// fresh at the tick (2 100, 1 100 ms after it) and stale by the guard
    /// (2 600, 1 600 ms, past the 1 500 ms window) renewed the grant from
    /// 2 600, an answer outside the window a sealing successor covers.
    #[test]
    fn the_freshness_rule_holds_at_the_extensions_own_reading() {
        let t = LockTables::default();
        let id = own_grant(&t);
        t.set_clock(Arc::new(|| Now::same(2_600)));
        let mut lane = RenewLane::new(DEFAULT_MAX_STALL_MS);
        let f = facts(LockLaneView {
            fast: true,
            last_fresh: HonourMs(1_000),
            ..view()
        });
        assert!(tick(&mut lane, &t, &f, 2_100).is_empty());
        assert_eq!(t.get(id).unwrap().until, WaitMs(4_500));
        assert_eq!(t.held(7).unwrap().until, HonourMs(3_500));
        // Fresh at the table's reading too: renewed from it.
        t.set_clock(Arc::new(|| Now::same(2_400)));
        assert_eq!(
            tick(&mut lane, &t, &f, 2_200),
            vec![LaneAction::OwnRenewed { renewed: 1 }]
        );
        assert_eq!(t.held(7).unwrap().until, HonourMs(2_400 + 4_000 - 500));
    }

    #[test]
    fn a_renewal_is_never_past_the_cap() {
        // A grant whose window ends at 2 400 (renewal due at 2 000), the
        // record at 2 900.
        let mk = || {
            let t = LockTables::default();
            let id = t.grant(1, 1, 7, LockMode::Exclusive, WaitMs(2_900), 0);
            t.install_held(7, held(1, id.seq, 2_400, 2_000));
            (t, id)
        };
        let mut lane = RenewLane::new(DEFAULT_MAX_STALL_MS);
        let mut v = view();
        // The lease's usable end is 3 000: at 2 000 a 1 s grant at most.
        v.cap_until = HonourMs(3_000);
        let (t, id) = mk();
        let f = facts(v.clone());
        assert_eq!(
            tick(&mut lane, &t, &f, 2_000),
            vec![LaneAction::OwnRenewed { renewed: 1 }]
        );
        assert_eq!(t.held(7).unwrap().until, HonourMs(2_000 + 1_000 - 500));
        // (2 500 < 2 400 is false: the window only ever grows.)
        assert_eq!(t.held(7).unwrap().until, HonourMs(2_500));
        assert_eq!(t.get(id).unwrap().until, WaitMs(2_000 + 1_000 + 500));
        // At the cap itself: nothing, and the mark is cleared (the core's).
        let (t, _) = mk();
        v.cap_until = HonourMs(2_000);
        let f = facts(v);
        assert!(tick(&mut lane, &t, &f, 2_000).is_empty());
        assert_eq!(t.held(7).unwrap().renewing, None);
        assert_eq!(t.held(7).unwrap().until, HonourMs(2_400));
    }

    #[test]
    fn nothing_is_renewed_when_the_certificate_does_not_stand() {
        let base = view();
        let cases: Vec<(&str, LockLaneView, bool)> = vec![
            (
                "no root authority",
                LockLaneView {
                    root_ok: false,
                    ..base.clone()
                },
                false,
            ),
            ("a delegation", base.clone(), true),
            (
                "a fast tenure without fresh liveness",
                LockLaneView {
                    fast: true,
                    last_fresh: HonourMs(500),
                    ..base.clone()
                },
                false,
            ),
            (
                "an unmarked lease",
                LockLaneView {
                    marked: false,
                    ..base.clone()
                },
                false,
            ),
        ];
        for (what, v, delegations) in cases {
            let t = LockTables::default();
            let id = own_grant(&t);
            let mut lane = RenewLane::new(DEFAULT_MAX_STALL_MS);
            let f = Facts {
                view: v,
                delegations,
            };
            let out = tick(&mut lane, &t, &f, 2_100);
            assert!(out.is_empty(), "{what}: {out:?}");
            assert_eq!(t.get(id).unwrap().until, WaitMs(4_500), "{what}");
            assert_eq!(t.held(7).unwrap().until, HonourMs(3_500), "{what}");
            assert_eq!(
                t.held(7).unwrap().renewing,
                None,
                "{what}: left for the core"
            );
        }
        // A fast tenure within the window of its proof: renewed.
        let t = LockTables::default();
        own_grant(&t);
        let mut lane = RenewLane::new(DEFAULT_MAX_STALL_MS);
        let f = facts(LockLaneView {
            fast: true,
            last_fresh: HonourMs(1_000),
            ..base
        });
        assert_eq!(
            tick(&mut lane, &t, &f, 2_100),
            vec![LaneAction::OwnRenewed { renewed: 1 }]
        );
    }

    #[test]
    fn an_expired_owner_record_or_a_missing_held_id_is_left_to_the_core() {
        let t = LockTables::default();
        let id = t.grant(1, 1, 7, LockMode::Exclusive, WaitMs(2_050), 0);
        t.install_held(7, held(1, id.seq, 3_500, 2_000));
        let mut lane = RenewLane::new(DEFAULT_MAX_STALL_MS);
        let f = facts(view());
        // The owner's record ended at 2 050: nothing is revived.
        assert!(tick(&mut lane, &t, &f, 2_100).is_empty());
        assert_eq!(t.get(id).unwrap().until, WaitMs(2_050));
        // A record of another node under the held id: not this node's.
        let t = LockTables::default();
        let id = t.grant(1, 2, 7, LockMode::Exclusive, WaitMs(4_500), 0);
        t.install_held(7, held(1, id.seq, 3_500, 2_000));
        assert!(tick(&mut lane, &t, &f, 2_100).is_empty());
    }

    fn remote(t: &LockTables, owner: u64) -> GrantId {
        let id = GrantId {
            node: owner,
            seq: 3,
        };
        t.install_held(8, held(owner, 3, 3_500, 2_000));
        id
    }

    fn ok(id: GrantId, ttl: u64) -> LockRenewResult {
        LockRenewResult::Ok {
            ttl_ms: ttl,
            recalled: false,
            id,
            mode: LockMode::Exclusive,
        }
    }

    #[test]
    fn a_held_grant_is_renewed_at_its_owner_and_the_answer_applied_from_the_send() {
        let t = LockTables::default();
        let id = remote(&t, 3);
        let mut lane = RenewLane::new(DEFAULT_MAX_STALL_MS);
        let mut v = view();
        v.root_ok = false;
        v.owner = 3;
        let f = facts(v);
        let out = tick(&mut lane, &t, &f, 2_000);
        let [LaneAction::Send { to, req, entries }] = out.as_slice() else {
            panic!("{out:?}");
        };
        assert_eq!(*to, 3);
        assert!(req & LOCK_LANE_REQ_BIT != 0, "a lane id");
        assert_eq!(entries.len(), 1);
        assert!(!entries[0].late);
        // `sent` is the tick's reading, however late the answer is.
        let out = lane.handle(
            at(2_300),
            LaneEvent::Answered {
                req: *req,
                results: vec![(8, id, ok(id, 4_000))],
            },
            &t,
            &f,
        );
        let h = t.held(8).unwrap();
        assert_eq!(h.until, HonourMs(2_000 + 4_000 - 500));
        assert_eq!(h.route, Some(3));
        assert_eq!(h.renewing, None);
        assert_eq!(
            out,
            vec![LaneAction::Renewed {
                from: 3,
                sent: HonourMs(2_000),
                results: vec![(8, id, ok(id, 4_000))],
            }]
        );
        // The next tick uses the route, not the (now unknown) holder.
        let mut v = view();
        v.root_ok = false;
        v.owner = 0;
        let f = facts(v);
        let out = tick(&mut lane, &t, &f, 2_000 + 4_000 / 2);
        assert!(
            matches!(out.as_slice(), [LaneAction::Send { to: 3, .. }]),
            "{out:?}"
        );
    }

    /// A renewal its route (the owner that answered the last one) does not
    /// answer is no longer routed there: the core is told (it reads the
    /// lease for the owner) and the next renewal goes to the holder as the
    /// core learned it. Sent to the route again, the lockers of a root that
    /// died renewed nowhere but at it until their windows ended.
    #[test]
    fn an_unanswered_route_is_dropped_and_the_core_told() {
        let t = LockTables::default();
        let id = remote(&t, 3);
        let mut lane = RenewLane::new(DEFAULT_MAX_STALL_MS);
        let mut v = view();
        v.root_ok = false;
        v.owner = 3;
        let f = facts(v.clone());
        let out = tick(&mut lane, &t, &f, 2_000);
        let LaneAction::Send { req, .. } = &out[0] else {
            panic!("{out:?}")
        };
        lane.handle(
            at(2_010),
            LaneEvent::Answered {
                req: *req,
                results: vec![(8, id, ok(id, 4_000))],
            },
            &t,
            &f,
        );
        assert_eq!(t.held(8).unwrap().route, Some(3));
        // Node 3 dies; the core has learned node 5 as the holder since.
        v.owner = 5;
        let f = facts(v);
        let due = 2_000 + (4_000 - 500) / 2;
        let out = tick(&mut lane, &t, &f, due);
        let [LaneAction::Send { to: 3, req, .. }] = out.as_slice() else {
            panic!("{out:?}")
        };
        let out = lane.handle(at(due + 400), LaneEvent::Failed { req: *req }, &t, &f);
        assert_eq!(out, vec![LaneAction::Unanswered { to: 3 }]);
        assert_eq!(t.held(8).unwrap().route, None);
        let out = tick(&mut lane, &t, &f, due + 400);
        assert!(
            matches!(out.as_slice(), [LaneAction::Send { to: 5, .. }]),
            "{out:?}"
        );
    }

    #[test]
    fn a_stale_mark_is_sent_again_and_a_late_ok_counts_from_its_own_send() {
        let t = LockTables::default();
        let id = remote(&t, 3);
        let mut lane = RenewLane::new(DEFAULT_MAX_STALL_MS);
        let mut v = view();
        v.root_ok = false;
        v.owner = 3;
        let f = facts(v);
        let first = tick(&mut lane, &t, &f, 2_000);
        let LaneAction::Send { req: r1, .. } = &first[0] else {
            panic!()
        };
        // Inside the forward timeout: one renewal in flight, no second.
        assert!(tick(&mut lane, &t, &f, 2_399).is_empty());
        // The core's tick is stuck, the answer queued behind it: past the
        // timeout the lane asks again.
        let second = tick(&mut lane, &t, &f, 2_400);
        let LaneAction::Send { req: r2, .. } = &second[0] else {
            panic!("{second:?}")
        };
        assert_ne!(r1, r2);
        // The first request is given up on, then its answer comes: applied
        // as `Ok` only, from its own send (2 000), never shortening what
        // the second answer gave.
        lane.handle(at(2_500), LaneEvent::Failed { req: *r1 }, &t, &f);
        let out = lane.handle(
            at(2_600),
            LaneEvent::Answered {
                req: *r1,
                results: vec![(8, id, ok(id, 4_000))],
            },
            &t,
            &f,
        );
        assert_eq!(t.held(8).unwrap().until, HonourMs(2_000 + 3_500));
        assert_eq!(lane.stats.late_ok, 1);
        assert!(
            matches!(out.as_slice(), [LaneAction::Renewed { sent, .. }] if *sent == HonourMs(2_000))
        );
        // A late refusal is the core's (the lane drops it).
        let out = lane.handle(
            at(2_700),
            LaneEvent::Answered {
                req: *r1,
                results: vec![(8, id, LockRenewResult::Lost)],
            },
            &t,
            &f,
        );
        assert!(out.is_empty());
    }

    #[test]
    fn non_ok_results_go_to_the_core_and_unnamed_entries_are_due_again() {
        let t = LockTables::default();
        let id = remote(&t, 3);
        let mut lane = RenewLane::new(DEFAULT_MAX_STALL_MS);
        let mut v = view();
        v.root_ok = false;
        v.owner = 3;
        let f = facts(v);
        let out = tick(&mut lane, &t, &f, 2_000);
        let LaneAction::Send { req, .. } = &out[0] else {
            panic!()
        };
        let before = t.held(8).unwrap().until;
        let out = lane.handle(
            at(2_100),
            LaneEvent::Answered {
                req: *req,
                results: vec![(8, id, LockRenewResult::NotOwner { owner: 5 })],
            },
            &t,
            &f,
        );
        assert_eq!(t.held(8).unwrap().until, before, "nothing extended");
        assert!(
            matches!(out.as_slice(), [LaneAction::Renewed { results, .. }]
            if matches!(results[0].2, LockRenewResult::NotOwner { owner: 5 }))
        );
        // (The core clears the mark when it applies the result; if it does
        // not get to it, the mark goes stale and the lane asks again.)
        assert!(tick(&mut lane, &t, &f, 2_100).is_empty());
        // An answer that names nothing leaves the entry due again.
        let out = tick(&mut lane, &t, &f, 2_500);
        let LaneAction::Send { req, .. } = &out[0] else {
            panic!("{out:?}")
        };
        lane.handle(
            at(2_150),
            LaneEvent::Answered {
                req: *req,
                results: vec![],
            },
            &t,
            &f,
        );
        assert_eq!(t.held(8).unwrap().renewing, None);
    }

    #[test]
    fn remote_renewals_stop_once_the_driver_is_hung_and_own_ones_do_not() {
        let t = LockTables::default();
        let _ = remote(&t, 3);
        let mut lane = RenewLane::new(10_000);
        let mut v = view();
        v.root_ok = false;
        v.owner = 3;
        let f = facts(v);
        let out = lane.handle(at(2_000), LaneEvent::Tick { stuck_ms: 10_001 }, &t, &f);
        assert!(out.is_empty());
        assert_eq!(lane.stats.stall_stops, 1);
        assert_eq!(t.held(8).unwrap().renewing, None, "left to lapse as before");
        let out = lane.handle(at(2_000), LaneEvent::Tick { stuck_ms: 10_000 }, &t, &f);
        assert!(matches!(out.as_slice(), [LaneAction::Send { .. }]));
        // Own renewals have no such bound: the lease cap ends them.
        let t = LockTables::default();
        own_grant(&t);
        let f = facts(view());
        let out = lane.handle(
            at(2_000),
            LaneEvent::Tick {
                stuck_ms: 1_000_000,
            },
            &t,
            &f,
        );
        assert_eq!(out, vec![LaneAction::OwnRenewed { renewed: 1 }]);
    }

    #[test]
    fn a_grant_with_no_known_owner_is_left_to_the_core() {
        let t = LockTables::default();
        let _ = remote(&t, 3);
        let mut lane = RenewLane::new(DEFAULT_MAX_STALL_MS);
        let mut v = view();
        v.root_ok = false;
        v.owner = 0;
        let f = facts(v);
        assert!(tick(&mut lane, &t, &f, 2_000).is_empty());
        assert_eq!(lane.stats.deferred, 1);
        assert_eq!(t.held(8).unwrap().renewing, None);
    }

    /// The lane logs nothing: a dropped line is lost evidence, a log storm
    /// could crowd out the lines that matter, and the lane's liveness must
    /// not depend on the log's queue.
    #[test]
    fn the_lane_never_logs() {
        let source = include_str!("lane.rs");
        let code = source.split("#[cfg(test)]").next().unwrap();
        for macro_name in ["tracing::", "log::", "println!", "eprintln!"] {
            assert!(!code.contains(macro_name), "lane.rs uses {macro_name}");
        }
    }

    /// Integration review (plan 41 L2 x a revoking step): the core's step
    /// that revokes the certificate (a marked release: `root_ok` false,
    /// then its CAS dispatched; a taker's grace counts from the CAS) runs
    /// between the tick's reading of the certificate and the extension
    /// inside the tables' guard. The extension is judged at the guard's
    /// reading of the clock, so it must be judged against the certificate
    /// as of then too, or it is made from an instant after the revocation.
    /// Fails on the parent: the tick's snapshot of the certificate was
    /// used inside the guard.
    #[test]
    fn a_certificate_revoked_before_the_guard_extends_nothing() {
        struct Cell(Arc<Mutex<LockLaneView>>);
        impl LaneFacts for Cell {
            fn view(&self) -> LockLaneView {
                self.0.lock().unwrap().clone()
            }
            fn delegations_any(&self) -> bool {
                false
            }
        }
        let t = LockTables::default();
        own_grant(&t);
        let cell = Arc::new(Mutex::new(view()));
        let f = Cell(cell.clone());
        // The tables' clock is read inside the guard: the core's revoking
        // step has published by then.
        let revoke = cell.clone();
        t.set_clock(Arc::new(move || {
            revoke.lock().unwrap().root_ok = false;
            Now::same(2_000)
        }));
        let mut lane = RenewLane::new(DEFAULT_MAX_STALL_MS);
        let out = lane.handle(at(2_000), LaneEvent::Tick { stuck_ms: 0 }, &t, &f);
        assert!(
            !out.contains(&LaneAction::OwnRenewed { renewed: 1 }),
            "{out:?}"
        );
        assert_eq!(t.held(7).unwrap().until, HonourMs(3_500), "not extended");
    }
}
