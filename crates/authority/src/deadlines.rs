//! Every protocol bound (a deadline, cap, window or horizon that a safety or
//! I4 argument rests on) computed in one place from the base knobs of
//! [`Config`].
//!
//! The module is pure: no clock and no `Core`. Each value is a span or a
//! duration in ms (`u64`, or `i64` where the site it stands for is signed);
//! the caller adds it to the clock the site uses today (`WaitMs` or
//! `HonourMs`) and keeps its own signedness at the edges. A bound that also
//! needs a runtime input (a link's round trip, a lease's time left) is a
//! function taking that input.
//!
//! Cadences (fixed ticks that set no bound) are not here.

use crate::core::Config;

/// The base knobs every bound derives from, as `Config` holds them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Base {
    /// L: `ttl_ms`.
    pub lease_ttl: u64,
    /// m: `expiry_margin_ms`.
    pub margin: u64,
    /// R: `read_delegation_ttl_ms`.
    pub read_ttl: u64,
    /// D: `delegation_ttl_ms`.
    pub deleg_ttl: u64,
    /// K: `lock_ttl_ms`.
    pub lock_ttl: u64,
    /// F: `forward_timeout_ms`.
    pub forward_timeout: u64,
    /// T: `backup_takeover_ms`.
    pub takeover: u64,
    /// k: `delegation_stream_tick_ms`.
    pub stream_tick: u64,
    pub forward_backoff: u64,
    pub acquire_retry_max: u64,
    pub s3_less_deadline: u64,
    pub inbox_deadline: u64,
    pub settle_window_knob: u64,
    pub drop_held_await_knob: u64,
    pub promise_ttl: u64,
    pub backup_heartbeat: u64,
    pub backup_ack_timeout: u64,
    pub backup_slow_max: u64,
    /// The store's request timeout (`0`: not known; D13 and D14 are then skipped).
    pub s3_request_timeout: u64,
}

impl Base {
    pub fn new(cfg: &Config) -> Base {
        Base {
            lease_ttl: cfg.ttl_ms,
            margin: cfg.expiry_margin_ms,
            read_ttl: cfg.read_delegation_ttl_ms,
            deleg_ttl: cfg.delegation_ttl_ms,
            lock_ttl: cfg.lock_ttl_ms,
            forward_timeout: cfg.forward_timeout_ms,
            takeover: cfg.backup_takeover_ms,
            stream_tick: cfg.delegation_stream_tick_ms,
            forward_backoff: cfg.forward_backoff_ms,
            acquire_retry_max: cfg.acquire_retry_max_ms,
            s3_less_deadline: cfg.s3_less_deadline_ms,
            inbox_deadline: cfg.inbox_deadline_ms,
            settle_window_knob: cfg.settle_window_ms,
            drop_held_await_knob: cfg.drop_held_await_ms,
            promise_ttl: cfg.promise_ttl_ms,
            backup_heartbeat: cfg.backup_heartbeat_ms,
            backup_ack_timeout: cfg.backup_ack_timeout_ms,
            backup_slow_max: cfg.backup_slow_max_ms,
            s3_request_timeout: cfg.s3_request_timeout_ms,
        }
    }
}

/// Every derived bound, computed once from a [`Config`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deadlines {
    pub base: Base,
    /// A, the client deadline.
    pub client_deadline: u64,
    /// w: the settle window.
    /// w: the settle window actually used: a quarter of the client deadline
    /// unless a knob is set, and never more than half of it.
    pub settle_window: u64,
    /// f: how old the read an inbox op executes from may be, on the executing
    /// node's own wait clock: half a settle window. A drain discards older
    /// bytes and reads the key again (the I4-safety rule of the inbox path).
    pub inbox_read_fresh: u64,
    /// A client op's own attempts end this long after its submit: `A - w`.
    pub attempts: u64,
    pub inbox_attempts: u64,
    /// The span of the settle cap past a settle's start: two settle windows,
    /// the reclaim horizon, the takeover window (an inherited generation's
    /// outwait ends a horizon and the window past the tenure's start), the
    /// longer of two forward timeouts and the release seal window, and the
    /// request timeout the outwait hold adds.
    pub settle_cap_span: u64,
    /// The bound a placeholder waits for its requester's retry:
    /// `drop_held_await_ms`, else the cap, and never more than the cap,
    /// `A - 2w` floored at one settle window (a settle window of half the
    /// deadline would make the cap 0, and a placeholder would be settled on
    /// the first tick, before its requester could retry). An own op on a
    /// placeholder's keys waits behind it, then for the settle's row to
    /// commit (one settle window), and must still execute inside its own
    /// window, `A - w`.
    pub drop_held_await: u64,
    /// How long a requester's shadow of a non-stealable answer may wait for
    /// its rid's outcome in the log, by the wall clock, before it is rolled
    /// back and replayed: `drop_held_await_ms`, else the client deadline.
    /// Not the placeholder bound's cap: the shadow's owner may hold its row
    /// for a whole client deadline.
    pub stale_shadow_after: u64,
    /// DRT: the request timeout of the delegation lane.
    pub deleg_request_timeout: u64,
    /// H: the reclaim horizon.
    pub reclaim_horizon: u64,
    pub grant_in_flight: u64,
    pub deleg_resend: u64,
    pub recall_backoff_max: u64,
    pub release_seal_window: u64,
    pub release_stream_window: u64,
    pub inherit_slack: u64,
    pub abandon_read_wait: u64,
    pub abandon_heard_window: u64,
    pub leave_wait: u64,
    pub leave_resend: u64,
    pub leave_note_lapse: u64,
    pub read_block: u64,
    /// The longest a persisted read-grant horizon leads the write that makes
    /// it: a read delegation's TTL, the margin the owner outwaits it by, a
    /// second of write-ahead (`Core::grant_horizon_covers`) and the second
    /// the write rounds up by.
    pub read_span: u64,
    /// The same for the lock-grant horizon: the longest grant (a lock's TTL,
    /// or a delegation's), its margin, the quarter TTL a renewal's write
    /// rounds up by and the second the write rounds up by.
    pub lock_span: u64,
    /// The same for the takeover floor a successor persists: the takeover
    /// window, the longer of a read delegation's and a lock's TTL, the
    /// write-delegation TTL, three margins and the second the write rounds
    /// up by.
    pub floor_span: u64,
    /// The same for the last promise this node issued.
    pub promise_span: u64,
    /// The takeover ack floor without the reclaim horizon term
    /// ([`Deadlines::ack_floor`] adds it).
    pub ack_floor_reads: u64,
    pub ack_floor_locks: u64,
    /// The longest bound a lock owner that cannot grant yet can have (a fast
    /// successor's floor: the takeover window, the longest grant, the
    /// delegations' reclaim horizon and the two margins), and the bound an
    /// op without its sequencer has on top, for the failover before it.
    pub lock_grace_wait_max: u64,
    /// The one bound of every wait a non-blocking lock request takes for an
    /// owner that cannot grant yet, from the request's start: a stale owner's
    /// lease (just renewed, plus the margin skew may add), its backup's
    /// acquisition, and the successor's grace. Finite for every
    /// configuration.
    pub lock_owner_wait_max: u64,
    /// How soon a non-blocking lock request told to wait for an owner that
    /// cannot say when it may grant asks again, the first time.
    pub lock_poll: u64,
    pub lock_min_grant: u64,
    pub lock_renew_gap: u64,
    pub claim_grace: u64,
    pub lag_threshold: u64,
    pub recall_hold: u64,
    pub lane_tick: u64,
    pub backup_heartbeat: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// The configuration is refused.
    Refuse,
    /// The configuration is logged.
    Warn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    pub id: &'static str,
    pub class: Class,
    pub msg: String,
}

/// The ceiling of a recall resend's back-off, before the request timeout
/// lifts it.
const RECALL_BACKOFF_CAP_MS: u64 = 2_000;
/// What a lock-renewal tick may be at most apart.
const LOCK_RENEW_GAP_MAX_MS: u64 = 250;
/// A leave waits this much on top of its grants' end.
const LEAVE_WAIT_SLACK_MS: u64 = 5_000;
/// A leaving node's note counts as lapsed after this many resends.
const LEAVE_NOTE_LAPSE_RESENDS: u64 = 3;

/// The delegation TTL a deployment gets when it sets none: the lock TTL,
/// and at least the core's own 5 s. One rule for every host of the core
/// ([`normalize`] applies it to a configuration whose `delegation_ttl_ms` is
/// `0`): a delegate's grants are then capped by about `lock_ttl - 2 * margin`
/// and by 2 to 3 s only where the TTL is set short (the sim's
/// `Config::defaults` keeps the 5 s: its delegations are the short ones,
/// whatever its lock TTL).
pub fn default_delegation_ttl(lock_ttl_ms: u64) -> u64 {
    lock_ttl_ms.max(5_000)
}

/// The lock TTL a daemon runs with: the configured one, cut to what D8
/// allows, `lease_ttl - 2 * margin` ([`Deadlines::lock_grace_fits_lease`]),
/// and at least 1 ms. A shorter grant only costs more renewals; a longer one
/// would outlive the lease that a TTL successor waits out.
pub fn lock_ttl_fitting_lease(lock_ttl_ms: u64, lease_ttl_ms: u64, margin_ms: u64) -> u64 {
    lock_ttl_ms
        .min(lease_ttl_ms.saturating_sub(2 * margin_ms))
        .max(1)
}

/// The lock lane's tick: a quarter of the forward timeout, between 50 and
/// 250 ms. The engine's lane loop and the simulation's both read it.
pub fn lane_tick_ms(forward_timeout_ms: u64) -> u64 {
    (forward_timeout_ms / 4).clamp(50, 250)
}

/// The derivations every host of the core applies to a configuration once
/// its knobs are set (the daemon's `load_config`, the simulation's driver at
/// spawn), so both run the same derived configuration:
///
/// - a forwarded reply held for recalls answers `Held` at half the forward
///   timeout, well inside the requester's RPC;
/// - the backup knobs: an ack timeout and a takeover window of at least 1
///   ms, a slow maximum of at least the ack timeout, a heartbeat of at most
///   a third of the takeover window (a longer one lets the backup seal a
///   holder that beats on time);
/// - the delegation TTL, when unset (`0`), is [`default_delegation_ttl`].
pub fn normalize(cfg: &mut Config) {
    cfg.recall_hold_ms = (cfg.forward_timeout_ms / 2).max(1);
    cfg.backup_ack_timeout_ms = cfg.backup_ack_timeout_ms.max(1);
    cfg.backup_slow_max_ms = cfg.backup_slow_max_ms.max(cfg.backup_ack_timeout_ms);
    cfg.backup_takeover_ms = cfg.backup_takeover_ms.max(1);
    cfg.backup_heartbeat_ms = cfg
        .backup_heartbeat_ms
        .max(1)
        .min(cfg.backup_takeover_ms / 3)
        .max(1);
    if cfg.delegation_ttl_ms == 0 {
        cfg.delegation_ttl_ms = default_delegation_ttl(cfg.lock_ttl_ms);
    }
}

impl Deadlines {
    pub fn new(cfg: &Config) -> Deadlines {
        let b = Base::new(cfg);
        let (l, m, r, d, k) = (b.lease_ttl, b.margin, b.read_ttl, b.deleg_ttl, b.lock_ttl);
        let (f, t, tick) = (b.forward_timeout, b.takeover, b.stream_tick);
        let a = 2 * l;
        let w = match b.settle_window_knob {
            0 => a / 4,
            ms => ms.min(a / 2),
        };
        let drt = f.max(tick * 4).max(1);
        let h = d + m;
        let seal = t.max(f);
        let drop_cap = a.saturating_sub(2 * w).max(w);
        let longest_grant = r.max(k);
        let lock_ttl = k.max(d);
        let ack = |first: u64| t + first + 2 * m;
        let grace = t + longest_grant + h + 2 * m + b.s3_less_deadline;
        Deadlines {
            base: b,
            client_deadline: a,
            settle_window: w,
            inbox_read_fresh: w / 2,
            attempts: a - w,
            inbox_attempts: b.inbox_deadline.saturating_sub(w).max(b.inbox_deadline / 2),
            settle_cap_span: 2 * w + h + t + (2 * f).max(seal) + drt,
            drop_held_await: match b.drop_held_await_knob {
                0 => drop_cap,
                ms => ms.min(drop_cap),
            },
            stale_shadow_after: match b.drop_held_await_knob {
                0 => a,
                ms => ms,
            },
            deleg_request_timeout: drt,
            reclaim_horizon: h,
            grant_in_flight: 2 * tick + 2 * drt,
            deleg_resend: crate::deleg_lane::stuck_threshold_ms(d / 2) as u64
                + crate::deleg_lane::DELEG_RESEND_SLACK_MS as u64,
            recall_backoff_max: RECALL_BACKOFF_CAP_MS.max(drt),
            release_seal_window: seal,
            release_stream_window: f,
            inherit_slack: t,
            abandon_read_wait: r + m,
            abandon_heard_window: d,
            leave_wait: d.saturating_mul(2) + f + LEAVE_WAIT_SLACK_MS,
            leave_resend: f.max(500),
            leave_note_lapse: f
                .max(500)
                .saturating_mul(LEAVE_NOTE_LAPSE_RESENDS)
                .max(d.saturating_mul(2)),
            read_block: 2 * (r + m),
            read_span: r + m + 2_000,
            lock_span: lock_ttl + lock_ttl / 4 + m + 1_000,
            floor_span: t + longest_grant + d + 3 * m + 1_000,
            promise_span: b.promise_ttl + 1_000,
            ack_floor_reads: ack(r),
            ack_floor_locks: ack(longest_grant),
            lock_grace_wait_max: grace,
            lock_owner_wait_max: l + m + b.acquire_retry_max + grace,
            lock_poll: (b.forward_backoff / 4).max(10),
            lock_min_grant: (2 * m).min(k),
            lock_renew_gap: (k.saturating_sub(m) / 64).min(LOCK_RENEW_GAP_MAX_MS),
            claim_grace: 2 * t,
            lag_threshold: (t / 8).max(1),
            recall_hold: (f / 2).max(1),
            lane_tick: lane_tick_ms(f),
            backup_heartbeat: b.backup_heartbeat.max(1).min(t / 3).max(1),
        }
    }

    /// Every invariant between the bounds, the failed ones only.
    pub fn check(&self) -> Vec<Violation> {
        let b = &self.base;
        let (l, m, r, d, k) = (b.lease_ttl, b.margin, b.read_ttl, b.deleg_ttl, b.lock_ttl);
        let (f, t) = (b.forward_timeout, b.takeover);
        let (a, w) = (self.client_deadline, self.settle_window);
        let mut v = Vec::new();
        let mut fail = |id: &'static str, class: Class, ok: bool, msg: String| {
            if !ok {
                v.push(Violation { id, class, msg });
            }
        };
        fail(
            "D1",
            Class::Refuse,
            2 * m < d && m < r && 2 * m < l,
            format!(
                "expiry margin {m} ms must stay below half the delegation TTL ({d}), the read \
                 delegation TTL ({r}) and half the lease TTL ({l})"
            ),
        );
        fail(
            "D2",
            Class::Refuse,
            w <= a / 2 && f < w,
            format!("settle window {w} ms must be at most half the client deadline ({a}) and above the forward timeout ({f})"),
        );
        fail(
            "D3",
            Class::Warn,
            w <= a / 3,
            format!("settle window {w} ms is above a third of the client deadline ({a})"),
        );
        fail(
            "D4",
            Class::Warn,
            a >= l + self.claim_grace + b.acquire_retry_max,
            format!(
                "client deadline {a} ms is shorter than the lease TTL ({l}), the claim grace ({}) \
                 and the acquire retry ({})",
                self.claim_grace, b.acquire_retry_max
            ),
        );
        let need = self.reclaim_horizon
            + self.inherit_slack
            + (2 * f).max(self.release_seal_window)
            + self.deleg_request_timeout
            + w;
        fail(
            "D5",
            Class::Refuse,
            self.settle_cap_span >= need,
            format!(
                "settle cap span {} ms is below the reclaim horizon, the inherit slack, the seal \
                 window, the request timeout and a settle window ({need})",
                self.settle_cap_span
            ),
        );
        fail(
            "D6",
            Class::Refuse,
            self.abandon_heard_window >= d,
            format!(
                "abandon heard window {} ms is below the delegation TTL ({d})",
                self.abandon_heard_window
            ),
        );
        fail(
            "D7",
            Class::Refuse,
            self.recall_backoff_max >= self.deleg_request_timeout,
            format!(
                "recall back-off cap {} ms is below the request timeout ({})",
                self.recall_backoff_max, self.deleg_request_timeout
            ),
        );
        fail(
            "D8",
            Class::Warn,
            self.lock_grace_fits_lease(),
            format!("lock TTL {k} ms and two margins do not fit in the lease TTL ({l})"),
        );
        fail(
            "D9",
            Class::Refuse,
            b.backup_heartbeat <= t / 3 && b.backup_slow_max >= b.backup_ack_timeout,
            format!(
                "backup heartbeat {} ms must be at most a third of the takeover window ({t}) and \
                 the slow maximum {} ms at least the ack timeout ({})",
                b.backup_heartbeat, b.backup_slow_max, b.backup_ack_timeout
            ),
        );
        fail(
            "D10",
            Class::Warn,
            self.attempts > self.read_span,
            format!(
                "an op's attempts ({} ms) do not outlast the read span ({})",
                self.attempts, self.read_span
            ),
        );
        fail(
            "D11",
            Class::Warn,
            self.settle_cap_span >= self.read_span,
            format!(
                "settle cap span {} ms is below the read span ({})",
                self.settle_cap_span, self.read_span
            ),
        );
        fail(
            "D13",
            Class::Warn,
            b.s3_request_timeout == 0 || w >= 4 * b.s3_request_timeout,
            format!(
                "settle window {w} ms is below four store request timeouts ({})",
                b.s3_request_timeout
            ),
        );
        fail(
            "D14",
            Class::Warn,
            b.s3_request_timeout == 0 || self.inbox_read_fresh >= 4 * b.s3_request_timeout,
            format!(
                "inbox read freshness {} ms is below four store request timeouts ({}): an \
                 ordinary inbox poll could be stale",
                self.inbox_read_fresh, b.s3_request_timeout
            ),
        );
        v
    }

    /// A lock grace after a released lease ends `lock_ttl + 2 margins` after
    /// its taker first saw the release, and a TTL successor of a tenure that
    /// inherited it arms no floor of its own: the grace must fit inside one
    /// lease TTL for that successor to find every grant of the old tenure
    /// over. Else the successor may grant a lock the old tenure's holder
    /// still honours: two holders of one lock. The daemon cuts its lock TTL
    /// to fit ([`lock_ttl_fitting_lease`]).
    pub fn lock_grace_fits_lease(&self) -> bool {
        self.base.lock_ttl + 2 * self.base.margin <= self.base.lease_ttl
    }

    /// The settle's cap, from its start: `max(until, start + span)`.
    pub fn settle_cap(&self, start: u64, until: u64) -> u64 {
        until.max(start + self.settle_cap_span)
    }

    /// When a delegate's generation lapses: two margins past its grant's end
    /// (`until`, 0 for a grant never renewed here), else the reclaim horizon
    /// from its install.
    pub fn deleg_lapse_at(&self, until: i64, installed: i64) -> i64 {
        let renewed = if until > 0 {
            until + 2 * self.base.margin as i64
        } else {
            0
        };
        renewed.max(installed + self.reclaim_horizon as i64)
    }

    /// When a root alive behind the link has ended the generation: its lapse
    /// and the recall that fetches the tail (four round trips, at most one
    /// request timeout; a link not measured, the whole timeout).
    pub fn deleg_end_due(&self, lapse: i64, rtt: Option<u64>) -> i64 {
        let timeout = self.deleg_request_timeout;
        lapse + rtt.map_or(timeout, |r| r.saturating_mul(4).min(timeout)) as i64
    }

    /// When the root is due to have ended a generation, as the oracle's
    /// classifier counts it: the latest of the grant's end plus two margins
    /// and a one-way delay, the generation's birth plus the horizon, and the
    /// inheriting tenure's start plus the horizon (plus the takeover slack
    /// after a fast takeover), then the recall.
    pub fn root_reclaim_due(
        &self,
        renewed_until: Option<u64>,
        born: u64,
        inherited_tenure: Option<(u64, bool)>,
        one_way: u64,
        recall: u64,
    ) -> u64 {
        let by_grant = renewed_until.map_or(0, |end| end + 2 * self.base.margin + one_way);
        let inherited = inherited_tenure.map_or(0, |(start, fast)| {
            start + self.reclaim_horizon + if fast { self.inherit_slack } else { 0 }
        });
        inherited.max((born + self.reclaim_horizon).max(by_grant)) + recall
    }

    /// The recall resend's delay after `prev`: doubled, at most 2 s, at
    /// least the request timeout.
    pub fn next_recall_backoff(&self, prev: u64) -> u64 {
        (prev * 2)
            .min(RECALL_BACKOFF_CAP_MS)
            .max(self.deleg_request_timeout)
    }

    /// The release's recall bound, from the latest generation end already
    /// clamped to the reclaim horizon: two forward timeouts and a stream
    /// tick (at least 100 ms) later.
    pub fn release_recall_bound(&self, gen_until_clamped: u64) -> u64 {
        gen_until_clamped + 2 * self.base.forward_timeout + self.base.stream_tick.max(100)
    }

    /// A read grant's TTL under a cap with `cap_left` ms to run (the lease's
    /// or the delegation's honour end): `min(R, cap_left - m)`. Negative
    /// when the cap is within a margin of its end.
    pub fn read_grant_ttl(&self, cap_left: i64) -> i64 {
        (self.base.read_ttl as i64).min(cap_left - self.base.margin as i64)
    }

    /// A delegation grant is honoured for `ttl - m` from its send.
    pub fn grant_until_span(&self) -> u64 {
        self.base.deleg_ttl.saturating_sub(self.base.margin)
    }

    /// The renewal point after a grant's send: a quarter of the TTL while
    /// lock grants are out under it, else half.
    pub fn renew_after(&self, locks_out: bool) -> u64 {
        if locks_out {
            self.base.deleg_ttl / 4
        } else {
            self.base.deleg_ttl / 2
        }
    }

    /// The takeover ack floor: the takeover window, the longest grant the
    /// floor covers (a lock's when `locks`), the reclaim horizon when there
    /// are delegations, and two margins.
    pub fn ack_floor(&self, locks: bool, delegations: bool) -> u64 {
        let base = if locks {
            self.ack_floor_locks
        } else {
            self.ack_floor_reads
        };
        base + if delegations { self.reclaim_horizon } else { 0 }
    }

    /// How long past a holder's last word a watch counts silence when its
    /// beats came late by `late_max` ms lately: `T + min(2 * late, 3T)`.
    pub fn silence_window(&self, late_max: u64) -> u64 {
        let t = self.base.takeover.max(1);
        t + (2 * late_max).min(3 * t)
    }

    /// Every derived bound: name, value, formula in base-knob names.
    pub fn table(&self) -> Vec<(&'static str, u64, &'static str)> {
        vec![
            ("client_deadline", self.client_deadline, "2L"),
            (
                "settle_window",
                self.settle_window,
                "A/4, or the knob clamped to A/2",
            ),
            ("inbox_read_fresh", self.inbox_read_fresh, "w/2"),
            ("attempts", self.attempts, "A - w"),
            (
                "inbox_attempts",
                self.inbox_attempts,
                "max(inbox_deadline - w, inbox_deadline/2)",
            ),
            (
                "settle_cap_span",
                self.settle_cap_span,
                "2w + H + T + max(2F, max(T, F)) + DRT",
            ),
            (
                "drop_held_await",
                self.drop_held_await,
                "knob (0: cap) clamped to max(A - 2w, w)",
            ),
            (
                "stale_shadow_after",
                self.stale_shadow_after,
                "knob, else A",
            ),
            (
                "deleg_request_timeout",
                self.deleg_request_timeout,
                "max(F, 4k, 1)",
            ),
            ("reclaim_horizon", self.reclaim_horizon, "D + m"),
            ("grant_in_flight", self.grant_in_flight, "2k + 2 DRT"),
            (
                "deleg_resend",
                self.deleg_resend,
                "clamp(D/8, 100, 500) + 250",
            ),
            (
                "recall_backoff_max",
                self.recall_backoff_max,
                "max(2000, DRT)",
            ),
            ("release_seal_window", self.release_seal_window, "max(T, F)"),
            ("release_stream_window", self.release_stream_window, "F"),
            ("inherit_slack", self.inherit_slack, "T"),
            ("abandon_read_wait", self.abandon_read_wait, "R + m"),
            ("abandon_heard_window", self.abandon_heard_window, "D"),
            ("leave_wait", self.leave_wait, "2D + F + 5000"),
            ("leave_resend", self.leave_resend, "max(F, 500)"),
            (
                "leave_note_lapse",
                self.leave_note_lapse,
                "max(3 * leave_resend, 2D)",
            ),
            ("read_block", self.read_block, "2 (R + m)"),
            ("read_span", self.read_span, "R + m + 2000"),
            ("lock_span", self.lock_span, "max(K, D) * 5/4 + m + 1000"),
            (
                "floor_span",
                self.floor_span,
                "T + max(R, K) + D + 3m + 1000",
            ),
            ("promise_span", self.promise_span, "promise_ttl + 1000"),
            ("ack_floor_reads", self.ack_floor_reads, "T + R + 2m"),
            (
                "ack_floor_locks",
                self.ack_floor_locks,
                "T + max(R, K) + 2m",
            ),
            (
                "lock_grace_wait_max",
                self.lock_grace_wait_max,
                "T + max(R, K) + H + 2m + s3_less_deadline",
            ),
            (
                "lock_owner_wait_max",
                self.lock_owner_wait_max,
                "L + m + acquire_retry_max + lock_grace_wait_max",
            ),
            ("lock_poll", self.lock_poll, "max(forward_backoff/4, 10)"),
            ("lock_min_grant", self.lock_min_grant, "min(2m, K)"),
            (
                "lock_renew_gap",
                self.lock_renew_gap,
                "min((K - m)/64, 250)",
            ),
            ("claim_grace", self.claim_grace, "2T"),
            ("lag_threshold", self.lag_threshold, "max(T/8, 1)"),
            ("recall_hold", self.recall_hold, "max(F/2, 1)"),
            ("lane_tick", self.lane_tick, "clamp(F/4, 50, 250)"),
            (
                "backup_heartbeat",
                self.backup_heartbeat,
                "min(heartbeat, T/3), at least 1",
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The simulation's core configuration (`tests/sim/run.rs`
    /// `sim_core_config`), the fields a bound reads.
    fn sim() -> Config {
        let mut c = Config::defaults(1, 1);
        c.ttl_ms = 6_000;
        c.expiry_margin_ms = 500;
        c.forward_timeout_ms = 400;
        c.forward_backoff_ms = 150;
        c.acquire_retry_max_ms = 1_000;
        c.inbox_deadline_ms = 8_000;
        c.lock_ttl_ms = 1_500;
        c
    }

    /// `sim_core_config` with `delegated_long_delegation_config`'s lease and
    /// delegation TTLs: the client deadline is twice the lease TTL, the
    /// settle window a quarter of it.
    fn sim_long() -> Config {
        let mut c = sim();
        c.ttl_ms = 30_000;
        c.delegation_ttl_ms = 20_000;
        c
    }

    /// `backup_core_config`.
    fn backup() -> Config {
        let mut c = sim();
        c.backup_takeover_ms = 1_000;
        c.backup_ack_timeout_ms = 600;
        c.backup_heartbeat_ms = 200;
        c
    }

    /// The daemon's delegation TTL default (`max(K, 5 s)`) over the defaults.
    fn engine() -> Config {
        let mut c = Config::defaults(1, 1);
        c.delegation_ttl_ms = default_delegation_ttl(c.lock_ttl_ms);
        c
    }

    /// Zero margin, tiny TTLs, a forward timeout above the recall cap.
    fn edge() -> Config {
        let mut c = Config::defaults(1, 1);
        c.ttl_ms = 1_000;
        c.expiry_margin_ms = 0;
        c.read_delegation_ttl_ms = 100;
        c.delegation_ttl_ms = 50;
        c.lock_ttl_ms = 10;
        c.forward_timeout_ms = 3_000;
        c.backup_takeover_ms = 8;
        c.delegation_stream_tick_ms = 1;
        c.forward_backoff_ms = 5;
        c.acquire_retry_max_ms = 0;
        c.s3_less_deadline_ms = 0;
        c.inbox_deadline_ms = 100;
        c
    }

    /// Every derived value of `cfg` against hand-computed numbers, by name:
    /// none may be missing from either side.
    #[track_caller]
    fn pin(cfg: &Config, expected: &[(&str, u64)]) {
        let table = Deadlines::new(cfg).table();
        for (name, want) in expected {
            let got = table
                .iter()
                .find(|(n, _, _)| n == name)
                .unwrap_or_else(|| panic!("{name} is not in the table"))
                .1;
            assert_eq!(got, *want, "{name}");
        }
        assert_eq!(table.len(), expected.len(), "a bound is not pinned");
    }

    /// D13 and D14 (late-landing S1): the settle window and the inbox read
    /// freshness hold four store request timeouts, so that an ordinary poll
    /// is never stale. Both are skipped while the timeout is unknown.
    #[test]
    fn d13_and_d14_warn_when_the_store_is_slower_than_the_window() {
        let mut c = Config::defaults(1, 1);
        assert!(Deadlines::new(&c).check().is_empty());
        // w = 30 s, f = 15 s.
        c.s3_request_timeout_ms = 3_750;
        assert!(Deadlines::new(&c).check().is_empty(), "f = 4 timeouts");
        c.s3_request_timeout_ms = 3_751;
        let v = Deadlines::new(&c).check();
        assert_eq!(v.len(), 1, "{v:?}");
        assert_eq!((v[0].id, v[0].class), ("D14", Class::Warn));
        c.s3_request_timeout_ms = 7_501;
        let v = Deadlines::new(&c).check();
        assert_eq!(
            v.iter().map(|v| v.id).collect::<Vec<_>>(),
            vec!["D13", "D14"]
        );
    }

    /// The production defaults of `Config::defaults`: L 60 s, m 1 s, R 5 s,
    /// D 5 s, K 20 s, F 500 ms, T 1.5 s, k 50 ms, A 120 s.
    #[test]
    fn pinned_defaults() {
        pin(
            &Config::defaults(1, 1),
            &[
                ("client_deadline", 120_000),
                // A/4.
                ("settle_window", 30_000),
                ("inbox_read_fresh", 15_000),
                // 120000 - 30000.
                ("attempts", 90_000),
                // max(120000 - 30000, 60000).
                ("inbox_attempts", 90_000),
                // DRT = max(500, 4*50, 1) = 500; H = 5000 + 1000 = 6000;
                // seal = max(1500, 500) = 1500; 2*30000 + 6000 + 1500 (T)
                // + max(2*500, 1500) + 500.
                ("settle_cap_span", 69_500),
                ("deleg_request_timeout", 500),
                ("reclaim_horizon", 6_000),
                // cap = max(120000 - 60000, 30000).
                ("drop_held_await", 60_000),
                ("stale_shadow_after", 120_000),
                // 2*50 + 2*500.
                ("grant_in_flight", 1_100),
                // clamp(5000/8 = 625, 100, 500) + 250.
                ("deleg_resend", 750),
                ("recall_backoff_max", 2_000),
                // max(1500, 500).
                ("release_seal_window", 1_500),
                ("release_stream_window", 500),
                ("inherit_slack", 1_500),
                // 5000 + 1000.
                ("abandon_read_wait", 6_000),
                ("abandon_heard_window", 5_000),
                // 2*5000 + 500 + 5000.
                ("leave_wait", 15_500),
                ("leave_resend", 500),
                // max(3*500, 2*5000).
                ("leave_note_lapse", 10_000),
                // 2 * (5000 + 1000).
                ("read_block", 12_000),
                // 5000 + 1000 + 1000.
                ("read_span", 8_000),
                // ttl = max(20000, 5000); 20000 + 5000 + 1000 + 1000.
                ("lock_span", 27_000),
                // 1500 + max(5000, 20000) + 5000 + 3*1000 + 1000.
                ("floor_span", 30_500),
                ("promise_span", 16_000),
                // 1500 + 5000 + 2*1000.
                ("ack_floor_reads", 8_500),
                // 1500 + 20000 + 2*1000.
                ("ack_floor_locks", 23_500),
                // 1500 + 20000 + 6000 + 2000 + 20000.
                ("lock_grace_wait_max", 49_500),
                // 60000 + 1000 + 2000 + 49500.
                ("lock_owner_wait_max", 112_500),
                // max(200/4, 10).
                ("lock_poll", 50),
                // min(2*1000, 20000).
                ("lock_min_grant", 2_000),
                // min((20000 - 1000)/64 = 296, 250).
                ("lock_renew_gap", 250),
                ("claim_grace", 3_000),
                // 1500/8.
                ("lag_threshold", 187),
                ("recall_hold", 250),
                // clamp(500/4 = 125, 50, 250).
                ("lane_tick", 125),
                // min(300, 1500/3 = 500).
                ("backup_heartbeat", 300),
            ],
        );
    }

    /// The daemon's defaults: the delegation TTL is the lock TTL (20 s), so
    /// the horizon is 21 s; everything else as `pinned_defaults`.
    #[test]
    fn pinned_engine_defaults() {
        pin(
            &engine(),
            &[
                ("client_deadline", 120_000),
                ("settle_window", 30_000),
                ("inbox_read_fresh", 15_000),
                ("attempts", 90_000),
                ("inbox_attempts", 90_000),
                // H = 20000 + 1000; 60000 + 21000 + 1500 + max(1000, 1500) + 500.
                ("settle_cap_span", 84_500),
                ("deleg_request_timeout", 500),
                ("reclaim_horizon", 21_000),
                ("drop_held_await", 60_000),
                ("stale_shadow_after", 120_000),
                ("grant_in_flight", 1_100),
                // clamp(20000/8 = 2500, 100, 500) + 250.
                ("deleg_resend", 750),
                ("recall_backoff_max", 2_000),
                ("release_seal_window", 1_500),
                ("release_stream_window", 500),
                ("inherit_slack", 1_500),
                ("abandon_read_wait", 6_000),
                ("abandon_heard_window", 20_000),
                // 2*20000 + 500 + 5000.
                ("leave_wait", 45_500),
                ("leave_resend", 500),
                // max(1500, 40000).
                ("leave_note_lapse", 40_000),
                ("read_block", 12_000),
                ("read_span", 8_000),
                // ttl = max(20000, 20000).
                ("lock_span", 27_000),
                // 1500 + 20000 + 20000 + 3000 + 1000.
                ("floor_span", 45_500),
                ("promise_span", 16_000),
                ("ack_floor_reads", 8_500),
                ("ack_floor_locks", 23_500),
                // 1500 + 20000 + 21000 + 2000 + 20000.
                ("lock_grace_wait_max", 64_500),
                // 60000 + 1000 + 2000 + 64500.
                ("lock_owner_wait_max", 127_500),
                ("lock_poll", 50),
                ("lock_min_grant", 2_000),
                ("lock_renew_gap", 250),
                ("claim_grace", 3_000),
                ("lag_threshold", 187),
                ("recall_hold", 250),
                ("lane_tick", 125),
                ("backup_heartbeat", 300),
            ],
        );
    }

    /// The simulation: L 6 s, m 500 ms, R 5 s, D 5 s, K 1.5 s, F 400 ms,
    /// T 1.5 s, A 12 s, inbox deadline 8 s.
    #[test]
    fn pinned_sim() {
        pin(
            &sim(),
            &[
                ("client_deadline", 12_000),
                // 12000/4.
                ("settle_window", 3_000),
                ("inbox_read_fresh", 1_500),
                ("attempts", 9_000),
                // max(8000 - 3000, 4000).
                ("inbox_attempts", 5_000),
                // DRT = max(400, 200, 1) = 400; H = 5500; seal = 1500;
                // 6000 + 5500 + 1500 (T) + max(800, 1500) + 400.
                ("settle_cap_span", 14_900),
                ("deleg_request_timeout", 400),
                ("reclaim_horizon", 5_500),
                // max(12000 - 6000, 3000).
                ("drop_held_await", 6_000),
                ("stale_shadow_after", 12_000),
                // 100 + 800.
                ("grant_in_flight", 900),
                ("deleg_resend", 750),
                ("recall_backoff_max", 2_000),
                ("release_seal_window", 1_500),
                ("release_stream_window", 400),
                ("inherit_slack", 1_500),
                ("abandon_read_wait", 5_500),
                ("abandon_heard_window", 5_000),
                // 10000 + 400 + 5000.
                ("leave_wait", 15_400),
                // max(400, 500).
                ("leave_resend", 500),
                ("leave_note_lapse", 10_000),
                // 2 * 5500.
                ("read_block", 11_000),
                // 5000 + 500 + 1000.
                ("read_span", 7_500),
                // ttl = max(1500, 5000); 5000 + 1250 + 500 + 1000.
                ("lock_span", 7_750),
                // 1500 + 5000 + 5000 + 1500 + 1000.
                ("floor_span", 14_000),
                ("promise_span", 16_000),
                // 1500 + 5000 + 1000.
                ("ack_floor_reads", 7_500),
                // max(5000, 1500) = 5000: the same.
                ("ack_floor_locks", 7_500),
                // 1500 + 5000 + 5500 + 1000 + 20000.
                ("lock_grace_wait_max", 33_000),
                // 6000 + 500 + 1000 + 33000.
                ("lock_owner_wait_max", 40_500),
                // 150/4 = 37.
                ("lock_poll", 37),
                // min(1000, 1500).
                ("lock_min_grant", 1_000),
                // (1500 - 500)/64 = 15.
                ("lock_renew_gap", 15),
                ("claim_grace", 3_000),
                ("lag_threshold", 187),
                // 400/2.
                ("recall_hold", 200),
                // 400/4 = 100.
                ("lane_tick", 100),
                ("backup_heartbeat", 300),
            ],
        );
    }

    /// The simulation with a raised lease TTL (30 s) and delegation TTL
    /// (20 s): the client deadline follows the lease TTL (production's
    /// ratio), so the settle window is a quarter of it.
    #[test]
    fn pinned_sim_raised_lease_ttl() {
        pin(
            &sim_long(),
            &[
                // 2 * 30000.
                ("client_deadline", 60_000),
                // 60000/4.
                ("settle_window", 15_000),
                ("inbox_read_fresh", 7_500),
                // 60000 - 15000.
                ("attempts", 45_000),
                // max(8000 - 15000 floored at 0, 4000).
                ("inbox_attempts", 4_000),
                // H = 20500; 2 * 15000 + 20500 + 1500 (T) + max(800, 1500) + 400.
                ("settle_cap_span", 53_900),
                ("deleg_request_timeout", 400),
                ("reclaim_horizon", 20_500),
                // max(60000 - 30000, 15000).
                ("drop_held_await", 30_000),
                ("stale_shadow_after", 60_000),
                ("grant_in_flight", 900),
                // clamp(20000/8 = 2500, 100, 500) + 250.
                ("deleg_resend", 750),
                ("recall_backoff_max", 2_000),
                ("release_seal_window", 1_500),
                ("release_stream_window", 400),
                ("inherit_slack", 1_500),
                ("abandon_read_wait", 5_500),
                ("abandon_heard_window", 20_000),
                // 40000 + 400 + 5000.
                ("leave_wait", 45_400),
                ("leave_resend", 500),
                ("leave_note_lapse", 40_000),
                ("read_block", 11_000),
                ("read_span", 7_500),
                // ttl = 20000; 20000 + 5000 + 500 + 1000.
                ("lock_span", 26_500),
                // 1500 + 5000 + 20000 + 1500 + 1000.
                ("floor_span", 29_000),
                ("promise_span", 16_000),
                ("ack_floor_reads", 7_500),
                ("ack_floor_locks", 7_500),
                // 1500 + 5000 + 20500 + 1000 + 20000.
                ("lock_grace_wait_max", 48_000),
                // 30000 + 500 + 1000 + 48000.
                ("lock_owner_wait_max", 79_500),
                ("lock_poll", 37),
                ("lock_min_grant", 1_000),
                ("lock_renew_gap", 15),
                ("claim_grace", 3_000),
                ("lag_threshold", 187),
                ("recall_hold", 200),
                ("lane_tick", 100),
                ("backup_heartbeat", 300),
            ],
        );
    }

    /// `backup_core_config`: the simulation with T 1 s, ack timeout 600 ms,
    /// heartbeat 200 ms. Only the bounds with a T term differ from
    /// `pinned_sim`.
    #[test]
    fn pinned_backup() {
        pin(
            &backup(),
            &[
                ("client_deadline", 12_000),
                ("settle_window", 3_000),
                ("inbox_read_fresh", 1_500),
                ("attempts", 9_000),
                ("inbox_attempts", 5_000),
                // T = 1000, seal = max(1000, 400) = 1000, DRT 400:
                // 6000 + 5500 + 1000 + max(800, 1000) + 400.
                ("settle_cap_span", 13_900),
                ("deleg_request_timeout", 400),
                ("reclaim_horizon", 5_500),
                ("drop_held_await", 6_000),
                ("stale_shadow_after", 12_000),
                ("grant_in_flight", 900),
                ("deleg_resend", 750),
                ("recall_backoff_max", 2_000),
                // max(1000, 400).
                ("release_seal_window", 1_000),
                ("release_stream_window", 400),
                ("inherit_slack", 1_000),
                ("abandon_read_wait", 5_500),
                ("abandon_heard_window", 5_000),
                ("leave_wait", 15_400),
                ("leave_resend", 500),
                ("leave_note_lapse", 10_000),
                ("read_block", 11_000),
                ("read_span", 7_500),
                ("lock_span", 7_750),
                // 1000 + 5000 + 5000 + 1500 + 1000.
                ("floor_span", 13_500),
                ("promise_span", 16_000),
                // 1000 + 5000 + 1000.
                ("ack_floor_reads", 7_000),
                ("ack_floor_locks", 7_000),
                // 1000 + 5000 + 5500 + 1000 + 20000.
                ("lock_grace_wait_max", 32_500),
                // 6000 + 500 + 1000 + 32500.
                ("lock_owner_wait_max", 40_000),
                ("lock_poll", 37),
                ("lock_min_grant", 1_000),
                ("lock_renew_gap", 15),
                ("claim_grace", 2_000),
                // 1000/8.
                ("lag_threshold", 125),
                ("recall_hold", 200),
                ("lane_tick", 100),
                // min(200, 1000/3 = 333).
                ("backup_heartbeat", 200),
            ],
        );
    }

    /// Zero margin, tiny TTLs and F above the recall cap: L 1 s, m 0,
    /// R 100, D 50, K 10, F 3 s, T 8, k 1, A 2 s, inbox deadline 100.
    #[test]
    fn pinned_edge() {
        pin(
            &edge(),
            &[
                ("client_deadline", 2_000),
                // 2000/4.
                ("settle_window", 500),
                ("inbox_read_fresh", 250),
                ("attempts", 1_500),
                // max(100 - 500 -> 0, 50).
                ("inbox_attempts", 50),
                // DRT = max(3000, 4, 1) = 3000; H = 50; T = 8; seal = 3000;
                // 1000 + 50 + 8 + max(6000, 3000) + 3000.
                ("settle_cap_span", 10_058),
                ("deleg_request_timeout", 3_000),
                ("reclaim_horizon", 50),
                // max(2000 - 1000, 500).
                ("drop_held_await", 1_000),
                ("stale_shadow_after", 2_000),
                // 2 + 6000.
                ("grant_in_flight", 6_002),
                // clamp(50/8 = 6, 100, 500) + 250.
                ("deleg_resend", 350),
                // max(2000, 3000).
                ("recall_backoff_max", 3_000),
                // max(8, 3000).
                ("release_seal_window", 3_000),
                ("release_stream_window", 3_000),
                ("inherit_slack", 8),
                ("abandon_read_wait", 100),
                ("abandon_heard_window", 50),
                // 100 + 3000 + 5000.
                ("leave_wait", 8_100),
                ("leave_resend", 3_000),
                // max(9000, 100).
                ("leave_note_lapse", 9_000),
                ("read_block", 200),
                ("read_span", 2_100),
                // ttl = max(10, 50); 50 + 12 + 0 + 1000.
                ("lock_span", 1_062),
                // 8 + 100 + 50 + 0 + 1000.
                ("floor_span", 1_158),
                ("promise_span", 16_000),
                ("ack_floor_reads", 108),
                // max(100, 10) = 100.
                ("ack_floor_locks", 108),
                // 8 + 100 + 50 + 0 + 0.
                ("lock_grace_wait_max", 158),
                // 1000 + 0 + 0 + 158.
                ("lock_owner_wait_max", 1_158),
                // max(5/4 = 1, 10).
                ("lock_poll", 10),
                // min(0, 10).
                ("lock_min_grant", 0),
                // (10 - 0)/64.
                ("lock_renew_gap", 0),
                ("claim_grace", 16),
                // max(8/8, 1).
                ("lag_threshold", 1),
                ("recall_hold", 1_500),
                // clamp(750, 50, 250).
                ("lane_tick", 250),
                // min(300, 8/3 = 2).
                ("backup_heartbeat", 2),
            ],
        );
    }

    /// The settle window knob and the drop-held knob: the knob is clamped to
    /// half the deadline, a drop-held knob to its cap.
    #[test]
    fn pinned_knobs() {
        let mut c = Config::defaults(1, 1);
        c.settle_window_ms = 10_000;
        c.drop_held_await_ms = 5_000;
        let d = Deadlines::new(&c);
        assert_eq!(d.settle_window, 10_000);
        // max(120000 - 20000, 10000) = 100000; the knob is below it.
        assert_eq!(d.drop_held_await, 5_000);
        assert_eq!(d.stale_shadow_after, 5_000);
        c.settle_window_ms = 100_000;
        c.drop_held_await_ms = 200_000;
        let d = Deadlines::new(&c);
        // min(100000, 120000/2).
        assert_eq!(d.settle_window, 60_000);
        // max(120000 - 120000, 60000) = 60000; the knob is above it.
        assert_eq!(d.drop_held_await, 60_000);
        assert_eq!(d.stale_shadow_after, 200_000);
    }

    /// The runtime-parameterised bounds, at the defaults (H 6000, m 1000,
    /// DRT 500, F 500, k 50, R 5000, D 5000, T 1500) and at the edge.
    #[test]
    fn pinned_runtime_functions() {
        let d = Deadlines::new(&Config::defaults(1, 1));
        // max(until, start + 69500).
        assert_eq!(d.settle_cap(1_000, 500), 70_500);
        assert_eq!(d.settle_cap(1_000, 100_000), 100_000);
        // renewed 0: installed + H.
        assert_eq!(d.deleg_lapse_at(0, 10_000), 16_000);
        // 30000 + 2*1000 beats 0 + 6000.
        assert_eq!(d.deleg_lapse_at(30_000, 0), 32_000);
        assert_eq!(d.deleg_end_due(16_000, None), 16_500);
        // 4 * 100 = 400 < 500.
        assert_eq!(d.deleg_end_due(16_000, Some(100)), 16_400);
        // 4 * 300 = 1200, capped at 500.
        assert_eq!(d.deleg_end_due(16_000, Some(300)), 16_500);
        // 1000 doubled; 1500 doubled is capped at 2000; the cap holds.
        assert_eq!(d.next_recall_backoff(500), 1_000);
        assert_eq!(d.next_recall_backoff(1_500), 2_000);
        assert_eq!(d.next_recall_backoff(2_000), 2_000);
        // 10000 + 2*500 + max(50, 100).
        assert_eq!(d.release_recall_bound(10_000), 11_100);
        // min(5000, 10000 - 1000), min(5000, 3000 - 1000), 500 - 1000 - 5000.
        assert_eq!(d.read_grant_ttl(10_000), 5_000);
        assert_eq!(d.read_grant_ttl(3_000), 2_000);
        assert_eq!(d.read_grant_ttl(500), -500);
        // 5000 - 1000; 5000/4, 5000/2.
        assert_eq!(d.grant_until_span(), 4_000);
        assert_eq!(d.renew_after(true), 1_250);
        assert_eq!(d.renew_after(false), 2_500);
        // 8500 (reads), 23500 (locks); plus H 6000 with delegations.
        assert_eq!(d.ack_floor(false, false), 8_500);
        assert_eq!(d.ack_floor(false, true), 14_500);
        assert_eq!(d.ack_floor(true, false), 23_500);
        assert_eq!(d.ack_floor(true, true), 29_500);
        // 1500 + min(2*late, 4500).
        assert_eq!(d.silence_window(0), 1_500);
        assert_eq!(d.silence_window(400), 2_300);
        assert_eq!(d.silence_window(2_000), 5_500);
        assert_eq!(d.silence_window(5_000), 6_000);
        // by grant 20000 + 2000 + 20 = 22020; inherited (fast) 30000 + 6000
        // + 1500 = 37500; birth 5000 + 6000 = 11000; plus the recall 100.
        assert_eq!(
            d.root_reclaim_due(Some(20_000), 5_000, Some((30_000, true)), 20, 100),
            37_600
        );
        // not fast: 30000 + 6000 = 36000.
        assert_eq!(
            d.root_reclaim_due(Some(20_000), 5_000, Some((30_000, false)), 20, 100),
            36_100
        );
        // no tenure, never renewed: 5000 + 6000.
        assert_eq!(d.root_reclaim_due(None, 5_000, None, 20, 100), 11_100);
        // by grant beats birth: 20000 + 2000 + 20.
        assert_eq!(
            d.root_reclaim_due(Some(20_000), 1_000, None, 20, 100),
            22_120
        );
        let e = Deadlines::new(&edge());
        // 3000 doubled is 6000, capped at 2000, lifted to DRT 3000.
        assert_eq!(e.next_recall_backoff(3_000), 3_000);
        // m 0: the TTL is the cap left.
        assert_eq!(e.read_grant_ttl(60), 60);
        assert_eq!(e.read_grant_ttl(500), 100);
        assert_eq!(e.grant_until_span(), 50);
        // 8 + min(2*10, 24).
        assert_eq!(e.silence_window(10), 28);
        // 0 + 2*3000 + max(1, 100).
        assert_eq!(e.release_recall_bound(0), 6_100);
        assert_eq!(e.deleg_lapse_at(0, 0), 50);
    }

    /// One failing configuration per invariant (D5, which holds by
    /// construction, on a hand-made cap).
    #[test]
    fn each_invariant_has_a_failing_configuration() {
        let ids = |d: &Deadlines| d.check().into_iter().map(|v| v.id).collect::<Vec<_>>();
        let has = |d: &Deadlines, id: &str| ids(d).contains(&id);
        let mut c = Config::defaults(1, 1);
        c.expiry_margin_ms = 2_500;
        assert!(has(&Deadlines::new(&c), "D1"));
        let mut c = Config::defaults(1, 1);
        c.forward_timeout_ms = 40_000;
        assert!(has(&Deadlines::new(&c), "D2"));
        let mut c = Config::defaults(1, 1);
        c.settle_window_ms = 60_000;
        let d = Deadlines::new(&c);
        assert!(has(&d, "D3"));
        assert!(!has(&d, "D2"));
        let mut c = Config::defaults(1, 1);
        c.ttl_ms = 2_000;
        assert!(has(&Deadlines::new(&c), "D4"));
        assert!(!has(&Deadlines::new(&sim_long()), "D4"));
        // (The span covers the takeover window by construction: a cap
        // shorter than the waits it caps is a hand-made one.)
        let mut c = Config::defaults(1, 1);
        c.backup_takeover_ms = 40_000;
        assert!(!has(&Deadlines::new(&c), "D5"));
        let mut d = Deadlines::new(&Config::defaults(1, 1));
        d.settle_cap_span = d.reclaim_horizon;
        assert!(has(&d, "D5"));
        let mut d = Deadlines::new(&Config::defaults(1, 1));
        d.abandon_heard_window = 4_999;
        assert!(has(&d, "D6"));
        let mut d = Deadlines::new(&Config::defaults(1, 1));
        d.recall_backoff_max = 499;
        assert!(has(&d, "D7"));
        let mut c = Config::defaults(1, 1);
        c.ttl_ms = 10_000;
        assert!(has(&Deadlines::new(&c), "D8"));
        // The daemon's cut makes it hold, whatever the lease.
        for lease in [200, 1_000, 10_000, 20_000, 22_000, 60_000] {
            let mut c = Config::defaults(1, 1);
            c.ttl_ms = lease;
            c.expiry_margin_ms = 1_000.min(lease / 4);
            c.lock_ttl_ms = lock_ttl_fitting_lease(20_000, c.ttl_ms, c.expiry_margin_ms);
            assert!(!has(&Deadlines::new(&c), "D8"), "lease {lease}");
        }
        assert_eq!(lock_ttl_fitting_lease(20_000, 60_000, 1_000), 20_000);
        assert_eq!(lock_ttl_fitting_lease(20_000, 20_000, 1_000), 18_000);
        assert_eq!(lock_ttl_fitting_lease(3_000, 20_000, 1_000), 3_000);
        assert_eq!(lock_ttl_fitting_lease(20_000, 200, 50), 100);
        let mut c = Config::defaults(1, 1);
        c.backup_heartbeat_ms = 600;
        assert!(has(&Deadlines::new(&c), "D9"));
        let mut c = Config::defaults(1, 1);
        c.backup_slow_max_ms = 500;
        assert!(has(&Deadlines::new(&c), "D9"));
        let mut c = Config::defaults(1, 1);
        c.ttl_ms = 4_000;
        assert!(has(&Deadlines::new(&c), "D10"));
        let mut c = Config::defaults(1, 1);
        c.read_delegation_ttl_ms = 100_000;
        assert!(has(&Deadlines::new(&c), "D11"));
        // The classes: D1 refuses, D3 warns.
        let mut c = Config::defaults(1, 1);
        c.expiry_margin_ms = 2_500;
        let v = Deadlines::new(&c).check();
        assert_eq!(
            v.iter().find(|v| v.id == "D1").unwrap().class,
            Class::Refuse
        );
        let mut c = Config::defaults(1, 1);
        c.ttl_ms = 2_000;
        assert_eq!(
            Deadlines::new(&c)
                .check()
                .iter()
                .find(|v| v.id == "D4")
                .unwrap()
                .class,
            Class::Warn
        );
    }
}
