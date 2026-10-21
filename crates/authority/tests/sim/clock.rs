//! The simulation's clocks: tokio's paused `Instant`, offset from a fixed
//! unix-ms base so lease expiry (which the core computes in unix ms) is
//! driven entirely by `tokio::time::advance`/auto-advance.
//!
//! Plan 43 C7-H1: each node reads a [`ProtocolClock`] (WALL, HONOUR, WAIT)
//! over [`SimSources`], as the daemon reads the host's. The wall source is
//! the base plus the node's constant offset (plan 30 §M8's skew) plus its
//! runtime *step* (`FaultKind::ClockStep`). The step is per node and survives
//! a restart (a host's clock does not reset when the process does); the
//! `ProtocolClock` does not: `NodeEnv::clock_of` builds a fresh one per
//! incarnation, whose HONOUR and WAIT start at the wall of that moment (P6)
//! and whose id is `node << 32 | incarnation`.
//!
//! Plan 43 C7a (commit 1): the three elapsed sources follow the node's
//! *oscillator* ([`NodeClockState`]), which is true time except where a fault
//! says otherwise:
//!
//! - **rate** (`FaultKind::RateSkew`, `SimConfig::node_rates`): the
//!   oscillator runs at `1 + ppm/1e6` of true time, from the fault on. The
//!   wall does not follow it (an NTP-disciplined host with a bad crystal)
//!   unless `SimConfig::wall_follows_rate`;
//! - **suspend** (`FaultKind::Suspend`): for an interval, `raw` and `mono`
//!   stop while `boot` and the wall run on (so HONOUR counts it through `S`
//!   and WAIT does not);
//! - **reboot** (`FaultKind::RebootNode`): `boot`, `raw` and `mono` restart
//!   from 0, and the boot id changes.
//!
//! Without a fault the sources are exactly the paused clock's whole
//! milliseconds, as they were before C7a: the sweeps of the no-fault
//! configurations do not move.
//!
//! `env.clock` (true time) has no offset, no step and no rate; the checkers
//! keep using it.

use constellation_authority::{HonourMs, Ms, Now, WaitMs};
use constellation_types::clock::{ClockSources, ProtocolClock, RawReading};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

const PPM: i128 = 1_000_000;
const MS_NS: i128 = 1_000_000;

/// One node's oscillator, as a function of true time (ns since the run's
/// base). Piecewise linear: the rate changes (`RateSkew`) take effect from
/// the instant of the fault; suspends are intervals (known at the fault, so
/// the inverse can see a timer across one); reboots restart the elapsed
/// sources.
struct Osc {
    /// `(from_true_ns, ppm)`, ascending; the first is `(0, initial)`.
    rates: Vec<(i128, i64)>,
    /// `[from, to)` in true ns, ascending, disjoint: raw and mono stand
    /// still, boot and the wall do not.
    suspends: Vec<(i128, i128)>,
    /// True ns at which each boot began (the first is 0).
    boots: Vec<i128>,
}

impl Osc {
    /// The oscillator's own ns at true time `t`: `∫ (1 + ppm/1e6) dt`.
    fn osc(&self, t: i128) -> i128 {
        let mut total = 0;
        for (i, &(from, ppm)) in self.rates.iter().enumerate() {
            if t <= from {
                break;
            }
            let to = self.rates.get(i + 1).map_or(t, |next| next.0.min(t));
            total += (to - from) * (PPM + ppm as i128) / PPM;
        }
        total
    }

    fn boot_start(&self, t: i128) -> i128 {
        self.boots.iter().copied().rfind(|b| *b <= t).unwrap_or(0)
    }

    /// `CLOCK_BOOTTIME`: since this boot, suspend included. Whole ms.
    fn boot_ns(&self, t: i128) -> i128 {
        let bs = self.boot_start(t);
        (self.osc(t) - self.osc(bs)) / MS_NS * MS_NS
    }

    /// `CLOCK_MONOTONIC_RAW` (and `CLOCK_MONOTONIC`): since this boot,
    /// without the time suspended. Whole ms.
    fn raw_ns(&self, t: i128) -> i128 {
        let bs = self.boot_start(t);
        let mut suspended = 0;
        for &(s, e) in &self.suspends {
            let (lo, hi) = (s.max(bs), e.min(t));
            if hi > lo {
                suspended += self.osc(hi) - self.osc(lo);
            }
        }
        (self.osc(t) - self.osc(bs) - suspended) / MS_NS * MS_NS
    }

    /// Nothing but true time: the readings are `t` itself (the fast path of
    /// the inverse, and the shape every no-fault run has).
    fn plain(&self) -> bool {
        self.rates.len() == 1
            && self.rates[0].1 == 0
            && self.suspends.is_empty()
            && self.boots.len() == 1
    }

    /// The earliest true time at which the raw clock reads `target` ns or
    /// more, looking from true time `now` (the future is the present rate
    /// and the suspends already announced). `target` is a whole number of ms.
    fn true_of_raw(&self, target: i128, now: i128) -> i128 {
        if self.plain() {
            return target;
        }
        let lo_bound = self.boot_start(now);
        if self.raw_ns(lo_bound) >= target {
            return lo_bound;
        }
        // Grow `hi` until the clock has reached the target.
        let mut hi = now.max(lo_bound);
        let mut grow = 1_000_000_000i128;
        while self.raw_ns(hi) < target {
            hi += grow;
            grow *= 2;
            if grow > 1 << 62 {
                return hi;
            }
        }
        // raw(lo) < target <= raw(hi): the first true ns it reads target.
        let mut lo = lo_bound;
        while hi - lo > 1 {
            let mid = lo + (hi - lo) / 2;
            if self.raw_ns(mid) >= target {
                hi = mid;
            } else {
                lo = mid;
            }
        }
        hi
    }
}

/// A node's host clock state: what survives the process (the wall step, the
/// oscillator, the boot). One per node for the run; every incarnation's
/// [`SimSources`] reads it.
pub struct NodeClockState {
    node: u64,
    /// The node's cumulative wall step (ms; moves WALL only).
    step: AtomicI64,
    osc: Mutex<Osc>,
    /// `SimConfig::wall_follows_rate`: an undisciplined host, whose wall
    /// follows the oscillator.
    wall_follows_rate: bool,
}

impl NodeClockState {
    /// Node `node`'s state: a constant `ppm` from the start of the run.
    pub fn new(node: u64, ppm: i64, wall_follows_rate: bool) -> Arc<Self> {
        Arc::new(Self {
            node,
            step: AtomicI64::new(0),
            osc: Mutex::new(Osc {
                rates: vec![(0, ppm)],
                suspends: Vec::new(),
                boots: vec![0],
            }),
            wall_follows_rate,
        })
    }

    /// The wall step (cumulative ms).
    pub fn step(&self) -> &AtomicI64 {
        &self.step
    }

    /// From true time `at` (ns) the oscillator runs at `ppm`.
    pub fn set_rate(&self, ppm: i64, at: i128) {
        let mut o = self.osc.lock().unwrap();
        while o.rates.len() > 1 && o.rates.last().is_some_and(|(from, _)| *from >= at) {
            o.rates.pop();
        }
        if at <= 0 {
            o.rates[0] = (0, ppm);
        } else {
            o.rates.push((at, ppm));
        }
    }

    /// From true time `from` (ns) for `for_ms` the host is suspended.
    pub fn suspend(&self, from: i128, for_ms: u64) {
        self.osc
            .lock()
            .unwrap()
            .suspends
            .push((from, from + for_ms as i128 * MS_NS));
    }

    /// A new boot at true time `at` (ns): the elapsed sources restart.
    pub fn reboot(&self, at: i128) {
        self.osc.lock().unwrap().boots.push(at);
    }

    /// How many boots this node has had before the current one.
    pub fn boot_no(&self) -> u64 {
        self.osc.lock().unwrap().boots.len() as u64 - 1
    }

    /// The rate in force at true time `at` (ns), ppm.
    pub fn ppm_at(&self, at: i128) -> i64 {
        let o = self.osc.lock().unwrap();
        o.rates
            .iter()
            .rfind(|(from, _)| *from <= at)
            .map_or(0, |(_, p)| *p)
    }
}

/// The simulated host's clocks for one node.
struct SimSources {
    base: tokio::time::Instant,
    base_ms: i64,
    /// Plan 30 §M8: this node's clock error (ms, constant).
    offset_ms: i64,
    state: Arc<NodeClockState>,
    /// The boot this incarnation started on (a process does not outlive
    /// its boot).
    boot_no: u64,
}

impl SimSources {
    fn true_ns(&self) -> i128 {
        self.base.elapsed().as_nanos() as i128
    }

    fn wall_at(&self, osc: &Osc, t: i128) -> i64 {
        let elapsed_ms = if self.state.wall_follows_rate {
            osc.osc(t) / MS_NS
        } else {
            t / MS_NS
        };
        self.base_ms + self.offset_ms + self.state.step.load(Ordering::SeqCst) + elapsed_ms as i64
    }
}

impl ClockSources for SimSources {
    fn wall_ms(&self) -> i64 {
        let t = self.true_ns();
        self.wall_at(&self.state.osc.lock().unwrap(), t)
    }

    fn boot_ns(&self) -> u64 {
        self.state.osc.lock().unwrap().boot_ns(self.true_ns()) as u64
    }

    fn raw_ns(&self) -> u64 {
        self.state.osc.lock().unwrap().raw_ns(self.true_ns()) as u64
    }

    fn mono_ns(&self) -> u64 {
        self.raw_ns()
    }

    fn boot_id(&self) -> u64 {
        ((self.state.node << 32) | self.boot_no) | (1 << 63)
    }

    /// One sample at one instant of true time (one lock).
    fn read(&self) -> RawReading {
        let t = self.true_ns();
        let osc = self.state.osc.lock().unwrap();
        let raw_ns = osc.raw_ns(t) as u64;
        RawReading {
            wall_ms: self.wall_at(&osc, t),
            boot_ns: osc.boot_ns(t) as u64,
            raw_ns,
            mono_ns: raw_ns,
        }
    }
}

#[derive(Clone)]
pub struct Clock {
    base: tokio::time::Instant,
    base_ms: i64,
    pc: Arc<ProtocolClock>,
    state: Arc<NodeClockState>,
    boot_id: u64,
    /// The WAIT reading and the raw clock (ns) at this clock's creation:
    /// the map from a WAIT value back to the runtime's `Instant` it is read
    /// at.
    created: (i64, i128),
    /// The clock's rate budget (`SimConfig::rate_budget_ppm`): WAIT runs
    /// slow by it, which `instant_at_wait` inverts.
    budget_ppm: u32,
}

impl Clock {
    /// True time: no offset, no step, no rate.
    pub fn start() -> Self {
        let base = tokio::time::Instant::now();
        // A fixed, plausible epoch so log lines read like production.
        let base_ms = 1_780_000_000_000;
        Self::over(base, base_ms, 0, NodeClockState::new(0, 0, false), 1, 0)
    }

    fn over(
        base: tokio::time::Instant,
        base_ms: i64,
        offset_ms: i64,
        state: Arc<NodeClockState>,
        id: u64,
        budget_ppm: u32,
    ) -> Self {
        let sources = SimSources {
            base,
            base_ms,
            offset_ms,
            state: state.clone(),
            boot_no: state.boot_no(),
        };
        let raw0 = sources.raw_ns() as i128;
        let boot_id = sources.boot_id();
        let pc = Arc::new(ProtocolClock::with_id(Arc::new(sources), id, budget_ppm));
        let created = (pc.wait().0, raw0);
        Self {
            base,
            base_ms,
            pc,
            state,
            boot_id,
            created,
            budget_ppm,
        }
    }

    /// A node incarnation's clock over true time: `offset_ms` off (a node
    /// whose clock disagrees), stepped, skewed in rate, suspended and
    /// rebooted as `state` says (shared with the node's next incarnation),
    /// named `id`, budgeted `budget_ppm` (`SimConfig::rate_budget_ppm`). A
    /// new `ProtocolClock`: its HONOUR and WAIT start at the node's wall of
    /// this moment.
    pub fn for_node(
        &self,
        offset_ms: i64,
        state: Arc<NodeClockState>,
        id: u64,
        budget_ppm: u32,
    ) -> Self {
        Self::over(self.base, self.base_ms, offset_ms, state, id, budget_ppm)
    }

    /// The three readings of this instant.
    pub fn now(&self) -> Now {
        self.pc.now()
    }

    pub fn wall(&self) -> Ms {
        self.pc.wall()
    }

    /// (Read by the honour sites of plan 43 C7-H1's later commits.)
    #[allow(dead_code)]
    pub fn honour(&self) -> HonourMs {
        self.pc.honour()
    }

    pub fn wait(&self) -> WaitMs {
        self.pc.wait()
    }

    /// The `ProtocolClock::id` the node's stamps carry.
    pub fn id(&self) -> u64 {
        self.pc.id()
    }

    /// The host's boot identity ([`ClockSources::boot_id`]).
    #[allow(dead_code)]
    pub fn boot_id(&self) -> u64 {
        self.boot_id
    }

    /// The steps this clock has seen (observability).
    pub fn step_stats(&self) -> constellation_types::clock::StepStats {
        self.pc.step_stats()
    }

    /// The earliest runtime `Instant` at which this clock's WAIT reads
    /// `wait` or more: relative timers count to it (`SetTimer { after_ms }`
    /// from the step's reading). It inverts the node's oscillator (its rate
    /// and the suspends already announced) and, from plan 43 C7a's commit 2,
    /// the budget's scaling of WAIT (`1/(1 + rho)` of the raw clock's
    /// elapsed time: the raw clock must have run `d (1 + rho)` ms, and one
    /// ms more for the fixed point's rounding), so a timer armed across a
    /// suspend ends after the node resumes, and one on a fast oscillator
    /// ends when its WAIT says. A budget of 0 is the raw clock itself.
    pub fn instant_at_wait(&self, wait: WaitMs) -> tokio::time::Instant {
        let (wait0, raw0) = self.created;
        let d = wait.0 as i128 - wait0 as i128;
        let raw_ms = if self.budget_ppm == 0 || d <= 0 {
            d
        } else {
            (d * (PPM + self.budget_ppm as i128) + PPM - 1) / PPM + 1
        };
        let target = raw0 + raw_ms * MS_NS;
        let now = self.base.elapsed().as_nanos() as i128;
        let t = self.state.osc.lock().unwrap().true_of_raw(target, now);
        self.base + std::time::Duration::from_nanos(t.max(0) as u64)
    }

    /// True elapsed time.
    pub fn elapsed_ms(&self) -> u64 {
        self.base.elapsed().as_millis() as u64
    }

    /// True elapsed time, ns.
    pub fn elapsed_ns(&self) -> i128 {
        self.base.elapsed().as_nanos() as i128
    }
}
