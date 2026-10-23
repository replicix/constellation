//! The protocol clocks (plan 43 C7-H1): three readings per node and a
//! type for each, so that a stamp and the comparison that judges it are
//! always on one clock.
//!
//! Pure logic over injected sources and std only. The host's sources, the
//! fault hooks and the process-wide instance live in
//! `constellation_platform::clock`; the simulator builds one
//! [`ProtocolClock`] per node incarnation over its own sources.
//!
//! # The three readings
//!
//! A [`ProtocolClock`] captures, at creation, `wall0`, the boot clock
//! (`CLOCK_BOOTTIME`, which counts suspend), the raw clock
//! (`CLOCK_MONOTONIC_RAW`, which is never slewed) and the monotonic clock
//! (`CLOCK_MONOTONIC`, which NTP slews). On every read it takes one
//! sample and computes, with `rho` the node's oscillator rate budget (plan
//! 43 C7a; 1 000 ppm by default, see "The rate budget")
//!
//! ```text
//! e_raw = raw - raw0                       // elapsed on the oscillator (unslewed)
//! S     = running max of (boot - mono) - (boot0 - mono0)   // time suspended
//! e_h   = ceil(e_raw / (1 - rho)) + S      // runs fast by rho
//! gap   = wall - wall0 - e_h               // how far wall is ahead of e_h
//! J     = running max of gap, at least 0
//! HONOUR = wall0 + e_h + J
//!
//! WAIT  = wall0 + floor(e_raw / (1 + rho)) // runs slow by rho
//!
//! WALL  = wall                             // as the host reports it
//! ```
//!
//! Each term is rate-pure on its own: RAW is never slewed, and on Linux
//! BOOTTIME and MONOTONIC are slewed together, so their difference is
//! exactly the time spent suspended, whatever the slew. (Taking the max or
//! min of the *absolute* elapsed values of the three clocks instead, as a
//! first version did, is wrong per interval: once the clocks have drifted
//! apart by their rates, one of them dominates and its slew leaks into
//! HONOUR or WAIT.)
//!
//! The newtypes [`Ms`] (WALL), [`HonourMs`] and [`WaitMs`] each carry one
//! reading. They share no `From` and no arithmetic; the only crossings are
//! the four named conversions of [`Now`] (`wall_of_wait`, `wall_of_honour`,
//! `wait_of_wall`, `wait_of_honour`), each of which maps a *remaining
//! duration* onto the target clock *now*, so a step before the conversion
//! is absorbed and only the offset at that instant matters.
//!
//! # Which clock decides what
//!
//! - HONOUR: a node that *holds* a right (its lease, a lock grant, a
//!   delegation, the right to answer strict reads) stops using it at
//!   `sent + ttl - margin` on this clock. It never under-counts, so a
//!   backward wall step cannot make a lapsed window look live.
//! - WAIT: a node that *waits out* someone else's right (an owner
//!   outwaiting a grant, a takeover floor, a quarantine) stops waiting at
//!   `recv + ttl + margin` on this clock. It never over-counts, so a
//!   forward wall step cannot end a wait early. Liveness stamps and
//!   timeouts use it as well.
//! - WALL: anything compared across nodes, written to S3, persisted across
//!   a restart, used as an identity key, or shown in status and logs.
//!
//! # Properties
//!
//! - **P1, monotone.** WAIT: RAW is monotone. HONOUR: RAW is monotone and
//!   `S` and `J` are running maxima.
//!   Across threads a reading taken after another one (happens-after) is
//!   not smaller: WAIT is a pure function of monotone sources, and `jump`
//!   is read with `Acquire` after any earlier `AcqRel` `fetch_max`.
//! - **P2, HONOUR never under-counts.** For readings `h1` before `h2`,
//!   `h2 - h1` is at least the real elapsed time, **exactly, for any
//!   oscillator within the budget** (C7a): it is RAW's delta scaled by
//!   `1/(1 - rho)` (so `>= real * (1 - rho)/(1 - rho)`, to the millisecond)
//!   plus the suspend term `S` and `J`, which only add (per interval,
//!   whatever the clocks' absolute values: `S` is a difference of two
//!   clocks slewed together). So `sent + ttl - margin` on HONOUR is never
//!   later in real time than the same window measured in real time: the
//!   margin is slack for transit and rounding, not for the oscillator.
//!   The exception is an interval that spans a suspend: `S` is the kernel's
//!   own account of the time asleep, outside the budget, and a host that
//!   times a suspend with a persistent clock of whole seconds (the x86 CMOS
//!   RTC, where no clock source runs through suspend) can count it short by
//!   up to a second. HONOUR therefore adds [`SUSPEND_PAD_MS`] (one second)
//!   per suspend it detects (`S` grew by more than [`SUSPEND_DETECT_MS`]
//!   between two readings; plan 43 C7b), on every platform: a suspend that
//!   was counted exactly is over-counted by a second, the safe direction (a
//!   window an honouring node keeps ends that much early, and a suspend
//!   already lapses every window shorter than it), and one counted short is
//!   covered. A suspend the host counts as nothing at all, shorter than a
//!   second, is not detected, and is absorbed by the margin as before.
//! - **P3, WAIT never over-counts.** `w2 - w1` is RAW's delta scaled by
//!   `1/(1 + rho)`: at most the real elapsed time, exactly for any
//!   oscillator within the budget, and not slewed. It does not count
//!   suspend, which is conservative for a wait.
//! - **P4, steps.** A backward WALL step changes neither HONOUR nor WAIT.
//!   A forward step of `D` advances HONOUR by `D` once, at the first read
//!   after it (that read raises `jump`) and leaves WAIT alone. A forward
//!   step that only undoes an earlier backward one does not advance
//!   HONOUR: `gap` returns to a value `jump` has already seen.
//! - **P5, order.** `HONOUR >= WALL` always (`J >= gap`) and
//!   `HONOUR >= WAIT` always. WAIT and WALL are not ordered after a step.
//! - **P5b, long uptime.** The budget is paid whether the oscillator uses it
//!   or not: HONOUR runs ahead of WALL by `rho` of the uptime and WAIT behind
//!   it (86 s a day at 1 000 ppm). Nothing compares them with each other or
//!   across processes or nodes (P6); the four conversions of [`Now`] map
//!   remaining durations and stay correct. `status.clock`'s `honour_ahead_ms`
//!   and `wait_behind_ms` grow with it. A forward wall step after uptime `U`
//!   raises `J` only by the part of the step beyond the lead that `rho * U`
//!   has already given HONOUR (P4's "advances HONOUR by `D`" is exact only
//!   while HONOUR is level with WALL).
//! - **P6, no common origin across incarnations.** A new process starts
//!   all three at WALL. HONOUR or WAIT values from another process, or
//!   another node, mean nothing on this one; [`ProtocolClock::id`] names
//!   the clock a stamp belongs to.
//!
//! # The rate budget (plan 43 C7a)
//!
//! The oscillator behind RAW is not exact: a crystal is 20 to 100 ppm off,
//! a hot or failing one more. Each node budgets its own oscillator (`rho`,
//! [`ProtocolClock::rate_budget_ppm`]; per node, nothing needs two nodes to
//! agree on it) and the readings pay it, once, here, instead of at every
//! margin: WAIT counts `1/(1 + rho)` of RAW's elapsed time and HONOUR
//! `1/(1 - rho)` of it. For any oscillator whose rate error is at most
//! `rho`, WAIT then never advances more, and HONOUR never less, than real
//! time, whatever the span, which makes every holder/waiter pair safe with
//! the margin it already has (the plan's `margin >= 2 rho T + transit`, the
//! `2 rho T` paid by the readings; the margin is left as transit, processing
//! and rounding slack). `0` reads exactly as C7-H1's clock did. The step
//! detector and WALL are not scaled.
//!
//! RAW is never slewed, which is why it is the source: a slewed source
//! could not be budgeted with 1 000 ppm (a time daemon slews up to 83 333
//! ppm). The scaling is fixed point (`2^40`-fraction multipliers computed
//! at creation: one `u128` multiply per reading).
//!
//! A timer of `after_ms` of WAIT, slept on the runtime's monotonic clock,
//! ends before WAIT advanced by that much (WAIT runs slow):
//! [`ProtocolClock::sleep_ms_for_wait`] stretches the sleep so that it does
//! not, for an oscillator that is exact; a slow one wakes the handler early
//! and the handler re-arms for the rest.
//!
//! # Read order
//!
//! A sample reads WALL first, then the boot, monotonic and raw clocks. If
//! the thread is descheduled between the reads, the elapsed terms are
//! larger than the wall value was, so `gap` is smaller and `J` cannot
//! rise: the only error is in the safe direction. The reverse order could
//! make a preempted read raise `J` for the rest of the process. A
//! preemption between the boot and the monotonic read lowers `S` for that
//! sample only (it is a running maximum); a late raw read only raises it.
//!
//! # Step detection (observability only)
//!
//! `wall - (boot - boot0)` moves only on a wall step (NTP slewing moves
//! the wall and boot clocks together on Linux). A read that sees it
//! differ from the last seen value by [`STEP_DETECT_MS`] or more takes a
//! second sample (a preemption between the two reads of the first would
//! look like a backward step) and, if it still differs, counts the step.
//! The counters decide nothing in the protocol: P1-P5 are the safety.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// A wall-clock difference this large between two samples is counted as a
/// step (`StepStats`).
pub const STEP_DETECT_MS: i64 = 100;

/// Growth of `S` between two readings above which they straddle a suspend
/// (rounding of the millisecond differences stays below it).
pub const SUSPEND_DETECT_MS: i64 = 5;

/// What HONOUR adds per detected suspend: the most the kernel's sleep
/// accounting can be short by on a host that times it in whole seconds.
pub const SUSPEND_PAD_MS: i64 = 1_000;

/// The largest rate budget a clock takes, ppm (the platform refuses the same
/// bound): a larger one is clamped, so the scale `1/(1 - rho)` stays finite.
pub const MAX_RATE_BUDGET_PPM: u32 = 499_999;

/// The fixed-point fraction of the readings' scale (see the module doc).
const SCALE_SHIFT: u32 = 40;

/// WAIT's and HONOUR's multipliers for `budget_ppm`: `floor(2^40 * 1e6 /
/// (1e6 + rho))` and `ceil(2^40 * 1e6 / (1e6 - rho))`.
fn scale_muls(budget_ppm: u32) -> (u64, u64) {
    let (one, ppm, rho) = (1u128 << SCALE_SHIFT, 1_000_000u128, budget_ppm as u128);
    let wait = one * ppm / (ppm + rho);
    let honour = (one * ppm).div_ceil(ppm - rho);
    (wait as u64, honour as u64)
}

macro_rules! clock_ms {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub i64);

        impl $name {
            pub fn plus(self, ms: u64) -> Self {
                Self(self.0.saturating_add(ms as i64))
            }

            pub fn minus(self, ms: u64) -> Self {
                Self(self.0.saturating_sub(ms as i64))
            }

            /// `self - earlier`, in ms (negative when `earlier` is later).
            pub fn since(self, earlier: Self) -> i64 {
                self.0 - earlier.0
            }
        }
    };
}

clock_ms! {
    /// Unix milliseconds on the host's wall clock (plus the fault
    /// offset and step): what is compared across nodes, written to S3,
    /// persisted, used as an identity key or shown in status. In the
    /// simulation, the paused tokio clock offset from a fixed base.
    Ms
}

clock_ms! {
    /// Unix-ms-like reading of a node's honour clock: monotone, never
    /// under-counts real elapsed time, follows forward wall steps and
    /// ignores backward ones. What a holder of a right stamps and judges
    /// it by. Meaningless on another node or in another process.
    HonourMs
}

clock_ms! {
    /// Unix-ms-like reading of a node's wait clock: monotone, never
    /// over-counts real elapsed time, ignores wall steps in both
    /// directions. What a waiter outwaiting someone else's right stamps
    /// and judges it by, and what liveness timers use. Meaningless on
    /// another node or in another process.
    WaitMs
}

/// The boot-scoped elapsed reading of one sample (plan 43 C7a): what a
/// deadline persisted across a restart is stamped with ([`BootStamp`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BootNow {
    /// This boot ([`ClockSources::boot_id`]; 0: unknown, which no stamp
    /// matches).
    pub id: u64,
    /// The raw clock since boot, ms, **unscaled** (the clock's budget is
    /// applied only where a WAIT duration is mapped onto it and back).
    pub raw_ms: i64,
    /// The clock's rate budget, ppm: the map between a WAIT duration and
    /// raw ms.
    pub budget_ppm: u32,
}

/// The three readings of one instant on one node, taken from one sample,
/// and the boot reading a persisted deadline is stamped with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Now {
    pub wall: Ms,
    pub honour: HonourMs,
    pub wait: WaitMs,
    pub boot: BootNow,
}

/// A deadline as persisted (plan 43 C7a, Appendix B row 14): written with
/// the boot it was measured on, so that a restart on the same boot reads the
/// time left on the raw clock (no wall step while the node was up or down
/// can move it) and a restart on another boot, or from a value that carries
/// no boot, waits the recorded `span`. See [`Now::stamp_of_wait`] and
/// [`Now::wait_of_stamp`]; the text form lives with the stores that persist
/// it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BootStamp {
    /// The deadline on the wall clock at the write (what status and logs
    /// show; token 0 of the persisted value, and the only token an older
    /// build wrote).
    pub wall: Ms,
    /// How far ahead of the write the deadline was, in WAIT ms: all that is
    /// left of it, at most, wherever and whenever the process that reads it
    /// starts.
    pub span_ms: i64,
    /// The boot of the write (0: none).
    pub boot_id: u64,
    /// The raw clock reading (since boot, ms, unscaled) at which the
    /// deadline ends.
    pub raw_until_ms: i64,
}

/// `at` mapped through a remaining duration: `base + (at - source_now)`,
/// saturating. `i64::MAX` ("never") stays `i64::MAX`; `0` and `i64::MIN`
/// ("none") become `0`.
fn map_remaining(at: i64, source_now: i64, target_now: i64) -> i64 {
    if at == i64::MAX {
        i64::MAX
    } else if at <= 0 {
        0
    } else {
        target_now.saturating_add(at.saturating_sub(source_now))
    }
}

impl Now {
    /// All three readings equal: tests, and any caller with a single
    /// reading and no steps.
    pub fn same(ms: i64) -> Now {
        Self::same_on_boot(ms, 0)
    }

    /// [`Now::same`] on a boot named `boot_id` (0: unknown), the raw clock
    /// reading `ms` and no budget: for tests of the persisted deadlines.
    pub fn same_on_boot(ms: i64, boot_id: u64) -> Now {
        Now {
            wall: Ms(ms),
            honour: HonourMs(ms),
            wait: WaitMs(ms),
            boot: BootNow {
                id: boot_id,
                raw_ms: ms,
                budget_ppm: 0,
            },
        }
    }

    /// Advance all of them by `ms` (time passing; no step).
    pub fn plus(self, ms: u64) -> Now {
        Now {
            wall: self.wall.plus(ms),
            honour: self.honour.plus(ms),
            wait: self.wait.plus(ms),
            boot: BootNow {
                raw_ms: self.boot.raw_ms.saturating_add(ms as i64),
                ..self.boot
            },
        }
    }

    /// A WAIT deadline as a WALL value: `wall + (at - wait)`. For a value
    /// that leaves the process (S3, a peer, a persisted horizon); the
    /// crossing's offset dependence is plan 43's Appendix B row for that
    /// value (rows 6, 9, 14).
    pub fn wall_of_wait(&self, at: WaitMs) -> Ms {
        Ms(map_remaining(at.0, self.wait.0, self.wall.0))
    }

    /// A HONOUR deadline as a WALL value (a token minted from its holder's
    /// honour window; row 6).
    pub fn wall_of_honour(&self, at: HonourMs) -> Ms {
        Ms(map_remaining(at.0, self.honour.0, self.wall.0))
    }

    /// A WALL deadline as a WAIT value (a persisted horizon loaded at
    /// start, a peer's absolute taken as a local wait; rows 6, 14).
    pub fn wait_of_wall(&self, at: Ms) -> WaitMs {
        WaitMs(map_remaining(at.0, self.wall.0, self.wait.0))
    }

    /// A WAIT deadline as a persisted stamp: `span = at - wait` (at least
    /// 0, in WAIT ms), the raw reading at which it ends (`span` mapped
    /// through the budget onto the raw clock: `ceil(span (1e6 + rho)/1e6)`
    /// past now; the raw reading is floored to the ms, so the end is early by
    /// less than 1 ms) and the wall value `wall_of_wait(at)`.
    pub fn stamp_of_wait(&self, at: WaitMs) -> BootStamp {
        let span = at.0.saturating_sub(self.wait.0).max(0);
        let raw = (span as i128 * (1_000_000 + self.boot.budget_ppm as i128) + 999_999) / 1_000_000;
        BootStamp {
            wall: self.wall_of_wait(at),
            span_ms: span,
            boot_id: self.boot.id,
            raw_until_ms: self
                .boot
                .raw_ms
                .saturating_add(raw.min(i64::MAX as i128) as i64),
        }
    }

    /// A persisted stamp as a WAIT deadline in this process. On the boot it
    /// was written on (ids equal, not 0): the time left on the raw clock,
    /// `ceil((raw_until - raw) 1e6 / (1e6 + rho))`, never more than the
    /// recorded span. On another boot, an unknown one, or a stamp with none:
    /// the whole span from now (only time has passed since the write, so no
    /// more than that is left, and a step of the wall clock changes
    /// nothing). `None`: nothing left.
    pub fn wait_of_stamp(&self, s: &BootStamp) -> Option<WaitMs> {
        let same_boot = s.boot_id != 0 && s.boot_id == self.boot.id;
        let left = if same_boot {
            let raw = (s.raw_until_ms as i128 - self.boot.raw_ms as i128).max(0);
            let wait = (raw * 1_000_000 + 1_000_000 + self.boot.budget_ppm as i128 - 1)
                / (1_000_000 + self.boot.budget_ppm as i128);
            wait.min(s.span_ms.max(0) as i128) as i64
        } else {
            s.span_ms
        };
        (left > 0).then(|| self.wait.plus(left as u64))
    }

    /// A HONOUR deadline as a WAIT value: the time left on a holder's
    /// window, as a stamp of the wait clock (a statistic that relates a
    /// holder's window to wait-clock stamps of freezes). Never used to
    /// decide what a node honours or waits out.
    pub fn wait_of_honour(&self, at: HonourMs) -> WaitMs {
        WaitMs(map_remaining(at.0, self.honour.0, self.wait.0))
    }
}

/// One sample of the four host clocks, in the units the host reports.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RawReading {
    /// Unix ms, as the host's wall clock reports it (plus faults).
    pub wall_ms: i64,
    /// Elapsed ns on a clock that counts suspend and may be slewed
    /// (`CLOCK_BOOTTIME`).
    pub boot_ns: u64,
    /// Elapsed ns on a clock that is never slewed (`CLOCK_MONOTONIC_RAW`).
    pub raw_ns: u64,
    /// Elapsed ns on the slewed monotonic clock (`CLOCK_MONOTONIC`).
    pub mono_ns: u64,
}

/// Where a [`ProtocolClock`] gets its samples. Each method reads one
/// host clock; `read` takes all four in the order the clock requires
/// (wall first, see the module doc).
pub trait ClockSources: Send + Sync {
    fn wall_ms(&self) -> i64;
    fn boot_ns(&self) -> u64;
    fn raw_ns(&self) -> u64;
    fn mono_ns(&self) -> u64;

    /// This boot's identity (plan 43 C7a: what a deadline persisted across
    /// a restart is stamped with, so that a restart on the same boot reads
    /// it on the raw clock and one after a reboot waits the recorded
    /// span). 0: unknown, which no stamp matches. The raw clock must be
    /// absolute (since this boot) wherever the id is not 0.
    fn boot_id(&self) -> u64 {
        0
    }

    fn read(&self) -> RawReading {
        let wall_ms = self.wall_ms();
        let boot_ns = self.boot_ns();
        let mono_ns = self.mono_ns();
        let raw_ns = self.raw_ns();
        RawReading {
            wall_ms,
            boot_ns,
            raw_ns,
            mono_ns,
        }
    }
}

/// What the clock has seen of wall steps (observability; decides nothing).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StepStats {
    /// Suspends the readings straddled (each padded by [`SUSPEND_PAD_MS`]
    /// on the honour clock).
    pub suspends: u64,
    pub steps_forward: u64,
    pub steps_backward: u64,
    /// The sum of the steps' magnitudes.
    pub stepped_ms_total: u64,
    /// The last step: the wall reading after it and its signed size.
    pub last_step: Option<(i64, i64)>,
}

/// One node's protocol clock (one per process; the simulator makes one
/// per node incarnation). See the module doc for the readings, the
/// properties and the read order.
pub struct ProtocolClock {
    sources: Arc<dyn ClockSources>,
    id: u64,
    /// The sources' boot id, read once ([`ClockSources::boot_id`]).
    boot_id: u64,
    /// The oscillator's rate budget, ppm (plan 43 C7a; clamped to
    /// [`MAX_RATE_BUDGET_PPM`]).
    budget_ppm: u32,
    /// `floor(2^40 / (1 + rho))` and `ceil(2^40 / (1 - rho))`: WAIT's and
    /// HONOUR's scale on RAW's elapsed time (`2^40` each for budget 0).
    wait_mul: u64,
    honour_mul: u64,
    wall0: i64,
    boot0: u64,
    raw0: u64,
    mono0: u64,
    /// The highest forward excess seen (the module doc's `J`).
    jump: AtomicI64,
    /// The most time suspended seen, ms (the module doc's `S`), packed with
    /// the number of suspends the readings straddled (`S` grew by more than
    /// [`SUSPEND_DETECT_MS`] at one reading), which HONOUR pads each by
    /// [`SUSPEND_PAD_MS`]: one word, so that a reader never sees `S` raised
    /// without the pad for it ([`pack_suspend`]).
    suspend: AtomicU64,
    /// `wall - (boot - boot0)` as last seen to change by a step.
    last_skew: AtomicI64,
    steps_forward: AtomicU64,
    steps_backward: AtomicU64,
    stepped_ms_total: AtomicU64,
    last_step: Mutex<Option<(i64, i64)>>,
}

impl std::fmt::Debug for ProtocolClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtocolClock")
            .field("id", &self.id)
            .field("wall0", &self.wall0)
            .finish_non_exhaustive()
    }
}

/// `ns * mul / 2^40`, rounded up when `up`, saturating.
fn scaled_ns(ns: u64, mul: u64, up: bool) -> u64 {
    let p = ns as u128 * mul as u128;
    let p = if up {
        p + ((1u128 << SCALE_SHIFT) - 1)
    } else {
        p
    };
    (p >> SCALE_SHIFT).min(u64::MAX as u128) as u64
}

fn ns_to_ms(ns: u64) -> i64 {
    (ns / 1_000_000).min(i64::MAX as u64) as i64
}

/// `S` (ms, below 2^40: 35 years of sleep) and the suspend count (the high
/// 24 bits) in one word.
const SUSPEND_BITS: u32 = 40;

fn pack_suspend((s, n): (i64, u64)) -> u64 {
    (n.min((1 << 24) - 1) << SUSPEND_BITS) | (s.clamp(0, (1 << SUSPEND_BITS) - 1) as u64)
}

fn unpack_suspend(w: u64) -> (i64, u64) {
    ((w & ((1 << SUSPEND_BITS) - 1)) as i64, w >> SUSPEND_BITS)
}

/// `S` and the count after a reading whose own `S` is `s_now`, from the
/// packed state `w`: `S` is the running maximum; a rise past
/// [`SUSPEND_DETECT_MS`] is one more suspend straddled.
fn raised_suspend(w: u64, s_now: i64) -> (i64, u64) {
    let (s, n) = unpack_suspend(w);
    if s_now > s {
        (s_now, n + u64::from(s_now > s + SUSPEND_DETECT_MS))
    } else {
        (s, n)
    }
}

/// What HONOUR adds for `count` suspends.
fn suspend_pad_ms(count: u64) -> i64 {
    (count as i64).saturating_mul(SUSPEND_PAD_MS)
}

impl ProtocolClock {
    /// A clock over `sources` with a random non-zero id, whose node's
    /// oscillator is budgeted `budget_ppm` (plan 43 C7a: the bound on its
    /// rate error; 0 reads exactly as C7-H1's clock did).
    pub fn new(sources: Arc<dyn ClockSources>, budget_ppm: u32) -> Self {
        let id = RandomState::new().hash_one(std::process::id());
        Self::with_id(sources, if id == 0 { 1 } else { id }, budget_ppm)
    }

    /// A clock with a chosen id (the simulator's deterministic ones).
    /// `id` must be non-zero: 0 means "no clock" in stamps that carry one.
    pub fn with_id(sources: Arc<dyn ClockSources>, id: u64, budget_ppm: u32) -> Self {
        assert_ne!(id, 0, "a protocol clock id is never 0");
        let budget_ppm = budget_ppm.min(MAX_RATE_BUDGET_PPM);
        let (wait_mul, honour_mul) = scale_muls(budget_ppm);
        let boot_id = sources.boot_id();
        let s = sources.read();
        Self {
            id,
            boot_id,
            budget_ppm,
            wait_mul,
            honour_mul,
            wall0: s.wall_ms,
            boot0: s.boot_ns,
            raw0: s.raw_ns,
            mono0: s.mono_ns,
            jump: AtomicI64::new(0),
            suspend: AtomicU64::new(0),
            last_skew: AtomicI64::new(s.wall_ms),
            steps_forward: AtomicU64::new(0),
            steps_backward: AtomicU64::new(0),
            stepped_ms_total: AtomicU64::new(0),
            last_step: Mutex::new(None),
            sources,
        }
    }

    /// Names this clock: a stamp or a beat carries it so that a reader
    /// knows which clock the value is on (P6). Never 0.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// The oscillator rate budget this clock was built with, ppm.
    pub fn rate_budget_ppm(&self) -> u32 {
        self.budget_ppm
    }

    /// How long to sleep, on a clock that runs at the oscillator's rate (the
    /// runtime's monotonic one), for WAIT to advance by `after_ms`:
    /// `ceil(after_ms * (1e6 + rho) / 1e6)`. A timer slept for `after_ms`
    /// would end before WAIT (which runs slow by `rho`) had advanced that
    /// much, and its handler would re-check and sleep again; this is that
    /// second sleep spared. An oscillator slower than real time still wakes
    /// the handler early, which re-arms for the rest.
    pub fn sleep_ms_for_wait(&self, after_ms: u64) -> u64 {
        let stretched =
            (after_ms as u128 * (1_000_000 + self.budget_ppm as u128)).div_ceil(1_000_000);
        stretched.min(u64::MAX as u128) as u64
    }

    /// All three readings from one sample.
    pub fn now(&self) -> Now {
        let s = self.sources.read();
        self.detect_step(s.wall_ms, s.boot_ns);
        Now {
            wall: Ms(s.wall_ms),
            honour: self.honour_of(s.wall_ms, s.boot_ns, s.mono_ns, s.raw_ns),
            wait: self.wait_of(s.raw_ns),
            boot: BootNow {
                id: self.boot_id,
                raw_ms: ns_to_ms(s.raw_ns),
                budget_ppm: self.budget_ppm,
            },
        }
    }

    /// The honour reading (all four host clocks and two atomic loads).
    pub fn honour(&self) -> HonourMs {
        let wall = self.sources.wall_ms();
        let boot = self.sources.boot_ns();
        let mono = self.sources.mono_ns();
        let raw = self.sources.raw_ns();
        self.detect_step(wall, boot);
        self.honour_of(wall, boot, mono, raw)
    }

    /// The honour reading this instant would give, recording nothing: `S`
    /// and `J` (the running maxima) are read, not raised. For an oracle
    /// that must see what a node honours without moving the node's clock
    /// (plan 43 C7b: the simulation's lease-claim check). Exactly
    /// [`Self::honour`]'s value, except that a later read still raises
    /// the maxima this one would have.
    pub fn peek_honour(&self) -> HonourMs {
        let s = self.sources.read();
        let diff = |b: u64, m: u64| (b as i128 - m as i128) / 1_000_000;
        let s_now = (diff(s.boot_ns, s.mono_ns) - diff(self.boot0, self.mono0))
            .clamp(0, i64::MAX as i128) as i64;
        let (susp, count) = raised_suspend(self.suspend.load(Ordering::Acquire), s_now);
        let e_h = ns_to_ms(scaled_ns(
            s.raw_ns.saturating_sub(self.raw0),
            self.honour_mul,
            true,
        ))
        .saturating_add(susp)
        .saturating_add(suspend_pad_ms(count));
        let gap = s.wall_ms.saturating_sub(self.wall0).saturating_sub(e_h);
        let jump = self.jump.load(Ordering::Acquire).max(gap);
        HonourMs(self.wall0.saturating_add(e_h).saturating_add(jump))
    }

    /// The wait reading (the raw clock: one host read).
    pub fn wait(&self) -> WaitMs {
        self.wait_of(self.sources.raw_ns())
    }

    /// The wall reading (one host read).
    pub fn wall(&self) -> Ms {
        Ms(self.sources.wall_ms())
    }

    fn honour_of(&self, wall: i64, boot: u64, mono: u64, raw: u64) -> HonourMs {
        // Time suspended since creation: BOOTTIME minus MONOTONIC, both
        // slewed together, so a slew cancels. A running maximum.
        let diff = |b: u64, m: u64| (b as i128 - m as i128) / 1_000_000;
        let s_now =
            (diff(boot, mono) - diff(self.boot0, self.mono0)).clamp(0, i64::MAX as i128) as i64;
        let mut cur = self.suspend.load(Ordering::Acquire);
        let (susp, count) = loop {
            let next = raised_suspend(cur, s_now);
            if next == unpack_suspend(cur) {
                break next;
            }
            // The thread whose reading raised `S` records it, and the
            // suspend it straddled if the rise passes the threshold, in one
            // step (a rise another thread made first is read, not redone).
            match self.suspend.compare_exchange_weak(
                cur,
                pack_suspend(next),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break next,
                Err(seen) => cur = seen,
            }
        };
        let e_h = ns_to_ms(scaled_ns(
            raw.saturating_sub(self.raw0),
            self.honour_mul,
            true,
        ))
        .saturating_add(susp)
        .saturating_add(suspend_pad_ms(count));
        let gap = wall.saturating_sub(self.wall0).saturating_sub(e_h);
        let seen = self.jump.load(Ordering::Acquire);
        let jump = if gap > seen {
            self.jump.fetch_max(gap, Ordering::AcqRel).max(gap)
        } else {
            seen
        };
        HonourMs(self.wall0.saturating_add(e_h).saturating_add(jump))
    }

    fn wait_of(&self, raw: u64) -> WaitMs {
        WaitMs(self.wall0.saturating_add(ns_to_ms(scaled_ns(
            raw.saturating_sub(self.raw0),
            self.wait_mul,
            false,
        ))))
    }

    fn skew_of(&self, wall: i64, boot: u64) -> i64 {
        wall.saturating_sub(ns_to_ms(boot.saturating_sub(self.boot0)))
    }

    fn detect_step(&self, wall: i64, boot: u64) {
        let seen = self.last_skew.load(Ordering::Relaxed);
        let skew = self.skew_of(wall, boot);
        if skew.saturating_sub(seen).abs() < STEP_DETECT_MS {
            return;
        }
        // A thread descheduled between the wall and boot reads looks like
        // a backward step; a real step persists into a fresh sample.
        let wall2 = self.sources.wall_ms();
        let boot2 = self.sources.boot_ns();
        let skew2 = self.skew_of(wall2, boot2);
        if skew2.saturating_sub(seen).abs() < STEP_DETECT_MS {
            return;
        }
        if self
            .last_skew
            .compare_exchange(seen, skew2, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            // Another thread counted this step.
            return;
        }
        let delta = skew2 - seen;
        if delta > 0 {
            self.steps_forward.fetch_add(1, Ordering::Relaxed);
        } else {
            self.steps_backward.fetch_add(1, Ordering::Relaxed);
        }
        self.stepped_ms_total
            .fetch_add(delta.unsigned_abs(), Ordering::Relaxed);
        if let Ok(mut last) = self.last_step.lock() {
            *last = Some((wall2, delta));
        }
    }

    /// How many steps (either direction) have been seen: two atomic loads,
    /// for a caller that polls and wants the full `step_stats` only when
    /// this changed.
    pub fn step_count(&self) -> u64 {
        self.steps_forward.load(Ordering::Relaxed) + self.steps_backward.load(Ordering::Relaxed)
    }

    /// The steps seen so far.
    pub fn step_stats(&self) -> StepStats {
        StepStats {
            suspends: unpack_suspend(self.suspend.load(Ordering::Relaxed)).1,
            steps_forward: self.steps_forward.load(Ordering::Relaxed),
            steps_backward: self.steps_backward.load(Ordering::Relaxed),
            stepped_ms_total: self.stepped_ms_total.load(Ordering::Relaxed),
            last_step: self.last_step.lock().ok().and_then(|l| *l),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// Sources the test moves by hand: one atomic per clock.
    #[derive(Default)]
    struct Stub {
        wall: AtomicI64,
        boot: AtomicU64,
        raw: AtomicU64,
        mono: AtomicU64,
    }

    impl ClockSources for Stub {
        fn wall_ms(&self) -> i64 {
            self.wall.load(Ordering::SeqCst)
        }
        fn boot_ns(&self) -> u64 {
            self.boot.load(Ordering::SeqCst)
        }
        fn raw_ns(&self) -> u64 {
            self.raw.load(Ordering::SeqCst)
        }
        fn mono_ns(&self) -> u64 {
            self.mono.load(Ordering::SeqCst)
        }
    }

    const MS: u64 = 1_000_000;
    const START: i64 = 1_780_000_000_000;

    fn stub() -> (Arc<Stub>, ProtocolClock) {
        let s = Arc::new(Stub::default());
        s.wall.store(START, Ordering::SeqCst);
        // Non-zero origins: nothing may assume the clocks start at 0.
        s.boot.store(5_000 * MS, Ordering::SeqCst);
        s.raw.store(7_000 * MS, Ordering::SeqCst);
        s.mono.store(9_000 * MS, Ordering::SeqCst);
        let c = ProtocolClock::with_id(s.clone(), 7, 0);
        (s, c)
    }

    /// Real time passes: every source advances `ms`, the wall with them.
    /// The wall is written last: a reader reads it first, so it can see
    /// an old wall with a new boot (a smaller `gap`), never the reverse.
    fn tick(s: &Stub, ms: u64) {
        s.boot.fetch_add(ms * MS, Ordering::SeqCst);
        s.raw.fetch_add(ms * MS, Ordering::SeqCst);
        s.mono.fetch_add(ms * MS, Ordering::SeqCst);
        s.wall.fetch_add(ms as i64, Ordering::SeqCst);
    }

    fn step(s: &Stub, ms: i64) {
        s.wall.fetch_add(ms, Ordering::SeqCst);
    }

    /// Expectation: the three readings start at the wall and move with
    /// real time; nothing is counted.
    #[test]
    fn the_readings_start_at_wall_and_follow_time() {
        let (s, c) = stub();
        let readings = |n: Now| (n.wall.0, n.honour.0, n.wait.0);
        assert_eq!(readings(c.now()), (START, START, START));
        tick(&s, 1_234);
        let (w, h, t) = (START + 1_234, START + 1_234, START + 1_234);
        assert_eq!(readings(c.now()), (w, h, t));
        assert_eq!(c.step_stats(), StepStats::default());
    }

    /// Expectation (plan 43 C7b): `peek_honour` returns what `honour` would
    /// and records nothing: a forward step peeked at and then undone leaves
    /// no `J` behind, as if nobody had looked.
    #[test]
    fn a_peek_reads_honour_and_records_nothing() {
        let (s, c) = stub();
        tick(&s, 1_000);
        step(&s, 3_000);
        let peeked = c.peek_honour();
        // Undo the step before anything reads: the peek raised no `J`.
        step(&s, -3_000);
        tick(&s, 100);
        let after = c.honour();
        assert_eq!(peeked.0, START + 4_000);
        assert_eq!(after.0, START + 1_100, "the peek must not have raised J");
        // Peek and read agree on an unstepped clock.
        tick(&s, 50);
        assert_eq!(c.peek_honour(), c.honour());
    }

    /// Expectation (P4): a backward wall step of 3 s changes neither
    /// HONOUR nor WAIT; WALL drops; one backward step is counted.
    #[test]
    fn a_backward_step_moves_only_wall() {
        let (s, c) = stub();
        tick(&s, 1_000);
        let before = c.now();
        step(&s, -3_000);
        tick(&s, 500);
        let after = c.now();
        assert_eq!(after.wall.0, before.wall.0 - 3_000 + 500);
        assert_eq!(after.honour.since(before.honour), 500);
        assert_eq!(after.wait.since(before.wait), 500);
        let st = c.step_stats();
        assert_eq!((st.steps_forward, st.steps_backward), (0, 1));
        assert_eq!(st.stepped_ms_total, 3_000);
        assert_eq!(st.last_step, Some((after.wall.0, -3_000)));
    }

    /// Expectation (P4): a forward step of 3 s advances HONOUR by exactly
    /// 3 000 once and WAIT not at all; HONOUR stays level with WALL.
    #[test]
    fn a_forward_step_advances_honour_once_and_not_wait() {
        let (s, c) = stub();
        tick(&s, 1_000);
        let before = c.now();
        step(&s, 3_000);
        let after = c.now();
        assert_eq!(after.honour.since(before.honour), 3_000);
        assert_eq!(after.wait.since(before.wait), 0);
        assert_eq!(after.honour.0, after.wall.0);
        // Time passing afterwards counts once, not again.
        tick(&s, 200);
        let later = c.now();
        assert_eq!(later.honour.since(after.honour), 200);
        assert_eq!(later.wait.since(after.wait), 200);
        let st = c.step_stats();
        assert_eq!((st.steps_forward, st.steps_backward), (1, 0));
        assert_eq!(st.stepped_ms_total, 3_000);
    }

    /// Expectation (P4): back 3 s then forward 3 s (a restore): HONOUR is
    /// never non-monotone and its total advance equals the real elapsed
    /// time; the restore is not counted twice.
    #[test]
    fn a_step_back_then_forward_is_not_counted_twice() {
        let (s, c) = stub();
        let start = c.now();
        let mut last = start.honour;
        let mut check = |c: &ProtocolClock| {
            let h = c.honour();
            assert!(h >= last, "honour went back: {h:?} < {last:?}");
            last = h;
        };
        tick(&s, 1_000);
        check(&c);
        step(&s, -3_000);
        check(&c);
        tick(&s, 1_000);
        check(&c);
        step(&s, 3_000);
        check(&c);
        tick(&s, 1_000);
        check(&c);
        let end = c.now();
        // 3 s of real time passed.
        assert_eq!(end.honour.since(start.honour), 3_000);
        assert_eq!(end.wait.since(start.wait), 3_000);
        assert_eq!(end.wall.since(start.wall), 3_000);
        let st = c.step_stats();
        assert_eq!((st.steps_forward, st.steps_backward), (1, 1));
    }

    /// Expectation (P4): forward 3 s then back 3 s: HONOUR stays 3 s ahead
    /// of WALL (`honour_ahead_ms`), WAIT is unaffected.
    #[test]
    fn a_step_forward_then_back_leaves_honour_ahead_of_wall() {
        let (s, c) = stub();
        tick(&s, 1_000);
        let before = c.now();
        step(&s, 3_000);
        let mid = c.now();
        assert_eq!(mid.honour.since(before.honour), 3_000);
        step(&s, -3_000);
        tick(&s, 500);
        let end = c.now();
        assert_eq!(end.honour.0 - end.wall.0, 3_000);
        assert_eq!(end.wait.since(before.wait), 500);
        // HONOUR kept the forward step and the elapsed time.
        assert_eq!(end.honour.since(before.honour), 3_500);
    }

    /// Expectation: a suspend (boot advances 10 s; raw and mono do not;
    /// wall advances 10 s) advances HONOUR by 10 s plus the pad
    /// ([`SUSPEND_PAD_MS`]) and WAIT by nothing.
    #[test]
    fn a_suspend_counts_for_honour_and_not_for_wait() {
        let (s, c) = stub();
        let before = c.now();
        s.wall.fetch_add(10_000, Ordering::SeqCst);
        s.boot.fetch_add(10_000 * MS, Ordering::SeqCst);
        let after = c.now();
        assert_eq!(after.honour.since(before.honour), 10_000 + SUSPEND_PAD_MS);
        assert_eq!(after.wait.since(before.wait), 0);
        // A suspend moves wall and boot together: not a step.
        assert_eq!(
            c.step_stats(),
            StepStats {
                suspends: 1,
                ..Default::default()
            }
        );
    }

    /// Plan 43 C7b (commit 7): each detected suspend is padded once, however
    /// many readings follow, and `S` growing by rounding (the detection
    /// threshold) is no suspend. `peek_honour` shows the pad a reading would
    /// add, recording nothing.
    #[test]
    fn a_detected_suspend_is_padded_once() {
        let (s, c) = stub();
        let t0 = c.now();
        // 5 ms of difference between boot and mono: rounding, no suspend.
        s.boot.fetch_add(5 * MS, Ordering::SeqCst);
        s.wall.fetch_add(5, Ordering::SeqCst);
        assert_eq!(c.now().honour.since(t0.honour), 5);
        assert_eq!(c.step_stats().suspends, 0);
        // A suspend of 3 s: padded, whoever reads first.
        s.boot.fetch_add(3_000 * MS, Ordering::SeqCst);
        s.wall.fetch_add(3_000, Ordering::SeqCst);
        let peeked = c.peek_honour();
        let a = c.honour();
        assert_eq!(a, peeked, "the peek is the reading");
        assert_eq!(a.since(t0.honour), 5 + 3_000 + SUSPEND_PAD_MS);
        assert_eq!(c.step_stats().suspends, 1);
        // Later readings add nothing more.
        tick(&s, 100);
        assert_eq!(c.now().honour.since(a), 100);
        assert_eq!(c.honour().since(a), 100);
        assert_eq!(c.step_stats().suspends, 1);
        // A second suspend is a second pad; WAIT never counts either.
        s.boot.fetch_add(20_000 * MS, Ordering::SeqCst);
        s.wall.fetch_add(20_000, Ordering::SeqCst);
        let n = c.now();
        assert_eq!(n.honour.since(a), 100 + 20_000 + SUSPEND_PAD_MS);
        assert_eq!(n.wait.since(t0.wait), 100);
        assert_eq!(c.step_stats().suspends, 2);
    }

    /// Expectation: with mono and boot slewed to 0.92 of raw (and the wall
    /// with them), HONOUR and WAIT both follow raw: HONOUR never
    /// under-counts, WAIT never over-counts. With raw the slow one, HONOUR
    /// follows the faster wall and WAIT raw.
    #[test]
    fn slewing_neither_under_counts_honour_nor_over_counts_wait() {
        let (s, c) = stub();
        let before = c.now();
        // 10 s of real (raw) time, during which the slewed clocks saw 9.2 s.
        s.raw.fetch_add(10_000 * MS, Ordering::SeqCst);
        s.boot.fetch_add(9_200 * MS, Ordering::SeqCst);
        s.mono.fetch_add(9_200 * MS, Ordering::SeqCst);
        s.wall.fetch_add(9_200, Ordering::SeqCst);
        let after = c.now();
        assert_eq!(after.honour.since(before.honour), 10_000);
        assert_eq!(after.wait.since(before.wait), 10_000);
        let (s, c) = stub();
        let before = c.now();
        s.raw.fetch_add(9_200 * MS, Ordering::SeqCst);
        s.boot.fetch_add(10_000 * MS, Ordering::SeqCst);
        s.mono.fetch_add(10_000 * MS, Ordering::SeqCst);
        s.wall.fetch_add(10_000, Ordering::SeqCst);
        let after = c.now();
        assert_eq!(after.honour.since(before.honour), 10_000);
        assert_eq!(after.wait.since(before.wait), 9_200);
        assert_eq!(c.step_stats(), StepStats::default());
    }

    /// Expectation (P2, P3, per interval): once the clocks have drifted
    /// apart (a day of uptime with raw 1.7 s ahead of boot and mono 3 s
    /// behind it), a suspend of 30 s advances HONOUR by 30 s, mono slewed
    /// fast by 8.3 % does not make WAIT over-count, and boot slewed slow does
    /// not make HONOUR under-count. (Max and min of the absolute elapsed
    /// values fail all three: +25 s, +13 s and +11 s.)
    #[test]
    fn pairwise_after_the_clocks_have_diverged() {
        let (s, c) = stub();
        let day = 86_400_000u64;
        s.raw.fetch_add(day * MS, Ordering::SeqCst);
        s.boot.fetch_add((day - 1_700) * MS, Ordering::SeqCst);
        s.mono.fetch_add((day - 3_000) * MS, Ordering::SeqCst);
        s.wall.fetch_add((day - 1_700) as i64, Ordering::SeqCst);
        let n0 = c.now();
        // A suspend: boot and wall go on, raw and mono do not.
        s.boot.fetch_add(30_000 * MS, Ordering::SeqCst);
        s.wall.fetch_add(30_000, Ordering::SeqCst);
        let n1 = c.now();
        assert_eq!(n1.honour.since(n0.honour), 30_000 + SUSPEND_PAD_MS);
        assert_eq!(n1.wait.since(n0.wait), 0);
        // 12 s real, mono (and boot, wall) slewed fast: 13 s.
        s.raw.fetch_add(12_000 * MS, Ordering::SeqCst);
        s.boot.fetch_add(13_000 * MS, Ordering::SeqCst);
        s.mono.fetch_add(13_000 * MS, Ordering::SeqCst);
        s.wall.fetch_add(13_000, Ordering::SeqCst);
        let n2 = c.now();
        assert!(n2.wait.since(n1.wait) <= 12_000, "{n1:?} {n2:?}");
        assert!(n2.honour.since(n1.honour) >= 12_000, "{n1:?} {n2:?}");
        // 12 s real, boot (and wall) slewed slow: 11 s; mono true.
        s.raw.fetch_add(12_000 * MS, Ordering::SeqCst);
        s.boot.fetch_add(11_000 * MS, Ordering::SeqCst);
        s.mono.fetch_add(12_000 * MS, Ordering::SeqCst);
        s.wall.fetch_add(11_000, Ordering::SeqCst);
        let n3 = c.now();
        assert!(n3.honour.since(n2.honour) >= 12_000, "{n2:?} {n3:?}");
        assert!(n3.wait.since(n2.wait) <= 12_000);
    }

    /// Expectation (P2, P3): over a random history of ticks (boot, mono and
    /// wall each slewed within ±8.3 %, boot and mono together), suspends and wall
    /// steps, starting from diverged clocks, every pair of consecutive
    /// readings has `honour delta >= real delta`, `wait delta <= real delta`
    /// and `honour delta >= suspended` (real time is RAW's, the unslewed
    /// clock, in this model).
    #[test]
    fn honour_and_wait_hold_per_interval_over_random_histories() {
        let (s, c) = stub();
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let mut rnd = move |n: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % n
        };
        // Diverged to begin with.
        s.raw.fetch_add(40_000_000 * MS, Ordering::SeqCst);
        s.boot.fetch_add(39_998_000 * MS, Ordering::SeqCst);
        s.mono.fetch_add(39_995_000 * MS, Ordering::SeqCst);
        s.wall.fetch_add(39_998_000, Ordering::SeqCst);
        let mut prev = c.now();
        for _ in 0..2_000 {
            let real = 1 + rnd(5_000); // ms
            let slew = |r: u64| real * (917 + r) / 1_000; // 0.917 .. 1.083
                                                          // BOOTTIME and MONOTONIC are slewed together (as on Linux).
            let (b, w) = (slew(rnd(167)), slew(rnd(167)));
            let m = b;
            let susp = if rnd(10) == 0 { rnd(60_000) } else { 0 };
            s.boot.fetch_add((b + susp) * MS, Ordering::SeqCst);
            s.mono.fetch_add(m * MS, Ordering::SeqCst);
            s.raw.fetch_add(real * MS, Ordering::SeqCst);
            s.wall.fetch_add((w + susp) as i64, Ordering::SeqCst);
            if rnd(25) == 0 {
                step(&s, rnd(20_000) as i64 - 10_000);
            }
            let now = c.now();
            let (dh, dw) = (now.honour.since(prev.honour), now.wait.since(prev.wait));
            assert!(dh >= real as i64, "honour {dh} < real {real}");
            assert!(dw <= real as i64, "wait {dw} > real {real}");
            assert!(dh >= susp as i64, "honour {dh} < suspended {susp}");
            prev = now;
        }
    }

    /// Expectation (P5): HONOUR >= WALL and HONOUR >= WAIT after any mix
    /// of steps, suspends and time.
    #[test]
    fn honour_is_never_behind_wall_or_wait() {
        let (s, c) = stub();
        for op in 0..6 {
            match op {
                0 => tick(&s, 700),
                1 => step(&s, -2_000),
                2 => tick(&s, 300),
                3 => step(&s, 5_000),
                4 => {
                    // A suspend of 4 s.
                    s.boot.fetch_add(4_000 * MS, Ordering::SeqCst);
                    s.wall.fetch_add(4_000, Ordering::SeqCst);
                }
                _ => step(&s, -9_000),
            }
            let n = c.now();
            assert!(n.honour.0 >= n.wall.0, "{n:?}");
            assert!(n.honour.0 >= n.wait.0, "{n:?}");
        }
    }

    /// Expectation (P1): 8 threads read while a step happens. Each
    /// thread's sequence is monotone, and a reading taken after another
    /// one is not smaller (happens-after through a shared counter).
    #[test]
    fn concurrent_readers_see_monotone_clocks_across_a_step() {
        let (s, c) = stub();
        let c = Arc::new(c);
        // `published` is the largest reading any thread has published,
        // together with the counter value it was published at.
        let published = Arc::new(AtomicI64::new(i64::MIN));
        let ticks = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut readers = Vec::new();
        for _ in 0..8 {
            let (c, published, ticks, stop) =
                (c.clone(), published.clone(), ticks.clone(), stop.clone());
            readers.push(std::thread::spawn(move || {
                let mut last_h = HonourMs(i64::MIN);
                let mut last_w = WaitMs(i64::MIN);
                while !stop.load(Ordering::Acquire) {
                    // A reading taken after another thread's published
                    // one happens-after it.
                    let floor = published.load(Ordering::SeqCst);
                    let n = c.now();
                    assert!(n.honour >= last_h && n.wait >= last_w);
                    assert!(n.honour.0 >= floor, "{} < {floor}", n.honour.0);
                    last_h = n.honour;
                    last_w = n.wait;
                    published.fetch_max(n.honour.0, Ordering::SeqCst);
                    ticks.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        // Let the readers see each state before the next change, so that
        // each step is observed (and counted) as a step.
        let observed = |ticks: &AtomicUsize| {
            let from = ticks.load(Ordering::Relaxed);
            while ticks.load(Ordering::Relaxed) < from + 64 {
                std::thread::yield_now();
            }
        };
        for i in 0..200 {
            tick(&s, 10);
            if i == 50 {
                step(&s, 3_000);
                observed(&ticks);
            }
            if i == 120 {
                step(&s, -3_000);
                observed(&ticks);
            }
            std::thread::yield_now();
        }
        stop.store(true, Ordering::Release);
        for r in readers {
            r.join().expect("a reader saw a non-monotone clock");
        }
        assert!(ticks.load(Ordering::Relaxed) > 0);
        let st = c.step_stats();
        assert_eq!((st.steps_forward, st.steps_backward), (1, 1));
    }

    /// Expectation: each conversion maps the remaining duration onto the
    /// target clock now; the sentinels survive: MAX stays MAX, 0 and MIN
    /// become 0; a conversion and its inverse at one instant round-trip.
    #[test]
    fn conversions_map_the_remaining_duration_and_keep_sentinels() {
        let now = Now {
            wall: Ms(1_000_000),
            honour: HonourMs(5_000_000),
            wait: WaitMs(3_000_000),
            ..Now::same(0)
        };
        assert_eq!(now.wall_of_wait(WaitMs(3_000_250)), Ms(1_000_250));
        assert_eq!(now.wall_of_honour(HonourMs(5_000_250)), Ms(1_000_250));
        assert_eq!(now.wait_of_wall(Ms(1_000_250)), WaitMs(3_000_250));
        assert_eq!(now.wait_of_honour(HonourMs(5_000_250)), WaitMs(3_000_250));
        // A deadline already past maps below now.
        assert_eq!(now.wall_of_wait(WaitMs(2_999_000)), Ms(999_000));
        // Round trips.
        for d in [-5_000i64, 0, 1, 40_000] {
            let w = WaitMs(now.wait.0 + d);
            assert_eq!(now.wait_of_wall(now.wall_of_wait(w)), w);
            assert_eq!(now.wait_of_wall(Ms(now.wall.0 + d)), WaitMs(now.wait.0 + d));
        }
        // Sentinels.
        assert_eq!(now.wall_of_wait(WaitMs(i64::MAX)), Ms(i64::MAX));
        assert_eq!(now.wall_of_honour(HonourMs(i64::MAX)), Ms(i64::MAX));
        assert_eq!(now.wait_of_wall(Ms(i64::MAX)), WaitMs(i64::MAX));
        assert_eq!(now.wait_of_honour(HonourMs(i64::MAX)), WaitMs(i64::MAX));
        assert_eq!(now.wall_of_wait(WaitMs(0)), Ms(0));
        assert_eq!(now.wall_of_honour(HonourMs(0)), Ms(0));
        assert_eq!(now.wait_of_wall(Ms(0)), WaitMs(0));
        assert_eq!(now.wait_of_honour(HonourMs(0)), WaitMs(0));
        assert_eq!(now.wall_of_wait(WaitMs(i64::MIN)), Ms(0));
        assert_eq!(now.wait_of_wall(Ms(i64::MIN)), WaitMs(0));
        // A remaining duration near the top saturates instead of wrapping.
        let late = Now::same(i64::MAX - 10);
        assert_eq!(late.wall_of_wait(WaitMs(i64::MAX - 1)), Ms(i64::MAX - 1));
    }

    /// Expectation: `Now::same` and `Now::plus` keep the three equal, and
    /// the newtypes' arithmetic saturates.
    #[test]
    fn now_helpers_and_newtype_arithmetic() {
        let n = Now::same(100).plus(50);
        assert_eq!(n, Now::same(150));
        assert_eq!(Now::same_on_boot(7, 9).boot.id, 9);
        assert_eq!(Ms(i64::MAX).plus(5), Ms(i64::MAX));
        assert_eq!(WaitMs(i64::MIN).minus(5), WaitMs(i64::MIN));
        assert_eq!(HonourMs(10).since(HonourMs(30)), -20);
    }

    /// Expectation: two `ProtocolClock::new` differ, `with_id` keeps its
    /// id, and no id is 0.
    #[test]
    fn ids() {
        let (s, c) = stub();
        assert_eq!(c.id(), 7);
        let a = ProtocolClock::new(s.clone(), 0);
        let b = ProtocolClock::new(s, 0);
        assert_ne!(a.id(), 0);
        assert_ne!(b.id(), 0);
        assert_ne!(a.id(), b.id());
    }

    /// Expectation: the rate budget is kept as given (up to the clamp) and
    /// scales the readings: a budgeted clock's WAIT runs slow and its HONOUR
    /// fast by the budget, an unbudgeted one's read RAW as it is.
    #[test]
    fn the_rate_budget_scales_the_readings() {
        let (s, c) = stub();
        assert_eq!(c.rate_budget_ppm(), 0);
        let budgeted = ProtocolClock::with_id(s.clone(), 8, 1_000);
        assert_eq!(budgeted.rate_budget_ppm(), 1_000);
        assert_eq!(
            ProtocolClock::new(s.clone(), 2_000).rate_budget_ppm(),
            2_000
        );
        assert_eq!(
            ProtocolClock::new(s.clone(), 9_000_000).rate_budget_ppm(),
            MAX_RATE_BUDGET_PPM
        );
        let (a, b) = (c.now(), budgeted.now());
        tick(&s, 100_000);
        let (a2, b2) = (c.now(), budgeted.now());
        assert_eq!(a2.wait.0 - a.wait.0, 100_000);
        assert_eq!(a2.honour.0 - a.honour.0, 100_000);
        // 100 s of RAW: 99 900.09 ms of WAIT, 100 100.1 ms of HONOUR.
        assert_eq!(b2.wait.0 - b.wait.0, 99_900);
        assert_eq!(b2.honour.0 - b.honour.0, 100_100);
        assert_eq!(s.boot_id(), 0);
    }

    /// Expectation: budget 0 reads exactly as C7-H1's clock did (the formula
    /// of the module doc without `rho`, plus the suspend pad of plan 43 C7b:
    /// a second for each reading at which `S` grew past the detection
    /// threshold), over random samples, steps and suspends: the identity the
    /// simulator's sweeps rely on.
    #[test]
    fn a_zero_budget_reads_exactly_as_before() {
        let (s, c) = stub();
        let mut x = 0x2545_f491_4f6c_dd1du64;
        let mut rnd = move |n: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % n
        };
        let (wall0, raw0) = (START, 7_000 * MS as i128);
        let diff0 = (5_000 - 9_000) as i128;
        let (mut susp_max, mut jump, mut suspends) = (0i128, 0i128, 0i128);
        for _ in 0..5_000 {
            let real = rnd(40_000_000_000); // ns
            let susp = if rnd(8) == 0 { rnd(30_000) } else { 0 };
            s.raw.fetch_add(real, Ordering::SeqCst);
            s.mono.fetch_add(real, Ordering::SeqCst);
            s.boot.fetch_add(real + susp * MS, Ordering::SeqCst);
            s.wall
                .fetch_add((real / MS) as i64 + susp as i64, Ordering::SeqCst);
            if rnd(6) == 0 {
                step(&s, rnd(20_000) as i64 - 8_000);
            }
            let n = c.now();
            let r = s.read();
            let e_raw_ms = (r.raw_ns as i128 - raw0) / MS as i128;
            let diff = (r.boot_ns as i128 - r.mono_ns as i128) / MS as i128;
            let before = susp_max;
            susp_max = susp_max.max(diff - diff0);
            if susp_max > before + SUSPEND_DETECT_MS as i128 {
                suspends += 1;
            }
            let e_h = e_raw_ms + susp_max + suspends * SUSPEND_PAD_MS as i128;
            jump = jump.max(r.wall_ms as i128 - wall0 as i128 - e_h);
            assert_eq!(n.wait.0 as i128, wall0 as i128 + e_raw_ms);
            assert_eq!(n.honour.0 as i128, wall0 as i128 + e_h + jump);
        }
    }

    /// Real time and an oscillator that runs at `1 + r` of it (one rate per
    /// segment), over a stub: `advance` moves every source as the host's
    /// would, and reports the real time that passed.
    struct Osc {
        s: Arc<Stub>,
        rate_ppm: i64,
    }

    impl Osc {
        /// `real` ms of real time, then `susp` ms of suspend (RAW and MONO
        /// stand still, BOOT and WALL go on), BOOT and MONO slewed by
        /// `slew_ppm` and the WALL by `wall_slew_ppm` over the first (as a
        /// time daemon does).
        fn advance(&self, real: u64, susp: u64, slew_ppm: i64, wall_slew_ppm: i64) {
            let osc = |ms: u64, ppm: i64| (ms as i128 * (1_000_000 + ppm as i128)) as u64;
            let raw = osc(real, self.rate_ppm); // ns
            let slewed = |ms: u64| osc(ms, slew_ppm);
            self.s
                .boot
                .fetch_add(slewed(real) + susp * MS, Ordering::SeqCst);
            self.s.mono.fetch_add(slewed(real), Ordering::SeqCst);
            self.s.raw.fetch_add(raw, Ordering::SeqCst);
            self.s.wall.fetch_add(
                (real as i128 * (1_000_000 + wall_slew_ppm as i128) / 1_000_000) as i64
                    + susp as i64,
                Ordering::SeqCst,
            );
        }
    }

    /// One random history, over an oscillator within 1 000 ppm of real
    /// time, on a clock budgeted `budget` ppm: every pair of
    /// consecutive readings is checked. Returns how many intervals had
    /// HONOUR short of the real elapsed time and how many had WAIT beyond
    /// it (a millisecond of rounding allowed).
    fn violations(seed: u64, budget: u32) -> (usize, usize, usize) {
        // The oscillator is always within 1 000 ppm; the clock's budget varies.
        const RHO: i64 = 1_000;
        let s = Arc::new(Stub::default());
        s.wall.store(START, Ordering::SeqCst);
        s.boot.store(5_000 * MS, Ordering::SeqCst);
        s.raw.store(7_000 * MS, Ordering::SeqCst);
        s.mono.store(9_000 * MS, Ordering::SeqCst);
        let c = ProtocolClock::with_id(s.clone(), 7, budget);
        let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let mut rnd = move |n: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % n
        };
        let mut osc = Osc {
            s: s.clone(),
            rate_ppm: 0,
        };
        let (mut honour_short, mut wait_over, mut checked) = (0, 0, 0);
        let mut prev = c.now();
        for i in 0..2_000 {
            if i % 40 == 0 {
                // A new segment: the extremes of the budget half the time.
                let b = RHO;
                osc.rate_ppm = match rnd(4) {
                    0 => -b,
                    1 => b,
                    _ => rnd(2 * b as u64 + 1) as i64 - b,
                };
            }
            let real = 1 + rnd(5_000);
            let susp = if rnd(12) == 0 { rnd(60_000) } else { 0 };
            // The slews of boot+mono and of the wall: up to 8.3 %.
            let slew = rnd(166_001) as i64 - 83_000;
            let wall_slew = rnd(166_001) as i64 - 83_000;
            osc.advance(real, susp, slew, wall_slew);
            if rnd(20) == 0 {
                step(&s, rnd(20_000) as i64 - 10_000);
            }
            let now = c.now();
            // The real time that passed (suspend counts for HONOUR).
            let (dh, dw) = (now.honour.since(prev.honour), now.wait.since(prev.wait));
            checked += 1;
            if dh + 1 < (real + susp) as i64 {
                honour_short += 1;
            }
            if dw > real as i64 + 1 {
                wait_over += 1;
            }
            prev = now;
        }
        (honour_short, wait_over, checked)
    }

    /// Expectation (P2, P3, plan 43 C7a): for an oscillator anywhere within
    /// the budget (a rate per segment of 40 readings, the extremes often),
    /// with suspends, boot and mono slewed together up to 8.3 %, the wall
    /// slewed and stepped, no pair of consecutive readings has HONOUR short
    /// of the real elapsed time or WAIT beyond it (a millisecond of
    /// rounding). Fail-before: the same histories on a clock with no budget
    /// (C7-H1's readings) violate both, so the test is not vacuous.
    #[test]
    fn honour_never_undercounts_and_wait_never_overcounts_within_the_budget() {
        let (mut short0, mut over0) = (0, 0);
        for seed in 1..=2_000u64 {
            let (short, over, n) = violations(seed, 1_000);
            assert_eq!(n, 2_000);
            assert_eq!((short, over), (0, 0), "seed {seed}");
            if seed <= 100 {
                let (s0, o0, _) = violations(seed, 0);
                short0 += s0;
                over0 += o0;
            }
        }
        assert!(
            short0 > 0 && over0 > 0,
            "the unbudgeted clock should violate: honour short {short0}, wait over {over0}"
        );
    }

    /// Plan 43 C7b (commit 7), P2 across a suspend: on a host that times its
    /// sleeps with a persistent clock of whole seconds (the x86 RTC path)
    /// BOOTTIME counts a suspend short by up to 999 ms, so `S` and with it
    /// HONOUR fell that far behind the real time that passed. With the pad
    /// (a second per detected suspend) no interval has HONOUR short of the
    /// real elapsed time plus the suspend. Returns `(intervals checked,
    /// intervals short with the pad, intervals that would be short without
    /// it: the parent's readings)`; suspends here are 2 s or more, as a
    /// shorter one is counted as nothing and not detected (the margin
    /// absorbs it).
    fn coarse_violations(seed: u64) -> (usize, usize, usize) {
        let s = Arc::new(Stub::default());
        s.wall.store(START, Ordering::SeqCst);
        s.boot.store(5_000 * MS, Ordering::SeqCst);
        s.raw.store(7_000 * MS, Ordering::SeqCst);
        s.mono.store(9_000 * MS, Ordering::SeqCst);
        let c = ProtocolClock::with_id(s.clone(), 7, 1_000);
        let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let mut rnd = move |n: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % n
        };
        let (mut checked, mut short, mut short_without) = (0, 0, 0);
        let mut prev = c.now();
        for _ in 0..1_000 {
            let real = 1 + rnd(5_000);
            let susp = if rnd(8) == 0 { 2_000 + rnd(58_000) } else { 0 };
            // The kernel counts the sleep in whole seconds, rounded down.
            let counted = susp / 1_000 * 1_000;
            s.raw.fetch_add(real * MS, Ordering::SeqCst);
            s.mono.fetch_add(real * MS, Ordering::SeqCst);
            s.boot.fetch_add((real + counted) * MS, Ordering::SeqCst);
            s.wall.fetch_add((real + susp) as i64, Ordering::SeqCst);
            let now = c.now();
            let dh = now.honour.since(prev.honour);
            checked += 1;
            if dh + 1 < (real + susp) as i64 {
                short += 1;
            }
            let pad = if susp > 0 { SUSPEND_PAD_MS } else { 0 };
            if dh - pad + 1 < (real + susp) as i64 {
                short_without += 1;
            }
            prev = now;
        }
        (checked, short, short_without)
    }

    #[test]
    fn a_suspend_counted_short_by_the_kernel_is_covered_by_the_pad() {
        let mut uncovered = 0;
        for seed in 1..=500u64 {
            let (n, short, without) = coarse_violations(seed);
            assert_eq!(n, 1_000);
            assert_eq!(short, 0, "seed {seed}");
            uncovered += without;
        }
        assert!(
            uncovered > 0,
            "without the pad some suspends would have HONOUR short: the test is vacuous"
        );
    }

    /// Expectation: `sleep_ms_for_wait(after)` of real time advances WAIT
    /// by `after` ms or more on an oscillator that runs true or fast (a
    /// slow one wakes the handler early, which re-arms), and costs the
    /// stretch only: `after * (1 + rho)` rounded up.
    #[test]
    fn sleep_ms_for_wait_reaches_the_deadline() {
        for budget in [0u32, 1_000, 2_000, 80_000] {
            for rate in [0i64, budget as i64 / 2, budget as i64] {
                for after in [1u64, 999, 20_000, 60_000] {
                    let s = Arc::new(Stub::default());
                    s.wall.store(START, Ordering::SeqCst);
                    let c = ProtocolClock::with_id(s.clone(), 7, budget);
                    let osc = Osc {
                        s: s.clone(),
                        rate_ppm: rate,
                    };
                    let before = c.wait();
                    // The sleep's real time, with a ms of timer granularity.
                    osc.advance(c.sleep_ms_for_wait(after), 0, 0, 0);
                    let got = c.wait().since(before);
                    assert!(
                        got + 1 >= after as i64,
                        "budget {budget} rate {rate} after {after}: WAIT advanced {got}"
                    );
                    if rate == budget as i64 && after >= 20_000 {
                        assert!(got >= after as i64, "{budget} {after}: {got}");
                    }
                }
            }
        }
        let c = ProtocolClock::with_id(Arc::new(Stub::default()), 7, 1_000);
        assert_eq!(c.sleep_ms_for_wait(0), 0);
        assert_eq!(c.sleep_ms_for_wait(20_000), 20_020);
        assert_eq!(c.sleep_ms_for_wait(1), 2);
        assert_eq!(c.sleep_ms_for_wait(u64::MAX), u64::MAX);
        let zero = ProtocolClock::with_id(Arc::new(Stub::default()), 7, 0);
        assert_eq!(zero.sleep_ms_for_wait(12_345), 12_345);
    }

    /// Timing, not an assertion (`cargo test --release -p constellation-types
    /// reading_cost -- --ignored --nocapture`): what one reading costs over
    /// sources that cost nothing (four atomic loads), so the scaling's cost
    /// (one `u128` multiply per elapsed term) can be compared between trees.
    #[test]
    #[ignore = "timing"]
    fn reading_cost() {
        let (s, _) = stub();
        let c = ProtocolClock::with_id(s.clone(), 9, 1_000);
        let n = 5_000_000u32;
        for round in 0..3 {
            let t = std::time::Instant::now();
            for _ in 0..n {
                tick(&s, 1);
                std::hint::black_box(c.now());
            }
            let now = t.elapsed();
            let t = std::time::Instant::now();
            for _ in 0..n {
                tick(&s, 1);
                std::hint::black_box(c.honour());
            }
            let honour = t.elapsed();
            let t = std::time::Instant::now();
            for _ in 0..n {
                tick(&s, 1);
                std::hint::black_box(c.wait());
            }
            let wait = t.elapsed();
            let per = |d: std::time::Duration| d.as_nanos() as f64 / n as f64;
            eprintln!(
                "round {round}: now() {:.1} ns, honour() {:.1} ns, wait() {:.1} ns",
                per(now),
                per(honour),
                per(wait)
            );
        }
    }

    /// A reading on boot `id`, the raw clock at `raw` ms since boot, the wait
    /// clock at `wait` (the wall far from both, so that nothing reads it by
    /// accident), the clock budgeted `ppm`.
    fn at(id: u64, raw: i64, wait: i64, ppm: u32) -> Now {
        Now {
            wall: Ms(1_000_000_000 + wait),
            honour: HonourMs(wait),
            wait: WaitMs(wait),
            boot: BootNow {
                id,
                raw_ms: raw,
                budget_ppm: ppm,
            },
        }
    }

    /// Expectation (plan 43 C7a, Appendix B row 14): a deadline stamped on
    /// a boot and read back on the same boot, by a process that started
    /// later, ends where it did, whatever the wait clock of the new process
    /// says: the time left is read on the raw clock.
    #[test]
    fn a_stamp_round_trips_on_one_boot() {
        // The writer: 20 s ahead, at raw 100 000 ms since boot.
        let w = at(5, 100_000, 7_000, 0);
        let stamp = w.stamp_of_wait(WaitMs(27_000));
        assert_eq!(stamp.span_ms, 20_000);
        assert_eq!((stamp.boot_id, stamp.raw_until_ms), (5, 120_000));
        assert_eq!(stamp.wall, w.wall_of_wait(WaitMs(27_000)));
        // The same process, 5 s on: 15 s left.
        let r = at(5, 105_000, 12_000, 0);
        assert_eq!(r.wait_of_stamp(&stamp), Some(WaitMs(27_000)));
        // A restarted process, 12 s on: its wait clock starts anywhere.
        let r = at(5, 112_000, 900_000, 0);
        assert_eq!(r.wait_of_stamp(&stamp), Some(WaitMs(908_000)));
        // Passed: nothing left.
        assert_eq!(at(5, 120_000, 1, 0).wait_of_stamp(&stamp), None);
        assert_eq!(at(5, 999_999, 1, 0).wait_of_stamp(&stamp), None);
        // A deadline already past when stamped has no span.
        assert_eq!(w.stamp_of_wait(WaitMs(6_000)).span_ms, 0);
        assert_eq!(w.wait_of_stamp(&w.stamp_of_wait(WaitMs(6_000))), None);
    }

    /// Expectation: another boot (or an unknown one, on either side) waits
    /// the recorded span from the process's start, however late the raw
    /// clock of the new boot says it is.
    #[test]
    fn a_stamp_of_another_boot_waits_its_span() {
        let stamp = at(5, 100_000, 7_000, 0).stamp_of_wait(WaitMs(27_000));
        // Rebooted: raw restarted near 0; the span, whole.
        assert_eq!(
            at(6, 3_000, 500, 0).wait_of_stamp(&stamp),
            Some(WaitMs(20_500))
        );
        // Rebooted, and the new boot's raw is past the old one's: the
        // same, not "passed".
        assert_eq!(
            at(6, 900_000, 500, 0).wait_of_stamp(&stamp),
            Some(WaitMs(20_500))
        );
        // This process does not know its boot, or the stamp has none.
        assert_eq!(
            at(0, 112_000, 500, 0).wait_of_stamp(&stamp),
            Some(WaitMs(20_500))
        );
        let unstamped = BootStamp {
            boot_id: 0,
            ..stamp
        };
        assert_eq!(
            at(5, 112_000, 500, 0).wait_of_stamp(&unstamped),
            Some(WaitMs(20_500))
        );
        // Both unknown: no match either.
        assert_eq!(
            at(0, 112_000, 500, 0).wait_of_stamp(&unstamped),
            Some(WaitMs(20_500))
        );
    }

    /// Expectation (the fail-before of the wall form): a step of the wall
    /// clock in either direction between the write and the read, while the
    /// node was up or down, moves neither the stamp nor its reading on the
    /// same boot; the wall form (`wait_of_wall` of the persisted value)
    /// moves by the whole step.
    #[test]
    fn a_stamp_ignores_wall_steps_on_one_boot() {
        let w = at(5, 100_000, 7_000, 0);
        let stamp = w.stamp_of_wait(WaitMs(27_000));
        for step in [-3_600_000i64, -1, 0, 1, 3_600_000] {
            let mut r = at(5, 108_000, 800_000, 0);
            // 8 s of real time passed, and the wall moved by `step` too.
            r.wall = Ms(w.wall.0 + 8_000 + step);
            assert_eq!(r.wait_of_stamp(&stamp), Some(WaitMs(812_000)), "{step}");
            // The wall form reads `stamp.wall` against this reading's wall.
            let wall_form = r.wait_of_wall(stamp.wall);
            assert_eq!(wall_form.0 - r.wait.0, 12_000 - step);
        }
    }

    /// Expectation: a stamp never waits more than its recorded span, whatever
    /// the raw clocks say (a raw clock that went back on one boot, or a
    /// corrupt `raw_until`).
    #[test]
    fn a_stamp_never_waits_more_than_its_span() {
        let stamp = at(5, 100_000, 7_000, 0).stamp_of_wait(WaitMs(27_000));
        // The raw clock reads earlier than at the write.
        assert_eq!(
            at(5, 40_000, 100, 0).wait_of_stamp(&stamp),
            Some(WaitMs(20_100))
        );
        let corrupt = BootStamp {
            raw_until_ms: i64::MAX,
            ..stamp
        };
        assert_eq!(
            at(5, 100_000, 100, 0).wait_of_stamp(&corrupt),
            Some(WaitMs(20_100))
        );
        // A negative span is nothing.
        let negative = BootStamp {
            span_ms: -5,
            ..stamp
        };
        assert_eq!(at(5, 100_000, 100, 0).wait_of_stamp(&negative), None);
        assert_eq!(at(6, 100_000, 100, 0).wait_of_stamp(&negative), None);
    }

    /// Expectation: a stamp maps a WAIT duration onto the raw clock and
    /// back through the budget (WAIT runs slow, so a span of 20 000 WAIT ms
    /// ends 20 020 raw ms on), and the budget in force when it is read may
    /// differ from the one it was written under (the raw reading is
    /// unscaled).
    #[test]
    fn stamps_map_through_the_budget() {
        let w = at(5, 100_000, 7_000, 1_000);
        let stamp = w.stamp_of_wait(WaitMs(27_000));
        assert_eq!(stamp.raw_until_ms, 120_020);
        // 10 010 raw ms later: 10 000 WAIT ms left.
        assert_eq!(
            at(5, 110_010, 50, 1_000).wait_of_stamp(&stamp),
            Some(WaitMs(10_050))
        );
        // Read by a clock with no budget: the raw ms are the left time,
        // capped at the span.
        assert_eq!(
            at(5, 110_010, 50, 0).wait_of_stamp(&stamp),
            Some(WaitMs(10_060))
        );
        // Written with none and read with a budget: the budget shortens the
        // left time as WAIT counts it, never lengthens it.
        let plain = at(5, 100_000, 7_000, 0).stamp_of_wait(WaitMs(27_000));
        let left = at(5, 110_000, 50, 2_000).wait_of_stamp(&plain).unwrap().0 - 50;
        assert!((9_980..=10_000).contains(&left), "{left}");
        // The round-up: a span of 1 ms maps to 2 raw ms at 1 000 ppm.
        assert_eq!(w.stamp_of_wait(WaitMs(7_001)).raw_until_ms, 100_002);
    }

    /// Expectation: a thread descheduled between the wall read and the
    /// boot read (the boot read is later than the wall value) neither
    /// raises `J` nor counts a step.
    #[test]
    fn a_preempted_sample_is_neither_a_jump_nor_a_step() {
        struct Late(Stub, AtomicUsize);
        impl ClockSources for Late {
            fn wall_ms(&self) -> i64 {
                self.0.wall_ms()
            }
            fn boot_ns(&self) -> u64 {
                // The first boot read after the wall read is 400 ms late,
                // the way a descheduled thread sees it.
                if self.1.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.0.boot.fetch_add(400 * MS, Ordering::SeqCst);
                    self.0.raw.fetch_add(400 * MS, Ordering::SeqCst);
                    self.0.mono.fetch_add(400 * MS, Ordering::SeqCst);
                    self.0.wall.fetch_add(400, Ordering::SeqCst);
                }
                self.0.boot_ns()
            }
            fn raw_ns(&self) -> u64 {
                self.0.raw_ns()
            }
            fn mono_ns(&self) -> u64 {
                self.0.mono_ns()
            }
        }
        let inner = Stub::default();
        inner.wall.store(START, Ordering::SeqCst);
        let late = Arc::new(Late(inner, AtomicUsize::new(1)));
        let c = ProtocolClock::with_id(late.clone(), 3, 0);
        late.1.store(0, Ordering::SeqCst);
        let n = c.now();
        // The wall was read 400 ms before boot and raw; the later honour
        // reading is what the elapsed terms say, not above it.
        assert_eq!(n.wall.0, START);
        assert_eq!(n.honour.0, START + 400);
        assert_eq!(n.wait.0, START + 400);
        assert_eq!(c.step_stats(), StepStats::default());
        // Subsequent readings are consistent (no `J` was raised).
        let m = c.now();
        assert_eq!(m.honour.0, m.wall.0);
    }
}
