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
//! sample and computes
//!
//! ```text
//! e_raw = raw - raw0                       // real elapsed (unslewed)
//! S     = running max of (boot - mono) - (boot0 - mono0)   // time suspended
//! e_h   = e_raw + S
//! gap   = wall - wall0 - e_h               // how far wall is ahead of e_h
//! J     = running max of gap, at least 0
//! HONOUR = wall0 + e_h + J
//!
//! WAIT  = wall0 + e_raw
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
//!   `h2 - h1` is at least the real elapsed time, up to the oscillator's
//!   error (C7a's rho): it is RAW's delta (not slewed) plus the suspend
//!   term `S` and `J`, which only add (per interval, whatever the clocks'
//!   absolute values: `S` is a difference of two clocks slewed together).
//!   So `sent + ttl - margin` on HONOUR is never later in real time than
//!   the same window measured in real time.
//! - **P3, WAIT never over-counts.** `w2 - w1` is RAW's delta: at most the
//!   real elapsed time, up to rho, and not slewed. It does not count
//!   suspend, which is conservative for a wait.
//! - **P4, steps.** A backward WALL step changes neither HONOUR nor WAIT.
//!   A forward step of `D` advances HONOUR by `D` once, at the first read
//!   after it (that read raises `jump`) and leaves WAIT alone. A forward
//!   step that only undoes an earlier backward one does not advance
//!   HONOUR: `gap` returns to a value `jump` has already seen.
//! - **P5, order.** `HONOUR >= WALL` always (`J >= gap`) and
//!   `HONOUR >= WAIT` always. WAIT and WALL are not ordered after a step.
//! - **P6, no common origin across incarnations.** A new process starts
//!   all three at WALL. HONOUR or WAIT values from another process, or
//!   another node, mean nothing on this one; [`ProtocolClock::id`] names
//!   the clock a stamp belongs to.
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

/// The three readings of one instant on one node, taken from one sample.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Now {
    pub wall: Ms,
    pub honour: HonourMs,
    pub wait: WaitMs,
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
        Now {
            wall: Ms(ms),
            honour: HonourMs(ms),
            wait: WaitMs(ms),
        }
    }

    /// Advance all three by `ms` (time passing; no step).
    pub fn plus(self, ms: u64) -> Now {
        Now {
            wall: self.wall.plus(ms),
            honour: self.honour.plus(ms),
            wait: self.wait.plus(ms),
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
    /// span). 0: unknown, which no stamp matches. Nothing reads it before
    /// C7a's boot-stamped horizons; the simulator models it for them.
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
    /// The oscillator's rate budget, ppm (plan 43 C7a). Stored here by
    /// commit 1 of C7a; the readings do not use it yet.
    budget_ppm: u32,
    wall0: i64,
    boot0: u64,
    raw0: u64,
    mono0: u64,
    /// The highest forward excess seen (the module doc's `J`).
    jump: AtomicI64,
    /// The most time suspended seen, ms (the module doc's `S`).
    suspend: AtomicI64,
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

fn ns_to_ms(ns: u64) -> i64 {
    (ns / 1_000_000).min(i64::MAX as u64) as i64
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
        let s = sources.read();
        Self {
            id,
            budget_ppm,
            wall0: s.wall_ms,
            boot0: s.boot_ns,
            raw0: s.raw_ns,
            mono0: s.mono_ns,
            jump: AtomicI64::new(0),
            suspend: AtomicI64::new(0),
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

    /// All three readings from one sample.
    pub fn now(&self) -> Now {
        let s = self.sources.read();
        self.detect_step(s.wall_ms, s.boot_ns);
        Now {
            wall: Ms(s.wall_ms),
            honour: self.honour_of(s.wall_ms, s.boot_ns, s.mono_ns, s.raw_ns),
            wait: self.wait_of(s.raw_ns),
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
        let seen_s = self.suspend.load(Ordering::Acquire);
        let susp = if s_now > seen_s {
            self.suspend.fetch_max(s_now, Ordering::AcqRel).max(s_now)
        } else {
            seen_s
        };
        let e_h = ns_to_ms(raw.saturating_sub(self.raw0)).saturating_add(susp);
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
        WaitMs(
            self.wall0
                .saturating_add(ns_to_ms(raw.saturating_sub(self.raw0))),
        )
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
        assert_eq!(c.now(), Now::same(START));
        tick(&s, 1_234);
        assert_eq!(c.now(), Now::same(START + 1_234));
        assert_eq!(c.step_stats(), StepStats::default());
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
    /// wall advances 10 s) advances HONOUR by 10 s and WAIT by nothing.
    #[test]
    fn a_suspend_counts_for_honour_and_not_for_wait() {
        let (s, c) = stub();
        let before = c.now();
        s.wall.fetch_add(10_000, Ordering::SeqCst);
        s.boot.fetch_add(10_000 * MS, Ordering::SeqCst);
        let after = c.now();
        assert_eq!(after.honour.since(before.honour), 10_000);
        assert_eq!(after.wait.since(before.wait), 0);
        // A suspend moves wall and boot together: not a step.
        assert_eq!(c.step_stats(), StepStats::default());
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
        assert_eq!(n1.honour.since(n0.honour), 30_000);
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

    /// Expectation: the rate budget is kept as given and, in this commit,
    /// changes no reading: a budgeted clock reads exactly as an unbudgeted
    /// one (plan 43 C7a commit 1; the readings use it from commit 2).
    #[test]
    fn the_rate_budget_is_stored_and_not_yet_read() {
        let (s, c) = stub();
        assert_eq!(c.rate_budget_ppm(), 0);
        let budgeted = ProtocolClock::with_id(s.clone(), 8, 1_000);
        assert_eq!(budgeted.rate_budget_ppm(), 1_000);
        assert_eq!(
            ProtocolClock::new(s.clone(), 2_000).rate_budget_ppm(),
            2_000
        );
        let (a, b) = (c.now(), budgeted.now());
        tick(&s, 5_000);
        let (a2, b2) = (c.now(), budgeted.now());
        assert_eq!(a2.wait.0 - a.wait.0, b2.wait.0 - b.wait.0);
        assert_eq!(a2.honour.0 - a.honour.0, b2.honour.0 - b.honour.0);
        assert_eq!(s.boot_id(), 0);
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
