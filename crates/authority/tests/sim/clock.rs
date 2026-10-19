//! The simulation's clocks: tokio's paused `Instant`, offset from a fixed
//! unix-ms base so lease expiry (which the core computes in unix ms) is
//! driven entirely by `tokio::time::advance`/auto-advance.
//!
//! Plan 43 C7-H1: each node reads a [`ProtocolClock`] (WALL, HONOUR, WAIT)
//! over [`SimSources`], as the daemon reads the host's. The wall source is
//! the base plus the node's constant offset (plan 30 §M8's skew) plus its
//! runtime *step* (`FaultKind::ClockStep`); the three elapsed sources are
//! the paused clock's whole milliseconds (no suspend and no rate error
//! until C7a), so without a step all three readings are equal. The step is
//! per node and survives a restart (a host's clock does not reset when the
//! process does); the `ProtocolClock` does not: `NodeEnv::clock_of` builds a
//! fresh one per incarnation, whose HONOUR and WAIT start at the wall of
//! that moment (P6) and whose id is `node << 32 | incarnation`.
//!
//! `env.clock` (true time) has no offset and no step; the checkers keep
//! using it.

use constellation_authority::{HonourMs, Ms, Now, WaitMs};
use constellation_types::clock::{ClockSources, ProtocolClock};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// The simulated host's clocks for one node.
struct SimSources {
    base: tokio::time::Instant,
    base_ms: i64,
    /// Plan 30 §M8: this node's clock error (ms, constant).
    offset_ms: i64,
    /// The node's cumulative wall step (ms; moves WALL only).
    step: Arc<AtomicI64>,
}

impl SimSources {
    fn elapsed_ms(&self) -> i64 {
        self.base.elapsed().as_millis() as i64
    }
}

impl ClockSources for SimSources {
    fn wall_ms(&self) -> i64 {
        self.base_ms + self.offset_ms + self.step.load(Ordering::SeqCst) + self.elapsed_ms()
    }

    fn boot_ns(&self) -> u64 {
        self.elapsed_ms() as u64 * 1_000_000
    }

    fn raw_ns(&self) -> u64 {
        self.elapsed_ms() as u64 * 1_000_000
    }

    fn mono_ns(&self) -> u64 {
        self.elapsed_ms() as u64 * 1_000_000
    }
}

#[derive(Clone)]
pub struct Clock {
    base: tokio::time::Instant,
    base_ms: i64,
    pc: Arc<ProtocolClock>,
    /// The true elapsed ms and the WAIT reading at this clock's creation:
    /// the map from a `Now` back to the runtime's `Instant` it was read at.
    created: (i64, i64),
}

impl Clock {
    /// True time: no offset, no step.
    pub fn start() -> Self {
        let base = tokio::time::Instant::now();
        // A fixed, plausible epoch so log lines read like production.
        let base_ms = 1_780_000_000_000;
        Self::over(base, base_ms, 0, Arc::new(AtomicI64::new(0)), 1)
    }

    fn over(
        base: tokio::time::Instant,
        base_ms: i64,
        offset_ms: i64,
        step: Arc<AtomicI64>,
        id: u64,
    ) -> Self {
        let sources = SimSources {
            base,
            base_ms,
            offset_ms,
            step,
        };
        let pc = Arc::new(ProtocolClock::with_id(Arc::new(sources), id));
        let created = (base.elapsed().as_millis() as i64, pc.wait().0);
        Self {
            base,
            base_ms,
            pc,
            created,
        }
    }

    /// A node incarnation's clock over true time: `offset_ms` off (a node
    /// whose clock disagrees), stepped by `step` (shared with the node's
    /// next incarnation), named `id`. A new `ProtocolClock`: its HONOUR and
    /// WAIT start at the node's wall of this moment.
    pub fn for_node(&self, offset_ms: i64, step: Arc<AtomicI64>, id: u64) -> Self {
        Self::over(self.base, self.base_ms, offset_ms, step, id)
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

    /// (Read by the wait sites of plan 43 C7-H1's later commits.)
    #[allow(dead_code)]
    pub fn wait(&self) -> WaitMs {
        self.pc.wait()
    }

    /// The `ProtocolClock::id` the node's stamps carry.
    pub fn id(&self) -> u64 {
        self.pc.id()
    }

    /// The steps this clock has seen (observability).
    pub fn step_stats(&self) -> constellation_types::clock::StepStats {
        self.pc.step_stats()
    }

    /// The runtime `Instant` at which `now` (a reading of this clock) was
    /// taken: relative timers count from it (`SetTimer { after_ms }`).
    pub fn instant_of(&self, now: Now) -> tokio::time::Instant {
        let (elapsed0, wait0) = self.created;
        let elapsed = (elapsed0 + (now.wait.0 - wait0)).max(0) as u64;
        self.base + std::time::Duration::from_millis(elapsed)
    }

    /// True elapsed time.
    pub fn elapsed_ms(&self) -> u64 {
        self.base.elapsed().as_millis() as u64
    }
}
