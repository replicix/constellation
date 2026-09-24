//! The simulation's logical clock: tokio's paused `Instant`, offset from a
//! fixed unix-ms base so lease expiry (which the core computes in unix
//! ms) is driven entirely by `tokio::time::advance`/auto-advance.

use constellation_authority::Ms;
use std::time::Duration;

#[derive(Clone, Copy)]
pub struct Clock {
    base: tokio::time::Instant,
    base_ms: i64,
    /// Plan 30 §M8: this node's clock error (ms, constant): its `now` is
    /// real time plus this. Leases and read delegations are judged on it.
    offset_ms: i64,
}

impl Clock {
    pub fn start() -> Self {
        Self {
            base: tokio::time::Instant::now(),
            // A fixed, plausible epoch so log lines read like production.
            base_ms: 1_780_000_000_000,
            offset_ms: 0,
        }
    }

    /// The same clock, `offset_ms` off (a node whose clock disagrees).
    pub fn skewed(&self, offset_ms: i64) -> Self {
        Self { offset_ms, ..*self }
    }

    pub fn now(&self) -> Ms {
        Ms(self.base_ms + self.offset_ms + self.base.elapsed().as_millis() as i64)
    }

    pub fn at(&self, at: Ms) -> tokio::time::Instant {
        let delta = (at.0 - self.offset_ms - self.base_ms).max(0) as u64;
        self.base + Duration::from_millis(delta)
    }

    pub fn elapsed_ms(&self) -> u64 {
        self.base.elapsed().as_millis() as u64
    }
}
