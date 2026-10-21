//! Test-only fault points of the FUSE adapter (`CONSTELLATION_FAULT_*`,
//! as the engine's `fault` module): unset, each is one cached
//! environment lookup and changes nothing.

use std::sync::OnceLock;
use std::time::Duration;

/// Plan 42 K1: `CONSTELLATION_FAULT_FUSE_WRITE_DELAY_US=N`: the adapter's
/// `write` sleeps `N` µs before anything else — before it hands the
/// request on — as a worker preempted between reading a `WRITE` and
/// serving it: the part of race 1's window the daemon cannot see.
pub(crate) fn write_delay() -> Option<Duration> {
    static DELAY: OnceLock<Option<Duration>> = OnceLock::new();
    *DELAY.get_or_init(|| {
        std::env::var("CONSTELLATION_FAULT_FUSE_WRITE_DELAY_US")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|us| *us > 0)
            .map(Duration::from_micros)
    })
}
