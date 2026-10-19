//! The host's clock sources and the process-wide protocol clock (plan 43
//! C7-H1).
//!
//! [`constellation_types::clock::ProtocolClock`] holds the logic (the
//! HONOUR, WAIT and WALL readings and their properties); this module
//! supplies what it reads on each OS and the one instance every daemon
//! thread shares, [`protocol()`].
//!
//! # Sources
//!
//! The protocol clock reads WAIT from `raw` alone and HONOUR from `raw`
//! plus the time suspended, `boot - mono`: the two must be slewed together
//! (or neither slewed) so that a slew cancels in their difference.
//!
//! | OS | boot (counts suspend) | raw (rate source) | mono (does not count suspend) |
//! |---|---|---|---|
//! | Linux, Android | `CLOCK_BOOTTIME` | `CLOCK_MONOTONIC_RAW` (never slewed) | `CLOCK_MONOTONIC` (slewed with BOOTTIME) |
//! | macOS, iOS | `CLOCK_MONOTONIC_RAW` (counts sleep: `mach_continuous_time`) | `CLOCK_UPTIME_RAW` | `CLOCK_UPTIME_RAW` (neither is slewed) |
//! | FreeBSD | `CLOCK_MONOTONIC` | `CLOCK_UPTIME` | `CLOCK_UPTIME` |
//! | Windows, others | one `std::time::Instant` for all three | | |
//!
//! FreeBSD's `CLOCK_MONOTONIC` and `CLOCK_UPTIME` are the same clock as far
//! as is known (from the man page; not run on a FreeBSD host): no suspend is
//! counted and the rate source is slewed, so a suspended holder may honour a
//! window longer by the suspend and WAIT may over-count by the slew. On the
//! last row too, suspend is not counted (the same as every `Instant` user
//! before C7-H1), and the wall (`SystemTime`, slewed by the host's time
//! service) drifts against the `Instant`, so the clock's step detection
//! counts the drift as steps now and then (observability only). C7a
//! revisits these. The wall clock is `SystemTime` plus the fault terms
//! everywhere.
//!
//! # Fault hooks (testing only)
//!
//! Only WALL moves, so HONOUR follows a forward step and ignores a backward
//! one, exactly as for a real step:
//!
//! - `CONSTELLATION_FAULT_CLOCK_OFFSET_MS=<ms>` (load campaign): a constant
//!   offset, read once. One daemon's clock runs ahead of or behind its
//!   peers' by up to what the margins assume (`cto-modes.md`, "Clocks and
//!   margins": ±500 ms at the defaults). It moves this clock and what
//!   reads it (the engine's epoch promises, the snapshot paths, the
//!   scheduler and the prune audit's `ts`); the registry's and commits'
//!   timestamps, the S3 objects' own and the log lines keep the host's
//!   clock, so one daemon mixes the two by the offset (within the margins
//!   the design tolerates across nodes anyway). The engine logs it once at
//!   start-up (this crate has no logger).
//! - [`set_fault_step_ms`]: a runtime step, cumulative, set by the engine's
//!   `CONSTELLATION_FAULT_CLOCK_STEP_FILE` poller (the harness's
//!   `clock:step:<ms>:<at_ms>[@node]` load item) and by engine tests.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};

use constellation_types::clock::{ClockSources, ProtocolClock};

static FAULT_STEP_MS: AtomicI64 = AtomicI64::new(0);

/// The constant wall offset the load campaign asks for
/// (`CONSTELLATION_FAULT_CLOCK_OFFSET_MS`), read once; 0 when unset.
pub fn fault_offset_ms() -> i64 {
    static OFFSET: OnceLock<i64> = OnceLock::new();
    *OFFSET.get_or_init(|| {
        std::env::var("CONSTELLATION_FAULT_CLOCK_OFFSET_MS")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .unwrap_or(0)
    })
}

/// Set the cumulative runtime step of the wall clock (testing only). Moves
/// WALL alone; the protocol clock's HONOUR follows a forward step once and
/// WAIT never moves.
pub fn set_fault_step_ms(ms: i64) {
    FAULT_STEP_MS.store(ms, Ordering::SeqCst);
}

/// The cumulative runtime step set so far.
pub fn fault_step_ms() -> i64 {
    FAULT_STEP_MS.load(Ordering::SeqCst)
}

/// The three elapsed clocks of the host, in ns, per OS.
#[cfg(all(
    unix,
    any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd"
    )
))]
mod elapsed {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    const BOOT: libc::clockid_t = libc::CLOCK_BOOTTIME;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    const RAW: libc::clockid_t = libc::CLOCK_MONOTONIC_RAW;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    const MONO: libc::clockid_t = libc::CLOCK_MONOTONIC;

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    const BOOT: libc::clockid_t = libc::CLOCK_MONOTONIC_RAW;
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    const RAW: libc::clockid_t = libc::CLOCK_UPTIME_RAW;
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    const MONO: libc::clockid_t = libc::CLOCK_UPTIME_RAW;

    #[cfg(target_os = "freebsd")]
    const BOOT: libc::clockid_t = libc::CLOCK_MONOTONIC;
    #[cfg(target_os = "freebsd")]
    const RAW: libc::clockid_t = libc::CLOCK_UPTIME;
    #[cfg(target_os = "freebsd")]
    const MONO: libc::clockid_t = libc::CLOCK_UPTIME;

    /// `clock_gettime(id)` in ns; 0 if the call fails (never on these ids).
    #[inline]
    fn gettime_ns(id: libc::clockid_t) -> u64 {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `ts` is a valid, writable timespec for the call.
        let rc = unsafe { libc::clock_gettime(id, &mut ts) };
        if rc != 0 {
            return 0;
        }
        (ts.tv_sec as u64)
            .saturating_mul(1_000_000_000)
            .saturating_add(ts.tv_nsec as u64)
    }

    #[inline]
    pub fn boot_ns() -> u64 {
        gettime_ns(BOOT)
    }
    #[inline]
    pub fn raw_ns() -> u64 {
        gettime_ns(RAW)
    }
    #[inline]
    pub fn mono_ns() -> u64 {
        gettime_ns(MONO)
    }
}

/// Windows and hosts without a verified source table: one `Instant` for
/// all three (suspend is not counted; see the module doc).
#[cfg(not(all(
    unix,
    any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd"
    )
)))]
mod elapsed {
    use std::sync::OnceLock;
    use std::time::Instant;

    fn ns() -> u64 {
        static BASE: OnceLock<Instant> = OnceLock::new();
        // A constant head start: no clock reads 0 at its first reading.
        1_000_000_000
            + BASE
                .get_or_init(Instant::now)
                .elapsed()
                .as_nanos()
                .min(u64::MAX as u128 / 2) as u64
    }
    pub fn boot_ns() -> u64 {
        ns()
    }
    pub fn raw_ns() -> u64 {
        ns()
    }
    pub fn mono_ns() -> u64 {
        ns()
    }
}

/// The host's clocks (see the module doc for the table).
#[derive(Clone, Copy, Debug, Default)]
pub struct HostSources;

impl HostSources {
    pub fn new() -> Self {
        Self
    }
}

impl ClockSources for HostSources {
    #[inline]
    fn wall_ms(&self) -> i64 {
        wall_ms()
    }
    #[inline]
    fn boot_ns(&self) -> u64 {
        elapsed::boot_ns()
    }
    #[inline]
    fn raw_ns(&self) -> u64 {
        elapsed::raw_ns()
    }
    #[inline]
    fn mono_ns(&self) -> u64 {
        elapsed::mono_ns()
    }
}

/// `SystemTime` plus the fault terms, in unix ms.
#[inline]
fn wall_ms() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    now + fault_offset_ms() + FAULT_STEP_MS.load(Ordering::Relaxed)
}

/// The process's protocol clock, built over the host's sources on first
/// use: every protocol timestamp of this process (the authority core's
/// `Now`, the FUSE threads' readings, the heartbeat and lane threads')
/// comes from it, so a stamp and its comparison share a clock.
///
/// A daemon restarted under a clock-step fault starts stepped: the
/// cumulative step in `CONSTELLATION_FAULT_CLOCK_STEP_FILE` is read once,
/// here, before the first reading, so the new incarnation's origin is the
/// stepped wall (the host clock does not reset when the process does) and it
/// does not see the whole step as a new one.
pub fn protocol() -> &'static ProtocolClock {
    static CLOCK: OnceLock<ProtocolClock> = OnceLock::new();
    CLOCK.get_or_init(|| {
        if let Some(step) = std::env::var_os("CONSTELLATION_FAULT_CLOCK_STEP_FILE")
            .and_then(|p| read_step_file(std::path::Path::new(&p)))
        {
            set_fault_step_ms(step);
        }
        ProtocolClock::new(Arc::new(HostSources::new()))
    })
}

/// The cumulative step a step file holds (a signed integer in ms); `None`
/// for a missing, unreadable or malformed file.
pub fn read_step_file(path: &std::path::Path) -> Option<i64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Expectation: on the host every source advances, and the boot clock
    /// (which counts suspend) never reads behind the monotonic one.
    #[test]
    fn every_source_advances_and_boot_is_not_behind_mono() {
        let h = HostSources::new();
        let a = h.read();
        std::thread::sleep(std::time::Duration::from_millis(30));
        let b = h.read();
        assert!(b.wall_ms - a.wall_ms >= 20, "{a:?} {b:?}");
        assert!(b.boot_ns - a.boot_ns >= 20_000_000, "{a:?} {b:?}");
        assert!(b.raw_ns - a.raw_ns >= 20_000_000, "{a:?} {b:?}");
        assert!(b.mono_ns - a.mono_ns >= 20_000_000, "{a:?} {b:?}");
        // Boot counts what mono does not (suspend), never less. Compare
        // elapsed time since an origin, not the absolute readings.
        assert!(b.boot_ns - a.boot_ns >= b.mono_ns - a.mono_ns - 5_000_000);
    }

    /// Expectation: the runtime step moves the wall reading by the
    /// cumulative value and nothing else; HONOUR follows a forward step
    /// and WAIT does not. This is the only test in the crate that sets
    /// the (process-wide) step, so it can run alongside the others: they
    /// read no wall value.
    #[test]
    fn set_fault_step_moves_wall_only() {
        let c = protocol();
        let before = c.now();
        let wait_before = before.wait;
        set_fault_step_ms(4_000);
        let after = c.now();
        assert!(
            after.wall.0 - before.wall.0 >= 4_000,
            "{before:?} {after:?}"
        );
        assert!(
            after.honour.0 - before.honour.0 >= 4_000,
            "{before:?} {after:?}"
        );
        assert!(after.wait.0 - wait_before.0 < 1_000, "{before:?} {after:?}");
        // Back to zero: the wall returns; HONOUR stays ahead; WAIT is
        // still a plain elapsed time.
        set_fault_step_ms(0);
        let back = c.now();
        assert!(back.wall.0 - after.wall.0 < 1_000);
        assert!(back.honour.0 >= after.honour.0);
        assert!(back.honour.0 - back.wall.0 >= 3_000, "{back:?}");
        assert!(back.wait.0 - wait_before.0 < 1_000);
        assert_eq!(fault_step_ms(), 0);
        let st = c.step_stats();
        assert!(st.steps_forward >= 1 && st.steps_backward >= 1, "{st:?}");
    }

    /// The cost of a reading, in ns per call (plan 43 C7-H1; `cargo test
    /// --release -p constellation-platform clock_read_costs -- --ignored
    /// --nocapture`). `old_wall` is what `now_unix_ms()` was before: a
    /// `SystemTime` read and a cached offset. Nothing is asserted: the
    /// numbers are the point (the FUSE path pays `wall`, the locks and the
    /// core's step pay `now`).
    #[test]
    #[ignore]
    fn clock_read_costs() {
        use std::hint::black_box;
        fn per_call(name: &str, mut f: impl FnMut() -> i64) {
            const N: u32 = 5_000_000;
            let mut best = f64::MAX;
            for _ in 0..5 {
                let t = std::time::Instant::now();
                let mut acc = 0i64;
                for _ in 0..N {
                    acc = acc.wrapping_add(black_box(f()));
                }
                black_box(acc);
                best = best.min(t.elapsed().as_nanos() as f64 / N as f64);
            }
            println!("{name:>12}: {best:6.1} ns/call");
        }
        let c = protocol();
        c.now();
        per_call("old_wall", || {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            now + black_box(fault_offset_ms())
        });
        per_call("wall", || c.wall().0);
        per_call("wait", || c.wait().0);
        per_call("honour", || c.honour().0);
        per_call("now", || c.now().honour.0);
        per_call("raw_only", || elapsed::raw_ns() as i64);
    }

    /// Expectation: a step file is a signed integer; anything else is no step.
    #[test]
    fn the_step_file_is_read_as_a_signed_integer() {
        let d = tempfile::tempdir().unwrap();
        let f = d.path().join("step");
        assert_eq!(read_step_file(&f), None);
        std::fs::write(&f, "-3000\n").unwrap();
        assert_eq!(read_step_file(&f), Some(-3_000));
        std::fs::write(&f, "+2500").unwrap();
        assert_eq!(read_step_file(&f), Some(2_500));
        for bad in ["", "soon", "1.5"] {
            std::fs::write(&f, bad).unwrap();
            assert_eq!(read_step_file(&f), None, "{bad:?}");
        }
    }

    /// Expectation: `protocol()` is one instance.
    #[test]
    fn protocol_is_a_singleton() {
        assert!(std::ptr::eq(protocol(), protocol()));
        assert_ne!(protocol().id(), 0);
    }
}
