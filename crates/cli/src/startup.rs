//! Startup phases (campaign 6 B-1 diagnostics). Node b's hung remount
//! left one line in `daemon.log` — the thread plan — and nothing to say
//! which of the dozen steps between it and the first bootstrap log line
//! never returned. Every phase of `mount` now logs when it starts (with
//! the previous phase's duration), and a watchdog thread warns, with the
//! process's thread count, whenever the current phase has run longer
//! than `CONSTELLATION_STARTUP_WARN_S` (default 30 s), repeating at that
//! interval, until [`done`] is called. A stuck startup therefore names
//! its phase in the log by itself.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

struct Current {
    phase: String,
    since: Instant,
    /// The instant of the first phase, for the "total" in [`done`].
    began: Instant,
}

static CURRENT: Mutex<Option<Current>> = Mutex::new(None);
static FINISHED: AtomicBool = AtomicBool::new(false);
static WATCHDOG: OnceLock<()> = OnceLock::new();

fn warn_after() -> Duration {
    Duration::from_secs(
        std::env::var("CONSTELLATION_STARTUP_WARN_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v: &u64| *v > 0)
            .unwrap_or(30),
    )
}

fn threads() -> Option<u64> {
    constellation_platform::native().process.thread_count()
}

/// Enter `phase`, logging the previous one's duration. The first call
/// starts the watchdog.
pub fn phase(phase: &str) {
    let now = Instant::now();
    let mut current = CURRENT.lock().unwrap_or_else(|p| p.into_inner());
    let began = match current.take() {
        Some(prev) => {
            tracing::info!(
                phase = %prev.phase,
                ms = prev.since.elapsed().as_millis() as u64,
                next = phase,
                "startup phase done"
            );
            prev.began
        }
        None => {
            tracing::info!(phase, "startup phase");
            now
        }
    };
    *current = Some(Current {
        phase: phase.to_string(),
        since: now,
        began,
    });
    drop(current);
    WATCHDOG.get_or_init(|| {
        let warn = warn_after();
        let _ = std::thread::Builder::new()
            .name("startup-watchdog".into())
            .spawn(move || watchdog(warn));
    });
}

/// Startup is complete: log the total and stop the watchdog.
pub fn done(what: &str) {
    FINISHED.store(true, Ordering::SeqCst);
    let mut current = CURRENT.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(prev) = current.take() {
        tracing::info!(
            phase = %prev.phase,
            ms = prev.since.elapsed().as_millis() as u64,
            total_ms = prev.began.elapsed().as_millis() as u64,
            "startup complete: {what}"
        );
    }
}

fn watchdog(warn: Duration) {
    let tick = warn.min(Duration::from_secs(5));
    let mut last_warned: Option<Instant> = None;
    loop {
        std::thread::sleep(tick);
        if FINISHED.load(Ordering::SeqCst) {
            return;
        }
        let current = CURRENT.lock().unwrap_or_else(|p| p.into_inner());
        let Some(cur) = current.as_ref() else {
            continue;
        };
        let elapsed = cur.since.elapsed();
        let due = elapsed >= warn && last_warned.is_none_or(|t| t.elapsed() >= warn);
        if due {
            tracing::warn!(
                phase = %cur.phase,
                elapsed_s = elapsed.as_secs(),
                threads = threads(),
                "startup is still in this phase (a daemon that never leaves it is stuck here; \
                 with a held daemon.lock see daemon_lock.rs)"
            );
            last_warned = Some(Instant::now());
        }
    }
}
