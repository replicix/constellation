//! Test-only fault points (`CONSTELLATION_FAULT_*`), for harness scenarios
//! that need a failure at a precise point rather than a race against the
//! daemon's own timing (plan 30 §M4 round 2, `poison-record-isolation`).
//! Unset, each is one cached environment lookup and changes nothing.

use constellation_fs_core::ChunkHash;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// `CONSTELLATION_FAULT_LOSE_CHUNKS=<hex>[,<hex>...]`: the upload pass
/// deletes these chunks from the local cache right before reading them,
/// so a pending upload of one finds its content gone — the plan 29 M6
/// "missing from local cache" condition, at exactly the point the upload
/// pass would notice it, instead of a harness deleting the cache file and
/// racing the upload round that may already hold the bytes.
pub fn lose_chunk(hash: &ChunkHash) -> bool {
    static LOST: OnceLock<HashSet<ChunkHash>> = OnceLock::new();
    LOST.get_or_init(|| {
        std::env::var("CONSTELLATION_FAULT_LOSE_CHUNKS")
            .ok()
            .map(|v| {
                v.split(',')
                    .filter_map(|h| ChunkHash::from_hex(h.trim()))
                    .collect()
            })
            .unwrap_or_default()
    })
    .contains(hash)
}

/// `CONSTELLATION_FAULT_HOLD_SYNC_FILE=<path>`: while that file exists,
/// every managed sync round returns at once — nothing is uploaded, shipped
/// or published — and the round writes `<path>.held` to say a round has
/// observed the hold, so the harness knows no round is still in flight
/// when it goes on (the sync task runs its rounds one at a time).
pub fn sync_held() -> bool {
    static PATH: OnceLock<Option<PathBuf>> = OnceLock::new();
    let Some(path) = PATH
        .get_or_init(|| std::env::var_os("CONSTELLATION_FAULT_HOLD_SYNC_FILE").map(PathBuf::from))
    else {
        return false;
    };
    if !path.exists() {
        return false;
    }
    let mut marker = path.clone().into_os_string();
    marker.push(".held");
    let _ = std::fs::write(PathBuf::from(marker), b"held");
    true
}

/// `CONSTELLATION_FAULT_HOLD_DRAIN_FILE=<path>`: while that file exists,
/// a clean shutdown's drain waits before it uploads or ships anything,
/// and writes `<path>.held` once it does — a long clean-unmount drain
/// (a large write-back queue on a slow bucket) for as long as a test
/// needs, so it can show what `umount` and a `mount` issued meanwhile do
/// while the daemon drains.
pub fn drain_held() -> bool {
    static PATH: OnceLock<Option<PathBuf>> = OnceLock::new();
    let Some(path) = PATH
        .get_or_init(|| std::env::var_os("CONSTELLATION_FAULT_HOLD_DRAIN_FILE").map(PathBuf::from))
    else {
        return false;
    };
    if !path.exists() {
        return false;
    }
    let mut marker = path.clone().into_os_string();
    marker.push(".held");
    let _ = std::fs::write(PathBuf::from(marker), b"held");
    true
}

/// `CONSTELLATION_FAULT_INBOX_POLL_PAUSE_FILE=<path>`: while that file
/// exists, every inbox GET-next this node makes as holder answers "no new
/// batch" without asking S3 — a holder that has not got round to a
/// requester's batch yet, for as long as the harness needs (the
/// `inbox-withdraw-hole` scenario's withdrawal of a batch nobody read).
pub fn inbox_polls_paused() -> bool {
    static PATH: OnceLock<Option<PathBuf>> = OnceLock::new();
    PATH.get_or_init(|| {
        std::env::var_os("CONSTELLATION_FAULT_INBOX_POLL_PAUSE_FILE").map(PathBuf::from)
    })
    .as_ref()
    .is_some_and(|p| p.exists())
}

/// `CONSTELLATION_FAULT_P2P_DENY_FILE=<path>`: while that file exists,
/// every backup append and pre-S3 stream batch to or from the node ids
/// it lists (one per line) is dropped on this node — a partition between
/// a holder and its backup that leaves S3 reachable to both (plan 30
/// §M9's `backup-partition` scenario). Read on every use so the harness
/// can start and end the partition mid-run.
pub fn p2p_denied(peer: u64) -> bool {
    static PATH: OnceLock<Option<PathBuf>> = OnceLock::new();
    let Some(path) = PATH
        .get_or_init(|| std::env::var_os("CONSTELLATION_FAULT_P2P_DENY_FILE").map(PathBuf::from))
    else {
        return false;
    };
    let Ok(text) = std::fs::read_to_string(path) else {
        return false;
    };
    text.lines()
        .filter_map(|l| l.trim().parse::<u64>().ok())
        .any(|id| id == peer)
}

/// `CONSTELLATION_FAULT_STEP_PAUSE_FILE=<path>`: once that file exists,
/// the next authority-core step stops the whole daemon (`SIGSTOP` to its
/// own pid) between `Core::handle` and the dispatch of the step's actions:
/// a step decided before a stall and dispatched after it, the production
/// twin of the deterministic simulation's `StallStep`. The file is
/// consumed (removed) first and `<path>.stopped` is written *before* the
/// stop, so each creation stops one step and the marker means "stopped,
/// or about to be": whoever created the file waits for the stop once it
/// sees the marker, and sends the `SIGCONT`. Unset, one cached lookup
/// per step; set, one `unlink(2)` per step.
pub fn step_pause() {
    static PATH: OnceLock<Option<PathBuf>> = OnceLock::new();
    let Some(path) = PATH
        .get_or_init(|| std::env::var_os("CONSTELLATION_FAULT_STEP_PAUSE_FILE").map(PathBuf::from))
    else {
        return;
    };
    // Only the step that removes the file stops: two racing steps (there
    // is one core, but a test may share the path) stop once.
    if std::fs::remove_file(path).is_err() {
        return;
    }
    let mut marker = path.clone().into_os_string();
    marker.push(".stopped");
    let _ = std::fs::write(PathBuf::from(marker), b"stopped");
    tracing::warn!("fault injection: stopping the daemon mid-step (testing only)");
    if let Err(error) = constellation_platform::native()
        .process
        .suspend(std::process::id())
    {
        tracing::warn!(%error, "fault injection: could not stop the daemon mid-step");
    }
}

/// `CONSTELLATION_FAULT_RUNTIME_HOG=<n>:<ms>[:<gap_ms>]`: `n` tasks on the
/// daemon's tokio runtime that each block their worker thread
/// (`std::thread::sleep(ms)`, deliberately not an `await`) in a loop,
/// yielding (and, with `gap_ms`, sleeping asynchronously that long)
/// between blocks — a runtime starved of workers while the process
/// itself has CPU, which is what a busy host does to the timers and the
/// heartbeat tasks that share the runtime. Started once per process.
pub fn start_runtime_hog(rt: &tokio::runtime::Handle) {
    static STARTED: OnceLock<()> = OnceLock::new();
    let Some((n, block, gap)) = std::env::var("CONSTELLATION_FAULT_RUNTIME_HOG")
        .ok()
        .and_then(|v| parse_runtime_hog(&v))
    else {
        return;
    };
    if STARTED.set(()).is_err() {
        return;
    }
    tracing::warn!(
        tasks = n,
        block_ms = block.as_millis() as u64,
        gap_ms = gap.as_millis() as u64,
        "fault injection: hogging the runtime's workers (testing only)"
    );
    for _ in 0..n {
        rt.spawn(async move {
            loop {
                std::thread::sleep(block);
                if gap.is_zero() {
                    tokio::task::yield_now().await;
                } else {
                    tokio::time::sleep(gap).await;
                }
            }
        });
    }
}

/// `<n>:<ms>[:<gap_ms>]` of [`start_runtime_hog`].
fn parse_runtime_hog(v: &str) -> Option<(usize, std::time::Duration, std::time::Duration)> {
    let mut it = v.trim().split(':');
    let n: usize = it.next()?.trim().parse().ok()?;
    let ms: u64 = it.next()?.trim().parse().ok()?;
    let gap: u64 = match it.next() {
        Some(g) => g.trim().parse().ok()?,
        None => 0,
    };
    if it.next().is_some() || n == 0 || ms == 0 {
        return None;
    }
    Some((
        n,
        std::time::Duration::from_millis(ms),
        std::time::Duration::from_millis(gap),
    ))
}

/// `CONSTELLATION_FAULT_RUNTIME_HOG_FILE=<path>`: while that file exists
/// and reads `<tasks>:<block_ms>:<every_ms>[:<node>,...]`, every
/// `every_ms` the daemon (only the listed nodes, when a list is given)
/// spawns `tasks` tasks on its shared runtime that each block their
/// worker thread for `block_ms` (`std::thread::sleep`, deliberately not
/// `block_in_place`): with `tasks` at least the worker count, the
/// runtime runs nothing else for `block_ms` while the process, its other
/// threads and the host are fine — a runtime starved by blocking calls.
/// Unlike [`start_runtime_hog`] (`CONSTELLATION_FAULT_RUNTIME_HOG`, for
/// the whole run and every node) it can be aimed at one node — the
/// lease holder, its backup — and started and stopped mid-run: the file
/// is read every 200 ms. Started once per process
/// ([`start_runtime_hog_file`], from the authority driver).
fn runtime_hog_file(node: u64) -> Option<(usize, u64, u64)> {
    static PATH: OnceLock<Option<PathBuf>> = OnceLock::new();
    let path = PATH
        .get_or_init(|| std::env::var_os("CONSTELLATION_FAULT_RUNTIME_HOG_FILE").map(PathBuf::from))
        .as_ref()?;
    let text = std::fs::read_to_string(path).ok()?;
    let mut fields = text.trim().split(':');
    let mut num = || fields.next().and_then(|v| v.trim().parse::<u64>().ok());
    let (tasks, block, every) = (num()?, num()?, num()?);
    if let Some(nodes) = fields.next() {
        if !nodes
            .split(',')
            .filter_map(|n| n.trim().parse::<u64>().ok())
            .any(|n| n == node)
        {
            return None;
        }
    }
    Some((tasks.clamp(1, 256) as usize, block, every.max(1)))
}

/// Start [`runtime_hog_file`]'s driver for node `node` on the current runtime,
/// once per process; a no-op without `CONSTELLATION_FAULT_RUNTIME_HOG_FILE`.
pub fn start_runtime_hog_file(node: u64) {
    static STARTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if std::env::var_os("CONSTELLATION_FAULT_RUNTIME_HOG_FILE").is_none()
        || STARTED.swap(true, std::sync::atomic::Ordering::SeqCst)
    {
        return;
    }
    tokio::spawn(async move {
        loop {
            match runtime_hog_file(node) {
                Some((tasks, block, every)) => {
                    tracing::info!(
                        tasks,
                        block_ms = block,
                        "fault injection: blocking the runtime"
                    );
                    for _ in 0..tasks {
                        tokio::spawn(async move {
                            std::thread::sleep(std::time::Duration::from_millis(block));
                        });
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(every)).await;
                }
                None => tokio::time::sleep(std::time::Duration::from_millis(200)).await,
            }
        }
    });
}

/// `CONSTELLATION_FAULT_CLOCK_STEP_FILE=<path>`: the harness's wall-clock
/// step (plan 43 C7-H1, the load item `clock:step:<ms>:<at_ms>[@node]`).
/// The file holds the **cumulative** step of this daemon's wall clock in
/// ms, a signed integer, written write-then-rename like the other `*_FILE`
/// hooks, so reading it again is idempotent. A plain thread (not tokio, so
/// a runtime hog does not delay it) polls it every 100 ms; on a change it
/// sets `constellation_platform::clock::set_fault_step_ms`, logs a
/// warning, and writes `<path>.applied` with the value in force. Only the
/// protocol clock's WALL reading moves: stepping the host clock would move
/// every container's. Also logs the constant `CONSTELLATION_FAULT_CLOCK_
/// OFFSET_MS` once (the platform crate has no logger). Started once per
/// process from the authority driver; a no-op without either variable. (The
/// step already in the file when the daemon starts is applied earlier, by
/// `platform::clock::protocol()` before its first reading: this poller only
/// follows later changes.)
pub fn start_clock_faults() {
    static STARTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if STARTED.swap(true, std::sync::atomic::Ordering::SeqCst) {
        return;
    }
    let offset = constellation_platform::clock::fault_offset_ms();
    if offset != 0 {
        tracing::warn!(
            offset_ms = offset,
            "fault injection: the protocol clock is offset (testing only)"
        );
    }
    let Some(path) = std::env::var_os("CONSTELLATION_FAULT_CLOCK_STEP_FILE").map(PathBuf::from)
    else {
        return;
    };
    let spawned = std::thread::Builder::new()
        .name("fault-clock-step".into())
        .spawn(move || {
            let mut applied = constellation_platform::clock::fault_step_ms();
            loop {
                poll_clock_step(
                    &path,
                    &mut applied,
                    constellation_platform::clock::set_fault_step_ms,
                );
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        });
    if let Err(error) = spawned {
        tracing::warn!(%error, "could not start the clock-step fault poller");
    }
}

/// One poll of the step file (see [`start_clock_faults`]): when it holds a
/// cumulative step other than `applied`, call `set` with it, log, remember
/// it and write `<path>.applied`. A missing file, an unreadable one or one
/// that does not hold an integer changes nothing. Returns whether a step
/// was applied.
fn poll_clock_step(path: &Path, applied: &mut i64, set: impl FnOnce(i64)) -> bool {
    let Some(want) = constellation_platform::clock::read_step_file(path) else {
        return false;
    };
    if want == *applied {
        return false;
    }
    set(want);
    tracing::warn!(
        delta_ms = want - *applied,
        total_ms = want,
        "fault injection: the protocol wall clock stepped (testing only)"
    );
    *applied = want;
    let mut marker = path.as_os_str().to_os_string();
    marker.push(".applied");
    let _ = std::fs::write(PathBuf::from(marker), want.to_string());
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Expectation: the step file is read as a cumulative value, applied once
    /// per change (reading it again does nothing), acknowledged in
    /// `<file>.applied`, and a missing or malformed file changes nothing.
    #[test]
    fn the_clock_step_file_is_cumulative_and_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("clock-step");
        let marker = dir.path().join("clock-step.applied");
        let mut applied = 0;
        let mut seen = Vec::new();
        assert!(!poll_clock_step(&file, &mut applied, |v| seen.push(v)));
        std::fs::write(&file, "3000\n").unwrap();
        assert!(poll_clock_step(&file, &mut applied, |v| seen.push(v)));
        assert_eq!((applied, seen.clone()), (3_000, vec![3_000]));
        assert_eq!(std::fs::read_to_string(&marker).unwrap(), "3000");
        // Again, unchanged: nothing.
        assert!(!poll_clock_step(&file, &mut applied, |v| seen.push(v)));
        // Garbage and an empty file (a torn write) change nothing.
        std::fs::write(&file, "soon").unwrap();
        assert!(!poll_clock_step(&file, &mut applied, |v| seen.push(v)));
        std::fs::write(&file, "").unwrap();
        assert!(!poll_clock_step(&file, &mut applied, |v| seen.push(v)));
        // The value is the cumulative step: back to 0 is a change.
        std::fs::write(&file, "-1500").unwrap();
        assert!(poll_clock_step(&file, &mut applied, |v| seen.push(v)));
        std::fs::write(&file, "0").unwrap();
        assert!(poll_clock_step(&file, &mut applied, |v| seen.push(v)));
        assert_eq!(seen, vec![3_000, -1_500, 0]);
        assert_eq!(std::fs::read_to_string(&marker).unwrap(), "0");
    }

    #[test]
    fn runtime_hog_spec() {
        assert_eq!(
            parse_runtime_hog("4:200"),
            Some((4, Duration::from_millis(200), Duration::ZERO))
        );
        assert_eq!(
            parse_runtime_hog(" 2:50:10 "),
            Some((2, Duration::from_millis(50), Duration::from_millis(10)))
        );
        for bad in ["", "4", "0:100", "4:0", "a:b", "1:2:3:4"] {
            assert_eq!(parse_runtime_hog(bad), None, "{bad:?}");
        }
    }
}
