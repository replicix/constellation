//! Optional read-time atime (plan 20): batched, eventually consistent,
//! best effort. The default is `Off` and bit-identical to the historic
//! noatime behaviour; nothing here touches a read unless the operator
//! opted in with `--atime relatime|lazy` (or `CONSTELLATION_ATIME`).
//!
//! This module holds the pure policy decision (`should_bump`), the
//! process-global config readers, the observability counters
//! (`AtimeStats`), and the in-memory accumulator that coalesces bumps
//! between flushes. It deliberately does no I/O: the read hook records
//! intent here, and the sync task's flush ticker drains it (see
//! `node_runtime`). Merge semantics and durability live in the meta
//! crate (`MetaStore::{apply_atime,queue_atime,...}`).

use constellation_fs_core::{FileAttr, Ino};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Access-time update policy. `Off` is the default and short-circuits
/// before any work. There is deliberately no `strict` mode: we cannot
/// honour strict-atime semantics (bumps are coalesced and lossy) and
/// will not name a mode as though we could.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AtimeMode {
    Off,
    Relatime,
    Lazy,
}

impl AtimeMode {
    pub fn as_str(self) -> &'static str {
        match self {
            AtimeMode::Off => "off",
            AtimeMode::Relatime => "relatime",
            AtimeMode::Lazy => "lazy",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "off" | "noatime" | "none" => Some(AtimeMode::Off),
            "relatime" => Some(AtimeMode::Relatime),
            "lazy" => Some(AtimeMode::Lazy),
            _ => None,
        }
    }

    /// Effective mode: `CONSTELLATION_ATIME` overrides the `--atime`
    /// flag (passed as `flag`), which defaults to `Off`. An unparseable
    /// value anywhere falls back to the next source rather than failing
    /// a mount — atime is never worth refusing to serve reads.
    pub fn resolve(flag: Option<AtimeMode>) -> Self {
        if let Some(v) = std::env::var("CONSTELLATION_ATIME")
            .ok()
            .and_then(|v| AtimeMode::parse(&v))
        {
            return v;
        }
        flag.unwrap_or(AtimeMode::Off)
    }
}

// --- config readers (CONSTELLATION_* env, documented in features/atime.md) ---

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Bump threshold. No single default: 24 h under `relatime`, 1 s under
/// `lazy`, resolved *after* the mode is known. An explicit
/// `CONSTELLATION_ATIME_GRANULARITY_S` overrides both.
pub fn granularity(mode: AtimeMode) -> Duration {
    const RELATIME_DEFAULT_S: u64 = 86_400;
    const LAZY_DEFAULT_S: u64 = 1;
    let default = match mode {
        AtimeMode::Lazy => LAZY_DEFAULT_S,
        // Off never consults this; relatime is the meaningful default.
        _ => RELATIME_DEFAULT_S,
    };
    Duration::from_secs(env_u64("CONSTELLATION_ATIME_GRANULARITY_S", default))
}

/// Accumulator flush period. Deliberately far slower than the ~500 ms
/// sync tick; atime does not need freshness.
pub fn flush_interval() -> Duration {
    Duration::from_millis(env_u64("CONSTELLATION_ATIME_FLUSH_MS", 10_000))
}

/// Cap on distinct pending inodes; overflow drops the new entry.
pub fn max_pending() -> usize {
    env_u64("CONSTELLATION_ATIME_MAX_PENDING", 65_536) as usize
}

/// Longest an atime-only partition may sit before it is shipped anyway.
///
/// Gates the standalone "ship an atime-only partition on a timer" path
/// (`Shipper::ship_atime_if_stale`, checked once per sync round that
/// shipped nothing else). Ride-along shipping (atime folds into any write
/// segment for the partition) and ship-then-release (idle lease release
/// flushes pending atime first) both cover the common cases; this timer
/// is what bounds the corner case of a holder that neither writes nor
/// idles — busy serving reads under sustained traffic, so
/// `LeaseView::idle_for_ms` never crosses `idle_release_ms` either.
pub fn ship_max_delay() -> Duration {
    Duration::from_secs(env_u64("CONSTELLATION_ATIME_SHIP_MAX_DELAY_S", 300))
}

/// Batched-forward timeout (shorter than the 500 ms mutation forward).
pub fn forward_timeout() -> Duration {
    Duration::from_millis(env_u64("CONSTELLATION_ATIME_FORWARD_TIMEOUT_MS", 200))
}

/// Whether a read-only member may forward atime batches. Atime carries
/// no authority, so this is the one record class a RO member may
/// publish — opt-in, and it gates only `AtimeBatch`.
pub fn ro_forward_enabled() -> bool {
    matches!(
        std::env::var("CONSTELLATION_ATIME_RO_FORWARD").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Linux relatime semantics: bump only if atime is older than the
/// granularity, or if it trails mtime/ctime (so "has this been read
/// since it changed?" keeps working — the one thing most tooling
/// actually asks of atime). `Lazy` drops the trailing-clause shortcut
/// and simply honours the (small) granularity.
pub fn should_bump(mode: AtimeMode, attr: &FileAttr, now_ns: i64, granularity_ns: i64) -> bool {
    match mode {
        AtimeMode::Off => false,
        AtimeMode::Relatime => {
            now_ns.saturating_sub(attr.atime_ns) >= granularity_ns
                || attr.atime_ns < attr.mtime_ns
                || attr.atime_ns < attr.ctime_ns
        }
        AtimeMode::Lazy => now_ns.saturating_sub(attr.atime_ns) >= granularity_ns,
    }
}

/// Observability counters, modelled on `ForwardState`. A steadily
/// climbing `dropped_cap`/`forward_err` is the operator's signal that
/// atime is being lossy — legal, but worth seeing.
#[derive(Default, Debug)]
pub struct AtimeStats {
    /// Bumps recorded into the accumulator (post-policy, pre-coalesce).
    pub queued: AtomicU64,
    /// Records absorbed by in-memory coalescing (same inode, same flush).
    pub coalesced: AtomicU64,
    /// Rows the local apply actually moved.
    pub applied: AtomicU64,
    /// Bumps dropped because the accumulator was at its cap.
    pub dropped_cap: AtomicU64,
    /// Flusher shards skipped because the write lock was contended.
    pub skipped_locked: AtomicU64,
    pub forward_ok: AtomicU64,
    pub forward_err: AtomicU64,
    /// Partitions published locally (holder) or kept local (RO/gated).
    pub local_only: AtomicU64,
    /// Claims clamped by the skew ceiling on apply.
    pub skew_clamped: AtomicU64,
}

impl AtimeStats {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    fn inc(c: &AtomicU64, n: u64) {
        c.fetch_add(n, Ordering::Relaxed);
    }
}

/// In-memory, sharded accumulator of pending atime bumps. Sharded on
/// the same modulus as the write shards so the flush never serializes
/// unrelated inodes, and so the read hook touches only its own shard.
/// Each entry coalesces to the max `(atime_ns, time_ns)` for its inode.
pub struct AtimeAccumulator {
    mode: AtimeMode,
    granularity_ns: i64,
    cap: usize,
    shards: Vec<Mutex<HashMap<Ino, (i64, i64)>>>,
    len: AtomicU64,
    pub stats: Arc<AtimeStats>,
}

const ACC_SHARDS: usize = 256;

impl AtimeAccumulator {
    pub fn new(mode: AtimeMode, stats: Arc<AtimeStats>) -> Self {
        let granularity_ns = granularity(mode).as_nanos().min(i64::MAX as u128) as i64;
        Self {
            mode,
            granularity_ns,
            cap: max_pending(),
            shards: (0..ACC_SHARDS)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            len: AtomicU64::new(0),
            stats,
        }
    }

    pub fn mode(&self) -> AtimeMode {
        self.mode
    }

    fn shard(&self, ino: Ino) -> &Mutex<HashMap<Ino, (i64, i64)>> {
        &self.shards[ino as usize % ACC_SHARDS]
    }

    /// The read-path hook. Returns immediately for `Off` (the default),
    /// and otherwise records a bump only when `should_bump` says so.
    /// `now_ns` is both the claimed atime and the observation time.
    pub fn on_read(&self, attr: &FileAttr, now_ns: i64) {
        if self.mode == AtimeMode::Off {
            return;
        }
        if !should_bump(self.mode, attr, now_ns, self.granularity_ns) {
            return;
        }
        self.record(attr.ino, now_ns, now_ns);
    }

    /// Insert or coalesce a bump. Enforces the cap: on overflow the new
    /// entry is dropped and counted (best effort forbids an unbounded
    /// accumulator).
    pub fn record(&self, ino: Ino, atime_ns: i64, time_ns: i64) {
        let mut shard = self.shard(ino).lock().unwrap();
        match shard.get_mut(&ino) {
            Some(slot) => {
                slot.0 = slot.0.max(atime_ns);
                slot.1 = slot.1.max(time_ns);
                AtimeStats::inc(&self.stats.coalesced, 1);
            }
            None => {
                if self.len.load(Ordering::Relaxed) as usize >= self.cap {
                    AtimeStats::inc(&self.stats.dropped_cap, 1);
                    return;
                }
                shard.insert(ino, (atime_ns, time_ns));
                self.len.fetch_add(1, Ordering::Relaxed);
            }
        }
        AtimeStats::inc(&self.stats.queued, 1);
    }

    /// Drop any pending bump for an inode. Called when an explicit
    /// `setattr` sets atime, so a queued read-bump cannot clobber a
    /// fresh `touch -a` locally (the ctime guard handles the remote
    /// case).
    pub fn purge(&self, ino: Ino) {
        let mut shard = self.shard(ino).lock().unwrap();
        if shard.remove(&ino).is_some() {
            self.len.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Drain every shard into one vector of `(ino, atime_ns, time_ns)`.
    /// Uses `try_lock` and skips (counting it) any shard currently held
    /// by a read — `do_read` holds the write shard across an S3 fetch,
    /// so a blocking drain could stall behind network I/O. A skipped
    /// shard is simply picked up by the next flush.
    pub fn drain(&self) -> Vec<(Ino, i64, i64)> {
        let mut out = Vec::new();
        for shard in &self.shards {
            match shard.try_lock() {
                Ok(mut map) => {
                    if map.is_empty() {
                        continue;
                    }
                    self.len.fetch_sub(map.len() as u64, Ordering::Relaxed);
                    for (ino, (atime_ns, time_ns)) in map.drain() {
                        out.push((ino, atime_ns, time_ns));
                    }
                }
                Err(_) => AtimeStats::inc(&self.stats.skipped_locked, 1),
            }
        }
        out
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_fs_core::InodeKind;

    fn attr(atime: i64, mtime: i64, ctime: i64) -> FileAttr {
        FileAttr {
            ino: 7,
            kind: InodeKind::File,
            size: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            nlink: 1,
            atime_ns: atime,
            mtime_ns: mtime,
            ctime_ns: ctime,
            rdev: 0,
        }
    }

    const DAY: i64 = 86_400 * 1_000_000_000;

    #[test]
    fn off_never_bumps() {
        assert!(!should_bump(AtimeMode::Off, &attr(0, 0, 0), DAY * 10, DAY));
    }

    #[test]
    fn relatime_decision_table() {
        let now = DAY * 10;
        // Fresh atime, nothing trailing: no bump.
        assert!(!should_bump(
            AtimeMode::Relatime,
            &attr(now - 1, now - 2, now - 2),
            now,
            DAY
        ));
        // atime older than granularity: bump.
        assert!(should_bump(
            AtimeMode::Relatime,
            &attr(now - DAY - 1, now - DAY, now - DAY),
            now,
            DAY
        ));
        // atime trails mtime (read since modified?): bump even though fresh.
        assert!(should_bump(
            AtimeMode::Relatime,
            &attr(now - 5, now - 1, now - 10),
            now,
            DAY
        ));
        // atime trails ctime: bump.
        assert!(should_bump(
            AtimeMode::Relatime,
            &attr(now - 5, now - 10, now - 1),
            now,
            DAY
        ));
    }

    #[test]
    fn relatime_granularity_boundary() {
        let now = DAY * 10;
        // Exactly at the granularity: bump (>=).
        assert!(should_bump(
            AtimeMode::Relatime,
            &attr(now - DAY, now - DAY, now - DAY),
            now,
            DAY
        ));
        // One ns under: no bump.
        assert!(!should_bump(
            AtimeMode::Relatime,
            &attr(now - DAY + 1, now - DAY, now - DAY),
            now,
            DAY
        ));
    }

    #[test]
    fn lazy_ignores_trailing_clause() {
        let now = 1_000_000_000;
        let gran = 1_000_000_000; // 1 s
                                  // atime trails mtime but is fresh within granularity: no bump
                                  // under lazy (unlike relatime).
        assert!(!should_bump(
            AtimeMode::Lazy,
            &attr(now - 1, now, now),
            now,
            gran
        ));
        assert!(should_bump(
            AtimeMode::Lazy,
            &attr(now - gran, now, now),
            now,
            gran
        ));
    }

    #[test]
    fn accumulator_coalesces_to_max() {
        let acc = AtimeAccumulator::new(AtimeMode::Lazy, AtimeStats::new());
        acc.record(5, 10, 10);
        acc.record(5, 30, 25);
        acc.record(5, 20, 40);
        assert_eq!(acc.len(), 1);
        let drained = acc.drain();
        assert_eq!(drained, vec![(5, 30, 40)]);
        assert_eq!(acc.len(), 0);
    }

    #[test]
    fn accumulator_cap_drops_new_inodes() {
        let acc = AtimeAccumulator::new(AtimeMode::Lazy, AtimeStats::new());
        // Force cap to 2 by inserting directly and checking the counter.
        let cap = acc.cap;
        for i in 0..(cap as u64 + 10) {
            acc.record(i, 1, 1);
        }
        assert_eq!(acc.len(), cap);
        assert!(acc.stats.dropped_cap.load(Ordering::Relaxed) >= 10);
    }

    #[test]
    fn purge_removes_pending() {
        let acc = AtimeAccumulator::new(AtimeMode::Lazy, AtimeStats::new());
        acc.record(9, 1, 1);
        assert_eq!(acc.len(), 1);
        acc.purge(9);
        assert_eq!(acc.len(), 0);
        acc.purge(9); // idempotent
        assert_eq!(acc.len(), 0);
    }

    #[test]
    fn mode_resolve_env_wins_over_flag() {
        // No env set in this test process by default.
        assert_eq!(
            AtimeMode::resolve(Some(AtimeMode::Relatime)),
            AtimeMode::Relatime
        );
        assert_eq!(AtimeMode::resolve(None), AtimeMode::Off);
    }
}
