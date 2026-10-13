//! Plan 30 §M12: a hybrid logical clock for inode timestamps.
//!
//! Every timestamp a mutation writes (`time_ns` on the records, the
//! attributes it sets) comes from [`now_ns`]: the wall clock when it is
//! ahead of everything this node has seen, one nanosecond past the last
//! stamp otherwise. Every stamp this node *applies* from the log (a
//! peer's record) advances the clock through [`observe`]. Two
//! consequences, both needed by M12's commutative parent attributes:
//!
//! - **Per node, stamps strictly increase** — a parent's mtime/ctime
//!   merged by `max` never goes backwards on the node that wrote it, so
//!   "the timestamp increases after a create" (pjdfstest) holds on that
//!   node whatever the wall clock does.
//! - **Across nodes, a stamp is above every stamp its writer had
//!   applied** (the log is the causal order): a create replayed after a
//!   peer's unlink of the same parent carries a larger stamp, so the
//!   `max` merge lands on the later write.
//!
//! The clock is at most the largest skew among the nodes ahead of the
//! wall clock (a node that applied a peer 5 s ahead stamps 5 s ahead
//! until its own clock catches up) — the same bound HLCs have always
//! had, and one that only moves inode timestamps: the lease, grant and
//! promise clocks (M8/M10's drift margins) stay on the wall clock.
//!
//! One record's stamp may be an earlier one: a manifest commit's
//! (`WriteManifest`) is the file's ctime, its last change — the write
//! session's last data change, itself stamped here by the writer, or the
//! row's later ctime (`store::writes::commit_ctime`). It touches no
//! parent, so the `max` merges above never see it, and only replay reads
//! a stamp ([`observe`]), where an earlier one changes nothing. It is
//! never above the sequencer's own clock, though: that clock never
//! observes the records it appends itself, so its next change of the
//! file would be stamped below such a ctime — ctime going back for every
//! observer. The sequencer observes the writer's stamp before it commits
//! instead, so a writer ahead of it moves its clock up — but only within
//! [`skew_tolerance_ns`] of its own wall clock ([`observe_within_tolerance`]).
//! Unlike a replayed record's, that stamp is any client's, and the clock
//! never comes down: one client with a bad RTC (2040, or `i64::MAX`,
//! which would pin the clock so it stops increasing) closing one file
//! would otherwise stamp every time cluster-wide past it, for good, and
//! break what compares inode times with the wall clock (retention
//! grace, relatime, the atime ceiling). Past the tolerance the stamp is
//! not observed, and the cap cuts that commit's ctime to the sequencer's
//! time: only that client sees its ctime drop, by its own skew.
//!
//! One process-wide clock: a node runs one `Meta`, and a test process
//! running several replicas shares a clock that is still a valid HLC
//! (only *more* ordered than a per-replica one).

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::OnceLock;

static LAST: AtomicI64 = AtomicI64::new(0);

/// `CONSTELLATION_HLC_SKEW_TOLERANCE_S`'s default: as far as a peer's
/// stamp may be ahead of this node's wall clock and still be observed
/// (the atime ceiling's tolerance, `store::atime`).
const SKEW_TOLERANCE_S_DEFAULT: i64 = 300;

static REJECTED: AtomicU64 = AtomicU64::new(0);
static WARNED: AtomicBool = AtomicBool::new(false);

/// The next stamp: the wall clock, or one past the last stamp issued or
/// observed when the wall clock is not ahead of it.
pub fn now_ns() -> i64 {
    let wall = constellation_fs_core::types::now_ns();
    let mut last = LAST.load(Ordering::Relaxed);
    loop {
        let next = wall.max(last.saturating_add(1));
        match LAST.compare_exchange_weak(last, next, Ordering::AcqRel, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(cur) => last = cur,
        }
    }
}

/// A stamp applied from the log (a peer's): the clock never issues one
/// below it again.
pub fn observe(stamp_ns: i64) {
    if stamp_ns > 0 {
        LAST.fetch_max(stamp_ns, Ordering::AcqRel);
    }
}

/// How far ahead of this node's wall clock a stamp from a writer's clock
/// may be and still be observed ([`observe_within_tolerance`]):
/// `CONSTELLATION_HLC_SKEW_TOLERANCE_S` (seconds, default 300), read
/// once.
pub fn skew_tolerance_ns() -> i64 {
    static TOLERANCE: OnceLock<i64> = OnceLock::new();
    *TOLERANCE.get_or_init(|| {
        std::env::var("CONSTELLATION_HLC_SKEW_TOLERANCE_S")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|s| *s >= 0)
            .unwrap_or(SKEW_TOLERANCE_S_DEFAULT)
            .saturating_mul(1_000_000_000)
    })
}

/// [`observe`] a stamp from a writer's clock, unless it is more than
/// [`skew_tolerance_ns`] ahead of this node's wall clock: then it is a
/// bad clock's, not to be propagated (counted, warned about once).
/// Whether it was observed.
pub fn observe_within_tolerance(stamp_ns: i64) -> bool {
    let wall = constellation_fs_core::types::now_ns();
    if stamp_ns > wall.saturating_add(skew_tolerance_ns()) {
        REJECTED.fetch_add(1, Ordering::Relaxed);
        if !WARNED.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                stamp_ns,
                wall_ns = wall,
                ahead_s = (stamp_ns - wall) / 1_000_000_000,
                "a writer's clock is ahead of this node's by more than \
                 CONSTELLATION_HLC_SKEW_TOLERANCE_S: its stamps are not \
                 observed (warned once; see hlc::rejected_stamps)"
            );
        }
        return false;
    }
    observe(stamp_ns);
    true
}

/// Stamps [`observe_within_tolerance`] refused (diagnostics).
pub fn rejected_stamps() -> u64 {
    REJECTED.load(Ordering::Relaxed)
}

/// The last stamp issued or observed (diagnostics).
pub fn last_ns() -> i64 {
    LAST.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamps_strictly_increase_and_observe_advances() {
        let a = now_ns();
        let b = now_ns();
        assert!(b > a);
        // (A small step: the clock is process-wide, and the crate's other
        // tests compare stamps with the wall clock.)
        let far = b + 1_000;
        observe(far);
        let c = now_ns();
        assert!(c > far, "{c} <= {far}");
        observe(far - 5);
        assert!(now_ns() > c);
    }
}
