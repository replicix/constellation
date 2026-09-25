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
//! One process-wide clock: a node runs one `Meta`, and a test process
//! running several replicas shares a clock that is still a valid HLC
//! (only *more* ordered than a per-replica one).

use std::sync::atomic::{AtomicI64, Ordering};

static LAST: AtomicI64 = AtomicI64::new(0);

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
