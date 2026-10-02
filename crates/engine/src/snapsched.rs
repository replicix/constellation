//! Automatic snapshot schedules (plan 32), the engine side.
//!
//! The policy language and the retention rule are pure and live in
//! `constellation_meta::snapsched`. A policy is bound to a directory by
//! the `user.constellation.snapshots` xattr alone (Step 3.1): the View's
//! setxattr gate validates it on the way in, the control methods in
//! `control::snapsched` read and write it, and nothing else records which
//! directories are policy roots.
//!
//! This module holds [`SnapSchedStats`], the node's counters for all of
//! it (Step 9). Every field the plan names exists from M2 on so the
//! `node.status` schema does not change shape again when the scheduler
//! (M3) and expiry (M4) start filling them; until then only
//! `last_parse_error` ever moves.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Scheduler and binding counters, modeled on `crate::prune::PruneStats`:
/// shared by the View's setxattr gate, the scheduler task (M3) and the
/// control plane, which copies them into `node.status`'s `snapsched`.
#[derive(Debug, Default)]
pub struct SnapSchedStats {
    /// Scheduler ticks run on this node.
    pub ticks: AtomicU64,
    /// Whether this node holds the `_snapsched` singleton lease.
    pub leader: AtomicBool,
    /// Policy roots seen by the last tick, and how many of them were
    /// paused, unparseable (skipped fail-closed), or capped
    /// (`CONSTELLATION_SNAPSCHED_MAX_PER_ROOT`).
    pub roots: AtomicU64,
    pub paused_roots: AtomicU64,
    pub unparseable_roots: AtomicU64,
    pub capped_roots: AtomicU64,
    /// Auto snapshots whose policy root no longer carries a parseable
    /// policy, as of the last tick: kept, never expired (Step 4.2).
    pub orphaned_snapshots: AtomicU64,
    pub created: AtomicU64,
    pub skipped_empty: AtomicU64,
    pub create_failed: AtomicU64,
    pub expired: AtomicU64,
    /// Victims dropped because the row changed under the expiry pass
    /// (held, deleted, re-owned) between evaluation and delete.
    pub skipped_reverify: AtomicU64,
    /// Victims kept by the grace window after a policy change (Step 4.3).
    pub skipped_grace: AtomicU64,
    pub budget_expired: AtomicU64,
    pub budget_stale: AtomicU64,
    /// Ticks refused for replica lag, and for node state (departed,
    /// frozen epoch, offline read-only, read-only member).
    pub refused_lag: AtomicU64,
    pub refused_state: AtomicU64,
    pub last_create_unix_ms: AtomicU64,
    /// The scheduler's last failure, as text.
    pub last_error: Mutex<Option<String>>,
    /// The last policy the setxattr gate refused: `(expression, byte
    /// offset, message)`, kept so an operator who got a bare `EINVAL`
    /// from `setfattr` can see why (plan 22's posture).
    pub last_parse_error: Mutex<Option<(String, usize, String)>>,
}

impl SnapSchedStats {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_parse_error(&self, expr: &str, offset: usize, msg: &str) {
        if let Ok(mut slot) = self.last_parse_error.lock() {
            *slot = Some((expr.to_string(), offset, msg.to_string()));
        }
    }

    pub fn record_error(&self, msg: impl Into<String>) {
        if let Ok(mut slot) = self.last_error.lock() {
            *slot = Some(msg.into());
        }
    }

    /// The counters as `node.status` reports them.
    pub fn status(&self) -> constellation_control::proto::types::SnapSchedStatus {
        let load = |c: &AtomicU64| c.load(Ordering::Relaxed);
        constellation_control::proto::types::SnapSchedStatus {
            ticks: load(&self.ticks),
            leader: self.leader.load(Ordering::Relaxed),
            roots: load(&self.roots),
            paused_roots: load(&self.paused_roots),
            unparseable_roots: load(&self.unparseable_roots),
            capped_roots: load(&self.capped_roots),
            orphaned_snapshots: load(&self.orphaned_snapshots),
            created: load(&self.created),
            skipped_empty: load(&self.skipped_empty),
            create_failed: load(&self.create_failed),
            expired: load(&self.expired),
            skipped_reverify: load(&self.skipped_reverify),
            skipped_grace: load(&self.skipped_grace),
            budget_expired: load(&self.budget_expired),
            budget_stale: load(&self.budget_stale),
            refused_lag: load(&self.refused_lag),
            refused_state: load(&self.refused_state),
            last_create_unix_ms: load(&self.last_create_unix_ms),
            last_error: self.last_error.lock().ok().and_then(|g| g.clone()),
            last_parse_error: self.last_parse_error.lock().ok().and_then(|g| g.clone()),
        }
    }
}
