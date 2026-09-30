//! What is left of the forwarding module after plan 30 M5 phase 2: the
//! knobs, the system-rid allocator and the status counters. The
//! forwarding *decisions* — routing, retries, the redirect, the
//! speculation installs, the holder-side dedup and execution — are the
//! core's (`constellation_authority::core::client` and `::holder`); the
//! wire round trip is `crate::authority_driver`'s.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Env: forwarded-mutation reply deadline (default 500 ms).
pub fn forward_timeout_ms() -> u64 {
    std::env::var("CONSTELLATION_FORWARD_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500)
}

/// Env: `CONSTELLATION_FORWARD=off|0|false` disables requester-side
/// forwarding. Non-holder mutations then fall back to ordinary lease
/// acquisition (P2P handoff, then S3 CAS) — the pre-forwarding
/// behavior. Exists for operators who want writer-follows-lease
/// placement, and for the harness to exercise the takeover path
/// deliberately (`p2p-handover`).
pub fn forwarding_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("CONSTELLATION_FORWARD") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false"
        ),
        Err(_) => true,
    })
}

/// The `Rid::incarnation` every [`ForwardState::next_system_rid`]
/// carries: a reserved namespace no mount incarnation reaches.
pub const SYSTEM_RID_INCARNATION: u32 = u32::MAX;

/// Forward round-trip counters for `status`, and the system-rid allocator.
pub struct ForwardState {
    pub ok: AtomicU64,
    pub err: AtomicU64,
    pub latencies_us: Mutex<Vec<u64>>,
    /// This mount's persisted incarnation (`Meta::bump_incarnation`),
    /// folded into every [`ForwardState::next_system_rid`] so system
    /// rids never repeat across mounts.
    mount_incarnation: u32,
    /// Per-mount counter behind [`ForwardState::next_system_rid`];
    /// restarts at 0 on every mount (only its low 32 bits are used).
    system_rid_seq: AtomicU64,
}

impl ForwardState {
    /// `mount_incarnation` is the value this mount's
    /// `Meta::bump_incarnation` returned.
    pub fn new(mount_incarnation: u32) -> Arc<Self> {
        Arc::new(Self {
            ok: AtomicU64::new(0),
            err: AtomicU64::new(0),
            latencies_us: Mutex::new(Vec::new()),
            mount_incarnation,
            system_rid_seq: AtomicU64::new(0),
        })
    }

    /// A rid for a mutation not issued through the FUSE write path's own
    /// allocator (`view::SyncHandle::next_rid_seq`) — retention
    /// pruning's forwarded unlinks, conflict-copy steps and best-effort
    /// atime batches.
    ///
    /// `incarnation` is the reserved [`SYSTEM_RID_INCARNATION`]
    /// (`u32::MAX`), which no real mount incarnation (bumped by one per
    /// mount) can ever reach, so these never collide with — or share a
    /// holder `recent` bucket with — a genuine FUSE-issued rid from the
    /// same node. `seq` is `mount_incarnation << 32 | counter`: the
    /// counter is volatile and restarts at 0 every mount, so the mount's
    /// persisted incarnation in the high bits is what keeps a system rid
    /// from being reissued after a restart and answered as "already
    /// done" from a `completed` row (or a holder's `recent` entry) the
    /// previous mount left behind within the retention window.
    pub fn next_system_rid(&self, node: u64) -> constellation_meta::Rid {
        let counter = self.system_rid_seq.fetch_add(1, Ordering::Relaxed) & u64::from(u32::MAX);
        constellation_meta::Rid {
            node,
            incarnation: SYSTEM_RID_INCARNATION,
            seq: (u64::from(self.mount_incarnation) << 32) | counter,
        }
    }

    pub fn record_ok(&self, took: Duration) {
        self.ok.fetch_add(1, Ordering::Relaxed);
        let mut v = self.latencies_us.lock().unwrap();
        v.push(took.as_micros() as u64);
        if v.len() > 256 {
            let drain = v.len() - 256;
            v.drain(0..drain);
        }
    }

    pub fn record_err(&self) {
        self.err.fetch_add(1, Ordering::Relaxed);
    }

    pub fn p50_ms(&self) -> Option<u64> {
        let mut v = self.latencies_us.lock().unwrap().clone();
        if v.is_empty() {
            return None;
        }
        v.sort_unstable();
        Some(v[v.len() / 2] / 1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two mounts of one state dir must never reissue the same system rid
    /// (a `completed` row from the previous mount would answer it).
    #[test]
    fn system_rids_differ_across_mounts() {
        let first = ForwardState::new(1);
        let second = ForwardState::new(2);
        let a = first.next_system_rid(9);
        let b = second.next_system_rid(9);
        assert_eq!(a.incarnation, SYSTEM_RID_INCARNATION);
        assert_ne!(a, b);
        assert_ne!(first.next_system_rid(9), a);
    }
}
