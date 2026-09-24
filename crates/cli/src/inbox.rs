//! The S3 inbox's knobs and status (plan 30 §M13), after plan 30 M5
//! moved the inbox itself — the requester's queue and group-commit
//! submitter, the outcome waiters, the holder's per-requester polls and
//! GC, the takeover drain, round 3b's escalation and round 3a's P2P
//! reachability rule — into the authority core
//! (`constellation_authority::core::inbox`). The driver
//! (`crate::authority_driver`) runs the S3 batch objects' PUT/GET/LIST/
//! DELETE the core asks for; this module reads the `CONSTELLATION_INBOX_*`
//! environment into the core's `Config` and shapes `status.inbox`.
//!
//! Knobs (all documented in `docs/reference/configuration.md`): `INBOX`
//! (`off` disables the path; non-holders then take the lease as before
//! M13), `INBOX_IDLE_MAX_MS` (warm poll ceiling, default 2000),
//! `INBOX_COLD_MAX_MS` (cold ceiling, default `SYNC_IDLE_MAX_MS`'s
//! 10 000), `INBOX_POLL_WIDTH` (GET-next width, default 4),
//! `INBOX_RECHECK_MS` (how often a waiting requester re-reads the lease,
//! default 1000), `INBOX_HOT_MS` / `INBOX_TAIL_MS` (round 2's hot poll
//! and tail intervals, default 20), `INBOX_P2P_GRACE_MS` (round 3a,
//! default 3000), `INBOX_ESCALATE` / `INBOX_ESCALATE_WINDOW_MS` /
//! `INBOX_ESCALATE_OPS` / `INBOX_ESCALATE_WAIT_MS` /
//! `INBOX_ESCALATE_RETRY_MS` (round 3b's hybrid).

use constellation_authority::{InboxView, Stats};
use std::time::Duration;

pub const DEFAULT_WARM_MAX_MS: u64 = 2_000;
pub const DEFAULT_COLD_MAX_MS: u64 = 10_000;
pub const DEFAULT_RECHECK_MS: u64 = 1_000;
/// Plan 30 M13 round 2: the holder's poll interval right after a hit
/// (`CONSTELLATION_INBOX_HOT_MS`) and for [`HOT_GRACE_ROUNDS`] misses
/// after it, and the requester's log-tail interval while it has an op
/// waiting on its outcome (`CONSTELLATION_INBOX_TAIL_MS`).
pub const DEFAULT_HOT_MS: u64 = 20;
pub const DEFAULT_TAIL_MS: u64 = 20;
/// Misses after a hit during which a requester stays hot.
pub const HOT_GRACE_ROUNDS: u32 = 25;
/// How long a P2P peer may be disconnected before the inbox counts the
/// path as gone (`CONSTELLATION_INBOX_P2P_GRACE_MS`).
pub const DEFAULT_P2P_GRACE_MS: u64 = 3_000;
/// Plan 30 M13 round 3b, the hybrid: a requester whose inbox demand is
/// *sustained* asks for the lease and executes locally once it holds; a
/// *sporadic* writer stays on the inbox. "Sustained" is a sliding window
/// of `CONSTELLATION_INBOX_ESCALATE_WINDOW_MS` over this node's
/// inbox-answered ops: at least `CONSTELLATION_INBOX_ESCALATE_OPS` of
/// them, or at least `CONSTELLATION_INBOX_ESCALATE_WAIT_MS` spent
/// waiting on their round trips, in the window. Plan 30 M5 retuned the
/// defaults with the sans-IO core in the simulation and on the
/// `3node-p2poff-*` meta-bench: 8 ops or 1.5 s of waiting in 10 s (round
/// 4's 20 ops / 3 s delayed the switch by most of a bench run), and the
/// escalating requester's batches carry `wants_lease`, which the holder
/// sees at its next poll instead of its half-TTL renewal. One write
/// every few seconds is 2–3 ops and well under a second of waiting per
/// window: never an escalation.
pub const DEFAULT_ESCALATE_WINDOW_MS: u64 = 10_000;
pub const DEFAULT_ESCALATE_OPS: u64 = 8;
pub const DEFAULT_ESCALATE_WAIT_MS: u64 = 1_500;
/// Longest gap between an escalated requester's lease requests; the
/// first requests back off from 100 ms like the FUSE lease path's own
/// retries.
pub const DEFAULT_ESCALATE_RETRY_MS: u64 = 2_000;
const DEFAULT_POLL_WIDTH: usize = 4;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// `CONSTELLATION_INBOX_ESCALATE=off|0|false` keeps a requester on the
/// inbox however sustained its demand (round 3b's hybrid off).
pub fn escalation_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("CONSTELLATION_INBOX_ESCALATE") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false"
        ),
        Err(_) => true,
    })
}

/// `CONSTELLATION_INBOX=off|0|false` disables the inbox path entirely.
pub fn inbox_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("CONSTELLATION_INBOX") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false"
        ),
        Err(_) => true,
    })
}

/// The in-doubt deadline for an inbox-submitted op: `min(2 × TTL,
/// retention / 2)`. Past it the op is in doubt and the caller takes the
/// lease path (whose own deadline bounds the rest). The retention half
/// keeps a re-submission inside the window its dedup rows live in; a TTL
/// large enough to force the clamp is warned about at mount.
pub fn wait_deadline(ttl_ms: u64, retention_s: u64) -> Duration {
    let two_ttl = 2 * ttl_ms;
    let half_retention = retention_s.saturating_mul(1_000) / 2;
    Duration::from_millis(two_ttl.min(half_retention).max(1_000))
}

/// The inbox's tunables as the core takes them.
pub struct InboxKnobs {
    pub enabled: bool,
    pub warm_max_ms: u64,
    pub cold_max_ms: u64,
    pub hot_ms: u64,
    pub hot_grace: u32,
    pub poll_width: usize,
    pub recheck_ms: u64,
    pub deadline_ms: u64,
    pub p2p_grace_ms: u64,
    pub tail_ms: u64,
    pub escalation: bool,
    pub escalate_window_ms: u64,
    pub escalate_ops: u64,
    pub escalate_wait_ms: u64,
    pub escalate_retry_ms: u64,
}

/// Read the knobs. `base_ms` is the sync interval (the poll schedule's
/// floor), `ttl_ms` the lease TTL, `retention_s` the completion
/// retention window.
pub fn knobs(base_ms: u64, ttl_ms: u64, retention_s: u64) -> InboxKnobs {
    let deadline = wait_deadline(ttl_ms, retention_s);
    if 2 * ttl_ms > retention_s.saturating_mul(1_000) / 2 {
        tracing::warn!(
            ttl_ms,
            retention_s,
            deadline_ms = deadline.as_millis() as u64,
            "CONSTELLATION_LEASE_TTL_MS is large enough that the inbox in-doubt deadline \
             is clamped to half the completion retention window"
        );
    }
    let hot_ms = env_u64("CONSTELLATION_INBOX_HOT_MS", DEFAULT_HOT_MS)
        .max(1)
        .min(base_ms.max(1));
    InboxKnobs {
        enabled: inbox_enabled(),
        warm_max_ms: env_u64("CONSTELLATION_INBOX_IDLE_MAX_MS", DEFAULT_WARM_MAX_MS),
        cold_max_ms: env_u64(
            "CONSTELLATION_INBOX_COLD_MAX_MS",
            env_u64("CONSTELLATION_SYNC_IDLE_MAX_MS", DEFAULT_COLD_MAX_MS),
        ),
        hot_ms,
        hot_grace: HOT_GRACE_ROUNDS,
        poll_width: env_u64("CONSTELLATION_INBOX_POLL_WIDTH", DEFAULT_POLL_WIDTH as u64).max(1)
            as usize,
        recheck_ms: env_u64("CONSTELLATION_INBOX_RECHECK_MS", DEFAULT_RECHECK_MS).max(100),
        deadline_ms: deadline.as_millis() as u64,
        p2p_grace_ms: env_u64("CONSTELLATION_INBOX_P2P_GRACE_MS", DEFAULT_P2P_GRACE_MS),
        tail_ms: env_u64("CONSTELLATION_INBOX_TAIL_MS", DEFAULT_TAIL_MS).max(1),
        escalation: escalation_enabled(),
        escalate_window_ms: env_u64(
            "CONSTELLATION_INBOX_ESCALATE_WINDOW_MS",
            DEFAULT_ESCALATE_WINDOW_MS,
        )
        .max(1_000),
        escalate_ops: env_u64("CONSTELLATION_INBOX_ESCALATE_OPS", DEFAULT_ESCALATE_OPS).max(2),
        escalate_wait_ms: env_u64(
            "CONSTELLATION_INBOX_ESCALATE_WAIT_MS",
            DEFAULT_ESCALATE_WAIT_MS,
        ),
        escalate_retry_ms: env_u64(
            "CONSTELLATION_INBOX_ESCALATE_RETRY_MS",
            DEFAULT_ESCALATE_RETRY_MS,
        )
        .max(100),
    }
}

fn avg_ms(total_ms: u64, samples: u64) -> f64 {
    if samples == 0 {
        0.0
    } else {
        total_ms as f64 / samples as f64
    }
}

/// `status.inbox` from the core's counters (`local_ops` is the FUSE
/// view's touch count).
pub fn status(
    enabled: bool,
    s: &Stats,
    view: &InboxView,
    local_ops: u64,
) -> constellation_api::InboxStatus {
    constellation_api::InboxStatus {
        enabled,
        submitted_batches: s.inbox_submitted_batches,
        submitted_ops: s.inbox_submitted_ops,
        resubmitted_ops: s.inbox_resubmitted_ops,
        unavailable: s.inbox_unavailable,
        pending_ops: view.pending_ops,
        next_n: view.next_n,
        executed_ops: s.inbox_executed_ops,
        refused_ops: s.inbox_refused_ops,
        deduped_ops: s.inbox_deduped_ops,
        drained_batches: s.inbox_drained_batches,
        drained_ops: s.inbox_drained_ops,
        polls: s.inbox_polls,
        poll_hits: s.inbox_poll_hits,
        gc_deleted: s.inbox_gc_deleted,
        tracked_requesters: view.tracked_requesters,
        roster: view.roster.clone(),
        avg_queue_wait_ms: avg_ms(s.inbox_queue_wait_ms_total, s.inbox_answered),
        avg_outcome_wait_ms: avg_ms(s.inbox_outcome_wait_ms_total, s.inbox_answered),
        avg_round_trip_ms: avg_ms(s.inbox_round_trip_ms_total, s.inbox_answered),
        avg_pickup_ms: avg_ms(s.inbox_pickup_ms_total, s.inbox_pickup_samples),
        avg_execute_ms: 0.0,
        avg_batch_ops: {
            let b = s.inbox_submitted_batches;
            if b == 0 {
                0.0
            } else {
                s.inbox_submitted_ops as f64 / b as f64
            }
        },
        largest_batch_ops: s.inbox_largest_batch_ops,
        escalated: view.escalated,
        escalations: s.inbox_escalations,
        lease_requests: s.inbox_lease_requests,
        inbox_ops: s.inbox_answered,
        local_ops,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadline_is_two_ttl_clamped_to_half_retention() {
        assert_eq!(wait_deadline(60_000, 900), Duration::from_millis(120_000));
        assert_eq!(wait_deadline(600_000, 900), Duration::from_millis(450_000));
        assert_eq!(wait_deadline(1, 900), Duration::from_millis(1_000));
    }
}
