//! Delegation grant/renew/expiry state machine (DESIGN.md §5.2).
//!
//! While a path is offline-designated and the designee is P2P-reachable,
//! other nodes still write there — but only under a short-TTL
//! **delegation** the designee grants directly, renewed like a
//! mini-lease. This is what keeps the designee's core invariant true:
//! *at any instant the designee provably holds all committed changes
//! under its path*, so its own offline writes (when it later loses S3,
//! or everyone else) are always a linear continuation of what came
//! before, never a fork.
//!
//! Two things follow from that invariant, and both are load-bearing
//! (unlike the M3.3 lease-handoff fast path, this one *is* a safety
//! mechanism, not a pure accelerator):
//!
//! 1. A **delegation must expire** if not renewed. An unreachable
//!    designee cannot keep certifying that it has seen everything, so
//!    other nodes must stop being able to claim delegated authority once
//!    the grant lapses — they go read-only on that path instead
//!    (DESIGN.md §5.2, §5 availability matrix "designee isolated").
//! 2. A delegated node's **flush must be acknowledged by the designee**
//!    before it is considered published, because otherwise the designee
//!    could later resume writing under a stale view of the subtree. The
//!    cost (~1 RTT to the designee per foreign flush) is the price
//!    DESIGN.md accepts for that guarantee; a timed-out ack does not
//!    fail the write, it just delays visibility — the record stays
//!    journaled and retries.
//!
//! Both state machines here are written against a live clock
//! (`now_ms` is passed in, not read from the OS) so expiry can be
//! tested deterministically without sleeping.

use crate::message::Payload;
use std::collections::HashMap;

/// How long a delegation is valid for before it must be renewed.
/// Deliberately short: an unreachable designee's grants must lapse
/// quickly so others fall back to read-only rather than assuming stale
/// authority.
pub const DEFAULT_DELEGATION_TTL_MS: u64 = 5_000;

/// One delegation this node (as designee) has granted to a requester.
#[derive(Debug, Clone)]
struct Grant {
    epoch: u64,
    expires_at_ms: i64,
}

/// Designee side: tracks every delegation granted for paths this node
/// is designated for.
#[derive(Default)]
pub struct DelegationGranter {
    /// Keyed by (path, requester node id).
    grants: HashMap<(String, u64), Grant>,
    next_epoch: u64,
}

impl DelegationGranter {
    pub fn new() -> Self {
        Self {
            grants: HashMap::new(),
            next_epoch: 1,
        }
    }

    /// Grant (or renew) a delegation for `requester` under `path`.
    /// Returns the reply to send. The caller is responsible for having
    /// already confirmed this node holds the designation for `path`;
    /// this type only tracks the grants themselves.
    pub fn grant(&mut self, path: &str, requester: u64, ttl_ms: u64, now_ms: i64) -> Payload {
        let epoch = self.next_epoch;
        self.next_epoch += 1;
        self.grants.insert(
            (path.to_string(), requester),
            Grant {
                epoch,
                expires_at_ms: now_ms + ttl_ms as i64,
            },
        );
        Payload::DelegationGrant {
            path: path.to_string(),
            epoch,
            ttl_ms,
            granted: true,
        }
    }

    pub fn decline(path: &str) -> Payload {
        Payload::DelegationGrant {
            path: path.to_string(),
            epoch: 0,
            ttl_ms: 0,
            granted: false,
        }
    }

    /// Is `requester`'s delegation for `path` currently valid?
    pub fn is_valid(&self, path: &str, requester: u64, now_ms: i64) -> bool {
        self.grants
            .get(&(path.to_string(), requester))
            .is_some_and(|g| g.expires_at_ms > now_ms)
    }

    /// Drop expired grants. Not required for correctness (`is_valid`
    /// already checks expiry) but keeps the map from growing forever
    /// across many short-lived delegations.
    pub fn sweep(&mut self, now_ms: i64) {
        self.grants.retain(|_, g| g.expires_at_ms > now_ms);
    }
}

/// Requester side: tracks delegations this node currently holds from
/// designees, so the FUSE write gate can check "am I still delegated
/// here?" without a network round trip on every op.
#[derive(Default)]
pub struct DelegationHolder {
    grants: HashMap<String, Grant>,
}

impl DelegationHolder {
    pub fn new() -> Self {
        Self {
            grants: HashMap::new(),
        }
    }

    /// Record a grant received from a designee's reply.
    pub fn record(&mut self, reply: &Payload, now_ms: i64) {
        if let Payload::DelegationGrant {
            path,
            epoch,
            ttl_ms,
            granted: true,
        } = reply
        {
            self.grants.insert(
                path.clone(),
                Grant {
                    epoch: *epoch,
                    expires_at_ms: now_ms + *ttl_ms as i64,
                },
            );
        }
    }

    /// Is our delegation for `path` (or an ancestor covering it) still
    /// valid right now? Delegations are granted per designated path;
    /// callers pass the innermost designation's path that covers the
    /// mutation, not the mutated file's own path.
    pub fn is_valid(&self, designation_path: &str, now_ms: i64) -> bool {
        self.grants
            .get(designation_path)
            .is_some_and(|g| g.expires_at_ms > now_ms)
    }

    pub fn epoch(&self, designation_path: &str) -> Option<u64> {
        self.grants.get(designation_path).map(|g| g.epoch)
    }

    pub fn forget(&mut self, designation_path: &str) {
        self.grants.remove(designation_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grant_then_renew_extends_expiry_and_bumps_epoch() {
        let mut g = DelegationGranter::new();
        let r1 = g.grant("/site", 2, 1000, 0);
        let Payload::DelegationGrant {
            epoch: e1, granted, ..
        } = r1
        else {
            panic!()
        };
        assert!(granted);
        assert!(g.is_valid("/site", 2, 500));
        assert!(!g.is_valid("/site", 2, 1500), "must expire without renewal");

        // Renew before expiry: a fresh epoch, fresh TTL window.
        let r2 = g.grant("/site", 2, 1000, 500);
        let Payload::DelegationGrant { epoch: e2, .. } = r2 else {
            panic!()
        };
        assert!(e2 > e1, "renewal must bump the epoch");
        assert!(g.is_valid("/site", 2, 1400));
        assert!(!g.is_valid("/site", 2, 1600));
    }

    /// A grant for one requester must not validate a different
    /// requester's claim to the same path.
    #[test]
    fn grants_are_per_requester() {
        let mut g = DelegationGranter::new();
        g.grant("/site", 2, 1000, 0);
        assert!(g.is_valid("/site", 2, 0));
        assert!(!g.is_valid("/site", 3, 0), "different requester");
    }

    #[test]
    fn sweep_drops_only_expired_grants() {
        let mut g = DelegationGranter::new();
        g.grant("/a", 1, 100, 0);
        g.grant("/b", 2, 100, 0);
        g.sweep(150);
        assert!(!g.is_valid("/a", 1, 200));
        assert!(!g.is_valid("/b", 2, 200));

        let mut g2 = DelegationGranter::new();
        g2.grant("/a", 1, 1000, 0);
        g2.sweep(50);
        assert!(
            g2.is_valid("/a", 1, 60),
            "unexpired grant must survive a sweep"
        );
    }

    #[test]
    fn holder_records_grant_and_expires() {
        let mut h = DelegationHolder::new();
        assert!(!h.is_valid("/site", 0));
        h.record(
            &Payload::DelegationGrant {
                path: "/site".into(),
                epoch: 7,
                ttl_ms: 1000,
                granted: true,
            },
            0,
        );
        assert!(h.is_valid("/site", 500));
        assert_eq!(h.epoch("/site"), Some(7));
        assert!(!h.is_valid("/site", 1500), "must lapse without renewal");
    }

    /// A decline must never be recorded as a grant.
    #[test]
    fn holder_ignores_a_decline() {
        let mut h = DelegationHolder::new();
        h.record(&DelegationGranter::decline("/site"), 0);
        assert!(!h.is_valid("/site", 0));
    }

    /// An unrelated message must never be mistaken for a grant.
    #[test]
    fn holder_ignores_unrelated_messages() {
        let mut h = DelegationHolder::new();
        h.record(&Payload::Ping { node_id: 1 }, 0);
        assert!(!h.is_valid("/site", 0));
    }

    #[test]
    fn forget_drops_a_delegation_immediately() {
        let mut h = DelegationHolder::new();
        h.record(
            &Payload::DelegationGrant {
                path: "/site".into(),
                epoch: 1,
                ttl_ms: 10_000,
                granted: true,
            },
            0,
        );
        assert!(h.is_valid("/site", 0));
        h.forget("/site");
        assert!(!h.is_valid("/site", 0));
    }
}
