//! `heartbeat/<node>` promise objects (plan 30 §M10, flexible-quorum
//! continuation epochs).
//!
//! With `epoch_slack = f > 0` a continuation epoch may form with up to `f`
//! write-eligible nodes missing. A missing node may still reach S3, so an
//! S3 takeover of an expired lease must be able to tell that no epoch can
//! be holding it. The heartbeat carries the evidence: each node promises
//! *"I will not join a continuation epoch before `no_epoch_until`"* (its
//! own clock, unix ms), published on demand: when the node observes a
//! lease expire unrenewed, or when a would-be taker asks for one over P2P
//! (plan 30 §M10 phase 2; there is no steady refresh).
//!
//! The rules (checked by the Stateright model `constellation_model::flex`,
//! see `docs/plans/v1/PROGRESS.md`, "Plan 30 M10 — phase 1"):
//!
//! - a node persists a promise locally **before** issuing its PUT, and
//!   joins an epoch only once the last promise it *issued* has expired in
//!   its own clock ([`may_join_epoch`]);
//! - a member of an open epoch publishes no promise until it has learned
//!   the epoch closed;
//! - an epoch needs [`epoch_quorum`] = `N − f` members;
//! - a TTL takeover of an expired lease held by another node needs at
//!   least `f` *other* roster nodes whose promise outlasts the lease's
//!   recorded expiry ([`takeover_check`]).
//!
//! The promise TTL must be at most a quarter of the lease TTL
//! ([`PromiseConfig::validate`]): after an S3 outage starts, the members'
//! promises must run out well before the leases the epoch wants to carry
//! stop being usable, or no epoch can ever hold anything.
//!
//! The object is plaintext JSON even on an E2E filesystem (node ids and
//! timestamps only, like the lease and registry objects).

use crate::error::StoreError;
use crate::layout;
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// The promise object's format version. Plan 30 waives compatibility: a
/// heartbeat of another version decodes as an error, which the takeover
/// check reads as "no promise" (the refusing direction).
pub const PROMISE_VERSION: u32 = 1;

/// The promise TTL as a fraction of the lease TTL: a quarter, the most
/// plan 30 §M10's validation allows (15 s at the default 60 s lease).
pub const PROMISE_TTL_LEASE_DIVISOR: u64 = 4;

/// The `heartbeat/<node>` object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Promise {
    pub v: u32,
    pub node: u64,
    /// The writer joins no continuation epoch before this instant (its
    /// clock, unix ms). `0`: no promise (a node with `f = 0` withdrawing
    /// an older promise).
    pub no_epoch_until_unix_ms: i64,
    /// The epoch slack the writer runs with. A taker uses the largest one
    /// any roster node still advertises ([`effective_slack`]), so a node
    /// that has not yet seen a lowered `f` keeps protecting the epochs it
    /// may form under the old one.
    pub epoch_slack: u32,
    /// When it was written (status display only).
    pub written_unix_ms: i64,
}

impl Promise {
    pub fn new(node: u64, no_epoch_until_unix_ms: i64, epoch_slack: u32, now_ms: i64) -> Self {
        Promise {
            v: PROMISE_VERSION,
            node,
            no_epoch_until_unix_ms,
            epoch_slack,
            written_unix_ms: now_ms,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a promise always serializes")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StoreError> {
        let p: Promise = serde_json::from_slice(bytes)?;
        if p.v != PROMISE_VERSION {
            return Err(StoreError::CorruptObject(format!(
                "heartbeat promise version {} is not supported by this binary",
                p.v
            )));
        }
        Ok(p)
    }

    /// Whether this promise counts for a takeover of a lease that
    /// expires at `lease_expires_unix_ms` (the lease object's field, in
    /// its holder's clock).
    pub fn outlasts(&self, lease_expires_unix_ms: i64) -> bool {
        self.no_epoch_until_unix_ms > lease_expires_unix_ms
    }
}

/// Promise lifetime. There is no refresh cadence (plan 30 §M10, the
/// coordinator's decision: promises are published on demand — when a
/// node observes a lease expire unrenewed, or a would-be taker asks — so
/// the steady state writes no heartbeat at all).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromiseConfig {
    pub ttl_ms: u64,
}

impl PromiseConfig {
    pub fn new(ttl_ms: u64) -> Self {
        PromiseConfig { ttl_ms }
    }

    /// `CONSTELLATION_PROMISE_TTL_S`, default lease TTL / 4.
    pub fn from_env(lease_ttl_ms: u64) -> Self {
        let ttl_ms = std::env::var("CONSTELLATION_PROMISE_TTL_S")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|v| *v > 0)
            .map(|s| s * 1000)
            .unwrap_or(lease_ttl_ms / PROMISE_TTL_LEASE_DIVISOR);
        PromiseConfig::new(ttl_ms)
    }

    /// Plan 30 §M10's validation: the promise TTL is positive and at most
    /// a quarter of the lease TTL (after an S3 outage starts, the members'
    /// promises must run out well before the leases the epoch wants to
    /// carry stop being usable).
    pub fn validate(&self, lease_ttl_ms: u64) -> Result<(), String> {
        if self.ttl_ms == 0 {
            return Err("promise TTL must be positive".into());
        }
        if self.ttl_ms.saturating_mul(PROMISE_TTL_LEASE_DIVISOR) > lease_ttl_ms {
            return Err(format!(
                "promise TTL {} ms exceeds lease TTL / 4 ({} ms): an epoch could never \
                 form before the leases it should carry stop being usable",
                self.ttl_ms,
                lease_ttl_ms / PROMISE_TTL_LEASE_DIVISOR
            ));
        }
        Ok(())
    }

    /// The promise to persist, then publish, at `now_ms`.
    pub fn next_promise_until(&self, now_ms: i64) -> i64 {
        now_ms + self.ttl_ms as i64
    }
}

/// A node may join a continuation epoch once the last promise it issued
/// (persisted before the PUT, whether or not the PUT landed) has expired
/// in its own clock.
pub fn may_join_epoch(last_issued_no_epoch_until_unix_ms: i64, now_ms: i64) -> bool {
    now_ms >= last_issued_no_epoch_until_unix_ms
}

/// Members an epoch needs out of a `roster_len`-node write-eligible
/// roster: `N − f`, never fewer than one. `None` when `f` leaves no
/// member at all (a configuration [`check_epoch_slack`] refuses).
pub fn epoch_quorum(roster_len: usize, epoch_slack: u32) -> Option<usize> {
    let q = roster_len.checked_sub(epoch_slack as usize)?;
    (q >= 1).then_some(q)
}

/// How an `epoch_slack` fits a roster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlackFit {
    /// Safe, and a single crashed holder can still be taken over by TTL.
    Ok,
    /// Safe, but `f > N − 2`: a TTL takeover needs `f` live promisers
    /// besides the taker and the dead holder, so one crash blocks TTL
    /// failover until the holder returns (`fs set` warns).
    NoTtlFailover,
    /// `f ≥ N`: an epoch of no members (refused).
    Invalid,
}

pub fn check_epoch_slack(epoch_slack: u32, roster_len: usize) -> SlackFit {
    let f = epoch_slack as usize;
    if f == 0 {
        SlackFit::Ok
    } else if f >= roster_len {
        SlackFit::Invalid
    } else if f + 2 > roster_len {
        SlackFit::NoTtlFailover
    } else {
        SlackFit::Ok
    }
}

/// The slack a taker must honour: its own, or the largest any roster
/// node's heartbeat still advertises.
pub fn effective_slack(own: u32, roster: &[u64], heartbeats: &[(u64, Promise)]) -> u32 {
    heartbeats
        .iter()
        .filter(|(key, p)| roster.contains(key) && p.node == *key)
        .map(|(_, p)| p.epoch_slack)
        .fold(own, u32::max)
}

/// The verdict of [`takeover_check`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TakeoverCheck {
    /// `f = 0`: today's rule (every epoch has every roster node, and a
    /// member does no S3 acquisition).
    NotRequired,
    /// The lease names the taker: re-claiming its own register (a
    /// restart, a lapsed renewal, or an epoch holder's flush) — no epoch
    /// of which the taker is not a member can hold it.
    OwnLease,
    Allowed {
        promisers: Vec<u64>,
    },
    Refused {
        promisers: Vec<u64>,
        needed: u32,
    },
}

impl TakeoverCheck {
    pub fn allows(&self) -> bool {
        !matches!(self, TakeoverCheck::Refused { .. })
    }
}

/// The S3 takeover check for an expired lease (`lease_holder`,
/// `lease_expires_unix_ms` as read from the lease object the CAS will
/// name). Allowed iff at least `f` roster nodes other than the taker
/// have a promise that outlasts the lease's expiry.
///
/// Why this is enough, with clocks off by at most `D` from real time and
/// the lease's own margin `M > 2D` (a holder uses its lease only while
/// its clock reads before `expires − M`): suppose an epoch holds this
/// lease. It formed at some instant `j` when a member `h` held it
/// usably, `h(j) < expires − M`, with at least `N − f` members, none the
/// taker. The taker counted `f` others, so some member `x` of the epoch
/// is among them; `x` joined only once its last issued promise `u` had
/// expired, `x(j) ≥ u`, and published nothing newer while in the epoch,
/// so the promise the taker read is at most `u`, and it outlasted the
/// expiry: `u > expires`. Then `x(j) − h(j) > M ≥ 2D`, two clocks at the
/// same instant further apart than drift allows. When the heartbeats are
/// read does not matter (the plan's "read after the expiry, promise
/// unexpired at the read" implies `u > expires` and is therefore also
/// safe, just stricter); the CAS naming the lease version read is what
/// fences a renewal in between.
///
/// Undecodable heartbeats and heartbeats whose `node` does not match
/// their key count as no promise.
pub fn takeover_check(
    epoch_slack: u32,
    taker: u64,
    roster: &[u64],
    lease_holder: u64,
    lease_expires_unix_ms: i64,
    heartbeats: &[(u64, Promise)],
) -> TakeoverCheck {
    if lease_holder == taker {
        return TakeoverCheck::OwnLease;
    }
    if epoch_slack == 0 {
        return TakeoverCheck::NotRequired;
    }
    let mut promisers: Vec<u64> = heartbeats
        .iter()
        .filter(|(key, p)| {
            *key != taker
                && p.node == *key
                && roster.contains(key)
                && p.outlasts(lease_expires_unix_ms)
        })
        .map(|(key, _)| *key)
        .collect();
    promisers.sort_unstable();
    promisers.dedup();
    if promisers.len() >= epoch_slack as usize {
        TakeoverCheck::Allowed { promisers }
    } else {
        TakeoverCheck::Refused {
            promisers,
            needed: epoch_slack,
        }
    }
}

/// `heartbeat/*` I/O.
pub struct HeartbeatStore {
    store: Arc<dyn ObjectStore>,
}

impl HeartbeatStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        HeartbeatStore { store }
    }

    /// Publish (overwrite) this node's promise. The caller has persisted
    /// `promise.no_epoch_until_unix_ms` locally first. A plain PUT: the
    /// node is the only writer of its own key.
    pub async fn put(&self, promise: &Promise) -> Result<(), StoreError> {
        self.store
            .put(
                &layout::heartbeat(promise.node),
                PutPayload::from(promise.encode()),
            )
            .await?;
        Ok(())
    }

    /// Every heartbeat object: one LIST plus one GET per object. An
    /// object that cannot be decoded is skipped (it promises nothing).
    pub async fn read_all(&self) -> Result<Vec<(u64, Promise)>, StoreError> {
        let metas = self
            .store
            .list(Some(&layout::heartbeat_prefix()))
            .try_collect::<Vec<_>>()
            .await?;
        let mut out = Vec::with_capacity(metas.len());
        for m in metas {
            let Some(node) = layout::heartbeat_node(&m.location) else {
                continue;
            };
            let bytes = match self.store.get(&m.location).await {
                Ok(r) => r.bytes().await?,
                Err(object_store::Error::NotFound { .. }) => continue,
                Err(e) => return Err(e.into()),
            };
            match Promise::decode(&bytes) {
                Ok(p) => out.push((node, p)),
                Err(error) => {
                    tracing::warn!(%error, node, "undecodable heartbeat; counted as no promise")
                }
            }
        }
        out.sort_by_key(|(node, _)| *node);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn p(node: u64, until: i64) -> (u64, Promise) {
        (node, Promise::new(node, until, 1, 0))
    }

    #[test]
    fn promise_roundtrips_and_refuses_other_versions() {
        let promise = Promise::new(7, 1_000, 1, 500);
        assert_eq!(Promise::decode(&promise.encode()).unwrap(), promise);
        let mut other = promise.clone();
        other.v = PROMISE_VERSION + 1;
        assert!(Promise::decode(&other.encode()).is_err());
        assert!(Promise::decode(b"{not json").is_err());
    }

    #[test]
    fn default_ttl_is_a_quarter_of_the_lease() {
        let cfg = PromiseConfig::new(crate::lease::DEFAULT_LEASE_TTL_MS / 4);
        assert_eq!(cfg.ttl_ms, 15_000);
        assert_eq!(cfg.validate(crate::lease::DEFAULT_LEASE_TTL_MS), Ok(()));
        assert_eq!(cfg.next_promise_until(1_000), 16_000);
    }

    #[test]
    fn ttl_rule_refuses_long_promises() {
        // 15 s = 60 s / 4 is the edge; 16 s is over.
        assert!(PromiseConfig::new(15_000).validate(60_000).is_ok());
        assert!(PromiseConfig::new(16_000).validate(60_000).is_err());
        assert!(PromiseConfig::new(0).validate(60_000).is_err());
    }

    #[test]
    fn joining_waits_for_the_last_issued_promise() {
        assert!(!may_join_epoch(1_000, 999));
        assert!(may_join_epoch(1_000, 1_000));
        assert!(may_join_epoch(0, 0));
    }

    #[test]
    fn quorum_and_slack_fit() {
        assert_eq!(epoch_quorum(3, 0), Some(3));
        assert_eq!(epoch_quorum(3, 1), Some(2));
        assert_eq!(epoch_quorum(3, 3), None);
        assert_eq!(epoch_quorum(1, 0), Some(1));
        assert_eq!(check_epoch_slack(0, 1), SlackFit::Ok);
        assert_eq!(check_epoch_slack(1, 1), SlackFit::Invalid);
        assert_eq!(check_epoch_slack(1, 2), SlackFit::NoTtlFailover);
        assert_eq!(check_epoch_slack(1, 3), SlackFit::Ok);
        assert_eq!(check_epoch_slack(2, 4), SlackFit::Ok);
        assert_eq!(check_epoch_slack(3, 4), SlackFit::NoTtlFailover);
    }

    #[test]
    fn effective_slack_takes_the_largest_advertised() {
        let roster = [1, 2, 3];
        let mut hb = vec![p(1, 0), p(2, 0)];
        hb[1].1.epoch_slack = 2;
        assert_eq!(effective_slack(0, &roster, &hb), 2);
        assert_eq!(effective_slack(3, &roster, &hb), 3);
        // Not in the roster (retired), or a mismatched key: ignored.
        let stray = vec![(9, Promise::new(9, 0, 5, 0)), (3, Promise::new(4, 0, 5, 0))];
        assert_eq!(effective_slack(1, &roster, &stray), 1);
    }

    #[test]
    fn takeover_check_counts_other_roster_promises_that_outlast_the_expiry() {
        let roster = [1, 2, 3];
        let expires = 10_000;
        // f = 0: today.
        assert_eq!(
            takeover_check(0, 3, &roster, 1, expires, &[]),
            TakeoverCheck::NotRequired
        );
        // The taker's own lease: exempt.
        assert_eq!(
            takeover_check(1, 1, &roster, 1, expires, &[]),
            TakeoverCheck::OwnLease
        );
        // Node 2 promises past the expiry: allowed.
        let hb = [p(1, 9_000), p(2, 10_001), p(3, 99_999)];
        assert_eq!(
            takeover_check(1, 3, &roster, 1, expires, &hb),
            TakeoverCheck::Allowed { promisers: vec![2] }
        );
        // Exactly at the expiry does not outlast it; the taker's own
        // promise never counts.
        let hb = [p(1, 9_000), p(2, 10_000), p(3, 99_999)];
        let v = takeover_check(1, 3, &roster, 1, expires, &hb);
        assert_eq!(
            v,
            TakeoverCheck::Refused {
                promisers: vec![],
                needed: 1
            }
        );
        assert!(!v.allows());
        // f = 2 needs two.
        let hb = [p(1, 20_000), p(2, 20_000)];
        assert!(takeover_check(2, 3, &roster, 1, expires, &hb).allows());
        assert!(!takeover_check(2, 3, &roster, 1, expires, &hb[..1]).allows());
        // Non-roster (retired) and mismatched keys promise nothing.
        let hb = [
            (9, Promise::new(9, 20_000, 1, 0)),
            (2, Promise::new(1, 20_000, 1, 0)),
        ];
        assert!(!takeover_check(1, 3, &roster, 1, expires, &hb).allows());
    }

    #[tokio::test]
    async fn store_roundtrip_skips_garbage() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let hb = HeartbeatStore::new(inner.clone());
        hb.put(&Promise::new(2, 5_000, 1, 1)).await.unwrap();
        hb.put(&Promise::new(1, 6_000, 1, 1)).await.unwrap();
        // Overwrite is a plain PUT.
        hb.put(&Promise::new(1, 7_000, 1, 2)).await.unwrap();
        inner
            .put(&layout::heartbeat(3), PutPayload::from(b"garbage".to_vec()))
            .await
            .unwrap();
        inner
            .put(
                &object_store::path::Path::from("heartbeat/not-a-node"),
                PutPayload::from(b"{}".to_vec()),
            )
            .await
            .unwrap();
        let all = hb.read_all().await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].0, 1);
        assert_eq!(all[0].1.no_epoch_until_unix_ms, 7_000);
        assert_eq!(all[1].0, 2);
    }
}
