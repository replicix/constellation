//! `heartbeat/<node>` promise objects (plan 30 §M10, flexible-quorum
//! continuation epochs; anchored to the claimed lease key by plan 43 C7b).
//!
//! With `epoch_slack = f > 0` a continuation epoch may form with up to `f`
//! write-eligible nodes missing. A missing node may still reach S3, so an
//! S3 takeover of an expired lease must be able to tell that no epoch can
//! be holding it. The heartbeat carries the evidence: a node that is in no
//! open epoch says, naming the lease [`RenewalKey`] it says it for, *"I was
//! in no open epoch when I wrote this, and I will join none for
//! `no_epoch_for_ms` of my own wait clock from now"*. It is published on
//! demand: when the node's own observation of a foreign lease key ran out
//! with P2P down, or when a would-be taker asks for one over P2P (plan 30 §M10
//! phase 2; there is no steady refresh).
//!
//! The rules (checked by the Stateright model `constellation_model::flex`,
//! see `docs/plans/v1/PROGRESS.md`, "Plan 30 M10 — phase 1" and plan 43
//! C7b, `PromiseRule::Anchored`):
//!
//! - a node persists a promise locally **before** issuing its PUT, and
//!   joins an epoch only once the last promise it *issued* has run out on
//!   its own wait clock ([`may_join_epoch`]);
//! - a member of an open epoch publishes no promise until it has learned
//!   the epoch closed;
//! - an epoch needs [`epoch_quorum`] = `N − f` members;
//! - a TTL takeover of an expired lease held by another node needs at
//!   least `f` *other* roster nodes with evidence for the key being
//!   claimed ([`takeover_check`]).
//!
//! The promise TTL must be at most a quarter of the lease TTL
//! ([`PromiseConfig::validate`]): after an S3 outage starts, the members'
//! promises must run out well before the leases the epoch wants to carry
//! stop being usable, or no epoch can ever hold anything.
//!
//! The object is plaintext JSON even on an E2E filesystem (node ids and
//! keys only, like the lease and registry objects).

use crate::error::StoreError;
use crate::layout;
use crate::lease::RenewalKey;
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
    /// The lease key this promise was made for (plan 43 C7b): written when
    /// the writer's own observation of that key had run out, or on a taker's
    /// request that names it, by a node in no open epoch. A taker counts it
    /// only if it names exactly the key it is claiming. `None`: made for no
    /// key (an advertisement of the slack alone, or an older build's
    /// object): it counts for nothing.
    #[serde(default)]
    pub key: Option<RenewalKey>,
    /// The writer joins no continuation epoch for this long after it
    /// persisted the promise, on its own wait clock (a duration; nothing
    /// compares it with another node's clock). 0: none.
    #[serde(default)]
    pub no_epoch_for_ms: u64,
    /// The epoch slack the writer runs with. A taker uses the largest one
    /// any roster node still advertises ([`effective_slack`]), so a node
    /// that has not yet seen a lowered `f` keeps protecting the epochs it
    /// may form under the old one.
    pub epoch_slack: u32,
    /// When it was written (status display only).
    pub written_unix_ms: i64,
}

impl Promise {
    pub fn new(
        node: u64,
        key: Option<RenewalKey>,
        no_epoch_for_ms: u64,
        epoch_slack: u32,
        now_ms: i64,
    ) -> Self {
        Promise {
            v: PROMISE_VERSION,
            node,
            key,
            no_epoch_for_ms,
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
}

/// A node may join a continuation epoch once the last promise it issued
/// (persisted before the PUT, whether or not the PUT landed) has run out on
/// its own wait clock: `issued_end_wait` is that promise's end there, `now_wait`
/// the reading now (plan 43 C7b: a duration measured by the promiser alone).
pub fn may_join_epoch(issued_end_wait: i64, now_wait: i64) -> bool {
    now_wait >= issued_end_wait
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

/// What a roster node offers a taker for the key it claims.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Evidence {
    /// A `heartbeat/<node>` object read from S3: counts iff it names exactly
    /// the key the taker claims.
    Heartbeat(Promise),
    /// A P2P answer to the taker's `PromiseRequest`: counts iff the node
    /// granted it and the request was sent after the taker's own observation
    /// of the key had run out (`fresh`). A refusal is `fresh: false`.
    Reply { fresh: bool },
}

/// The S3 takeover check for a lease whose renewal key is `claimed`.
/// Allowed iff at least `f` roster nodes other than the taker offer
/// [`Evidence`] for exactly that key.
///
/// Why this is enough, with no clock compared across nodes (plan 43 C7b;
/// the model's `PromiseRule::Anchored`). Under the observation rule the
/// taker claims a key `K` only after its own observation of `K` has run
/// out: a TTL on its wait clock from its first sight, which is after `K` was
/// written, so at least a lease TTL of true time after the holder sent
/// `K`, and every key up to `K` has ended by then. Suppose an epoch `E`
/// carrying this lease is open. It formed at a true instant `j` when a
/// member held the lease usably, so `j` is before the end of `K` (every key
/// up to it ends by `T(send K) + ttl`). It has `N − f` members, none the
/// taker; the taker counted `f` others, so one of them, `x`, is a member.
/// `x`'s evidence was produced at a true instant `w` at least `T(send K) +
/// ttl`: a heartbeat is written when `x`'s own observation of `K` ran out, a
/// P2P answer follows a request sent after the taker's did. At `w`, `x` was in
/// no open epoch (a member of an open epoch publishes nothing and answers
/// nothing), and it joined none before its promise ran out, after `w`: so `x`
/// joined `E` after `w`, and `E` formed after `w` ≥ the end of `K`, a
/// contradiction. When the heartbeats are read does not matter, and the CAS
/// naming the lease version read is what fences a renewal in between.
/// Only rate bounds and causality are used: no offset between clocks.
///
/// Undecodable heartbeats, heartbeats whose `node` does not match their key,
/// and ones that name another key (or none) count as no evidence.
pub fn takeover_check(
    epoch_slack: u32,
    taker: u64,
    roster: &[u64],
    claimed: &RenewalKey,
    evidence: &[(u64, Evidence)],
) -> TakeoverCheck {
    if claimed.holder == taker {
        return TakeoverCheck::OwnLease;
    }
    if epoch_slack == 0 {
        return TakeoverCheck::NotRequired;
    }
    let mut promisers: Vec<u64> = evidence
        .iter()
        .filter(|(key, ev)| {
            *key != taker
                && roster.contains(key)
                && match ev {
                    Evidence::Heartbeat(p) => p.node == *key && p.key == Some(*claimed),
                    Evidence::Reply { fresh } => *fresh,
                }
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
    /// the promise's gate locally first. A plain PUT: the
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

    fn key(holder: u64, wait: i64) -> RenewalKey {
        RenewalKey {
            holder,
            epoch: 4,
            holder_clock: 9,
            holder_wait_ms: wait,
            released: false,
        }
    }

    /// A heartbeat of `node` naming `k`.
    fn p(node: u64, k: RenewalKey) -> (u64, Evidence) {
        (
            node,
            Evidence::Heartbeat(Promise::new(node, Some(k), 15_000, 1, 0)),
        )
    }

    #[test]
    fn promise_roundtrips_and_refuses_other_versions() {
        let promise = Promise::new(7, Some(key(1, 50)), 1_000, 1, 500);
        assert_eq!(Promise::decode(&promise.encode()).unwrap(), promise);
        let mut other = promise.clone();
        other.v = PROMISE_VERSION + 1;
        assert!(Promise::decode(&other.encode()).is_err());
        assert!(Promise::decode(b"{not json").is_err());
        // An object an older build wrote names no key: it decodes, and
        // counts for nothing.
        let old =
            br#"{"v":1,"node":7,"no_epoch_until_unix_ms":9,"epoch_slack":1,"written_unix_ms":3}"#;
        let old = Promise::decode(old).unwrap();
        assert_eq!((old.key, old.no_epoch_for_ms), (None, 0));
    }

    #[test]
    fn default_ttl_is_a_quarter_of_the_lease() {
        let cfg = PromiseConfig::new(crate::lease::DEFAULT_LEASE_TTL_MS / 4);
        assert_eq!(cfg.ttl_ms, 15_000);
        assert_eq!(cfg.validate(crate::lease::DEFAULT_LEASE_TTL_MS), Ok(()));
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
        let mut hb = vec![
            (1, Promise::new(1, None, 0, 1, 0)),
            (2, Promise::new(2, None, 0, 1, 0)),
        ];
        hb[1].1.epoch_slack = 2;
        assert_eq!(effective_slack(0, &roster, &hb), 2);
        assert_eq!(effective_slack(3, &roster, &hb), 3);
        // Not in the roster (retired), or a mismatched key: ignored.
        let stray = vec![
            (9, Promise::new(9, None, 0, 5, 0)),
            (3, Promise::new(4, None, 0, 5, 0)),
        ];
        assert_eq!(effective_slack(1, &roster, &stray), 1);
    }

    #[test]
    fn takeover_check_counts_other_roster_nodes_with_evidence_for_the_claimed_key() {
        let roster = [1, 2, 3];
        let k = key(1, 5_000);
        // f = 0: today.
        assert_eq!(
            takeover_check(0, 3, &roster, &k, &[]),
            TakeoverCheck::NotRequired
        );
        // The taker's own lease: exempt.
        assert_eq!(
            takeover_check(1, 1, &roster, &k, &[]),
            TakeoverCheck::OwnLease
        );
        // Node 2 has a heartbeat for the key: allowed.
        let ev = [p(2, k)];
        assert_eq!(
            takeover_check(1, 3, &roster, &k, &ev),
            TakeoverCheck::Allowed { promisers: vec![2] }
        );
        // A heartbeat for another key (an older renewal, another holder), for
        // none, or the taker's own counts for nothing.
        let ev = [
            p(2, key(1, 4_999)),
            (1, Evidence::Heartbeat(Promise::new(1, None, 15_000, 1, 0))),
            p(3, k),
        ];
        let v = takeover_check(1, 3, &roster, &k, &ev);
        assert_eq!(
            v,
            TakeoverCheck::Refused {
                promisers: vec![],
                needed: 1
            }
        );
        assert!(!v.allows());
        // A P2P answer counts iff it is fresh.
        assert!(
            takeover_check(1, 3, &roster, &k, &[(2, Evidence::Reply { fresh: true })]).allows()
        );
        assert!(
            !takeover_check(1, 3, &roster, &k, &[(2, Evidence::Reply { fresh: false })]).allows()
        );
        // f = 2 needs two, from different nodes.
        let ev = [p(1, k), (2, Evidence::Reply { fresh: true })];
        assert!(takeover_check(2, 3, &roster, &k, &ev).allows());
        assert!(!takeover_check(2, 3, &roster, &k, &ev[..1]).allows());
        // Non-roster (retired) and mismatched keys promise nothing.
        let ev = [
            p(9, k),
            (
                2,
                Evidence::Heartbeat(Promise::new(1, Some(k), 15_000, 1, 0)),
            ),
        ];
        assert!(!takeover_check(1, 3, &roster, &k, &ev).allows());
    }

    #[tokio::test]
    async fn store_roundtrip_skips_garbage() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let hb = HeartbeatStore::new(inner.clone());
        hb.put(&Promise::new(2, None, 5_000, 1, 1)).await.unwrap();
        hb.put(&Promise::new(1, None, 6_000, 1, 1)).await.unwrap();
        // Overwrite is a plain PUT.
        hb.put(&Promise::new(1, Some(key(2, 70)), 7_000, 1, 2))
            .await
            .unwrap();
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
        assert_eq!(all[0].1.no_epoch_for_ms, 7_000);
        assert_eq!(all[0].1.key, Some(key(2, 70)));
        assert_eq!(all[1].0, 2);
    }
}
