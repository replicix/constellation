//! Offline designation (DESIGN.md §5.2): the daemon-side manager tying
//! together the S3 `DesignationStore`, the P2P delegation state
//! machines, and the FUSE write gate.
//!
//! # The gate decision
//!
//! For a mutating op on `ino`, resolve the innermost designation
//! covering its path (there is at most one — overlap is refused at
//! creation), then:
//!
//! - No covering designation → ordinary lease path (unchanged).
//! - We are the designee → proceed; our own writes never need a
//!   delegation (DESIGN.md: "its root claim is non-stealable").
//! - Someone else is the designee → we need a live delegation from
//!   them. If we already hold one, proceed. Otherwise request one
//!   (bounded wait); granted → proceed, declined/timeout → EROFS.
//!
//! `--ro` designations never gate writes (DESIGN.md: "non-designee
//! writes are NOT restricted in `--ro` mode"); they only pin+promise a
//! read guarantee, so the gate treats them as "no covering designation"
//! for write purposes.

use anyhow::{bail, Context, Result};
use constellation_meta::Meta;
use constellation_net::Peers;
use constellation_store_s3::designation::{Designation, DesignationStore};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Bounded wait for a delegation request before falling back to EROFS.
/// The requester's own deadline; a slow or unreachable designee just
/// costs latency for the caller, never a wedge.
const DELEGATION_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// Bounded wait for a `FlushAck` from the designee before a foreign
/// flush gives up and leaves the record journaled for retry.
const FLUSH_ACK_TIMEOUT: Duration = Duration::from_secs(2);

/// Daemon-side manager tying together the S3 `DesignationStore`, the
/// P2P delegation state machines, and the FUSE write gate.
pub struct DesignationManager {
    store: DesignationStore,
    meta: Arc<Meta>,
    peers: Peers,
    node_id: u64,
    /// Cached snapshot of every live (non-released) designation,
    /// refreshed on demand and by the periodic sync loop. The FUSE gate
    /// reads this synchronously — it must never block on S3.
    active: Mutex<Vec<Designation>>,
    /// Designee-side: grants we have issued to others.
    granter: Mutex<constellation_net::DelegationGranter>,
    /// Requester-side: delegations we currently hold from others.
    holder: Mutex<constellation_net::DelegationHolder>,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Outcome of the write gate's designation check.
pub enum GateDecision {
    /// No covering designation: fall through to the ordinary lease gate.
    NoDesignation,
    /// We are the designee (or hold a live delegation): proceed.
    Proceed,
    /// Someone else is the designee and we have no delegation: refuse.
    ReadOnly { designee: u64, path: String },
}

impl DesignationManager {
    pub fn new(store: DesignationStore, meta: Arc<Meta>, peers: Peers, node_id: u64) -> Self {
        Self {
            store,
            meta,
            peers,
            node_id,
            active: Mutex::new(Vec::new()),
            granter: Mutex::new(constellation_net::DelegationGranter::new()),
            holder: Mutex::new(constellation_net::DelegationHolder::new()),
        }
    }

    /// Re-read every live designation from S3 into the local cache. Cheap
    /// to call periodically (one LIST + N small GETs, all rare operator
    /// objects); the FUSE gate itself never triggers this — it only
    /// reads the cache synchronously.
    pub async fn refresh(&self) {
        match self.store.list_all().await {
            Ok(all) => {
                let live: Vec<Designation> = all.into_iter().filter(|d| !d.released).collect();
                *self.active.lock().unwrap() = live;
                self.granter.lock().unwrap().sweep(now_ms());
            }
            Err(e) => {
                tracing::debug!(error = %e, "designation refresh failed; keeping cached view")
            }
        }
    }

    /// The innermost live designation covering `path`, if any.
    fn covering(&self, path: &str) -> Option<Designation> {
        self.active
            .lock()
            .unwrap()
            .iter()
            // Innermost = longest covering path.
            .filter(|d| d.covers(path))
            .max_by_key(|d| d.path.len())
            .cloned()
    }

    /// Every currently-active designation, for the control API.
    pub fn snapshot(&self) -> Vec<Designation> {
        self.active.lock().unwrap().clone()
    }

    /// The write gate's designation check for a mutation at `path`.
    ///
    /// See the module doc for the decision table. `path` should be the
    /// absolute path of the inode being mutated (or its parent, for ops
    /// that create/remove a name) — the caller resolves this once via
    /// `Meta::path_of`.
    pub async fn check(&self, path: &str) -> GateDecision {
        let Some(d) = self.covering(path) else {
            return GateDecision::NoDesignation;
        };
        if d.read_only {
            // A read guarantee only: writes are unrestricted.
            return GateDecision::NoDesignation;
        }
        if d.designee == self.node_id {
            return GateDecision::Proceed;
        }
        if self.holder.lock().unwrap().is_valid(&d.path, now_ms()) {
            return GateDecision::Proceed;
        }
        // No live delegation: ask for one if the fast path is up.
        if self.peers.is_enabled() && self.request_delegation(&d).await {
            return GateDecision::Proceed;
        }
        GateDecision::ReadOnly {
            designee: d.designee,
            path: d.path,
        }
    }

    /// Ask the designee for a delegation and record it if granted.
    async fn request_delegation(&self, d: &Designation) -> bool {
        let Some(peer) = self
            .peers
            .snapshot()
            .into_iter()
            .find(|p| p.node_id == d.designee)
        else {
            return false;
        };
        let payload = constellation_net::Payload::DelegationRequest {
            path: d.path.clone(),
            requester: self.node_id,
        };
        let reply = tokio::time::timeout(
            DELEGATION_REQUEST_TIMEOUT,
            self.peers.request_raw(peer.addr, &payload),
        )
        .await;
        match reply {
            Ok(Ok(constellation_net::Payload::DelegationGrant {
                path,
                granted: true,
                ..
            })) if path == d.path => {
                self.holder.lock().unwrap().record(
                    &constellation_net::Payload::DelegationGrant {
                        path: d.path.clone(),
                        epoch: 0,
                        ttl_ms: constellation_net::DEFAULT_DELEGATION_TTL_MS,
                        granted: true,
                    },
                    now_ms(),
                );
                true
            }
            _ => false,
        }
    }

    /// Designee side: answer an inbound `DelegationRequest`. Only grants
    /// if this node actually holds a (non-released) designation
    /// covering `path` — a stale or forged request must not manufacture
    /// authority.
    pub fn handle_delegation_request(
        &self,
        path: &str,
        requester: u64,
    ) -> constellation_net::Payload {
        let Some(d) = self.covering(path) else {
            return constellation_net::DelegationGranter::decline(path);
        };
        if d.designee != self.node_id || d.read_only {
            return constellation_net::DelegationGranter::decline(path);
        }
        tracing::info!(path, requester, "granted a delegation");
        self.granter.lock().unwrap().grant(
            path,
            requester,
            constellation_net::DEFAULT_DELEGATION_TTL_MS,
            now_ms(),
        )
    }

    /// If `path` is designated to someone else, wait (bounded) for their
    /// ack of `seq`/`part` before the caller considers the flush
    /// published.
    ///
    /// This is the other half of the designee invariant: it is not
    /// enough that a delegation was live when the write happened — the
    /// designee must actually have *seen* the record before the writer
    /// treats it as durable, or the designee could resume writing (after
    /// losing S3 itself) from a view that is missing it. A timeout does
    /// NOT fail the caller: the record stays journaled and the caller
    /// retries on the next sync round, per the plan's documented
    /// trade-off (visibility delay, never a failed write).
    pub async fn await_flush_ack(&self, path: &str, part: &str, seq: u64) -> bool {
        let Some(d) = self.covering(path) else {
            return true; // no designation: nothing to ack
        };
        if d.designee == self.node_id || d.read_only {
            return true; // our own flush, or a read-only designation
        }
        let Some(peer) = self
            .peers
            .snapshot()
            .into_iter()
            .find(|p| p.node_id == d.designee)
        else {
            // Designee unreachable: DESIGN.md's documented trade-off —
            // the caller leaves the record journaled and retries.
            return false;
        };
        let payload = constellation_net::Payload::FlushAck {
            path: path.to_string(),
            part: part.to_string(),
            seq,
            acked: false,
        };
        let reply = tokio::time::timeout(
            FLUSH_ACK_TIMEOUT,
            self.peers.request_raw(peer.addr, &payload),
        )
        .await;
        matches!(
            reply,
            Ok(Ok(constellation_net::Payload::FlushAck { acked: true, .. }))
        )
    }

    pub async fn offline(&self, path: &str, read_only: bool) -> Result<String> {
        let ino = self
            .meta
            .resolve_path(path)
            .with_context(|| format!("resolving {path}"))?
            .ok_or_else(|| anyhow::anyhow!("no such path: {path}"))?;
        let d = Designation::new(path, self.node_id, read_only);
        self.store
            .create(&d)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        self.refresh().await;
        let _ = ino; // resolved only to validate the path exists
        Ok(format!(
            "designated {path} (node {}{})",
            self.node_id,
            if read_only { ", read-only" } else { "" }
        ))
    }

    /// `constellation online <path>`: release a designation. Only the
    /// designee may release its own claim — DESIGN.md's "non-stealable"
    /// property extends to ending it, otherwise a network partition
    /// would let another node unilaterally strip the designee's
    /// authority out from under an in-flight write.
    pub async fn online(&self, path: &str) -> Result<String> {
        let (d, tag) = self
            .store
            .get(path)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .ok_or_else(|| anyhow::anyhow!("no designation at {path}"))?;
        if d.released {
            bail!("{path} is already online");
        }
        if d.designee != self.node_id {
            bail!(
                "{path} is designated to node {}, not this node ({}): \
                 only the designee can release it",
                d.designee,
                self.node_id
            );
        }
        self.store
            .release(&d, &tag)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        self.refresh().await;
        Ok(format!("{path} back online"))
    }
}
