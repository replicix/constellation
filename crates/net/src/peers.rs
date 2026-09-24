//! The P2P supervisor: what the daemon actually holds.
//!
//! Ties together the endpoint, the gossip topic, the registry-derived
//! allowlist and peer directory, and the accept loop. The daemon gets a
//! [`Peers`] handle it can call from the sync task and the FUSE lease
//! wait; every method is best-effort and cheap to call when P2P is off.
//!
//! Design rule for this whole module: **no method may block progress**.
//! Broadcasting a hint, asking for a lease, or looking up a peer all
//! either succeed quickly or give up, because the S3 path behind them is
//! what actually guarantees the operation completes.

use crate::allowlist::Decision;
use crate::endpoint::{P2p, PathKind, PeerService};
use crate::message::{read_frame, write_frame, Payload, Signed, ALPN};
use anyhow::Result;
use iroh::EndpointAddr;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How long to wait for a peer's answer to a lease request before
/// falling back to the S3 path. Generous enough for a WAN round trip,
/// short enough that it never dominates the idle-release window.
const REQUEST_TIMEOUT: Duration = Duration::from_millis(500);

/// How long after closing every connection to a dead peer incarnation
/// to send the gossip `Join` (see [`Peers::new`]).
const GOSSIP_REJOIN_DELAY: Duration = Duration::from_millis(300);

/// A holder agreed to hand `part` over — see [`Peers::request_lease`].
#[derive(Debug, Clone, Copy)]
pub struct HandoffAccepted {
    /// The highest log sequence the holder's flush shipped before
    /// releasing, if it reported one. Plan 30 §M2: the requester must
    /// tail at least this far before trusting a `completed` lookup.
    pub head_seq: Option<u64>,
}
/// A chunk includes its body, unlike the small control requests above.
/// Keep this named and single-layered so source hedging/cancellation has
/// one predictable upper bound.
const CHUNK_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Per-connection ceiling on concurrently handled streams. High enough
/// that a peer's chunk requests overlap (the cooperative cache caps its
/// own serving well below this), low enough that one peer cannot spawn
/// unbounded work.
const MAX_CONCURRENT_STREAMS: usize = 32;

/// One known peer, as learned from the node registry.
#[derive(Debug, Clone)]
pub struct Peer {
    pub node_id: u64,
    pub pubkey_hex: String,
    pub addr: EndpointAddr,
    /// Last successful round trip, for status output.
    pub rtt_ms: Option<u64>,
    pub connected: bool,
    /// How the last successful QUIC path reached this peer.
    pub path: PathKind,
    /// When we last observed this peer live (gossip neighbor or RPC).
    pub last_seen: Option<Instant>,
    pub hostname: String,
    /// Peer binary version from the registry, when published.
    pub version: String,
    pub created_unix: i64,
    pub p2p_updated_unix: Option<i64>,
    pub ro: bool,
}

/// Registry row used to (re)build the peer directory.
#[derive(Debug, Clone)]
pub struct PeerEnrollment {
    pub node_id: u64,
    pub pubkey_hex: String,
    pub addr_json: serde_json::Value,
    pub hostname: String,
    pub version: String,
    pub created_unix: i64,
    pub p2p_updated_unix: Option<i64>,
    pub ro: bool,
}

impl From<(u64, String, serde_json::Value)> for PeerEnrollment {
    fn from((node_id, pubkey_hex, addr_json): (u64, String, serde_json::Value)) -> Self {
        Self {
            node_id,
            pubkey_hex,
            addr_json,
            hostname: String::new(),
            version: String::new(),
            created_unix: 0,
            p2p_updated_unix: None,
            ro: false,
        }
    }
}

/// Handle the daemon holds. Cloneable and cheap.
#[derive(Clone)]
pub struct Peers {
    inner: Option<Arc<Inner>>,
}

/// Re-reads the node registry on demand. Supplied by the daemon, which
/// owns the object store handle.
pub type Refresher =
    Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

struct Inner {
    p2p: P2p,
    node_id: u64,
    peers: Mutex<HashMap<u64, Peer>>,
    refresher: Mutex<Option<Refresher>>,
    /// One-shot latch for the shared-node-key misconfiguration warning.
    warned_shared_key: std::sync::atomic::AtomicBool,
}

impl Peers {
    /// A disabled handle: every operation is a no-op that reports "no
    /// fast path", so callers need no `if p2p_enabled` branches.
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    pub fn new(p2p: P2p, node_id: u64) -> Self {
        let inner = Arc::new(Inner {
            p2p,
            node_id,
            peers: Mutex::new(HashMap::new()),
            refresher: Mutex::new(None),
            warned_shared_key: std::sync::atomic::AtomicBool::new(false),
        });
        // A dead pooled connection was just evicted (the peer crashed,
        // or restarted under the same key) and every connection to it
        // closed: the peer is down *on those connections*, so say so,
        // then re-dial at once. A restarted peer is back to `connected`
        // within one round trip instead of waiting for the next
        // registry-tick probe (or a gossip neighbor-up, which a restart
        // under the same key does not produce); a dead one costs one
        // failed dial. Gossip is re-formed with a `Join` once its actor
        // has seen the closes (a `Join` queued before that would go out
        // on a closing connection and be lost).
        let weak = Arc::downgrade(&inner);
        inner.p2p.set_evict_hook(Arc::new(move |endpoint| {
            let Some(inner) = weak.upgrade() else {
                return;
            };
            let node = {
                let mut map = inner.peers.lock().unwrap();
                map.values_mut().find(|p| p.addr.id == endpoint).map(|p| {
                    p.connected = false;
                    p.node_id
                })
            };
            if let (Some(node), Ok(rt)) = (node, tokio::runtime::Handle::try_current()) {
                let peers = Peers { inner: Some(inner) };
                let rejoin = peers.clone();
                rt.spawn(async move {
                    let _ = peers.ping_node(node).await;
                });
                rt.spawn(async move {
                    tokio::time::sleep(GOSSIP_REJOIN_DELAY).await;
                    if let Some(inner) = rejoin.inner.as_ref() {
                        if let Err(error) = inner.p2p.rejoin(endpoint).await {
                            tracing::debug!(%error, node, "gossip rejoin failed");
                        }
                    }
                });
            }
        }));
        Self { inner: Some(inner) }
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    pub fn node_addr(&self) -> Option<EndpointAddr> {
        Some(self.inner.as_ref()?.p2p.addr())
    }

    pub fn pubkey_hex(&self) -> Option<String> {
        Some(self.inner.as_ref()?.p2p.pubkey_hex())
    }

    /// Relay policy label, or `"off"` when P2P is disabled.
    pub fn relay_label(&self) -> String {
        self.inner
            .as_ref()
            .map(|i| i.p2p.relay_label().to_string())
            .unwrap_or_else(|| "off".into())
    }

    /// Join the gossip topic and return the receiver to drive.
    pub async fn join_topic(
        &self,
        bootstrap: Vec<iroh::EndpointId>,
    ) -> Result<iroh_gossip::api::GossipReceiver> {
        let inner = self
            .inner
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("P2P disabled"))?;
        inner.p2p.join(bootstrap).await
    }

    /// Refresh the peer directory and the accept-time allowlist from the
    /// registry. Each record is a [`PeerEnrollment`].
    pub fn refresh_registry(&self, records: impl IntoIterator<Item = impl Into<PeerEnrollment>>) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        let records: Vec<PeerEnrollment> = records.into_iter().map(Into::into).collect();
        let own_key = inner.p2p.pubkey_hex();
        let mut allowed = Vec::with_capacity(records.len());
        let mut peers = HashMap::new();
        for rec in records {
            allowed.push(rec.pubkey_hex.clone());
            if rec.node_id == inner.node_id {
                continue; // never dial ourselves
            }
            // A *different* node advertising *our* key is a fatal
            // misconfiguration for the fast path: iroh refuses to dial
            // its own endpoint id, so every forward/handoff/coop fetch
            // to that peer fails and the mount silently degrades to
            // S3-polling with full idle-release/TTL waits. Seen when
            // several mounts on one host share the default per-user
            // key path. Warn loudly, once.
            if rec.pubkey_hex == own_key
                && !inner
                    .warned_shared_key
                    .swap(true, std::sync::atomic::Ordering::Relaxed)
            {
                tracing::warn!(
                    peer_node = rec.node_id,
                    our_node = inner.node_id,
                    pubkey = %own_key,
                    "peer registered with OUR node key: P2P to it cannot work \
                     (dialing ourself). Give every node its own key, e.g. via \
                     CONSTELLATION_NODE_KEY; only the S3 slow path will be used."
                );
            }
            // An unparseable address just means we cannot dial that peer
            // yet; it stays on the allowlist so it may still dial us.
            if let Ok(addr) = serde_json::from_value::<EndpointAddr>(rec.addr_json) {
                // Feed the address to iroh so gossip, which bootstraps
                // from bare endpoint ids, can dial this peer at all.
                inner.p2p.learn_addr(addr.clone());
                let prev = inner.peers.lock().unwrap().get(&rec.node_id).cloned();
                // The peer re-published its record: a new mount (a
                // restart, possibly under the same key after a crash).
                // Any connection we pool to it may lead to the dead
                // previous incarnation; have it probed now.
                if let Some(prev) = prev.as_ref() {
                    if prev.addr.id == addr.id
                        && (prev.addr != addr || prev.p2p_updated_unix != rec.p2p_updated_unix)
                    {
                        inner.p2p.suspect_restart(addr.id);
                    }
                }
                peers.insert(
                    rec.node_id,
                    Peer {
                        node_id: rec.node_id,
                        pubkey_hex: rec.pubkey_hex,
                        addr,
                        rtt_ms: prev.as_ref().and_then(|p| p.rtt_ms),
                        connected: prev.as_ref().map(|p| p.connected).unwrap_or(false),
                        path: prev.as_ref().map(|p| p.path).unwrap_or(PathKind::Unknown),
                        last_seen: prev.and_then(|p| p.last_seen),
                        hostname: rec.hostname,
                        version: rec.version,
                        created_unix: rec.created_unix,
                        p2p_updated_unix: rec.p2p_updated_unix,
                        ro: rec.ro,
                    },
                );
            }
        }
        inner.p2p.set_allowed(allowed);
        *inner.peers.lock().unwrap() = peers;
    }

    /// Install the callback used to re-read the registry when an unknown
    /// key connects. Without it a node that mounted before its peers
    /// rejects them until the next periodic refresh, which in practice
    /// means the gossip topic never forms on a cold start.
    pub fn set_refresher(&self, refresher: Refresher) {
        if let Some(inner) = self.inner.as_ref() {
            *inner.refresher.lock().unwrap() = Some(refresher);
        }
    }

    /// Plan 30 §M4: the open paths of this node's pooled connection to
    /// `node_id` (see `crate::paths`), without waiting.
    pub fn path_summary(&self, node_id: u64) -> Option<crate::paths::PathSummary> {
        let inner = self.inner.as_ref()?;
        let id = inner.peers.lock().unwrap().get(&node_id)?.addr.id;
        inner.p2p.path_summary_now(id)
    }

    pub fn snapshot(&self) -> Vec<Peer> {
        let Some(inner) = self.inner.as_ref() else {
            return Vec::new();
        };
        let mut v: Vec<Peer> = inner.peers.lock().unwrap().values().cloned().collect();
        v.sort_by_key(|p| p.node_id);
        v
    }

    /// Is `peer` really this node? Either its node id is ours, or it was
    /// enrolled under our own endpoint key (several mounts sharing one
    /// node key, see the warning in [`Peers::refresh_registry`]). A dial
    /// to it can only fail with "connecting to ourself", so nothing that
    /// initiates traffic from the roster should pick it.
    pub fn is_self(&self, peer: &Peer) -> bool {
        let Some(inner) = self.inner.as_ref() else {
            return false;
        };
        peer.node_id == inner.node_id
            || peer.addr.id.as_bytes() == inner.p2p.secret_key().public().as_bytes()
    }

    /// [`Peers::is_self`] by node id: our own id, or a roster entry that
    /// shares our endpoint key.
    pub fn is_self_node(&self, node_id: u64) -> bool {
        let Some(inner) = self.inner.as_ref() else {
            return false;
        };
        if node_id == inner.node_id {
            return true;
        }
        let peer = inner.peers.lock().unwrap().get(&node_id).cloned();
        peer.is_some_and(|p| self.is_self(&p))
    }

    /// [`Peers::snapshot`] without entries that are really this node:
    /// the roster to initiate traffic from.
    pub fn remote_snapshot(&self) -> Vec<Peer> {
        let mut v = self.snapshot();
        v.retain(|p| !self.is_self(p));
        v
    }

    /// Best-effort gossip of an already-signed-capable payload (digests,
    /// segment hints). Failure is fine: peers will learn on the next
    /// snapshot or S3 poll.
    pub async fn gossip(&self, payload: Payload) -> Result<()> {
        let Some(inner) = self.inner.as_ref() else {
            anyhow::bail!("P2P disabled");
        };
        inner.p2p.broadcast(&payload).await
    }

    /// Fetch one content-addressed chunk from `node_id`. `None` means
    /// the peer declined (busy, miss, dirty); an error means the
    /// transport failed.
    pub async fn request_chunk(
        &self,
        node_id: u64,
        hash: &[u8; 32],
    ) -> Result<std::result::Result<crate::endpoint::ChunkFetch, crate::message::ChunkDecline>>
    {
        self.request_chunk_with_timeout(node_id, hash, CHUNK_REQUEST_TIMEOUT)
            .await
    }

    async fn request_chunk_with_timeout(
        &self,
        node_id: u64,
        hash: &[u8; 32],
        timeout: Duration,
    ) -> Result<std::result::Result<crate::endpoint::ChunkFetch, crate::message::ChunkDecline>>
    {
        let Some(inner) = self.inner.as_ref() else {
            anyhow::bail!("P2P disabled");
        };
        let addr = inner
            .peers
            .lock()
            .unwrap()
            .get(&node_id)
            .map(|p| p.addr.clone())
            .ok_or_else(|| anyhow::anyhow!("peer {node_id} has no endpoint"))?;
        match tokio::time::timeout(timeout, inner.p2p.request_chunk(addr.clone(), hash)).await {
            Ok(Ok(Ok(fetch))) => {
                let rtt = fetch.rtt.unwrap_or(Duration::ZERO);
                self.note_rtt(node_id, rtt, true, fetch.path);
                Ok(Ok(fetch))
            }
            Ok(Ok(Err(why))) => {
                // Soft miss: path still observed if the connection is live.
                let path = inner.p2p.path_kind(addr.id).await;
                if path != PathKind::Unknown {
                    self.note_path(node_id, path);
                }
                Ok(Err(why))
            }
            Ok(Err(e)) => {
                self.note_rtt(node_id, Duration::ZERO, false, PathKind::Unknown);
                Err(e)
            }
            Err(_) => {
                self.note_rtt(node_id, timeout, false, PathKind::Unknown);
                anyhow::bail!("chunk request to {node_id} timed out")
            }
        }
    }

    /// Tell peers a segment is durable so they tail now instead of at
    /// their next poll. Failure is fine: the poll still happens. Plan 30
    /// §M7: a hint only; the segment itself reaches the holder's
    /// subscribers on their log streams.
    pub async fn announce_segment(&self, part: &str, seq: u64, epoch: u64) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        let payload = Payload::SegmentPublished {
            part: part.to_string(),
            seq,
            epoch,
        };
        match inner.p2p.broadcast(&payload).await {
            Ok(()) => tracing::debug!(part, seq, epoch, "announced segment to peers"),
            Err(e) => {
                tracing::debug!(error = %e, part, seq, "segment announce failed; peers will poll")
            }
        }
    }

    /// Plan 30 §M7: subscribe to `node_id`'s log stream from `from` (see
    /// [`crate::P2p::open_log_stream`]).
    pub async fn subscribe_log(
        &self,
        node_id: u64,
        req_id: u64,
        from: u64,
    ) -> Result<tokio::sync::mpsc::Receiver<crate::endpoint::LogEvent>> {
        let inner = self
            .inner
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("P2P is off"))?;
        let addr = self
            .snapshot()
            .into_iter()
            .find(|p| p.node_id == node_id)
            .ok_or_else(|| anyhow::anyhow!("no address for node {node_id}"))?
            .addr;
        let started = std::time::Instant::now();
        let payload = Payload::LogSubscribe {
            part: "p0".to_string(),
            requester: inner.node_id,
            req_id,
            from,
        };
        let id = addr.id;
        let result = inner.p2p.open_log_stream(addr, &payload).await;
        let path = inner.p2p.path_kind(id).await;
        self.note_rtt(node_id, started.elapsed(), result.is_ok(), path);
        result
    }

    pub async fn announce_condemned(&self, epoch: u64) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        if let Err(error) = inner
            .p2p
            .broadcast(&Payload::CondemnedPublished { epoch })
            .await
        {
            tracing::debug!(%error, epoch, "condemned announce failed; writers read S3");
        }
    }

    /// Ask whoever holds `part` to hand the lease over.
    ///
    /// `Some(accepted)` only when a holder said it flushed and released,
    /// so the caller should attempt its CAS immediately —
    /// `accepted.head_seq` is the highest sequence that flush shipped
    /// (plan 30 §M2 coverage rule: the caller must tail at least this
    /// far, not just run its ordinary claim-time tail, before treating
    /// any `completed` lookup as authoritative — see
    /// `node_runtime.rs`'s `SyncRequest::Acquire` arm). Everything else —
    /// no peers, no answer, a decline, a forged reply — returns `None`
    /// and leaves the caller on the S3 path.
    ///
    /// When `holder_id` is known, that peer is asked first.
    pub async fn request_lease(
        &self,
        part: &str,
        holder_id: Option<u64>,
    ) -> Option<HandoffAccepted> {
        let inner = self.inner.as_ref()?;
        let mut peers = self.snapshot();
        if peers.is_empty() {
            return None;
        }
        if let Some(id) = holder_id {
            peers.sort_by_key(|p| if p.node_id == id { 0u8 } else { 1 });
        }
        let payload = Payload::LeaseRequest {
            part: part.to_string(),
            requester: inner.node_id,
        };
        for peer in peers {
            let started = Instant::now();
            let reply = tokio::time::timeout(
                REQUEST_TIMEOUT,
                inner.p2p.request(peer.addr.clone(), &payload),
            )
            .await;
            match reply {
                Ok(Ok(body)) => {
                    let path = inner.p2p.path_kind(peer.addr.id).await;
                    self.note_rtt(peer.node_id, started.elapsed(), true, path);
                    if crate::interpret_reply(part, &body) == crate::RequestOutcome::ClaimNow {
                        tracing::info!(
                            part,
                            holder = peer.node_id,
                            took_ms = started.elapsed().as_millis(),
                            "peer handed the lease over"
                        );
                        let head_seq = match &body {
                            Payload::LeaseHandoff { head_seq, .. } => *head_seq,
                            _ => None,
                        };
                        return Some(HandoffAccepted { head_seq });
                    }
                }
                Ok(Err(e)) => {
                    tracing::debug!(error = %e, peer = peer.node_id, part, "lease request failed");
                    self.note_rtt(peer.node_id, started.elapsed(), false, PathKind::Unknown);
                }
                Err(_) => {
                    tracing::debug!(peer = peer.node_id, part, "lease request timed out");
                    self.note_rtt(peer.node_id, started.elapsed(), false, PathKind::Unknown);
                }
            }
        }
        None
    }

    fn note_rtt(&self, node_id: u64, took: Duration, ok: bool, path: PathKind) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        if let Some(p) = inner.peers.lock().unwrap().get_mut(&node_id) {
            p.connected = ok;
            if ok {
                if !took.is_zero() {
                    p.rtt_ms = Some(took.as_millis() as u64);
                }
                if path != PathKind::Unknown {
                    p.path = path;
                }
                p.last_seen = Some(Instant::now());
            }
        }
    }

    fn note_path(&self, node_id: u64, path: PathKind) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        if let Some(p) = inner.peers.lock().unwrap().get_mut(&node_id) {
            if path != PathKind::Unknown {
                p.path = path;
            }
        }
    }

    /// Reflect gossip membership in the peer directory so status/UI do
    /// not wait for an opportunistic lease/chunk RPC to flip `connected`.
    fn mark_neighbor(&self, endpoint: iroh::EndpointId, up: bool) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        let mut map = inner.peers.lock().unwrap();
        if let Some(p) = map.values_mut().find(|p| p.addr.id == endpoint) {
            p.connected = up;
            if up {
                p.last_seen = Some(Instant::now());
            }
        }
    }

    /// Ping every known peer in parallel so the UI gets fresh RTT and
    /// connected flags even when nothing else is talking over P2P.
    pub async fn probe_all(&self) {
        let ids: Vec<u64> = self.snapshot().into_iter().map(|p| p.node_id).collect();
        if ids.is_empty() {
            return;
        }
        let mut set = tokio::task::JoinSet::new();
        for id in ids {
            let this = self.clone();
            set.spawn(async move {
                let _ = this.ping_node(id).await;
            });
        }
        while set.join_next().await.is_some() {}
    }

    /// Send an arbitrary payload directly to a known peer address and
    /// wait for its reply, with the same RTT bound as
    /// [`Peers::request_lease`]. For requests that are not one of the
    /// built-in convenience methods (e.g. `DelegationRequest`).
    pub async fn request_raw(&self, addr: EndpointAddr, payload: &Payload) -> Result<Payload> {
        self.request_raw_timeout(addr, payload, REQUEST_TIMEOUT)
            .await
    }

    /// [`Peers::request_raw`] with a caller-chosen bound, for background
    /// exchanges (cache reconciliation) that must tolerate a WAN round
    /// trip plus a cold QUIC dial rather than the lease path's 500 ms.
    pub async fn request_raw_timeout(
        &self,
        addr: EndpointAddr,
        payload: &Payload,
        timeout: Duration,
    ) -> Result<Payload> {
        let Some(inner) = self.inner.as_ref() else {
            anyhow::bail!("P2P disabled");
        };
        let node_id = inner
            .peers
            .lock()
            .unwrap()
            .values()
            .find(|p| p.addr.id == addr.id)
            .map(|p| p.node_id);
        let started = Instant::now();
        let result = tokio::time::timeout(timeout, inner.p2p.request(addr.clone(), payload)).await;
        match result {
            Ok(Ok(reply)) => {
                if let Some(id) = node_id {
                    let path = inner.p2p.path_kind(addr.id).await;
                    self.note_rtt(id, started.elapsed(), true, path);
                }
                Ok(reply)
            }
            Ok(Err(e)) => {
                if let Some(id) = node_id {
                    self.note_rtt(id, started.elapsed(), false, PathKind::Unknown);
                }
                Err(e)
            }
            Err(_) => {
                if let Some(id) = node_id {
                    self.note_rtt(id, started.elapsed(), false, PathKind::Unknown);
                }
                anyhow::bail!("request timed out")
            }
        }
    }

    /// Whether an open QUIC connection to `node_id` is pooled right now
    /// (see [`crate::endpoint::P2p::connection_alive`]); `false` with P2P
    /// disabled or the node unknown.
    pub async fn connection_alive(&self, node_id: u64) -> bool {
        let Some(inner) = self.inner.as_ref() else {
            return false;
        };
        let id = inner.peers.lock().unwrap().get(&node_id).map(|p| p.addr.id);
        match id {
            Some(id) => inner.p2p.connection_alive(id).await,
            None => false,
        }
    }

    /// Direct request to a registry-known node id.
    pub async fn request_to_node(&self, node_id: u64, payload: &Payload) -> Result<Payload> {
        let addr = self
            .snapshot()
            .into_iter()
            .find(|p| p.node_id == node_id)
            .ok_or_else(|| anyhow::anyhow!("no address for node {node_id}"))?
            .addr;
        self.request_raw(addr, payload).await
    }

    /// [`Peers::request_to_node`] with a caller-chosen bound.
    pub async fn request_to_node_timeout(
        &self,
        node_id: u64,
        payload: &Payload,
        timeout: Duration,
    ) -> Result<Payload> {
        let addr = self
            .snapshot()
            .into_iter()
            .find(|p| p.node_id == node_id)
            .ok_or_else(|| anyhow::anyhow!("no address for node {node_id}"))?
            .addr;
        self.request_raw_timeout(addr, payload, timeout).await
    }

    /// Registry node id enrolled under `pubkey_hex`, if any. Used to
    /// refuse gossip that claims to speak for a different node.
    pub fn node_id_for_key(&self, pubkey_hex: &str) -> Option<u64> {
        let inner = self.inner.as_ref()?;
        let peers = inner.peers.lock().unwrap();
        if let Some(p) = peers
            .values()
            .find(|p| p.pubkey_hex.eq_ignore_ascii_case(pubkey_hex))
        {
            return Some(p.node_id);
        }
        drop(peers);
        inner
            .p2p
            .pubkey_hex()
            .eq_ignore_ascii_case(pubkey_hex)
            .then_some(inner.node_id)
    }

    /// Liveness probe used by continuation epochs. Failure is a missing
    /// member, not a safety input on its own — the persisted promise is.
    pub async fn ping_node(&self, node_id: u64) -> bool {
        let inner = match self.inner.as_ref() {
            Some(i) => i,
            None => return false,
        };
        matches!(
            self.request_to_node(
                node_id,
                &Payload::Ping {
                    node_id: inner.node_id
                }
            )
            .await,
            Ok(Payload::Pong { .. })
        )
    }

    pub async fn announce_epoch_activate(
        &self,
        epoch_id: &str,
        members: &[u64],
        base: &[(String, u64)],
    ) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        let payload = Payload::EpochActivate {
            epoch_id: epoch_id.to_string(),
            members: members.to_vec(),
            base: base.to_vec(),
        };
        let _ = inner.p2p.broadcast(&payload).await;
        for p in self.snapshot() {
            let _ = self.request_raw(p.addr, &payload).await;
        }
    }

    /// Serve inbound direct requests until shutdown.
    ///
    /// Uses iroh's [`Router`] so connections are dispatched **by ALPN**: a
    /// bare `endpoint.accept()` loop here would also swallow
    /// `iroh-gossip`'s connections and reject them, which silently kills
    /// the gossip mesh while leaving direct requests working.
    pub async fn serve<S: PeerService>(&self, service: Arc<S>) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        let handler = DirectHandler {
            inner: inner.clone(),
            service,
        };
        let router = iroh::protocol::Router::builder(inner.p2p.endpoint().clone())
            .accept(ALPN, handler)
            .accept(iroh_gossip::ALPN, inner.p2p.gossip().clone())
            .spawn();
        // Hold the router for the daemon's lifetime; dropping it would
        // abort the accept loop.
        std::mem::forget(router);
        std::future::pending::<()>().await
    }
}

/// Handles our own ALPN: verify, dispatch, reply.
struct DirectHandler<S: PeerService> {
    inner: Arc<Inner>,
    service: Arc<S>,
}

// `ProtocolHandler` requires `Debug`; neither the endpoint nor the
// service is usefully printable, so keep it minimal.
impl<S: PeerService> std::fmt::Debug for DirectHandler<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DirectHandler")
    }
}

impl<S: PeerService> iroh::protocol::ProtocolHandler for DirectHandler<S> {
    fn accept(
        &self,
        conn: iroh::endpoint::Connection,
    ) -> impl std::future::Future<Output = std::result::Result<(), iroh::protocol::AcceptError>> + Send
    {
        let inner = self.inner.clone();
        let service = self.service.clone();
        async move {
            if let Err(e) = handle_conn(&inner, &service, conn).await {
                tracing::debug!(error = %e, "peer connection ended with an error");
            }
            Ok(())
        }
    }
}

/// Consume gossip events, dispatching verified `SegmentPublished` hints
/// to the service. Returns when the topic ends.
///
/// Unsigned, forged, or unauthorized messages are dropped: gossip is
/// forwarded by third parties, so the sender of a datagram is not
/// necessarily its author.
pub async fn run_gossip<S: PeerService>(
    peers: Peers,
    mut rx: iroh_gossip::api::GossipReceiver,
    service: Arc<S>,
) {
    use futures_lite::StreamExt;
    let Some(inner) = peers.inner.clone() else {
        return;
    };
    tracing::info!("gossip receive loop started");
    while let Some(event) = rx.next().await {
        let Ok(event) = event else {
            tracing::warn!("gossip event stream error; continuing");
            continue;
        };
        let msg = match event {
            iroh_gossip::api::Event::NeighborUp(endpoint) => {
                let (node_id, peer) = gossip_peer_label(&peers, endpoint);
                tracing::info!(%endpoint, ?node_id, %peer, "gossip neighbor joined");
                peers.mark_neighbor(endpoint, true);
                if let Some(id) = node_id {
                    let peers = peers.clone();
                    tokio::spawn(async move {
                        let _ = peers.ping_node(id).await;
                    });
                }
                continue;
            }
            iroh_gossip::api::Event::NeighborDown(endpoint) => {
                let (node_id, peer) = gossip_peer_label(&peers, endpoint);
                tracing::info!(%endpoint, ?node_id, %peer, "gossip neighbor left or lost");
                peers.mark_neighbor(endpoint, false);
                continue;
            }
            iroh_gossip::api::Event::Lagged => {
                tracing::warn!("gossip receiver lagged; some messages were missed");
                continue;
            }
            iroh_gossip::api::Event::Received(msg) => msg,
        };
        // Gossip carries whole messages, so the length prefix used for
        // stream framing is not present here — decode the payload
        // directly.
        let Ok(signed) = Signed::decode(&msg.content) else {
            tracing::debug!(bytes = msg.content.len(), "undecodable gossip message");
            continue;
        };
        let Ok((author, payload)) = signed.verify() else {
            tracing::debug!("dropping gossip message with a bad signature");
            continue;
        };
        let hex = crate::identity::hex32(author.as_bytes());
        if inner.p2p.check(&hex) == Decision::Refresh {
            let refresher = inner.refresher.lock().unwrap().clone();
            if let Some(refresh) = refresher {
                refresh().await;
            }
        }
        if inner.p2p.check(&hex) != Decision::Accept {
            tracing::debug!(peer = %hex, "dropping gossip from an unenrolled key");
            continue;
        }
        match &payload {
            Payload::SegmentPublished { part, seq, epoch } => {
                service.segment_published(part, *seq, *epoch);
            }
            Payload::MutateRequest {
                part,
                requester,
                req_id,
                epoch_seen,
                op,
                rid,
                acked_through,
            } => {
                let _ = service
                    .mutate_requested(
                        part.clone(),
                        *requester,
                        *req_id,
                        *epoch_seen,
                        op.clone(),
                        *rid,
                        *acked_through,
                    )
                    .await;
            }
            Payload::LeaseOffer { part, epoch } => {
                service.lease_offered(part.clone(), *epoch);
            }
            Payload::PeerRtts { node_id, rtts } => {
                service.peer_rtts(*node_id, rtts.clone());
            }
            Payload::CondemnedPublished { epoch } => {
                tracing::debug!(epoch, "GC condemned pointer was published");
            }
            Payload::CacheDigest {
                node_id,
                generation,
                bits,
                nbits,
                k,
                n,
                bucket,
                buckets,
            } => {
                service.cache_digest(crate::endpoint::DigestSnapshot {
                    node_id: *node_id,
                    generation: *generation,
                    bits: bits.clone(),
                    nbits: *nbits,
                    k: *k,
                    n: *n,
                    bucket: *bucket,
                    buckets: *buckets,
                });
            }
            Payload::CacheDigestDelta {
                node_id,
                generation,
                adds,
                buckets,
            } => {
                service.cache_digest_delta(crate::endpoint::DigestDelta {
                    node_id: *node_id,
                    generation: *generation,
                    adds: adds.clone(),
                    buckets: *buckets,
                });
            }
            Payload::CacheSummary { node_id, summary } => {
                // Exact mirrors are only worth anything if a summary
                // speaks for its real author: an enrolled peer must not
                // be able to rewrite another node's advertised set.
                if peers.node_id_for_key(&hex).is_some_and(|id| id == *node_id) {
                    service.cache_summary(*node_id, *summary);
                }
            }
            Payload::CacheSetDelta { node_id, delta } => {
                if peers.node_id_for_key(&hex).is_some_and(|id| id == *node_id) {
                    service.cache_set_delta(*node_id, delta.clone());
                }
            }
            Payload::EpochActivate {
                epoch_id,
                members,
                base,
            } => {
                service.epoch_activated(epoch_id.clone(), members.clone(), base.clone());
            }
            _ => {}
        }
    }
    tracing::info!("gossip receive loop ended");
}

/// Resolve a gossip endpoint to `(node_id, short pubkey hex)` when the
/// registry has already enrolled it; otherwise `node_id` is `None`.
fn gossip_peer_label(peers: &Peers, endpoint: iroh::EndpointId) -> (Option<u64>, String) {
    let hex = crate::identity::hex32(endpoint.as_bytes());
    let short = hex.get(..12).unwrap_or(hex.as_str()).to_string();
    let node_id = peers
        .snapshot()
        .into_iter()
        .find(|p| p.addr.id == endpoint)
        .map(|p| p.node_id);
    (node_id, short)
}

/// Serve one inbound connection: enforce the allowlist, then answer
/// frames until the peer goes away.
async fn handle_conn<S: PeerService>(
    inner: &Arc<Inner>,
    service: &Arc<S>,
    conn: iroh::endpoint::Connection,
) -> Result<()> {
    let remote = conn.remote_id();
    let remote_key = *remote.as_bytes();
    let hex = crate::identity::hex32(remote.as_bytes());
    // Accept-time authorization: the key must be enrolled in the
    // registry, which requires bucket write. A miss re-reads the registry
    // once (rate-limited) before rejecting, because a peer that mounted
    // after us is legitimately absent from our cached view — on a cold
    // start that is the common case, not the exception.
    if inner.p2p.check(&hex) == Decision::Refresh {
        let refresher = inner.refresher.lock().unwrap().clone();
        if let Some(refresh) = refresher {
            refresh().await;
        }
    }
    if inner.p2p.check(&hex) != Decision::Accept {
        tracing::warn!(peer = %hex, "rejecting peer: not in the registry allowlist");
        conn.close(1u32.into(), b"not allowed");
        return Ok(());
    }
    // Streams run concurrently rather than one-at-a-time. A chunk
    // request costs a disk read plus a multi-megabyte transfer, so
    // serializing behind `accept_bi` would both stall unrelated requests
    // and make the cooperative cache's serving budget (`cli::coop`)
    // unreachable — it could never see more than one serve in flight.
    // The semaphore keeps the resulting concurrency bounded per peer.
    let slots = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_STREAMS));
    loop {
        let (send, recv) = match conn.accept_bi().await {
            Ok(pair) => pair,
            // Normal close.
            Err(_) => return Ok(()),
        };
        let Ok(permit) = slots.clone().acquire_owned().await else {
            return Ok(());
        };
        let inner = inner.clone();
        let service = service.clone();
        let hex = hex.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let r = handle_stream(&inner, service.as_ref(), &remote_key, &hex, send, recv).await;
            if let Err(e) = r {
                tracing::debug!(peer = %hex, error = %e, "peer stream ended with an error");
            }
        });
    }
}

/// One request/response exchange on its own bidirectional stream.
///
/// Errors here are scoped to the stream: a malformed or unauthorized
/// frame drops that exchange and leaves the connection serving.
async fn handle_stream<S: PeerService>(
    inner: &Arc<Inner>,
    service: &S,
    remote_key: &[u8; 32],
    hex: &str,
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
) -> Result<()> {
    let req = read_frame(&mut recv).await?;
    let (author, payload) = req.verify()?;
    // The signer must be the peer we authorized, so an allowed peer
    // cannot relay a third party's request through its connection.
    if author.as_bytes() != remote_key {
        tracing::warn!(peer = %hex, "dropping frame signed by a different key");
        return Ok(());
    }
    let reply = match payload {
        Payload::SegmentPublished { part, seq, epoch } => {
            service.segment_published(&part, seq, epoch);
            None
        }
        Payload::LogSubscribe {
            requester,
            req_id,
            from,
            ..
        } => {
            serve_log_stream(inner, service, hex, requester, req_id, from, &mut send).await?;
            let _ = send.finish();
            return Ok(());
        }
        Payload::CondemnedPublished { .. } => None,
        Payload::LeaseRequest { part, requester } => {
            Some(service.lease_requested(part, requester).await)
        }
        Payload::MutateRequest {
            part,
            requester,
            req_id,
            epoch_seen,
            op,
            rid,
            acked_through,
        } => Some(
            service
                .mutate_requested(part, requester, req_id, epoch_seen, op, rid, acked_through)
                .await,
        ),
        Payload::LeaseOffer { part, epoch } => {
            service.lease_offered(part, epoch);
            None
        }
        Payload::PeerRtts { node_id, rtts } => {
            service.peer_rtts(node_id, rtts);
            None
        }
        Payload::DelegationRequest { path, requester } => {
            Some(service.delegation_requested(path, requester).await)
        }
        Payload::FlushAck {
            path, part, seq, ..
        } => {
            // Inbound `FlushAck` on this ALPN is a *request* for an
            // ack (see the module doc on `Payload::FlushAck`): the
            // field name is shared with the reply for symmetry, but
            // an incoming message's `acked` is meaningless — only
            // the reply's `acked` matters.
            Some(service.flush_ack_requested(path, part, seq).await)
        }
        Payload::Ping { .. } => Some(Payload::Pong {
            node_id: service.node_id(),
        }),
        Payload::EpochPropose {
            epoch_id,
            members,
            base,
            proposer,
        } => Some(
            service
                .epoch_proposed(epoch_id, members, base, proposer)
                .await,
        ),
        Payload::EpochActivate {
            epoch_id,
            members,
            base,
        } => {
            service.epoch_activated(epoch_id.clone(), members, base);
            Some(Payload::EpochAck {
                epoch_id,
                member: service.node_id(),
                accepted: true,
            })
        }
        Payload::ReconcileRequest { queries } => Some(service.reconcile_requested(queries).await),
        Payload::ChunkRequest { hash } => {
            let served = service.serve_chunk(hash, hex.to_string()).await;
            let status = match &served {
                Ok(_) => crate::message::ChunkStatus::Found,
                Err(why) => crate::message::ChunkStatus::Declined(*why),
            };
            let reply = Payload::ChunkResponse { hash, status };
            let signed = Signed::new(inner.p2p.secret_key(), &reply)?;
            write_frame(&mut send, &signed).await?;
            if let Ok(bytes) = served {
                use tokio::io::AsyncWriteExt;
                send.write_u64(bytes.len() as u64).await?;
                send.write_all(&bytes).await?;
            }
            let _ = send.finish();
            return Ok(());
        }
        Payload::Pong { .. }
        | Payload::LeaseHandoff { .. }
        | Payload::DelegationGrant { .. }
        | Payload::EpochAck { .. }
        | Payload::CacheDigest { .. }
        | Payload::CacheDigestDelta { .. }
        | Payload::CacheSummary { .. }
        | Payload::CacheSetDelta { .. }
        | Payload::ReconcileReply { .. }
        | Payload::ChunkResponse { .. }
        | Payload::LogFrame { .. }
        | Payload::LogEnd { .. }
        | Payload::MutateReply { .. } => None,
    };
    if let Some(reply) = reply {
        let signed = Signed::new(inner.p2p.secret_key(), &reply)?;
        write_frame(&mut send, &signed).await?;
    }
    let _ = send.finish();
    Ok(())
}

/// Plan 30 §M7: write `requester`'s log stream until the service closes
/// it, the subscriber goes away, or a frame is not taken within
/// [`crate::endpoint::LOG_FRAME_WRITE_TIMEOUT`]. Every frame is a signed
/// [`Payload::LogFrame`] followed by the raw segment bytes, like a chunk
/// body; the subscriber checks the bytes against the signed hash.
#[allow(clippy::too_many_arguments)]
async fn serve_log_stream<S: PeerService>(
    inner: &Arc<Inner>,
    service: &S,
    hex: &str,
    requester: u64,
    req_id: u64,
    from: u64,
    send: &mut iroh::endpoint::SendStream,
) -> Result<()> {
    use crate::endpoint::{LogEvent, LOG_FRAME_WRITE_TIMEOUT};
    use tokio::io::AsyncWriteExt;
    let Some(mut rx) = service.log_subscribe(requester, req_id, from) else {
        let end = Signed::new(
            inner.p2p.secret_key(),
            &Payload::LogEnd {
                req_id,
                refused: true,
            },
        )?;
        write_frame(send, &end).await?;
        return Ok(());
    };
    while let Some(event) = rx.recv().await {
        let (payload, body) = match event {
            LogEvent::Frame {
                n,
                epoch,
                head,
                segment,
            } => {
                let (meta, body) = match segment {
                    Some((seq, bytes)) => {
                        let hash = *blake3::hash(&bytes).as_bytes();
                        (Some((seq, bytes.len() as u64, hash)), Some(bytes))
                    }
                    None => (None, None),
                };
                (
                    Payload::LogFrame {
                        req_id,
                        n,
                        epoch,
                        head,
                        segment: meta,
                    },
                    body,
                )
            }
            LogEvent::End { refused } => (Payload::LogEnd { req_id, refused }, None),
        };
        let end = matches!(payload, Payload::LogEnd { .. });
        let signed = Signed::new(inner.p2p.secret_key(), &payload)?;
        let write = async {
            write_frame(send, &signed).await?;
            if let Some(body) = &body {
                send.write_all(body).await?;
                send.flush().await?;
            }
            anyhow::Ok(())
        };
        match tokio::time::timeout(LOG_FRAME_WRITE_TIMEOUT, write).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::debug!(peer = %hex, error = %e, "log stream subscriber went away");
                return Ok(());
            }
            Err(_) => {
                tracing::debug!(peer = %hex, "log stream subscriber too slow; dropping it");
                let _ = send.reset(0u32.into());
                return Ok(());
            }
        }
        if end {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering as AtomicOrder};

    /// A recording [`PeerService`] so tests can assert what the endpoint
    /// dispatched.
    #[derive(Default)]
    struct Recorder {
        segments: Mutex<Vec<(String, u64, u64)>>,
        lease_asks: Mutex<Vec<(String, u64)>>,
        digests: Mutex<Vec<crate::endpoint::DigestSnapshot>>,
        /// What to answer a lease request with.
        release: bool,
        /// Plan 30 §M7: subscriptions seen, as `(requester, req_id, from)`.
        log_subs: Mutex<Vec<(u64, u64, u64)>>,
    }

    impl PeerService for Recorder {
        fn segment_published(&self, part: &str, seq: u64, epoch: u64) {
            self.segments
                .lock()
                .unwrap()
                .push((part.to_string(), seq, epoch));
        }
        fn lease_requested(
            &self,
            part: String,
            requester: u64,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
            self.lease_asks
                .lock()
                .unwrap()
                .push((part.clone(), requester));
            let released = self.release;
            Box::pin(async move {
                Payload::LeaseHandoff {
                    part,
                    epoch: 5,
                    released,
                    etag: None,
                    head_seq: None,
                }
            })
        }
        fn cache_digest(&self, digest: crate::endpoint::DigestSnapshot) {
            self.digests.lock().unwrap().push(digest);
        }
        fn node_id(&self) -> u64 {
            7
        }
        /// Serves (when `release`) a heartbeat, one segment larger than
        /// a control frame may be, and an end; refuses otherwise.
        fn log_subscribe(
            &self,
            requester: u64,
            req_id: u64,
            from: u64,
        ) -> Option<tokio::sync::mpsc::Receiver<crate::endpoint::LogEvent>> {
            use crate::endpoint::LogEvent;
            self.log_subs
                .lock()
                .unwrap()
                .push((requester, req_id, from));
            if !self.release {
                return None;
            }
            let (tx, rx) = tokio::sync::mpsc::channel(8);
            tokio::spawn(async move {
                let big = vec![0x5a; 3 * crate::message::MAX_FRAME];
                for event in [
                    LogEvent::Frame {
                        n: 0,
                        epoch: 4,
                        head: 9,
                        segment: None,
                    },
                    LogEvent::Frame {
                        n: 1,
                        epoch: 4,
                        head: 10,
                        segment: Some((10, big)),
                    },
                    LogEvent::End { refused: false },
                ] {
                    let _ = tx.send(event).await;
                }
            });
            Some(rx)
        }
    }

    async fn pair(release: bool) -> (Peers, Peers, Arc<Recorder>) {
        let topic = crate::topic_for(Some(&[3u8; 32]), "fs");
        let a = P2p::spawn(iroh::SecretKey::generate(), topic)
            .await
            .unwrap();
        let b = P2p::spawn(iroh::SecretKey::generate(), topic)
            .await
            .unwrap();
        let (a_key, b_key) = (a.pubkey_hex(), b.pubkey_hex());
        let (a_addr, b_addr) = (a.addr(), b.addr());
        let holder = Peers::new(a, 1);
        let asker = Peers::new(b, 2);
        // Each side enrolls the other, as the registry would.
        holder.refresh_registry(vec![
            (1, a_key.clone(), serde_json::to_value(&a_addr).unwrap()),
            (2, b_key.clone(), serde_json::to_value(&b_addr).unwrap()),
        ]);
        asker.refresh_registry(vec![
            (1, a_key, serde_json::to_value(&a_addr).unwrap()),
            (2, b_key, serde_json::to_value(&b_addr).unwrap()),
        ]);
        let service = Arc::new(Recorder {
            release,
            ..Default::default()
        });
        let serving = holder.clone();
        let svc = service.clone();
        tokio::spawn(async move { serving.serve(svc).await });
        (holder, asker, service)
    }

    /// Plan 30 §M7: a log stream over real QUIC: frames arrive in order,
    /// a segment bigger than any control frame comes through whole (and
    /// hash-checked), and the holder's end closes it.
    #[tokio::test]
    async fn a_log_stream_crosses_quic_in_order() {
        use crate::endpoint::LogEvent;
        let (_holder, asker, service) = pair(true).await;
        let mut rx = asker.subscribe_log(1, 42, 9).await.unwrap();
        let mut got = Vec::new();
        while let Some(event) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("log stream stalled")
        {
            got.push(event);
        }
        assert_eq!(got.len(), 3, "{got:?}");
        assert!(matches!(
            got[0],
            LogEvent::Frame {
                n: 0,
                head: 9,
                segment: None,
                ..
            }
        ));
        match &got[1] {
            LogEvent::Frame {
                n: 1,
                segment: Some((10, bytes)),
                ..
            } => assert_eq!(bytes.len(), 3 * crate::message::MAX_FRAME),
            other => panic!("{other:?}"),
        }
        assert_eq!(got[2], LogEvent::End { refused: false });
        assert_eq!(service.log_subs.lock().unwrap().as_slice(), [(2, 42, 9)]);
    }

    /// A node with no log to serve refuses at once.
    #[tokio::test]
    async fn a_refused_log_stream_says_so() {
        use crate::endpoint::LogEvent;
        let (_holder, asker, _service) = pair(false).await;
        let mut rx = asker.subscribe_log(1, 1, 1).await.unwrap();
        let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("no answer");
        assert_eq!(first, Some(LogEvent::End { refused: true }));
    }

    /// The disabled handle must be safe to call everywhere, so the
    /// daemon needs no `if p2p` branches and `CONSTELLATION_P2P=off`
    /// cannot change behaviour beyond losing the speedup.
    #[tokio::test]
    async fn disabled_handle_is_inert() {
        let p = Peers::disabled();
        assert!(!p.is_enabled());
        assert!(p.node_addr().is_none());
        assert!(p.pubkey_hex().is_none());
        assert!(p.snapshot().is_empty());
        p.refresh_registry(vec![(1, "aa".into(), serde_json::json!({}))]);
        p.announce_segment("p0", 1, 1).await;
        assert!(
            p.request_lease("p0", None).await.is_none(),
            "no fast path means the caller must use S3"
        );
    }

    /// The end-to-end fast path: the asker's request reaches the holder's
    /// service and a release turns into "claim now".
    #[tokio::test]
    async fn lease_request_reaches_the_holder_and_releases() {
        let (_holder, asker, service) = pair(true).await;
        assert!(
            asker.request_lease("p0", None).await.is_some(),
            "a released lease must tell the caller to CAS now"
        );
        assert_eq!(
            service.lease_asks.lock().unwrap().as_slice(),
            [("p0".to_string(), 2)]
        );
        // RTT is recorded for status output.
        let peer = asker
            .snapshot()
            .into_iter()
            .find(|p| p.node_id == 1)
            .unwrap();
        assert!(peer.connected && peer.rtt_ms.is_some());
    }

    /// A holder that declines must leave the caller on the S3 path.
    #[tokio::test]
    async fn declined_lease_request_keeps_the_caller_waiting() {
        let (_holder, asker, service) = pair(false).await;
        assert!(asker.request_lease("p0", None).await.is_none());
        assert_eq!(service.lease_asks.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_full_bloom_bucket_crosses_real_gossip() {
        let (holder, asker, service) = pair(false).await;
        let holder_id = holder.node_addr().unwrap().id;
        let asker_id = asker.node_addr().unwrap().id;
        let mut holder_rx = holder.join_topic(vec![asker_id]).await.unwrap();
        let mut asker_rx = asker.join_topic(vec![holder_id]).await.unwrap();
        let asker_service = Arc::new(Recorder::default());
        let serving = asker.clone();
        let svc = asker_service.clone();
        tokio::spawn(async move { serving.serve(svc).await });
        let (holder_joined, asker_joined) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(holder_rx.joined(), asker_rx.joined())
        })
        .await
        .expect("gossip peers did not join");
        holder_joined.unwrap();
        asker_joined.unwrap();
        tokio::spawn(run_gossip(holder.clone(), holder_rx, service.clone()));
        tokio::spawn(run_gossip(asker.clone(), asker_rx, asker_service));

        let bits = vec![0x5a; crate::bloom::MAX_BITS_BYTES];
        asker
            .gossip(Payload::CacheDigest {
                node_id: 2,
                generation: 1,
                nbits: (bits.len() * 8) as u64,
                k: crate::bloom::K,
                n: crate::bloom::ENTRIES_PER_BUCKET as u64,
                bits,
                bucket: 0,
                buckets: 1,
            })
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if service.digests.lock().unwrap().len() == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("full digest never crossed the gossip transport");

        // The large digest must not poison the gossip connection.
        asker
            .gossip(Payload::SegmentPublished {
                part: "after-digest".into(),
                seq: 1,
                epoch: 1,
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if service.segments.lock().unwrap().len() == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("digest killed unrelated gossip");
    }

    /// A peer that is not enrolled in the registry must be refused even
    /// though it can reach us: enrolment requires bucket write, so IAM
    /// stays the trust root.
    #[tokio::test]
    async fn unenrolled_peer_is_refused() {
        let topic = crate::topic_for(Some(&[4u8; 32]), "fs");
        let holder_p2p = P2p::spawn(iroh::SecretKey::generate(), topic)
            .await
            .unwrap();
        let holder_addr = holder_p2p.addr();
        let holder_key = holder_p2p.pubkey_hex();
        let holder = Peers::new(holder_p2p, 1);
        // Registry contains only the holder: the stranger is unknown.
        holder.refresh_registry(vec![(
            1,
            holder_key.clone(),
            serde_json::to_value(&holder_addr).unwrap(),
        )]);
        let service = Arc::new(Recorder {
            release: true,
            ..Default::default()
        });
        let serving = holder.clone();
        let svc = service.clone();
        tokio::spawn(async move { serving.serve(svc).await });

        let stranger_p2p = P2p::spawn(iroh::SecretKey::generate(), topic)
            .await
            .unwrap();
        let stranger_addr = stranger_p2p.addr();
        let stranger = Peers::new(stranger_p2p, 9);
        stranger.refresh_registry(vec![
            (1, holder_key, serde_json::to_value(&holder_addr).unwrap()),
            (
                9,
                "ff".repeat(32),
                serde_json::to_value(&stranger_addr).unwrap(),
            ),
        ]);
        assert!(
            stranger.request_lease("p0", None).await.is_none(),
            "an unenrolled peer must not get a handoff"
        );
        assert!(
            service.lease_asks.lock().unwrap().is_empty(),
            "the request must never reach the service"
        );
    }

    /// Serves one fixed chunk, recording how many serves overlapped and
    /// stalling long enough for the overlap to be observable.
    struct ChunkServer {
        data: Option<Vec<u8>>,
        delay: Duration,
        in_flight: AtomicU32,
        peak: AtomicU32,
    }

    impl ChunkServer {
        fn new(data: Option<Vec<u8>>, delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                data,
                delay,
                in_flight: AtomicU32::new(0),
                peak: AtomicU32::new(0),
            })
        }
    }

    impl PeerService for ChunkServer {
        fn segment_published(&self, _part: &str, _seq: u64, _epoch: u64) {}
        fn lease_requested(
            &self,
            part: String,
            _requester: u64,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
            Box::pin(async move {
                Payload::LeaseHandoff {
                    part,
                    epoch: 0,
                    released: false,
                    etag: None,
                    head_seq: None,
                }
            })
        }
        fn serve_chunk(
            &self,
            _hash: [u8; 32],
            _from_hex: String,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<Vec<u8>, crate::message::ChunkDecline>>
                    + Send
                    + '_,
            >,
        > {
            Box::pin(async move {
                let n = self.in_flight.fetch_add(1, AtomicOrder::SeqCst) + 1;
                self.peak.fetch_max(n, AtomicOrder::SeqCst);
                tokio::time::sleep(self.delay).await;
                self.in_flight.fetch_sub(1, AtomicOrder::SeqCst);
                self.data
                    .clone()
                    .ok_or(crate::message::ChunkDecline::Absent)
            })
        }
        fn node_id(&self) -> u64 {
            1
        }
    }

    /// Stand up a chunk-serving holder and return the asker's handle.
    async fn chunk_pair(service: Arc<ChunkServer>) -> Peers {
        let topic = crate::topic_for(Some(&[8u8; 32]), "fs");
        let a = P2p::spawn(iroh::SecretKey::generate(), topic)
            .await
            .unwrap();
        let b = P2p::spawn(iroh::SecretKey::generate(), topic)
            .await
            .unwrap();
        let (a_key, b_key) = (a.pubkey_hex(), b.pubkey_hex());
        let (a_addr, b_addr) = (a.addr(), b.addr());
        let holder = Peers::new(a, 1);
        let asker = Peers::new(b, 2);
        let registry = vec![
            (1, a_key, serde_json::to_value(&a_addr).unwrap()),
            (2, b_key, serde_json::to_value(&b_addr).unwrap()),
        ];
        holder.refresh_registry(registry.clone());
        asker.refresh_registry(registry);
        tokio::spawn(async move { holder.serve(service).await });
        asker
    }

    fn a_hash() -> [u8; 32] {
        [9u8; 32]
    }

    /// Two chunk fetches issued at once must overlap end to end.
    #[tokio::test]
    async fn concurrent_chunk_requests_are_served_in_parallel() {
        let service = ChunkServer::new(Some(vec![1u8; 4096]), Duration::from_millis(300));
        let asker = chunk_pair(service.clone()).await;
        let hash = a_hash();
        let (x, y) = tokio::join!(asker.request_chunk(1, &hash), asker.request_chunk(1, &hash));
        assert!(x.unwrap().is_ok() && y.unwrap().is_ok());
        assert_eq!(
            service.peak.load(AtomicOrder::SeqCst),
            2,
            "the two requests were serialized, not served concurrently"
        );
    }

    #[tokio::test]
    async fn sequential_and_concurrent_chunks_reuse_one_connection() {
        let service = ChunkServer::new(Some(vec![1u8; 4096]), Duration::ZERO);
        let asker = chunk_pair(service).await;
        let inner = asker.inner.as_ref().unwrap();
        let peer = inner.peers.lock().unwrap().get(&1).unwrap().addr.id;
        asker.request_chunk(1, &a_hash()).await.unwrap().unwrap();
        let first = inner.p2p.pooled_connection_id(peer).await.unwrap();
        let hash = a_hash();
        let (a, b) = tokio::join!(asker.request_chunk(1, &hash), asker.request_chunk(1, &hash));
        assert!(a.unwrap().is_ok() && b.unwrap().is_ok());
        let after = inner.p2p.pooled_connection_id(peer).await.unwrap();
        assert_eq!(first, after, "all streams should share one QUIC connection");
    }

    #[tokio::test]
    async fn a_closed_pooled_connection_is_redialed_once() {
        let service = ChunkServer::new(Some(vec![1u8; 4096]), Duration::ZERO);
        let asker = chunk_pair(service).await;
        let inner = asker.inner.as_ref().unwrap();
        let peer = inner.peers.lock().unwrap().get(&1).unwrap().addr.id;
        asker.request_chunk(1, &a_hash()).await.unwrap().unwrap();
        let first = inner.p2p.pooled_connection_id(peer).await.unwrap();
        inner.p2p.close_pooled_connection(peer).await;
        asker.request_chunk(1, &a_hash()).await.unwrap().unwrap();
        let replacement = inner.p2p.pooled_connection_id(peer).await.unwrap();
        assert_ne!(
            first, replacement,
            "a closed pooled connection must be replaced"
        );
    }

    /// A pooled connection that was closed (by the peer, or the idle
    /// timeout) must not be handed out again: the pool's own strong
    /// handle kept `weak_handle().upgrade()` succeeding, so the next
    /// request (which, unlike a chunk fetch, does not retry) failed.
    #[tokio::test]
    async fn a_closed_pooled_connection_is_not_reused_by_a_request() {
        let service = ChunkServer::new(Some(vec![1u8; 4096]), Duration::ZERO);
        let asker = chunk_pair(service).await;
        let inner = asker.inner.as_ref().unwrap();
        let peer = inner.peers.lock().unwrap().get(&1).unwrap().addr.id;
        assert!(asker.ping_node(1).await);
        let first = inner.p2p.pooled_connection_id(peer).await.unwrap();
        inner.p2p.close_pooled_connection(peer).await;
        assert!(
            asker.ping_node(1).await,
            "the first request after a close must redial, not fail"
        );
        assert_ne!(Some(first), inner.p2p.pooled_connection_id(peer).await);
    }

    /// Plan 30 §M13's "slow is not gone": requests to a peer that is
    /// merely slow to answer time out at the application level, and the
    /// liveness probe they trigger must find its QUIC stack answering —
    /// the pooled connection stays, and `connection_alive` stays true.
    #[tokio::test]
    async fn a_slow_peer_keeps_its_pooled_connection() {
        let service = ChunkServer::new(Some(vec![1u8; 4096]), Duration::from_secs(5));
        let asker = chunk_pair(service).await;
        let inner = asker.inner.as_ref().unwrap();
        let peer = inner.peers.lock().unwrap().get(&1).unwrap().addr.id;
        assert!(asker.ping_node(1).await);
        let first = inner.p2p.pooled_connection_id(peer).await.unwrap();
        let started = Instant::now();
        while started.elapsed() < crate::endpoint::PROBE_WINDOW + Duration::from_secs(1) {
            let got = asker
                .request_chunk_with_timeout(1, &a_hash(), Duration::from_millis(200))
                .await;
            assert!(got.is_err(), "the slow serve should have timed out");
        }
        assert_eq!(
            inner.p2p.pooled_connection_id(peer).await,
            Some(first),
            "a slow but live peer's connection was evicted"
        );
        assert!(asker.connection_alive(1).await);
    }

    #[tokio::test]
    async fn chunk_timeout_bounds_the_wait_and_server_work_releases() {
        let service = ChunkServer::new(Some(vec![1u8; 4096]), Duration::from_millis(150));
        let asker = chunk_pair(service.clone()).await;
        // Warm the pooled QUIC connection. Otherwise the deliberately
        // tiny timeout can expire during the first handshake before
        // server work starts, making the assertion below scheduler-
        // dependent instead of testing cancellation of an active serve.
        asker.request_chunk(1, &a_hash()).await.unwrap().unwrap();
        let started = Instant::now();
        let got = asker
            .request_chunk_with_timeout(1, &a_hash(), Duration::from_millis(20))
            .await;
        assert!(got.is_err());
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "foreground read outlived its timeout"
        );
        assert_eq!(
            service.in_flight.load(AtomicOrder::SeqCst),
            1,
            "test did not time out an active serve"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if service.in_flight.load(AtomicOrder::SeqCst) == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timed-out server work never released its serving slot");
    }

    /// The same, but both requests share one QUIC connection — which is
    /// what a peer does when its transport pools. Handling streams
    /// one-at-a-time inside the accept loop passes the test above (each
    /// dial gets its own connection) while still stalling every request
    /// behind the slowest stream here, and capping the cooperative
    /// cache's serving budget at one serve per peer.
    #[tokio::test]
    async fn two_streams_on_one_connection_are_served_concurrently() {
        let topic = crate::topic_for(Some(&[10u8; 32]), "fs");
        let holder_p2p = P2p::spawn(iroh::SecretKey::generate(), topic)
            .await
            .unwrap();
        let client_p2p = P2p::spawn(iroh::SecretKey::generate(), topic)
            .await
            .unwrap();
        let holder_addr = holder_p2p.addr();
        let holder = Peers::new(holder_p2p, 1);
        holder.refresh_registry(vec![
            (
                1,
                holder.pubkey_hex().unwrap(),
                serde_json::to_value(&holder_addr).unwrap(),
            ),
            (
                2,
                client_p2p.pubkey_hex(),
                serde_json::to_value(client_p2p.addr()).unwrap(),
            ),
        ]);
        let service = ChunkServer::new(Some(vec![5u8; 4096]), Duration::from_millis(300));
        let serving = holder.clone();
        let svc = service.clone();
        tokio::spawn(async move { serving.serve(svc).await });

        let conn = client_p2p
            .endpoint()
            .connect(holder_addr, ALPN)
            .await
            .unwrap();
        let msg = Signed::new(
            client_p2p.secret_key(),
            &Payload::ChunkRequest { hash: a_hash() },
        )
        .unwrap();
        let mut pending = Vec::new();
        for _ in 0..2 {
            let (mut send, recv) = conn.open_bi().await.unwrap();
            write_frame(&mut send, &msg).await.unwrap();
            send.finish().ok();
            pending.push(recv);
        }
        for mut recv in pending {
            let (_, body) = read_frame(&mut recv).await.unwrap().verify().unwrap();
            assert!(
                matches!(
                    body,
                    Payload::ChunkResponse {
                        status: crate::message::ChunkStatus::Found,
                        ..
                    }
                ),
                "unexpected reply {body:?}"
            );
        }
        assert_eq!(
            service.peak.load(AtomicOrder::SeqCst),
            2,
            "streams on one connection were handled one at a time"
        );
    }

    /// A decline is a clean `None`, not a transport error: the caller
    /// must be able to tell "peer said no" from "peer is broken".
    #[tokio::test]
    async fn a_declined_chunk_request_is_a_miss_not_an_error() {
        let service = ChunkServer::new(None, Duration::ZERO);
        let asker = chunk_pair(service).await;
        let got = asker.request_chunk(1, &a_hash()).await;
        assert!(
            matches!(got, Ok(Err(crate::message::ChunkDecline::Absent))),
            "expected a clean miss with its reason, got {got:?}"
        );
    }

    /// Answers reconciliation rounds from a fixed owner set.
    struct ReconServer {
        set: crate::reconcile::KeySet,
    }

    impl PeerService for ReconServer {
        fn segment_published(&self, _part: &str, _seq: u64, _epoch: u64) {}
        fn lease_requested(
            &self,
            part: String,
            _requester: u64,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
            Box::pin(async move {
                Payload::LeaseHandoff {
                    part,
                    epoch: 0,
                    released: false,
                    etag: None,
                    head_seq: None,
                }
            })
        }
        fn reconcile_requested(
            &self,
            queries: Vec<crate::reconcile::Query>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
            Box::pin(async move {
                let summary = crate::reconcile::Summary {
                    incarnation: 1,
                    seq: 1,
                    root: self.set.root_fingerprint(),
                    count: self.set.len() as u64,
                };
                Payload::ReconcileReply {
                    reply: crate::reconcile::respond(
                        &self.set,
                        summary,
                        &queries,
                        crate::reconcile::REPLY_BUDGET,
                    ),
                }
            })
        }
        fn node_id(&self) -> u64 {
            1
        }
    }

    /// A multi-round session over real QUIC frames: the initial sync of
    /// 50k keys pages through several MAX_FRAME-bounded replies and the
    /// mirror comes out exact.
    #[tokio::test]
    async fn a_reconciliation_session_converges_over_quic() {
        use crate::reconcile::{start_bits_for, KeySet, Session};
        let owner =
            KeySet::from_keys((0..50_000u64).map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15)));
        let topic = crate::topic_for(Some(&[9u8; 32]), "fs");
        let a = P2p::spawn(iroh::SecretKey::generate(), topic)
            .await
            .unwrap();
        let b = P2p::spawn(iroh::SecretKey::generate(), topic)
            .await
            .unwrap();
        let (a_key, b_key) = (a.pubkey_hex(), b.pubkey_hex());
        let (a_addr, b_addr) = (a.addr(), b.addr());
        let holder = Peers::new(a, 1);
        let asker = Peers::new(b, 2);
        let registry = vec![
            (1, a_key.clone(), serde_json::to_value(&a_addr).unwrap()),
            (2, b_key, serde_json::to_value(&b_addr).unwrap()),
        ];
        holder.refresh_registry(registry.clone());
        asker.refresh_registry(registry);
        assert_eq!(asker.node_id_for_key(&a_key), Some(1));
        assert_eq!(asker.node_id_for_key(&"00".repeat(32)), None);
        let service = Arc::new(ReconServer { set: owner.clone() });
        tokio::spawn(async move { holder.serve(service).await });

        let mut mirror = KeySet::new();
        let mut session = Session::new(start_bits_for(owner.len()));
        while let Some(queries) = session.next_request(&mirror) {
            let reply = asker
                .request_to_node_timeout(
                    1,
                    &Payload::ReconcileRequest { queries },
                    Duration::from_secs(20),
                )
                .await
                .expect("reconcile round failed");
            let Payload::ReconcileReply { reply } = reply else {
                panic!("unexpected reply {reply:?}");
            };
            session.apply(&mut mirror, &reply).unwrap();
        }
        assert!(session.rounds > 1, "50k keys cannot fit one frame");
        assert!(mirror == owner);
    }

    /// TTFB is measured to the control reply, so it tracks how long the
    /// holder took to answer and excludes the body transfer. The source
    /// selector needs these apart to learn goodput at all.
    #[tokio::test]
    async fn chunk_fetch_reports_time_to_the_reply_not_to_the_last_byte() {
        let service = ChunkServer::new(Some(vec![2u8; 4096]), Duration::from_millis(250));
        let asker = chunk_pair(service).await;
        let fetch = asker.request_chunk(1, &a_hash()).await.unwrap().unwrap();
        assert!(
            fetch.ttfb >= Duration::from_millis(250),
            "ttfb {:?} does not include the holder's own latency",
            fetch.ttfb
        );

        let service = ChunkServer::new(Some(vec![2u8; 4096]), Duration::ZERO);
        let asker = chunk_pair(service).await;
        let fetch = asker.request_chunk(1, &a_hash()).await.unwrap().unwrap();
        assert!(
            fetch.ttfb < Duration::from_millis(250),
            "ttfb {:?} looks like a constant, not a measurement",
            fetch.ttfb
        );
    }

    /// Refreshing the registry must not list ourselves as a peer (we
    /// would otherwise dial our own endpoint on every lease wait) while
    /// still enrolling our own key so peers accept us.
    #[tokio::test]
    async fn refresh_excludes_self_but_enrolls_own_key() {
        let topic = crate::topic_for(Some(&[5u8; 32]), "fs");
        let p2p = P2p::spawn(iroh::SecretKey::generate(), topic)
            .await
            .unwrap();
        let (own_key, own_addr) = (p2p.pubkey_hex(), p2p.addr());
        let peers = Peers::new(p2p, 1);
        peers.refresh_registry(vec![(
            1,
            own_key.clone(),
            serde_json::to_value(&own_addr).unwrap(),
        )]);
        assert!(peers.snapshot().is_empty(), "must not dial ourselves");
        assert!(
            peers.request_lease("p0", None).await.is_none(),
            "nobody else to ask"
        );
    }

    /// A different node enrolled under *our* key (mounts sharing a node
    /// key) stays in the roster, but it is really us: the initiating
    /// paths must see it as self and leave it out.
    #[tokio::test]
    async fn a_peer_sharing_our_key_is_self_and_not_a_remote() {
        let topic = crate::topic_for(Some(&[7u8; 32]), "fs");
        let p2p = P2p::spawn(iroh::SecretKey::generate(), topic)
            .await
            .unwrap();
        let other = P2p::spawn(iroh::SecretKey::generate(), topic)
            .await
            .unwrap();
        let (own_key, own_addr) = (p2p.pubkey_hex(), p2p.addr());
        let (other_key, other_addr) = (other.pubkey_hex(), other.addr());
        let peers = Peers::new(p2p, 1);
        peers.refresh_registry(vec![
            (1, own_key.clone(), serde_json::to_value(&own_addr).unwrap()),
            (2, own_key, serde_json::to_value(&own_addr).unwrap()),
            (3, other_key, serde_json::to_value(&other_addr).unwrap()),
        ]);
        let listed: Vec<u64> = peers.snapshot().iter().map(|p| p.node_id).collect();
        assert_eq!(listed, vec![2, 3], "the shared-key node stays enrolled");
        assert!(peers.is_self_node(1), "our own id");
        assert!(peers.is_self_node(2), "a node sharing our endpoint key");
        assert!(!peers.is_self_node(3));
        assert!(!peers.is_self_node(99), "unknown ids are not self");
        let remote: Vec<u64> = peers.remote_snapshot().iter().map(|p| p.node_id).collect();
        assert_eq!(remote, vec![3]);
        assert!(!Peers::disabled().is_self_node(1));
    }

    /// A registry record whose address will not parse must not drop the
    /// peer from the allowlist: it can still dial us even if we cannot
    /// dial it.
    #[tokio::test]
    async fn unparseable_peer_address_is_skipped_but_still_allowed() {
        let topic = crate::topic_for(Some(&[6u8; 32]), "fs");
        let p2p = P2p::spawn(iroh::SecretKey::generate(), topic)
            .await
            .unwrap();
        let own = p2p.pubkey_hex();
        let peers = Peers::new(p2p, 1);
        let other = "cc".repeat(32);
        peers.refresh_registry(vec![
            (1, own, serde_json::json!({})),
            (2, other.clone(), serde_json::json!("not-an-addr")),
        ]);
        assert!(peers.snapshot().is_empty(), "undialable peer is not listed");
        assert_eq!(
            peers.inner.as_ref().unwrap().p2p.check(&other),
            Decision::Accept,
            "but it stays enrolled so it may dial us"
        );
    }
}
