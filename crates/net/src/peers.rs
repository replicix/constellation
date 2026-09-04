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
        Self {
            inner: Some(Arc::new(Inner {
                p2p,
                node_id,
                peers: Mutex::new(HashMap::new()),
                refresher: Mutex::new(None),
                warned_shared_key: std::sync::atomic::AtomicBool::new(false),
            })),
        }
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

    pub fn snapshot(&self) -> Vec<Peer> {
        let Some(inner) = self.inner.as_ref() else {
            return Vec::new();
        };
        let mut v: Vec<Peer> = inner.peers.lock().unwrap().values().cloned().collect();
        v.sort_by_key(|p| p.node_id);
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
    ) -> Result<Option<crate::endpoint::ChunkFetch>> {
        self.request_chunk_with_timeout(node_id, hash, CHUNK_REQUEST_TIMEOUT)
            .await
    }

    async fn request_chunk_with_timeout(
        &self,
        node_id: u64,
        hash: &[u8; 32],
        timeout: Duration,
    ) -> Result<Option<crate::endpoint::ChunkFetch>> {
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
            Ok(Ok(Some(fetch))) => {
                let rtt = fetch.rtt.unwrap_or(Duration::ZERO);
                self.note_rtt(node_id, rtt, true, fetch.path);
                Ok(Some(fetch))
            }
            Ok(Ok(None)) => {
                // Soft miss: path still observed if the connection is live.
                let path = inner.p2p.path_kind(addr.id).await;
                if path != PathKind::Unknown {
                    self.note_path(node_id, path);
                }
                Ok(None)
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
    /// their next poll. Failure is fine: the poll still happens.
    /// `payload` is the zstd segment bytes when they fit the gossip budget.
    pub async fn announce_segment(
        &self,
        part: &str,
        seq: u64,
        epoch: u64,
        payload: Option<Vec<u8>>,
    ) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        let payload = Payload::SegmentPublished {
            part: part.to_string(),
            seq,
            epoch,
            payload,
        };
        match inner.p2p.broadcast(&payload).await {
            Ok(()) => tracing::debug!(part, seq, epoch, "announced segment to peers"),
            Err(e) => {
                tracing::debug!(error = %e, part, seq, "segment announce failed; peers will poll")
            }
        }
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
    /// Returns `true` only when a holder said it flushed and released, so
    /// the caller should attempt its CAS immediately. Everything else —
    /// no peers, no answer, a decline, a forged reply — returns `false`
    /// and leaves the caller on the S3 path.
    ///
    /// When `holder_id` is known, that peer is asked first.
    pub async fn request_lease(&self, part: &str, holder_id: Option<u64>) -> bool {
        let Some(inner) = self.inner.as_ref() else {
            return false;
        };
        let mut peers = self.snapshot();
        if peers.is_empty() {
            return false;
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
                        return true;
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
        false
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
        let result =
            tokio::time::timeout(REQUEST_TIMEOUT, inner.p2p.request(addr.clone(), payload)).await;
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
            Payload::SegmentPublished {
                part,
                seq,
                epoch,
                payload: body,
            } => {
                service.segment_published(part, *seq, *epoch, body.clone());
            }
            Payload::MutateRequest {
                part,
                requester,
                req_id,
                epoch_seen,
                op,
            } => {
                let _ = service
                    .mutate_requested(part.clone(), *requester, *req_id, *epoch_seen, op.clone())
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
        Payload::SegmentPublished {
            part,
            seq,
            epoch,
            payload,
        } => {
            service.segment_published(&part, seq, epoch, payload);
            None
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
        } => Some(
            service
                .mutate_requested(part, requester, req_id, epoch_seen, op)
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
        Payload::ChunkRequest { hash } => {
            let data = service.serve_chunk(hash, hex.to_string()).await;
            let found = data.is_some();
            let reply = Payload::ChunkResponse { hash, found };
            let signed = Signed::new(inner.p2p.secret_key(), &reply)?;
            write_frame(&mut send, &signed).await?;
            if let Some(bytes) = data {
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
        | Payload::ChunkResponse { .. }
        | Payload::MutateReply { .. } => None,
    };
    if let Some(reply) = reply {
        let signed = Signed::new(inner.p2p.secret_key(), &reply)?;
        write_frame(&mut send, &signed).await?;
    }
    let _ = send.finish();
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
    }

    impl PeerService for Recorder {
        fn segment_published(&self, part: &str, seq: u64, epoch: u64, _payload: Option<Vec<u8>>) {
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
        p.announce_segment("p0", 1, 1, None).await;
        assert!(
            !p.request_lease("p0", None).await,
            "no fast path means the caller must use S3"
        );
    }

    /// The end-to-end fast path: the asker's request reaches the holder's
    /// service and a release turns into "claim now".
    #[tokio::test]
    async fn lease_request_reaches_the_holder_and_releases() {
        let (_holder, asker, service) = pair(true).await;
        assert!(
            asker.request_lease("p0", None).await,
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
        assert!(!asker.request_lease("p0", None).await);
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
                payload: None,
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
            !stranger.request_lease("p0", None).await,
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
        fn segment_published(
            &self,
            _part: &str,
            _seq: u64,
            _epoch: u64,
            _payload: Option<Vec<u8>>,
        ) {
        }
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
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<Vec<u8>>> + Send + '_>>
        {
            Box::pin(async move {
                let n = self.in_flight.fetch_add(1, AtomicOrder::SeqCst) + 1;
                self.peak.fetch_max(n, AtomicOrder::SeqCst);
                tokio::time::sleep(self.delay).await;
                self.in_flight.fetch_sub(1, AtomicOrder::SeqCst);
                self.data.clone()
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
        assert!(x.unwrap().is_some() && y.unwrap().is_some());
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
        assert!(a.unwrap().is_some() && b.unwrap().is_some());
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
                matches!(body, Payload::ChunkResponse { found: true, .. }),
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
            matches!(got, Ok(None)),
            "expected a clean miss, got {got:?}"
        );
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
        assert!(!peers.request_lease("p0", None).await, "nobody else to ask");
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
