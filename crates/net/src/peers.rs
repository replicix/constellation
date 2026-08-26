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
use crate::endpoint::{P2p, PeerService};
use crate::message::{read_frame, write_frame, Payload, Signed, ALPN};
use anyhow::Result;
use iroh::EndpointAddr;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How long to wait for a peer's answer to a lease request before
/// falling back to the S3 path. Generous enough for a WAN round trip,
/// short enough that it never dominates the idle-release window.
const REQUEST_TIMEOUT: Duration = Duration::from_millis(1_500);

/// One known peer, as learned from the node registry.
#[derive(Debug, Clone)]
pub struct Peer {
    pub node_id: u64,
    pub pubkey_hex: String,
    pub addr: EndpointAddr,
    /// Last successful round trip, for status output.
    pub rtt_ms: Option<u64>,
    pub connected: bool,
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
    /// registry. `records` is `(node_id, pubkey_hex, addr_json)`.
    pub fn refresh_registry(&self, records: Vec<(u64, String, serde_json::Value)>) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        let mut allowed = Vec::with_capacity(records.len());
        let mut peers = HashMap::new();
        for (node_id, pubkey_hex, addr_json) in records {
            allowed.push(pubkey_hex.clone());
            if node_id == inner.node_id {
                continue; // never dial ourselves
            }
            // An unparseable address just means we cannot dial that peer
            // yet; it stays on the allowlist so it may still dial us.
            if let Ok(addr) = serde_json::from_value::<EndpointAddr>(addr_json) {
                // Feed the address to iroh so gossip, which bootstraps
                // from bare endpoint ids, can dial this peer at all.
                inner.p2p.learn_addr(addr.clone());
                let prev = inner.peers.lock().unwrap().get(&node_id).cloned();
                peers.insert(
                    node_id,
                    Peer {
                        node_id,
                        pubkey_hex,
                        addr,
                        rtt_ms: prev.as_ref().and_then(|p| p.rtt_ms),
                        connected: prev.map(|p| p.connected).unwrap_or(false),
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

    /// Tell peers a segment is durable so they tail now instead of at
    /// their next poll. Failure is fine: the poll still happens.
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

    /// Ask whoever holds `part` to hand the lease over.
    ///
    /// Returns `true` only when a holder said it flushed and released, so
    /// the caller should attempt its CAS immediately. Everything else —
    /// no peers, no answer, a decline, a forged reply — returns `false`
    /// and leaves the caller on the S3 path.
    pub async fn request_lease(&self, part: &str) -> bool {
        let Some(inner) = self.inner.as_ref() else {
            return false;
        };
        let peers = self.snapshot();
        if peers.is_empty() {
            return false;
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
                    self.note_rtt(peer.node_id, started.elapsed(), true);
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
                    self.note_rtt(peer.node_id, started.elapsed(), false);
                }
                Err(_) => {
                    tracing::debug!(peer = peer.node_id, part, "lease request timed out");
                    self.note_rtt(peer.node_id, started.elapsed(), false);
                }
            }
        }
        false
    }

    fn note_rtt(&self, node_id: u64, took: Duration, ok: bool) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        if let Some(p) = inner.peers.lock().unwrap().get_mut(&node_id) {
            p.connected = ok;
            if ok {
                p.rtt_ms = Some(took.as_millis() as u64);
            }
        }
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
        let result = tokio::time::timeout(REQUEST_TIMEOUT, inner.p2p.request(addr, payload)).await;
        match result {
            Ok(Ok(reply)) => {
                if let Some(id) = node_id {
                    self.note_rtt(id, started.elapsed(), true);
                }
                Ok(reply)
            }
            Ok(Err(e)) => {
                if let Some(id) = node_id {
                    self.note_rtt(id, started.elapsed(), false);
                }
                Err(e)
            }
            Err(_) => {
                if let Some(id) = node_id {
                    self.note_rtt(id, started.elapsed(), false);
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
            if let Err(e) = handle_conn(&inner, service.as_ref(), conn).await {
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
    tracing::debug!("gossip receive loop started");
    while let Some(event) = rx.next().await {
        let Ok(event) = event else { continue };
        let iroh_gossip::api::Event::Received(msg) = event else {
            continue; // neighbor up/down: status only
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
        if let Payload::SegmentPublished { part, seq, epoch } = &payload {
            service.segment_published(part, *seq, *epoch);
        }
        if let Payload::EpochActivate {
            epoch_id,
            members,
            base,
        } = payload
        {
            service.epoch_activated(epoch_id, members, base);
        }
    }
}

/// Serve one inbound connection: enforce the allowlist, then answer
/// frames until the peer goes away.
async fn handle_conn<S: PeerService>(
    inner: &Arc<Inner>,
    service: &S,
    conn: iroh::endpoint::Connection,
) -> Result<()> {
    let remote = conn.remote_id();
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
    loop {
        let (mut send, mut recv) = match conn.accept_bi().await {
            Ok(pair) => pair,
            // Normal close.
            Err(_) => return Ok(()),
        };
        let req = read_frame(&mut recv).await?;
        let (author, payload) = req.verify()?;
        // The signer must be the peer we authorized, so an allowed peer
        // cannot relay a third party's request through its connection.
        if author.as_bytes() != remote.as_bytes() {
            tracing::warn!(peer = %hex, "dropping frame signed by a different key");
            continue;
        }
        let reply = match payload {
            Payload::SegmentPublished { part, seq, epoch } => {
                service.segment_published(&part, seq, epoch);
                None
            }
            Payload::LeaseRequest { part, requester } => {
                Some(service.lease_requested(part, requester).await)
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
            Payload::Pong { .. }
            | Payload::LeaseHandoff { .. }
            | Payload::DelegationGrant { .. }
            | Payload::EpochAck { .. } => None,
        };
        if let Some(reply) = reply {
            let signed = Signed::new(inner.p2p.secret_key(), &reply)?;
            write_frame(&mut send, &signed).await?;
        }
        let _ = send.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A recording [`PeerService`] so tests can assert what the endpoint
    /// dispatched.
    #[derive(Default)]
    struct Recorder {
        segments: Mutex<Vec<(String, u64, u64)>>,
        lease_asks: Mutex<Vec<(String, u64)>>,
        /// What to answer a lease request with.
        release: bool,
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
                }
            })
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
        p.announce_segment("p0", 1, 1).await;
        assert!(
            !p.request_lease("p0").await,
            "no fast path means the caller must use S3"
        );
    }

    /// The end-to-end fast path: the asker's request reaches the holder's
    /// service and a release turns into "claim now".
    #[tokio::test]
    async fn lease_request_reaches_the_holder_and_releases() {
        let (_holder, asker, service) = pair(true).await;
        assert!(
            asker.request_lease("p0").await,
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
        assert!(!asker.request_lease("p0").await);
        assert_eq!(service.lease_asks.lock().unwrap().len(), 1);
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
            !stranger.request_lease("p0").await,
            "an unenrolled peer must not get a handoff"
        );
        assert!(
            service.lease_asks.lock().unwrap().is_empty(),
            "the request must never reach the service"
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
        assert!(!peers.request_lease("p0").await, "nobody else to ask");
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
