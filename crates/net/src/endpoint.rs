//! The iroh endpoint that carries the P2P fast path.
//!
//! One QUIC endpoint per daemon, keyed by the host node key. Peers learn
//! dial info from the filesystem's own node registry in S3, so the bucket
//! stays the only directory and the only trust root (DESIGN.md §8).
//! Relays are optional (`CONSTELLATION_P2P_RELAY`) and never replace the
//! registry: they only help when published direct addresses are not
//! mutually reachable (NAT / no shared L3).
//!
//! Everything here is best-effort. `spawn` returning an error, a peer that
//! never answers, and a gossip topic that never forms all degrade to the
//! S3 polling path that phases 1–2 already rely on.

use crate::allowlist::{Allowlist, Decision};
use crate::message::{ChunkDecline, ChunkStatus, Payload, Signed, ALPN};
use crate::relay::RelayPolicy;
use anyhow::{Context, Result};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, SecretKey};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type ConnectionSlot = Arc<tokio::sync::Mutex<Option<iroh::endpoint::Connection>>>;
type ConnectionPool = Arc<Mutex<HashMap<iroh::EndpointId, ConnectionSlot>>>;

/// One hash-prefix bucket of a peer's cache bloom, as gossiped.
pub struct DigestSnapshot {
    pub node_id: u64,
    pub generation: u64,
    pub bits: Vec<u8>,
    pub nbits: u64,
    pub k: u32,
    pub n: u64,
    pub bucket: u32,
    pub buckets: u32,
}

/// Add-only bloom delta. `adds` are raw hashes; the receiver routes
/// each one with this sender's `buckets`.
pub struct DigestDelta {
    pub node_id: u64,
    pub generation: u64,
    pub adds: Vec<[u8; 32]>,
    pub buckets: u32,
}

/// One chunk pulled from a peer, with the timing the source selector
/// needs. `ttfb` is measured to the control reply, so `ttfb` and the
/// caller's end-to-end duration bracket the body transfer.
#[derive(Debug)]
pub struct ChunkFetch {
    pub data: Vec<u8>,
    pub ttfb: std::time::Duration,
    pub rtt: Option<std::time::Duration>,
    pub path: PathKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathKind {
    Direct,
    Relay,
    Unknown,
}

impl PathKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Relay => "relay",
            Self::Unknown => "unknown",
        }
    }
}

/// What the daemon gives the endpoint so it can serve peer requests.
/// Kept as a trait object so `cli` owns the shipper/lease logic and this
/// crate stays free of filesystem concerns.
///
/// `lease_requested` is async because answering it means flushing to S3;
/// doing that on a blocking call inside the accept task would stall a
/// runtime worker.
pub trait PeerService: Send + Sync + 'static {
    /// A peer published a segment: tail now instead of at the next poll.
    /// `payload` is the zstd segment body when it fit the gossip budget.
    fn segment_published(&self, part: &str, seq: u64, epoch: u64, payload: Option<Vec<u8>>);
    /// A peer wants `part`'s lease. Returns the reply to send.
    fn lease_requested(
        &self,
        part: String,
        requester: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>>;
    /// A peer wants a delegation to write under `path`, which this node
    /// may be offline-designated for. Returns the grant or decline.
    fn delegation_requested(
        &self,
        _path: String,
        _requester: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        let decline = crate::delegation::DelegationGranter::decline("");
        Box::pin(async move { decline })
    }
    /// A delegated peer flushed `seq` of `part` (touching `path`) and
    /// wants this node's ack. Returns whether it has been tailed yet.
    fn flush_ack_requested(
        &self,
        _path: String,
        _part: String,
        _seq: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::FlushAck {
                path: String::new(),
                part: String::new(),
                seq: 0,
                acked: false,
            }
        })
    }
    /// A peer proposed a continuation epoch. Persist the promise, then
    /// reply with an ack. Default declines (P2P-disabled / tests).
    fn epoch_proposed(
        &self,
        _epoch_id: String,
        _members: Vec<u64>,
        _base: Vec<(String, u64)>,
        _proposer: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::EpochAck {
                epoch_id: String::new(),
                member: 0,
                accepted: false,
            }
        })
    }
    /// A peer redistributed activation. Default is a no-op.
    fn epoch_activated(&self, _epoch_id: String, _members: Vec<u64>, _base: Vec<(String, u64)>) {}
    /// Cooperative-cache digest snapshot from a peer.
    fn cache_digest(&self, _digest: DigestSnapshot) {}
    fn cache_digest_delta(&self, _delta: DigestDelta) {}
    /// Plan 30 §M15: a peer's exact-membership heartbeat.
    fn cache_summary(&self, _node_id: u64, _summary: crate::reconcile::Summary) {}
    /// Plan 30 §M15: a peer's exact adds/removes for one publish tick.
    fn cache_set_delta(&self, _node_id: u64, _delta: crate::reconcile::Delta) {}
    /// Plan 30 §M15: answer one reconciliation round against this node's
    /// published chunk set. The default declines (`processed: 0`), which
    /// the initiator treats as "no progress" and abandons.
    fn reconcile_requested(
        &self,
        _queries: Vec<crate::reconcile::Query>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::ReconcileReply {
                reply: crate::reconcile::Reply {
                    summary: crate::reconcile::Summary {
                        incarnation: 0,
                        seq: 0,
                        root: [0; 16],
                        count: 0,
                    },
                    processed: 0,
                    answers: Vec::new(),
                },
            }
        })
    }
    /// Serve a clean/pinned chunk, or decline with the reason (busy,
    /// absent, or recently removed — see [`ChunkDecline`]). `from_hex`
    /// is the requester's node-key hex, used for the per-peer
    /// concurrency cap.
    ///
    /// Async for the same reason as `lease_requested`: answering means
    /// reading and verifying up to a whole chunk from disk, which must
    /// not happen inline on a runtime worker.
    fn serve_chunk(
        &self,
        _hash: [u8; 32],
        _from_hex: String,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<u8>, ChunkDecline>> + Send + '_>,
    > {
        Box::pin(async move { Err(ChunkDecline::Busy) })
    }
    /// Non-holder asked us to journal `op`. Default declines with an
    /// empty outcome; callers treat that as `MutateOutcome::Busy`.
    #[allow(clippy::too_many_arguments)]
    fn mutate_requested(
        &self,
        _part: String,
        _requester: u64,
        req_id: u64,
        _epoch_seen: u64,
        _op: Vec<u8>,
        _rid: (u64, u32, u64),
        _acked_through: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
        Box::pin(async move {
            Payload::MutateReply {
                req_id,
                outcome: Vec::new(),
                base: None,
                position_seq: 0,
                position_pending: None,
            }
        })
    }
    /// Holder offered us this lease (placement). Default ignores it.
    fn lease_offered(&self, _part: String, _epoch: u64) {}
    /// A peer gossiped its RTT vector. Default ignores it.
    fn peer_rtts(&self, _node_id: u64, _rtts: Vec<(u64, u16)>) {}
    /// This node's id, for `Ping`/`Pong`.
    fn node_id(&self) -> u64;
}

/// A live P2P endpoint.
pub struct P2p {
    endpoint: Endpoint,
    gossip: Gossip,
    topic: TopicId,
    key: SecretKey,
    allow: Arc<Mutex<Allowlist>>,
    /// Registry-sourced peer addresses, fed to iroh so gossip (which
    /// bootstraps from bare endpoint ids) can actually dial them. With
    /// address publishing disabled this is the only address source.
    lookup: MemoryLookup,
    /// Broadcast handle for the joined topic, once it exists.
    sender: Arc<tokio::sync::Mutex<Option<iroh_gossip::api::GossipSender>>>,
    /// One gate per remote prevents dial storms while allowing unrelated
    /// peers to connect concurrently. QUIC streams multiplex over the
    /// retained connection.
    connections: ConnectionPool,
    /// Active relay policy label (`disabled` / `default` / URL…).
    relay: String,
}

/// Derive the gossip topic. Prefers the `gossip_secret` from
/// `meta.json` so topic membership is not guessable from a bucket name;
/// falls back to hashing the filesystem UUID for filesystems created
/// before the secret existed (weaker: anyone who learns the UUID can
/// join the topic, but messages are still signed and the allowlist still
/// gates direct connections, so the worst case is unsolicited traffic).
pub fn topic_for(gossip_secret: Option<&[u8; 32]>, fs_uuid: &str) -> TopicId {
    match gossip_secret {
        Some(s) => TopicId::from_bytes(*s),
        None => TopicId::from_bytes(*blake3::hash(fs_uuid.as_bytes()).as_bytes()),
    }
}

impl P2p {
    /// Bind the endpoint and spawn gossip. Errors are the caller's cue to
    /// run without a fast path.
    ///
    /// Relay behaviour comes from [`RelayPolicy::from_env`] unless
    /// `relay` is passed explicitly (tests).
    pub async fn spawn(key: SecretKey, topic: TopicId) -> Result<Self> {
        Self::spawn_with(key, topic, RelayPolicy::from_env()?).await
    }

    pub async fn spawn_with(key: SecretKey, topic: TopicId, relay: RelayPolicy) -> Result<Self> {
        let lookup = MemoryLookup::new();
        let relay_mode = relay.to_iroh()?;
        let relay_label = relay.label();
        let endpoint = Endpoint::builder(presets::Minimal)
            // Registry remains the peer directory (DESIGN.md §8). Relays
            // are optional connectivity help when direct addrs cannot
            // reach (NAT / no shared L3). See docs/reference/features/p2p-relays.md.
            .relay_mode(relay_mode)
            .secret_key(key.clone())
            .address_lookup(lookup.clone())
            .alpns(vec![ALPN.to_vec(), iroh_gossip::ALPN.to_vec()])
            .bind()
            .await
            .context("binding the iroh endpoint")?;
        let gossip = Gossip::builder()
            .max_message_size(crate::message::GOSSIP_MAX_MESSAGE_SIZE)
            .spawn(endpoint.clone());
        Ok(Self {
            endpoint,
            gossip,
            topic,
            key,
            allow: Arc::new(Mutex::new(Allowlist::new())),
            lookup,
            sender: Arc::new(tokio::sync::Mutex::new(None)),
            connections: Arc::new(Mutex::new(HashMap::new())),
            relay: relay_label,
        })
    }

    /// Teach iroh how to reach a peer learned from the registry.
    pub fn learn_addr(&self, addr: EndpointAddr) {
        self.lookup.add_endpoint_info(addr);
    }

    pub fn addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    /// Relay policy label active on this endpoint.
    pub fn relay_label(&self) -> &str {
        &self.relay
    }

    pub fn pubkey_hex(&self) -> String {
        crate::identity::pubkey_hex(&self.key.public())
    }

    /// Update the accept-time allowlist from the registry.
    pub fn set_allowed(&self, keys: impl IntoIterator<Item = String>) {
        self.allow.lock().unwrap().replace(keys);
    }

    pub fn allowed_len(&self) -> usize {
        self.allow.lock().unwrap().len()
    }

    /// Is `pubkey_hex` currently permitted? `Refresh` is reported as not
    /// allowed; the caller refreshes and asks again.
    pub fn check(&self, pubkey_hex: &str) -> Decision {
        self.allow.lock().unwrap().check(pubkey_hex)
    }

    /// Join the gossip topic, bootstrapping from `peers`, and return the
    /// receiver so the caller can drive incoming events. The sender is
    /// retained for [`P2p::broadcast`].
    pub async fn join(
        &self,
        peers: Vec<iroh::EndpointId>,
    ) -> Result<iroh_gossip::api::GossipReceiver> {
        let topic = self.gossip.subscribe(self.topic, peers).await?;
        let (tx, rx) = topic.split();
        *self.sender.lock().await = Some(tx);
        Ok(rx)
    }

    /// Broadcast a signed payload to the topic. Best effort: a failure
    /// only means peers learn from their next poll instead.
    ///
    /// Gossip delivers whole datagrams, so this sends bare postcard —
    /// the 4-byte length prefix from [`Signed::encode`] exists only to
    /// frame messages on a byte stream, and including it here would make
    /// every receiver's decode fail silently.
    pub async fn broadcast(&self, payload: &Payload) -> Result<()> {
        let msg = Signed::new(&self.key, payload)?;
        let body = msg.encode_bare()?;
        anyhow::ensure!(
            body.len() <= crate::message::GOSSIP_CONTENT_LIMIT,
            "gossip content is {} bytes, limit is {}",
            body.len(),
            crate::message::GOSSIP_CONTENT_LIMIT
        );
        let guard = self.sender.lock().await;
        let Some(tx) = guard.as_ref() else {
            anyhow::bail!("gossip topic not joined yet");
        };
        tx.broadcast(body.into()).await?;
        Ok(())
    }

    /// Send `payload` to one peer and wait for a single reply.
    ///
    /// `peer` is the full [`EndpointAddr`] from the registry, not just a
    /// key: with address publishing disabled the registry record is the
    /// only way to learn how to dial.
    pub async fn request(&self, peer: EndpointAddr, payload: &Payload) -> Result<Payload> {
        let expect = peer.id;
        let conn = self.connection(&peer).await?;
        let result = async {
            let (mut send, mut recv) = conn.open_bi().await.context("opening a stream")?;
            let msg = Signed::new(&self.key, payload)?;
            crate::message::write_frame(&mut send, &msg).await?;
            send.finish().ok();
            let reply = crate::message::read_frame(&mut recv).await?;
            let (author, body) = reply.verify()?;
            anyhow::ensure!(
                author.as_bytes() == expect.as_bytes(),
                "reply signed by an unexpected key"
            );
            Ok(body)
        }
        .await;
        if result.is_err() {
            self.invalidate_connection(expect, conn.stable_id()).await;
        }
        result
    }

    /// Fetch one chunk from `peer`. The control frames use the same
    /// signed postcard as everything else; the payload (up to the FS chunk
    /// size) follows as `u64be length + bytes` so we never shove 4 MiB
    /// through [`crate::message::MAX_FRAME`]. `Ok(Err(reason))` is a
    /// clean decline, distinct from a transport failure.
    pub async fn request_chunk(
        &self,
        peer: EndpointAddr,
        hash: &[u8; 32],
    ) -> Result<std::result::Result<ChunkFetch, ChunkDecline>> {
        let expect = peer.id;
        let first = self.connection(&peer).await?;
        match self.request_chunk_on(&first, expect, hash).await {
            Ok(value) => Ok(value),
            Err(first_error) => {
                self.invalidate_connection(expect, first.stable_id()).await;
                let retry = self.connection(&peer).await?;
                self.request_chunk_on(&retry, expect, hash)
                    .await
                    .with_context(|| format!("chunk stream failed after redial: {first_error:#}"))
            }
        }
    }

    /// Plan 30 §M4: every open path of the pooled connection to `id`,
    /// without waiting (`None` if none is pooled, or one is being dialed
    /// right now). For `status`, which must not block.
    pub fn path_summary_now(&self, id: iroh::EndpointId) -> Option<crate::paths::PathSummary> {
        let gate = self.connections.lock().unwrap().get(&id).cloned()?;
        let slot = gate.try_lock().ok()?;
        slot.as_ref().map(crate::paths::PathSummary::of)
    }

    /// Whether a pooled, still-open QUIC connection to `id` exists right
    /// now. A request that timed out at the application level leaves
    /// the connection in the pool (only a transport error invalidates
    /// it), so this distinguishes "slow to answer" from "cannot be
    /// reached": a dial failure never pools anything, a transport error
    /// evicts it, and a peer that went away closes it (or the idle
    /// timeout does). Plan 30 §M13 uses it to start a P2P outage only
    /// from the latter.
    pub async fn connection_alive(&self, id: iroh::EndpointId) -> bool {
        let gate = self.connections.lock().unwrap().get(&id).cloned();
        let Some(gate) = gate else {
            return false;
        };
        let slot = gate.lock().await;
        slot.as_ref()
            .is_some_and(|conn| conn.close_reason().is_none())
    }

    /// Selected-path kind for a pooled connection, if any.
    pub async fn path_kind(&self, id: iroh::EndpointId) -> PathKind {
        let gate = self.connections.lock().unwrap().get(&id).cloned();
        let Some(gate) = gate else {
            return PathKind::Unknown;
        };
        let slot = gate.lock().await;
        match slot.as_ref() {
            Some(conn) => transport_observation(conn).1,
            None => PathKind::Unknown,
        }
    }

    async fn request_chunk_on(
        &self,
        conn: &iroh::endpoint::Connection,
        expect: iroh::EndpointId,
        hash: &[u8; 32],
    ) -> Result<std::result::Result<ChunkFetch, ChunkDecline>> {
        use tokio::io::AsyncReadExt;
        let started = std::time::Instant::now();
        let (mut send, mut recv) = conn.open_bi().await.context("opening a chunk stream")?;
        let msg = Signed::new(&self.key, &Payload::ChunkRequest { hash: *hash })?;
        crate::message::write_frame(&mut send, &msg).await?;
        send.finish().ok();
        let reply = crate::message::read_frame(&mut recv).await?;
        // The control reply precedes the body, so this is a true
        // first-byte mark: everything after it is transfer time.
        let ttfb = started.elapsed();
        let (author, body) = reply.verify()?;
        anyhow::ensure!(
            author.as_bytes() == expect.as_bytes(),
            "chunk reply signed by an unexpected key"
        );
        match body {
            Payload::ChunkResponse {
                status: ChunkStatus::Declined(why),
                ..
            } => Ok(Err(why)),
            Payload::ChunkResponse {
                status: ChunkStatus::Found,
                ..
            } => {
                let len = recv.read_u64().await.context("chunk length")?;
                anyhow::ensure!(
                    len > 0 && len <= 64 * 1024 * 1024,
                    "implausible chunk length {len}"
                );
                let mut data = vec![0u8; len as usize];
                recv.read_exact(&mut data).await?;
                let (rtt, path) = transport_observation(conn);
                Ok(Ok(ChunkFetch {
                    data,
                    ttfb,
                    rtt,
                    path,
                }))
            }
            other => anyhow::bail!("unexpected chunk reply {other:?}"),
        }
    }

    async fn connection(&self, peer: &EndpointAddr) -> Result<iroh::endpoint::Connection> {
        let gate = self
            .connections
            .lock()
            .unwrap()
            .entry(peer.id)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None)))
            .clone();
        let mut slot = gate.lock().await;
        if let Some(conn) = slot.as_ref() {
            if conn.weak_handle().upgrade().is_some() {
                return Ok(conn.clone());
            }
            *slot = None;
        }
        let conn = self
            .endpoint
            .connect(peer.clone(), ALPN)
            .await
            .context("dialing peer")?;
        *slot = Some(conn.clone());
        Ok(conn)
    }

    async fn invalidate_connection(&self, peer: iroh::EndpointId, stable_id: usize) {
        let gate = self.connections.lock().unwrap().get(&peer).cloned();
        if let Some(gate) = gate {
            let mut slot = gate.lock().await;
            if slot
                .as_ref()
                .is_some_and(|conn| conn.stable_id() == stable_id)
            {
                *slot = None;
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn pooled_connection_id(&self, peer: iroh::EndpointId) -> Option<usize> {
        let gate = self.connections.lock().unwrap().get(&peer).cloned()?;
        let id = gate.lock().await.as_ref().map(|conn| conn.stable_id());
        id
    }

    #[cfg(test)]
    pub(crate) async fn close_pooled_connection(&self, peer: iroh::EndpointId) {
        let gate = self.connections.lock().unwrap().get(&peer).cloned();
        if let Some(gate) = gate {
            if let Some(conn) = gate.lock().await.as_ref() {
                conn.close(iroh::endpoint::VarInt::from_u32(0), b"test close");
            }
        }
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// The gossip actor, so the router can dispatch its ALPN.
    pub fn gossip(&self) -> &Gossip {
        &self.gossip
    }

    pub fn secret_key(&self) -> &SecretKey {
        &self.key
    }
}

fn transport_observation(
    conn: &iroh::endpoint::Connection,
) -> (Option<std::time::Duration>, PathKind) {
    let paths = conn.paths();
    let selected = paths.iter().find(|path| path.is_selected());
    match selected {
        Some(path) if path.is_relay() => (Some(path.rtt()), PathKind::Relay),
        Some(path) if path.is_ip() => (Some(path.rtt()), PathKind::Direct),
        Some(path) => (Some(path.rtt()), PathKind::Unknown),
        None => (None, PathKind::Unknown),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_prefers_the_secret_over_the_uuid() {
        let secret = [7u8; 32];
        let from_secret = topic_for(Some(&secret), "uuid-a");
        assert_eq!(
            from_secret,
            topic_for(Some(&secret), "uuid-b"),
            "the secret alone determines the topic"
        );
        let a = topic_for(None, "uuid-a");
        let b = topic_for(None, "uuid-b");
        assert_ne!(a, b, "legacy filesystems get distinct topics per uuid");
        assert_ne!(from_secret, a);
        // Deterministic, so every node of one filesystem agrees.
        assert_eq!(a, topic_for(None, "uuid-a"));
    }

    /// End-to-end over real QUIC on loopback: a direct request reaches the
    /// peer, the peer's handler runs, and the signed reply verifies as
    /// coming from that peer. This is the piece the unit tests for
    /// `message`/`handoff` deliberately stub out.
    #[tokio::test]
    async fn direct_request_round_trip_over_quic() {
        let topic = topic_for(Some(&[1u8; 32]), "fs");
        let server = P2p::spawn(SecretKey::generate(), topic).await.unwrap();
        let client = P2p::spawn(SecretKey::generate(), topic).await.unwrap();
        // Each side allows the other, as the registry would.
        server.set_allowed([client.pubkey_hex()]);
        client.set_allowed([server.pubkey_hex()]);

        let server_key = server.secret_key().clone();
        let ep = server.endpoint().clone();
        let accept = tokio::spawn(async move {
            let incoming = ep.accept().await.expect("no inbound connection");
            let conn = incoming.await.unwrap();
            let (mut send, mut recv) = conn.accept_bi().await.unwrap();
            let req = crate::message::read_frame(&mut recv).await.unwrap();
            let (_, payload) = req.verify().unwrap();
            let reply = match payload {
                Payload::LeaseRequest { part, .. } => Payload::LeaseHandoff {
                    part,
                    epoch: 11,
                    released: true,
                    etag: None,
                    head_seq: None,
                },
                other => panic!("unexpected {other:?}"),
            };
            let signed = Signed::new(&server_key, &reply).unwrap();
            crate::message::write_frame(&mut send, &signed)
                .await
                .unwrap();
            send.finish().ok();
            // Keep the connection alive until the client has read.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });

        let server_addr = server.addr();
        let reply = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            client.request(
                server_addr,
                &Payload::LeaseRequest {
                    part: "p0".into(),
                    requester: 2,
                },
            ),
        )
        .await
        .expect("request timed out")
        .expect("request failed");
        assert_eq!(
            crate::interpret_reply("p0", &reply),
            crate::RequestOutcome::ClaimNow
        );
        accept.abort();
    }
}
