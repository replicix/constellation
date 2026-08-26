//! The iroh endpoint that carries the P2P fast path.
//!
//! One QUIC endpoint per daemon, keyed by the host node key. Address
//! discovery is **not** published to any global service: peers learn how
//! to dial each other from the filesystem's own node registry in S3, so
//! the bucket stays the only directory and the only trust root
//! (DESIGN.md §8).
//!
//! Everything here is best-effort. `spawn` returning `None`, a peer that
//! never answers, and a gossip topic that never forms all degrade to the
//! S3 polling path that phases 1–2 already rely on.

use crate::allowlist::{Allowlist, Decision};
use crate::message::{Payload, Signed, ALPN};
use anyhow::{Context, Result};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use std::sync::{Arc, Mutex};

/// What the daemon gives the endpoint so it can serve peer requests.
/// Kept as a trait object so `cli` owns the shipper/lease logic and this
/// crate stays free of filesystem concerns.
pub trait PeerService: Send + Sync + 'static {
    /// A peer published a segment: tail now instead of at the next poll.
    fn segment_published(&self, part: &str, seq: u64, epoch: u64);
    /// A peer wants `part`'s lease. Returns the reply to send.
    fn lease_requested(&self, part: &str, requester: u64) -> Payload;
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
    /// Broadcast handle for the joined topic, once it exists.
    sender: Arc<tokio::sync::Mutex<Option<iroh_gossip::api::GossipSender>>>,
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
    pub async fn spawn(key: SecretKey, topic: TopicId) -> Result<Self> {
        let endpoint = Endpoint::builder(presets::Minimal)
            // No relay and no address publishing: the registry in S3 is
            // the only directory (DESIGN.md §8).
            .relay_mode(RelayMode::Disabled)
            .secret_key(key.clone())
            .alpns(vec![ALPN.to_vec(), iroh_gossip::ALPN.to_vec()])
            .bind()
            .await
            .context("binding the iroh endpoint")?;
        let gossip = Gossip::builder().spawn(endpoint.clone());
        Ok(Self {
            endpoint,
            gossip,
            topic,
            key,
            allow: Arc::new(Mutex::new(Allowlist::new())),
            sender: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }

    pub fn addr(&self) -> EndpointAddr {
        self.endpoint.addr()
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
    pub async fn broadcast(&self, payload: &Payload) -> Result<()> {
        let msg = Signed::new(&self.key, payload)?;
        let guard = self.sender.lock().await;
        let Some(tx) = guard.as_ref() else {
            anyhow::bail!("gossip topic not joined yet");
        };
        tx.broadcast(msg.encode()?.into()).await?;
        Ok(())
    }

    /// Send `payload` to one peer and wait for a single reply.
    ///
    /// `peer` is the full [`EndpointAddr`] from the registry, not just a
    /// key: with address publishing disabled the registry record is the
    /// only way to learn how to dial.
    pub async fn request(&self, peer: EndpointAddr, payload: &Payload) -> Result<Payload> {
        let expect = peer.id;
        let conn = self
            .endpoint
            .connect(peer, ALPN)
            .await
            .context("dialing peer")?;
        let (mut send, mut recv) = conn.open_bi().await.context("opening a stream")?;
        let msg = Signed::new(&self.key, payload)?;
        crate::message::write_frame(&mut send, &msg).await?;
        send.finish().ok();
        let reply = crate::message::read_frame(&mut recv).await?;
        let (author, body) = reply.verify()?;
        // The reply must be signed by the peer we dialled, not merely by
        // somebody on the allowlist.
        anyhow::ensure!(
            author.as_bytes() == expect.as_bytes(),
            "reply signed by an unexpected key"
        );
        Ok(body)
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub fn secret_key(&self) -> &SecretKey {
        &self.key
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
