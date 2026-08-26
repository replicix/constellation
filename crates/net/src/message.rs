//! Wire messages for the P2P fast path (DESIGN.md §4, §12).
//!
//! Encoding is length-prefixed JSON, matching the rest of the codebase
//! (log segments and the control API are both JSON) — the volume here is
//! tiny (a few hundred bytes per segment PUT), so a compact binary
//! encoding would buy nothing and cost readability in `tcpdump`.
//!
//! Every message is **signed by the sender's node key and verified
//! before it is acted on**. Signing matters even though QUIC already
//! authenticates the peer: gossip messages are forwarded by third
//! parties, so the transport peer is not necessarily the author.
//!
//! Nothing here is load-bearing for correctness. Every message is an
//! accelerator for something the S3 path already does on a timer, so a
//! dropped, delayed, or rejected message can only cost latency.

use anyhow::{Context, Result};
use iroh::{PublicKey, SecretKey};
use serde::{Deserialize, Serialize};

/// ALPN for direct (non-gossip) requests. Versioned so a future
/// message-set change can be negotiated rather than mis-parsed.
pub const ALPN: &[u8] = b"constellation/1";

/// Largest accepted frame. Messages are small; the cap just stops a
/// malicious peer from making us allocate.
pub const MAX_FRAME: usize = 64 * 1024;

/// What a peer can say. Extensible on purpose: phase 5 adds cooperative
/// chunk serving over the same ALPN.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "msg", rename_all = "snake_case")]
pub enum Payload {
    /// Gossiped after a segment PUT succeeds: recipients tail
    /// immediately instead of waiting for their next poll.
    SegmentPublished {
        part: String,
        seq: u64,
        epoch: u64,
    },
    /// "I want the lease for `part`." Sent directly to the holder.
    LeaseRequest {
        part: String,
        requester: u64,
    },
    /// Holder's answer: it flushed and released, so the requester can
    /// CAS-claim now. `released: false` means it declined (still busy).
    LeaseHandoff {
        part: String,
        epoch: u64,
        released: bool,
    },
    /// Liveness for status UX only — never a safety input.
    Ping {
        node_id: u64,
    },
    Pong {
        node_id: u64,
    },
    /// "I want to write under `path`, which you are designated for."
    /// Sent directly to the designee.
    DelegationRequest {
        path: String,
        requester: u64,
    },
    /// Designee's answer: a short-TTL delegation to write under `path`,
    /// or a refusal (`granted: false`) if the designee does not hold
    /// that designation. Renewed like a mini-lease.
    DelegationGrant {
        path: String,
        epoch: u64,
        ttl_ms: u64,
        granted: bool,
    },
    /// "I flushed segment `seq` of `part`, which touches your
    /// delegation for `path` — please ack so I can consider it
    /// published." Sent to the designee after a foreign flush.
    FlushAck {
        path: String,
        part: String,
        seq: u64,
        /// `false` means the designee has not (yet) tailed this segment;
        /// the requester keeps waiting up to its bounded deadline.
        acked: bool,
    },
    /// Propose a continuation epoch (DESIGN.md §5.3). Recipients persist
    /// the promise locally BEFORE replying; activation is a later
    /// [`Payload::EpochActivate`] once every member has acked.
    EpochPropose {
        epoch_id: String,
        members: Vec<u64>,
        /// Applied-seq vector at proposal time (`part` → seq).
        base: Vec<(String, u64)>,
        proposer: u64,
    },
    /// Signed ack of an [`Payload::EpochPropose`]. The ack is only sent
    /// after the promise is durable on the member's local disk.
    EpochAck {
        epoch_id: String,
        member: u64,
        accepted: bool,
    },
    /// All members have acked: the epoch is now the authority root.
    /// Redistributed by the proposer; also gossiped so a late joiner of
    /// the message stream still activates.
    EpochActivate {
        epoch_id: String,
        members: Vec<u64>,
        base: Vec<(String, u64)>,
    },
}

/// A payload plus its author and signature.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signed {
    /// Author's public key, hex (also the iroh endpoint id).
    pub from: String,
    /// Ed25519 signature over the canonical payload bytes, hex.
    sig: String,
    /// Canonical JSON of the [`Payload`]. Kept as a string so verify
    /// checks exactly the bytes that were signed, not a re-serialization
    /// (which could differ in key order and fail spuriously).
    body: String,
}

impl Signed {
    pub fn new(key: &SecretKey, payload: &Payload) -> Result<Self> {
        let body = serde_json::to_string(payload)?;
        let sig = key.sign(body.as_bytes());
        Ok(Self {
            from: crate::identity::pubkey_hex(&key.public()),
            sig: crate::identity::hex32(&sig.to_bytes()[..32].try_into().unwrap())
                + &crate::identity::hex32(&sig.to_bytes()[32..].try_into().unwrap()),
            body,
        })
    }

    /// Verify the signature and return the payload. The author is
    /// [`Signed::from`]; the caller still has to decide whether that
    /// key is allowed (see [`crate::allowlist`]).
    pub fn verify(&self) -> Result<(PublicKey, Payload)> {
        let key = crate::identity::parse_pubkey(&self.from)?;
        anyhow::ensure!(self.sig.len() == 128, "signature must be 64 bytes of hex");
        let mut raw = [0u8; 64];
        raw[..32].copy_from_slice(&crate::identity::decode_hex32(&self.sig[..64])?);
        raw[32..].copy_from_slice(&crate::identity::decode_hex32(&self.sig[64..])?);
        key.verify(self.body.as_bytes(), &iroh::Signature::from_bytes(&raw))
            .context("bad signature on peer message")?;
        Ok((key, serde_json::from_str(&self.body)?))
    }

    /// Length-prefixed frame: 4-byte big-endian length, then JSON.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let json = serde_json::to_vec(self)?;
        anyhow::ensure!(json.len() <= MAX_FRAME, "message too large: {}", json.len());
        let mut out = Vec::with_capacity(4 + json.len());
        out.extend_from_slice(&(json.len() as u32).to_be_bytes());
        out.extend_from_slice(&json);
        Ok(out)
    }

    pub fn decode(frame: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(frame)?)
    }
}

/// Read one length-prefixed frame.
pub async fn read_frame<R>(r: &mut R) -> Result<Signed>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len) as usize;
    anyhow::ensure!(len <= MAX_FRAME, "peer announced a {len}-byte frame");
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Signed::decode(&buf)
}

/// Write one length-prefixed frame.
pub async fn write_frame<W>(w: &mut W, msg: &Signed) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;
    w.write_all(&msg.encode()?).await?;
    w.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> SecretKey {
        SecretKey::generate()
    }

    #[test]
    fn sign_verify_roundtrip() {
        let k = key();
        let payload = Payload::SegmentPublished {
            part: "p0".into(),
            seq: 7,
            epoch: 3,
        };
        let signed = Signed::new(&k, &payload).unwrap();
        let (author, got) = signed.verify().unwrap();
        assert_eq!(author, k.public());
        assert_eq!(got, payload);
    }

    /// A tampered body must not verify: gossip is forwarded by third
    /// parties, so the transport peer is not necessarily the author.
    #[test]
    fn tampered_body_is_rejected() {
        let k = key();
        let mut signed = Signed::new(
            &k,
            &Payload::LeaseRequest {
                part: "p0".into(),
                requester: 1,
            },
        )
        .unwrap();
        signed.body = signed.body.replace("\"p0\"", "\"p1\"");
        assert!(signed.verify().is_err(), "modified body must not verify");
    }

    /// Re-signing someone else's payload under our own key changes the
    /// author, so a relay cannot impersonate the original sender.
    #[test]
    fn signature_from_another_key_is_rejected() {
        let (a, b) = (key(), key());
        let payload = Payload::Ping { node_id: 1 };
        let real = Signed::new(&a, &payload).unwrap();
        let forged = Signed {
            from: crate::identity::pubkey_hex(&b.public()),
            ..real
        };
        assert!(forged.verify().is_err(), "author/signature mismatch");
    }

    #[test]
    fn frame_roundtrip_over_a_duplex() {
        let k = key();
        let msg = Signed::new(&k, &Payload::Pong { node_id: 42 }).unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (mut a, mut b) = tokio::io::duplex(4096);
            write_frame(&mut a, &msg).await.unwrap();
            let got = read_frame(&mut b).await.unwrap();
            assert_eq!(got.verify().unwrap().1, Payload::Pong { node_id: 42 });
        });
    }

    /// Gossip carries whole datagrams while streams need framing, so the
    /// two encodings differ. A prefixed frame must NOT decode as a bare
    /// gossip payload — the mismatch was silent and disabled push
    /// invalidation entirely (every peer fell back to polling).
    #[test]
    fn stream_framing_and_bare_encoding_are_distinct() {
        let k = key();
        let payload = Payload::SegmentPublished {
            part: "p0".into(),
            seq: 3,
            epoch: 1,
        };
        let signed = Signed::new(&k, &payload).unwrap();
        let bare = serde_json::to_vec(&signed).unwrap();
        let framed = signed.encode().unwrap();
        assert_eq!(framed.len(), bare.len() + 4, "frame adds a length prefix");
        // The bare form is what gossip receivers decode.
        assert_eq!(Signed::decode(&bare).unwrap().verify().unwrap().1, payload);
        // The framed form must not be mistaken for it.
        assert!(
            Signed::decode(&framed).is_err(),
            "a length-prefixed frame must not decode as a bare payload"
        );
    }

    #[test]
    fn oversized_frame_is_refused() {
        let k = key();
        let msg = Signed::new(
            &k,
            &Payload::SegmentPublished {
                part: "p".repeat(MAX_FRAME),
                seq: 1,
                epoch: 1,
            },
        )
        .unwrap();
        assert!(msg.encode().is_err(), "must refuse to send a huge frame");
    }
}
