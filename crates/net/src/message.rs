//! Wire messages for the P2P fast path (DESIGN.md §4, §12).
//!
//! Encoding is postcard. Gossip carries the bare signed envelope while
//! direct streams add a four-byte length prefix. Cache digests make JSON
//! materially expensive (base64 bloom bits and hex hashes), so every P2P
//! message uses one compact format rather than maintaining two codecs.
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
/// iroh-gossip's transport frame. Its default is only 4 KiB; every node
/// must configure this same value so a full bloom bucket is deliverable.
pub const GOSSIP_MAX_MESSAGE_SIZE: usize = 32 * 1024;
/// Conservative content budget beneath iroh-gossip's postcard protocol
/// envelope. `Signed::encode_bare` enforces this before enqueue, because
/// the gossip sender otherwise reports an oversized frame asynchronously.
pub const GOSSIP_CONTENT_LIMIT: usize = GOSSIP_MAX_MESSAGE_SIZE - 1024;
/// Largest log segment a stream frame may carry (plan 30 §M7). A shipped
/// segment is capped at 4 MiB of records; this leaves room for the
/// envelope and stops a peer from making us allocate.
pub const MAX_LOG_SEGMENT: u64 = 64 * 1024 * 1024;
/// Conservative delta batch under [`GOSSIP_CONTENT_LIMIT`]. Raw hashes
/// cost 32 bytes each; the remaining headroom covers payload/envelope
/// tags, counters, author, and signature.
pub const MAX_GOSSIP_DELTA_ADDS: usize = 900;

/// What a peer can say. Extensible on purpose: phase 5 adds cooperative
/// chunk serving over the same ALPN.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Payload {
    /// Gossiped after a segment PUT succeeds: recipients that do not
    /// follow the holder's log stream tail immediately instead of waiting
    /// for their next poll. Plan 30 §M7: a hint only — the log itself
    /// travels on direct log streams ([`Payload::LogSubscribe`]) or through
    /// S3; gossip carries membership and digests.
    SegmentPublished {
        part: String,
        seq: u64,
        epoch: u64,
    },
    /// Plan 30 §M7: subscribe to the holder's log stream from `from`.
    /// Sent directly to the holder, on a bidirectional stream that stays
    /// open: the holder answers with [`Payload::LogFrame`]s (each followed
    /// by its segment bytes) until one side lets go, or with one
    /// [`Payload::LogEnd`].
    LogSubscribe {
        part: String,
        requester: u64,
        req_id: u64,
        from: u64,
    },
    /// Plan 30 §M7: frame `n` of subscription `req_id`. `segment` names
    /// the log sequence whose S3 object bytes (`len` of them, `blake3`
    /// hash `hash`) follow the frame on the stream; `None` is a
    /// heartbeat. `head` is the holder's highest applied-or-shipped
    /// sequence, `epoch` the epoch it holds.
    LogFrame {
        req_id: u64,
        n: u64,
        epoch: u64,
        head: u64,
        segment: Option<(u64, u64, [u8; 32])>,
    },
    /// Plan 30 §M7: the holder ends subscription `req_id` (it stopped
    /// holding, or `refused`: it never held).
    LogEnd {
        req_id: u64,
        refused: bool,
    },
    /// GC has CAS-published `gc/condemned.json`. Writers still re-read S3;
    /// this is only a freshness nudge, never the authority.
    CondemnedPublished {
        epoch: u64,
    },
    /// "I want the lease for `part`." Sent directly to the holder.
    LeaseRequest {
        part: String,
        requester: u64,
    },
    /// Holder's answer: it flushed and released, so the requester can
    /// CAS-claim now. `released: false` means it declined (still busy).
    /// `etag` / `head_seq` are optional accelerators so the requester
    /// can skip a classify GET.
    LeaseHandoff {
        part: String,
        epoch: u64,
        released: bool,
        etag: Option<String>,
        head_seq: Option<u64>,
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
    /// Gossiped bloom of one hash-prefix bucket of this node's
    /// clean/pinned chunk cache (DESIGN.md §7). Recipients consult it
    /// locally on a miss — zero per-request messages. Caches larger
    /// than one frame are split: `bucket` / `buckets` identify the
    /// slice. A node with a small cache sends `buckets = 1`.
    CacheDigest {
        node_id: u64,
        generation: u64,
        /// Packed bloom bit vector (see [`crate::bloom`]).
        bits: Vec<u8>,
        nbits: u64,
        k: u32,
        n: u64,
        /// Index of this slice.
        bucket: u32,
        /// How many slices this sender currently uses.
        buckets: u32,
    },
    /// Add-only delta between snapshots. Removals are not sent: they
    /// raise FPR until the next [`Payload::CacheDigest`] for that
    /// bucket. `adds` may span buckets; the receiver routes each hash
    /// with the sender's `buckets` count.
    CacheDigestDelta {
        node_id: u64,
        generation: u64,
        /// Raw blake3 hashes newly inserted since the last snapshot.
        adds: Vec<[u8; 32]>,
        buckets: u32,
    },
    /// Direct request for one cached chunk. The payload is tiny; the
    /// bytes follow on the same stream (length-prefixed) if `found`.
    ChunkRequest {
        hash: [u8; 32],
    },
    ChunkResponse {
        hash: [u8; 32],
        status: ChunkStatus,
    },
    /// Plan 30 §M15: exact cache-membership heartbeat, gossiped every
    /// digest interval. A receiver whose mirror of `node_id` does not
    /// match starts a reconciliation session with it.
    CacheSummary {
        node_id: u64,
        summary: crate::reconcile::Summary,
    },
    /// Plan 30 §M15: one publish tick's exact adds and removes, chained
    /// by `seq` and by before/after root fingerprints.
    CacheSetDelta {
        node_id: u64,
        delta: crate::reconcile::Delta,
    },
    /// Plan 30 §M15: one reconciliation round, sent directly to the
    /// owner of the set being mirrored.
    ReconcileRequest {
        queries: Vec<crate::reconcile::Query>,
    },
    ReconcileReply {
        reply: crate::reconcile::Reply,
    },
    /// Non-holder asks the lease holder to validate and journal `op`
    /// (postcard-encoded `MutateOp` from constellation-meta).
    MutateRequest {
        part: String,
        requester: u64,
        req_id: u64,
        epoch_seen: u64,
        /// Postcard bytes of `constellation_meta::MutateOp`.
        op: Vec<u8>,
        /// Plan 30 §M2: this op's exactly-once identity. Stable across
        /// every retry of the same op (unlike `req_id`, a fresh
        /// per-attempt correlation id every time) — the holder keys its
        /// dedup/`recent`-outcome lookup on this, not on `req_id`. A
        /// plain `(u64, u32, u64)` here rather than importing
        /// `constellation_meta::Rid`: this crate stays free of the
        /// metadata dependency (see the module doc).
        rid: (u64, u32, u64),
        /// Plan 30 §M2 GC: the highest contiguous `rid.seq` of this
        /// requester's current incarnation whose reply it has already
        /// received. The holder drops `recent` outcomes for that
        /// requester's incarnation up to this seq — a request carries
        /// its own future GC receipt.
        acked_through: u64,
    },
    /// Holder's answer: postcard-encoded `MutateOutcome`.
    MutateReply {
        req_id: u64,
        /// Postcard bytes of `constellation_meta::MutateOutcome`.
        outcome: Vec<u8>,
        /// Plan 30 M5 (§M6's first form): the log position the requester
        /// must have applied before it may install the reply's records
        /// ahead of the log — the holder's last shipped sequence when it
        /// evaluated the op — or `None` when the holder's unshipped
        /// journal already touched one of the op's keys (then only the
        /// log delivers the records, in their order).
        #[serde(default)]
        base: Option<u64>,
        /// Plan 30 §M6: the state the holder evaluated the op against —
        /// its shipped-through log sequence, and its unshipped journal
        /// position `(epoch, journal seq)` if it had one
        /// (`constellation_meta::Position`).
        #[serde(default)]
        position_seq: u64,
        #[serde(default)]
        position_pending: Option<(u64, u64)>,
    },
    /// Plan 30 §M8: a `cto=strict` reader asks the sequencer where the
    /// state of `ino` is (its record; with `dir`, its entries; with
    /// `name`, that entry and its target) — `constellation_authority`'s
    /// `PeerMsg::ReadIndex`.
    ReadIndex {
        requester: u64,
        req_id: u64,
        ino: u64,
        dir: bool,
        name: Option<String>,
    },
    /// The sequencer's answer: `status` 0 = the position (and maybe a
    /// read delegation `(id, ttl_ms, epoch)`), 1 = not the holder
    /// (`holder`: whom it believes holds, 0 unknown), 2 = busy (fenced).
    ReadIndexReply {
        req_id: u64,
        status: u8,
        holder: u64,
        position_seq: u64,
        position_pending: Option<(u64, u64)>,
        grant: Option<(u64, u64, u64)>,
    },
    /// Plan 30 §M8: the sequencer recalls read delegation `grant` on
    /// `ino`; the delegate stops honouring it, then answers
    /// [`Payload::ReadRecalled`].
    ReadRecall {
        holder: u64,
        req_id: u64,
        ino: u64,
        grant: u64,
    },
    ReadRecalled {
        req_id: u64,
    },
    /// Plan 30 §M9: the holder streams whole journal transactions to a
    /// backup (`constellation_authority`'s `PeerMsg::BackupAppend`).
    /// `txs` is postcard of `Vec<constellation_meta::BackupTx>`; `from`
    /// is the journal seq the batch starts at (the seq after what the
    /// backup last acknowledged), `through` the holder's shipped-through
    /// seq. Answered by [`Payload::BackupAck`].
    BackupAppend {
        holder: u64,
        req_id: u64,
        epoch: u64,
        config_version: u64,
        from: u64,
        txs: Vec<u8>,
        through: u64,
    },
    /// Plan 30 §M9: the backup holds every row through `acked`, or has
    /// `sealed` the epoch (it will never acknowledge it again).
    BackupAck {
        req_id: u64,
        epoch: u64,
        acked: u64,
        sealed: bool,
    },
    /// Plan 30 §M9: pre-S3 streaming — backup-acked transactions
    /// (postcard of `Vec<BackupTx>`) evaluated against the log through
    /// `base`, for a subscriber to install ahead of the log. Answered
    /// with [`Payload::Ok`] (nothing to say).
    StreamAhead {
        from: u64,
        epoch: u64,
        base: u64,
        txs: Vec<u8>,
    },
    /// A reply with nothing to say.
    Ok {
        req_id: u64,
    },
    /// Gossiped RTT vector for holder-driven lease placement.
    PeerRtts {
        node_id: u64,
        /// `(peer_id, rtt_ms)` samples.
        rtts: Vec<(u64, u16)>,
    },
    /// Holder offers the lease to a better-placed writer (placement).
    LeaseOffer {
        part: String,
        epoch: u64,
    },
}

/// Why a holder did not serve a requested chunk. The requester needs
/// the distinction to account precisely: `Absent` for a chunk it was
/// told the holder has is a false-positive peer fetch; a chunk the
/// holder dropped moments ago is only a propagation race.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkDecline {
    /// Serving budget exhausted, cooperative cache off, or a local read
    /// error: nothing to learn about membership.
    Busy,
    /// The holder does not have it and has not dropped it recently.
    Absent,
    /// The holder dropped it (evicted, or it became dirty) within its
    /// recent-removal window, so a requester's view may not have caught
    /// up yet.
    RecentlyRemoved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkStatus {
    /// The chunk body follows on the stream.
    Found,
    Declined(ChunkDecline),
}

/// Bytes a [`Signed`] envelope adds around a payload's postcard body:
/// author key, 64-byte signature and their length prefixes.
pub const SIGNED_ENVELOPE: usize = 32 + 1 + 64 + 3;

/// Approximate on-the-wire size of `payload` once signed, for the
/// digest-plane byte counters. Exact postcard body plus the fixed
/// envelope; gossip fan-out and QUIC framing are not included.
pub fn wire_len(payload: &Payload) -> usize {
    postcard::to_allocvec(payload).map(|v| v.len()).unwrap_or(0) + SIGNED_ENVELOPE
}

/// A payload plus its author and signature.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signed {
    /// Author's raw public key (also the iroh endpoint id).
    pub from: [u8; 32],
    /// Ed25519 signature over `body`.
    sig: Vec<u8>,
    /// Exact postcard bytes that were signed.
    body: Vec<u8>,
}

impl Signed {
    pub fn new(key: &SecretKey, payload: &Payload) -> Result<Self> {
        let body = postcard::to_allocvec(payload)?;
        let sig = key.sign(&body);
        Ok(Self {
            from: *key.public().as_bytes(),
            sig: sig.to_bytes().to_vec(),
            body,
        })
    }

    /// Verify the signature and return the payload. The author is
    /// [`Signed::from`]; the caller still has to decide whether that
    /// key is allowed (see [`crate::allowlist`]).
    pub fn verify(&self) -> Result<(PublicKey, Payload)> {
        let key = PublicKey::from_bytes(&self.from)?;
        let sig: [u8; 64] = self
            .sig
            .as_slice()
            .try_into()
            .context("signature must be 64 bytes")?;
        key.verify(&self.body, &iroh::Signature::from_bytes(&sig))
            .context("bad signature on peer message")?;
        Ok((key, postcard::from_bytes(&self.body)?))
    }

    /// Bare postcard envelope used by gossip and inside stream framing.
    pub fn encode_bare(&self) -> Result<Vec<u8>> {
        Ok(postcard::to_allocvec(self)?)
    }

    /// Length-prefixed frame: 4-byte big-endian length, then postcard.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let bare = self.encode_bare()?;
        anyhow::ensure!(bare.len() <= MAX_FRAME, "message too large: {}", bare.len());
        let mut out = Vec::with_capacity(4 + bare.len());
        out.extend_from_slice(&(bare.len() as u32).to_be_bytes());
        out.extend_from_slice(&bare);
        Ok(out)
    }

    /// Decode exactly one bare message. Trailing bytes and a signature
    /// that is not Ed25519-sized are refused: `postcard::from_bytes`
    /// ignores trailing input, which let a length-prefixed frame decode
    /// as a bare message whenever the prefix happened to parse (about 1
    /// in 20 random keys) — the silent framing mismatch
    /// `stream_framing_and_bare_encoding_are_distinct` exists to catch.
    pub fn decode(frame: &[u8]) -> Result<Self> {
        let (signed, rest): (Signed, _) = postcard::take_from_bytes(frame)?;
        anyhow::ensure!(
            rest.is_empty(),
            "{} trailing bytes after a message",
            rest.len()
        );
        anyhow::ensure!(signed.sig.len() == 64, "signature must be 64 bytes");
        Ok(signed)
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
        signed.body[0] ^= 1;
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
            from: *b.public().as_bytes(),
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
        let bare = signed.encode_bare().unwrap();
        let framed = signed.encode().unwrap();
        assert_eq!(framed.len(), bare.len() + 4, "frame adds a length prefix");
        // The bare form is what gossip receivers decode.
        assert_eq!(Signed::decode(&bare).unwrap().verify().unwrap().1, payload);
        // The framed form must not be mistaken for it. What a receiver
        // does is decode *then verify*; decode alone refuses almost every
        // frame structurally, and the signature check closes the rest
        // (a shifted `from`/`sig` pair cannot verify).
        assert!(
            Signed::decode(&framed)
                .and_then(|signed| signed.verify())
                .is_err(),
            "a length-prefixed frame must not be accepted as a bare payload"
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

    #[test]
    fn a_full_bloom_bucket_fits_the_real_gossip_budget() {
        let bits = vec![0xa5; crate::bloom::MAX_BITS_BYTES];
        let msg = Signed::new(
            &key(),
            &Payload::CacheDigest {
                node_id: 1,
                generation: 2,
                nbits: (bits.len() * 8) as u64,
                k: crate::bloom::K,
                n: crate::bloom::ENTRIES_PER_BUCKET as u64,
                bits,
                bucket: 127,
                buckets: 128,
            },
        )
        .unwrap();
        let n = msg.encode_bare().unwrap().len();
        assert!(
            n <= GOSSIP_CONTENT_LIMIT,
            "full digest is {n} bytes, content limit is {GOSSIP_CONTENT_LIMIT}"
        );
    }

    #[test]
    fn a_maximum_delta_batch_fits_the_real_gossip_budget() {
        let msg = Signed::new(
            &key(),
            &Payload::CacheDigestDelta {
                node_id: 1,
                generation: 2,
                adds: vec![[7; 32]; MAX_GOSSIP_DELTA_ADDS],
                buckets: 128,
            },
        )
        .unwrap();
        let n = msg.encode_bare().unwrap().len();
        assert!(
            n <= GOSSIP_CONTENT_LIMIT,
            "maximum delta is {n} bytes, content limit is {GOSSIP_CONTENT_LIMIT}"
        );
    }

    /// A delta of [`crate::reconcile::MAX_DELTA_KEYS`] worst-case keys
    /// (10-byte varints) must fit one gossip frame.
    #[test]
    fn a_maximum_exact_delta_fits_the_real_gossip_budget() {
        use crate::reconcile::{Delta, MAX_DELTA_KEYS};
        // Real gaps between 2048 sorted random keys are ~8-byte varints;
        // charge every key the 10-byte maximum.
        let mut worst = Vec::with_capacity(MAX_DELTA_KEYS * 10);
        for _ in 0..MAX_DELTA_KEYS {
            worst.extend_from_slice(&[0xff; 9]);
            worst.push(0x01);
        }
        let msg = Signed::new(
            &key(),
            &Payload::CacheSetDelta {
                node_id: u64::MAX,
                delta: Delta {
                    incarnation: u64::MAX,
                    seq: u64::MAX,
                    base: [0xff; 16],
                    root: [0xff; 16],
                    adds: worst,
                    removes: Vec::new(),
                },
            },
        )
        .unwrap();
        let n = msg.encode_bare().unwrap().len();
        assert!(
            n <= GOSSIP_CONTENT_LIMIT,
            "maximum exact delta is {n} bytes, content limit is {GOSSIP_CONTENT_LIMIT}"
        );
    }

    /// The largest request and reply a session can produce must fit a
    /// direct-stream frame, or the round silently fails on the wire.
    #[test]
    fn maximum_reconcile_frames_fit_a_stream_frame() {
        use crate::reconcile::{
            respond, KeySet, Query, Range, Summary, MAX_QUERIES_PER_REQUEST, REPLY_BUDGET,
        };
        let queries: Vec<Query> = (0..MAX_QUERIES_PER_REQUEST as u64)
            .map(|i| Query {
                range: Range {
                    bits: 60,
                    path: (1u64 << 60) - 1 - i,
                },
                fp: [0xff; 16],
                count: u64::MAX,
            })
            .collect();
        let req = Signed::new(&key(), &Payload::ReconcileRequest { queries }).unwrap();
        assert!(req.encode().is_ok(), "maximum request exceeds MAX_FRAME");

        // A dense owner answering "send everything" pages at the budget.
        let owner =
            KeySet::from_keys((0..200_000u64).map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15)));
        let empty = KeySet::new();
        let queries: Vec<Query> = Range::level(8)
            .into_iter()
            .map(|r| {
                let a = empty.acc(&r);
                Query {
                    range: r,
                    fp: a.fingerprint(),
                    count: 0,
                }
            })
            .collect();
        let summary = Summary {
            incarnation: u64::MAX,
            seq: u64::MAX,
            root: owner.root_fingerprint(),
            count: owner.len() as u64,
        };
        let reply = respond(&owner, summary, &queries, REPLY_BUDGET);
        let msg = Signed::new(&key(), &Payload::ReconcileReply { reply }).unwrap();
        let n = msg.encode().expect("maximum reply exceeds MAX_FRAME").len();
        assert!(n <= MAX_FRAME, "{n}");

        // And a reply made only of child splits.
        let mirror = KeySet::from_keys((0..200_000u64).map(|i| i.wrapping_mul(31)));
        let queries: Vec<Query> = Range::level(8)
            .into_iter()
            .map(|r| {
                let a = mirror.acc(&r);
                Query {
                    range: r,
                    fp: a.fingerprint(),
                    count: a.count.max(1),
                }
            })
            .collect();
        let reply = respond(&owner, summary, &queries, REPLY_BUDGET);
        let msg = Signed::new(&key(), &Payload::ReconcileReply { reply }).unwrap();
        assert!(
            msg.encode().is_ok(),
            "children-only reply exceeds MAX_FRAME"
        );
    }

    #[test]
    fn wire_len_tracks_the_signed_encoding() {
        let payload = Payload::Ping { node_id: 7 };
        let real = Signed::new(&key(), &payload)
            .unwrap()
            .encode_bare()
            .unwrap()
            .len();
        let est = wire_len(&payload);
        assert!(est.abs_diff(real) <= 8, "estimate {est} vs real {real}");
    }
}
