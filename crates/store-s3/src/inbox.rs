//! The S3 inbox: forwarded mutations for a holder a requester cannot
//! reach over P2P (plan 30 §M13).
//!
//! A non-holder that has an op to sequence and no P2P path to the holder
//! used to pull the lease to itself, and the holder pulled it back on its
//! next write — the "ping-pong" that caps a P2P-off cluster at a few
//! dozen ops/s. Instead it now writes the op, together with its rid, into
//! a *batch object* `inbox/<epoch>/<node>/<n>` (see [`crate::layout::
//! inbox_batch`]), and the holder polls that prefix. The outcome never
//! comes back over a reply: the holder journals `Completed { rid }` (or
//! `Refused { rid, errno }`) with the op's own records, ships them, and
//! the requester — which tails the log anyway — reads the answer there.
//!
//! This module is the S3 half only: batch encoding, the CAS-created
//! submission with its restart-safe numbering, the holder's GET-next
//! poll with idle backoff, discovery of requesters and old epochs by
//! LIST, and GC by unconditional DELETE. Executing a batch, deduplicating
//! rids and mapping outcomes back to FUSE callers live in `crates/cli`
//! and `crates/meta`; the batch carries each op as opaque postcard bytes
//! (`constellation_meta::MutateOp::to_postcard`) so this crate need not
//! know the op vocabulary.
//!
//! ### The request-class budget
//!
//! Every primitive here is one of the four portable verbs plus
//! `If-None-Match` on PUT — the same set the lease and the log already
//! rely on, and the set `constellation doctor` probes. Nothing depends
//! on conditional DELETE, on object versioning, or on anything AWS-only.
//!
//! - **Submit** is one CAS-created PUT per batch. The requester numbers
//!   its batches sequentially and never has more than one PUT in flight,
//!   so `n` is always the next free slot and the holder's GET-next never
//!   sees a gap that fills later.
//! - **Poll** is one GET per requester per round, exactly the
//!   speculative-GET probe the log tailer uses instead of a LIST
//!   (`LogStore::get_run`'s numbers: a GET-404 costs 1/12.5 of a LIST and
//!   is no slower), backed off per requester while it misses.
//! - **Discovery** costs no requests of its own in the steady state: the
//!   holder polls the write-eligible roster it already reads for
//!   membership, minus the peers it is P2P-connected to (those forward
//!   directly). A takeover LISTs `inbox/` once to find every batch below
//!   its epoch.
//! - **GC** is one unconditional DELETE per executed batch, after the
//!   segment carrying its outcomes has shipped. A DELETE that races a
//!   restart or a takeover is harmless: the object is immutable, and
//!   whoever reads it later deduplicates every rid in it.
//!
//! ### Why the numbering survives a restart without a persisted counter
//!
//! `n` restarts at zero in every epoch, and a requester that starts
//! (or restarts) mid-epoch resumes with one LIST of its own prefix
//! (`inbox/<epoch>/<node>/`) — the highest key there plus one. Only this
//! node ever writes under its prefix, so the listing is authoritative
//! the moment it returns; a PUT that timed out but landed is found by
//! the same listing. A fresh epoch needs no LIST at all: this node has
//! not written under it yet, and nobody else can.
//!
//! That listing is only authoritative if GC can never empty the prefix
//! behind the holder's cursor: a holder that consumed and deleted
//! batches `0..=k` while the requester restarted would leave the
//! restarted requester numbering from zero again, at keys the holder
//! (cursor at `k+1`) never probes. So **the holder never deletes a
//! requester's newest consumed batch in the current epoch**
//! ([`gc_keep_newest`]): at most one executed object per requester per
//! epoch stays behind as the high-water mark, and the next epoch's
//! takeover drain sweeps it (every rid in it is already an outcome, so
//! the drain deduplicates and deletes). Batches of older epochs are
//! deleted without exception.
//!
//! ### Why an ambiguous PUT is safe to retry
//!
//! A PUT whose response was lost may or may not have landed. The retry
//! is the same `If-None-Match: *` PUT of the same bytes at the same key.
//! If it lands, fine; if it collides, the collision is read back and
//! compared with what was sent (node, incarnation, `n`, the ops): a match
//! means the first attempt landed and the batch is submitted. A mismatch
//! is impossible by construction (one writer per prefix) and is reported
//! as [`StoreError::AlreadyExists`] rather than papered over. Either
//! way the batch is submitted exactly once, and the rids inside make
//! even a submission that *did* happen twice execute once.

use crate::e2e::{decrypt_object, encrypt_object, SharedE2eKeys};
use crate::error::StoreError;
use crate::layout;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, ObjectStoreExt, PutMode};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Current batch encoding version. A reader refuses any other
/// ([`StoreError::InboxVersion`]): plan 30 waives compatibility, and a
/// batch is a short-lived object that never outlives an upgrade window.
pub const INBOX_VERSION: u32 = 2;

/// The magic the version tag follows: a batch object is self-describing
/// enough that a stray object under the prefix is rejected as corrupt
/// rather than decoded into nonsense.
const MAGIC: &[u8; 4] = b"CINB";

const ZSTD_LEVEL: i32 = 3;

/// Hard cap on the ops one batch may carry. Batches are group-commit
/// windows, not files: a requester under load fills one with everything
/// that arrived while the previous PUT was in flight, and past this many
/// ops the marginal request saving is nil while the holder's execute
/// time per batch (and the size of the segment carrying its outcomes)
/// keeps growing.
pub const MAX_OPS_PER_BATCH: usize = 512;

/// Soft cap on a batch's encoded size, for the same reason.
pub const MAX_BATCH_BYTES: usize = 1 << 20;

/// `(node, incarnation, seq)`, mirroring `constellation_meta::Rid` field
/// for field. Repeated here rather than imported because this crate
/// sits below `meta` in the dependency graph; the `cli` crate converts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InboxRid {
    pub node: u64,
    pub incarnation: u32,
    pub seq: u64,
}

/// One forwarded op: its exactly-once identity and its postcard-encoded
/// `MutateOp`, opaque to this crate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxOp {
    pub rid: InboxRid,
    pub op: Vec<u8>,
}

/// One batch object. `epoch`, `node` and `n` are also the key; they are
/// repeated in the body so a decoded batch is self-identifying (the
/// ambiguous-PUT comparison, and a defence against an object copied to
/// the wrong key). `incarnation` is the requester's mount incarnation:
/// with `node` and `n` it is the batch's identity across retries, and it
/// tells a holder which mount submitted it (a restarted requester's
/// batches carry a higher one, so nothing it wrote before can be
/// confused with what it writes now).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxBatch {
    pub epoch: u64,
    pub node: u64,
    pub incarnation: u32,
    pub n: u64,
    /// Requester wall clock at submission, for logs and status only.
    /// Nothing decides anything from it (clock skew must not matter).
    pub submitted_unix_ms: i64,
    pub ops: Vec<InboxOp>,
    /// Plan 30 §M5: the requester's inbox demand is sustained and it is
    /// asking for the lease. The holder treats it as the requester having
    /// written itself into `wanted_by` — it learns at its next poll of
    /// this requester rather than at its half-TTL renewal, with no extra
    /// S3 request on either side.
    #[serde(default)]
    pub wants_lease: bool,
}

impl InboxBatch {
    pub fn key(&self) -> InboxKey {
        InboxKey {
            epoch: self.epoch,
            node: self.node,
            n: self.n,
        }
    }

    /// What two submissions of "the same batch" must agree on for a
    /// collision to count as our own earlier attempt: everything but the
    /// submission clock (a retry re-stamps it).
    fn identity(&self) -> (u64, u64, u32, u64, &[InboxOp]) {
        (self.epoch, self.node, self.incarnation, self.n, &self.ops)
    }

    /// `MAGIC ++ version(4, BE) ++ postcard(body)`. The version tag is
    /// outside the postcard body so a reader can refuse an unknown
    /// version before trying to decode anything with the wrong schema.
    pub fn encode(&self) -> Result<Vec<u8>, StoreError> {
        let body = postcard::to_allocvec(self)
            .map_err(|e| StoreError::CorruptObject(format!("inbox batch encode: {e}")))?;
        let mut out = Vec::with_capacity(8 + body.len());
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&INBOX_VERSION.to_be_bytes());
        out.extend_from_slice(&body);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StoreError> {
        if bytes.len() < 8 || &bytes[0..4] != MAGIC {
            return Err(StoreError::CorruptObject(
                "inbox batch: bad magic".to_string(),
            ));
        }
        let version = u32::from_be_bytes(bytes[4..8].try_into().expect("4 bytes"));
        if version != INBOX_VERSION {
            return Err(StoreError::InboxVersion(version));
        }
        postcard::from_bytes(&bytes[8..])
            .map_err(|e| StoreError::CorruptObject(format!("inbox batch decode: {e}")))
    }
}

/// The `(epoch, node, n)` address of one batch. Ordered the way the
/// bucket lists it, which is the order a drain executes in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InboxKey {
    pub epoch: u64,
    pub node: u64,
    pub n: u64,
}

impl InboxKey {
    pub fn path(&self) -> Path {
        layout::inbox_batch(self.epoch, self.node, self.n)
    }

    /// Inverse of [`InboxKey::path`]; `None` for anything else under the
    /// prefix (a stray object is ignored, never executed).
    pub fn parse(path: &Path) -> Option<InboxKey> {
        let mut parts = path.parts();
        if parts.next()?.as_ref() != "inbox" {
            return None;
        }
        let epoch = u64::from_str_radix(parts.next()?.as_ref(), 16).ok()?;
        let node = u64::from_str_radix(parts.next()?.as_ref(), 16).ok()?;
        let n = u64::from_str_radix(parts.next()?.as_ref(), 16).ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some(InboxKey { epoch, node, n })
    }
}

/// How a [`InboxStore::put_batch`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutBatch {
    /// The CAS-create landed now.
    Created,
    /// The key was already taken by a byte-for-byte identical earlier
    /// attempt of this very batch (an ambiguous PUT that had in fact
    /// landed). Submitted, exactly once.
    AlreadyOurs,
}

/// Batch I/O for one filesystem's `inbox/` prefix.
#[derive(Clone)]
pub struct InboxStore {
    store: Arc<dyn ObjectStore>,
    e2e: Option<SharedE2eKeys>,
}

impl InboxStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store, e2e: None }
    }

    /// Batches carry file names and whole manifests, exactly what the
    /// log carries; under E2E they are sealed under the same partition
    /// DEK as the log, with the object key as AAD.
    pub fn new_e2e(store: Arc<dyn ObjectStore>, keys: SharedE2eKeys) -> Self {
        Self {
            store,
            e2e: Some(keys),
        }
    }

    pub fn inner(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    pub fn is_e2e(&self) -> bool {
        self.e2e.is_some()
    }

    fn seal(&self, key: &Path, plaintext: &[u8]) -> Result<Vec<u8>, StoreError> {
        let compressed = zstd::encode_all(plaintext, ZSTD_LEVEL)?;
        match &self.e2e {
            Some(keys) => Ok(encrypt_object(
                &keys.dek(crate::log::PARTITION),
                key.as_ref().as_bytes(),
                &compressed,
            )?),
            None => Ok(compressed),
        }
    }

    fn open(&self, key: &Path, body: &[u8]) -> Result<Vec<u8>, StoreError> {
        let compressed = match &self.e2e {
            Some(keys) => decrypt_object(
                &keys.dek(crate::log::PARTITION),
                key.as_ref().as_bytes(),
                body,
            )?,
            None => body.to_vec(),
        };
        Ok(zstd::decode_all(&compressed[..])?)
    }

    /// CAS-create `batch` at its key, through [`crate::cas::put_conditional`]
    /// (plan 30 §M4's rules: a 409 retries the same attempt; a 412 whose
    /// object turns out to be absent is retried as a 409 the classifier
    /// could not see). See the module doc for why a real collision is
    /// read back and compared rather than treated as an error outright:
    /// [`PutBatch::AlreadyOurs`] is the ambiguous-PUT retry finding its
    /// own earlier attempt. The comparison is by batch identity, not
    /// bytes ([`crate::cas::Verify::Caller`]): a sealed body is encrypted
    /// under a fresh nonce per attempt, so byte equality would miss our
    /// own write. A collision with *different* content is
    /// [`StoreError::AlreadyExists`]: somebody else wrote under this
    /// node's prefix, which the numbering rules make impossible, so the
    /// caller resyncs with a LIST rather than guessing.
    pub async fn put_batch(&self, batch: &InboxBatch) -> Result<PutBatch, StoreError> {
        let key = batch.key().path();
        let body = self.seal(&key, &batch.encode()?)?;
        match crate::cas::put_conditional(
            self.store.as_ref(),
            &key,
            bytes::Bytes::from(body),
            PutMode::Create,
            crate::cas::Verify::Caller,
        )
        .await?
        {
            crate::cas::CasPut::Won(_) => Ok(PutBatch::Created),
            crate::cas::CasPut::Lost | crate::cas::CasPut::Missing => {
                match self.get_batch(batch.key()).await? {
                    Some(existing) if existing.identity() == batch.identity() => {
                        Ok(PutBatch::AlreadyOurs)
                    }
                    _ => Err(StoreError::AlreadyExists),
                }
            }
        }
    }

    /// One batch, or `None` if the key is absent. A batch whose body
    /// does not name the key it sits at is reported as corrupt: nothing
    /// under `inbox/` is ever executed on the strength of its key alone.
    pub async fn get_batch(&self, key: InboxKey) -> Result<Option<InboxBatch>, StoreError> {
        let path = key.path();
        let res = match self.store.get(&path).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let body = res.bytes().await?;
        let batch = InboxBatch::decode(&self.open(&path, &body)?)?;
        if batch.key() != key {
            return Err(StoreError::CorruptObject(format!(
                "inbox batch at {path} names {:?}",
                batch.key()
            )));
        }
        Ok(Some(batch))
    }

    /// GET-next: batches `from, from+1, …` of one requester under one
    /// epoch, `k` in flight, returning the longest contiguous run present
    /// — the same shape as `LogStore::get_run`, for the same reason: in
    /// the steady state a poll is one GET that misses, which is the
    /// cheapest way S3 has of saying "nothing new".
    pub async fn get_run(
        &self,
        epoch: u64,
        node: u64,
        from: u64,
        k: usize,
    ) -> Result<Vec<InboxBatch>, StoreError> {
        use futures::StreamExt;
        if k == 0 {
            return Ok(Vec::new());
        }
        let mut fetched = futures::stream::iter((0..k as u64).map(|i| {
            let n = from.saturating_add(i);
            async move { (n, self.get_batch(InboxKey { epoch, node, n }).await) }
        }))
        .buffered(k);
        let mut run = Vec::new();
        while let Some((_, got)) = fetched.next().await {
            match got? {
                Some(batch) => run.push(batch),
                None => break,
            }
        }
        Ok(run)
    }

    /// Every batch key under `prefix`, ascending. Stray objects (keys
    /// that do not parse) are skipped.
    async fn list_keys(&self, prefix: &Path) -> Result<Vec<InboxKey>, StoreError> {
        let mut keys: Vec<InboxKey> = self
            .store
            .list(Some(prefix))
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .filter_map(|m| InboxKey::parse(&m.location))
            .collect();
        keys.sort_unstable();
        Ok(keys)
    }

    /// The highest `n` this requester has written under `epoch`, if any
    /// — the LIST-last a (re)starting requester resumes its numbering
    /// from.
    pub async fn last_n(&self, epoch: u64, node: u64) -> Result<Option<u64>, StoreError> {
        Ok(self
            .list_keys(&layout::inbox_requester_prefix(epoch, node))
            .await?
            .into_iter()
            .filter(|k| k.epoch == epoch && k.node == node)
            .map(|k| k.n)
            .max())
    }

    /// Every batch under `epoch`, in `(node, n)` order.
    pub async fn list_epoch(&self, epoch: u64) -> Result<Vec<InboxKey>, StoreError> {
        Ok(self
            .list_keys(&layout::inbox_epoch_prefix(epoch))
            .await?
            .into_iter()
            .filter(|k| k.epoch == epoch)
            .collect())
    }

    /// Every batch in the bucket, in `(epoch, node, n)` order: one LIST.
    pub async fn list_all(&self) -> Result<Vec<InboxKey>, StoreError> {
        self.list_keys(&layout::inbox_prefix()).await
    }

    /// The requesters that have at least one batch under `epoch` — what
    /// a LIST-based discovery would poll. The steady-state holder does
    /// not need this (it polls the roster); a takeover uses
    /// [`InboxStore::drain_below`] instead.
    pub async fn requesters_in(&self, epoch: u64) -> Result<Vec<u64>, StoreError> {
        let mut nodes: Vec<u64> = self
            .list_epoch(epoch)
            .await?
            .into_iter()
            .map(|k| k.node)
            .collect();
        nodes.dedup();
        Ok(nodes)
    }

    /// Everything a new holder of `epoch` must execute before it opens
    /// its view: every batch of every lower epoch, fetched in
    /// `(epoch, node, n)` order — per requester that is submission
    /// order, and across requesters it is the order the bucket lists.
    /// One LIST plus one GET per batch found. A batch deleted between the
    /// LIST and its GET (the old holder's late GC, or its requester
    /// re-submitting) is simply not returned.
    pub async fn drain_below(&self, epoch: u64) -> Result<Vec<InboxBatch>, StoreError> {
        let mut out = Vec::new();
        for key in self.list_all().await? {
            if key.epoch >= epoch {
                break;
            }
            if let Some(batch) = self.get_batch(key).await? {
                out.push(batch);
            }
        }
        Ok(out)
    }

    /// Unconditional DELETE; a missing key is success (whoever got there
    /// first did the same job).
    pub async fn delete(&self, key: InboxKey) -> Result<(), StoreError> {
        match self.store.delete(&key.path()).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

/// The requester side of the numbering: `next_n` for one `(epoch,
/// node)`, resumed from a LIST once per (re)start and advanced by every
/// successful submission. One in flight at a time by construction (the
/// methods take `&mut self`).
pub struct InboxSubmitter {
    store: InboxStore,
    node: u64,
    incarnation: u32,
    epoch: u64,
    next_n: u64,
}

/// Bound on the resync-and-retry loop in [`InboxSubmitter::submit`]. It
/// only ever iterates on a foreign collision, which the numbering rules
/// rule out; the bound turns "impossible" into an error instead of a
/// spin.
const MAX_RESYNC_ATTEMPTS: usize = 8;

impl InboxSubmitter {
    /// A submitter for an epoch this incarnation has not written under
    /// yet: numbering starts at zero, no request needed.
    pub fn fresh(store: InboxStore, node: u64, incarnation: u32, epoch: u64) -> Self {
        Self {
            store,
            node,
            incarnation,
            epoch,
            next_n: 0,
        }
    }

    /// A submitter that continues wherever this node's earlier writes
    /// under `epoch` left off — one LIST of the node's own prefix.
    /// Used at (re)start, when a previous incarnation may have written
    /// under the current epoch; see the module doc.
    pub async fn resume(
        store: InboxStore,
        node: u64,
        incarnation: u32,
        epoch: u64,
    ) -> Result<Self, StoreError> {
        let next_n = store.last_n(epoch, node).await?.map(|n| n + 1).unwrap_or(0);
        Ok(Self {
            store,
            node,
            incarnation,
            epoch,
            next_n,
        })
    }

    /// Move to a later epoch: numbering restarts at zero, no request
    /// needed (this incarnation has not written there, and nobody else
    /// can). Going *back* to an older epoch is a caller bug and refused.
    pub fn advance_epoch(&mut self, epoch: u64) {
        assert!(epoch >= self.epoch, "inbox epoch cannot move backwards");
        if epoch > self.epoch {
            self.epoch = epoch;
            self.next_n = 0;
        }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn node(&self) -> u64 {
        self.node
    }

    pub fn next_n(&self) -> u64 {
        self.next_n
    }

    /// Submit `ops` as the next batch. On success the batch is durable
    /// at the returned key and `next_n` has advanced. On a transport
    /// error nothing has advanced: the caller retries with the *same*
    /// `ops`, and if the first attempt had in fact landed, the retry
    /// finds it ([`PutBatch::AlreadyOurs`]) and advances. Different
    /// `ops` after a transport error would be a caller bug: they would
    /// collide with the earlier attempt and come back as
    /// [`StoreError::AlreadyExists`].
    pub async fn submit(
        &mut self,
        ops: Vec<InboxOp>,
        now_unix_ms: i64,
    ) -> Result<InboxKey, StoreError> {
        assert!(!ops.is_empty(), "an inbox batch carries at least one op");
        assert!(
            ops.len() <= MAX_OPS_PER_BATCH,
            "inbox batch of {} ops exceeds MAX_OPS_PER_BATCH",
            ops.len()
        );
        let mut batch = InboxBatch {
            epoch: self.epoch,
            node: self.node,
            incarnation: self.incarnation,
            n: self.next_n,
            submitted_unix_ms: now_unix_ms,
            ops,
            wants_lease: false,
        };
        for _ in 0..MAX_RESYNC_ATTEMPTS {
            match self.store.put_batch(&batch).await {
                Ok(PutBatch::Created) | Ok(PutBatch::AlreadyOurs) => {
                    self.next_n = batch.n + 1;
                    return Ok(batch.key());
                }
                Err(StoreError::AlreadyExists) => {
                    // Foreign content at our slot: by the numbering rules
                    // this cannot happen, but a LIST is the honest way to
                    // find out where our prefix really ends.
                    let last = self.store.last_n(self.epoch, self.node).await?;
                    self.next_n = last.map(|n| n + 1).unwrap_or(0);
                    batch.n = self.next_n;
                }
                Err(e) => return Err(e),
            }
        }
        Err(StoreError::Conflict(format!(
            "inbox/{:x}/{:x}: could not find a free batch slot after {MAX_RESYNC_ATTEMPTS} attempts",
            self.epoch, self.node
        )))
    }
}

/// Exponential idle backoff for one polled requester: the poll interval
/// doubles per miss from `base_ms` up to a ceiling and snaps back to
/// `base_ms` on a hit — `node_runtime`'s sync-loop schedule
/// (`next_poll_ms`), applied per requester rather than per node, so a
/// busy requester's hits never make the holder hammer an idle one.
///
/// Two ceilings (plan 30 M13's latency decision): a requester that has
/// submitted recently is *warm* and its polls settle at `warm_max_ms`
/// (default 2 s), so its next occasional write is picked up within that;
/// after [`COLD_AFTER_ROUNDS`] consecutive misses at the warm ceiling
/// (about a minute) it is *cold* and settles at `cold_max_ms` (the sync
/// loop's own idle ceiling, 10 s), which is what an idle cluster pays. A
/// hit makes it warm again. The first write after a long quiet therefore
/// waits up to the cold ceiling, exactly like a P2P-off follower's
/// freshness bound today; everything within the following minute waits
/// at most the warm one.
///
/// A third, *hot* tier sits under the base interval (plan 30 M13 round
/// 2): right after a hit, and for `hot_grace` misses after it, the
/// requester is polled every `hot_ms` — a requester that is writing right
/// now has its next batch picked up within tens of milliseconds, which
/// is what makes an inbox round trip short enough to beat the lease
/// ping-pong it replaces. A hit costs one GET that returns data; the
/// misses in the grace window cost `hot_grace` cheap GETs per burst.
/// The default (`hot_ms == base_ms`, `hot_grace == 0`) is the plain
/// schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollBackoff {
    base_ms: u64,
    warm_max_ms: u64,
    cold_max_ms: u64,
    hot_ms: u64,
    hot_grace: u32,
    idle_rounds: u32,
}

/// Consecutive misses at the warm ceiling before a requester is cold.
/// With a 2 s warm ceiling and the doublings before it (0.5+1+2 s), this
/// is roughly a minute of silence.
pub const COLD_AFTER_ROUNDS: u32 = 32;

impl PollBackoff {
    /// One ceiling: warm and cold are the same.
    pub fn new(base_ms: u64, max_ms: u64) -> Self {
        Self::two_tier(base_ms, max_ms, max_ms)
    }

    pub fn two_tier(base_ms: u64, warm_max_ms: u64, cold_max_ms: u64) -> Self {
        let base_ms = base_ms.max(1);
        let warm_max_ms = warm_max_ms.max(base_ms);
        Self {
            base_ms,
            warm_max_ms,
            cold_max_ms: cold_max_ms.max(warm_max_ms),
            hot_ms: base_ms,
            hot_grace: 0,
            idle_rounds: 0,
        }
    }

    /// Poll every `hot_ms` after a hit and for `grace` misses after it
    /// (see the type doc). `hot_ms` is clamped to the base interval.
    pub fn with_hot(mut self, hot_ms: u64, grace: u32) -> Self {
        self.hot_ms = hot_ms.clamp(1, self.base_ms);
        self.hot_grace = grace;
        self
    }

    /// Whether this requester has gone cold (see the type doc).
    pub fn is_cold(&self) -> bool {
        self.idle_rounds >= self.hot_grace.saturating_add(COLD_AFTER_ROUNDS)
    }

    /// Whether this requester is in its hot window (see the type doc).
    pub fn is_hot(&self) -> bool {
        self.idle_rounds < self.hot_grace
    }

    /// A poll that found something: back to the base interval.
    pub fn hit(&mut self) {
        self.idle_rounds = 0;
    }

    /// A poll that found nothing: one more doubling, capped.
    pub fn miss(&mut self) {
        self.idle_rounds = self.idle_rounds.saturating_add(1);
    }

    pub fn idle_rounds(&self) -> u32 {
        self.idle_rounds
    }

    /// How long to wait before the next poll of this requester.
    pub fn delay_ms(&self) -> u64 {
        if self.is_cold() {
            self.cold_max_ms
        } else if self.is_hot() {
            self.hot_ms
        } else {
            next_poll_ms(
                self.base_ms,
                self.idle_rounds - self.hot_grace,
                self.warm_max_ms,
            )
        }
    }
}

/// `interval` doubled per idle round, clamped to `max`, never below
/// `interval` — the same function as `node_runtime::next_poll_ms`, kept
/// in this crate so the poller has no dependency on the binary.
pub fn next_poll_ms(interval_ms: u64, idle_rounds: u32, max_ms: u64) -> u64 {
    let backoff = if idle_rounds >= u64::BITS {
        u64::MAX
    } else {
        interval_ms.saturating_mul(1u64 << idle_rounds)
    };
    backoff.min(max_ms).max(interval_ms)
}

/// One tracked requester on the holder: where its GET-next stands and
/// how backed off it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequesterCursor {
    pub next_n: u64,
    pub backoff: PollBackoff,
}

/// The holder side: per-requester GET-next cursors under one epoch, with
/// per-requester idle backoff. Purely in memory — a holder's tenure is
/// one epoch and one process lifetime, and a successor drains what this
/// one did not finish ([`InboxStore::drain_below`]) instead of inheriting
/// cursors.
///
/// The poller decides *what* to fetch; *when* is the caller's (the sync
/// loop's) business: [`InboxPoller::delay_ms`] says how long each
/// requester wants to be left alone, and [`InboxPoller::min_delay_ms`]
/// is the earliest the loop needs to wake for any of them.
pub struct InboxPoller {
    store: InboxStore,
    epoch: u64,
    base_ms: u64,
    warm_max_ms: u64,
    cold_max_ms: u64,
    hot_ms: u64,
    hot_grace: u32,
    /// Batches fetched per poll; a saturated run tells the caller the
    /// requester is far ahead and to poll again at once.
    width: usize,
    cursors: BTreeMap<u64, RequesterCursor>,
}

/// Default GET-next width: enough to absorb a requester's burst in one
/// round without fanning out sixteen speculative GETs at every idle poll
/// the way the log tailer's catch-up width would.
pub const DEFAULT_POLL_WIDTH: usize = 4;

impl InboxPoller {
    pub fn new(store: InboxStore, epoch: u64, base_ms: u64, max_ms: u64) -> Self {
        Self::two_tier(store, epoch, base_ms, max_ms, max_ms)
    }

    /// See [`PollBackoff::two_tier`].
    pub fn two_tier(
        store: InboxStore,
        epoch: u64,
        base_ms: u64,
        warm_max_ms: u64,
        cold_max_ms: u64,
    ) -> Self {
        Self {
            store,
            epoch,
            base_ms,
            warm_max_ms,
            cold_max_ms,
            hot_ms: base_ms,
            hot_grace: 0,
            width: DEFAULT_POLL_WIDTH,
            cursors: BTreeMap::new(),
        }
    }

    /// See [`PollBackoff::with_hot`]; applies to requesters tracked from
    /// now on.
    pub fn with_hot(mut self, hot_ms: u64, grace: u32) -> Self {
        self.hot_ms = hot_ms;
        self.hot_grace = grace;
        self
    }

    pub fn with_width(mut self, width: usize) -> Self {
        self.width = width.max(1);
        self
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Start polling `node` from batch zero of this epoch. Idempotent.
    pub fn track(&mut self, node: u64) {
        self.cursors.entry(node).or_insert(RequesterCursor {
            next_n: 0,
            backoff: PollBackoff::two_tier(self.base_ms, self.warm_max_ms, self.cold_max_ms)
                .with_hot(self.hot_ms, self.hot_grace),
        });
    }

    /// Stop polling `node` (it left the roster, or it is P2P-connected
    /// again and forwards directly).
    pub fn untrack(&mut self, node: u64) {
        self.cursors.remove(&node);
    }

    /// Reconcile the tracked set with `nodes` (the roster minus the
    /// P2P-connected peers and this node itself): new ones start at
    /// zero, gone ones are dropped, the rest keep their cursors.
    pub fn retain_only(&mut self, nodes: &[u64]) {
        self.cursors.retain(|n, _| nodes.contains(n));
        for n in nodes {
            self.track(*n);
        }
    }

    pub fn requesters(&self) -> Vec<u64> {
        self.cursors.keys().copied().collect()
    }

    pub fn cursor(&self, node: u64) -> Option<RequesterCursor> {
        self.cursors.get(&node).copied()
    }

    /// How long `node` wants to be left alone before its next poll.
    pub fn delay_ms(&self, node: u64) -> Option<u64> {
        self.cursors.get(&node).map(|c| c.backoff.delay_ms())
    }

    /// The shortest delay over every tracked requester, or `None` when
    /// nobody is tracked (then the holder polls nothing at all — the
    /// single-node and healthy-P2P cases).
    pub fn min_delay_ms(&self) -> Option<u64> {
        self.cursors.values().map(|c| c.backoff.delay_ms()).min()
    }

    /// The requesters whose delay has run out after `elapsed_ms` of
    /// quiet, in id order.
    pub fn due(&self, elapsed_ms: u64) -> Vec<u64> {
        self.cursors
            .iter()
            .filter(|(_, c)| c.backoff.delay_ms() <= elapsed_ms)
            .map(|(n, _)| *n)
            .collect()
    }

    /// GET-next for `node`: the contiguous run of new batches from its
    /// cursor, cursor advanced past them, backoff reset on a hit and
    /// bumped on a miss. The caller executes the batches in the order
    /// returned; if it cannot finish one, it [`InboxPoller::rewind`]s to
    /// that batch's `n` so nothing is skipped.
    pub async fn poll(&mut self, node: u64) -> Result<Vec<InboxBatch>, StoreError> {
        let Some(cursor) = self.cursors.get(&node).copied() else {
            return Ok(Vec::new());
        };
        let run = self
            .store
            .get_run(self.epoch, node, cursor.next_n, self.width)
            .await?;
        let c = self.cursors.get_mut(&node).expect("checked above");
        if run.is_empty() {
            c.backoff.miss();
        } else {
            c.backoff.hit();
            c.next_n = cursor.next_n + run.len() as u64;
        }
        Ok(run)
    }

    /// Whether the last poll of `node` came back saturated (as many
    /// batches as the width), i.e. it is worth polling again at once.
    pub fn saturated(&self, run_len: usize) -> bool {
        run_len >= self.width
    }

    /// Put `node`'s cursor back to `n` (a batch the caller could not
    /// execute), so the next poll re-fetches from there.
    pub fn rewind(&mut self, node: u64, n: u64) {
        if let Some(c) = self.cursors.get_mut(&node) {
            c.next_n = c.next_n.min(n);
        }
    }

    /// GC one executed batch. The caller invokes this only once the
    /// segment carrying the batch's outcomes has shipped, and only for
    /// keys [`gc_keep_newest`] returns; see the module doc for why the
    /// DELETE itself may be unconditional.
    pub async fn delete(&self, key: InboxKey) -> Result<(), StoreError> {
        self.store.delete(key).await
    }

    pub fn store(&self) -> &InboxStore {
        &self.store
    }
}

/// The GC rule for the current epoch: of the executed batches in
/// `executed` (any order), every one except the highest-`n` batch of each
/// `(epoch, node)` may be deleted. The one kept is the requester's
/// high-water mark, which its LIST-last resume after a restart depends
/// on (module doc). Keeping the highest *executed* rather than the
/// highest *existing* batch is conservative (a newer, not-yet-consumed
/// batch may exist) and needs no extra request.
///
/// Batches below the holder's epoch are not subject to this rule (the
/// caller passes only current-epoch keys here; a drain deletes old
/// epochs outright).
pub fn gc_keep_newest(executed: &[InboxKey]) -> Vec<InboxKey> {
    let mut newest: BTreeMap<(u64, u64), u64> = BTreeMap::new();
    for k in executed {
        let e = newest.entry((k.epoch, k.node)).or_insert(k.n);
        *e = (*e).max(k.n);
    }
    let mut out: Vec<InboxKey> = executed
        .iter()
        .filter(|k| newest.get(&(k.epoch, k.node)) != Some(&k.n))
        .copied()
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::{PutOptions, PutPayload};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn rid(node: u64, seq: u64) -> InboxRid {
        InboxRid {
            node,
            incarnation: 1,
            seq,
        }
    }

    fn op(node: u64, seq: u64) -> InboxOp {
        InboxOp {
            rid: rid(node, seq),
            op: format!("op-{node}-{seq}").into_bytes(),
        }
    }

    fn batch(epoch: u64, node: u64, n: u64, ops: Vec<InboxOp>) -> InboxBatch {
        InboxBatch {
            epoch,
            node,
            incarnation: 1,
            n,
            submitted_unix_ms: 1_000,
            ops,
            wants_lease: false,
        }
    }

    fn mem() -> InboxStore {
        InboxStore::new(Arc::new(InMemory::new()))
    }

    #[test]
    fn batch_encoding_roundtrips_behind_a_version_tag() {
        let b = batch(3, 7, 2, vec![op(7, 0), op(7, 1)]);
        let bytes = b.encode().unwrap();
        assert_eq!(&bytes[0..4], b"CINB");
        assert_eq!(
            u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
            INBOX_VERSION
        );
        assert_eq!(InboxBatch::decode(&bytes).unwrap(), b);

        // An unknown version is refused before any decoding is attempted.
        let mut future = bytes.clone();
        future[4..8].copy_from_slice(&(INBOX_VERSION + 1).to_be_bytes());
        assert!(matches!(
            InboxBatch::decode(&future),
            Err(StoreError::InboxVersion(v)) if v == INBOX_VERSION + 1
        ));
        // As is anything that is not a batch at all.
        assert!(matches!(
            InboxBatch::decode(b"not a batch"),
            Err(StoreError::CorruptObject(_))
        ));
        assert!(matches!(
            InboxBatch::decode(&bytes[..6]),
            Err(StoreError::CorruptObject(_))
        ));
    }

    #[test]
    fn keys_parse_back_from_their_paths() {
        let key = InboxKey {
            epoch: 0x1f,
            node: u64::MAX,
            n: 42,
        };
        assert_eq!(InboxKey::parse(&key.path()), Some(key));
        assert_eq!(InboxKey::parse(&Path::from("inbox/zz/00/00")), None);
        assert_eq!(InboxKey::parse(&Path::from("inbox/01/02")), None);
        assert_eq!(InboxKey::parse(&Path::from("inbox/01/02/03/04")), None);
        assert_eq!(InboxKey::parse(&Path::from("log/p0/01")), None);
    }

    #[tokio::test]
    async fn put_batch_is_create_only_and_recognizes_its_own_retry() {
        let s = mem();
        let b = batch(1, 5, 0, vec![op(5, 0)]);
        assert_eq!(s.put_batch(&b).await.unwrap(), PutBatch::Created);
        assert_eq!(s.get_batch(b.key()).await.unwrap(), Some(b.clone()));

        // The identical batch again (the ambiguous-PUT retry) is ours.
        let mut retry = b.clone();
        retry.submitted_unix_ms += 99; // a retry re-stamps the clock
        assert_eq!(s.put_batch(&retry).await.unwrap(), PutBatch::AlreadyOurs);
        // The bucket keeps the first attempt's bytes.
        assert_eq!(
            s.get_batch(b.key())
                .await
                .unwrap()
                .unwrap()
                .submitted_unix_ms,
            1_000
        );

        // Different content at the same key is a foreign collision.
        let foreign = batch(1, 5, 0, vec![op(5, 9)]);
        assert!(matches!(
            s.put_batch(&foreign).await,
            Err(StoreError::AlreadyExists)
        ));
        let mut other_mount = b.clone();
        other_mount.incarnation = 2;
        assert!(matches!(
            s.put_batch(&other_mount).await,
            Err(StoreError::AlreadyExists)
        ));
        assert_eq!(s.get_batch(b.key()).await.unwrap(), Some(b));
    }

    #[tokio::test]
    async fn a_batch_at_the_wrong_key_is_corrupt_not_executed() {
        let s = mem();
        let b = batch(1, 5, 0, vec![op(5, 0)]);
        let wrong = InboxKey {
            epoch: 1,
            node: 5,
            n: 7,
        }
        .path();
        let body = s.seal(&wrong, &b.encode().unwrap()).unwrap();
        s.inner().put(&wrong, PutPayload::from(body)).await.unwrap();
        assert!(matches!(
            s.get_batch(InboxKey {
                epoch: 1,
                node: 5,
                n: 7
            })
            .await,
            Err(StoreError::CorruptObject(_))
        ));
    }

    #[tokio::test]
    async fn get_run_stops_at_the_first_gap() {
        let s = mem();
        for n in [0u64, 1, 2, 4] {
            s.put_batch(&batch(1, 5, n, vec![op(5, n)])).await.unwrap();
        }
        let run = s.get_run(1, 5, 0, 8).await.unwrap();
        assert_eq!(run.iter().map(|b| b.n).collect::<Vec<_>>(), vec![0, 1, 2]);
        // The steady-state miss: nothing at the cursor.
        assert!(s.get_run(1, 5, 3, 8).await.unwrap().is_empty());
        // Another requester, another epoch: independent streams.
        assert!(s.get_run(1, 6, 0, 8).await.unwrap().is_empty());
        assert!(s.get_run(2, 5, 0, 8).await.unwrap().is_empty());
        assert_eq!(s.get_run(1, 5, 1, 2).await.unwrap().len(), 2);
        assert!(s.get_run(1, 5, 0, 0).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn submitter_numbers_sequentially_and_resumes_after_a_restart() {
        let s = mem();
        let mut sub = InboxSubmitter::fresh(s.clone(), 5, 1, 1);
        assert_eq!(sub.next_n(), 0);
        let k0 = sub.submit(vec![op(5, 0)], 1).await.unwrap();
        let k1 = sub.submit(vec![op(5, 1), op(5, 2)], 2).await.unwrap();
        assert_eq!((k0.n, k1.n, sub.next_n()), (0, 1, 2));

        // "Restart": a new incarnation resumes from a LIST of its own
        // prefix, after the old mount's last batch.
        let mut restarted = InboxSubmitter::resume(s.clone(), 5, 2, 1).await.unwrap();
        assert_eq!(restarted.next_n(), 2);
        let k2 = restarted.submit(vec![op(5, 0)], 3).await.unwrap();
        assert_eq!(k2.n, 2);
        assert_eq!(s.last_n(1, 5).await.unwrap(), Some(2));
        assert_eq!(
            s.get_batch(k2).await.unwrap().unwrap().incarnation,
            2,
            "the batch says which mount wrote it"
        );

        // A prefix nobody wrote under resumes at zero, and a fresh epoch
        // restarts numbering without a request.
        let untouched = InboxSubmitter::resume(s.clone(), 9, 1, 1).await.unwrap();
        assert_eq!(untouched.next_n(), 0);
        restarted.advance_epoch(2);
        assert_eq!((restarted.epoch(), restarted.next_n()), (2, 0));
        let k = restarted.submit(vec![op(5, 1)], 4).await.unwrap();
        assert_eq!((k.epoch, k.n), (2, 0));
        // The old epoch's numbering is untouched by the new one's.
        assert_eq!(s.last_n(1, 5).await.unwrap(), Some(2));
        assert_eq!(s.last_n(2, 5).await.unwrap(), Some(0));
    }

    /// A store whose PUTs land but whose responses get lost: the shape
    /// of a request that timed out after S3 committed it.
    struct AmbiguousStore {
        inner: InMemory,
        lose_next_put_response: AtomicBool,
        puts: AtomicUsize,
    }

    impl std::fmt::Display for AmbiguousStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "AmbiguousStore")
        }
    }

    impl std::fmt::Debug for AmbiguousStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "AmbiguousStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for AmbiguousStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            let res = self.inner.put_opts(location, payload, opts).await?;
            if self.lose_next_put_response.swap(false, Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "AmbiguousStore",
                    source: "request timed out after the object was committed".into(),
                });
            }
            Ok(res)
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn an_ambiguous_put_is_retried_and_submitted_exactly_once() {
        let raw = Arc::new(AmbiguousStore {
            inner: InMemory::new(),
            lose_next_put_response: AtomicBool::new(true),
            puts: AtomicUsize::new(0),
        });
        let s = InboxStore::new(raw.clone());
        let mut sub = InboxSubmitter::fresh(s.clone(), 5, 1, 1);
        let ops = vec![op(5, 0), op(5, 1)];

        // First attempt: committed, but the caller only sees an error and
        // the cursor does not move.
        let err = sub.submit(ops.clone(), 1).await.unwrap_err();
        assert!(matches!(err, StoreError::ObjectStore(_)), "{err}");
        assert_eq!(sub.next_n(), 0);
        assert_eq!(s.last_n(1, 5).await.unwrap(), Some(0), "it did land");

        // The retry with the same ops finds its own earlier attempt.
        let key = sub.submit(ops.clone(), 2).await.unwrap();
        assert_eq!((key.n, sub.next_n()), (0, 1));
        assert_eq!(raw.puts.load(Ordering::SeqCst), 2);
        let all = s.list_all().await.unwrap();
        assert_eq!(all.len(), 1, "exactly one batch object: {all:?}");
        let stored = s.get_batch(key).await.unwrap().unwrap();
        assert_eq!(stored.ops, ops);
        assert_eq!(stored.submitted_unix_ms, 1, "the first attempt's bytes");

        // Life goes on at n=1.
        let next = sub.submit(vec![op(5, 2)], 3).await.unwrap();
        assert_eq!(next.n, 1);
    }

    #[tokio::test]
    async fn a_retry_with_different_ops_is_a_foreign_collision_not_a_silent_overwrite() {
        let raw = Arc::new(AmbiguousStore {
            inner: InMemory::new(),
            lose_next_put_response: AtomicBool::new(true),
            puts: AtomicUsize::new(0),
        });
        let s = InboxStore::new(raw);
        let mut sub = InboxSubmitter::fresh(s.clone(), 5, 1, 1);
        sub.submit(vec![op(5, 0)], 1).await.unwrap_err();
        // The caller violates the contract and retries with other ops:
        // the submitter resyncs past the landed batch instead of
        // overwriting it, so nothing submitted is ever lost or replaced.
        let key = sub.submit(vec![op(5, 1)], 2).await.unwrap();
        assert_eq!(key.n, 1);
        let first = s
            .get_batch(InboxKey {
                epoch: 1,
                node: 5,
                n: 0,
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.ops, vec![op(5, 0)]);
    }

    /// Two writers under one prefix must never share a slot, and both
    /// must keep making progress. The design has one writer per prefix;
    /// this pins what the CAS gives even if that rule were broken.
    #[tokio::test]
    async fn concurrent_writers_never_share_a_slot() {
        let s = mem();
        let mut a = InboxSubmitter::fresh(s.clone(), 5, 1, 1);
        let mut b = InboxSubmitter::fresh(s.clone(), 5, 2, 1);
        const EACH: u64 = 12;
        let fa = async {
            let mut keys = Vec::new();
            for i in 0..EACH {
                keys.push(
                    a.submit(
                        vec![InboxOp {
                            rid: InboxRid {
                                node: 5,
                                incarnation: 1,
                                seq: i,
                            },
                            op: vec![1],
                        }],
                        1,
                    )
                    .await
                    .unwrap(),
                );
                tokio::task::yield_now().await;
            }
            keys
        };
        let fb = async {
            let mut keys = Vec::new();
            for i in 0..EACH {
                keys.push(
                    b.submit(
                        vec![InboxOp {
                            rid: InboxRid {
                                node: 5,
                                incarnation: 2,
                                seq: i,
                            },
                            op: vec![2],
                        }],
                        1,
                    )
                    .await
                    .unwrap(),
                );
                tokio::task::yield_now().await;
            }
            keys
        };
        let (ka, kb) = tokio::join!(fa, fb);
        let mut all: Vec<u64> = ka.iter().chain(kb.iter()).map(|k| k.n).collect();
        all.sort_unstable();
        assert_eq!(
            all,
            (0..2 * EACH).collect::<Vec<_>>(),
            "contiguous, no slot shared"
        );
        let stored = s.get_run(1, 5, 0, 64).await.unwrap();
        assert_eq!(stored.len() as u64, 2 * EACH);
        for batch in stored {
            let want = if batch.incarnation == 1 {
                vec![1]
            } else {
                vec![2]
            };
            assert_eq!(batch.ops[0].op, want, "every batch is intact: {batch:?}");
        }
    }

    #[tokio::test]
    async fn poller_advances_per_requester_with_idle_backoff() {
        let s = mem();
        let mut p = InboxPoller::new(s.clone(), 1, 100, 1_000).with_width(2);
        assert_eq!(p.min_delay_ms(), None, "nobody tracked: nothing to poll");
        p.retain_only(&[5, 6]);
        assert_eq!(p.requesters(), vec![5, 6]);
        assert_eq!(p.delay_ms(5), Some(100));

        // Misses double the delay up to the ceiling.
        assert!(p.poll(5).await.unwrap().is_empty());
        assert_eq!(p.delay_ms(5), Some(200));
        for _ in 0..5 {
            p.poll(5).await.unwrap();
        }
        assert_eq!(p.delay_ms(5), Some(1_000));
        assert_eq!(
            p.delay_ms(6),
            Some(100),
            "requester 6 is not punished for 5's silence"
        );
        assert_eq!(p.min_delay_ms(), Some(100));
        assert_eq!(p.due(150), vec![6]);
        assert_eq!(p.due(1_000), vec![5, 6]);

        // A hit resets the backoff and advances the cursor past the run.
        for n in 0..3u64 {
            s.put_batch(&batch(1, 5, n, vec![op(5, n)])).await.unwrap();
        }
        let run = p.poll(5).await.unwrap();
        assert_eq!(run.iter().map(|b| b.n).collect::<Vec<_>>(), vec![0, 1]);
        assert!(p.saturated(run.len()), "a full run means poll again now");
        assert_eq!(p.delay_ms(5), Some(100));
        assert_eq!(p.cursor(5).unwrap().next_n, 2);
        let run = p.poll(5).await.unwrap();
        assert_eq!(run.iter().map(|b| b.n).collect::<Vec<_>>(), vec![2]);
        assert!(!p.saturated(run.len()));
        assert!(p.poll(5).await.unwrap().is_empty());
        assert_eq!(p.delay_ms(5), Some(200));

        // A rewind re-fetches from the batch the caller could not finish.
        p.rewind(5, 1);
        let run = p.poll(5).await.unwrap();
        assert_eq!(run.iter().map(|b| b.n).collect::<Vec<_>>(), vec![1, 2]);
        p.rewind(5, 99); // never forward
        assert_eq!(p.cursor(5).unwrap().next_n, 3);

        // Another epoch's batches are invisible to this poller.
        s.put_batch(&batch(2, 5, 0, vec![op(5, 7)])).await.unwrap();
        assert!(p.poll(5).await.unwrap().is_empty());

        // Untracked requesters are not polled, and re-tracking starts over.
        p.untrack(5);
        assert!(p.poll(5).await.unwrap().is_empty());
        p.track(5);
        assert_eq!(p.cursor(5).unwrap().next_n, 0);
    }

    #[test]
    fn gc_keeps_each_requesters_newest_consumed_batch() {
        let k = |epoch, node, n| InboxKey { epoch, node, n };
        let executed = [k(1, 5, 2), k(1, 5, 0), k(1, 6, 0), k(1, 5, 1), k(1, 5, 2)];
        assert_eq!(
            gc_keep_newest(&executed),
            vec![k(1, 5, 0), k(1, 5, 1)],
            "5's newest (2) and 6's only batch (0) stay as high-water marks"
        );
        assert!(gc_keep_newest(&[]).is_empty());
        assert!(gc_keep_newest(&[k(1, 5, 0)]).is_empty());
    }

    /// The restart hazard the GC rule exists for: a holder that consumed
    /// everything a requester wrote, a requester that restarts, and the
    /// requester's LIST-last must still land at the holder's cursor.
    #[tokio::test]
    async fn a_restarted_requester_resumes_at_the_holders_cursor_after_gc() {
        let s = mem();
        let mut sub = InboxSubmitter::fresh(s.clone(), 5, 1, 1);
        for i in 0..3u64 {
            sub.submit(vec![op(5, i)], 1).await.unwrap();
        }
        let mut p = InboxPoller::new(s.clone(), 1, 100, 1_000).with_width(8);
        p.track(5);
        let run = p.poll(5).await.unwrap();
        assert_eq!(run.len(), 3);
        assert_eq!(p.cursor(5).unwrap().next_n, 3);
        // Outcomes shipped (not modeled here): GC by the rule.
        let executed: Vec<InboxKey> = run.iter().map(|b| b.key()).collect();
        for key in gc_keep_newest(&executed) {
            p.delete(key).await.unwrap();
        }
        assert_eq!(
            s.list_all().await.unwrap(),
            vec![InboxKey {
                epoch: 1,
                node: 5,
                n: 2
            }],
            "the newest consumed batch stays"
        );
        // Restart: the new incarnation lists its prefix and continues at
        // exactly the holder's cursor, so the next batch is polled.
        let mut restarted = InboxSubmitter::resume(s.clone(), 5, 2, 1).await.unwrap();
        assert_eq!(restarted.next_n(), 3);
        restarted.submit(vec![op(5, 9)], 2).await.unwrap();
        let run = p.poll(5).await.unwrap();
        assert_eq!(run.iter().map(|b| b.n).collect::<Vec<_>>(), vec![3]);
    }

    #[test]
    fn backoff_schedule_matches_the_sync_loop() {
        assert_eq!(next_poll_ms(500, 0, 10_000), 500);
        assert_eq!(next_poll_ms(500, 1, 10_000), 1_000);
        assert_eq!(next_poll_ms(500, 4, 10_000), 8_000);
        assert_eq!(next_poll_ms(500, 5, 10_000), 10_000);
        assert_eq!(next_poll_ms(500, 99, 10_000), 10_000);
        assert_eq!(next_poll_ms(500, 0, 100), 500, "never below the interval");
        let mut b = PollBackoff::new(0, 0);
        assert_eq!(b.delay_ms(), 1, "degenerate config clamps to 1ms");
        b.miss();
        assert_eq!(b.delay_ms(), 1);
    }

    /// The hot window: a hit is followed by `grace` polls at `hot_ms`,
    /// then the ordinary doubling from the base interval, then cold.
    #[test]
    fn a_hit_opens_a_hot_window_before_the_ordinary_backoff() {
        let mut b = PollBackoff::two_tier(200, 2_000, 10_000).with_hot(20, 3);
        assert!(b.is_hot());
        assert_eq!(b.delay_ms(), 20);
        b.miss();
        b.miss();
        assert_eq!(b.delay_ms(), 20, "still inside the grace window");
        b.miss();
        assert!(!b.is_hot());
        assert_eq!(b.delay_ms(), 200, "grace over: the base interval");
        b.miss();
        assert_eq!(b.delay_ms(), 400);
        b.hit();
        assert_eq!(b.delay_ms(), 20);
        for _ in 0..(3 + COLD_AFTER_ROUNDS) {
            b.miss();
        }
        assert!(b.is_cold());
        assert_eq!(b.delay_ms(), 10_000);
        // Never hotter than the base interval allows.
        assert_eq!(PollBackoff::new(50, 1_000).with_hot(500, 1).delay_ms(), 50);
    }

    /// The two-tier schedule: warm ceiling for about a minute of misses,
    /// then the cold one; any hit makes the requester warm again.
    #[test]
    fn a_silent_requester_goes_cold_and_a_hit_warms_it() {
        let mut b = PollBackoff::two_tier(500, 2_000, 10_000);
        assert_eq!(b.delay_ms(), 500);
        b.miss();
        b.miss();
        assert_eq!(b.delay_ms(), 2_000, "two doublings reach the warm ceiling");
        for _ in 0..(COLD_AFTER_ROUNDS - 3) {
            b.miss();
            assert_eq!(b.delay_ms(), 2_000, "still warm");
        }
        assert!(!b.is_cold());
        b.miss();
        assert!(b.is_cold());
        assert_eq!(b.delay_ms(), 10_000, "cold: the idle-cluster ceiling");
        b.hit();
        assert!(!b.is_cold());
        assert_eq!(b.delay_ms(), 500);
        // A cold ceiling below the warm one is clamped up, never down.
        assert_eq!(PollBackoff::two_tier(100, 2_000, 1).delay_ms(), 100);
        let mut c = PollBackoff::two_tier(100, 2_000, 1);
        for _ in 0..COLD_AFTER_ROUNDS {
            c.miss();
        }
        assert_eq!(c.delay_ms(), 2_000);
    }

    #[tokio::test]
    async fn drain_lists_only_older_epochs_in_execution_order_and_gc_is_idempotent() {
        let s = mem();
        // Two requesters across three epochs, written out of order.
        for (e, node, n) in [
            (3u64, 5u64, 0u64),
            (1, 6, 1),
            (2, 5, 0),
            (1, 6, 0),
            (1, 5, 0),
            (2, 6, 0),
        ] {
            s.put_batch(&batch(e, node, n, vec![op(node, e * 10 + n)]))
                .await
                .unwrap();
        }
        let drained = s.drain_below(3).await.unwrap();
        assert_eq!(
            drained
                .iter()
                .map(|b| (b.epoch, b.node, b.n))
                .collect::<Vec<_>>(),
            vec![(1, 5, 0), (1, 6, 0), (1, 6, 1), (2, 5, 0), (2, 6, 0)],
            "everything below epoch 3, in (epoch, node, n) order"
        );
        assert!(s.drain_below(1).await.unwrap().is_empty());
        assert_eq!(s.requesters_in(1).await.unwrap(), vec![5, 6]);
        assert_eq!(s.requesters_in(3).await.unwrap(), vec![5]);
        assert!(s.requesters_in(4).await.unwrap().is_empty());
        assert_eq!(s.list_epoch(2).await.unwrap().len(), 2);
        assert_eq!(s.list_all().await.unwrap().len(), 6);

        // GC: unconditional, and a second delete of the same key is fine.
        for b in &drained {
            s.delete(b.key()).await.unwrap();
        }
        for b in &drained {
            s.delete(b.key()).await.unwrap();
        }
        let left = s.list_all().await.unwrap();
        assert_eq!(
            left,
            vec![InboxKey {
                epoch: 3,
                node: 5,
                n: 0
            }]
        );
        // A stray object under the prefix is neither listed as a batch nor
        // fatal to the listing.
        s.inner()
            .put(&Path::from("inbox/README"), PutPayload::from(b"x".to_vec()))
            .await
            .unwrap();
        assert_eq!(s.list_all().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn e2e_batches_are_ciphertext_and_roundtrip() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let keys = Arc::new(crate::e2e::E2eKeys::generate());
        let s = InboxStore::new_e2e(store.clone(), keys);
        assert!(s.is_e2e());
        let secret = b"secret filename".to_vec();
        let b = batch(
            1,
            5,
            0,
            vec![InboxOp {
                rid: rid(5, 0),
                op: secret.clone(),
            }],
        );
        s.put_batch(&b).await.unwrap();
        assert_eq!(s.get_batch(b.key()).await.unwrap(), Some(b.clone()));
        let raw = store
            .get(&b.key().path())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(!raw.windows(secret.len()).any(|w| w == secret.as_slice()));
        // The ambiguous-PUT comparison works through the seal too.
        assert_eq!(s.put_batch(&b).await.unwrap(), PutBatch::AlreadyOurs);
        // Another keyring cannot open it.
        let outsider = InboxStore::new_e2e(store, Arc::new(crate::e2e::E2eKeys::generate()));
        assert!(outsider.get_batch(b.key()).await.is_err());
    }
}
