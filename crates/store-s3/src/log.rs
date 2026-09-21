//! Metadata log segments and checkpoints in S3 (DESIGN.md §4).
//!
//! Each partition has its own ordered stream:
//! - Segments `log/<part>/<seq:016x>.zst` hold a zstd postcard envelope of
//!   log records, written with conditional create (CAS): a sequence number
//!   can never be silently overwritten.
//! - A whole-DB checkpoint lives under `checkpoints/p0/` (as in phase 1)
//!   plus a `checkpoints/VECTOR.json` sidecar recording the applied_seq
//!   of every partition the snapshot covers. Bootstrap = snapshot +
//!   per-partition replay from the vector.
//! - A child stream that has been merged is sealed by a `sealed` marker
//!   object in the child's log prefix; tailers treat sealed+fully-applied
//!   as removable from the active set.
//!
//! Segments are small and are moved with one request each. A checkpoint
//! is not: it is the whole metadata DB, hundreds of MiB at a large
//! namespace, and a single GET of it is limited by one TCP stream's
//! congestion window rather than by the link. Measured for 64 MiB on
//! 2026-09-10 (plan 26's appendix): 23.5 s single-GET against 6.3 s with
//! parallel ranged GETs from Hungary to us-west-2, 6.6 vs 2.7 s against
//! OVH Milan, 1.3 vs 0.30 s from an instance in the bucket's own region.
//! That 3-4x is free — no format change, no extra request class — so
//! checkpoint bodies move through [`get_object_parallel`] on the way in
//! and, once they are large enough for it to matter, through a multipart
//! upload on the way out.

use crate::e2e::{decrypt_object, encrypt_object, SharedE2eKeys};
use crate::error::StoreError;
use crate::layout;
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Genesis partition id. A filesystem always has at least this one.
pub const PARTITION: &str = "p0";
const ZSTD_LEVEL: i32 = 3;

/// Granularity of a parallel checkpoint transfer, in both directions: the
/// ranged GET asks for this much per request and the multipart PUT sends
/// this much per part. 8 MiB is comfortably above S3's 5 MiB minimum part
/// size and large enough that per-request overhead is noise next to the
/// transfer itself, while still leaving a 64 MiB image split into 8 pieces
/// — enough parallelism to fill a fat, far link.
const CHECKPOINT_RANGE_BYTES: u64 = 8 << 20;

/// Bodies larger than this go up as a multipart upload; smaller ones as a
/// single PUT. Below a couple of parts the extra create/complete round
/// trips cost more than the concurrency buys, and the local-file backend
/// pays them for nothing.
const CHECKPOINT_MULTIPART_MIN_BYTES: usize = 16 << 20;

/// Default in-flight ranges (GET) / parts (PUT) for a checkpoint transfer.
const CHECKPOINT_IO_CONCURRENCY: usize = 8;

/// Env `CONSTELLATION_CHECKPOINT_IO_CONCURRENCY`: how many 8 MiB ranges or
/// parts of a checkpoint are in flight at once. More is not reliably
/// better — measured against AWS from Hungary, 64 MiB in 1 MiB pieces went
/// *slower* at concurrency 64 than at 16 (5.1 s vs 3.4 s) — so the default
/// stays modest and this exists for paths whose bandwidth-delay product
/// differs enough to want tuning. `0` or unparseable falls back.
fn checkpoint_io_concurrency() -> usize {
    std::env::var("CONSTELLATION_CHECKPOINT_IO_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&v| v > 0)
        .unwrap_or(CHECKPOINT_IO_CONCURRENCY)
}

/// Download one object as a whole `Vec<u8>`, using concurrent ranged GETs
/// once it is big enough for them to pay off.
///
/// A HEAD first, because the range plan needs the size and a HEAD is the
/// cheapest way to learn it (176 ms HU→AWS, 13 ms same-region — the same
/// round trip a GET would have spent anyway). Objects that fit in one
/// range skip the fan-out and take a plain GET, so the small-checkpoint
/// case costs exactly one extra HEAD over the old path.
///
/// The ranges are issued here rather than handed to `ObjectStore::get_ranges`
/// deliberately: that helper coalesces ranges closer together than
/// `OBJECT_STORE_COALESCE_DEFAULT` (1 MiB) into one request, and a
/// contiguous split of a single object is *zero* bytes apart, so every
/// piece would merge straight back into the one big GET this exists to
/// avoid. It also fixes its own parallelism at 10, ignoring the knob.
///
/// The pieces are assembled in issue order into one buffer. Nothing may
/// consume them incrementally: under E2E the AEAD seal covers the entire
/// body, so it has to be whole before it can be opened at all.
async fn get_object_parallel(
    store: &Arc<dyn ObjectStore>,
    key: &object_store::path::Path,
    range_bytes: u64,
    concurrency: usize,
) -> Result<Vec<u8>, object_store::Error> {
    use futures::StreamExt;
    let size = store.head(key).await?.size;
    if size <= range_bytes {
        return Ok(store.get(key).await?.bytes().await?.to_vec());
    }
    let ranges: Vec<std::ops::Range<u64>> = (0..size)
        .step_by(range_bytes as usize)
        .map(|start| start..(start + range_bytes).min(size))
        .collect();
    let mut body = Vec::with_capacity(size as usize);
    let mut pieces = futures::stream::iter(ranges.into_iter().map(|r| store.get_range(key, r)))
        .buffered(concurrency.max(1));
    while let Some(piece) = pieces.next().await {
        body.extend_from_slice(&piece?);
    }
    Ok(body)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointRef {
    pub seq: u64,
    /// Uncompressed size of the snapshot this checkpoint covers. A
    /// restarting node reads it to seed the proportional-cadence baseline
    /// (`last_ckpt_bytes`) so the first post-restart checkpoint fires on
    /// bytes shipped rather than on the segment count alone.
    pub bytes: u64,
}

/// Applied-seq vector covering a whole-DB checkpoint (plan 01 / M3.2).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CheckpointVector {
    /// Highest sequence applied per partition at the moment the
    /// snapshot was taken.
    pub applied: BTreeMap<String, u64>,
}

fn latest_key() -> object_store::path::Path {
    object_store::path::Path::from(format!("checkpoints/{PARTITION}/LATEST"))
}

fn vector_key() -> object_store::path::Path {
    object_store::path::Path::from("checkpoints/VECTOR.json")
}

fn sealed_key(partition: &str) -> object_store::path::Path {
    object_store::path::Path::from(format!("log/{partition}/sealed"))
}

/// Metadata log I/O for one partition.
pub struct LogStore {
    store: Arc<dyn ObjectStore>,
    partition: String,
    e2e: Option<SharedE2eKeys>,
}

impl LogStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self::for_partition(store, PARTITION)
    }

    pub fn for_partition(store: Arc<dyn ObjectStore>, partition: &str) -> Self {
        Self {
            store,
            partition: partition.to_string(),
            e2e: None,
        }
    }

    pub fn new_e2e(store: Arc<dyn ObjectStore>, keys: SharedE2eKeys) -> Self {
        Self {
            store,
            partition: PARTITION.to_string(),
            e2e: Some(keys),
        }
    }

    pub fn partition(&self) -> &str {
        &self.partition
    }

    pub fn with_partition(&self, partition: &str) -> Self {
        Self {
            store: self.store.clone(),
            partition: partition.to_string(),
            e2e: self.e2e.clone(),
        }
    }

    pub fn inner(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    /// Whether this stream seals segments under an E2E partition DEK.
    pub fn is_e2e(&self) -> bool {
        self.e2e.is_some()
    }

    /// The filesystem's E2E keyring, when it has one. Plan 28's commit
    /// readers need the addressing key to hash and shape the tree the
    /// way its writers did (§P13).
    pub fn e2e_keys(&self) -> Option<&SharedE2eKeys> {
        self.e2e.as_ref()
    }

    /// Encode a plaintext segment body to its at-rest / on-wire form:
    /// zstd, then (E2E) AEAD-seal under the partition DEK with the S3
    /// object path as AAD. [`put_segment`] and the gossip fast path share
    /// this, so a pushed E2E segment is byte-identical to what a peer would
    /// GET from S3 — and topic membership alone (the `gossip_secret` in
    /// `meta.json`) cannot read it without the passphrase-derived DEK.
    pub fn seal_segment(&self, seq: u64, payload: &[u8]) -> Result<Vec<u8>, StoreError> {
        let key = layout::log_segment(&self.partition, seq);
        let compressed = zstd::encode_all(payload, ZSTD_LEVEL)?;
        match &self.e2e {
            Some(keys) => Ok(encrypt_object(
                &keys.dek(&self.partition),
                key.as_ref().as_bytes(),
                &compressed,
            )?),
            None => Ok(compressed),
        }
    }

    /// Inverse of [`seal_segment`]: (E2E) AEAD-open under the partition
    /// DEK, then zstd-decompress. The caller must already hold the
    /// partition key; a missing DEK surfaces as an error so a gossip
    /// receiver can fall back to the ordinary S3 tailer (which refreshes
    /// the key first).
    pub fn open_segment(&self, seq: u64, body: &[u8]) -> Result<Vec<u8>, StoreError> {
        let compressed = match &self.e2e {
            Some(keys) => decrypt_object(
                &keys.dek(&self.partition),
                layout::log_segment(&self.partition, seq)
                    .as_ref()
                    .as_bytes(),
                body,
            )?,
            None => body.to_vec(),
        };
        Ok(zstd::decode_all(&compressed[..])?)
    }

    /// CAS-create segment `seq`. `AlreadyExists` when the sequence was
    /// already written (crash replay or a second writer).
    pub async fn put_segment(&self, seq: u64, payload: &[u8]) -> Result<(), StoreError> {
        let key = layout::log_segment(&self.partition, seq);
        let body = self.seal_segment(seq, payload)?;
        match self
            .store
            .put_opts(
                &key,
                PutPayload::from(body),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => Err(StoreError::AlreadyExists),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_segment(&self, seq: u64) -> Result<Vec<u8>, StoreError> {
        let res = self
            .store
            .get(&layout::log_segment(&self.partition, seq))
            .await?;
        let body = res.bytes().await?;
        self.open_segment(seq, &body)
    }

    /// Fetch segments `from, from+1, …` concurrently (`k` in flight) and
    /// return the longest contiguous run present, in order. A `NotFound`
    /// at `from + i` ends the run at `i`; anything fetched beyond the
    /// first gap is discarded (it is unreachable until the gap fills, and
    /// a gap in a CAS-created stream fills only from the sole appender).
    ///
    /// This is the steady-state alternative to listing a stream that
    /// usually has nothing new: measured against AWS S3, a GET-404 is no
    /// slower than an empty LIST (177 vs 175 ms from Europe, 34 vs 34 ms
    /// against OVH Milan) and costs ~1/12.5 of a LIST request. A LIST page
    /// still wins when genuinely far behind — 1000 keys in one round trip
    /// — so the tailer keeps it for catch-up and uses this for the poll.
    pub async fn get_run(&self, from: u64, k: usize) -> Result<Vec<(u64, Vec<u8>)>, StoreError> {
        use futures::StreamExt;
        if k == 0 {
            return Ok(Vec::new());
        }
        let store = &self.store;
        let partition = self.partition.as_str();
        let mut fetched = futures::stream::iter((0..k as u64).map(|i| {
            let seq = from.saturating_add(i);
            async move {
                let key = layout::log_segment(partition, seq);
                match store.get(&key).await {
                    Ok(res) => (seq, res.bytes().await),
                    Err(e) => (seq, Err(e)),
                }
            }
        }))
        .buffered(k);
        let mut run = Vec::new();
        // `buffered` yields in issue order, so the first miss is the end of
        // the run and every later reply is simply never consumed.
        while let Some((seq, body)) = fetched.next().await {
            match body {
                Ok(body) => run.push((seq, self.open_segment(seq, &body)?)),
                Err(object_store::Error::NotFound { .. }) => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(run)
    }

    /// All segment sequence numbers, ascending.
    pub async fn list_segments(&self) -> Result<Vec<u64>, StoreError> {
        self.list_segments_from(1).await
    }

    /// Segment sequence numbers `>= from`, ascending. Keys are
    /// zero-padded hex, so lexicographic offset listing is numeric.
    pub async fn list_segments_from(&self, from: u64) -> Result<Vec<u64>, StoreError> {
        let prefix = layout::log_prefix(&self.partition);
        let offset = layout::log_segment(&self.partition, from.saturating_sub(1));
        let mut seqs: Vec<u64> = self
            .store
            .list_with_offset(Some(&prefix), &offset)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .filter_map(|m| {
                let name = m.location.filename()?.strip_suffix(".zst")?.to_string();
                u64::from_str_radix(&name, 16).ok()
            })
            .filter(|&s| s >= from)
            .collect();
        seqs.sort_unstable();
        Ok(seqs)
    }

    /// Mark this partition's stream sealed (after a merge into a parent).
    pub async fn seal(&self) -> Result<(), StoreError> {
        self.store
            .put(
                &sealed_key(&self.partition),
                PutPayload::from(b"1".to_vec()),
            )
            .await?;
        Ok(())
    }

    pub async fn is_sealed(&self) -> Result<bool, StoreError> {
        match self.store.head(&sealed_key(&self.partition)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Store a checkpoint snapshot covering the log up to `seq`, then
    /// move the LATEST pointer. Also writes `checkpoints/VECTOR.json`
    /// so a bootstrap knows every partition's applied_seq.
    pub async fn put_checkpoint(&self, seq: u64, snapshot: &[u8]) -> Result<(), StoreError> {
        self.put_checkpoint_with_vector(seq, snapshot, &CheckpointVector::default())
            .await
    }

    pub async fn put_checkpoint_with_vector(
        &self,
        seq: u64,
        snapshot: &[u8],
        vector: &CheckpointVector,
    ) -> Result<(), StoreError> {
        let key = layout::checkpoint(PARTITION, seq);
        let compressed = zstd::encode_all(snapshot, ZSTD_LEVEL)?;
        let body = match &self.e2e {
            Some(keys) => {
                encrypt_object(&keys.dek(PARTITION), key.as_ref().as_bytes(), &compressed)?
            }
            None => compressed,
        };
        self.put_checkpoint_body(&key, body).await?;
        let ptr = serde_json::to_vec(&CheckpointRef {
            seq,
            bytes: snapshot.len() as u64,
        })?;
        self.store.put(&latest_key(), PutPayload::from(ptr)).await?;
        let vec_body = serde_json::to_vec(vector)?;
        self.store
            .put(&vector_key(), PutPayload::from(vec_body))
            .await?;
        // Both pointers now name this checkpoint, so every older one is
        // superseded. Prune inline (newest-2 kept) instead of leaning on
        // the daily GC timer — bulk ingest would otherwise pile up
        // hundreds of dead whole-DB copies between GC runs. Best effort: a
        // failed delete is logged and left for GC, and never fails the put.
        if let Err(e) = self.prune_superseded_checkpoints(seq).await {
            tracing::warn!(error = %e, "inline checkpoint prune failed; GC will catch up");
        }
        Ok(())
    }

    /// Write the snapshot object itself. A large body goes up as a
    /// multipart upload so the parts fly concurrently — the upload side of
    /// the same asymmetry the ranged download exploits — while a small one
    /// keeps the single PUT and its one round trip.
    ///
    /// In-flight parts are capped at [`checkpoint_io_concurrency`].
    /// `WriteMultipart` otherwise starts every part the moment its chunk is
    /// buffered, which for a 400 MiB checkpoint would be fifty concurrent
    /// uploads: past a modest depth extra concurrency measured *worse*, not
    /// better, on the far path.
    async fn put_checkpoint_body(
        &self,
        key: &object_store::path::Path,
        body: Vec<u8>,
    ) -> Result<(), StoreError> {
        if body.len() <= CHECKPOINT_MULTIPART_MIN_BYTES {
            self.store.put(key, PutPayload::from(body)).await?;
            return Ok(());
        }
        let concurrency = checkpoint_io_concurrency();
        let mut upload = object_store::WriteMultipart::new_with_chunk_size(
            self.store.put_multipart(key).await?,
            CHECKPOINT_RANGE_BYTES as usize,
        );
        let mut rest = bytes::Bytes::from(body);
        while !rest.is_empty() {
            let chunk = rest.split_to(rest.len().min(CHECKPOINT_RANGE_BYTES as usize));
            upload.wait_for_capacity(concurrency).await?;
            upload.put(chunk);
        }
        upload.finish().await?;
        Ok(())
    }

    /// Delete every checkpoint snapshot under `checkpoints/p0/` except the
    /// newest two, never touching `keep_seq` (the just-published one, which
    /// is always among the newest two — the guard is defensive). Deletes
    /// run with bounded concurrency; a `NotFound` is ignored (a concurrent
    /// prune or GC already removed it). The `LATEST`/`VECTOR.json` pointers
    /// have no `.zst` suffix and are never candidates.
    ///
    /// Newest-2 rather than newest-1 covers a bootstrap that read `LATEST`
    /// a moment before it moved: the checkpoint it is about to GET is the
    /// previous one, and it must still be present.
    async fn prune_superseded_checkpoints(&self, keep_seq: u64) -> Result<(), StoreError> {
        use futures::StreamExt;
        let prefix = object_store::path::Path::from(format!("checkpoints/{PARTITION}"));
        let mut seqs: Vec<u64> = self
            .store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .filter_map(|m| {
                let name = m.location.filename()?.strip_suffix(".zst")?.to_string();
                u64::from_str_radix(&name, 16).ok()
            })
            .collect();
        seqs.sort_unstable();
        let remove = seqs.len().saturating_sub(2);
        let victims: Vec<u64> = seqs
            .into_iter()
            .take(remove)
            .filter(|seq| *seq != keep_seq)
            .collect();
        let results = futures::stream::iter(victims.into_iter().map(|seq| {
            let store = self.store.clone();
            async move {
                match store.delete(&layout::checkpoint(PARTITION, seq)).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(e) => Err(StoreError::from(e)),
                }
            }
        }))
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await;
        for result in results {
            result?;
        }
        Ok(())
    }

    /// Read the `LATEST` pointer only (no snapshot GET). `NotFound → None`.
    /// Used to seed the checkpoint-cadence baseline across restarts and by
    /// [`get_latest_checkpoint`] itself.
    pub async fn get_checkpoint_ref(&self) -> Result<Option<CheckpointRef>, StoreError> {
        match self.store.get(&latest_key()).await {
            Ok(r) => Ok(Some(serde_json::from_slice(&r.bytes().await?)?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Latest checkpoint `(covered_seq, snapshot)` if any exists.
    pub async fn get_latest_checkpoint(&self) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        let Some(mut r) = self.get_checkpoint_ref().await? else {
            return Ok(None);
        };
        let range_bytes = CHECKPOINT_RANGE_BYTES;
        let concurrency = checkpoint_io_concurrency();
        let fetch = |seq: u64| async move {
            get_object_parallel(
                &self.store,
                &layout::checkpoint(PARTITION, seq),
                range_bytes,
                concurrency,
            )
            .await
        };
        let body = match fetch(r.seq).await {
            Ok(body) => body,
            // The named snapshot vanished between our pointer read and the
            // GET: two rapid checkpoints raced our bootstrap and the inline
            // prune removed the one we read. `LATEST` is written before the
            // prune, so re-reading it names a present object; retry once.
            Err(object_store::Error::NotFound { .. }) => {
                let Some(fresh) = self.get_checkpoint_ref().await? else {
                    return Ok(None);
                };
                r = fresh;
                fetch(r.seq).await?
            }
            Err(e) => return Err(e.into()),
        };
        let compressed = match &self.e2e {
            Some(keys) => decrypt_object(
                &keys.dek(PARTITION),
                layout::checkpoint(PARTITION, r.seq).as_ref().as_bytes(),
                &body,
            )?,
            None => body.to_vec(),
        };
        Ok(Some((r.seq, zstd::decode_all(&compressed[..])?)))
    }

    pub async fn get_checkpoint_vector(&self) -> Result<CheckpointVector, StoreError> {
        match self.store.get(&vector_key()).await {
            Ok(r) => Ok(serde_json::from_slice(&r.bytes().await?)?),
            Err(object_store::Error::NotFound { .. }) => Ok(CheckpointVector::default()),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path as OPath;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutResult,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn ls() -> LogStore {
        LogStore::new(Arc::new(InMemory::new()))
    }

    #[tokio::test]
    async fn segment_roundtrip_and_cas() {
        let s = ls();
        s.put_segment(1, b"first").await.unwrap();
        s.put_segment(2, b"second").await.unwrap();
        assert_eq!(s.get_segment(1).await.unwrap(), b"first");
        assert_eq!(s.list_segments().await.unwrap(), vec![1, 2]);
        // A sequence number can never be overwritten.
        assert!(matches!(
            s.put_segment(1, b"evil").await,
            Err(StoreError::AlreadyExists)
        ));
        assert_eq!(s.get_segment(1).await.unwrap(), b"first");
    }

    #[tokio::test]
    async fn get_run_stops_at_the_first_gap() {
        let s = ls();
        for seq in [1u64, 2, 3, 5] {
            s.put_segment(seq, format!("seg-{seq}").as_bytes())
                .await
                .unwrap();
        }
        // 4 is missing. 5 exists and is fetched by the same fan-out, but it
        // is unreachable until 4 fills, so it must not be returned: applying
        // it would break the contiguity the replay depends on.
        let run = s.get_run(1, 8).await.unwrap();
        assert_eq!(
            run.iter().map(|(seq, _)| *seq).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(run[0].1, b"seg-1");
        assert_eq!(run[2].1, b"seg-3");

        // A miss at `from` is the steady-state case: no run at all, one
        // request class cheaper than the LIST it replaces.
        assert!(s.get_run(4, 8).await.unwrap().is_empty());
        assert!(s.get_run(99, 8).await.unwrap().is_empty());

        // k wider than the run present, and k narrower than it.
        assert_eq!(s.get_run(2, 64).await.unwrap().len(), 2);
        assert_eq!(
            s.get_run(1, 2)
                .await
                .unwrap()
                .iter()
                .map(|(seq, _)| *seq)
                .collect::<Vec<_>>(),
            vec![1, 2],
            "a saturated run is what tells the tailer it may be far behind"
        );
        assert!(s.get_run(1, 0).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn get_run_opens_e2e_segments() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let keys = Arc::new(crate::e2e::E2eKeys::generate());
        let logs = LogStore::new_e2e(store, keys);
        logs.put_segment(1, b"sealed one").await.unwrap();
        logs.put_segment(2, b"sealed two").await.unwrap();
        let run = logs.get_run(1, 4).await.unwrap();
        assert_eq!(run.len(), 2);
        assert_eq!(run[1].1, b"sealed two");
    }

    #[tokio::test]
    async fn per_partition_streams_are_independent() {
        let store = Arc::new(InMemory::new());
        let a = LogStore::for_partition(store.clone(), "p0");
        let b = LogStore::for_partition(store, "p1");
        a.put_segment(1, b"a").await.unwrap();
        b.put_segment(1, b"b").await.unwrap();
        assert_eq!(a.get_segment(1).await.unwrap(), b"a");
        assert_eq!(b.get_segment(1).await.unwrap(), b"b");
        b.seal().await.unwrap();
        assert!(b.is_sealed().await.unwrap());
        assert!(!a.is_sealed().await.unwrap());
    }

    #[tokio::test]
    async fn checkpoint_roundtrip() {
        let s = ls();
        assert!(s.get_latest_checkpoint().await.unwrap().is_none());
        s.put_checkpoint(7, b"snap-7").await.unwrap();
        let (seq, snap) = s.get_latest_checkpoint().await.unwrap().unwrap();
        assert_eq!((seq, snap.as_slice()), (7, b"snap-7".as_slice()));
        // Newer checkpoint replaces the pointer.
        s.put_checkpoint(9, b"snap-9").await.unwrap();
        let (seq, snap) = s.get_latest_checkpoint().await.unwrap().unwrap();
        assert_eq!((seq, snap.as_slice()), (9, b"snap-9".as_slice()));
    }

    #[tokio::test]
    async fn checkpoint_vector_roundtrip() {
        let s = ls();
        let mut v = CheckpointVector::default();
        v.applied.insert("p0".into(), 3);
        v.applied.insert("p1".into(), 1);
        s.put_checkpoint_with_vector(3, b"snap", &v).await.unwrap();
        let got = s.get_checkpoint_vector().await.unwrap();
        assert_eq!(got.applied.get("p1").copied(), Some(1));
    }

    #[tokio::test]
    async fn e2e_segments_and_checkpoints_are_ciphertext() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let keys = Arc::new(crate::e2e::E2eKeys::generate());
        let logs = LogStore::new_e2e(store.clone(), keys);
        logs.put_segment(1, b"secret filename").await.unwrap();
        logs.put_checkpoint(1, b"checkpoint filename")
            .await
            .unwrap();
        assert_eq!(logs.get_segment(1).await.unwrap(), b"secret filename");
        assert_eq!(
            logs.get_latest_checkpoint().await.unwrap().unwrap().1,
            b"checkpoint filename"
        );
        let raw_log = store
            .get(&layout::log_segment(PARTITION, 1))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(!raw_log
            .windows(b"secret filename".len())
            .any(|window| window == b"secret filename"));
    }

    #[tokio::test]
    async fn e2e_sealed_push_matches_s3_and_hides_plaintext() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let keys = Arc::new(crate::e2e::E2eKeys::generate());
        let logs = LogStore::new_e2e(store.clone(), keys);
        let plaintext = b"secret filename in a gossip push";
        logs.put_segment(1, plaintext).await.unwrap();

        // The gossip fast path pushes the segment sealed under the same DEK
        // and AAD as the S3 object (a fresh AEAD nonce makes the bytes
        // differ, but both open with the partition key), so a topic member
        // without the DEK sees only ciphertext.
        let sealed = logs.seal_segment(1, plaintext).unwrap();
        assert!(!sealed
            .windows(plaintext.len())
            .any(|w| w == plaintext.as_slice()));

        // A holder of the DEK opens the push — and the S3 object — back to
        // plaintext.
        assert_eq!(logs.open_segment(1, &sealed).unwrap(), plaintext);
        assert_eq!(logs.get_segment(1).await.unwrap(), plaintext);

        // A member of the topic without the passphrase (no partition DEK)
        // cannot open the pushed body.
        let other_keys = Arc::new(crate::e2e::E2eKeys::generate());
        let outsider = LogStore::new_e2e(store.clone(), other_keys);
        assert!(outsider.open_segment(1, &sealed).is_err());
    }

    #[tokio::test]
    async fn non_e2e_push_is_plaintext_encoded() {
        // Non-E2E behaviour is unchanged: no sealing, plain zstd only, and
        // the sealed form round-trips through open.
        let s = ls();
        let payload = b"plain segment";
        let sealed = s.seal_segment(1, payload).unwrap();
        assert_eq!(s.open_segment(1, &sealed).unwrap(), payload);
        assert!(!s.is_e2e());
    }

    #[tokio::test]
    async fn checkpoint_ref_roundtrips_with_bytes() {
        // Plain serde round-trip: `bytes` is a required field.
        let r = CheckpointRef {
            seq: 42,
            bytes: 4096,
        };
        let back: CheckpointRef = serde_json::from_slice(&serde_json::to_vec(&r).unwrap()).unwrap();
        assert_eq!((back.seq, back.bytes), (42, 4096));

        // And through S3: LATEST records the uncompressed snapshot size,
        // which `get_checkpoint_ref` reads back for the cadence baseline.
        let s = ls();
        assert!(s.get_checkpoint_ref().await.unwrap().is_none());
        s.put_checkpoint(7, b"snapshot-body").await.unwrap();
        let got = s.get_checkpoint_ref().await.unwrap().unwrap();
        assert_eq!((got.seq, got.bytes), (7, b"snapshot-body".len() as u64));
    }

    async fn checkpoint_snapshot_count(store: &Arc<dyn ObjectStore>) -> usize {
        store
            .list(Some(&OPath::from(format!("checkpoints/{PARTITION}"))))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .filter(|m| m.location.filename().is_some_and(|n| n.ends_with(".zst")))
            .count()
    }

    #[tokio::test]
    async fn inline_prune_keeps_only_newest_two_checkpoints() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let s = LogStore::new(store.clone());
        for seq in [1u64, 2, 3, 4, 5] {
            s.put_checkpoint(seq, format!("snap-{seq}").as_bytes())
                .await
                .unwrap();
        }
        assert_eq!(
            checkpoint_snapshot_count(&store).await,
            2,
            "only the newest two snapshots survive the inline prune"
        );
        let (seq, snap) = s.get_latest_checkpoint().await.unwrap().unwrap();
        assert_eq!((seq, snap.as_slice()), (5, b"snap-5".as_slice()));
    }

    /// A decorator whose `delete` always fails. Everything else delegates.
    #[derive(Debug)]
    struct DeleteFailingStore {
        inner: InMemory,
    }

    impl std::fmt::Display for DeleteFailingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "DeleteFailingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for DeleteFailingStore {
        async fn put_opts(
            &self,
            location: &OPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &OPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &OPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<OPath>>,
        ) -> BoxStream<'static, object_store::Result<OPath>> {
            // `delete` (an `ObjectStoreExt` method) routes through here, so
            // yielding an error per location fails every prune delete.
            use futures::StreamExt;
            locations
                .map(|_| {
                    Err(object_store::Error::Generic {
                        store: "DeleteFailingStore",
                        source: "delete disabled (test injection)".into(),
                    })
                })
                .boxed()
        }
        fn list(
            &self,
            prefix: Option<&OPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&OPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &OPath,
            to: &OPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn inline_prune_failure_does_not_fail_the_put() {
        let store: Arc<dyn ObjectStore> = Arc::new(DeleteFailingStore {
            inner: InMemory::new(),
        });
        let s = LogStore::new(store.clone());
        // Every prune delete fails, but the put path swallows it: the
        // checkpoint is durable and LATEST resolves.
        for seq in [1u64, 2, 3] {
            s.put_checkpoint(seq, format!("snap-{seq}").as_bytes())
                .await
                .unwrap();
        }
        let (seq, snap) = s.get_latest_checkpoint().await.unwrap().unwrap();
        assert_eq!((seq, snap.as_slice()), (3, b"snap-3".as_slice()));
    }

    /// A decorator that, on the first GET of a checkpoint snapshot object,
    /// deletes that object and repoints LATEST at `recover_seq` — modelling
    /// two rapid checkpoints (and their inline prune) racing a bootstrap
    /// between its pointer read and its snapshot GET.
    #[derive(Debug)]
    struct GetRacingStore {
        inner: InMemory,
        raced: AtomicBool,
        recover_seq: u64,
    }

    impl std::fmt::Display for GetRacingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "GetRacingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for GetRacingStore {
        async fn put_opts(
            &self,
            location: &OPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &OPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &OPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let is_snapshot = location
                .as_ref()
                .starts_with(&format!("checkpoints/{PARTITION}/"))
                && location.filename().is_some_and(|n| n.ends_with(".zst"));
            if is_snapshot && !self.raced.swap(true, Ordering::SeqCst) {
                let _ = self.inner.delete(location).await;
                let ptr = serde_json::to_vec(&CheckpointRef {
                    seq: self.recover_seq,
                    bytes: 0,
                })
                .unwrap();
                self.inner
                    .put(&latest_key(), PutPayload::from(ptr))
                    .await
                    .unwrap();
            }
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<OPath>>,
        ) -> BoxStream<'static, object_store::Result<OPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&OPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&OPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &OPath,
            to: &OPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Counts checkpoint-snapshot reads by shape: HEAD (`with_head`),
    /// ranged GET (`with_range`) and whole-object GET are all one
    /// `get_opts` call at this layer, and the point of the assertions below
    /// is exactly which of the three a given body size produces.
    #[derive(Debug, Default)]
    struct ShapeCountingStore {
        inner: InMemory,
        heads: AtomicUsize,
        ranged: AtomicUsize,
        whole: AtomicUsize,
        multiparts: AtomicUsize,
    }

    impl std::fmt::Display for ShapeCountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ShapeCountingStore")
        }
    }

    impl ShapeCountingStore {
        fn counts(&self) -> (usize, usize, usize) {
            (
                self.heads.load(Ordering::SeqCst),
                self.ranged.load(Ordering::SeqCst),
                self.whole.load(Ordering::SeqCst),
            )
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for ShapeCountingStore {
        async fn put_opts(
            &self,
            location: &OPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &OPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.multiparts.fetch_add(1, Ordering::SeqCst);
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &OPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if location.filename().is_some_and(|n| n.ends_with(".zst")) {
                if options.head {
                    self.heads.fetch_add(1, Ordering::SeqCst);
                } else if options.range.is_some() {
                    self.ranged.fetch_add(1, Ordering::SeqCst);
                } else {
                    self.whole.fetch_add(1, Ordering::SeqCst);
                }
            }
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<OPath>>,
        ) -> BoxStream<'static, object_store::Result<OPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&OPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&OPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &OPath,
            to: &OPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Deterministic incompressible filler: a checkpoint of zeros would
    /// zstd away to nothing and never reach either the multipart or the
    /// ranged path, so the size assertions have to survive compression.
    fn pseudo_random(len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        let mut state = 0x2545_f491_4f6c_dd1du64;
        while out.len() < len {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            out.extend_from_slice(&state.to_le_bytes());
        }
        out.truncate(len);
        out
    }

    /// A checkpoint too large for one PUT and one GET must survive the
    /// round trip byte for byte through the multipart upload and the
    /// ranged download — including under E2E, where the AEAD seal covers
    /// the whole body and a single misordered or short range would surface
    /// as an open failure rather than as wrong bytes.
    #[tokio::test]
    async fn large_checkpoint_roundtrips_through_parallel_io() {
        let body = pseudo_random(40 << 20);
        for e2e in [false, true] {
            let counting = Arc::new(ShapeCountingStore::default());
            let store = counting.clone() as Arc<dyn ObjectStore>;
            let s = match e2e {
                false => LogStore::new(store.clone()),
                true => LogStore::new_e2e(store.clone(), Arc::new(crate::e2e::E2eKeys::generate())),
            };
            s.put_checkpoint(11, &body).await.unwrap();
            let (seq, got) = s.get_latest_checkpoint().await.unwrap().unwrap();
            assert_eq!(seq, 11);
            assert_eq!(got.len(), body.len(), "e2e={e2e}");
            assert!(got == body, "e2e={e2e}: checkpoint body changed in transit");

            let (heads, ranged, whole) = counting.counts();
            assert_eq!(
                (heads, whole),
                (1, 0),
                "e2e={e2e}: one HEAD for the size, and no whole-object GET"
            );
            assert!(
                ranged >= 5,
                "e2e={e2e}: a 40 MiB body must split into 8 MiB ranges, got {ranged}"
            );
            assert_eq!(
                counting.multiparts.load(Ordering::SeqCst),
                1,
                "e2e={e2e}: and it must have gone up as a multipart upload"
            );
        }
    }

    /// The common case is a small checkpoint, and it must not pay for the
    /// machinery the large one needs: one GET, as before, plus the single
    /// HEAD that decides between the two paths (accepted deliberately — it
    /// is one round trip on a path that is already doing several, and
    /// guessing the size wrong is what costs 3-4x on a big one).
    #[tokio::test]
    async fn small_checkpoint_takes_the_single_get_path() {
        let counting = Arc::new(ShapeCountingStore::default());
        let s = LogStore::new(counting.clone() as Arc<dyn ObjectStore>);
        let body = pseudo_random(1024);
        s.put_checkpoint(3, &body).await.unwrap();
        assert_eq!(s.get_latest_checkpoint().await.unwrap().unwrap().1, body);

        assert_eq!(
            counting.counts(),
            (1, 0, 1),
            "a 1 KiB checkpoint must be one HEAD and one whole-object GET"
        );
        assert_eq!(
            counting.multiparts.load(Ordering::SeqCst),
            0,
            "and a single PUT, not a multipart upload"
        );
    }

    #[tokio::test]
    async fn get_latest_checkpoint_recovers_when_named_object_vanishes() {
        let store: Arc<dyn ObjectStore> = Arc::new(GetRacingStore {
            inner: InMemory::new(),
            raced: AtomicBool::new(false),
            recover_seq: 5,
        });
        let s = LogStore::new(store.clone());
        // Two checkpoints; the inline prune keeps both (newest 2). LATEST
        // names 9. The get hook then deletes 9 mid-flight and repoints
        // LATEST at 5, which is still present.
        s.put_checkpoint(5, b"snap-5").await.unwrap();
        s.put_checkpoint(9, b"snap-9").await.unwrap();
        let (seq, snap) = s.get_latest_checkpoint().await.unwrap().unwrap();
        assert_eq!(
            (seq, snap.as_slice()),
            (5, b"snap-5".as_slice()),
            "the pointer re-read after a NotFound recovers a present snapshot"
        );
    }
}
