//! Metadata log segments in S3 (DESIGN.md §4).
//!
//! Each partition has its own ordered stream:
//! - Segments `log/<part>/<seq:016x>.zst` hold a zstd postcard envelope of
//!   log records, written with conditional create (CAS): a sequence number
//!   can never be silently overwritten.
//! - A child stream that has been merged is sealed by a `sealed` marker
//!   object in the child's log prefix; tailers treat sealed+fully-applied
//!   as removable from the active set.
//!
//! Segments are small and are moved with one request each. Plan 28
//! retired the whole-DB `VACUUM INTO` checkpoint that used to live
//! alongside them (`checkpoints/p0/*` + `checkpoints/VECTOR.json`):
//! bootstrap now restores the commit chain's head and replays the log
//! from its `applied` position, or replays from genesis when no commit
//! exists yet (`shipper::bootstrap`).

use crate::e2e::{decrypt_object, encrypt_object, SharedE2eKeys};
use crate::error::StoreError;
use crate::layout;
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutPayload};
use std::sync::Arc;

/// Genesis partition id. A filesystem always has at least this one.
pub const PARTITION: &str = "p0";
const ZSTD_LEVEL: i32 = 3;

fn sealed_key(partition: &str) -> object_store::path::Path {
    object_store::path::Path::from(format!("log/{partition}/sealed"))
}

/// Metadata log I/O for one partition.
#[derive(Clone)]
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
        // Bounded, but not by the shipper's 4 MiB `segment_max_bytes`: a
        // transaction larger than that ships whole, and a subtree clone is one
        // record that grows with the tree. `MAX_LOG_SEGMENT` in `net` limits
        // the *compressed* bytes of a streamed segment, not this output. See
        // `codec::max_decompressed_len`.
        crate::codec::decompress_to_ceiling(&compressed[..])
    }

    /// CAS-create segment `seq`. `AlreadyExists` when the sequence was
    /// already written (crash replay or a second writer).
    ///
    /// Plan 30 §M4 item 1 (`crate::cas`): a 409 retries the same create
    /// (before, it read as `AlreadyExists`, and the shipper's tail then
    /// found nothing at `seq` and came straight back), and a 412 whose
    /// object is this very segment — our own create that landed behind a
    /// retried 5xx or a lost reply, which includes a takeover's epoch
    /// marker — is a success, not a collision. Only another writer's
    /// segment is `AlreadyExists`.
    pub async fn put_segment(&self, seq: u64, payload: &[u8]) -> Result<(), StoreError> {
        let key = layout::log_segment(&self.partition, seq);
        let body = self.seal_segment(seq, payload)?;
        match crate::cas::put_conditional(
            self.store.as_ref(),
            &key,
            body.into(),
            PutMode::Create,
            crate::cas::Verify::Body,
        )
        .await?
        {
            crate::cas::CasPut::Won(_) => Ok(()),
            crate::cas::CasPut::Lost | crate::cas::CasPut::Missing => {
                Err(StoreError::AlreadyExists)
            }
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
    ///
    /// The GETs past the gap are left to finish in the background rather
    /// than cancelled, so their connections go back to the pool; see
    /// [`crate::run`].
    pub async fn get_run(&self, from: u64, k: usize) -> Result<Vec<(u64, Vec<u8>)>, StoreError> {
        let keys =
            (0..k as u64).map(|i| layout::log_segment(&self.partition, from.saturating_add(i)));
        crate::run::get_run(&self.store, keys)
            .await?
            .into_iter()
            .zip(from..)
            .map(|(body, seq)| Ok((seq, self.open_segment(seq, &body)?)))
            .collect()
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

    /// The lowest segment sequence at or after `from` that exists, from
    /// one LIST-with-offset that reads only its first key. This is the
    /// authoritative answer to the question a GET-next `404` cannot
    /// settle: "is `from` the head, or was it pruned?" — `None` says
    /// head, `Some(from)` says a segment landed in the meantime, and
    /// `Some(later)` says retention removed `from..later` (DESIGN.md
    /// §14 "Falling behind segment GC"). Every S3 this project targets
    /// lists strongly consistently after a write, and a segment is
    /// never overwritten, so the answer cannot go stale in the direction
    /// that matters: a `None` or `Some(from)` may become `Some(from)` as
    /// the holder appends, but a pruned gap never closes.
    pub async fn first_segment_from(&self, from: u64) -> Result<Option<u64>, StoreError> {
        let prefix = layout::log_prefix(&self.partition);
        let offset = layout::log_segment(&self.partition, from.saturating_sub(1));
        let mut stream = self.store.list_with_offset(Some(&prefix), &offset);
        while let Some(meta) = stream.try_next().await? {
            let Some(seq) = meta
                .location
                .filename()
                .and_then(|name| name.strip_suffix(".zst"))
                .and_then(|name| u64::from_str_radix(name, 16).ok())
            else {
                continue; // the `sealed` marker
            };
            if seq >= from {
                return Ok(Some(seq));
            }
        }
        Ok(None)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

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

    /// Plan 30 §M4 item 1: segment create (and so the epoch marker, which
    /// is an empty segment) under each error code.
    #[tokio::test]
    async fn segment_create_error_codes() {
        use crate::faulty::{Calls, Fault, FaultyStore, OpKind};
        let faulty = FaultyStore::new();
        let s = LogStore::new(faulty.clone());
        // 409: the same create again, which lands.
        faulty.script(OpKind::Put, "log/", Calls::Nth(1), Fault::Status(409));
        s.put_segment(1, b"one").await.unwrap();
        // Our own create landed behind a 412 (or a lost reply): ours.
        faulty.clear();
        faulty.script(OpKind::Put, "log/", Calls::Nth(1), Fault::AppliedThen(412));
        s.put_segment(2, b"").await.unwrap();
        // Another writer's segment: a collision.
        faulty.clear();
        assert!(matches!(
            s.put_segment(2, b"theirs").await,
            Err(StoreError::AlreadyExists)
        ));
        // A 500 or a timeout is the store's error, never a collision.
        for fault in [Fault::Status(500), Fault::Timeout] {
            faulty.clear();
            faulty.script(OpKind::Put, "log/", Calls::Nth(1), fault);
            assert!(matches!(
                s.put_segment(3, b"three").await,
                Err(StoreError::ObjectStore(_))
            ));
        }
        // A timed-out reply whose write landed: the retry meets our own
        // object and is a success.
        faulty.clear();
        faulty.script(
            OpKind::Put,
            "log/",
            Calls::Nth(1),
            Fault::AppliedThenTimeout,
        );
        assert!(s.put_segment(3, b"three").await.is_err());
        s.put_segment(3, b"three").await.unwrap();
        assert_eq!(s.list_segments().await.unwrap(), vec![1, 2, 3]);
        assert_eq!(s.get_segment(2).await.unwrap(), b"");
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

    /// The gap probe behind a GET-next `404`: head, a segment that
    /// landed meanwhile, or a pruned range.
    #[tokio::test]
    async fn first_segment_from_tells_head_from_a_pruned_gap() {
        let s = ls();
        for seq in [3u64, 4, 5] {
            s.put_segment(seq, b"x").await.unwrap();
        }
        s.seal().await.unwrap();
        // 1 and 2 were pruned: a replica at applied=0 or 1 has a gap.
        assert_eq!(s.first_segment_from(1).await.unwrap(), Some(3));
        assert_eq!(s.first_segment_from(2).await.unwrap(), Some(3));
        // At the head, or inside the retained range: exact.
        assert_eq!(s.first_segment_from(3).await.unwrap(), Some(3));
        assert_eq!(s.first_segment_from(5).await.unwrap(), Some(5));
        assert_eq!(s.first_segment_from(6).await.unwrap(), None);
        assert_eq!(s.first_segment_from(u64::MAX).await.unwrap(), None);
        assert_eq!(ls().first_segment_from(1).await.unwrap(), None);
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
    async fn e2e_segments_are_ciphertext() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let keys = Arc::new(crate::e2e::E2eKeys::generate());
        let logs = LogStore::new_e2e(store.clone(), keys);
        logs.put_segment(1, b"secret filename").await.unwrap();
        assert_eq!(logs.get_segment(1).await.unwrap(), b"secret filename");
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

    /// A legitimate segment past the shipper's usual size — one oversized
    /// transaction, like a large subtree clone, ships whole — opens in full,
    /// however well it compresses. (A 64 MiB decompression cap once refused
    /// it, which would have stalled replay on every replica.)
    #[test]
    fn a_segment_larger_than_64_mib_opens() {
        let s = ls();
        let payload = vec![0x5au8; (80 << 20) + 3];
        let sealed = s.seal_segment(1, &payload).unwrap();
        assert!(sealed.len() < payload.len() / 1000, "compresses >1000x");
        assert_eq!(s.open_segment(1, &sealed).unwrap().len(), payload.len());
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
}
