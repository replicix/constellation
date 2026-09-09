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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointRef {
    pub seq: u64,
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
        self.store.put(&key, PutPayload::from(body)).await?;
        let ptr = serde_json::to_vec(&CheckpointRef { seq })?;
        self.store.put(&latest_key(), PutPayload::from(ptr)).await?;
        let vec_body = serde_json::to_vec(vector)?;
        self.store
            .put(&vector_key(), PutPayload::from(vec_body))
            .await?;
        Ok(())
    }

    /// Latest checkpoint `(covered_seq, snapshot)` if any exists.
    pub async fn get_latest_checkpoint(&self) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        let ptr = match self.store.get(&latest_key()).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let r: CheckpointRef = serde_json::from_slice(&ptr.bytes().await?)?;
        let res = self
            .store
            .get(&layout::checkpoint(PARTITION, r.seq))
            .await?;
        let body = res.bytes().await?;
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
}
