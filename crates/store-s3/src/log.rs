//! Metadata log segments and checkpoints in S3 (DESIGN.md §4).
//!
//! Each partition has its own ordered stream:
//! - Segments `log/<part>/<seq:016x>.zst` hold a zstd JSON array of log
//!   records, written with conditional create (CAS): a sequence number
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
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
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

    /// Persist a newly generated partition DEK before the split record can
    /// direct any metadata into that partition's encrypted stream.
    pub async fn ensure_partition_key(&self, partition: &str) -> Result<(), StoreError> {
        if let Some(keys) = &self.e2e {
            keys.ensure_partition(&self.store, partition).await?;
        }
        Ok(())
    }

    /// CAS-create segment `seq`. `AlreadyExists` when the sequence was
    /// already written (crash replay or a second writer).
    pub async fn put_segment(&self, seq: u64, payload: &[u8]) -> Result<(), StoreError> {
        let key = layout::log_segment(&self.partition, seq);
        let compressed = zstd::encode_all(payload, ZSTD_LEVEL)?;
        let body = match &self.e2e {
            Some(keys) => encrypt_object(
                &keys.dek(&self.partition)?,
                key.as_ref().as_bytes(),
                &compressed,
            )?,
            None => compressed,
        };
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
        let compressed = match &self.e2e {
            Some(keys) => {
                keys.refresh_partition(&self.store, &self.partition).await?;
                decrypt_object(
                    &keys.dek(&self.partition)?,
                    layout::log_segment(&self.partition, seq)
                        .as_ref()
                        .as_bytes(),
                    &body,
                )?
            }
            None => body.to_vec(),
        };
        Ok(zstd::decode_all(&compressed[..])?)
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
                encrypt_object(&keys.dek(PARTITION)?, key.as_ref().as_bytes(), &compressed)?
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
                &keys.dek(PARTITION)?,
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
        let keys = crate::e2e::put_keyring(&store, "test-pass").await.unwrap();
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
}
