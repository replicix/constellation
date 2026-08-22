//! Metadata log segments and checkpoints in S3 (DESIGN.md §4).
//!
//! Single-partition ("p0") phase-1 shape:
//! - Segments `log/p0/<seq:016x>.zst` hold a zstd JSON array of log
//!   records, written with conditional create (CAS): a sequence number
//!   can never be silently overwritten.
//! - Checkpoints `checkpoints/p0/<seq:016x>.zst` hold a zstd snapshot
//!   of the whole metadata DB covering the log up to and including
//!   `seq`; `checkpoints/p0/LATEST` points at the newest one.
//! - A fresh node bootstraps from the latest checkpoint plus replay of
//!   segments `> seq`; without a checkpoint it replays from segment 1.

use crate::error::StoreError;
use crate::layout;
use futures::TryStreamExt;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub const PARTITION: &str = "p0";
const ZSTD_LEVEL: i32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointRef {
    pub seq: u64,
}

fn latest_key() -> object_store::path::Path {
    object_store::path::Path::from(format!("checkpoints/{PARTITION}/LATEST"))
}

/// Metadata log I/O for one partition.
pub struct LogStore {
    store: Arc<dyn ObjectStore>,
}

impl LogStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    /// CAS-create segment `seq`. `AlreadyExists` when the sequence was
    /// already written (crash replay or a second writer).
    pub async fn put_segment(&self, seq: u64, payload: &[u8]) -> Result<(), StoreError> {
        let body = zstd::encode_all(payload, ZSTD_LEVEL)?;
        let key = layout::log_segment(PARTITION, seq);
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
        let res = self.store.get(&layout::log_segment(PARTITION, seq)).await?;
        Ok(zstd::decode_all(&res.bytes().await?[..])?)
    }

    /// All segment sequence numbers, ascending.
    pub async fn list_segments(&self) -> Result<Vec<u64>, StoreError> {
        let prefix = layout::log_prefix(PARTITION);
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
        Ok(seqs)
    }

    /// Store a checkpoint snapshot covering the log up to `seq`, then
    /// move the LATEST pointer.
    pub async fn put_checkpoint(&self, seq: u64, snapshot: &[u8]) -> Result<(), StoreError> {
        let body = zstd::encode_all(snapshot, ZSTD_LEVEL)?;
        self.store
            .put(&layout::checkpoint(PARTITION, seq), PutPayload::from(body))
            .await?;
        let ptr = serde_json::to_vec(&CheckpointRef { seq })?;
        self.store.put(&latest_key(), PutPayload::from(ptr)).await?;
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
        Ok(Some((r.seq, zstd::decode_all(&res.bytes().await?[..])?)))
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
}
