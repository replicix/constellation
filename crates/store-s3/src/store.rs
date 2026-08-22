//! Chunk store over any `object_store` backend, plus filesystem metadata
//! (`meta.json`) lifecycle with conditional-create.

use crate::codec::CompressionSetting;
use crate::error::StoreError;
use crate::{format, layout};
use constellation_fs_core::{ChunkHash, DEFAULT_CHUNK_SIZE};
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

pub const FORMAT_VERSION: u32 = 1;

/// `meta.json`: filesystem identity and settings (DESIGN.md §2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMeta {
    pub uuid: Uuid,
    pub format_version: u32,
    pub chunk_size: u32,
    /// Root compression setting, e.g. "zstd:3" or "raw".
    pub compression: String,
    pub e2e: bool,
    pub created_unix: i64,
}

impl FsMeta {
    pub fn new(chunk_size: u32, compression: &str) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            format_version: FORMAT_VERSION,
            chunk_size,
            compression: compression.to_string(),
            e2e: false,
            created_unix: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
        }
    }
}

impl Default for FsMeta {
    fn default() -> Self {
        Self::new(DEFAULT_CHUNK_SIZE, "zstd:3")
    }
}

/// Content-addressed chunk store over an `object_store` backend.
///
/// The backend is expected to be pre-scoped to the filesystem prefix
/// (e.g. via `object_store::prefix::PrefixStore` or a bucket subpath).
pub struct ChunkStore {
    store: Arc<dyn ObjectStore>,
}

impl ChunkStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub fn inner(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// Create a new filesystem at the prefix. Fails with `AlreadyExists`
    /// if a `meta.json` is already present (conditional create).
    pub async fn create_fs(&self, meta: &FsMeta) -> Result<(), StoreError> {
        let body = serde_json::to_vec_pretty(meta)?;
        let opts = PutOptions::from(PutMode::Create);
        match self
            .store
            .put_opts(&layout::meta_json(), PutPayload::from(body), opts)
            .await
        {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => Err(StoreError::AlreadyExists),
            Err(e) => Err(e.into()),
        }
    }

    /// Load `meta.json`; `NotFound` when the prefix holds no filesystem.
    pub async fn load_fs(&self) -> Result<FsMeta, StoreError> {
        let res = match self.store.get(&layout::meta_json()).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Err(StoreError::NotFound),
            Err(e) => return Err(e.into()),
        };
        let bytes = res.bytes().await?;
        let meta: FsMeta = serde_json::from_slice(&bytes)?;
        if meta.format_version > FORMAT_VERSION {
            return Err(StoreError::Meta(format!(
                "filesystem format version {} is newer than this binary supports ({FORMAT_VERSION})",
                meta.format_version
            )));
        }
        Ok(meta)
    }

    /// Store a chunk under its content address. Skips the upload when the
    /// object already exists (dedup fast path).
    pub async fn put_chunk(
        &self,
        hash: &ChunkHash,
        data: &[u8],
        setting: CompressionSetting,
    ) -> Result<(), StoreError> {
        debug_assert_eq!(&ChunkHash::of(data), hash);
        let key = layout::chunk_key(hash);
        if self.store.head(&key).await.is_ok() {
            return Ok(()); // dedup: identical content already stored
        }
        let obj = format::encode_object(data, setting)?;
        self.store.put(&key, PutPayload::from(obj)).await?;
        Ok(())
    }

    /// Fetch and verify a chunk by content address.
    pub async fn get_chunk(&self, hash: &ChunkHash) -> Result<Vec<u8>, StoreError> {
        let key = layout::chunk_key(hash);
        let res = self.store.get(&key).await?;
        let obj = res.bytes().await?;
        let data = format::decode_object(&obj)?;
        if &ChunkHash::of(&data) != hash {
            return Err(StoreError::HashMismatch {
                key: key.to_string(),
            });
        }
        Ok(data)
    }

    pub async fn has_chunk(&self, hash: &ChunkHash) -> Result<bool, StoreError> {
        match self.store.head(&layout::chunk_key(hash)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// `doctor` probe: verify which conditional-write primitives the
    /// backend supports. Create-if-absent is required from phase 1;
    /// etag CAS becomes load-bearing with leases (phase 3).
    pub async fn probe_conditional_writes(&self) -> Result<Capabilities, StoreError> {
        let key = object_store::path::Path::from(format!(".doctor/{}", Uuid::new_v4()));
        // Create-if-absent.
        let first = self
            .store
            .put_opts(
                &key,
                PutPayload::from_static(b"a"),
                PutOptions::from(PutMode::Create),
            )
            .await?;
        let create_if_absent = match self
            .store
            .put_opts(
                &key,
                PutPayload::from_static(b"b"),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Err(object_store::Error::AlreadyExists { .. }) => true,
            Ok(_) => false,
            Err(e) => {
                let _ = self.store.delete(&key).await;
                return Err(e.into());
            }
        };
        // CAS with the correct etag must succeed; with a stale etag it must fail.
        let update = PutOptions::from(PutMode::Update(object_store::UpdateVersion {
            e_tag: first.e_tag.clone(),
            version: first.version.clone(),
        }));
        let etag_cas = match self
            .store
            .put_opts(&key, PutPayload::from_static(b"c"), update)
            .await
        {
            Ok(_) => {
                let stale = PutOptions::from(PutMode::Update(object_store::UpdateVersion {
                    e_tag: first.e_tag,
                    version: first.version,
                }));
                matches!(
                    self.store
                        .put_opts(&key, PutPayload::from_static(b"d"), stale)
                        .await,
                    Err(object_store::Error::Precondition { .. })
                )
            }
            Err(object_store::Error::NotImplemented) => false,
            Err(e) => {
                let _ = self.store.delete(&key).await;
                return Err(e.into());
            }
        };
        let _ = self.store.delete(&key).await;
        Ok(Capabilities {
            create_if_absent,
            etag_cas,
        })
    }
}

/// Conditional-write support reported by [`ChunkStore::probe_conditional_writes`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    pub create_if_absent: bool,
    pub etag_cas: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn store() -> ChunkStore {
        ChunkStore::new(Arc::new(InMemory::new()))
    }

    #[tokio::test]
    async fn fs_create_and_load() {
        let s = store();
        assert!(matches!(s.load_fs().await, Err(StoreError::NotFound)));
        let meta = FsMeta::default();
        s.create_fs(&meta).await.unwrap();
        let loaded = s.load_fs().await.unwrap();
        assert_eq!(loaded.uuid, meta.uuid);
        assert_eq!(loaded.chunk_size, DEFAULT_CHUNK_SIZE);
        // Second create refused.
        assert!(matches!(
            s.create_fs(&meta).await,
            Err(StoreError::AlreadyExists)
        ));
    }

    #[tokio::test]
    async fn chunk_roundtrip_and_dedup() {
        let s = store();
        let data = vec![9u8; 10_000];
        let hash = ChunkHash::of(&data);
        assert!(!s.has_chunk(&hash).await.unwrap());
        let setting = CompressionSetting::zstd(3).unwrap();
        s.put_chunk(&hash, &data, setting).await.unwrap();
        assert!(s.has_chunk(&hash).await.unwrap());
        // Idempotent re-put (dedup path).
        s.put_chunk(&hash, &data, setting).await.unwrap();
        assert_eq!(s.get_chunk(&hash).await.unwrap(), data);
    }

    #[tokio::test]
    async fn tampered_chunk_detected() {
        let s = store();
        let data = b"legit data".to_vec();
        let hash = ChunkHash::of(&data);
        // Store different content under the same key, bypassing put_chunk.
        let evil = format::encode_object(b"evil data!", CompressionSetting::RAW).unwrap();
        s.inner()
            .put(&layout::chunk_key(&hash), PutPayload::from(evil))
            .await
            .unwrap();
        assert!(matches!(
            s.get_chunk(&hash).await,
            Err(StoreError::HashMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn conditional_write_probe() {
        store().probe_conditional_writes().await.unwrap();
    }

    #[tokio::test]
    async fn future_format_version_rejected() {
        let s = store();
        let meta = FsMeta {
            format_version: FORMAT_VERSION + 1,
            ..Default::default()
        };
        let body = serde_json::to_vec(&meta).unwrap();
        s.inner()
            .put(&layout::meta_json(), PutPayload::from(body))
            .await
            .unwrap();
        assert!(s.load_fs().await.unwrap_err().to_string().contains("newer"));
    }
}
