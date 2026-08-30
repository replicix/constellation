//! Chunk store over any `object_store` backend, plus filesystem metadata
//! (`meta.json`) lifecycle with conditional-create.

use crate::codec::CompressionSetting;
use crate::e2e::{decrypt_object, encrypt_object, SharedE2eKeys};
use crate::error::StoreError;
use crate::{format, layout};
use constellation_fs_core::{ChunkHash, DEFAULT_CHUNK_SIZE};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
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
    /// Random 32 bytes (hex) that seed the P2P gossip topic id (M3.3).
    /// Optional: filesystems created before this existed derive the
    /// topic from the UUID instead, which is weaker (the UUID is not a
    /// secret) but still works — messages are signed and direct
    /// connections are gated by the registry allowlist regardless.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gossip_secret: Option<String>,
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
            gossip_secret: Some(random_hex32()),
        }
    }

    /// The gossip topic seed, if this filesystem has one.
    pub fn gossip_seed(&self) -> Option<[u8; 32]> {
        let hex = self.gossip_secret.as_deref()?;
        if hex.len() != 64 {
            return None;
        }
        let mut out = [0u8; 32];
        for (i, pair) in hex.as_bytes().chunks(2).enumerate() {
            let s = std::str::from_utf8(pair).ok()?;
            out[i] = u8::from_str_radix(s, 16).ok()?;
        }
        Some(out)
    }
}

fn random_hex32() -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(64);
    for b in rand::random::<[u8; 32]>() {
        let _ = write!(s, "{b:02x}");
    }
    s
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
    e2e: Option<SharedE2eKeys>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkPutMode {
    /// Probe with HEAD, then PUT only on a miss.
    Probe,
    /// One conditional PUT (`If-None-Match: *`).
    Create,
    /// Unconditional PUT for backends lacking create-if-absent.
    Overwrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkPutResult {
    pub existed: bool,
}

impl ChunkStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store, e2e: None }
    }

    pub fn new_e2e(store: Arc<dyn ObjectStore>, keys: SharedE2eKeys) -> Self {
        Self {
            store,
            e2e: Some(keys),
        }
    }

    pub fn inner(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// Compute the filesystem's chunk identity. All callers which mint a
    /// manifest use this helper so an E2E mount cannot accidentally expose a
    /// plain confirmation-of-file hash.
    pub fn hash(&self, data: &[u8]) -> ChunkHash {
        self.e2e
            .as_ref()
            .map_or_else(|| ChunkHash::of(data), |keys| keys.hash(data))
    }

    /// Protect a cooperative-cache response with the filesystem DEK. Peers
    /// cache plaintext for fast local reads, so E2E mounts create a fresh
    /// authenticated envelope at the serving boundary rather than putting
    /// plaintext on the application stream.
    pub fn protect_peer_chunk(&self, hash: &ChunkHash, data: &[u8]) -> Result<Vec<u8>, StoreError> {
        match &self.e2e {
            Some(keys) => encrypt_object(&keys.dek("p0")?, &hash.0, data),
            None => Ok(data.to_vec()),
        }
    }

    pub fn open_peer_chunk(&self, hash: &ChunkHash, data: &[u8]) -> Result<Vec<u8>, StoreError> {
        match &self.e2e {
            Some(keys) => decrypt_object(&keys.dek("p0")?, &hash.0, data),
            None => Ok(data.to_vec()),
        }
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
        self.put_chunk_mode(hash, data, setting, ChunkPutMode::Create)
            .await
            .map(|_| ())
    }

    /// Store a chunk using one rung of the upload dedup ladder.
    ///
    /// Encoding is CPU-bound zstd work and must not occupy an async
    /// runtime worker when a bounded upload pool runs many chunks at
    /// once. `PutMode::Create` is one RTT and treats `AlreadyExists` as
    /// success. object_store does not send `Expect: 100-continue`, so
    /// a create hit still transmits the body; it saves the HEAD and
    /// avoids creating another version, not uplink bandwidth.
    pub async fn put_chunk_mode(
        &self,
        hash: &ChunkHash,
        data: &[u8],
        setting: CompressionSetting,
        mode: ChunkPutMode,
    ) -> Result<ChunkPutResult, StoreError> {
        debug_assert_eq!(&self.hash(data), hash);
        let key = layout::chunk_key(hash);
        // Stronger than the minimum renewal-time refresh: checking the
        // pointer at each dedup decision also covers a writer acquired just
        // before publication. An unconditional idempotent PUT resurrects the
        // bytes before its manifest can commit.
        let mode = if crate::gc::is_condemned(&self.store, hash).await? {
            ChunkPutMode::Overwrite
        } else {
            mode
        };
        if mode == ChunkPutMode::Probe {
            match self.store.head(&key).await {
                Ok(_) => return Ok(ChunkPutResult { existed: true }),
                Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        let owned = data.to_vec();
        let e2e = self.e2e.clone();
        let aad = hash.0;
        let obj = tokio::task::spawn_blocking(move || {
            let encoded = format::encode_object(&owned, setting)?;
            match e2e {
                Some(keys) => encrypt_object(&keys.dek("p0")?, &aad, &encoded),
                None => Ok(encoded),
            }
        })
        .await
        .map_err(|error| StoreError::Meta(format!("chunk encoder task failed: {error}")))??;
        let result = match mode {
            ChunkPutMode::Create => {
                self.store
                    .put_opts(
                        &key,
                        PutPayload::from(obj),
                        PutOptions::from(PutMode::Create),
                    )
                    .await
            }
            ChunkPutMode::Probe | ChunkPutMode::Overwrite => {
                self.store.put(&key, PutPayload::from(obj)).await
            }
        };
        match result {
            Ok(_) => Ok(ChunkPutResult { existed: false }),
            Err(object_store::Error::AlreadyExists { .. }) if mode == ChunkPutMode::Create => {
                Ok(ChunkPutResult { existed: true })
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Fetch and verify a chunk by content address.
    pub async fn get_chunk(&self, hash: &ChunkHash) -> Result<Vec<u8>, StoreError> {
        Ok(self.get_chunk_timed(hash).await?.0)
    }

    /// `get_chunk` plus the time-to-first-byte: the point where the
    /// response head has landed and the body is still streaming. The
    /// source selector needs the two halves apart, because TTFB and
    /// goodput are separate terms of its ETA (DESIGN.md §7) and a single
    /// end-to-end duration can only feed one of them.
    pub async fn get_chunk_timed(
        &self,
        hash: &ChunkHash,
    ) -> Result<(Vec<u8>, std::time::Duration), StoreError> {
        let key = layout::chunk_key(hash);
        let started = std::time::Instant::now();
        let res = self.store.get(&key).await?;
        let ttfb = started.elapsed();
        let obj = res.bytes().await?;
        let encoded = match &self.e2e {
            Some(keys) => decrypt_object(&keys.dek("p0")?, &hash.0, &obj)?,
            None => obj.to_vec(),
        };
        let data = format::decode_object(&encoded)?;
        if &self.hash(&data) != hash {
            return Err(StoreError::HashMismatch {
                key: key.to_string(),
            });
        }
        Ok((data, ttfb))
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
            Err(object_store::Error::NotImplemented { .. }) => false,
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
    #[tokio::test]
    async fn condemned_dedup_hit_is_reuploaded() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = ChunkStore::new(inner.clone());
        let data = b"condemned-race";
        let hash = store.hash(data);
        store
            .put_chunk_mode(&hash, data, CompressionSetting::RAW, ChunkPutMode::Create)
            .await
            .unwrap();
        crate::gc::publish_condemned(&inner, vec![hash.to_hex()], 1)
            .await
            .unwrap();
        inner.delete(&layout::chunk_key(&hash)).await.unwrap();
        let result = store
            .put_chunk_mode(&hash, data, CompressionSetting::RAW, ChunkPutMode::Probe)
            .await
            .unwrap();
        assert!(!result.existed);
        assert_eq!(store.get_chunk(&hash).await.unwrap(), data);
    }

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
        assert_eq!(loaded.gossip_secret, meta.gossip_secret);
        // Second create refused.
        assert!(matches!(
            s.create_fs(&meta).await,
            Err(StoreError::AlreadyExists)
        ));
    }

    /// A fresh filesystem gets a random gossip seed; two filesystems
    /// never share one, so joining a topic needs the bucket.
    #[test]
    fn gossip_secret_is_random_and_decodes() {
        let a = FsMeta::default();
        let b = FsMeta::default();
        assert_ne!(a.gossip_secret, b.gossip_secret);
        let seed = a.gossip_seed().expect("a fresh fs has a seed");
        assert_eq!(seed.len(), 32);
        assert_ne!(seed, [0u8; 32], "seed must not be all zeroes");
        assert_eq!(a.gossip_seed(), a.gossip_seed(), "stable across calls");
    }

    /// A pre-M3.3 `meta.json` has no `gossip_secret`. It must still load
    /// (the daemon falls back to a UUID-derived topic), and a malformed
    /// secret must degrade to that same fallback rather than panicking.
    #[tokio::test]
    async fn legacy_meta_json_without_gossip_secret_loads() {
        let s = store();
        let legacy = br#"{"uuid":"3f2504e0-4f89-41d3-9a0c-0305e82c3301",
            "format_version":1,"chunk_size":1048576,"compression":"zstd:3",
            "e2e":false,"created_unix":1}"#;
        s.inner()
            .put(
                &object_store::path::Path::from("meta.json"),
                PutPayload::from(legacy.to_vec()),
            )
            .await
            .unwrap();
        let loaded = s.load_fs().await.unwrap();
        assert!(loaded.gossip_secret.is_none());
        assert!(loaded.gossip_seed().is_none(), "no seed to derive from");

        let bad = FsMeta {
            gossip_secret: Some("not-hex".into()),
            ..FsMeta::default()
        };
        assert!(bad.gossip_seed().is_none(), "malformed seed must not panic");
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
