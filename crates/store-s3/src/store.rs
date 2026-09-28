//! Chunk store over any `object_store` backend, plus filesystem metadata
//! (`meta.json`) lifecycle with conditional-create.

use crate::codec::CompressionSetting;
use crate::decode_gate::{DecodeGate, Priority as DecodePriority};
use crate::e2e::{decrypt_object, encrypt_object, KeyringBlock, SharedE2eKeys};
use crate::error::StoreError;
use crate::{format, layout};
use constellation_fs_core::cache::SpillFile;
use constellation_fs_core::{ChunkHash, DEFAULT_CHUNK_SIZE};
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::sync::Arc;
use uuid::Uuid;

pub const FORMAT_VERSION: u32 = 1;

/// How long [`ChunkStore::load_fs`] keeps retrying a `GET` of
/// `meta.json` that answers 404 while a `HEAD` finds the object
/// (`CONSTELLATION_META_READ_WAIT_S`, default 600 s).
pub fn meta_read_wait() -> std::time::Duration {
    std::time::Duration::from_secs(
        std::env::var("CONSTELLATION_META_READ_WAIT_S")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(600),
    )
}

/// What a prefix holds when `meta.json` is missing
/// ([`ChunkStore::locate_missing_fs`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MissingFs {
    /// The bucket does not exist at this endpoint (a `LIST` answered
    /// 404): the request reached a different store than the one the
    /// filesystem was created on.
    NoBucket,
    /// The bucket exists and holds nothing under the prefix.
    EmptyPrefix,
    /// Objects exist under the prefix, but no `meta.json`.
    NoMeta { sample: String },
    /// The `LIST` failed otherwise (the error's text).
    Unknown(String),
}

impl std::fmt::Display for MissingFs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MissingFs::NoBucket => write!(
                f,
                "the bucket does not exist at this endpoint: the command is talking to a \
                 different store than the one the filesystem was created on"
            ),
            MissingFs::EmptyPrefix => write!(
                f,
                "the bucket exists but holds nothing under this prefix (`fs create` first?)"
            ),
            MissingFs::NoMeta { sample } => write!(
                f,
                "objects exist under this prefix (e.g. {sample}) but meta.json does not: a \
                 partly deleted filesystem, or a prefix holding something else"
            ),
            MissingFs::Unknown(e) => write!(f, "listing the prefix failed too: {e}"),
        }
    }
}

/// Whether a `LIST` error says the bucket is absent: `object_store` keeps
/// a `LIST`'s status only in the error text (S3's `NoSuchBucket` code, or
/// a 404 status; a `LIST` cannot 404 for any other reason).
fn is_missing_bucket(e: &object_store::Error) -> bool {
    if matches!(e, object_store::Error::NotFound { .. }) {
        return true;
    }
    let mut text = e.to_string();
    let mut source = std::error::Error::source(e);
    while let Some(s) = source {
        text.push_str(&s.to_string());
        source = s.source();
    }
    text.contains("NoSuchBucket") || text.contains("status code: 404")
}

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
    /// E2E secret block: the passphrase-wrapped master key. Present iff
    /// `e2e`. All usable keys (addressing, per-partition DEKs, gossip
    /// seed) are derived from the master, so this is the only secret
    /// material stored, and `fs passwd` rewrites only this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keyring: Option<KeyringBlock>,
    /// Optional creation-time logical byte cap. Seeded into the replicated
    /// journal on first mount; live changes do not rewrite `meta.json`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_logical_bytes: Option<u64>,
    /// Plan 30 §M9: the filesystem's acknowledgement policy, "local"
    /// (the default: a backup within the RTT budget when there is one,
    /// else today's behaviour) or "s3" (`ack=s3`: every mutation is
    /// acknowledged only once its records are in the shared log). A
    /// mount's `--ack` overrides it for that mount.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ack_policy: Option<String>,
    /// Plan 30 §M10: `f`, how many write-eligible nodes a continuation
    /// epoch may form without (`fs create --epoch-slack`, `fs set
    /// epoch-slack`). Absent means 0: every roster node must be a member
    /// (today's rule), and no heartbeat promises are published.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch_slack: Option<u32>,
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
            keyring: None,
            max_logical_bytes: None,
            ack_policy: None,
            epoch_slack: None,
        }
    }

    /// `f` (0 when unset).
    pub fn epoch_slack(&self) -> u32 {
        self.epoch_slack.unwrap_or(0)
    }

    /// Unwrap the E2E keys from this filesystem's keyring block with the
    /// passphrase. Errors if the filesystem is E2E but has no block.
    pub fn unlock(&self, passphrase: &str) -> Result<SharedE2eKeys, StoreError> {
        let block = self
            .keyring
            .as_ref()
            .ok_or_else(|| StoreError::Meta("E2E filesystem has no keyring block".into()))?;
        crate::e2e::unlock(block, passphrase)
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
    /// CPU-heavy encoding is a separate resource from network uploads.
    /// The permit is moved into the blocking closure so cancelling the
    /// async caller cannot admit replacement work while that closure runs.
    encode_gate: Arc<tokio::sync::Semaphore>,
    /// Whole-object E2E decrypt briefly materializes encoded plaintext.
    /// Serialize it so simultaneous GET completions cannot multiply RSS.
    /// A demand (foreground) decrypt always cuts ahead of queued
    /// background (prefetch) decrypts — see `decode_gate.rs`.
    decode_gate: Arc<DecodeGate>,
    /// The condemned pointer as this store's dedup decisions last saw it
    /// (`crate::gc::CondemnedView`: read only after a hit, and why that
    /// is as safe as reading it first).
    condemned: crate::gc::CondemnedView,
}

const DEFAULT_MAX_ENCODE_CONCURRENCY: usize = 8;

fn encode_concurrency() -> usize {
    std::env::var("CONSTELLATION_ENCODE_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1)
                .min(DEFAULT_MAX_ENCODE_CONCURRENCY)
        })
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
        Self::with_encode_concurrency(store, None, encode_concurrency())
    }

    pub fn new_e2e(store: Arc<dyn ObjectStore>, keys: SharedE2eKeys) -> Self {
        Self::with_encode_concurrency(store, Some(keys), encode_concurrency())
    }

    fn with_encode_concurrency(
        store: Arc<dyn ObjectStore>,
        e2e: Option<SharedE2eKeys>,
        concurrency: usize,
    ) -> Self {
        Self {
            store,
            e2e,
            encode_gate: Arc::new(tokio::sync::Semaphore::new(concurrency.max(1))),
            decode_gate: Arc::new(DecodeGate::new(1)),
            condemned: crate::gc::CondemnedView::default(),
        }
    }

    async fn encode_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, StoreError> {
        self.encode_gate
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StoreError::Meta("chunk encoder gate closed".into()))
    }

    async fn run_encoder<T, F>(
        permit: tokio::sync::OwnedSemaphorePermit,
        encode: F,
    ) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, StoreError> + Send + 'static,
    {
        // The permit belongs to the blocking closure, not its JoinHandle.
        // Tokio cannot cancel a blocking closure after it starts, so this
        // placement keeps cancellation from admitting replacement work.
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            encode()
        })
        .await
        .map_err(|error| StoreError::Meta(format!("chunk encoder task failed: {error}")))?
    }

    pub fn inner(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    pub fn is_e2e(&self) -> bool {
        self.e2e.is_some()
    }

    /// The filesystem's E2E keyring, when it has one (plan 28's metadata
    /// tree readers hash and shape nodes under its addressing key).
    pub fn e2e_keys(&self) -> Option<&SharedE2eKeys> {
        self.e2e.as_ref()
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
            Some(keys) => encrypt_object(&keys.dek("p0"), &hash.0, data),
            None => Ok(data.to_vec()),
        }
    }

    pub fn open_peer_chunk(&self, hash: &ChunkHash, data: &[u8]) -> Result<Vec<u8>, StoreError> {
        match &self.e2e {
            Some(keys) => decrypt_object(&keys.dek("p0"), &hash.0, data),
            None => Ok(data.to_vec()),
        }
    }

    /// Create a new filesystem at the prefix. Fails with `AlreadyExists`
    /// if a `meta.json` is already present (conditional create).
    pub async fn create_fs(&self, meta: &FsMeta) -> Result<(), StoreError> {
        let body = serde_json::to_vec_pretty(meta)?;
        // Plan 30 §M4 item 1: a 409 retries; our own create that landed
        // behind a 412 is ours (the body carries a fresh uuid).
        match crate::cas::put_conditional(
            self.store.as_ref(),
            &layout::meta_json(),
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

    /// Load `meta.json`; `NotFound` when the prefix holds no filesystem.
    ///
    /// A `GET` answered 404 is only believed once a `HEAD` agrees — a
    /// defence against a store whose reads disagree (a cached negative
    /// answer, a lagging replica). The OVH runs' "missing meta.json" that
    /// motivated it turned out to be something else (a `mount` run
    /// without the OVH profile asked AWS; see
    /// [`Self::locate_missing_fs`]), and OVH served every read
    /// consistently when probed; the check costs one `HEAD` on the 404
    /// path only. When the `HEAD` finds the object, the `GET` is retried
    /// with backoff (alternating a plain and a ranged request) for up to
    /// [`meta_read_wait`], saying so, and then fails with an error that
    /// names the inconsistency instead of "no filesystem".
    pub async fn load_fs(&self) -> Result<FsMeta, StoreError> {
        self.load_fs_waiting(meta_read_wait()).await
    }

    /// [`Self::load_fs`] with an explicit wait for a `GET` that disagrees
    /// with the `HEAD`.
    pub async fn load_fs_waiting(&self, wait: std::time::Duration) -> Result<FsMeta, StoreError> {
        let key = layout::meta_json();
        let bytes = match self.store.get(&key).await {
            Ok(r) => r.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => self.meta_json_behind_a_404(wait).await?,
            Err(e) => return Err(e.into()),
        };
        let meta: FsMeta = serde_json::from_slice(&bytes)?;
        if meta.format_version > FORMAT_VERSION {
            return Err(StoreError::Meta(format!(
                "filesystem format version {} is newer than this binary supports ({FORMAT_VERSION})",
                meta.format_version
            )));
        }
        // `fs create` validated the chunk size it wrote, but what comes
        // back is whatever is in the bucket now: every read and write path
        // divides by it (`ChunkLayout`), so a corrupt or hostile `0` would
        // panic each node the moment it touched a file, and an absurdly
        // large one drives chunk-sized allocations.
        constellation_fs_core::validate_chunk_size(meta.chunk_size)
            .map_err(|e| StoreError::Meta(format!("meta.json: {e}")))?;
        Ok(meta)
    }

    /// Whether `meta.json` exists, by `HEAD` alone (`doctor`: a `GET` of
    /// the key before `fs create` is what a store that caches negative
    /// answers would keep serving to the `mount` after it).
    pub async fn fs_exists(&self) -> Result<bool, StoreError> {
        match self.store.head(&layout::meta_json()).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// `load_fs` said [`StoreError::NotFound`]: what the prefix holds
    /// instead, so the error can say whether the request reached the
    /// store the filesystem lives on. One `LIST` of the prefix — only on
    /// this failure path. A `LIST` answers 404 only when the bucket itself
    /// is absent, which means the command talks to a different store (a
    /// different endpoint or account) than the one the filesystem was
    /// created on: the OVH campaigns' "mount lag" was a scripted `mount`
    /// that ran without the OVH profile and asked AWS for the bucket.
    pub async fn locate_missing_fs(&self) -> MissingFs {
        match self.store.list(None).next().await {
            None => MissingFs::EmptyPrefix,
            Some(Ok(object)) => MissingFs::NoMeta {
                sample: object.location.to_string(),
            },
            Some(Err(e)) if is_missing_bucket(&e) => MissingFs::NoBucket,
            Some(Err(e)) => MissingFs::Unknown(e.to_string()),
        }
    }

    /// `load_fs`'s `GET` said 404: `NotFound` if a `HEAD` agrees, else
    /// the body once a `GET` serves it (see [`Self::load_fs`]).
    async fn meta_json_behind_a_404(
        &self,
        wait: std::time::Duration,
    ) -> Result<bytes::Bytes, StoreError> {
        let key = layout::meta_json();
        let started = std::time::Instant::now();
        let mut backoff = std::time::Duration::from_millis(250);
        let mut attempt = 0u32;
        let mut warned = false;
        loop {
            let head = match self.store.head(&key).await {
                Ok(head) => head,
                Err(object_store::Error::NotFound { .. }) => return Err(StoreError::NotFound),
                Err(e) => return Err(e.into()),
            };
            if !warned {
                warned = true;
                tracing::warn!(
                    key = %key,
                    size = head.size,
                    wait_s = wait.as_secs(),
                    "meta.json exists (HEAD) but a GET answered 404: the backend's reads \
                     disagree; retrying the GET"
                );
            }
            if started.elapsed() >= wait {
                return Err(StoreError::Meta(format!(
                    "meta.json exists (HEAD answers {} bytes) but every GET for {:?} answered \
                     404 — the backend serves inconsistent reads; retry the mount later \
                     (CONSTELLATION_META_READ_WAIT_S sets the wait)",
                    head.size,
                    started.elapsed()
                )));
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(std::time::Duration::from_secs(5));
            attempt += 1;
            // Alternate a ranged GET with the plain one: a cache in front
            // of the store keyed on the request's shape answers them apart.
            let options = object_store::GetOptions {
                range: (attempt % 2 == 1).then_some(object_store::GetRange::Offset(0)),
                ..Default::default()
            };
            match self.store.get_opts(&key, options).await {
                Ok(r) => {
                    tracing::info!(
                        attempt,
                        waited_ms = started.elapsed().as_millis() as u64,
                        "meta.json read after the backend's GET caught up"
                    );
                    return Ok(r.bytes().await?);
                }
                Err(object_store::Error::NotFound { .. }) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Change the E2E passphrase: rewrap the master key in `meta.json`
    /// under a CAS update. Only the keyring block moves — the master key,
    /// and therefore every derived key and the gossip topic, is unchanged,
    /// so mounted nodes (which hold the master in memory) keep running
    /// with no remount.
    pub async fn change_passphrase(&self, old: &str, new: &str) -> Result<(), StoreError> {
        let key = layout::meta_json();
        let object = self.store.get(&key).await?;
        let version = UpdateVersion {
            e_tag: object.meta.e_tag.clone(),
            version: object.meta.version.clone(),
        };
        let mut meta: FsMeta = serde_json::from_slice(&object.bytes().await?)?;
        if !meta.e2e {
            return Err(StoreError::Meta("filesystem is not in E2E mode".into()));
        }
        let block = meta
            .keyring
            .as_ref()
            .ok_or_else(|| StoreError::Meta("E2E filesystem has no keyring block".into()))?;
        meta.keyring = Some(crate::e2e::rewrap_master(block, old, new)?);
        self.store
            .put_opts(
                &key,
                PutPayload::from(serde_json::to_vec_pretty(&meta)?),
                PutOptions::from(PutMode::Update(version)),
            )
            .await?;
        Ok(())
    }

    /// Plan 30 §M10: set `epoch_slack` in `meta.json` with a CAS update
    /// (a lost race re-reads and retries, a few times). `0` removes the
    /// field. Returns the previous value. The caller validates `f`
    /// against the roster (`heartbeat::check_epoch_slack`); mounted nodes
    /// pick the new value up on their next `meta.json` read.
    pub async fn set_epoch_slack(&self, epoch_slack: u32) -> Result<u32, StoreError> {
        let key = layout::meta_json();
        for _ in 0..5 {
            let object = self.store.get(&key).await.map_err(|e| match e {
                object_store::Error::NotFound { .. } => StoreError::NotFound,
                e => e.into(),
            })?;
            let version = UpdateVersion {
                e_tag: object.meta.e_tag.clone(),
                version: object.meta.version.clone(),
            };
            let mut meta: FsMeta = serde_json::from_slice(&object.bytes().await?)?;
            let previous = meta.epoch_slack();
            meta.epoch_slack = (epoch_slack > 0).then_some(epoch_slack);
            match crate::cas::put_conditional(
                self.store.as_ref(),
                &key,
                serde_json::to_vec_pretty(&meta)?.into(),
                PutMode::Update(version),
                crate::cas::Verify::Body,
            )
            .await?
            {
                crate::cas::CasPut::Won(_) => return Ok(previous),
                crate::cas::CasPut::Lost | crate::cas::CasPut::Missing => continue,
            }
        }
        Err(StoreError::CasConflict)
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
    /// a dedup hit. object_store does not send `Expect: 100-continue`, so
    /// a create hit still transmits the body; it saves the HEAD and
    /// avoids creating another version, not uplink bandwidth.
    ///
    /// The condemned pointer is read only after S3 said the object is
    /// already there, and only then (`crate::gc::CondemnedView` has the
    /// safety argument): a unique chunk costs one request — the create,
    /// or `Probe`'s HEAD and PUT — where it used to cost a pointer GET
    /// first. A hit costs the existence request plus one pointer read,
    /// as before, and that read is a bodyless `304` while the pointer is
    /// unchanged; the first hit after a mount or a GC publication adds a
    /// HEAD (`DedupVerdict::Unordered`).
    pub async fn put_chunk_mode(
        &self,
        hash: &ChunkHash,
        data: &[u8],
        setting: CompressionSetting,
        mode: ChunkPutMode,
    ) -> Result<ChunkPutResult, StoreError> {
        debug_assert_eq!(&self.hash(data), hash);
        let key = layout::chunk_key(hash);
        // Taken before the existence request is sent: what a hit's
        // verdict compares the pointer with.
        let before = self.condemned.snapshot();
        let mut mode = mode;
        if mode == ChunkPutMode::Probe {
            match self.store.head(&key).await {
                Ok(_) => match self.condemned.verdict(&self.store, hash, before).await? {
                    crate::gc::DedupVerdict::Sound => return Ok(ChunkPutResult { existed: true }),
                    // Re-upload: resurrects a condemned chunk.
                    crate::gc::DedupVerdict::Condemned => mode = ChunkPutMode::Overwrite,
                    crate::gc::DedupVerdict::Unordered => {
                        if self.exists_after_verdict(&key).await? {
                            return Ok(ChunkPutResult { existed: true });
                        }
                        mode = ChunkPutMode::Overwrite
                    }
                },
                Err(object_store::Error::NotFound { .. }) => mode = ChunkPutMode::Overwrite,
                Err(error) => return Err(error.into()),
            }
        }
        // Wait for CPU admission before making the blocking closure's owned
        // copy. Upload futures waiting here retain only cache.get's Vec.
        let encode_permit = self.encode_permit().await?;
        let owned = data.to_vec();
        let e2e = self.e2e.clone();
        let aad = hash.0;
        let obj = Self::run_encoder(encode_permit, move || {
            let encoded = format::encode_object(&owned, setting)?;
            match e2e {
                Some(keys) => encrypt_object(&keys.dek("p0"), &aad, &encoded),
                None => Ok(encoded),
            }
        })
        .await?;
        let obj = bytes::Bytes::from(obj);
        if mode == ChunkPutMode::Create {
            // Plan 30 §M4 item 1: a 409 is retried, never a dedup hit
            // (`crate::cas::create_content_addressed`).
            let existed =
                crate::cas::create_content_addressed(self.store.as_ref(), &key, obj.clone())
                    .await?;
            if !existed {
                return Ok(ChunkPutResult { existed: false });
            }
            match self.condemned.verdict(&self.store, hash, before).await? {
                crate::gc::DedupVerdict::Sound => return Ok(ChunkPutResult { existed: true }),
                crate::gc::DedupVerdict::Condemned => {}
                crate::gc::DedupVerdict::Unordered => {
                    if self.exists_after_verdict(&key).await? {
                        return Ok(ChunkPutResult { existed: true });
                    }
                }
            }
        }
        // `Overwrite`, a `Probe` miss, or a create hit the pointer does
        // not let this upload rely on: an unconditional PUT (idempotent
        // for content-addressed bytes; it resurrects a deleted chunk).
        self.store.put(&key, PutPayload::from(obj)).await?;
        Ok(ChunkPutResult { existed: false })
    }

    /// An undecided dedup hit (`DedupVerdict::Unordered`: the pointer
    /// read found no condemnation, but it moved since the last
    /// observation, or there was none) asks again: a `HEAD` sent after
    /// that read. Found, the hit is sound — read, then object found, is
    /// the old order exactly; absent, the caller uploads.
    async fn exists_after_verdict(
        &self,
        key: &object_store::path::Path,
    ) -> Result<bool, StoreError> {
        match self.store.head(key).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// Whether `hash` is durable in the bucket and may stand in for an
    /// upload: its object exists and bucket GC has not condemned it (a
    /// condemned chunk may be deleted at any moment, which is why
    /// [`Self::put_chunk_mode`] re-uploads rather than dedups one). The
    /// same test a `Probe` put's HEAD applies before acknowledging an
    /// upload without sending the bytes, in the same order: the HEAD
    /// first, the pointer only when the object is there
    /// (`crate::gc::CondemnedView`). An undecided verdict asks again with
    /// a second HEAD, which the pointer read then precedes.
    pub async fn chunk_durable(&self, hash: &ChunkHash) -> Result<bool, StoreError> {
        let key = layout::chunk_key(hash);
        let before = self.condemned.snapshot();
        match self.store.head(&key).await {
            Ok(_) => {}
            Err(object_store::Error::NotFound { .. }) => return Ok(false),
            Err(error) => return Err(error.into()),
        }
        match self.condemned.verdict(&self.store, hash, before).await? {
            crate::gc::DedupVerdict::Sound => Ok(true),
            crate::gc::DedupVerdict::Condemned => Ok(false),
            crate::gc::DedupVerdict::Unordered => self.exists_after_verdict(&key).await,
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
            Some(keys) => decrypt_object(&keys.dek("p0"), &hash.0, &obj)?,
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

    /// Fetch, decode, hash, and write a chunk without buffering the S3 body.
    pub async fn get_chunk_to_writer(
        &self,
        hash: &ChunkHash,
        out: &mut (impl Write + Send),
    ) -> Result<(u64, std::time::Duration, std::time::Duration), StoreError> {
        let started = std::time::Instant::now();

        // Whole-object AEAD still needs a one-shot authenticated decrypt.
        // The cache fetch path will replace this compatibility fallback with
        // ciphertext spill + serialized decrypt before enabling e2e prefetch.
        if self.e2e.is_some() {
            let (data, ttfb) = self.get_chunk_timed(hash).await?;
            out.write_all(&data)?;
            return Ok((data.len() as u64, ttfb, started.elapsed()));
        }

        let key = layout::chunk_key(hash);
        let result = self.store.get(&key).await?;
        let ttfb = started.elapsed();
        let mut stream = result.into_stream();
        let mut header = Vec::with_capacity(format::HEADER_LEN);
        let mut decoder = None;

        while let Some(piece) = stream.next().await {
            let piece = piece?;
            let mut payload = piece.as_ref();
            if decoder.is_none() {
                let needed = format::HEADER_LEN - header.len();
                let take = needed.min(payload.len());
                header.extend_from_slice(&payload[..take]);
                payload = &payload[take..];
                if header.len() == format::HEADER_LEN {
                    let header: [u8; format::HEADER_LEN] = header.as_slice().try_into().unwrap();
                    decoder = Some(format::StreamingDecoder::new(
                        &header,
                        &mut *out,
                        blake3::Hasher::new(),
                    )?);
                }
            }
            if !payload.is_empty() {
                decoder.as_mut().unwrap().write_payload(payload)?;
            }
        }

        let decoder =
            decoder.ok_or_else(|| StoreError::CorruptObject("truncated header".into()))?;
        let (bytes, actual) = decoder.finish()?;
        if actual.as_bytes() != &hash.0 {
            return Err(StoreError::HashMismatch {
                key: key.to_string(),
            });
        }
        Ok((bytes, ttfb, started.elapsed()))
    }

    /// E2E variant which keeps the network-lifetime ciphertext out of RAM.
    ///
    /// Ciphertext streams into `cipher_spill`; only after the complete AEAD
    /// envelope is present is it mapped and authenticated in one shot.
    ///
    /// `priority` decides queue order at the decode gate, not the total
    /// number of concurrent decrypts (that stays capped at 1 to bound
    /// RSS). Pass [`DecodePriority::Demand`] for a read the caller is
    /// blocked on right now, [`DecodePriority::Background`] for prefetch,
    /// scan-ahead, or cooperative-cache warm — so a bulk background
    /// backlog cannot make a foreground read wait behind an arbitrarily
    /// deep decrypt queue.
    pub async fn get_chunk_to_writer_e2e(
        &self,
        hash: &ChunkHash,
        cipher_spill: &mut SpillFile,
        out: &mut (impl Write + Send),
        priority: DecodePriority,
    ) -> Result<(u64, std::time::Duration, std::time::Duration), StoreError> {
        let Some(keys) = &self.e2e else {
            return self.get_chunk_to_writer(hash, out).await;
        };
        let key = layout::chunk_key(hash);
        let started = std::time::Instant::now();
        let result = self.store.get(&key).await?;
        let ttfb = started.elapsed();
        let mut stream = result.into_stream();
        while let Some(piece) = stream.next().await {
            cipher_spill.write_all(&piece?)?;
        }
        cipher_spill.flush()?;
        // Goodput for the source selector is a *network* quantity: bytes over
        // the wire divided by body time. Stop the transfer clock here — before
        // the decode gate and the CPU decrypt — otherwise a deep decrypt queue
        // (capacity 1 by design, to bound RSS) makes every stream look like a
        // few Mbps even while aggregate WAN throughput is hundreds of Mbps.
        let transfer_done = started.elapsed();

        let permit = self.decode_gate.clone().acquire(priority).await;
        let mapping = unsafe { memmap2::Mmap::map(cipher_spill.as_file())? };
        let dek = keys.dek("p0");
        let aad = hash.0;
        let encoded = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            decrypt_object(&dek, &aad, &mapping)
        })
        .await
        .map_err(|error| StoreError::Meta(format!("chunk decoder task failed: {error}")))??;

        if encoded.len() < format::HEADER_LEN {
            return Err(StoreError::CorruptObject("truncated header".into()));
        }
        let header: [u8; format::HEADER_LEN] = encoded[..format::HEADER_LEN].try_into().unwrap();
        let hasher = blake3::Hasher::new_keyed(keys.addressing_key());
        let mut decoder = format::StreamingDecoder::new(&header, out, hasher)?;
        decoder.write_payload(&encoded[format::HEADER_LEN..])?;
        let (bytes, actual) = decoder.finish()?;
        if actual.as_bytes() != &hash.0 {
            return Err(StoreError::HashMismatch {
                key: key.to_string(),
            });
        }
        Ok((bytes, ttfb, transfer_done))
    }

    pub async fn has_chunk(&self, hash: &ChunkHash) -> Result<bool, StoreError> {
        match self.store.head(&layout::chunk_key(hash)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Exercise every object-store operation a constellation filesystem
    /// relies on against a throwaway key under the configured prefix, so
    /// `fs create` can refuse a backend up front with one clean message
    /// instead of failing mid-create with a raw protocol error (e.g.
    /// Backblaze B2, which 501s the `If-None-Match`/`If-Match` headers
    /// that create-if-absent and etag CAS depend on).
    ///
    /// Every check runs even when an earlier one fails, so the report
    /// lists all problems at once. Cleanup is best-effort.
    pub async fn preflight(&self) -> Vec<PreflightCheck> {
        let key = object_store::path::Path::from(format!(".preflight/{}", Uuid::new_v4()));
        let mut checks = Vec::new();

        // Unconditional write: connectivity, credentials, bucket/prefix
        // write permission. Its version feeds the etag-CAS check below.
        let write = self
            .store
            .put_opts(
                &key,
                PutPayload::from_static(b"preflight"),
                PutOptions::from(PutMode::Overwrite),
            )
            .await;
        let written = match &write {
            Ok(r) => Some(UpdateVersion {
                e_tag: r.e_tag.clone(),
                version: r.version.clone(),
            }),
            Err(_) => None,
        };
        checks.push(PreflightCheck::new(
            "write object (PUT)",
            true,
            write.map(|_| ()).map_err(|e| concise_os_error(&e)),
        ));

        // Read back.
        let read = match self.store.get(&key).await {
            Ok(r) => r.bytes().await.map(|_| ()),
            Err(e) => Err(e),
        };
        checks.push(PreflightCheck::new(
            "read object (GET)",
            true,
            read.map_err(|e| concise_os_error(&e)),
        ));

        // List the prefix (GC and node discovery walk object listings).
        let list = self
            .store
            .list(Some(&object_store::path::Path::from(".preflight")))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>();
        checks.push(PreflightCheck::new(
            "list objects (LIST)",
            true,
            list.map(|_| ()).map_err(|e| concise_os_error(&e)),
        ));

        // Create-if-absent (`If-None-Match: *`): load-bearing from phase 1.
        let cia_key = object_store::path::Path::from(format!(".preflight/{}", Uuid::new_v4()));
        let create_if_absent = match self
            .store
            .put_opts(
                &cia_key,
                PutPayload::from_static(b"a"),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => match self
                .store
                .put_opts(
                    &cia_key,
                    PutPayload::from_static(b"b"),
                    PutOptions::from(PutMode::Create),
                )
                .await
            {
                Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
                Ok(_) => Err("backend accepted a second create over an existing object; \
                              create-if-absent is not enforced"
                    .to_string()),
                Err(e) => Err(concise_os_error(&e)),
            },
            Err(e) => Err(concise_os_error(&e)),
        };
        let _ = self.store.delete(&cia_key).await;
        checks.push(PreflightCheck::new(
            "create-if-absent (If-None-Match)",
            true,
            create_if_absent,
        ));

        // Etag CAS (`If-Match`): required for lease renew/takeover, and so
        // for multi-node mounts. Single-node mounts degrade without it, so
        // it is reported but not treated as fatal for `fs create`.
        let etag_cas = match &written {
            Some(v) => match self
                .store
                .put_opts(
                    &key,
                    PutPayload::from_static(b"cas"),
                    PutOptions::from(PutMode::Update(v.clone())),
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(e) => Err(concise_os_error(&e)),
            },
            None => Err("skipped (initial write failed)".to_string()),
        };
        checks.push(PreflightCheck::new("etag CAS (If-Match)", false, etag_cas));

        // Delete: cleanup uses it everywhere (GC, lease release).
        let delete = self.store.delete(&key).await;
        checks.push(PreflightCheck::new(
            "delete object (DELETE)",
            true,
            delete.map_err(|e| concise_os_error(&e)),
        ));

        checks
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

/// One operation exercised by [`ChunkStore::preflight`].
#[derive(Debug, Clone)]
pub struct PreflightCheck {
    /// Human-readable operation name (e.g. `"create-if-absent (If-None-Match)"`).
    pub name: &'static str,
    /// Whether a filesystem cannot be created without this operation.
    pub required: bool,
    /// `Ok(())` when the operation worked, `Err(reason)` with a concise
    /// explanation otherwise.
    pub outcome: Result<(), String>,
}

impl PreflightCheck {
    fn new(name: &'static str, required: bool, outcome: Result<(), String>) -> Self {
        Self {
            name,
            required,
            outcome,
        }
    }
}

/// Collapse a verbose `object_store` error (which embeds the full S3 XML
/// body) into a one-line reason suitable for a preflight report.
fn concise_os_error(e: &object_store::Error) -> String {
    match e {
        object_store::Error::NotImplemented { .. } => {
            "not supported by this S3 backend (server returned 501 Not Implemented)".to_string()
        }
        object_store::Error::NotFound { .. } => "object not found".to_string(),
        object_store::Error::Precondition { .. } => "precondition failed".to_string(),
        object_store::Error::AlreadyExists { .. } => "object already exists".to_string(),
        object_store::Error::Generic { source, .. } => {
            // Generic S3 errors wrap the transport error; some backends
            // report an unimplemented conditional header this way.
            let msg = source.to_string();
            if msg.contains("501") || msg.contains("NotImplemented") {
                "not supported by this S3 backend (server returned 501 Not Implemented)".to_string()
            } else {
                // Keep just the first line; the rest is an XML dump.
                msg.lines().next().unwrap_or("request failed").to_string()
            }
        }
        other => other
            .to_string()
            .lines()
            .next()
            .unwrap_or("request failed")
            .to_string(),
    }
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

    /// Requests a put made, by kind, on chunk keys and on the pointer.
    fn counts(faulty: &crate::faulty::FaultyStore) -> [usize; 4] {
        use crate::faulty::OpKind;
        [
            faulty.calls(OpKind::Put, "chunks/"),
            faulty.calls(OpKind::Head, "chunks/"),
            faulty.calls(OpKind::Get, "chunks/"),
            faulty.calls(OpKind::Get, "condemned"),
        ]
    }

    /// The OVH run's finding 3: a unique chunk is one request — the
    /// create, or `Probe`'s HEAD and PUT — with no condemned-pointer read
    /// in front of it (it used to GET `gc/condemned` first, every time).
    #[tokio::test]
    async fn a_unique_chunk_costs_no_condemned_read() {
        let faulty = crate::faulty::FaultyStore::new();
        let s = ChunkStore::new(faulty.clone());
        // Published behind the counters' back (the GC round's own GET).
        let uncounted: Arc<dyn ObjectStore> = faulty.inner();
        crate::gc::publish_condemned(&uncounted, vec![], 1)
            .await
            .unwrap();
        let a = b"unique-a";
        let r = s
            .put_chunk_mode(&s.hash(a), a, CompressionSetting::RAW, ChunkPutMode::Create)
            .await
            .unwrap();
        assert!(!r.existed);
        assert_eq!(counts(&faulty), [1, 0, 0, 0], "create: one PUT");
        let b = b"unique-b";
        let r = s
            .put_chunk_mode(&s.hash(b), b, CompressionSetting::RAW, ChunkPutMode::Probe)
            .await
            .unwrap();
        assert!(!r.existed);
        assert_eq!(counts(&faulty), [2, 1, 0, 0], "probe miss: HEAD + PUT");
        let c = b"unique-c";
        assert!(!s.chunk_durable(&s.hash(c)).await.unwrap());
        assert_eq!(counts(&faulty), [2, 2, 0, 0], "absent: one HEAD");
    }

    /// A hit reads the pointer after the existence answer: the first hit
    /// after mount has nothing observed before it, so it asks again with
    /// a HEAD; every later hit with the pointer unchanged is sound at
    /// once, and its read is a bodyless `304`.
    #[tokio::test]
    async fn a_hit_reads_the_pointer_after_the_existence_answer() {
        let faulty = crate::faulty::FaultyStore::new();
        let s = ChunkStore::new(faulty.clone());
        let uncounted: Arc<dyn ObjectStore> = faulty.inner();
        crate::gc::publish_condemned(&uncounted, vec![], 1)
            .await
            .unwrap();
        let a = b"shared-content";
        let h = s.hash(a);
        s.put_chunk_mode(&h, a, CompressionSetting::RAW, ChunkPutMode::Create)
            .await
            .unwrap();
        let r = s
            .put_chunk_mode(&h, a, CompressionSetting::RAW, ChunkPutMode::Create)
            .await
            .unwrap();
        assert!(r.existed, "asked again after the read, and found");
        // create, create (412), pointer GET, HEAD (nothing observed before
        // the first create's answer: the HEAD after the read decides)
        assert_eq!(counts(&faulty), [2, 1, 0, 1]);
        let r = s
            .put_chunk_mode(&h, a, CompressionSetting::RAW, ChunkPutMode::Create)
            .await
            .unwrap();
        assert!(r.existed, "the pointer did not move around the hit");
        assert_eq!(counts(&faulty), [3, 1, 0, 2]);
        let r = s
            .put_chunk_mode(&h, a, CompressionSetting::RAW, ChunkPutMode::Probe)
            .await
            .unwrap();
        assert!(r.existed);
        assert_eq!(counts(&faulty), [3, 2, 0, 3], "probe hit: HEAD + pointer");
        assert!(s.chunk_durable(&h).await.unwrap());
        assert_eq!(counts(&faulty), [3, 3, 0, 4]);
    }

    /// No round ever published a pointer: every hit is sound at once.
    #[tokio::test]
    async fn a_hit_with_no_pointer_published_is_sound() {
        let faulty = crate::faulty::FaultyStore::new();
        let s = ChunkStore::new(faulty.clone());
        let a = b"never-condemned";
        let h = s.hash(a);
        s.put_chunk_mode(&h, a, CompressionSetting::RAW, ChunkPutMode::Create)
            .await
            .unwrap();
        let r = s
            .put_chunk_mode(&h, a, CompressionSetting::RAW, ChunkPutMode::Create)
            .await
            .unwrap();
        assert!(r.existed);
        assert_eq!(counts(&faulty), [2, 0, 0, 1]);
    }

    /// A hit on a condemned chunk is re-uploaded, whatever the ladder
    /// rung, and the upload resurrects a chunk GC already deleted.
    #[tokio::test]
    async fn a_condemned_hit_is_reuploaded_on_every_rung() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let s = ChunkStore::new(inner.clone());
        let a = b"condemned-content";
        let h = s.hash(a);
        s.put_chunk_mode(&h, a, CompressionSetting::RAW, ChunkPutMode::Create)
            .await
            .unwrap();
        crate::gc::publish_condemned(&inner, vec![h.to_hex()], 1)
            .await
            .unwrap();
        // Observed, so a later identity match would otherwise count.
        let _ = s.chunk_durable(&h).await.unwrap();
        for mode in [ChunkPutMode::Create, ChunkPutMode::Probe] {
            let r = s
                .put_chunk_mode(&h, a, CompressionSetting::RAW, mode)
                .await
                .unwrap();
            assert!(!r.existed, "{mode:?}: a condemned hit is not relied on");
        }
        assert!(!s.chunk_durable(&h).await.unwrap());
        // GC deletes it after the last upload's pointer read; a writer
        // that still sees it condemned uploads again and brings it back.
        inner.delete(&layout::chunk_key(&h)).await.unwrap();
        s.put_chunk_mode(&h, a, CompressionSetting::RAW, ChunkPutMode::Create)
            .await
            .unwrap();
        assert_eq!(s.get_chunk(&h).await.unwrap(), a);
    }

    /// The window the post-hit read opens (`CondemnedView`'s doc): the
    /// create finds the chunk, then — before the pointer read — a round
    /// that condemned it deletes it and a newer round publishes a list
    /// without it. The read sees a pointer that moved since the snapshot
    /// taken before the create, so the hit is not relied on: the HEAD
    /// asked after it finds nothing and the bytes go up again — the chunk
    /// exists when the put returns.
    #[tokio::test]
    async fn a_pointer_that_moved_around_the_hit_is_not_relied_on() {
        use object_store::path::Path;
        #[derive(Debug)]
        struct RaceStore {
            inner: Arc<InMemory>,
            chunk: Path,
            fired: std::sync::atomic::AtomicBool,
        }
        impl std::fmt::Display for RaceStore {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "RaceStore")
            }
        }
        #[async_trait::async_trait]
        impl ObjectStore for RaceStore {
            async fn put_opts(
                &self,
                location: &Path,
                payload: PutPayload,
                opts: PutOptions,
            ) -> object_store::Result<object_store::PutResult> {
                self.inner.put_opts(location, payload, opts).await
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
                let armed = location.as_ref().contains("condemned")
                    && !options.head
                    && self.inner.head(&self.chunk).await.is_ok();
                if armed && !self.fired.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    // Round 2 deletes the chunk; round 3 publishes a list
                    // without it — all between the hit and this read.
                    self.inner.delete(&self.chunk).await?;
                    let store: Arc<dyn ObjectStore> = self.inner.clone();
                    crate::gc::publish_condemned(&store, vec![], 3)
                        .await
                        .expect("round 3 publishes");
                }
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
        for mode in [ChunkPutMode::Create, ChunkPutMode::Probe] {
            let a = b"deleted-behind-the-hit";
            let hash = ChunkStore::new(Arc::new(InMemory::new())).hash(a);
            let inner = Arc::new(InMemory::new());
            let race = Arc::new(RaceStore {
                inner: inner.clone(),
                chunk: layout::chunk_key(&hash),
                fired: std::sync::atomic::AtomicBool::new(true),
            });
            let s = ChunkStore::new(race.clone());
            s.put_chunk_mode(&hash, a, CompressionSetting::RAW, ChunkPutMode::Create)
                .await
                .unwrap();
            let dyn_inner: Arc<dyn ObjectStore> = inner.clone();
            // Round 1 (an unrelated list), observed by this store.
            crate::gc::publish_condemned(&dyn_inner, vec![], 1)
                .await
                .unwrap();
            assert!(s.chunk_durable(&hash).await.unwrap());
            // Round 2 condemns it (unobserved), then the race arms.
            crate::gc::publish_condemned(&dyn_inner, vec![hash.to_hex()], 2)
                .await
                .unwrap();
            race.fired.store(false, std::sync::atomic::Ordering::SeqCst);
            let r = s
                .put_chunk_mode(&hash, a, CompressionSetting::RAW, mode)
                .await
                .unwrap();
            assert!(race.fired.load(std::sync::atomic::Ordering::SeqCst));
            assert!(
                !r.existed,
                "{mode:?}: relied on a hit the pointer moved around"
            );
            assert_eq!(s.get_chunk(&hash).await.unwrap(), a, "{mode:?}");
        }
    }

    /// The OVH run: after `fs create`, a `GET` of `meta.json` answered
    /// 404 for minutes while a `HEAD` found it. `load_fs` believes the
    /// 404 only when a `HEAD` agrees; otherwise it retries the `GET`.
    #[tokio::test(start_paused = true)]
    async fn a_get_404_that_a_head_contradicts_is_retried_not_believed() {
        use crate::faulty::{Calls, Fault, FaultyStore, OpKind};
        let faulty = FaultyStore::new();
        let s = ChunkStore::new(faulty.clone());
        s.create_fs(&FsMeta::new(DEFAULT_CHUNK_SIZE, "raw"))
            .await
            .unwrap();
        faulty.script(
            OpKind::Get,
            "meta.json",
            Calls::First(3),
            Fault::Status(404),
        );
        let meta = s
            .load_fs_waiting(std::time::Duration::from_secs(60))
            .await
            .expect("read once the GET caught up");
        assert_eq!(meta.chunk_size, DEFAULT_CHUNK_SIZE);
        assert_eq!(faulty.calls(OpKind::Get, "meta.json"), 4);
        assert!(faulty.calls(OpKind::Head, "meta.json") >= 1);
        assert!(s.fs_exists().await.unwrap());
    }

    /// A 404 that a `HEAD` confirms is "no filesystem", at once.
    #[tokio::test(start_paused = true)]
    async fn a_get_404_that_a_head_confirms_is_no_filesystem() {
        use crate::faulty::{FaultyStore, OpKind};
        let faulty = FaultyStore::new();
        let s = ChunkStore::new(faulty.clone());
        let started = tokio::time::Instant::now();
        assert!(matches!(
            s.load_fs_waiting(std::time::Duration::from_secs(60)).await,
            Err(StoreError::NotFound)
        ));
        assert!(started.elapsed() < std::time::Duration::from_millis(100));
        assert_eq!(faulty.calls(OpKind::Get, "meta.json"), 1);
        assert_eq!(faulty.calls(OpKind::Head, "meta.json"), 1);
        assert!(!s.fs_exists().await.unwrap());
    }

    /// The OVH campaigns' "mount lag": a `mount` run without the OVH
    /// profile asked AWS, which has no such bucket — `GET` and `HEAD` of
    /// `meta.json` 404, and the one `LIST` that follows says why: the
    /// bucket is not there, so the command reached the wrong store.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_missing_bucket_is_told_apart_from_a_missing_filesystem() {
        use crate::scripted_http::{methods, real_s3, scripted_s3_replies, Reply};
        let no_bucket = Reply::with_body(
            404,
            "<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not \
             exist</Message><BucketName>b</BucketName></Error>",
        );
        let (endpoint, seen) = scripted_s3_replies(vec![no_bucket]).await;
        let s = ChunkStore::new(real_s3(&endpoint));
        assert!(matches!(s.load_fs().await, Err(StoreError::NotFound)));
        assert_eq!(s.locate_missing_fs().await, MissingFs::NoBucket);
        assert_eq!(methods(&seen), vec!["GET", "HEAD", "GET"]);
        assert!(seen.lock().unwrap()[2].contains("list-type=2"));
        assert!(MissingFs::NoBucket.to_string().contains("different store"));
    }

    /// A bucket that is there answers the `LIST`: empty, or holding
    /// objects but no `meta.json`.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_present_bucket_without_meta_json_says_what_the_prefix_holds() {
        use crate::scripted_http::{real_s3, scripted_s3_replies, Reply};
        let empty = Reply::with_body(
            200,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>b</Name>\
             <Prefix></Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys>\
             <IsTruncated>false</IsTruncated></ListBucketResult>",
        );
        let (endpoint, _) = scripted_s3_replies(vec![empty]).await;
        let s = ChunkStore::new(real_s3(&endpoint));
        assert_eq!(s.locate_missing_fs().await, MissingFs::EmptyPrefix);

        let mem = ChunkStore::new(Arc::new(InMemory::new()));
        assert_eq!(mem.locate_missing_fs().await, MissingFs::EmptyPrefix);
        mem.inner()
            .put(
                &object_store::path::Path::from("log/0001"),
                PutPayload::from_static(b"x"),
            )
            .await
            .unwrap();
        assert!(matches!(mem.load_fs().await, Err(StoreError::NotFound)));
        assert_eq!(
            mem.locate_missing_fs().await,
            MissingFs::NoMeta {
                sample: "log/0001".into()
            }
        );
    }

    /// A `GET` that never catches up fails after the wait with an error
    /// that names the inconsistency, not "no filesystem".
    #[tokio::test(start_paused = true)]
    async fn a_get_that_never_catches_up_names_the_inconsistency() {
        use crate::faulty::{Calls, Fault, FaultyStore, OpKind};
        let faulty = FaultyStore::new();
        let s = ChunkStore::new(faulty.clone());
        s.create_fs(&FsMeta::new(DEFAULT_CHUNK_SIZE, "raw"))
            .await
            .unwrap();
        faulty.script(OpKind::Get, "meta.json", Calls::Every, Fault::Status(404));
        let err = s
            .load_fs_waiting(std::time::Duration::from_secs(30))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, StoreError::Meta(m) if m.contains("inconsistent")),
            "{err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_encoder_keeps_its_permit_until_blocking_work_stops() {
        let store = Arc::new(ChunkStore::with_encode_concurrency(
            Arc::new(InMemory::new()),
            None,
            1,
        ));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let task_store = store.clone();
        let task = tokio::spawn(async move {
            let permit = task_store.encode_permit().await?;
            ChunkStore::run_encoder(permit, move || {
                let _ = started_tx.send(());
                release_rx.recv().unwrap();
                Ok(())
            })
            .await
        });
        started_rx.await.unwrap();

        task.abort();
        tokio::task::yield_now().await;
        assert!(
            store.encode_gate.clone().try_acquire_owned().is_err(),
            "cancelling the async waiter must not release a running encoder's permit"
        );

        release_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if store.encode_gate.available_permits() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
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
        // Second create refused. A real second attempt carries its own
        // fresh uuid/gossip secret (`FsMeta::default()` randomizes both),
        // so use a distinct instance rather than replaying the first
        // one's exact bytes — otherwise this would exercise the "our own
        // create landed behind a 412" recognition instead of a genuine
        // conflict between two different filesystems.
        assert!(matches!(
            s.create_fs(&FsMeta::default()).await,
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

    /// A `meta.json` whose chunk size no `fs create` could have written is
    /// refused on load rather than trusted into every `/ chunk_size`.
    #[tokio::test]
    async fn meta_json_with_an_invalid_chunk_size_is_refused() {
        for chunk_size in ["0", "4294967295", "3145728"] {
            let s = store();
            let bad = format!(
                r#"{{"uuid":"3f2504e0-4f89-41d3-9a0c-0305e82c3301","format_version":1,
                "chunk_size":{chunk_size},"compression":"zstd:3","e2e":false,"created_unix":1}}"#
            );
            s.inner()
                .put(
                    &object_store::path::Path::from("meta.json"),
                    PutPayload::from(bad.into_bytes()),
                )
                .await
                .unwrap();
            assert!(
                matches!(s.load_fs().await, Err(StoreError::Meta(_))),
                "chunk_size {chunk_size} must not load"
            );
        }
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
    async fn chunk_streams_to_writer() {
        let s = store();
        let data = vec![9u8; 100_000];
        let hash = ChunkHash::of(&data);
        s.put_chunk(&hash, &data, CompressionSetting::zstd(3).unwrap())
            .await
            .unwrap();
        let mut out = Vec::new();
        let (bytes, _, _) = s.get_chunk_to_writer(&hash, &mut out).await.unwrap();
        assert_eq!(bytes, data.len() as u64);
        assert_eq!(out, data);
    }

    #[tokio::test]
    async fn e2e_chunk_streams_ciphertext_via_spill() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let keys = Arc::new(crate::e2e::E2eKeys::generate());
        let s = ChunkStore::new_e2e(inner, keys.clone());
        let data = vec![7u8; 100_000];
        let hash = s.hash(&data);
        s.put_chunk(&hash, &data, CompressionSetting::zstd(3).unwrap())
            .await
            .unwrap();

        let dir = tempfile::TempDir::new().unwrap();
        let cache = constellation_fs_core::cache::DiskCache::open_keyed(
            dir.path(),
            1 << 20,
            *keys.addressing_key(),
        )
        .unwrap();
        let mut cipher = cache.begin_spill().unwrap();
        let mut plain = cache.begin_spill().unwrap();
        let (bytes, _, _) = s
            .get_chunk_to_writer_e2e(&hash, &mut cipher, &mut plain, DecodePriority::Demand)
            .await
            .unwrap();
        assert_eq!(bytes, data.len() as u64);
        cache
            .commit_spill(
                &hash,
                plain,
                constellation_fs_core::cache::ChunkState::Clean,
            )
            .unwrap();
        assert_eq!(cache.get(&hash).unwrap(), Some(data));
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
    async fn preflight_passes_and_cleans_up_on_capable_backend() {
        let s = store();
        let checks = s.preflight().await;
        assert!(!checks.is_empty());
        for c in &checks {
            assert!(c.outcome.is_ok(), "{} should pass: {:?}", c.name, c.outcome);
        }
        // Every scratch object must be removed afterward.
        let leftover = s
            .inner()
            .list(Some(&object_store::path::Path::from(".preflight")))
            .collect::<Vec<_>>()
            .await;
        assert!(leftover.is_empty(), "preflight left scratch objects behind");
    }

    #[tokio::test]
    async fn epoch_slack_is_absent_by_default_and_set_by_cas() {
        let s = store();
        let meta = FsMeta::default();
        assert_eq!(meta.epoch_slack(), 0);
        let json = serde_json::to_string(&meta).unwrap();
        assert!(
            !json.contains("epoch_slack"),
            "f = 0 stays out of meta.json"
        );
        assert!(matches!(
            s.set_epoch_slack(1).await,
            Err(StoreError::NotFound)
        ));
        s.create_fs(&meta).await.unwrap();
        assert_eq!(s.set_epoch_slack(1).await.unwrap(), 0);
        let loaded = s.load_fs().await.unwrap();
        assert_eq!(loaded.epoch_slack(), 1);
        assert_eq!(loaded.uuid, meta.uuid);
        assert_eq!(s.set_epoch_slack(0).await.unwrap(), 1);
        assert_eq!(s.load_fs().await.unwrap().epoch_slack, None);
        // A meta.json written before the field existed decodes as 0.
        let old = r#"{"uuid":"00000000-0000-0000-0000-000000000000","format_version":1,
            "chunk_size":1048576,"compression":"raw","e2e":false,"created_unix":0}"#;
        let parsed: FsMeta = serde_json::from_str(old).unwrap();
        assert_eq!(parsed.epoch_slack(), 0);
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
