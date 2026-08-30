//! End-to-end key management and authenticated encryption.
//!
//! The keyring is the only bucket object protected by the passphrase.  A
//! memory-hard Argon2id derivation produces a wrapping key; changing the
//! passphrase therefore rewrites only this small object and never changes
//! chunk identities or data-encryption keys.  User-content objects use a
//! separate per-partition DEK.  Registry, lease, designation, heartbeat,
//! and pointer objects intentionally remain plaintext because they carry
//! coordination data rather than filenames or file contents.

use argon2::{Algorithm, Argon2, Params, Version};
use chacha20poly1305::{
    aead::{Aead, Payload},
    KeyInit, XChaCha20Poly1305, XNonce,
};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
use zeroize::Zeroize;

use crate::error::StoreError;

const KEYRING_KEY: &str = "keys/keyring.json";
const KEYRING_VERSION: u8 = 1;
const ENVELOPE_VERSION: u8 = 1;
const SALT_LEN: usize = 16;
const NONCE_LEN: usize = 24;
const KEY_LEN: usize = 32;

/// OWASP's memory-constrained Argon2id profile: 19 MiB, two iterations,
/// one lane. Parameters are persisted so stronger future defaults do not
/// make existing keyrings unreadable.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct Argon2Params {
    pub m_cost_kib: u32,
    pub t_cost: u32,
    pub p_cost: u32,
}

impl Default for Argon2Params {
    fn default() -> Self {
        Self {
            m_cost_kib: 19_456,
            t_cost: 2,
            p_cost: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WrappedKeys {
    pub addressing_key: String,
    pub deks: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Keyring {
    pub v: u8,
    pub argon2_params: Argon2Params,
    pub salt: String,
    pub wrapped: WrappedKeys,
}

/// Unwrapped filesystem secrets.  The allocation is page-locked on a
/// best-effort basis. Containers and unprivileged hosts often have a small
/// RLIMIT_MEMLOCK; failure is observable but does not make a filesystem
/// unavailable.
pub struct E2eKeys {
    addressing_key: Box<[u8; KEY_LEN]>,
    deks: RwLock<BTreeMap<String, Box<[u8; KEY_LEN]>>>,
    wrapping_key: Box<[u8; KEY_LEN]>,
    salt: [u8; SALT_LEN],
    params: Argon2Params,
    locked: bool,
}

impl std::fmt::Debug for E2eKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("E2eKeys")
            .field(
                "partitions",
                &self.deks.read().unwrap().keys().collect::<Vec<_>>(),
            )
            .field("locked", &self.locked)
            .finish_non_exhaustive()
    }
}

impl E2eKeys {
    pub fn generate() -> Self {
        let mut deks = BTreeMap::new();
        deks.insert("p0".into(), Box::new(rand::random()));
        Self::new(
            Box::new(rand::random()),
            deks,
            Box::new([0; KEY_LEN]),
            [0; SALT_LEN],
            Argon2Params::default(),
        )
    }

    fn new(
        addressing_key: Box<[u8; KEY_LEN]>,
        deks: BTreeMap<String, Box<[u8; KEY_LEN]>>,
        wrapping_key: Box<[u8; KEY_LEN]>,
        salt: [u8; SALT_LEN],
        params: Argon2Params,
    ) -> Self {
        let mut keys = Self {
            addressing_key,
            deks: RwLock::new(deks),
            wrapping_key,
            salt,
            params,
            locked: false,
        };
        keys.locked = keys.try_lock();
        if !keys.locked {
            tracing::warn!(
                "could not mlock E2E keys; continuing with secrets in ordinary process memory"
            );
        }
        keys
    }

    fn try_lock(&self) -> bool {
        let addressing = region::lock(self.addressing_key.as_ptr(), self.addressing_key.len());
        if addressing.is_err() {
            return false;
        }
        if region::lock(self.wrapping_key.as_ptr(), self.wrapping_key.len()).is_err() {
            return false;
        }
        for dek in self.deks.read().unwrap().values() {
            if region::lock(dek.as_ptr(), dek.len()).is_err() {
                return false;
            }
        }
        true
    }

    pub fn addressing_key(&self) -> &[u8; KEY_LEN] {
        &self.addressing_key
    }

    pub fn dek(&self, partition: &str) -> Result<[u8; KEY_LEN], StoreError> {
        self.deks
            .read()
            .unwrap()
            .get(partition)
            .map(|key| **key)
            .ok_or_else(|| StoreError::Meta(format!("keyring has no DEK for {partition}")))
    }

    fn insert_partition(&self, partition: &str, key: [u8; KEY_LEN]) -> bool {
        let mut deks = self.deks.write().unwrap();
        if deks.contains_key(partition) {
            return false;
        }
        let boxed = Box::new(key);
        if region::lock(boxed.as_ptr(), boxed.len()).is_err() {
            tracing::warn!(partition, "could not mlock new partition DEK");
        }
        deks.insert(partition.to_string(), boxed);
        true
    }

    pub fn hash(&self, plaintext: &[u8]) -> constellation_fs_core::ChunkHash {
        constellation_fs_core::ChunkHash::keyed(self.addressing_key(), plaintext)
    }

    /// Create and persist a random DEK before a new partition becomes
    /// writable. Partition creation is lease-serialized; rewriting the small
    /// keyring first makes a crash leave an unused key, never an unreadable
    /// log stream.
    pub async fn ensure_partition(
        &self,
        store: &Arc<dyn ObjectStore>,
        partition: &str,
    ) -> Result<(), StoreError> {
        if !self.insert_partition(partition, rand::random()) {
            return Ok(());
        }
        let path = object_store::path::Path::from(KEYRING_KEY);
        loop {
            let object = store.get(&path).await?;
            let version = UpdateVersion {
                e_tag: object.meta.e_tag.clone(),
                version: object.meta.version.clone(),
            };
            let remote: Keyring = serde_json::from_slice(&object.bytes().await?)?;
            for (remote_partition, wrapped) in remote.wrapped.deks {
                if self.deks.read().unwrap().contains_key(&remote_partition) {
                    continue;
                }
                let key = decrypt_envelope(
                    &self.wrapping_key,
                    remote_partition.as_bytes(),
                    &unhex(&wrapped)?,
                )?;
                self.insert_partition(&remote_partition, array32(&key)?);
            }
            let ring = wrap_with_key(self, &self.wrapping_key, self.salt, self.params)?;
            let options = PutOptions::from(PutMode::Update(version));
            match store
                .put_opts(
                    &path,
                    PutPayload::from(serde_json::to_vec_pretty(&ring)?),
                    options,
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Refresh one DEK added by another node's partition split. The retained
    /// wrapping key decrypts the updated keyring without retaining the user's
    /// passphrase.
    pub async fn refresh_partition(
        &self,
        store: &Arc<dyn ObjectStore>,
        partition: &str,
    ) -> Result<(), StoreError> {
        if self.deks.read().unwrap().contains_key(partition) {
            return Ok(());
        }
        let object = store
            .get(&object_store::path::Path::from(KEYRING_KEY))
            .await?;
        let ring: Keyring = serde_json::from_slice(&object.bytes().await?)?;
        let wrapped = ring
            .wrapped
            .deks
            .get(partition)
            .ok_or_else(|| StoreError::Meta(format!("keyring has no DEK for {partition}")))?;
        let key = decrypt_envelope(&self.wrapping_key, partition.as_bytes(), &unhex(wrapped)?)?;
        self.insert_partition(partition, array32(&key)?);
        Ok(())
    }
}

impl Drop for E2eKeys {
    fn drop(&mut self) {
        if self.locked {
            let _ =
                unsafe { region::unlock(self.addressing_key.as_ptr(), self.addressing_key.len()) };
            let _ = unsafe { region::unlock(self.wrapping_key.as_ptr(), self.wrapping_key.len()) };
            for dek in self.deks.get_mut().unwrap().values() {
                let _ = unsafe { region::unlock(dek.as_ptr(), dek.len()) };
            }
        }
        self.addressing_key.zeroize();
        self.wrapping_key.zeroize();
        for dek in self.deks.get_mut().unwrap().values_mut() {
            dek.zeroize();
        }
    }
}

pub type SharedE2eKeys = Arc<E2eKeys>;

pub async fn put_keyring(
    store: &Arc<dyn ObjectStore>,
    passphrase: &str,
) -> Result<SharedE2eKeys, StoreError> {
    let params = Argon2Params::default();
    let salt: [u8; SALT_LEN] = rand::random();
    let wrapping_key = derive_key(passphrase, &salt, params)?;
    let mut deks = BTreeMap::new();
    deks.insert("p0".into(), Box::new(rand::random()));
    let keys = E2eKeys::new(
        Box::new(rand::random()),
        deks,
        Box::new(wrapping_key),
        salt,
        params,
    );
    let ring = wrap_with_key(&keys, &wrapping_key, salt, params)?;
    store
        .put(
            &object_store::path::Path::from(KEYRING_KEY),
            PutPayload::from(serde_json::to_vec_pretty(&ring)?),
        )
        .await?;
    Ok(Arc::new(keys))
}

pub async fn load_keyring(
    store: &Arc<dyn ObjectStore>,
    passphrase: &str,
) -> Result<SharedE2eKeys, StoreError> {
    let object = store
        .get(&object_store::path::Path::from(KEYRING_KEY))
        .await?;
    let ring: Keyring = serde_json::from_slice(&object.bytes().await?)?;
    Ok(Arc::new(unwrap(&ring, passphrase)?))
}

pub async fn change_passphrase(
    store: &Arc<dyn ObjectStore>,
    old: &str,
    new: &str,
) -> Result<(), StoreError> {
    let path = object_store::path::Path::from(KEYRING_KEY);
    let object = store.get(&path).await?;
    let version = UpdateVersion {
        e_tag: object.meta.e_tag.clone(),
        version: object.meta.version.clone(),
    };
    let old_ring: Keyring = serde_json::from_slice(&object.bytes().await?)?;
    let keys = unwrap(&old_ring, old)?;
    let ring = wrap(&keys, new, Argon2Params::default())?;
    store
        .put_opts(
            &path,
            PutPayload::from(serde_json::to_vec_pretty(&ring)?),
            PutOptions::from(PutMode::Update(version)),
        )
        .await?;
    Ok(())
}

fn derive_key(
    passphrase: &str,
    salt: &[u8],
    params: Argon2Params,
) -> Result<[u8; KEY_LEN], StoreError> {
    let params = Params::new(
        params.m_cost_kib,
        params.t_cost,
        params.p_cost,
        Some(KEY_LEN),
    )
    .map_err(|error| StoreError::Meta(format!("invalid Argon2 parameters: {error}")))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut key = [0u8; KEY_LEN];
    argon2
        .hash_password_into(passphrase.as_bytes(), salt, &mut key)
        .map_err(|error| StoreError::Meta(format!("Argon2 key derivation failed: {error}")))?;
    Ok(key)
}

fn wrap(keys: &E2eKeys, passphrase: &str, params: Argon2Params) -> Result<Keyring, StoreError> {
    let salt: [u8; SALT_LEN] = rand::random();
    let mut kek = derive_key(passphrase, &salt, params)?;
    let ring = wrap_with_key(keys, &kek, salt, params)?;
    kek.zeroize();
    Ok(ring)
}

fn wrap_with_key(
    keys: &E2eKeys,
    kek: &[u8; KEY_LEN],
    salt: [u8; SALT_LEN],
    params: Argon2Params,
) -> Result<Keyring, StoreError> {
    let addressing_key = encrypt_envelope(kek, b"addressing-key", keys.addressing_key())?;
    let mut deks = BTreeMap::new();
    for (partition, dek) in keys.deks.read().unwrap().iter() {
        deks.insert(
            partition.clone(),
            encrypt_envelope(kek, partition.as_bytes(), dek.as_ref())?,
        );
    }
    Ok(Keyring {
        v: KEYRING_VERSION,
        argon2_params: params,
        salt: hex(&salt),
        wrapped: WrappedKeys {
            addressing_key: hex(&addressing_key),
            deks: deks
                .into_iter()
                .map(|(partition, bytes)| (partition, hex(&bytes)))
                .collect(),
        },
    })
}

fn unwrap(ring: &Keyring, passphrase: &str) -> Result<E2eKeys, StoreError> {
    if ring.v != KEYRING_VERSION {
        return Err(StoreError::Meta(format!(
            "unsupported keyring version {}",
            ring.v
        )));
    }
    let salt: [u8; SALT_LEN] = unhex(&ring.salt)?
        .try_into()
        .map_err(|_| StoreError::CorruptObject("keyring salt has wrong length".into()))?;
    let kek = derive_key(passphrase, &salt, ring.argon2_params)?;
    let addressing = decrypt_envelope(
        &kek,
        b"addressing-key",
        &unhex(&ring.wrapped.addressing_key)?,
    )?;
    let mut deks = BTreeMap::new();
    for (partition, wrapped) in &ring.wrapped.deks {
        let key = decrypt_envelope(&kek, partition.as_bytes(), &unhex(wrapped)?)?;
        deks.insert(partition.clone(), Box::new(array32(&key)?));
    }
    Ok(E2eKeys::new(
        Box::new(array32(&addressing)?),
        deks,
        Box::new(kek),
        salt,
        ring.argon2_params,
    ))
}

/// Encrypt an already-compressed object. The version and random nonce make
/// the envelope self-describing; callers supply the immutable object identity
/// as AAD so ciphertext cannot be moved to another key.
pub fn encrypt_object(
    key: &[u8; KEY_LEN],
    aad: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, StoreError> {
    encrypt_envelope(key, aad, plaintext)
}

pub fn decrypt_object(
    key: &[u8; KEY_LEN],
    aad: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, StoreError> {
    decrypt_envelope(key, aad, ciphertext)
}

fn encrypt_envelope(
    key: &[u8; KEY_LEN],
    aad: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, StoreError> {
    let nonce: [u8; NONCE_LEN] = rand::random();
    let cipher = XChaCha20Poly1305::new(key.into());
    let nonce_ref = XNonce::try_from(&nonce[..])
        .map_err(|_| StoreError::CorruptObject("invalid E2E nonce".into()))?;
    let body = cipher
        .encrypt(
            &nonce_ref,
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| StoreError::CorruptObject("E2E encryption failed".into()))?;
    let mut out = Vec::with_capacity(1 + NONCE_LEN + body.len());
    out.push(ENVELOPE_VERSION);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&body);
    Ok(out)
}

fn decrypt_envelope(
    key: &[u8; KEY_LEN],
    aad: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, StoreError> {
    if ciphertext.len() < 1 + NONCE_LEN || ciphertext[0] != ENVELOPE_VERSION {
        return Err(StoreError::CorruptObject(
            "bad or truncated E2E envelope".into(),
        ));
    }
    let nonce = XNonce::try_from(&ciphertext[1..1 + NONCE_LEN])
        .map_err(|_| StoreError::CorruptObject("invalid E2E nonce".into()))?;
    XChaCha20Poly1305::new(key.into())
        .decrypt(
            &nonce,
            Payload {
                msg: &ciphertext[1 + NONCE_LEN..],
                aad,
            },
        )
        .map_err(|_| StoreError::CorruptObject("E2E authentication failed".into()))
}

fn array32(bytes: &[u8]) -> Result<[u8; KEY_LEN], StoreError> {
    bytes
        .try_into()
        .map_err(|_| StoreError::CorruptObject("wrapped key has wrong length".into()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn unhex(value: &str) -> Result<Vec<u8>, StoreError> {
    if !value.len().is_multiple_of(2) {
        return Err(StoreError::CorruptObject("invalid keyring hex".into()));
    }
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|s| u8::from_str_radix(s, 16).ok())
                .ok_or_else(|| StoreError::CorruptObject("invalid keyring hex".into()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[test]
    fn keyring_roundtrip_and_wrong_passphrase() {
        let keys = E2eKeys::generate();
        let ring = wrap(&keys, "correct horse", Argon2Params::default()).unwrap();
        let opened = unwrap(&ring, "correct horse").unwrap();
        assert_eq!(opened.addressing_key(), keys.addressing_key());
        assert_eq!(opened.dek("p0").unwrap(), keys.dek("p0").unwrap());
        assert!(unwrap(&ring, "wrong battery").is_err());
    }

    #[test]
    fn object_roundtrip_rejects_wrong_aad() {
        let key = rand::random();
        let ciphertext = encrypt_object(&key, b"hash-a", b"compressed bytes").unwrap();
        assert_eq!(
            decrypt_object(&key, b"hash-a", &ciphertext).unwrap(),
            b"compressed bytes"
        );
        assert!(decrypt_object(&key, b"hash-b", &ciphertext).is_err());
        assert!(!ciphertext
            .windows(b"compressed bytes".len())
            .any(|window| window == b"compressed bytes"));
    }

    #[tokio::test]
    async fn partition_split_persists_a_fresh_dek() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let keys = put_keyring(&store, "passphrase").await.unwrap();
        keys.ensure_partition(&store, "p7").await.unwrap();
        let reopened = load_keyring(&store, "passphrase").await.unwrap();
        assert_eq!(reopened.dek("p7").unwrap(), keys.dek("p7").unwrap());
        assert_ne!(reopened.dek("p7").unwrap(), reopened.dek("p0").unwrap());
    }

    #[tokio::test]
    async fn passphrase_change_rewraps_without_rotating_data_keys() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let before = put_keyring(&store, "old-pass").await.unwrap();
        let address = *before.addressing_key();
        let dek = before.dek("p0").unwrap();
        change_passphrase(&store, "old-pass", "new-pass")
            .await
            .unwrap();
        assert!(load_keyring(&store, "old-pass").await.is_err());
        let after = load_keyring(&store, "new-pass").await.unwrap();
        assert_eq!(*after.addressing_key(), address);
        assert_eq!(after.dek("p0").unwrap(), dek);
    }

    #[tokio::test]
    async fn concurrent_partition_additions_merge_through_cas() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put_keyring(&store, "passphrase").await.unwrap();
        let a = load_keyring(&store, "passphrase").await.unwrap();
        let b = load_keyring(&store, "passphrase").await.unwrap();
        let (ra, rb) = tokio::join!(
            a.ensure_partition(&store, "p_a"),
            b.ensure_partition(&store, "p_b")
        );
        ra.unwrap();
        rb.unwrap();
        let reopened = load_keyring(&store, "passphrase").await.unwrap();
        reopened.dek("p_a").unwrap();
        reopened.dek("p_b").unwrap();
    }
}
