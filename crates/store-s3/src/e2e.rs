//! End-to-end key management and authenticated encryption.
//!
//! An E2E filesystem stores exactly one secret on S3: a random 32-byte
//! **master key (KMK)**, wrapped under a memory-hard Argon2id key derived
//! from the passphrase and kept inside `meta.json` (there is no separate
//! keyring object). Every other key is *derived* from the KMK, never
//! stored:
//!
//! * the **addressing key** (keyed chunk-hash identity, DESIGN §8),
//! * a **per-partition DEK** for log/chunk encryption, and
//! * the **gossip topic seed** for the P2P fast path.
//!
//! Two consequences follow. Changing the passphrase rewraps only the KMK
//! envelope — chunk identities, DEKs, and the gossip topic are unchanged,
//! so `fs passwd` never rotates data keys and never forces a remount. And
//! a partition split needs no key persistence at all: any node holding
//! the KMK derives the new partition's DEK locally. Registry, lease,
//! designation, heartbeat, and pointer objects intentionally remain
//! plaintext because they carry coordination data, not filenames or file
//! contents.

use argon2::{Algorithm, Argon2, Params, Version};
use chacha20poly1305::{
    aead::{Aead, Payload},
    KeyInit, XChaCha20Poly1305, XNonce,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use zeroize::Zeroize;

use crate::error::StoreError;

const ENVELOPE_VERSION: u8 = 1;
const SALT_LEN: usize = 16;
const NONCE_LEN: usize = 24;
const KEY_LEN: usize = 32;

/// Application-scoped prefix on every derivation message, so a KMK reused
/// (by mistake) in another context cannot produce the same subkeys.
const KDF_DOMAIN: &[u8] = b"constellation/e2e/v1/";
const PURPOSE_ADDRESSING: u8 = 0x01;
const PURPOSE_GOSSIP: u8 = 0x02;
const PURPOSE_DEK: u8 = 0x03;

/// OWASP's memory-constrained Argon2id profile: 19 MiB, two iterations,
/// one lane. Parameters are persisted so stronger future defaults do not
/// make existing filesystems unreadable.
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

/// The E2E secret block embedded in `meta.json` (present iff `e2e`). It
/// holds only the wrapped master key and the parameters needed to unwrap
/// it; every usable key is derived from the KMK at runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyringBlock {
    pub argon2_params: Argon2Params,
    pub salt: String,
    /// KMK sealed with XChaCha20-Poly1305 under the Argon2id KEK, AAD
    /// `b"keyring-master"`. Hex.
    pub wrapped_master: String,
}

/// A key's purpose, for domain-separated derivation from the KMK.
#[derive(Debug, Clone, Copy)]
pub enum KeyPurpose<'a> {
    Addressing,
    Gossip,
    Dek(&'a str),
}

/// Derive a subkey from the master key. Domain separation is collision-free
/// by construction: every message is `KDF_DOMAIN` then a one-byte purpose
/// tag, and only a DEK appends further bytes (the partition name). The
/// fixed purposes therefore produce length-`|domain|+1` messages with
/// distinct tags, and a DEK message carries the distinct `PURPOSE_DEK` tag
/// followed by the name — so no partition name can make a DEK message equal
/// a fixed-purpose message, and the name→DEK map is injective.
fn derive_from_master(master: &[u8; KEY_LEN], purpose: KeyPurpose) -> [u8; KEY_LEN] {
    let mut msg = Vec::with_capacity(KDF_DOMAIN.len() + 1);
    msg.extend_from_slice(KDF_DOMAIN);
    match purpose {
        KeyPurpose::Addressing => msg.push(PURPOSE_ADDRESSING),
        KeyPurpose::Gossip => msg.push(PURPOSE_GOSSIP),
        KeyPurpose::Dek(partition) => {
            msg.push(PURPOSE_DEK);
            msg.extend_from_slice(partition.as_bytes());
        }
    }
    let out = *blake3::keyed_hash(master, &msg).as_bytes();
    msg.zeroize();
    out
}

/// Unwrapped filesystem secrets. The allocations are page-locked on a
/// best-effort basis. Containers and unprivileged hosts often have a small
/// RLIMIT_MEMLOCK; failure is observable but does not make a filesystem
/// unavailable. The hot, fixed subkeys (addressing key, gossip seed) are
/// precomputed; per-partition DEKs are derived on demand (a keyed BLAKE3
/// hash, sub-microsecond).
pub struct E2eKeys {
    master_key: Box<[u8; KEY_LEN]>,
    addressing_key: Box<[u8; KEY_LEN]>,
    gossip_secret: Box<[u8; KEY_LEN]>,
    locked: bool,
}

impl std::fmt::Debug for E2eKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("E2eKeys")
            .field("locked", &self.locked)
            .finish_non_exhaustive()
    }
}

impl E2eKeys {
    /// A fresh filesystem's keys, from a random master. Used by `fs
    /// create` and tests; the master is wrapped separately (see
    /// [`create_keyring_block`]).
    pub fn generate() -> Self {
        Self::from_master(Box::new(rand::random()))
    }

    fn from_master(master_key: Box<[u8; KEY_LEN]>) -> Self {
        let addressing_key = Box::new(derive_from_master(&master_key, KeyPurpose::Addressing));
        let gossip_secret = Box::new(derive_from_master(&master_key, KeyPurpose::Gossip));
        let mut keys = Self {
            master_key,
            addressing_key,
            gossip_secret,
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
        region::lock(self.master_key.as_ptr(), self.master_key.len()).is_ok()
            && region::lock(self.addressing_key.as_ptr(), self.addressing_key.len()).is_ok()
            && region::lock(self.gossip_secret.as_ptr(), self.gossip_secret.len()).is_ok()
    }

    pub fn addressing_key(&self) -> &[u8; KEY_LEN] {
        &self.addressing_key
    }

    /// The P2P gossip topic seed for this filesystem. Derived from the
    /// KMK, so only a passphrase holder can compute it and join the topic.
    pub fn gossip_secret(&self) -> &[u8; KEY_LEN] {
        &self.gossip_secret
    }

    /// The data-encryption key for `partition`. Derived, never stored, so
    /// it is always available with no keyring read — a partition split
    /// needs no key coordination.
    pub fn dek(&self, partition: &str) -> [u8; KEY_LEN] {
        derive_from_master(&self.master_key, KeyPurpose::Dek(partition))
    }

    pub fn hash(&self, plaintext: &[u8]) -> constellation_fs_core::ChunkHash {
        constellation_fs_core::ChunkHash::keyed(self.addressing_key(), plaintext)
    }
}

impl Drop for E2eKeys {
    fn drop(&mut self) {
        if self.locked {
            let _ = unsafe { region::unlock(self.master_key.as_ptr(), self.master_key.len()) };
            let _ =
                unsafe { region::unlock(self.addressing_key.as_ptr(), self.addressing_key.len()) };
            let _ =
                unsafe { region::unlock(self.gossip_secret.as_ptr(), self.gossip_secret.len()) };
        }
        self.master_key.zeroize();
        self.addressing_key.zeroize();
        self.gossip_secret.zeroize();
    }
}

pub type SharedE2eKeys = Arc<E2eKeys>;

/// Mint a fresh master key and return the block to embed in `meta.json`.
/// The KMK is wrapped under the passphrase and then dropped — the caller
/// (mount) re-derives the live keys with [`unlock`].
pub fn create_keyring_block(passphrase: &str) -> Result<KeyringBlock, StoreError> {
    let mut master: [u8; KEY_LEN] = rand::random();
    let block = seal_master(&master, passphrase, Argon2Params::default());
    master.zeroize();
    block
}

/// Unwrap the master key from a `meta.json` keyring block and derive the
/// live keys. No S3 access — the block is already in the loaded meta.
pub fn unlock(block: &KeyringBlock, passphrase: &str) -> Result<SharedE2eKeys, StoreError> {
    let mut master = open_master(block, passphrase)?;
    let keys = E2eKeys::from_master(Box::new(master));
    master.zeroize();
    Ok(Arc::new(keys))
}

/// Rewrap the master under a new passphrase, preserving its value. Every
/// derived key (addressing, DEKs, gossip seed) is therefore unchanged;
/// only the KEK envelope moves. The caller CAS-writes the returned block
/// into `meta.json`.
pub fn rewrap_master(
    block: &KeyringBlock,
    old: &str,
    new: &str,
) -> Result<KeyringBlock, StoreError> {
    let mut master = open_master(block, old)?;
    let out = seal_master(&master, new, Argon2Params::default());
    master.zeroize();
    out
}

fn seal_master(
    master: &[u8; KEY_LEN],
    passphrase: &str,
    params: Argon2Params,
) -> Result<KeyringBlock, StoreError> {
    let salt: [u8; SALT_LEN] = rand::random();
    let mut kek = derive_key(passphrase, &salt, params)?;
    let wrapped = encrypt_envelope(&kek, b"keyring-master", master);
    kek.zeroize();
    Ok(KeyringBlock {
        argon2_params: params,
        salt: hex(&salt),
        wrapped_master: hex(&wrapped?),
    })
}

fn open_master(block: &KeyringBlock, passphrase: &str) -> Result<[u8; KEY_LEN], StoreError> {
    let salt: [u8; SALT_LEN] = unhex(&block.salt)?
        .try_into()
        .map_err(|_| StoreError::CorruptObject("keyring salt has wrong length".into()))?;
    let mut kek = derive_key(passphrase, &salt, block.argon2_params)?;
    let opened = decrypt_envelope(&kek, b"keyring-master", &unhex(&block.wrapped_master)?);
    kek.zeroize();
    array32(&opened?)
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

    #[test]
    fn derivation_is_deterministic_and_key_specific() {
        let k = E2eKeys::generate();
        // Deterministic per master.
        assert_eq!(k.dek("p0"), k.dek("p0"));
        // Purposes are distinct from one another.
        assert_ne!(k.addressing_key().as_slice(), k.gossip_secret().as_slice());
        assert_ne!(k.addressing_key().as_slice(), k.dek("p0").as_slice());
        assert_ne!(k.gossip_secret().as_slice(), k.dek("p0").as_slice());
        // A different master changes every derived key.
        let other = E2eKeys::generate();
        assert_ne!(k.addressing_key(), other.addressing_key());
        assert_ne!(k.gossip_secret(), other.gossip_secret());
        assert_ne!(k.dek("p0"), other.dek("p0"));
    }

    /// The one new failure mode option A introduces: a partition name that
    /// collides with a fixed purpose or another partition. Guard it by
    /// construction and here.
    #[test]
    fn key_purposes_never_collide() {
        let k = E2eKeys::generate();
        let names: Vec<String> = vec![
            "".into(),
            "p0".into(),
            "p1".into(),
            "addressing".into(),
            "gossip".into(),
            "dek".into(),
            "dek:p0".into(),
            "\u{1}".into(), // a raw tag byte as a name
            "\u{2}".into(),
            "\u{3}".into(),
            "a/b".into(),
            "p".repeat(300),
        ];
        let mut seen = std::collections::HashSet::new();
        assert!(seen.insert(*k.addressing_key()));
        assert!(seen.insert(*k.gossip_secret()));
        for name in &names {
            assert!(
                seen.insert(k.dek(name)),
                "derived-key collision for partition {name:?}"
            );
        }
        assert_ne!(k.dek("p1"), k.dek("p2"));
    }

    #[test]
    fn unlock_roundtrip_and_wrong_passphrase() {
        let block = create_keyring_block("correct horse").unwrap();
        let a = unlock(&block, "correct horse").unwrap();
        let b = unlock(&block, "correct horse").unwrap();
        assert_eq!(a.addressing_key(), b.addressing_key());
        assert_eq!(a.dek("p0"), b.dek("p0"));
        assert_eq!(a.gossip_secret(), b.gossip_secret());
        assert!(unlock(&block, "wrong battery").is_err());
    }

    /// The headline property: a passphrase change is a pure envelope
    /// rewrap. A key handle unwrapped *before* the change keeps deriving
    /// identical DEKs — including for partitions that do not exist yet —
    /// so a live node needs no remount.
    #[test]
    fn passwd_rewraps_master_keeping_all_derived_keys() {
        let block = create_keyring_block("old-pass").unwrap();
        let before = unlock(&block, "old-pass").unwrap();

        let new_block = rewrap_master(&block, "old-pass", "new-pass").unwrap();
        assert!(unlock(&new_block, "old-pass").is_err());
        let after = unlock(&new_block, "new-pass").unwrap();

        assert_eq!(after.addressing_key(), before.addressing_key());
        assert_eq!(after.gossip_secret(), before.gossip_secret());
        assert_eq!(after.dek("p0"), before.dek("p0"));
        // A partition that did not exist when the passphrase changed.
        assert_eq!(
            after.dek("part-created-later"),
            before.dek("part-created-later")
        );
    }

    #[test]
    fn keyring_block_hides_secrets() {
        let block = create_keyring_block("pass").unwrap();
        let keys = unlock(&block, "pass").unwrap();
        let json = serde_json::to_vec(&block).unwrap();
        for secret in [
            keys.addressing_key().as_slice(),
            keys.gossip_secret().as_slice(),
            keys.dek("p0").as_slice(),
        ] {
            assert!(
                !json.windows(secret.len()).any(|w| w == secret),
                "a raw secret leaked into the keyring block"
            );
        }
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
}
