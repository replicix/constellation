//! Node identity for the P2P fast path (DESIGN.md §8).
//!
//! Every host has one Ed25519 key at `~/.config/constellation/node.key`
//! (0600), **per host and not per state dir**: the key identifies the
//! machine that peers dial, while a node id identifies one mount's
//! state dir. `constellation host init` creates it; a mount generates
//! one when it is missing and logs that it did. It is a secret of the
//! host's `SecretStore` (`constellation_platform`): the file store keeps
//! it at that path, and a host without a disk for secrets (plan 37's
//! engine pods) can hold it in memory instead.
//!
//! The key is iroh's `SecretKey`, so the public half is directly the
//! endpoint id peers dial. Trust flows from the bucket, not from the
//! key: a peer is accepted only when its pubkey is listed in the node
//! registry, and writing the registry needs bucket write permission, so
//! IAM stays the trust root.

use anyhow::{Context, Result};
use constellation_platform::{FileSecretStore, HostServices, SecretStore};
use iroh::{PublicKey, SecretKey};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The key's name in the host's secret store.
pub const KEY_NAME: &str = "node.key";

/// Hex-encoded public key, as stored in the node registry.
pub type PubKeyHex = String;

/// Where the key lives: `CONSTELLATION_NODE_KEY` (a whole path) when
/// set, else [`KEY_NAME`] in `host`'s secret store (the config dir:
/// `$XDG_CONFIG_HOME/constellation/node.key`, else
/// `~/.config/constellation/node.key`), else — no `HOME` — under
/// `/etc/constellation`. Returns the store and the key's name in it.
pub fn default_key_store(host: &HostServices) -> (Arc<dyn SecretStore>, String) {
    if let Some(p) = std::env::var_os("CONSTELLATION_NODE_KEY") {
        if let Ok((store, name)) = FileSecretStore::for_path(Path::new(&p)) {
            return (Arc::new(store), name);
        }
    }
    if host.dirs.config_dir().is_ok() {
        return (host.secrets.clone(), KEY_NAME.to_string());
    }
    (
        Arc::new(FileSecretStore::new("/etc/constellation")),
        KEY_NAME.to_string(),
    )
}

/// Default key location, overridable with `CONSTELLATION_NODE_KEY`, when
/// the host keeps secrets as files (see [`default_key_store`]).
pub fn default_key_path(host: &HostServices) -> PathBuf {
    let (store, name) = default_key_store(host);
    PathBuf::from(store.describe(&name))
}

/// Load the host node key from the file at `path`, generating it if
/// absent. Returns `(key, generated)` so the caller can log a first-time
/// creation.
pub fn load_or_create(path: &Path) -> Result<(SecretKey, bool)> {
    let (store, name) = FileSecretStore::for_path(path)
        .with_context(|| format!("locating the node key at {}", path.display()))?;
    load_or_create_in(&store, &name)
}

/// Load the node key `name` from `store`, generating (and storing) it if
/// absent.
pub fn load_or_create_in(store: &dyn SecretStore, name: &str) -> Result<(SecretKey, bool)> {
    if let Some(key) = load_in(store, name)? {
        return Ok((key, false));
    }
    let key = SecretKey::generate();
    // The store writes then renames, owner-only from creation, so the
    // key is never briefly world-readable at its final name.
    let hex = constellation_platform::Secret::new(hex32(&key.to_bytes()).into_bytes());
    store
        .put(name, hex.expose())
        .with_context(|| format!("writing {}", store.describe(name)))?;
    Ok((key, true))
}

/// Load the key from the file at `path` if it exists.
pub fn load(path: &Path) -> Result<Option<SecretKey>> {
    let (store, name) = FileSecretStore::for_path(path)
        .with_context(|| format!("locating the node key at {}", path.display()))?;
    load_in(&store, &name)
}

/// Load the key `name` from `store` if it exists. A malformed key is an
/// error, not a silent regeneration: overwriting it would change this
/// host's identity and drop it out of every registry allowlist.
pub fn load_in(store: &dyn SecretStore, name: &str) -> Result<Option<SecretKey>> {
    let Some(raw) = store
        .get(name)
        .with_context(|| format!("reading {}", store.describe(name)))?
    else {
        return Ok(None);
    };
    let text = raw.expose_str().unwrap_or("");
    let bytes = decode_hex32(text.trim())
        .with_context(|| format!("{} is not a 32-byte hex node key", store.describe(name)))?;
    Ok(Some(SecretKey::from_bytes(&bytes)))
}

pub fn hex32(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

pub fn decode_hex32(s: &str) -> Result<[u8; 32]> {
    anyhow::ensure!(s.len() == 64, "expected 64 hex chars, got {}", s.len());
    let mut out = [0u8; 32];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hi = hex_val(chunk[0])?;
        let lo = hex_val(chunk[1])?;
        out[i] = hi << 4 | lo;
    }
    Ok(out)
}

fn hex_val(c: u8) -> Result<u8> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => anyhow::bail!("invalid hex digit {:?}", c as char),
    }
}

/// Hex form of a public key, as it appears in the registry.
pub fn pubkey_hex(key: &PublicKey) -> PubKeyHex {
    hex32(key.as_bytes())
}

/// Parse a registry `pubkey` field.
pub fn parse_pubkey(hex: &str) -> Result<PublicKey> {
    let bytes = decode_hex32(hex)?;
    PublicKey::from_bytes(&bytes).context("not a valid Ed25519 public key")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_then_reloads_the_same_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sub").join("node.key");
        let (a, generated) = load_or_create(&path).unwrap();
        assert!(generated, "first call must generate");
        let (b, generated) = load_or_create(&path).unwrap();
        assert!(!generated, "second call must reuse");
        assert_eq!(a.public(), b.public());
    }

    #[cfg(unix)]
    #[test]
    fn key_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.key");
        load_or_create(&path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(
            mode & 0o777,
            0o600,
            "node key must not be readable by others"
        );
    }

    /// A corrupt key must be reported, never silently replaced: a new
    /// key is a new identity, which drops this host out of every peer's
    /// registry allowlist.
    #[test]
    fn malformed_key_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.key");
        std::fs::write(&path, "not-a-key").unwrap();
        assert!(load(&path).is_err());
        assert!(load_or_create(&path).is_err());
    }

    /// A host whose secrets live in memory keeps its key there: nothing
    /// is written anywhere.
    #[test]
    fn a_memory_store_holds_the_key() {
        let store = constellation_platform::EphemeralSecretStore::new();
        let (a, generated) = load_or_create_in(&store, KEY_NAME).unwrap();
        assert!(generated);
        let (b, generated) = load_or_create_in(&store, KEY_NAME).unwrap();
        assert!(!generated);
        assert_eq!(a.public(), b.public());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn the_default_location_honours_the_override() {
        let host = constellation_platform::native();
        // Only this test sets it, and it only reads it back through
        // `default_key_path` (the one reader in this crate's tests).
        std::env::set_var("CONSTELLATION_NODE_KEY", "/some/where/host.key");
        let path = default_key_path(host);
        std::env::remove_var("CONSTELLATION_NODE_KEY");
        assert_eq!(path, PathBuf::from("/some/where/host.key"));
    }

    #[test]
    fn pubkey_hex_roundtrip() {
        let key = SecretKey::generate();
        let hex = pubkey_hex(&key.public());
        assert_eq!(hex.len(), 64);
        assert_eq!(parse_pubkey(&hex).unwrap(), key.public());
        assert!(parse_pubkey("zz").is_err());
    }
}
