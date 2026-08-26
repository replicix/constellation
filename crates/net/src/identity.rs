//! Node identity for the P2P fast path (DESIGN.md §8).
//!
//! Every host has one Ed25519 key at `~/.config/constellation/node.key`
//! (0600), **per host and not per state dir**: the key identifies the
//! machine that peers dial, while a node id identifies one mount's
//! state dir. `constellation host init` creates it; a mount generates
//! one when it is missing and logs that it did.
//!
//! The key is iroh's `SecretKey`, so the public half is directly the
//! endpoint id peers dial. Trust flows from the bucket, not from the
//! key: a peer is accepted only when its pubkey is listed in the node
//! registry, and writing the registry needs bucket write permission, so
//! IAM stays the trust root.

use anyhow::{Context, Result};
use iroh::{PublicKey, SecretKey};
use std::path::{Path, PathBuf};

/// Hex-encoded public key, as stored in the node registry.
pub type PubKeyHex = String;

/// Default key location, overridable with `CONSTELLATION_NODE_KEY`.
pub fn default_key_path() -> PathBuf {
    if let Some(p) = std::env::var_os("CONSTELLATION_NODE_KEY") {
        return PathBuf::from(p);
    }
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
        .unwrap_or_else(|| PathBuf::from("/etc"));
    base.join("constellation").join("node.key")
}

/// Load the host node key, generating it if absent. Returns
/// `(key, generated)` so the caller can log a first-time creation.
pub fn load_or_create(path: &Path) -> Result<(SecretKey, bool)> {
    if let Some(key) = load(path)? {
        return Ok((key, false));
    }
    let key = SecretKey::generate();
    write_key(path, &key)?;
    Ok((key, true))
}

/// Load the key if it exists. A malformed key is an error, not a
/// silent regeneration: overwriting it would change this host's
/// identity and drop it out of every registry allowlist.
pub fn load(path: &Path) -> Result<Option<SecretKey>> {
    let raw = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let bytes = decode_hex32(raw.trim())
        .with_context(|| format!("{} is not a 32-byte hex node key", path.display()))?;
    Ok(Some(SecretKey::from_bytes(&bytes)))
}

fn write_key(path: &Path, key: &SecretKey) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    // Write-then-rename with the mode set before the rename, so the key
    // is never briefly world-readable at its final name.
    let tmp = path.with_extension("key.tmp");
    std::fs::write(&tmp, hex32(&key.to_bytes()))
        .with_context(|| format!("writing {}", tmp.display()))?;
    set_owner_only(&tmp)?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("renaming {} to {}", tmp.display(), path.display()))?;
    Ok(())
}

#[cfg(unix)]
fn set_owner_only(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("chmod 600 {}", path.display()))
}

#[cfg(not(unix))]
fn set_owner_only(_path: &Path) -> Result<()> {
    Ok(())
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

    #[test]
    fn pubkey_hex_roundtrip() {
        let key = SecretKey::generate();
        let hex = pubkey_hex(&key.public());
        assert_eq!(hex.len(), 64);
        assert_eq!(parse_pubkey(&hex).unwrap(), key.public());
        assert!(parse_pubkey("zz").is_err());
    }
}
