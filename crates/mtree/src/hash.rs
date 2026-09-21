//! Node identity, and the one place the E2E decision shows up (§P13).
//!
//! ADR-8's argument against plaintext content addressing is that an
//! object key which is the hash of the plaintext lets the storage
//! provider hash a *guessed* plaintext and test for its presence.
//! Applied to metadata the guess is a directory listing rather than a
//! file, so the countermeasure has to be the same: in E2E mode node
//! hashes are `blake3::keyed_hash` under the filesystem's addressing
//! key, and the node blob is sealed before it is packed (sealing is
//! S4's job; the addressing is this crate's).
//!
//! The keyed mode is selectable at [`crate::Tree`] construction rather
//! than bolted on later because it has to reach *two* places, and the
//! second one is easy to miss: the boundary function (`§P1`) also hashes
//! keys, and it decides the shape of the tree. Leaving boundaries
//! unkeyed would publish an oracle for the shape — node sizes and split
//! points computable from a guessed key set — through exactly the side
//! channel ADR-8 is about. So a [`Hasher`] answers both questions, and
//! keyed and unkeyed trees differ in every hash but in nothing else:
//! both are canonical, both are byte-identical to a bulk build of the
//! same key set under the same hasher.

/// The identity of an encoded node: 32 bytes of blake3, keyed or not.
///
/// A distinct type from `fs-core::ChunkHash` on purpose. They are the
/// same 32 bytes and the same algorithm, but a chunk hash addresses
/// file data and a node hash addresses metadata structure; the two
/// namespaces are packed and garbage-collected by different rules
/// (§P8, §P10) and mixing them up should not typecheck.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeHash(pub [u8; 32]);

impl NodeHash {
    pub const ZERO: NodeHash = NodeHash([0u8; 32]);

    pub fn to_hex(self) -> String {
        let mut s = String::with_capacity(64);
        for b in self.0 {
            s.push(char::from_digit((b >> 4) as u32, 16).unwrap_or('0'));
            s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap_or('0'));
        }
        s
    }

    pub fn from_hex(hex: &str) -> Option<NodeHash> {
        let bytes = hex.as_bytes();
        if bytes.len() != 64 {
            return None;
        }
        let mut out = [0u8; 32];
        for (i, byte) in out.iter_mut().enumerate() {
            let hi = (bytes[i * 2] as char).to_digit(16)?;
            let lo = (bytes[i * 2 + 1] as char).to_digit(16)?;
            *byte = ((hi << 4) | lo) as u8;
        }
        Some(NodeHash(out))
    }
}

impl std::fmt::Display for NodeHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl std::fmt::Debug for NodeHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NodeHash({})", self.to_hex())
    }
}

/// How this tree hashes: plaintext blake3, or keyed blake3 for E2E.
///
/// `Copy` because it is consulted on every boundary test, which is once
/// per key on every build and every commit.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Hasher {
    /// Plain `blake3::hash`. Node hashes are computable by anyone who
    /// has the bytes, which is what makes non-E2E buckets debuggable.
    Plain,
    /// `blake3::keyed_hash` under a per-filesystem addressing key. One
    /// key per filesystem, so structural sharing and dedup are
    /// unaffected (§P13).
    Keyed([u8; 32]),
}

impl Hasher {
    pub fn hash(&self, bytes: &[u8]) -> NodeHash {
        match self {
            Hasher::Plain => NodeHash(*blake3::hash(bytes).as_bytes()),
            Hasher::Keyed(key) => NodeHash(*blake3::keyed_hash(key, bytes).as_bytes()),
        }
    }

    /// The `window`-th little-endian `u32` of `blake3(key)`, which is
    /// what the boundary function thresholds. `window` is taken modulo
    /// the digest width by the caller.
    pub(crate) fn key_word(&self, key: &[u8], window: usize) -> u32 {
        let digest = self.hash(key);
        let at = window * 4;
        u32::from_le_bytes([
            digest.0[at],
            digest.0[at + 1],
            digest.0[at + 2],
            digest.0[at + 3],
        ])
    }
}

/// Redacted: a `Debug` that printed the addressing key would leak it
/// into every log line that formats a [`crate::Config`].
impl std::fmt::Debug for Hasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Hasher::Plain => f.write_str("Hasher::Plain"),
            Hasher::Keyed(_) => f.write_str("Hasher::Keyed(<redacted>)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trips_and_rejects_junk() {
        let h = Hasher::Plain.hash(b"node");
        assert_eq!(NodeHash::from_hex(&h.to_hex()), Some(h));
        assert_eq!(NodeHash::from_hex("beef"), None);
        assert_eq!(NodeHash::from_hex(&"z".repeat(64)), None);
    }

    #[test]
    fn keyed_and_plain_disagree_on_everything() {
        let plain = Hasher::Plain;
        let keyed = Hasher::Keyed([7u8; 32]);
        assert_ne!(plain.hash(b"node"), keyed.hash(b"node"));
        assert_ne!(plain.key_word(b"key", 0), keyed.key_word(b"key", 0));
        assert_eq!(keyed.hash(b"node"), Hasher::Keyed([7u8; 32]).hash(b"node"));
    }

    #[test]
    fn the_addressing_key_is_not_printable() {
        let rendered = format!("{:?}", Hasher::Keyed([0xab; 32]));
        assert!(!rendered.contains("ab"), "{rendered}");
    }
}
