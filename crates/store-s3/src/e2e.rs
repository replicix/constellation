//! End-to-end key management: Constellation's key domain over the shared
//! `replicix-chunkfmt` hierarchy, and the sealing keys of the metadata
//! tree.
//!
//! The master key, its Argon2id wrap, the derivation of the addressing
//! key, per-partition DEKs and gossip seed, and the XChaCha20-Poly1305
//! envelope live in `replicix_chunkfmt::e2e`. The two labels below are
//! Constellation's own and decide every derived key of every existing
//! filesystem: they must never change (the tests pin them).

use replicix_chunkfmt::KeyringBlock;
use zeroize::Zeroize;

use crate::error::StoreError;

/// The derivation labels of Constellation's E2E key hierarchy.
pub struct ConstellationDomain;

impl replicix_chunkfmt::KeyDomain for ConstellationDomain {
    const KDF_DOMAIN: &'static [u8] = b"constellation/e2e/v1/";
    const PIN_CONTEXT: &'static str = "constellation 2026 e2e master pin v1";
}

pub type E2eKeys = replicix_chunkfmt::E2eKeys<ConstellationDomain>;
pub type SharedE2eKeys = replicix_chunkfmt::SharedE2eKeys<ConstellationDomain>;

/// Unwrap the master key from a `meta.json` keyring block and derive the
/// live keys. No S3 access: the block is already in the loaded meta.
pub fn unlock(block: &KeyringBlock, passphrase: &str) -> Result<SharedE2eKeys, StoreError> {
    Ok(replicix_chunkfmt::unlock::<ConstellationDomain>(
        block, passphrase,
    )?)
}

/// Plan 28 §P13: the keys that seal the metadata tree on an E2E
/// filesystem — pack frames and indices, spilled-value blobs, and commit
/// objects — one derived key per object kind.
///
/// Keyed *addressing* alone (node hashes and the boundary function under
/// the addressing key) hides nothing about content: a pack frame is a
/// zstd'd node whose keys are file names and whose values are attributes.
/// So every such object is AEAD-sealed under a key only a passphrase
/// holder can derive, with the object's identity as associated data. The
/// purpose strings contain a `/`, which no partition id does, so they
/// cannot collide with a log partition's DEK.
#[derive(Clone)]
pub struct TreeSealing {
    pub(crate) nodes: [u8; 32],
    pub(crate) blobs: [u8; 32],
    pub(crate) commits: [u8; 32],
}

impl TreeSealing {
    pub fn from_keys(keys: &E2eKeys) -> TreeSealing {
        TreeSealing {
            nodes: *keys.dek("mtree/nodes"),
            blobs: *keys.dek("mtree/blobs"),
            commits: *keys.dek("mtree/commits"),
        }
    }

    /// For an optional keyring, as every constructor that takes one has.
    pub fn for_keys(keys: Option<&SharedE2eKeys>) -> Option<TreeSealing> {
        keys.map(|keys| TreeSealing::from_keys(keys))
    }
}

impl std::fmt::Debug for TreeSealing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TreeSealing(..)")
    }
}

impl Drop for TreeSealing {
    fn drop(&mut self) {
        self.nodes.zeroize();
        self.blobs.zeroize();
        self.commits.zeroize();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use replicix_chunkfmt::KeyDomain;

    /// The labels are part of every existing filesystem's key hierarchy.
    #[test]
    fn domain_labels_are_pinned() {
        assert_eq!(ConstellationDomain::KDF_DOMAIN, b"constellation/e2e/v1/");
        assert_eq!(
            ConstellationDomain::PIN_CONTEXT,
            "constellation 2026 e2e master pin v1"
        );
    }

    #[test]
    fn tree_sealing_derives_one_key_per_kind() {
        let keys = E2eKeys::generate();
        let s = TreeSealing::from_keys(&keys);
        assert_eq!(s.nodes, *keys.dek("mtree/nodes"));
        assert_eq!(s.blobs, *keys.dek("mtree/blobs"));
        assert_eq!(s.commits, *keys.dek("mtree/commits"));
        assert_ne!(s.nodes, s.blobs);
        assert_ne!(s.blobs, s.commits);
        assert!(TreeSealing::for_keys(None).is_none());
    }
}
