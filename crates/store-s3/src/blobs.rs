//! `blobs/<hex>`: the values §P6 refused to put in a node.
//!
//! [`crate::packs`] stores tree *structure*; this stores the handful of
//! values that structure could not hold. `mtree::record` caps an encoded
//! value at `VALUE_SPILL` (1 KiB) because a node targets 8 KiB and a
//! point lookup decompresses a whole leaf, so one 64 KiB xattr taxes
//! every read of its neighbours. Above the cap the value is replaced by
//! a [`BlobHash`] and the bytes move here — exactly the rule
//! `fs-core::manifest` already applies to long chunk lists, generalized.
//!
//! ## Why this is a third object namespace
//!
//! There are now three content-addressed namespaces on a bucket, and
//! the split is about *reachability*, not about hashing:
//!
//! - `chunks/` holds file data, reachable from a manifest, swept by the
//!   chunk store's rules;
//! - `packs/` holds tree nodes, reachable from a commit root;
//! - `blobs/` holds overflowed metadata values, reachable **only**
//!   through a node's value bytes.
//!
//! A blob filed under `chunks/` would be handed to a GC that has no way
//! to see the reference — the reference lives inside an `mtree` leaf,
//! not in any manifest — and would be collected out from under a live
//! inode. §P10's mark must walk node values and add what it finds here
//! to the live set; see the note in `PROGRESS.md`, because S7a's mark
//! walks structure only and does not do that yet.
//!
//! ## Durability ordering
//!
//! A blob obeys the same rule as a pack: **it must be on the bucket
//! before the commit whose tree names it**. [`CommitChain::publish`]
//! cannot check this for us — it verifies packs, and a blob reference
//! is buried in an opaque value it must not parse — so the discipline
//! lives at the writer: S5's publisher PUTs every blob a batch produced
//! before it seals packs, and therefore long before the CAS. The crash
//! outcomes are the same two as for packs: orphan blobs before the
//! commit (garbage, invisible), or a commit all of whose blobs exist.
//!
//! ## Addressing
//!
//! The hash is the caller's, not ours. On an E2E filesystem blob
//! identity is a *keyed* blake3 under the addressing key, the same way
//! node identity is (§P13), so [`BlobStore`] is told which hasher to
//! verify with rather than assuming a plain digest. Writes are
//! `PutMode::Create` and an existing object is success: the address is
//! the content, so a second writer producing the same bytes is not a
//! conflict to resolve but the deduplication working.

use crate::error::StoreError;
use crate::layout;
use constellation_mtree::{BlobHash, Hasher, NodeHash};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use std::sync::Arc;

/// Read and write `blobs/*` against one bucket prefix.
#[derive(Clone)]
pub struct BlobStore {
    store: Arc<dyn ObjectStore>,
    hasher: Hasher,
}

impl BlobStore {
    pub fn new(store: Arc<dyn ObjectStore>, hasher: Hasher) -> BlobStore {
        BlobStore { store, hasher }
    }

    /// The address these bytes have under this filesystem's hasher.
    pub fn hash(&self, bytes: &[u8]) -> BlobHash {
        BlobHash(self.hasher.hash(bytes).0)
    }

    fn path(hash: &BlobHash) -> object_store::path::Path {
        layout::blob(&NodeHash(hash.0).to_hex())
    }

    /// Store `bytes` under their own address.
    ///
    /// Refuses to write bytes whose hash is not `hash`: the caller
    /// computed the reference it is about to embed in a node, and a
    /// mismatch here means the value that reaches the bucket is not the
    /// value the tree names.
    pub async fn put(&self, hash: &BlobHash, bytes: Vec<u8>) -> Result<(), StoreError> {
        if self.hash(&bytes) != *hash {
            return Err(StoreError::HashMismatch {
                key: Self::path(hash).to_string(),
            });
        }
        match self
            .store
            .put_opts(
                &Self::path(hash),
                PutPayload::from(bytes),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => Ok(()),
            // Content-addressed: the object already there *is* these
            // bytes. Nothing to reconcile.
            Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Fetch and verify. A blob may arrive from an untrusted bucket, so
    /// the address is re-checked before the bytes are handed back —
    /// the same rule `node_cache` applies to a node.
    pub async fn get(&self, hash: &BlobHash) -> Result<Vec<u8>, StoreError> {
        let path = Self::path(hash);
        let body = self.store.get(&path).await?.bytes().await?.to_vec();
        if self.hash(&body) != *hash {
            return Err(StoreError::HashMismatch {
                key: path.to_string(),
            });
        }
        Ok(body)
    }

    pub async fn contains(&self, hash: &BlobHash) -> Result<bool, StoreError> {
        match self.store.head(&Self::path(hash)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Make a batch durable, concurrently. Returns the addresses so a
    /// caller can assert on what it wrote.
    pub async fn put_all(&self, bodies: Vec<Vec<u8>>) -> Result<Vec<BlobHash>, StoreError> {
        use futures::StreamExt;
        let hashes: Vec<BlobHash> = bodies.iter().map(|body| self.hash(body)).collect();
        {
            let mut writes = futures::stream::iter(
                hashes
                    .iter()
                    .copied()
                    .zip(bodies)
                    .map(|(hash, body)| async move { self.put(&hash, body).await }),
            )
            .buffer_unordered(8);
            while let Some(result) = writes.next().await {
                result?;
            }
        }
        Ok(hashes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn store() -> (Arc<dyn ObjectStore>, BlobStore) {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        (inner.clone(), BlobStore::new(inner, Hasher::Plain))
    }

    #[tokio::test]
    async fn a_blob_round_trips_under_its_own_address() {
        let (_, blobs) = store();
        let body = vec![7u8; 8192];
        let hash = blobs.hash(&body);
        assert!(!blobs.contains(&hash).await.unwrap());
        blobs.put(&hash, body.clone()).await.unwrap();
        assert!(blobs.contains(&hash).await.unwrap());
        assert_eq!(blobs.get(&hash).await.unwrap(), body);
        // Writing the same content twice is deduplication, not a race.
        blobs.put(&hash, body).await.unwrap();
    }

    #[tokio::test]
    async fn a_blob_that_is_not_its_address_is_refused_in_both_directions() {
        let (inner, blobs) = store();
        let body = vec![1u8; 2048];
        let wrong = BlobHash([0xab; 32]);
        assert!(matches!(
            blobs.put(&wrong, body.clone()).await,
            Err(StoreError::HashMismatch { .. })
        ));

        // And a bucket that hands back something else is caught on read
        // rather than trusted.
        let hash = blobs.hash(&body);
        inner
            .put(&BlobStore::path(&hash), PutPayload::from(vec![2u8; 2048]))
            .await
            .unwrap();
        assert!(matches!(
            blobs.get(&hash).await,
            Err(StoreError::HashMismatch { .. })
        ));
    }

    /// An E2E filesystem addresses blobs with a keyed digest, so the
    /// same bytes land at a different key and a plain-hashing reader
    /// cannot even name them. Mirrors `node_cache`'s keyed addressing.
    #[tokio::test]
    async fn keyed_addressing_changes_the_object_key() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let plain = BlobStore::new(inner.clone(), Hasher::Plain);
        let keyed = BlobStore::new(inner, Hasher::Keyed([9u8; 32]));
        let body = vec![3u8; 4096];
        assert_ne!(plain.hash(&body), keyed.hash(&body));
        keyed.put(&keyed.hash(&body), body.clone()).await.unwrap();
        assert!(!plain.contains(&plain.hash(&body)).await.unwrap());
        assert_eq!(keyed.get(&keyed.hash(&body)).await.unwrap(), body);
    }

    #[tokio::test]
    async fn a_batch_lands_and_reports_its_addresses() {
        let (_, blobs) = store();
        let bodies: Vec<Vec<u8>> = (0..16u8).map(|i| vec![i; 3000]).collect();
        let hashes = blobs.put_all(bodies.clone()).await.unwrap();
        assert_eq!(hashes.len(), bodies.len());
        for (hash, body) in hashes.iter().zip(&bodies) {
            assert_eq!(&blobs.get(hash).await.unwrap(), body);
        }
        assert!(blobs.put_all(Vec::new()).await.unwrap().is_empty());
    }
}
