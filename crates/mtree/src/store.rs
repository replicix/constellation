//! Where nodes live, as far as this crate is concerned: a synchronous
//! content-addressed map from [`NodeHash`] to bytes.
//!
//! The trait is this narrow on purpose. S4 will implement it over
//! `packs/<hash>` objects, a `fs-core::cache`-backed node cache, and the
//! same peer-then-S3 resolution ladder the data plane already uses for
//! chunks — all of which is asynchronous, retrying, and I/O-bound. None
//! of that belongs in the data structure, and the tree gets easier to
//! test and impossible to make accidentally order-dependent if it
//! cannot do I/O at all. Callers that need async bridge it the way the
//! rest of the repo does (`SyncHandle` in `cli::fusefs`), on their side
//! of this trait.
//!
//! The hash is computed by the [`crate::Tree`], not by the store,
//! because only the tree knows whether this filesystem addresses nodes
//! with plain or keyed blake3 (§P13). A store is free to verify it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

use crate::error::MtreeError;
use crate::hash::NodeHash;

pub trait NodeStore {
    /// Fetch a node's encoded bytes.
    ///
    /// Returns [`MtreeError::MissingNode`] when the store genuinely
    /// does not hold it — which for a reachable tree means bucket
    /// corruption or a partial replica that was asked for a leaf it
    /// deliberately did not fetch, and never a normal condition.
    fn get(&self, hash: &NodeHash) -> Result<Arc<[u8]>, MtreeError>;

    /// Store `bytes` under `hash`. Idempotent: nodes are immutable and
    /// content-addressed, so a repeat put is a no-op and a put with a
    /// different body under the same hash cannot happen.
    ///
    /// `level` is passed because it is free here and expensive later —
    /// S4 packs interior and leaf nodes differently (interior nodes
    /// stay resident, §14.1's 19.75 MiB) and would otherwise have to
    /// decode the header to find out.
    fn put(&self, hash: NodeHash, level: u8, bytes: Vec<u8>) -> Result<(), MtreeError>;
}

impl<T: NodeStore + ?Sized> NodeStore for &T {
    fn get(&self, hash: &NodeHash) -> Result<Arc<[u8]>, MtreeError> {
        (**self).get(hash)
    }

    fn put(&self, hash: NodeHash, level: u8, bytes: Vec<u8>) -> Result<(), MtreeError> {
        (**self).put(hash, level, bytes)
    }
}

impl<T: NodeStore + ?Sized> NodeStore for Arc<T> {
    fn get(&self, hash: &NodeHash) -> Result<Arc<[u8]>, MtreeError> {
        (**self).get(hash)
    }

    fn put(&self, hash: NodeHash, level: u8, bytes: Vec<u8>) -> Result<(), MtreeError> {
        (**self).put(hash, level, bytes)
    }
}

/// Every node resident and uncompressed, plus read/write counters.
///
/// The counters are not debug instrumentation: "diff cost tracks the
/// difference and not the state" (§14.6) is a claim about the number of
/// node reads, and the only way to *assert* it in a test is to count
/// them. They are also what a production store would want to export,
/// which is why they are on the store rather than threaded through
/// every return type.
#[derive(Default)]
pub struct MemoryNodeStore {
    nodes: Mutex<HashMap<NodeHash, Arc<[u8]>>>,
    reads: AtomicU64,
    writes: AtomicU64,
    distinct_writes: AtomicU64,
}

impl MemoryNodeStore {
    pub fn new() -> MemoryNodeStore {
        MemoryNodeStore::default()
    }

    /// Node reads served since the last [`MemoryNodeStore::reset_counters`].
    pub fn reads(&self) -> u64 {
        self.reads.load(Ordering::Relaxed)
    }

    pub fn writes(&self) -> u64 {
        self.writes.load(Ordering::Relaxed)
    }

    /// Writes whose content was not already stored: the bytes a commit
    /// actually adds after structural sharing.
    pub fn distinct_writes(&self) -> u64 {
        self.distinct_writes.load(Ordering::Relaxed)
    }

    pub fn len(&self) -> usize {
        self.nodes.lock().expect("node map").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn reset_counters(&self) {
        self.reads.store(0, Ordering::Relaxed);
        self.writes.store(0, Ordering::Relaxed);
        self.distinct_writes.store(0, Ordering::Relaxed);
    }
}

impl NodeStore for MemoryNodeStore {
    fn get(&self, hash: &NodeHash) -> Result<Arc<[u8]>, MtreeError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.nodes
            .lock()
            .expect("node map")
            .get(hash)
            .cloned()
            .ok_or(MtreeError::MissingNode(*hash))
    }

    fn put(&self, hash: NodeHash, _level: u8, bytes: Vec<u8>) -> Result<(), MtreeError> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        if self
            .nodes
            .lock()
            .expect("node map")
            .insert(hash, bytes.into())
            .is_none()
        {
            self.distinct_writes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_repeat_put_is_not_a_new_node() {
        let store = MemoryNodeStore::new();
        let hash = NodeHash([1u8; 32]);
        store.put(hash, 0, b"node".to_vec()).unwrap();
        store.put(hash, 0, b"node".to_vec()).unwrap();
        assert_eq!(store.writes(), 2);
        assert_eq!(store.distinct_writes(), 1);
        assert_eq!(store.len(), 1);
        assert_eq!(&*store.get(&hash).unwrap(), b"node");
        assert_eq!(store.reads(), 1);
    }

    #[test]
    fn a_missing_node_is_an_error_not_a_panic() {
        let store = MemoryNodeStore::new();
        assert!(matches!(
            store.get(&NodeHash([2u8; 32])),
            Err(MtreeError::MissingNode(_))
        ));
    }
}
