//! The crate's one error type.
//!
//! Two of these variants exist purely because node bytes arrive off a
//! network: [`MtreeError::Malformed`] and [`MtreeError::MissingNode`] are
//! the only honest answers to "this buffer is not a node" and "the store
//! does not have what an interior entry points at". Neither may be a
//! panic — a corrupted or truncated pack must fail the operation that
//! read it, not the process.

use crate::hash::NodeHash;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MtreeError {
    /// A node buffer failed a structural check: bad magic, a truncated
    /// header or offset table, an entry extent outside the buffer, a
    /// varint that never terminates, or entries out of key order.
    #[error("malformed node: {0}")]
    Malformed(&'static str),

    /// The encoding is versioned and this crate refuses to guess. ADR-5
    /// promises no migration, so an unknown version is a hard stop
    /// rather than a best-effort read.
    #[error("unsupported node format version {found} (this build writes {expected})")]
    UnsupportedVersion { found: u8, expected: u8 },

    /// An interior entry named a node the store does not hold. For a
    /// reachable tree this is bucket corruption or an incomplete
    /// partial replica, never a normal condition.
    #[error("node {0} is not in the store")]
    MissingNode(NodeHash),

    /// `build`/`apply` were handed keys that are not strictly ascending.
    /// Both are documented as taking a sorted, deduplicated sequence and
    /// silently accepting an unsorted one would produce a tree whose
    /// shape is a function of the caller's mistake.
    #[error("keys must be strictly ascending and deduplicated")]
    Unsorted,

    /// A [`crate::Config`] that cannot describe a canonical tree.
    #[error("invalid configuration: {0}")]
    Config(&'static str),

    /// The node store was closed (its process is shutting down) and
    /// does no more I/O; a tree operation still running is abandoned.
    #[error("the node store is closed")]
    Closed,

    /// Whatever the [`crate::NodeStore`] implementation failed with.
    /// This crate does no I/O of its own, so it has nothing more
    /// specific to say.
    #[error("node store failure")]
    Store(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl MtreeError {
    /// Wrap a store implementation's own error. Kept as a constructor
    /// rather than a `From` impl so a blanket conversion cannot swallow
    /// an unrelated error type by accident.
    pub fn store<E>(err: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        MtreeError::Store(Box::new(err))
    }
}
