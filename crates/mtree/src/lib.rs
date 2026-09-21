//! A canonical Merkle map: a prolly tree (probabilistic B-tree, a.k.a.
//! Merkle search tree) over opaque byte keys and small opaque values.
//!
//! This is plan 28 §P1 as a library. It is the shared-state primitive
//! the S3-native metadata store is built from, and it is deliberately
//! **pure and synchronous**: no S3, no tokio, no filesystem, no clock,
//! no global state. Nodes reach it through a [`NodeStore`], which the
//! caller implements; S4 implements one over packed objects, a node
//! cache and the peer-then-S3 ladder, and none of that is visible from
//! here. The reason to draw the line there is not tidiness — it is that
//! the properties this crate exists to guarantee are properties of a
//! pure function, and a pure function is the only thing that can be
//! exhaustively property-tested in milliseconds.
//!
//! ## The one idea
//!
//! A node ends after key `k` when `hash(k)`'s level-th `u32` window
//! falls below `u32::MAX / TARGET_ENTRIES`, with min/max entry clamps.
//! The predicate reads the key and *only* the key:
//!
//! - it never reads the value, so changing a value rewrites the leaves
//!   along one root path and reshapes nothing;
//! - it never reads history, so the tree's shape — and therefore its
//!   root hash — is a function of the key set alone. Insertion order
//!   cannot matter, a tree built by incremental [`Tree::apply`] is
//!   byte-identical to a bulk [`Tree::build`] of the same key set, and
//!   delete-then-reinsert returns the original root hash.
//!
//! Everything else follows from that. Two roots can be compared by
//! descending only where hashes differ, which makes `diff` cost
//! O(difference) rather than O(state) (§14.6: a one-key diff of a
//! 35.8M-key tree costs 20 node reads). Two divergent branches from a
//! common base can be merged by delta, deterministically, with an exact
//! conflict set. A snapshot *is* a root hash. `fsck` becomes "recompute
//! the root hash and compare". None of these are features that were
//! added; they are consequences of canonicality, which is why the
//! property tests in `tests/properties.rs` are the real specification
//! of this crate and are worth more than its API docs.
//!
//! ## What this crate does not know
//!
//! It does not know what a key means. The §P6 key encoding — inode,
//! dentry, xattr, reverse-dentry and subsystem ranges — lives in
//! [`keys`] and [`record`], which depend on the core and which **the
//! core does not depend on**. That direction is the point: the structure
//! and the encoding are versioned independently, and no structural
//! decision can quietly depend on the key layout. The one place the tree
//! needs a fact about a value is the §P7 aggregate, and that arrives as
//! a caller-supplied projection ([`Config::leaf_agg`]) — which
//! [`record::leaf_agg`] supplies, because only the codec knows which key
//! range holds the authoritative record.
//!
//! It also does not compress, seal, or pack anything: node bytes go to
//! the [`NodeStore`] as encoded, and zstd, AEAD sealing and pack
//! assembly are S4's.
//!
//! ## Provenance
//!
//! This is a productionization of `bench/prollybench`'s `node.rs` and
//! `tree.rs`, the code whose measurements §14 of the plan reports. The
//! algorithm is unchanged; what changed is that the entry clamps and
//! the hasher moved from process globals into a [`Config`], every
//! `expect` on decoded bytes became an error, and the node header
//! carries an explicit [`node::FORMAT_VERSION`].
//!
//! ## Example
//!
//! ```
//! use constellation_mtree::{MemoryNodeStore, Tree};
//!
//! let tree = Tree::new(MemoryNodeStore::new());
//! let bulk = tree.build((0u32..1000).map(|i| (i.to_be_bytes().to_vec(), b"v".to_vec())))?;
//!
//! // The same key set, inserted backwards one key at a time.
//! let mut root = tree.empty()?;
//! for i in (0u32..1000).rev() {
//!     root = tree.apply(&root, &[(i.to_be_bytes().to_vec(), Some(b"v".to_vec()))])?;
//! }
//! assert_eq!(root, bulk);
//! # Ok::<(), constellation_mtree::MtreeError>(())
//! ```

pub mod config;
pub mod error;
pub mod hash;
pub mod keys;
pub mod node;
pub mod record;
pub mod store;
pub mod tree;

pub use config::{no_leaf_agg, Config, LeafAgg};
pub use error::MtreeError;
pub use hash::{Hasher, NodeHash};
pub use keys::{Field, Key, KeyError, KeyRange, Subsystem};
pub use node::{Agg, Entry, NodeRef, Value, FORMAT_VERSION, MAX_ENTRIES, MIN_ENTRIES};
pub use record::{
    leaf_agg, Attrs, BlobHash, DentryRecord, InodePlan, InodeRecord, Kind, Payload, RecordError,
    XattrPlacement, VALUE_SPILL, XATTR_INLINE,
};
pub use store::{MemoryNodeStore, NodeStore};
pub use tree::{Census, ChangeKind, Cursor, CursorEntry, Edit, LevelStats, Merged, Pair, Tree};
