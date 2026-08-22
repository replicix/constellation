//! S3 backend: bucket layout, conditional writes (CAS), self-describing
//! chunk objects, the pluggable compression codec registry.
//!
//! See docs/DESIGN.md §2 (bucket layout) and §3 (data plane).

pub mod codec;
pub mod error;
pub mod format;
pub mod layout;
pub mod log;
pub mod store;

pub use codec::{Codec, CompressionSetting};
pub use error::StoreError;
pub use log::LogStore;
pub use store::{Capabilities, ChunkStore, FsMeta};
