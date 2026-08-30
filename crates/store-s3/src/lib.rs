//! S3 backend: bucket layout, conditional writes (CAS), self-describing
//! chunk objects, the pluggable compression codec registry.
//!
//! See docs/DESIGN.md §2 (bucket layout) and §3 (data plane).

pub mod codec;
pub mod designation;
pub mod error;
pub mod format;
pub mod layout;
pub mod lease;
pub mod log;
pub mod nodes;
pub mod store;

pub use codec::{Codec, CompressionSetting};
pub use designation::{Designation, DesignationMode, DesignationStore, DesignationTag};
pub use error::StoreError;
pub use lease::{live_leases_held_by, Lease, LeaseMode, LeaseStore, LeaseTag};
pub use log::{CheckpointVector, LogStore};
pub use nodes::{
    claim_node_id, get_node, leave_node, list_node_ids, list_nodes, publish_p2p, publish_ro,
    write_eligible_roster, NodeInfo,
};
pub use store::{Capabilities, ChunkPutMode, ChunkPutResult, ChunkStore, FsMeta};
