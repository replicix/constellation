//! S3 backend: bucket layout, conditional writes (CAS), self-describing
//! chunk objects, the pluggable compression codec registry.
//!
//! See docs/explanation/DESIGN.md §2 (bucket layout) and §3 (data plane).

pub mod aws_auth;
pub mod codec;
pub mod decode_gate;
pub mod designation;
pub mod e2e;
pub mod error;
pub mod existence;
pub mod format;
pub mod gc;
pub mod layout;
pub mod lease;
pub mod log;
pub mod nodes;
pub mod snapshot;
pub mod store;

pub use aws_auth::amazon_s3_builder;

pub use codec::{Codec, CompressionSetting};
pub use decode_gate::{DecodeGate, DecodeGatePermit, Priority as DecodePriority};
pub use designation::{Designation, DesignationMode, DesignationStore, DesignationTag};
pub use e2e::{
    change_passphrase, load_keyring, put_keyring, Argon2Params, E2eKeys, Keyring, SharedE2eKeys,
};
pub use error::StoreError;
pub use existence::{parse_chunk_key, scan_chunk_hashes, ChunkHashScan};
pub use gc::{
    append_journal, is_condemned, publish_condemned, read_condemned, CondemnedList, GcJournalEntry,
};
pub use lease::{live_leases_held_by, Lease, LeaseMode, LeaseStore, LeaseTag};
pub use log::{CheckpointVector, LogStore};
pub use nodes::{
    claim_node_id, get_node, leave_node, list_node_ids, list_nodes, publish_p2p, publish_ro,
    write_eligible_roster, NodeInfo,
};
pub use snapshot::{snapshot_id, SnapshotRecord, SnapshotStore};
pub use store::{Capabilities, ChunkPutMode, ChunkPutResult, ChunkStore, FsMeta};
