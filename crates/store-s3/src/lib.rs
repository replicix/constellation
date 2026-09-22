//! S3 backend: bucket layout, conditional writes (CAS), self-describing
//! chunk objects, the pluggable compression codec registry.
//!
//! See docs/explanation/DESIGN.md §2 (bucket layout) and §3 (data plane).

pub mod aws_auth;
pub mod blobs;
pub mod codec;
pub mod commits;
pub mod compact;
pub mod decode_gate;
pub mod designation;
pub mod e2e;
pub mod error;
pub mod format;
pub mod gc;
pub mod layout;
pub mod lease;
pub mod log;
pub mod mark;
pub mod node_cache;
pub mod nodes;
pub mod packs;
pub mod parallel;
pub mod snapshot;
pub mod store;

pub use aws_auth::amazon_s3_builder;

pub use blobs::BlobStore;
pub use codec::{Codec, CompressionSetting};
pub use commits::{vector_covers, Commit, CommitAgg, CommitChain, CommitPayload, Intent, SHARD0};
pub use compact::{CompactionPacer, Compactor, PackFate, PackVerdict, Reclaim, Sweep, Unpaced};
pub use decode_gate::{DecodeGate, DecodeGatePermit, Priority as DecodePriority};
pub use designation::{Designation, DesignationMode, DesignationStore, DesignationTag};
pub use e2e::{
    create_keyring_block, unlock, Argon2Params, E2eKeys, KeyringBlock, SharedE2eKeys, TreeSealing,
};
pub use error::StoreError;
pub use gc::{
    append_journal, is_condemned, publish_condemned, publish_condemned_blobs,
    publish_condemned_packs, read_condemned, read_condemned_blobs, read_condemned_packs,
    CondemnedList, GcJournalEntry,
};
pub use lease::{live_leases_held_by, Lease, LeaseMode, LeaseStore, LeaseTag};
pub use log::LogStore;
pub use mark::{live_set, mark, CatalogPack, LiveSet, Mark, PackCatalog};
pub use node_cache::{NodeCache, NodeCacheStats, NodeLocation, PeerNodeSource};
pub use nodes::{
    claim_node_id, get_node, leave_node, list_node_ids, list_nodes, publish_p2p, publish_ro,
    write_eligible_roster, NodeInfo,
};
pub use packs::{
    build_packs, build_packs_concurrent, BuiltPack, PackEntry, PackHash, PackIndex, PackNode,
    PackStore,
};
pub use parallel::{effective_threads, gc_threads};
pub use snapshot::{snapshot_id, SnapshotRecord, SnapshotStore, SnapshotTreeRoot};
pub use store::{Capabilities, ChunkPutMode, ChunkPutResult, ChunkStore, FsMeta, PreflightCheck};
