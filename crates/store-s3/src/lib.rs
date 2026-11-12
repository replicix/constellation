//! S3 backend: bucket layout, conditional writes (CAS), self-describing
//! chunk objects, the pluggable compression codec registry.
//!
//! The conditional-write primitives, leases, probes, the chunk format and
//! the codec registry live in the shared `replicix-objcas` and
//! `replicix-chunkfmt` crates; [`adapters`] plugs Constellation's error
//! table and protocol clock into the former.
//!
//! See docs/explanation/DESIGN.md §2 (bucket layout) and §3 (data plane).

pub mod adapters;
pub mod blobs;
pub mod commits;
pub mod compact;
pub mod decode_gate;
pub mod designation;
pub mod e2e;
pub mod error;
#[cfg(test)]
pub(crate) mod faulty;
pub mod gc;
pub mod heartbeat;
pub mod inbox;
pub mod layout;
pub mod log;
pub mod mark;
pub mod node_cache;
pub mod nodes;
pub mod packs;
pub mod parallel;
pub(crate) mod run;
#[cfg(test)]
pub(crate) mod scripted_http;
pub mod snapsched;
pub mod snapshot;
pub mod store;

pub use blobs::BlobStore;
pub use commits::{vector_covers, Commit, CommitAgg, CommitChain, CommitPayload, Intent, SHARD0};
pub use compact::{CompactionPacer, Compactor, PackFate, PackVerdict, Reclaim, Sweep, Unpaced};
pub use decode_gate::{DecodeGate, DecodeGatePermit, Priority as DecodePriority};
pub use designation::{Designation, DesignationMode, DesignationStore, DesignationTag};
pub use e2e::{unlock, ConstellationDomain, E2eKeys, SharedE2eKeys, TreeSealing};
pub use error::StoreError;
pub use gc::{
    append_journal, publish_condemned, publish_condemned_blobs, publish_condemned_packs,
    read_chunk_census, read_condemned, read_condemned_blobs, read_condemned_packs,
    write_chunk_census, ChunkCensus, CondemnedList, CondemnedView, DedupVerdict, GcJournalEntry,
    PointerId,
};
pub use heartbeat::{HeartbeatStore, Promise, PromiseConfig, TakeoverCheck};
pub use inbox::{
    gc_keep_newest, InboxBatch, InboxKey, InboxOp, InboxPoller, InboxRid, InboxStore,
    InboxSubmitter, PollBackoff, PutBatch, INBOX_VERSION,
};
pub use log::LogStore;
pub use mark::{live_set, mark, CatalogPack, LiveSet, Mark, PackCatalog};
pub use node_cache::{NodeCache, NodeCacheStats, NodeLocation, PeerNodeSource};
pub use nodes::{
    claim_node_id, get_node, leave_node, list_node_ids, list_nodes, publish_p2p, publish_ro,
    registry_scan, write_eligible_roster, NodeInfo, RecordRead, RegistryScan, RosterRead,
};
pub use packs::{
    build_packs, build_packs_concurrent, BuiltPack, PackEntry, PackHash, PackIndex, PackNode,
    PackStore,
};
pub use parallel::{effective_threads, gc_threads};
pub use snapshot::{snapshot_id, SnapshotRecord, SnapshotStore, SnapshotTreeRoot};
pub use store::{ChunkPutMode, ChunkPutResult, ChunkStore, FsMeta, MissingFs};
