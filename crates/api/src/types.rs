//! Control API request/response types (stable JSON surface).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Request {
    Ping,
    Status,
    /// Fully cache `path`'s subtree on this node and keep it current.
    Pin {
        path: String,
    },
    /// Stop keeping `path` resident; its chunks become evictable.
    Unpin {
        path: String,
    },
    ListPins,
    /// `constellation offline <path> [--ro]`: designate this node for
    /// `path` (DESIGN.md §5.2).
    Offline {
        path: String,
        #[serde(default)]
        read_only: bool,
    },
    /// Release this node's designation for `path`.
    Online {
        path: String,
    },
    ListDesignations,
    /// Replay this node's stranded journal against the shared log
    /// (DESIGN.md §6 / §9). Needs the write lease.
    Reintegrate,
    /// Permanently retire a registry member (phase 4c). `node_id: None`
    /// means this node (flush + tombstone + stop writing); `Some(id)` is
    /// admin removal of a *different* node via a still-mounted peer.
    Leave {
        #[serde(default)]
        node_id: Option<u64>,
        #[serde(default)]
        force: bool,
    },
    SetWriteMode {
        mode: String,
    },
    SnapshotCreate {
        selector: String,
    },
    SnapshotList {
        path: Option<String>,
    },
    /// Preferred additive spelling for snapshot listing. `SnapshotList`
    /// remains accepted for compatibility with phase-6 clients.
    ListSnapshots {
        path: Option<String>,
    },
    SnapshotDelete {
        selector: String,
    },
    Clone {
        selector: String,
        destination: String,
    },
    SnapRefs {
        id: String,
    },
    /// List one directory from the authoritative local metadata replica.
    ReadDir {
        path: String,
    },
    /// Inspect one namespace object and its manifest summary.
    Inspect {
        path: String,
    },
    /// Voluntarily release a locally held partition lease. This is
    /// cooperative administration, not a fencing operation.
    ForceRelease {
        part: String,
    },
    /// Return the most recent daemon log lines from the in-memory ring.
    LogTail {
        lines: usize,
    },
    /// Run the mounted daemon's backend capability probes.
    Doctor,
    /// Enumerate local cache entries for operator inspection.
    CacheList,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "resp", rename_all = "snake_case")]
pub enum Response {
    Pong,
    Status(Box<StatusReport>),
    /// A mutating command succeeded; `detail` is a human-readable summary.
    Ok {
        detail: String,
    },
    /// Wrapped in a struct rather than a bare `Pins(Vec<PinStatus>)`:
    /// serde's internally-tagged representation (`tag = "resp"`) cannot
    /// serialize a newtype variant whose content is a sequence — it
    /// needs the variant's content to be map-like so the tag field can
    /// be merged in. A bare `Vec` there makes serialization fail
    /// silently at runtime (the error is only visible at debug-level
    /// tracing), which manifests as the daemon closing the connection
    /// with no response at all.
    Pins {
        pins: Vec<PinStatus>,
    },
    Designations {
        designations: Vec<DesignationStatus>,
    },
    Snapshots {
        snapshots: Vec<SnapshotStatus>,
    },
    Directory {
        path: String,
        entries: Vec<DirectoryEntry>,
    },
    Inspection {
        entry: InspectStatus,
    },
    Logs {
        lines: Vec<String>,
    },
    Doctor {
        report: DoctorStatus,
    },
    CacheEntries {
        entries: Vec<CacheEntryStatus>,
    },
    Refs {
        hashes: Vec<String>,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub name: String,
    pub path: String,
    pub ino: u64,
    pub kind: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InspectStatus {
    pub path: String,
    pub ino: u64,
    pub kind: String,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub atime_ns: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub rdev: u64,
    #[serde(default)]
    pub manifest: Option<ManifestStatus>,
}

/// Streaming file download handle for the localhost web UI.
///
/// `chunks` yields successive buffers (typically one content-addressed
/// chunk each). Closing the receiver cancels the producer via backpressure
/// drop; the producer never holds the whole file.
pub struct DownloadSession {
    pub file_name: String,
    pub size: u64,
    pub chunks: tokio::sync::mpsc::Receiver<Result<Vec<u8>, String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ManifestStatus {
    pub chunk_size: u32,
    pub chunk_count: u64,
    pub spilled: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DoctorStatus {
    pub create_if_absent: bool,
    pub etag_cas: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CacheEntryStatus {
    pub hash: String,
    pub size: u64,
    pub state: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SnapshotStatus {
    pub id: String,
    pub path: String,
    pub name: String,
    pub root_hash: String,
    pub created_unix_ms: i64,
}

/// One offline designation, as exposed by the control API.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DesignationStatus {
    pub path: String,
    pub designee: u64,
    pub read_only: bool,
}

/// One pinned subtree on this node.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PinStatus {
    pub path: String,
    /// Logical bytes the subtree's manifests describe.
    pub bytes: u64,
    /// Chunks of the subtree currently resident.
    pub chunks_cached: u64,
    /// Chunks the subtree needs in total.
    pub chunks_total: u64,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusReport {
    pub fs_uuid: String,
    pub backend: String,
    pub mountpoint: String,
    /// This node's cluster-unique id (scopes ino allocation, marks log
    /// segment origin).
    #[serde(default)]
    pub node_id: u64,
    /// Whether this node's registry record is present and not retired.
    /// False after a successful `leave`, or when an admin retired us.
    #[serde(default = "default_true")]
    pub enrolled: bool,
    pub uptime_s: u64,
    pub spool: SpoolStatus,
    pub cache: CacheStatus,
    /// Write authority for the genesis partition (p0). Kept for
    /// backward-compatible `status` consumers; per-partition detail is
    /// in [`StatusReport::partitions`].
    #[serde(default)]
    pub lease: LeaseStatus,
    /// Partition map + per-partition lease (M3.2). Empty on pre-partition
    /// daemons (serde default).
    #[serde(default)]
    pub partitions: Vec<PartitionStatus>,
    /// P2P fast path (M3.3). `enabled: false` on daemons without it, or
    /// when `CONSTELLATION_P2P=off`.
    #[serde(default)]
    pub p2p: P2pStatus,
    /// Locally pinned subtrees (phase 4a). Node-local, not replicated.
    #[serde(default)]
    pub pins: Vec<PinStatus>,
    /// Live offline designations visible to this node (phase 4a,
    /// DESIGN.md §5.2). Replicated via S3, so every node's view should
    /// agree modulo the periodic refresh lag.
    #[serde(default)]
    pub designations: Vec<DesignationStatus>,
    /// Continuation epoch (phase 4b, DESIGN.md §5.3).
    #[serde(default)]
    pub epoch: EpochStatus,
    /// Stranded-journal reintegration (phase 4b).
    #[serde(default)]
    pub reintegration: ReintegrationStatus,
    /// Cooperative cache (phase 5, DESIGN.md §7).
    #[serde(default)]
    pub coop: CoopStatus,
    /// Phase 5b write-back queue and adaptive upload policy.
    #[serde(default)]
    pub writeback: WritebackStatus,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WritebackStatus {
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub dirty_bytes: u64,
    #[serde(default)]
    pub pending_uploads: u64,
    #[serde(default)]
    pub upload_concurrency: u32,
    #[serde(default)]
    pub remote_probe_enabled: bool,
    #[serde(default)]
    pub remote_probe_hit_rate: f64,
    #[serde(default)]
    pub existence_listed: u64,
    #[serde(default)]
    pub existence_complete: bool,
    #[serde(default)]
    pub existence_bloom_hits: u64,
    #[serde(default)]
    pub existence_bloom_misses: u64,
    #[serde(default)]
    pub existence_peer_hints: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CoopStatus {
    #[serde(default)]
    pub peer_hits: u64,
    /// Soft negatives: peer declined (bloom FP, busy, not present).
    #[serde(default)]
    pub peer_misses: u64,
    /// Hard negatives: transport / hash failures against a peer.
    #[serde(default)]
    pub peer_errors: u64,
    #[serde(default)]
    pub s3_fetches: u64,
    #[serde(default)]
    pub hedges_fired: u64,
    #[serde(default)]
    pub bytes_served_to_peers: u64,
    #[serde(default)]
    pub stale_digests_pruned: u64,
    #[serde(default)]
    pub digest_rebuilds: u64,
    #[serde(default)]
    pub digest_capacity_exceeded: bool,
    #[serde(default)]
    pub per_source: Vec<SourceStatus>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceStatus {
    pub id: String,
    #[serde(default)]
    pub ttfb_ms_ewma: f64,
    #[serde(default)]
    pub goodput_mbps_ewma: f64,
    /// Soft negatives (declines). Previously folded into `err_rate`.
    #[serde(default)]
    pub miss_rate: f64,
    /// Hard negatives (transport/hash failures).
    #[serde(default)]
    pub err_rate: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_rtt_ms: Option<f64>,
    #[serde(default)]
    pub path: String,
}

/// Live continuation-epoch snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EpochStatus {
    #[serde(default)]
    pub active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch_id: Option<String>,
    #[serde(default)]
    pub members: Vec<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReintegrationStatus {
    #[serde(default)]
    pub stranded_records: u64,
    #[serde(default)]
    pub conflicts_materialized: u64,
    #[serde(default)]
    pub in_progress: bool,
}

/// P2P fast-path state. Purely observational: the filesystem is correct
/// with `enabled: false` and every peer disconnected, just slower.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct P2pStatus {
    #[serde(default)]
    pub enabled: bool,
    /// This node's dialable address, as published to the registry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_addr: Option<String>,
    #[serde(default)]
    pub peers: Vec<PeerStatus>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerStatus {
    pub node_id: u64,
    #[serde(default)]
    pub connected: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rtt_ms: Option<u64>,
    /// Milliseconds since this peer was last observed live.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pubkey: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_id: Option<String>,
    #[serde(default)]
    pub addrs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_unix: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p2p_updated_unix: Option<i64>,
    #[serde(default)]
    pub ro: bool,
    /// Whether this peer is a member of the active continuation epoch.
    #[serde(default)]
    pub epoch_member: bool,
    /// Offline designations this peer currently holds (paths).
    #[serde(default)]
    pub designations: Vec<String>,
    /// Cooperative-cache source stats for this peer, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coop: Option<SourceStatus>,
}

/// One partition as exposed by the control API.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PartitionStatus {
    pub id: String,
    pub root_path: String,
    pub lease: LeaseStatus,
}

/// Partition lease state (DESIGN.md §4). `held` is this node's own
/// authority; `holder`/`epoch` also describe the *foreign* holder once
/// this node has been deposed, which is what `lost` reports.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LeaseStatus {
    #[serde(default)]
    pub held: bool,
    #[serde(default)]
    pub holder: u64,
    #[serde(default)]
    pub epoch: u64,
    #[serde(default)]
    pub expires_in_ms: i64,
    /// This node was deposed: it refuses to ship and its unshipped
    /// journal is stranded pending reintegration.
    #[serde(default)]
    pub lost: bool,
}

/// Spool observability (DESIGN.md §12): outstanding unflushed metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpoolStatus {
    /// Journal records not yet shipped to S3.
    pub journal_backlog: u64,
    /// Highest log segment sequence shipped or applied so far.
    pub head_seq: u64,
    /// Foreign records skipped because a pending local op won (phase 2
    /// leaseless conflict detection).
    #[serde(default)]
    pub conflicts: u64,
    /// Last sync error, if the most recent round failed (S3 outage).
    pub last_ship_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStatus {
    pub used_bytes: u64,
    pub budget_bytes: u64,
    pub chunks: u64,
    /// Bytes protected from eviction by local pins.
    #[serde(default)]
    pub pinned_bytes: u64,
    /// In-flight (unflushed) write bytes staged to local disk, bounded
    /// independently of the chunk cache (plan 05a). Zero on daemons
    /// with no open dirty inode.
    #[serde(default)]
    pub staging_bytes: u64,
    #[serde(default)]
    pub staging_budget_bytes: u64,
}
