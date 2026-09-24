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
    /// Run one prune pass now. `path` restricts to the marked root at
    /// that path; `dry_run` forces dry-run regardless of arming.
    PruneRun {
        #[serde(default)]
        path: Option<String>,
        #[serde(default)]
        dry_run: bool,
    },
    /// List every marked prune root and its effective policy.
    PruneList,
    /// Run chunk + metadata-tree GC now, in the already-running daemon's
    /// process. `fjall` (unlike SQLite/WAL) refuses a second process's
    /// open of the same metadata store while a mount holds it, so
    /// `constellation gc run`/`gc verify` route through here whenever a
    /// daemon is up for the target state dir instead of opening the
    /// store directly.
    GcRun {
        #[serde(default)]
        verify_only: bool,
    },
    /// Run `fsck` now, in the already-running daemon's process — the
    /// same lock-avoidance reason as `GcRun` (plan 29 M3a): `fsck`
    /// otherwise opens the metadata store directly, which a live mount
    /// already holds under `fjall`'s single-process lock.
    FsckRun {
        #[serde(default)]
        repair: bool,
        #[serde(default)]
        force_release: Option<String>,
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
    /// Drop clean LRU chunks until total used bytes are at most
    /// `target_bytes` (default 0: free every clean chunk). Pinned and
    /// dirty chunks are never removed.
    CachePrune {
        #[serde(default)]
        target_bytes: u64,
    },
    /// Set the cluster-wide logical byte cap. `None` clears it (unlimited).
    SetQuota {
        max_bytes: Option<u64>,
    },
    /// Read the current quota and used bytes.
    GetQuota,
    /// Attach a new view (root, subtree, or `@snapshot` selector) to the
    /// running daemon, on its own `fuser::Session`. Lets a second CLI
    /// invocation extend an already-running daemon instead of starting a
    /// new process (plan 21).
    MountAdd {
        subtree: String,
        mountpoint: std::path::PathBuf,
        opts: MountViewOpts,
    },
    /// Detach one view, identified by where it is mounted (not by
    /// subtree: the same subtree may legitimately be mounted at two
    /// places).
    MountRemove {
        mountpoint: std::path::PathBuf,
    },
    /// Every view currently mounted by this daemon.
    MountList,
    /// Plan 30 §M4: `constellation repair drop-held <ino>` — discard the
    /// journal records held back behind `ino`'s unrecoverable chunk(s)
    /// into a conflict copy (see `StatusReport::held`).
    DropHeld {
        ino: u64,
    },
}

/// Per-view mount options, mirroring today's `mount` CLI flags that are
/// genuinely per-view rather than per-node.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MountViewOpts {
    #[serde(default)]
    pub allow_other: bool,
    #[serde(default)]
    pub fs_name: Option<String>,
    #[serde(default)]
    pub fuse_threads: Option<usize>,
    #[serde(default)]
    pub rw: bool,
    #[serde(default)]
    pub clone_name: Option<String>,
    #[serde(default)]
    pub ephemeral: bool,
}

/// One mounted view, as exposed by the control API (`MountList`,
/// `StatusReport::mounts`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountInfo {
    pub id: u64,
    pub subtree: String,
    pub mountpoint: String,
    /// Milliseconds since this view was mounted.
    pub mounted_ms_ago: u64,
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
    Quota {
        max_bytes: Option<u64>,
        used_bytes: u64,
    },
    Mounts {
        mounts: Vec<MountInfo>,
    },
    PruneRoots {
        roots: Vec<PruneRootStatus>,
    },
    /// `cli::gc::GcReport`, carried as opaque JSON: `constellation-api`
    /// sits below `cli` in the dependency graph and must not depend on
    /// its report types.
    GcReport {
        report: serde_json::Value,
    },
    /// `cli::fsck::FsckReport`, carried as opaque JSON for the same
    /// reason as [`Response::GcReport`].
    FsckReport {
        report: serde_json::Value,
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
    /// Plan 30 §M4: what the provider answered at each CAS edge
    /// (`constellation_store_s3::probe`).
    #[serde(default)]
    pub cas_probes: Vec<CasProbeStatus>,
    /// Plan 30 §M4: bucket versioning as seen on a probe PUT
    /// (informational).
    #[serde(default)]
    pub versioning: String,
}

/// One conditional-write probe (plan 30 §M4).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CasProbeStatus {
    pub name: String,
    pub observed: String,
    /// The answer has a meaning the CAS rules know.
    pub known: bool,
    /// The provider did not enforce the precondition atomically.
    pub violation: bool,
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
    /// Every view this daemon currently has mounted (plan 21, step 1).
    /// Was a single `mountpoint: String` field; a daemon serves exactly
    /// one view in every release before this one, so old single-view
    /// clients should read `mounts[0].mountpoint` if they need the
    /// scalar back, but the struct field itself is gone — multi-view is
    /// the whole point of this change and there is no sane single value
    /// to keep reporting once more than one view is live.
    #[serde(default)]
    pub mounts: Vec<MountInfo>,
    /// This node's cluster-unique id (scopes ino allocation, marks log
    /// segment origin).
    #[serde(default)]
    pub node_id: u64,
    /// Running binary version (`git describe` / package version).
    #[serde(default)]
    pub version: String,
    /// Whether this node's registry record is present and not retired.
    /// False after a successful `leave`, or when an admin retired us.
    #[serde(default = "default_true")]
    pub enrolled: bool,
    pub uptime_s: u64,
    pub spool: SpoolStatus,
    pub cache: CacheStatus,
    /// Write authority for the one metadata stream (`p0`).
    #[serde(default)]
    pub lease: LeaseStatus,
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
    /// Plan 30 §M3a speculation log and stranded-op recovery.
    #[serde(default)]
    pub speculation: SpeculationStatus,
    /// Plan 30 §M4: journal records held back behind unrecoverable
    /// pending chunks (everything else keeps shipping).
    #[serde(default)]
    pub held: HeldStatus,
    /// Cooperative cache (phase 5, DESIGN.md §7).
    #[serde(default)]
    pub coop: CoopStatus,
    /// Adaptive sequential and directory readahead.
    #[serde(default)]
    pub prefetch: PrefetchStatus,
    /// Phase 5b write-back queue and adaptive upload policy.
    #[serde(default)]
    pub writeback: WritebackStatus,
    #[serde(default)]
    pub forwarded_ok: u64,
    #[serde(default)]
    pub forwarded_err: u64,
    #[serde(default)]
    pub forward_p50_ms: Option<u64>,
    #[serde(default)]
    pub pushed_segments_applied: u64,
    /// Plan 30 §M2: forwarded requests the holder answered from
    /// `recent`/`completed` instead of re-executing (a retried rid).
    #[serde(default)]
    pub forward_dedup_hits: u64,
    /// Plan 30 §M2: same-rid forward retries this node's requester side
    /// made (same holder, or a redirected one) before falling back to
    /// the lease-acquisition path.
    #[serde(default)]
    pub forward_retries: u64,
    /// Plan 30 §M2: in-doubt ops the lease-path resolved against
    /// `completed` instead of re-executing (a genuine takeover finding
    /// the op already happened).
    #[serde(default)]
    pub forward_indoubt_resolved: u64,
    /// Plan 30 §M13: the S3 inbox (forwarding without P2P).
    #[serde(default)]
    pub inbox: InboxStatus,
    #[serde(default)]
    pub placement_reason: Option<String>,
    /// Optional cluster-wide logical byte cap and current used bytes.
    #[serde(default)]
    pub quota: QuotaStatus,
    /// Read-time atime (plan 20). Default (`off`) leaves every counter
    /// zero and `mode` "off".
    #[serde(default)]
    pub atime: AtimeStatus,
    /// Retention pruning (plan 22). Default (no marked roots) leaves
    /// every counter zero.
    #[serde(default)]
    pub prune: PruneStatus,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AtimeStatus {
    /// "off", "relatime", or "lazy".
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub queued: u64,
    #[serde(default)]
    pub coalesced: u64,
    #[serde(default)]
    pub applied: u64,
    #[serde(default)]
    pub dropped_cap: u64,
    #[serde(default)]
    pub forward_ok: u64,
    #[serde(default)]
    pub forward_err: u64,
    #[serde(default)]
    pub local_only: u64,
    #[serde(default)]
    pub skew_clamped: u64,
}

/// Retention-pruning counters (plan 22), surfaced in `constellation
/// status`, on the web UI, and in `/metrics`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PruneStatus {
    #[serde(default)]
    pub runs: u64,
    #[serde(default)]
    pub roots: u64,
    #[serde(default)]
    pub armed_roots: u64,
    #[serde(default)]
    pub unparseable_roots: u64,
    #[serde(default)]
    pub inert_roots: u64,
    #[serde(default)]
    pub entries_examined: u64,
    #[serde(default)]
    pub selected: u64,
    #[serde(default)]
    pub deleted: u64,
    #[serde(default)]
    pub bytes_deleted: u64,
    #[serde(default)]
    pub bytes_freed: u64,
    #[serde(default)]
    pub skipped_reverify: u64,
    #[serde(default)]
    pub skipped_forward_err: u64,
    #[serde(default)]
    pub skipped_hardlink: u64,
    #[serde(default)]
    pub skipped_repartition: u64,
    #[serde(default)]
    pub leases_acquired: u64,
    #[serde(default)]
    pub refused_lag: u64,
    #[serde(default)]
    pub last_run_unix_ms: u64,
    /// Last setxattr policy rejection: `(expression, byte_offset, message)`.
    #[serde(default)]
    pub last_parse_error: Option<(String, usize, String)>,
}

/// One marked prune root, for `constellation prune ls`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PruneRootStatus {
    pub path: String,
    pub policy: String,
    #[serde(default)]
    pub armed: bool,
    #[serde(default)]
    pub valid: bool,
    /// A note when the policy is unparseable or inert.
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QuotaStatus {
    /// `None` = unlimited.
    #[serde(default)]
    pub max_bytes: Option<u64>,
    #[serde(default)]
    pub used_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PrefetchStatus {
    #[serde(default)]
    pub inflight: u64,
    #[serde(default)]
    pub queued: u64,
    #[serde(default)]
    pub streams: u64,
    #[serde(default)]
    pub window_bytes: u64,
    #[serde(default)]
    pub stalls: u64,
    #[serde(default)]
    pub gate_target: u32,
    #[serde(default)]
    pub scan_ahead_files: u64,
    #[serde(default)]
    pub scan_ahead_bytes: u64,
    /// Times a stream's queued readahead was cancelled because the reader
    /// stopped consuming it (DESIGN.md §7 "abandoned reader").
    #[serde(default)]
    pub abandoned: u64,
    /// Chunks dropped from queues by those cancellations — GETs saved from
    /// readers that never came back for them.
    #[serde(default)]
    pub abandoned_chunks: u64,
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
    pub existence_bloom_hits: u64,
    #[serde(default)]
    pub existence_chunk_ref_hits: u64,
    #[serde(default)]
    pub existence_misses: u64,
    #[serde(default)]
    pub existence_peer_hints: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CoopStatus {
    #[serde(default)]
    pub peer_hits: u64,
    /// Soft negatives: every peer decline (busy, absent, recently
    /// removed); see `peer_false_positives` / `peer_stale_misses`.
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
    /// Plan 30 §M15: `exact` (mirrors + reconciliation) or `bloom`.
    #[serde(default)]
    pub digest_mode: String,
    /// Peer declined with `Absent` a chunk our digest said it held.
    #[serde(default)]
    pub peer_false_positives: u64,
    /// Peer had dropped the chunk within its recent-removal window.
    #[serde(default)]
    pub peer_stale_misses: u64,
    /// Digest-plane traffic (summaries, deltas, rounds, or blooms).
    #[serde(default)]
    pub digest_bytes_sent: u64,
    #[serde(default)]
    pub digest_bytes_received: u64,
    #[serde(default)]
    pub digest_messages: u64,
    /// Microseconds spent building, applying and answering digests.
    #[serde(default)]
    pub digest_cpu_us: u64,
    #[serde(default)]
    pub reconcile_sessions: u64,
    #[serde(default)]
    pub reconcile_rounds: u64,
    #[serde(default)]
    pub reconcile_failures: u64,
    /// Part of `digest_cpu_us` spent on reconciliation rounds.
    #[serde(default)]
    pub reconcile_cpu_us: u64,
    /// Keys in this node's published servable set.
    #[serde(default)]
    pub local_set_entries: u64,
    /// Entries held about peers' caches (mirror keys or bloom inserts).
    #[serde(default)]
    pub peer_set_entries: u64,
    #[serde(default)]
    pub peer_set_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceStatus {
    pub id: String,
    /// First-byte EWMA from successful transfers only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttfb_ms_ewma: Option<f64>,
    /// Goodput EWMA from successful transfers only (per-stream body rate).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goodput_mbps_ewma: Option<f64>,
    /// Aggregate path throughput across concurrent streams (wall-clock).
    /// Prefer this over `goodput_mbps_ewma` when displaying "S3 BW" to operators.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate_mbps_ewma: Option<f64>,
    /// Successful fetches (EWMA complementary to miss/err).
    #[serde(default)]
    pub hit_rate: f64,
    /// Soft negatives (declines). Previously folded into `err_rate`.
    #[serde(default)]
    pub miss_rate: f64,
    /// Hard negatives (transport/hash failures).
    #[serde(default)]
    pub err_rate: f64,
    /// Successful transfers contributing to lat/BW EWMAs.
    #[serde(default)]
    pub ok_samples: u64,
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

/// Plan 30 §M3a: this node's speculation log (`constellation_meta::
/// store::spec`) — effects applied ahead of the durable log, and the
/// stranded ops being replayed by rid.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SpeculationStatus {
    /// Outstanding shadows (accepted forwarded ops) and `Exists` hints not
    /// yet confirmed by the log. While non-zero this node does not publish
    /// commits.
    #[serde(default)]
    pub outstanding: u64,
    /// Stranded ops queued for replay by rid.
    #[serde(default)]
    pub pending_replay: u64,
    /// Speculative entries rolled back because a later epoch stranded
    /// them, since start.
    #[serde(default)]
    pub rolled_back: u64,
    /// Stranded ops replayed by rid and accepted, since start.
    #[serde(default)]
    pub stranded_replayed: u64,
    /// Stranded ops whose replay was refused and materialized as a
    /// `.constellation-conflict/` copy, since start.
    #[serde(default)]
    pub replay_conflicts: u64,
    /// Plan 30 §M3b: this node's own journaled transactions captured as
    /// speculation and not yet shipped (a holder's unshipped journal).
    /// Unlike `outstanding`, these do not stop a publish: the publisher
    /// substitutes their before-images.
    #[serde(default)]
    pub local: u64,
    /// Plan 30 §M3b: this node's own unshipped transactions rolled back
    /// because it was deposed (and queued for replay by rid), since start.
    #[serde(default)]
    pub local_rolled_back: u64,
    /// Plan 30 §M3b: deposition recoveries run (rollback plus replay, or
    /// the capture-off rebuild), since start.
    #[serde(default)]
    pub depositions: u64,
    /// Plan 30 §M3b: epoch-marker segments this node shipped right after a
    /// takeover, since start.
    #[serde(default)]
    pub epoch_markers: u64,
    /// Plan 30 §M3b: this node holds the lease but its takeover gate has
    /// not completed (a marker or a local replay failed); new mutations are
    /// refused until a sync round completes it.
    #[serde(default)]
    pub gate_pending: bool,
    /// Plan 30 §M4: refused replays whose `.constellation-conflict/` copy
    /// could not be made yet (retried with backoff; later ops are not held
    /// up), and those failing for at least 10 s — a stall worth looking
    /// at (the node then also asks for the lease to make them locally).
    #[serde(default)]
    pub copies_pending: u64,
    #[serde(default)]
    pub copies_stalled: u64,
}

/// Plan 30 §M13: the S3 inbox — a non-holder's forwarded mutations
/// when it has no P2P path to the holder, and the holder's polling of
/// them. Requester-side counters (`submitted_*`, `resubmitted_ops`,
/// `unavailable`, `pending_ops`, `next_n`) and holder-side ones
/// (`executed_ops`, `refused_ops`, `deduped_ops`, `drained_*`, `polls`,
/// `poll_hits`, `gc_deleted`, `tracked_requesters`) are both reported by
/// every node; whichever role it plays moves.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InboxStatus {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub submitted_batches: u64,
    #[serde(default)]
    pub submitted_ops: u64,
    /// Ops re-submitted under a newer epoch because a takeover stranded
    /// their batch.
    #[serde(default)]
    pub resubmitted_ops: u64,
    /// Forwards the inbox could not take (no live holder to leave them
    /// with, S3 refused the batch, or the in-doubt deadline passed); they
    /// took the lease path.
    #[serde(default)]
    pub unavailable: u64,
    /// Submitted ops still waiting for their outcome in the log.
    #[serde(default)]
    pub pending_ops: u64,
    /// This node's next batch number under its current epoch.
    #[serde(default)]
    pub next_n: u64,
    #[serde(default)]
    pub executed_ops: u64,
    #[serde(default)]
    pub refused_ops: u64,
    /// Batch positions answered without executing (rid already had an
    /// outcome, or the position was below the watermark).
    #[serde(default)]
    pub deduped_ops: u64,
    #[serde(default)]
    pub drained_batches: u64,
    #[serde(default)]
    pub drained_ops: u64,
    #[serde(default)]
    pub polls: u64,
    #[serde(default)]
    pub poll_hits: u64,
    #[serde(default)]
    pub gc_deleted: u64,
    #[serde(default)]
    pub tracked_requesters: u64,
    /// The write-eligible roster this node's authority core last read
    /// (the registry, refreshed periodically and at a holder's inbox
    /// tenure start): who a holder polls with P2P off. A bench waits for
    /// it to name every node before timing.
    #[serde(default)]
    pub roster: Vec<u64>,
    /// Round-2 instrumentation, requester side: mean time from queueing
    /// an op to its batch being durable, from durable to its outcome
    /// applied from the log, and their sum.
    #[serde(default)]
    pub avg_queue_wait_ms: f64,
    #[serde(default)]
    pub avg_outcome_wait_ms: f64,
    #[serde(default)]
    pub avg_round_trip_ms: f64,
    /// Holder side: mean time from a batch's submission stamp to its
    /// poll hit (requester and holder clocks), and per-hit execute time.
    #[serde(default)]
    pub avg_pickup_ms: f64,
    #[serde(default)]
    pub avg_execute_ms: f64,
    /// Requester side: ops per batch, mean and maximum.
    #[serde(default)]
    pub avg_batch_ops: f64,
    #[serde(default)]
    pub largest_batch_ops: u64,
    /// Plan 30 M13 round 3b (the hybrid): whether this node's inbox
    /// demand is currently sustained enough that it is asking for the
    /// lease; how many times it started asking; the lease requests it
    /// sent for that; ops it forwarded through the inbox vs. ops it
    /// executed locally as holder.
    #[serde(default)]
    pub escalated: bool,
    #[serde(default)]
    pub escalations: u64,
    #[serde(default)]
    pub lease_requests: u64,
    #[serde(default)]
    pub inbox_ops: u64,
    #[serde(default)]
    pub local_ops: u64,
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
    /// Active relay policy: `disabled`, `default`, or a custom URL label.
    #[serde(default)]
    pub relay: String,
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
    /// Peer binary version from the registry (`git describe` / package).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
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
    /// Synthetic S3 backend row (`node_id` 0). Always listed first among
    /// [`P2pStatus::peers`] so the UI can compare lat/BW/hit% with peers.
    #[serde(default)]
    pub s3: bool,
    /// Connectivity path: `direct`, `relay`, `unknown`, or empty for S3.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub path: String,
    /// Plan 30 §M4: every open QUIC path to this peer right now.
    #[serde(default)]
    pub paths: PeerPathsStatus,
}

/// Plan 30 §M4: the open network paths of the pooled connection to one
/// peer (iroh 1.x on noq keeps several open at once: typically the relay
/// path and, once holepunching succeeds, a direct one).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerPathsStatus {
    /// Kind of the path application data currently uses: `direct`,
    /// `relay`, or empty when no connection is pooled.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub selected: String,
    /// Open direct (IP) paths.
    #[serde(default)]
    pub direct: u32,
    /// Open relay paths.
    #[serde(default)]
    pub relay: u32,
    /// More than one path is open, so a failure of the selected one can
    /// fail over without a new handshake.
    #[serde(default)]
    pub multipath: bool,
    /// Round-trip estimate of each open path, `kind:ms`, selected first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rtts: Vec<String>,
}

/// Plan 30 §M4: records held back behind unrecoverable pending chunks.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HeldStatus {
    /// Journaled transactions held back (the seeds and everything that
    /// depends on them).
    #[serde(default)]
    pub transactions: u64,
    #[serde(default)]
    pub records: u64,
    /// The oldest held journal seq.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_seq: Option<u64>,
    /// An uncaptured transaction (holder capture off) was held, so every
    /// transaction after it is held too.
    #[serde(default)]
    pub opaque: bool,
    /// Each inode with unrecoverable chunks: `constellation repair
    /// drop-held <ino>` discards its held records.
    #[serde(default)]
    pub inodes: Vec<HeldInodeStatus>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HeldInodeStatus {
    pub ino: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Hex hashes of the pending chunks gone from the local cache.
    #[serde(default)]
    pub missing_chunks: Vec<String>,
    /// Held transactions whose manifest names them.
    #[serde(default)]
    pub seeds: u64,
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
    /// `run_managed_sync_round` invocations that ran to completion (plan
    /// 30 M2b measurement counter — see `shipper::SpoolInfo`'s doc).
    #[serde(default)]
    pub ship_rounds_completed: u64,
    /// Rounds dropped mid-flight for a request that still cancels one
    /// (plan 30 M2b: `Mutate`/`Forward` no longer do — see
    /// `shipper::SpoolInfo`'s doc).
    #[serde(default)]
    pub ship_rounds_cancelled: u64,
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
    /// independently of the chunk cache (plan 07). Zero on daemons
    /// with no open dirty inode.
    #[serde(default)]
    pub staging_bytes: u64,
    #[serde(default)]
    pub staging_budget_bytes: u64,
}
