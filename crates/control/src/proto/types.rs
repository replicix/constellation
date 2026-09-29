//! Result types carried by the method table (plan 31 §9.2).
//!
//! Everything from `StatusReport` down to `DirectoryEntry` is copied from
//! the retired `crates/api/src/types.rs` (deleted in C5b, so this is the
//! surviving home, not a duplicate) with a `JsonSchema` derive added so the
//! schema document covers it. They keep today's field names and
//! `#[serde(default)]` markers: a JSON client that predates a counter still
//! decodes a newer daemon's report.
//!
//! ## Rules every type in here follows
//!
//! - **Postcard is positional.** The binary encoding is not
//!   self-describing, so no type here uses `skip_serializing_if`,
//!   `flatten`, an internally tagged enum, or a bare `serde_json::Value`
//!   (which needs `deserialize_any`). Free-form JSON goes through
//!   [`JsonValue`], which is inline JSON under the JSON encoding and a JSON
//!   string under postcard.
//! - **Results are objects**, never a bare array, so a method can gain a
//!   field later without a wire break (`pin.list` is `{"pins": [...]}`).
//! - **Secrets are [`Secret`]**, which serializes as a plain string but
//!   prints as `[redacted]` in `Debug`, so a stray `tracing::debug!(?params)`
//!   cannot leak a passphrase.

use crate::proto::{ByteBuf, JsonValue, Secret};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Mirror of [`constellation_types::Rdev`] for the schema (the `types`
/// crate carries no `schemars` dependency): a portable `(major, minor)`
/// pair.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RdevSchema {
    pub major: u32,
    pub minor: u32,
}

/// Per-view mount options, mirroring today's `mount` CLI flags that are
/// genuinely per-view rather than per-node.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MountInfo {
    pub id: u64,
    pub subtree: String,
    pub mountpoint: String,
    /// Milliseconds since this view was mounted.
    pub mounted_ms_ago: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DirectoryEntry {
    pub name: String,
    pub path: String,
    pub ino: u64,
    pub kind: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
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
    /// Plan 31 §7: the portable `(major, minor)` pair, never an OS's
    /// packed `dev_t`.
    #[schemars(with = "RdevSchema")]
    pub rdev: constellation_types::Rdev,
    #[serde(default)]
    pub manifest: Option<ManifestStatus>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ManifestStatus {
    pub chunk_size: u32,
    pub chunk_count: u64,
    pub spilled: bool,
    /// The manifest's own length (the inode's `size` can differ while a
    /// setattr and a manifest commit are apart).
    #[serde(default)]
    pub file_len: u64,
    /// BLAKE3 of the encoded manifest: two nodes serving the same file
    /// version agree on it.
    #[serde(default)]
    pub digest: String,
    /// `index:chunk hash` of the first chunks (or the spilled list's
    /// hash), for telling apart what two nodes serve.
    #[serde(default)]
    pub chunks: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct CasProbeStatus {
    pub name: String,
    pub observed: String,
    /// The answer has a meaning the CAS rules know.
    pub known: bool,
    /// The provider did not enforce the precondition atomically.
    pub violation: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct CacheEntryStatus {
    pub hash: String,
    pub size: u64,
    pub state: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotStatus {
    pub id: String,
    pub path: String,
    pub name: String,
    pub root_hash: String,
    pub created_unix_ms: i64,
}

/// One offline designation, as exposed by the control API.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct DesignationStatus {
    pub path: String,
    pub designee: u64,
    pub read_only: bool,
}

/// Plan 30 §M11: one live delegation.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct DelegationStatus {
    pub dir: u64,
    pub path: String,
    pub node: u64,
    pub gen: u64,
    /// Phase 2b: an offline designation (never recalled by TTL or
    /// placement).
    #[serde(default)]
    pub designated: bool,
    /// Plan 30 §M12: `"<idx>/<count>"` for a hash range of the
    /// directory's names; empty for the whole directory.
    #[serde(default)]
    pub range: String,
}

/// Plan 30 §M11: this node's delegation state.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct DelegationReport {
    /// `CONSTELLATION_DELEGATION` is on (and P2P).
    #[serde(default)]
    pub enabled: bool,
    /// The live table.
    #[serde(default)]
    pub table: Vec<DelegationStatus>,
    /// Delegations this node holds: `(dir, gen, until_ms, stopped,
    /// streamed_through, executed, parked)`.
    #[serde(default)]
    pub mine: Vec<(u64, u64, i64, bool, u64, u64, usize)>,
    /// As the root: `(dir, node, gen, cursor, until_ms, recall, ended)`.
    #[serde(default)]
    pub gens: Vec<(u64, u64, u64, u64, i64, String, bool)>,
    #[serde(default)]
    pub executed: u64,
    /// Ops the FUSE fast path executed here as the delegate (not
    /// counted in `executed`, which is the core's).
    #[serde(default)]
    pub fast_path_executed: u64,
    /// Plan 30 §M12: ops the root's fast path sent through the core
    /// because a live delegation owned their keys.
    #[serde(default)]
    pub fast_path_routed: u64,
    #[serde(default)]
    pub forwarded_to_delegate: u64,
    #[serde(default)]
    pub deps_waits: u64,
    #[serde(default)]
    pub parked_expired: u64,
    #[serde(default)]
    pub not_owner: u64,
    #[serde(default)]
    pub installed: u64,
    #[serde(default)]
    pub streamed_txs: u64,
    #[serde(default)]
    pub stream_refused: u64,
    #[serde(default)]
    pub renewals: u64,
    #[serde(default)]
    pub renewals_refused: u64,
    #[serde(default)]
    pub recalls_received: u64,
    #[serde(default)]
    pub delegated: u64,
    #[serde(default)]
    pub appended_txs: u64,
    #[serde(default)]
    pub stream_refusals: u64,
    #[serde(default)]
    pub deps_unsatisfied_at_append: u64,
    #[serde(default)]
    pub cross_subtree: u64,
    #[serde(default)]
    pub recalls_sent: u64,
    #[serde(default)]
    pub recalls_drained: u64,
    #[serde(default)]
    pub recalls_expired: u64,
    #[serde(default)]
    pub reclaimed: u64,
    #[serde(default)]
    pub ended: u64,
    #[serde(default)]
    pub deps_overflow_to_root: u64,
    #[serde(default)]
    pub exec_parked: u64,
    /// Delegate rows stranded here by a recall (rolled back, replayed).
    #[serde(default)]
    pub stranded: u64,
    // ---- phase 2b ----
    /// As the root: `(gen, kind, backup)` per generation (kind: Manual,
    /// Placed, Designated).
    #[serde(default)]
    pub kinds: Vec<(u64, String, u64)>,
    /// As a delegate: `(gen, backup, backup_acked)`.
    #[serde(default)]
    pub backups: Vec<(u64, u64, u64)>,
    /// The placement's busiest subtrees, `(dir, node, node_ops,
    /// subtree_ops)` over the window (M12's input).
    #[serde(default)]
    pub placement: Vec<(u64, u64, u64, u64)>,
    #[serde(default)]
    pub inherited: u64,
    #[serde(default)]
    pub refused_designated: u64,
    #[serde(default)]
    pub designated: u64,
    #[serde(default)]
    pub redelegated: u64,
    #[serde(default)]
    pub seals_sent: u64,
    #[serde(default)]
    pub sealed_drained: u64,
    #[serde(default)]
    pub restreams: u64,
    #[serde(default)]
    pub backup_appends: u64,
    #[serde(default)]
    pub backup_acks: u64,
    #[serde(default)]
    pub acks_parked: u64,
    #[serde(default)]
    pub backup_persisted: u64,
    #[serde(default)]
    pub backup_seals: u64,
    #[serde(default)]
    pub place_evaluations: u64,
    #[serde(default)]
    pub place_delegated: u64,
    #[serde(default)]
    pub place_recalled: u64,
    #[serde(default)]
    pub place_skipped_cooldown: u64,
    #[serde(default)]
    pub place_skipped_unreachable: u64,
    /// Plan 30 §M12: hot directories split into hash ranges, and range
    /// generations recalled by the placement.
    #[serde(default)]
    pub place_splits: u64,
    #[serde(default)]
    pub place_range_recalls: u64,
    #[serde(default)]
    pub read_index_served: u64,
    #[serde(default)]
    pub read_grants: u64,
}

/// One pinned subtree on this node.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
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

/// The FUSE request watchdog (`crate::fuse_watch`, EC2 campaign 7
/// B-2): requests in flight, and those unanswered past
/// `CONSTELLATION_FUSE_REQUEST_STALL_S`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct FuseRequestsStatus {
    /// Requests being handled right now.
    #[serde(default)]
    pub in_flight: u64,
    /// Of them, reported as stalled (older than the threshold).
    #[serde(default)]
    pub stalled: u64,
    /// Requests ever reported as stalled since the daemon started.
    #[serde(default)]
    pub stalled_total: u64,
    /// Of those, the ones that did complete eventually.
    #[serde(default)]
    pub stalled_completed: u64,
    /// The oldest request in flight, in seconds (blocking locks aside).
    #[serde(default)]
    pub oldest_s: u64,
    #[serde(default)]
    pub stall_threshold_s: u64,
    /// The stalled requests (and blocking lock waits past the
    /// threshold, marked `blocking`), oldest first.
    #[serde(default)]
    pub stalled_requests: Vec<StalledFuseRequest>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct StalledFuseRequest {
    pub op: String,
    pub ino: u64,
    pub age_s: u64,
    /// What the handler last noted it was waiting on.
    pub stage: String,
    /// The OS thread handling it.
    pub tid: u32,
    /// A blocking lock request: unbounded by design, not a stall.
    #[serde(default)]
    pub blocking: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
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
    /// Plan 30 §M11.
    #[serde(default)]
    pub delegation: DelegationReport,
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
    /// Plan 30 §M6: the session wait on local reads (read-your-writes,
    /// monotonic reads) and its latency distribution.
    #[serde(default)]
    pub session: SessionStatus,
    /// Plan 30 §M8: `cto=strict` and read delegations.
    #[serde(default)]
    pub cto: CtoStatus,
    /// Plan 30 §M14: `--locks` and cross-node lock grants.
    #[serde(default)]
    pub locks: LockStatus,
    /// Plan 30 §M9: acknowledgement policy, backups, seals.
    #[serde(default)]
    pub ack: AckStatus,
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
    /// Plan 30 §M7: the direct log stream (this node's subscription to
    /// the holder, or the subscribers it serves as the holder).
    #[serde(default)]
    pub log_stream: LogStreamStatus,
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
    /// EC2 campaign 8 A-1: this node's own S3 path, and what its ops did
    /// without it.
    #[serde(default)]
    pub own_s3: OwnS3Status,
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
    /// The FUSE request watchdog (EC2 campaign 7 B-2).
    #[serde(default)]
    pub fuse_requests: FuseRequestsStatus,
    /// Object-store requests this daemon has issued since it started
    /// (every filesystem it serves), by kind and by key area.
    #[serde(default)]
    pub s3: S3RequestStatus,
}

/// Object-store requests by kind (one per call the daemon makes; a
/// request's own retries inside the S3 client are not counted again),
/// and by `<KIND> <area>`, the area being the key's first segment under
/// the filesystem prefix (`log`, `leases`, `nodes`, `chunks`, …).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct S3RequestStatus {
    #[serde(default)]
    pub get: u64,
    #[serde(default)]
    pub head: u64,
    #[serde(default)]
    pub put: u64,
    #[serde(default)]
    pub list: u64,
    #[serde(default)]
    pub delete: u64,
    #[serde(default)]
    pub copy: u64,
    #[serde(default)]
    pub by_area: std::collections::BTreeMap<String, u64>,
    /// Requests that failed other than as an error of the request itself
    /// (see `last_answered_unix_ms`): a transport error or timeout after
    /// the client's own retries, a 5xx, a refusal.
    #[serde(default)]
    pub unanswered: u64,
    /// When S3 last answered a request (unix ms; 0: never) — a success,
    /// or an error of the request itself (not found, a failed
    /// precondition), which proves the path works as well.
    #[serde(default)]
    pub last_answered_unix_ms: i64,
    /// When a request last went unanswered (unix ms; 0: never), and why.
    #[serde(default)]
    pub last_unanswered_unix_ms: i64,
    #[serde(default)]
    pub last_unanswered_error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct QuotaStatus {
    /// `None` = unlimited.
    #[serde(default)]
    pub max_bytes: Option<u64>,
    #[serde(default)]
    pub used_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
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
    /// Pending rows for chunks other nodes forwarded in a manifest while
    /// they were still uploading there (`--write-mode back` on a
    /// non-owner): what this node's ship waits for their reports on.
    #[serde(default)]
    pub remote_chunks_awaited: u64,
    /// The oldest of them, in seconds (0: none).
    #[serde(default)]
    pub remote_chunks_oldest_s: u64,
    /// EC2 finding 1: drains that handed their chunks to a peer because
    /// this node's own uploads made no progress (this node's S3 path
    /// was down), how many succeeded, and the chunks they covered.
    #[serde(default)]
    pub handoffs_sent: u64,
    #[serde(default)]
    pub handoffs_ok: u64,
    #[serde(default)]
    pub handoff_chunks: u64,
    /// Chunks this node fetched from a peer and uploaded for it.
    #[serde(default)]
    pub handoff_chunks_accepted: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
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
    /// Chunks fetched from another member of an open continuation epoch.
    #[serde(default)]
    pub epoch_member_fetches: u64,
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
    /// Fetches of a chunk no mirror listed yet from the node that wrote
    /// the manifest naming it (a file another node just wrote): served,
    /// and declined (the fetch then went on to S3).
    #[serde(default)]
    pub fresh_hint_hits: u64,
    #[serde(default)]
    pub fresh_hint_misses: u64,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SourceStatus {
    pub id: String,
    /// First-byte EWMA from successful transfers only.
    #[serde(default)]
    pub ttfb_ms_ewma: Option<f64>,
    /// Goodput EWMA from successful transfers only (per-stream body rate).
    #[serde(default)]
    pub goodput_mbps_ewma: Option<f64>,
    /// Aggregate path throughput across concurrent streams (wall-clock).
    /// Prefer this over `goodput_mbps_ewma` when displaying "S3 BW" to operators.
    #[serde(default)]
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
    #[serde(default)]
    pub transport_rtt_ms: Option<f64>,
    #[serde(default)]
    pub path: String,
}

/// Live continuation-epoch snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct EpochStatus {
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub epoch_id: Option<String>,
    #[serde(default)]
    pub members: Vec<u64>,
    /// Plan 30 §M10: `f` (the filesystem's `epoch_slack`).
    #[serde(default)]
    pub epoch_slack: u32,
    /// Plan 30 §M10: the node whose lease the current (or last) epoch
    /// carried.
    #[serde(default)]
    pub carrier: Option<u64>,
    /// Plan 30 §M10: this node's last issued heartbeat promise (unix ms;
    /// 0: none).
    #[serde(default)]
    pub promise_until_ms: i64,
    /// EC2 follow-up 3c: this node's S3 is failing while a live member's
    /// works — its own outage, not the bucket's: it proposes no epoch.
    #[serde(default)]
    pub own_s3_outage: bool,
    /// Proposals this node made (sent to its members).
    #[serde(default)]
    pub proposals: u64,
    /// Plan 30 §M10 counters (the core's): promises persisted and PUT;
    /// promise requests answered and refused; TTL-takeover promise checks
    /// run, takeovers refused for too few promises, flush re-claims
    /// exempt; activations that found this node's claim stale.
    #[serde(default)]
    pub promise_puts: u64,
    #[serde(default)]
    pub promise_requests_answered: u64,
    #[serde(default)]
    pub promise_requests_refused: u64,
    #[serde(default)]
    pub promise_checks: u64,
    #[serde(default)]
    pub takeovers_refused_promises: u64,
    #[serde(default)]
    pub promise_flush_exempt: u64,
    #[serde(default)]
    pub stale_claims: u64,
    /// Members keep following the hold owner's log stream during an
    /// epoch: journal transactions the hold owner streamed ahead, those
    /// this member installed, and this member's forwards the stream
    /// answered.
    #[serde(default)]
    pub streamed_ahead: u64,
    #[serde(default)]
    pub streamed_installed: u64,
    #[serde(default)]
    pub forwards_streamed: u64,
    /// Epoch hold transfers declined because the requester had not
    /// applied the holder's whole log.
    #[serde(default)]
    pub handoffs_behind: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct InboxStatus {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub submitted_batches: u64,
    #[serde(default)]
    pub submitted_ops: u64,
    /// Ops re-submitted by rid: under a newer epoch because a takeover
    /// stranded their batch, or in a new batch because one they shared
    /// was withdrawn.
    #[serde(default)]
    pub resubmitted_ops: u64,
    /// Requester: batches this node withdrew (overwrote with a tombstone)
    /// before forwarding their op over P2P or holding it back.
    #[serde(default)]
    pub withdrawn_ops: u64,
    /// Holder: withdrawn batches its polls read and stepped past.
    #[serde(default)]
    pub tombstones_read: u64,
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
    /// EC2 finding 2: rounds in which this node, holding the lease,
    /// kept it from wanters across a P2P partition from the nodes using
    /// it.
    #[serde(default)]
    pub leases_kept_for_p2p_side: u64,
    #[serde(default)]
    pub inbox_ops: u64,
    #[serde(default)]
    pub local_ops: u64,
}

/// P2P fast-path state. Purely observational: the filesystem is correct
/// with `enabled: false` and every peer disconnected, just slower.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct P2pStatus {
    #[serde(default)]
    pub enabled: bool,
    /// This node's dialable address, as published to the registry.
    #[serde(default)]
    pub node_addr: Option<String>,
    /// Active relay policy: `disabled`, `default`, or a custom URL label.
    #[serde(default)]
    pub relay: String,
    #[serde(default)]
    pub peers: Vec<PeerStatus>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct PeerStatus {
    pub node_id: u64,
    #[serde(default)]
    pub connected: bool,
    #[serde(default)]
    pub rtt_ms: Option<u64>,
    /// Milliseconds since this peer was last observed live.
    #[serde(default)]
    pub last_seen_ms: Option<u64>,
    #[serde(default)]
    pub hostname: Option<String>,
    /// Peer binary version from the registry (`git describe` / package).
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub pubkey: Option<String>,
    #[serde(default)]
    pub endpoint_id: Option<String>,
    #[serde(default)]
    pub addrs: Vec<String>,
    #[serde(default)]
    pub created_unix: Option<i64>,
    #[serde(default)]
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
    #[serde(default)]
    pub coop: Option<SourceStatus>,
    /// Synthetic S3 backend row (`node_id` 0). Always listed first among
    /// [`P2pStatus::peers`] so the UI can compare lat/BW/hit% with peers.
    #[serde(default)]
    pub s3: bool,
    /// Connectivity path: `direct`, `relay`, `unknown`, or empty for S3.
    #[serde(default)]
    pub path: String,
    /// Plan 30 §M4: every open QUIC path to this peer right now.
    #[serde(default)]
    pub paths: PeerPathsStatus,
}

/// Plan 30 §M4: the open network paths of the pooled connection to one
/// peer (iroh 1.x on noq keeps several open at once: typically the relay
/// path and, once holepunching succeeds, a direct one).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct PeerPathsStatus {
    /// Kind of the path application data currently uses: `direct`,
    /// `relay`, or empty when no connection is pooled.
    #[serde(default)]
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
    #[serde(default)]
    pub rtts: Vec<String>,
}

/// Plan 30 §M6: the session wait FUSE reads go through
/// (`constellation_meta::session`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SessionStatus {
    /// `CONSTELLATION_SESSION_WAIT_MS` (0: disabled).
    #[serde(default)]
    pub budget_ms: u64,
    /// Reads checked, and how each ended: at once on the applied
    /// position, at once on covering speculation, after a wait, or
    /// degraded after the whole budget.
    #[serde(default)]
    pub reads: u64,
    #[serde(default)]
    pub fast: u64,
    #[serde(default)]
    pub covered: u64,
    #[serde(default)]
    pub waited: u64,
    #[serde(default)]
    pub timeouts: u64,
    /// Timeouts while M4 held-back rows existed (a held row stalls the
    /// shipped-through position).
    #[serde(default)]
    pub degraded_held: u64,
    /// Reads that waited for a queued replay of this node's own write.
    #[serde(default)]
    pub replay_blocked: u64,
    /// Waits by log2 milliseconds: `[0]` < 1 ms, `[i]` < 2^i ms.
    #[serde(default)]
    pub waits_ms: Vec<u64>,
    #[serde(default)]
    pub wait_ms_total: u64,
    /// Times the `observed` watermark rose (replies whose effects were
    /// not installed here).
    #[serde(default)]
    pub raised: u64,
    /// `CONSTELLATION_SESSION_WATERMARK_TTL_MS` (0: never dropped).
    #[serde(default)]
    pub watermark_ttl_ms: u64,
    /// Watermarks dropped after staying unreached for the TTL, and
    /// stream dependencies voided because the delegation table showed
    /// their generation ended before this incarnation (EC2 campaign 7
    /// B-2: either one left every read on the node degraded for good).
    #[serde(default)]
    pub abandoned: u64,
    #[serde(default)]
    pub voided_ended: u64,
}

/// Plan 30 §M8: `cto=strict` reads, read delegations and recalls.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct CtoStatus {
    /// This mount is `--cto strict`.
    #[serde(default)]
    pub strict: bool,
    /// This node, as sequencer, grants read delegations.
    #[serde(default)]
    pub grants_enabled: bool,
    // ---- reader side ----
    /// Strict opens, lookups and listings, and how each was answered: by
    /// this node as the sequencer, under a read delegation, after a
    /// ReadIndex round trip, by tailing S3 (no live sequencer, or no
    /// P2P), or degraded (no answer in the budget).
    #[serde(default)]
    pub strict_reads: u64,
    #[serde(default)]
    pub holder_local: u64,
    #[serde(default)]
    pub delegation_local: u64,
    #[serde(default)]
    pub read_index: u64,
    #[serde(default)]
    pub s3_tail: u64,
    #[serde(default)]
    pub degraded: u64,
    /// ReadIndex round trip plus the wait for its position, total ms and
    /// log2 histogram (`[0]` < 1 ms, `[i]` < 2^i ms).
    #[serde(default)]
    pub read_index_ms_total: u64,
    #[serde(default)]
    pub read_index_ms: Vec<u64>,
    #[serde(default)]
    pub renewals: u64,
    #[serde(default)]
    pub delegations_installed: u64,
    /// Grants not installed because a recall overtook their reply.
    #[serde(default)]
    pub delegations_raced: u64,
    /// Delegations held right now, and recalls received.
    #[serde(default)]
    pub delegations_held: u64,
    #[serde(default)]
    pub recalled: u64,
    // ---- sequencer side ----
    #[serde(default)]
    pub read_index_served: u64,
    #[serde(default)]
    pub read_index_refused: u64,
    #[serde(default)]
    pub grants: u64,
    #[serde(default)]
    pub live_grants: u64,
    /// Recalls sent, acked, and outwaited (TTL + margin: the delegate was
    /// unreachable).
    #[serde(default)]
    pub recalls_sent: u64,
    #[serde(default)]
    pub recalls_acked: u64,
    #[serde(default)]
    pub recalls_expired: u64,
    /// Acknowledgements that waited for recalls, and the total wait (ms).
    #[serde(default)]
    pub recall_waits: u64,
    #[serde(default)]
    pub recall_wait_ms_total: u64,
    /// Forwarded replies answered `Held` (and, as requester, retried).
    #[serde(default)]
    pub held_replies: u64,
    #[serde(default)]
    pub held_retries: u64,
    /// This node's own FUSE writes that waited for recalls, and how long.
    #[serde(default)]
    pub fuse_writes_recalled: u64,
    #[serde(default)]
    pub fuse_recall_wait_ms_total: u64,
    /// Parked acknowledgements and recalls in flight right now.
    #[serde(default)]
    pub parked_acks: u64,
    #[serde(default)]
    pub recalls_in_flight: u64,
}

/// Plan 30 §M14: `--locks cluster` — lock grants leased from the
/// owning sequencer.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct LockStatus {
    /// `cluster` or `local`.
    #[serde(default)]
    pub mode: String,
    // ---- this node as a lock holder ----
    /// Grants this node holds now (cached across unlocks).
    #[serde(default)]
    pub grants_held: u64,
    /// Local lock requests, answered under a held grant, refused by
    /// another local owner, granted by the sequencer, refused
    /// (`EAGAIN`), or unavailable (`ENOLCK`).
    #[serde(default)]
    pub requests: u64,
    #[serde(default)]
    pub local_hits: u64,
    #[serde(default)]
    pub local_conflicts: u64,
    #[serde(default)]
    pub granted: u64,
    #[serde(default)]
    pub would_block: u64,
    #[serde(default)]
    pub unavailable: u64,
    /// Grant round trips: total ms and log2 histogram (`[0]` < 1 ms).
    #[serde(default)]
    pub grant_ms_total: u64,
    #[serde(default)]
    pub grant_ms: Vec<u64>,
    #[serde(default)]
    pub renewals: u64,
    /// Grants the sequencer no longer knew (I/O under them is fenced).
    #[serde(default)]
    pub lost: u64,
    /// Recalls received (and how many found local locks), releases sent.
    #[serde(default)]
    pub recalled: u64,
    #[serde(default)]
    pub recalled_busy: u64,
    #[serde(default)]
    pub released: u64,
    /// I/O refused with `EIO` under a lapsed grant.
    #[serde(default)]
    pub fenced_io: u64,
    /// Grants whose floor this replica had not reached on arrival (the
    /// first read under the lock waited), how long those waits took in
    /// all, and the ones that timed out after the session budget: under
    /// such a grant the holder's reads were answered degraded, so the
    /// lock's visibility guarantee did not hold for that turn (logged at
    /// WARN too).
    #[serde(default)]
    pub grants_waited: u64,
    #[serde(default)]
    pub grant_wait_ms_total: u64,
    #[serde(default)]
    pub grants_degraded: u64,
    // ---- this node as a sequencer ----
    /// Live grants in this node's table.
    #[serde(default)]
    pub grants_table: u64,
    #[serde(default)]
    pub grants_made: u64,
    #[serde(default)]
    pub recalls_sent: u64,
    #[serde(default)]
    pub recalls_released: u64,
    #[serde(default)]
    pub recalls_expired: u64,
    #[serde(default)]
    pub reclaimed: u64,
    #[serde(default)]
    pub waiters_parked: u64,
    #[serde(default)]
    pub grace_refusals: u64,
    /// Waiters re-parked at their old queue position after a grant to
    /// them went unused, and releases that named a grant id this owner
    /// had replaced (both ended a grant that was otherwise outwaited).
    #[serde(default)]
    pub requeued_in_place: u64,
    #[serde(default)]
    pub released_superseded: u64,
    /// Right now: requests in flight, parked waiters, recalls in flight.
    #[serde(default)]
    pub requests_in_flight: u64,
    #[serde(default)]
    pub waiters: u64,
    #[serde(default)]
    pub recalls_in_flight: u64,
}

/// EC2 campaign 8 A-1: this node's own S3 path. While it is stalled (no
/// S3 request of this node completed for `CONSTELLATION_S3_STALL_MS`,
/// default 6 s) client ops keep forwarding over P2P rather than waiting
/// on a lease acquisition that needs S3, and one not answered within
/// `CONSTELLATION_S3_LESS_OP_DEADLINE_MS` (default 20 s) fails with `EIO`
/// (in doubt) — unless the peers report that none of them reaches S3
/// either (a bucket outage, served by the continuation epoch).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct OwnS3Status {
    pub stalled: bool,
    /// While stalled: some live peer reaches S3 (`true`), none does
    /// (`false`), or nobody answered (`null`).
    #[serde(default)]
    pub peers_reach_s3: Option<bool>,
    /// How long the stall has lasted, as far as the core knows.
    #[serde(default)]
    pub stalled_for_ms: Option<u64>,
    /// Forward retries past the ordinary budget, made instead of the
    /// lease path.
    #[serde(default)]
    pub retries: u64,
    /// Ops on the lease path forwarded again.
    #[serde(default)]
    pub forwards: u64,
    /// Ops that failed at the bound.
    #[serde(default)]
    pub deadlines: u64,
    /// Forwards this node answered `Held` while re-adopting the lease a
    /// previous incarnation of it left behind.
    #[serde(default)]
    pub readopted_for_forward: u64,
}

/// Plan 30 §M7: the direct log stream.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct LogStreamStatus {
    /// `CONSTELLATION_LOG_STREAMS` (and P2P) on.
    #[serde(default)]
    pub enabled: bool,
    /// The holder this node is subscribed to (0: none), whether a frame
    /// has arrived on the subscription, and segments waiting in its
    /// reorder buffer for a sequence S3 must supply.
    #[serde(default)]
    pub upstream: u64,
    #[serde(default)]
    pub live: bool,
    #[serde(default)]
    pub buffered: u64,
    /// Segments applied from the stream (no S3 GET), and rounds that
    /// skipped their S3 tail because the stream covered it.
    #[serde(default)]
    pub applied: u64,
    #[serde(default)]
    pub tail_skips: u64,
    /// Subscriptions made, and how they ended: refused (not the holder),
    /// ended by the holder (it let the lease go), a frame gap, lost at the
    /// transport, silent past the timeout, or a full reorder buffer.
    #[serde(default)]
    pub subscribes: u64,
    #[serde(default)]
    pub refused: u64,
    #[serde(default)]
    pub ended: u64,
    #[serde(default)]
    pub gaps: u64,
    #[serde(default)]
    pub lost: u64,
    #[serde(default)]
    pub timeouts: u64,
    #[serde(default)]
    pub overflows: u64,
    #[serde(default)]
    pub duplicates: u64,
    /// As the holder: subscribers served now, subscriptions accepted and
    /// declined, frames sent, and subscribers dropped for falling behind
    /// (their bounded buffer overflowed) or going away.
    #[serde(default)]
    pub serving: u64,
    #[serde(default)]
    pub served: u64,
    #[serde(default)]
    pub declined: u64,
    #[serde(default)]
    pub frames_sent: u64,
    #[serde(default)]
    pub subscribers_dropped: u64,
}

/// Plan 30 §M4: records held back behind unrecoverable pending chunks.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct HeldStatus {
    /// Journaled transactions held back (the seeds and everything that
    /// depends on them).
    #[serde(default)]
    pub transactions: u64,
    #[serde(default)]
    pub records: u64,
    /// The oldest held journal seq.
    #[serde(default)]
    pub oldest_seq: Option<u64>,
    /// An uncaptured transaction (holder capture off) was held, so every
    /// transaction after it is held too.
    #[serde(default)]
    pub opaque: bool,
    /// Plan 30 §M7: transactions deferred (not held) because a chunk their
    /// manifest names is still uploading; they ship once it is up.
    #[serde(default)]
    pub deferred: u64,
    /// Each inode with unrecoverable chunks: `constellation repair
    /// drop-held <ino>` discards its held records.
    #[serde(default)]
    pub inodes: Vec<HeldInodeStatus>,
    /// Pending chunks another node forwarded as still uploading there
    /// (`--write-mode back`, or any write inside a continuation epoch):
    /// the transactions naming them are deferred (counted in
    /// `deferred`) until that node uploads them, or the sequencer finds
    /// them in S3. If the node is gone for good, `constellation repair
    /// drop-held <ino> --remote` drops them.
    #[serde(default)]
    pub remote: Vec<RemoteChunkStatus>,
}

/// One chunk the sequencer waits for another node to upload.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct RemoteChunkStatus {
    pub ino: u64,
    #[serde(default)]
    pub path: Option<String>,
    /// The node expected to upload it.
    pub node: u64,
    /// Hex hash.
    pub chunk: String,
    /// Seconds since it was enrolled.
    pub age_s: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct HeldInodeStatus {
    pub ino: u64,
    #[serde(default)]
    pub path: Option<String>,
    /// Hex hashes of the pending chunks gone from the local cache.
    #[serde(default)]
    pub missing_chunks: Vec<String>,
    /// Held transactions whose manifest names them.
    #[serde(default)]
    pub seeds: u64,
}

/// Plan 30 §M9: the acknowledgement policy, backups and seals.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct AckStatus {
    /// This mount asks for `ack=s3`.
    #[serde(default)]
    pub ack_s3: bool,
    /// The policy of the lease this node holds: "local", "backup", "s3",
    /// or "-" when it holds none.
    #[serde(default)]
    pub policy: String,
    #[serde(default)]
    pub backups: Vec<u64>,
    #[serde(default)]
    pub candidate: Option<u64>,
    #[serde(default)]
    pub config_version: u64,
    /// The durable journal seq (min over the backups' acks, or the
    /// shipped-through seq under `s3`); `u64::MAX` when nothing gates.
    #[serde(default)]
    pub durable: u64,
    /// Acknowledgements parked for durability right now.
    #[serde(default)]
    pub parked_acks: u64,
    /// The fast path is closed (local writes go through the core).
    #[serde(default)]
    pub gated: bool,
    // ---- this node as a backup ----
    #[serde(default)]
    pub backing_holder: u64,
    #[serde(default)]
    pub backing_epoch: u64,
    #[serde(default)]
    pub backing_acked: u64,
    #[serde(default)]
    pub sealed_epoch: u64,
    // ---- counters ----
    #[serde(default)]
    pub backups_added: u64,
    #[serde(default)]
    pub backups_removed: u64,
    #[serde(default)]
    pub reconfig_cas: u64,
    #[serde(default)]
    pub backup_appends: u64,
    #[serde(default)]
    pub backup_acks: u64,
    #[serde(default)]
    pub backup_ack_timeouts: u64,
    #[serde(default)]
    pub acks_waited: u64,
    #[serde(default)]
    pub ack_wait_ms_total: u64,
    #[serde(default)]
    pub acks_aborted: u64,
    #[serde(default)]
    pub streamed_ahead: u64,
    #[serde(default)]
    pub streamed_installed: u64,
    #[serde(default)]
    pub streamed_dropped: u64,
    /// Accepted forwards of this node that could not install as a
    /// shadow (the holder ran unshipped work on their keys before them)
    /// and waited for their transaction, and of those, the ones the
    /// pre-S3 stream answered before the log did.
    #[serde(default)]
    pub awaited_log: u64,
    #[serde(default)]
    pub awaited_log_streamed: u64,
    /// Of `awaited_log_streamed`, a delegate's replies (answered from
    /// the root's stream of its append of the delegate's transaction).
    #[serde(default)]
    pub awaited_log_streamed_deleg: u64,
    #[serde(default)]
    pub backup_persisted: u64,
    #[serde(default)]
    pub seals: u64,
    #[serde(default)]
    pub backup_takeovers: u64,
    #[serde(default)]
    pub backup_tail_applied: u64,
    #[serde(default)]
    pub s3_fast_takeovers: u64,
    #[serde(default)]
    pub ack_floor_waits: u64,
    #[serde(default)]
    pub stale_liveness_refusals: u64,
    /// Plan 30 §M10's claim rule: continuation-epoch activations that
    /// did not carry this node's lease (an `ack=s3` lease, or a backup
    /// outside the epoch).
    #[serde(default)]
    pub epoch_carry_refused: u64,
    /// Plan 30 §M9: definitive refusals of forwarded ops journaled as
    /// outcomes, and refused replays of never-acknowledged ops.
    #[serde(default)]
    pub refusals_journaled: u64,
    #[serde(default)]
    pub unacked_replays_refused: u64,
    /// Holder-local reads that waited for durability of the unshipped
    /// rows they would have observed.
    #[serde(default)]
    pub reads_durability_blocked: u64,
}

/// Partition lease state (DESIGN.md §4). `held` is this node's own
/// authority; `holder`/`epoch` also describe the *foreign* holder once
/// this node has been deposed, which is what `lost` reports.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
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

// ---------------------------------------------------------------------------
// Parameters and results of the method table (plan 31 §9.2).
//
// Everything above this line is a copy of the retired API's report types;
// everything below is new: the request parameters that used to be `Request`
// variant fields, and the results that used to be `String` messages or
// `Response` variants.
// ---------------------------------------------------------------------------

/// A method's `Ok` result when the only news is a human-readable summary
/// (the old `Response::Ok { detail }`). Methods whose outcome has an
/// obvious structure return that instead.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Ack {
    pub detail: String,
}

impl Ack {
    pub fn new(detail: impl Into<String>) -> Ack {
        Ack {
            detail: detail.into(),
        }
    }
}

/// `node.ping`'s answer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Pong {}

/// Parameters of the many methods that name one path.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PathParams {
    pub path: String,
}

/// `designation.offline`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OfflineParams {
    pub path: String,
    #[serde(default)]
    pub read_only: bool,
}

/// `designation.delegate` (plan 30 §M11).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DelegateParams {
    pub path: String,
    pub node: u64,
    /// Plan 30 §M12: one name-hash range of the directory,
    /// `"<idx>/<count>"` (`count` a power of two); absent: the whole
    /// directory and its subtree.
    #[serde(default)]
    pub range: Option<String>,
}

/// `node.leave`. `node_id: None` retires this node; `Some(id)` is admin
/// removal of a different node via a still-mounted peer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LeaveParams {
    #[serde(default)]
    pub node_id: Option<u64>,
    #[serde(default)]
    pub force: bool,
}

/// `node.set_write_mode`. `mode` is the CLI's spelling (`"through"`,
/// `"back"`); C5b may replace it with an enum once the engine owns the
/// type.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SetWriteModeParams {
    pub mode: String,
}

/// `prune.run`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PruneRunParams {
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
}

/// `gc.run`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GcRunParams {
    #[serde(default)]
    pub verify_only: bool,
}

/// `fsck.run`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FsckRunParams {
    #[serde(default)]
    pub repair: bool,
    #[serde(default)]
    pub force_release: Option<String>,
}

/// The result of `gc.run`: `cli::gc::GcReport` as JSON. Opaque **for now**:
/// C5b may replace it with the engine's own type; JSON clients see the same
/// object either way.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct GcReport {
    pub report: JsonValue,
}

/// The result of `fsck.run`: `cli::fsck::FsckReport` as JSON (opaque for
/// now, like [`GcReport`]).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FsckReport {
    pub report: JsonValue,
}

/// `snapshot.create`. `hold` (plans 32/37): a retention hold name that keeps
/// the snapshot out of policy pruning until released.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotCreateParams {
    pub selector: String,
    #[serde(default)]
    pub hold: Option<String>,
}

/// `snapshot.list`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotListParams {
    #[serde(default)]
    pub path: Option<String>,
}

/// `snapshot.delete`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotDeleteParams {
    pub selector: String,
}

/// `clone.create`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CloneParams {
    pub selector: String,
    pub destination: String,
}

/// `snapshot.refs`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapRefsParams {
    pub id: String,
}

/// `locks.force_release`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ForceReleaseParams {
    pub part: String,
}

/// `locks.drop_held` (plan 30 §M4). `remote`: the chunks are ones another
/// node forwarded as pending and never uploaded; they are declared
/// unrecoverable first.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DropHeldParams {
    pub ino: u64,
    #[serde(default)]
    pub remote: bool,
}

/// `node.logs.tail`: the most recent `lines` log lines, then (with
/// `follow`) new ones as they are logged until cancelled. Delivered as
/// newline-terminated UTF-8 in `Chunk` frames.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LogTailParams {
    pub lines: usize,
    #[serde(default)]
    pub follow: bool,
}

/// `cache.prune`: drop clean LRU chunks until used bytes are at most
/// `target_bytes` (0: every clean chunk). Pinned and dirty chunks stay.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CachePruneParams {
    #[serde(default)]
    pub target_bytes: u64,
}

/// `cache.prune`'s result.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CachePruneResult {
    pub freed_bytes: u64,
    pub remaining_bytes: u64,
    pub detail: String,
}

/// `quota.set`. `None` clears the cap (unlimited).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SetQuotaParams {
    pub max_bytes: Option<u64>,
}

/// Per-view QoS (plan 31 §9.10). Over-limit ops defer and, past their
/// deadline, fail `EAGAIN`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ViewQos {
    #[serde(default)]
    pub max_inflight_ops: Option<u32>,
    #[serde(default)]
    pub max_staging_bytes: Option<u64>,
}

/// Where a new view's FUSE session comes from (plan 31 §6.11).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum MountSource {
    /// Mount at `mountpoint` with `mount(2)`, as today.
    Path {
        mountpoint: PathBuf,
        #[serde(default)]
        opts: MountViewOpts,
    },
    /// An already-mounted `/dev/fuse` descriptor (from the CSI node plugin
    /// or a previous process) that arrives **attached to this request**
    /// over fd passing. On a transport that cannot pass descriptors the
    /// call fails `NotSupported` immediately.
    PreopenedFd,
}

/// `view.mount`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ViewMountParams {
    /// Root, subtree or `@snapshot` selector to expose.
    pub subtree: String,
    pub source: MountSource,
    /// Free-form `{pv, pvc, namespace}`-style labels (plan 31 §9.10);
    /// filterable in `view.list`, never promoted wholesale to metrics.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub qos: ViewQos,
    /// `link()` across the view's subtree root fails `EXDEV` (§6.12).
    #[serde(default)]
    pub confine_links: bool,
}

/// `view.unmount`: identified by where it is mounted, not by subtree (the
/// same subtree may be mounted at two places).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ViewUnmountParams {
    pub mountpoint: PathBuf,
}

/// `view.list`: with `labels`, only views carrying every listed label.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ViewListParams {
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

/// One mounted view with its labels (`view.list` rows). The old
/// `MountInfo` plus the §9.10 additions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ViewInfo {
    pub id: u64,
    pub subtree: String,
    pub mountpoint: String,
    pub mounted_ms_ago: u64,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub qos: ViewQos,
    #[serde(default)]
    pub confine_links: bool,
}

/// `view.stats`: name the view by id or by mountpoint (exactly one).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ViewStatsParams {
    #[serde(default)]
    pub id: Option<u64>,
    #[serde(default)]
    pub mountpoint: Option<PathBuf>,
}

/// One view's numbers (CSI `NodeGetVolumeStats`): `statfs` plus the
/// recursive size/count of the view's subtree.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ViewStatsReport {
    pub id: u64,
    pub block_size: u32,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub inodes_total: u64,
    pub inodes_used: u64,
    /// Recursive logical bytes under the view's root.
    pub rsize: u64,
    /// Recursive entry count under the view's root.
    pub rcount: u64,
}

/// `browse.readdir`'s result.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DirectoryListing {
    pub path: String,
    pub entries: Vec<DirectoryEntry>,
}

/// `browse.stat`: `getattr` for one path.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct FileStat {
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
    #[schemars(with = "RdevSchema")]
    pub rdev: constellation_types::Rdev,
}

/// `browse.read`: `length: None` reads to end of file. The bytes arrive as
/// `Chunk` frames.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct BrowseReadParams {
    pub path: String,
    #[serde(default)]
    pub offset: u64,
    #[serde(default)]
    pub length: Option<u64>,
}

/// `browse.write`: write `data` at `offset`. One bounded call per slice
/// (uploads are not streamed in this protocol version), so `data` plus
/// envelope must fit the 8 MiB frame limit.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct BrowseWriteParams {
    pub path: String,
    #[serde(default)]
    pub offset: u64,
    pub data: ByteBuf,
    /// Create the file if absent (mode from `create_mode`, else 0644).
    #[serde(default)]
    pub create: bool,
    #[serde(default)]
    pub create_mode: Option<u32>,
    /// Truncate to `offset + data.len()` after writing.
    #[serde(default)]
    pub truncate: bool,
}

/// `browse.write`'s result.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WriteResult {
    pub written: u64,
    /// The file's size afterwards.
    pub size: u64,
}

/// `browse.mkdir`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MkdirParams {
    pub path: String,
    #[serde(default)]
    pub mode: Option<u32>,
    /// Create missing parents too (`mkdir -p`).
    #[serde(default)]
    pub parents: bool,
}

/// `browse.rename`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RenameParams {
    pub from: String,
    pub to: String,
    /// Replace an existing target (`rename(2)` semantics); otherwise an
    /// existing target fails `EEXIST`.
    #[serde(default)]
    pub overwrite: bool,
}

/// `browse.delete`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DeleteParams {
    pub path: String,
    /// Remove a directory and everything under it.
    #[serde(default)]
    pub recursive: bool,
}

/// What `browse.xattr` does. One method for all four because they share the
/// path and the plans that use it (34/35/36 scratch, prune and EA controls)
/// treat them as one surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum XattrOp {
    Get { name: String },
    List,
    Set { name: String, value: ByteBuf },
    Remove { name: String },
}

/// `browse.xattr`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct XattrParams {
    pub path: String,
    pub op: XattrOp,
}

/// `browse.xattr`'s result: `value` for `Get`, `names` for `List`, both
/// empty for `Set`/`Remove`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct XattrResult {
    #[serde(default)]
    pub value: Option<ByteBuf>,
    #[serde(default)]
    pub names: Vec<String>,
}

/// `peers.list`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct PeerListing {
    pub peers: Vec<PeerStatus>,
}

/// `pin.list`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct PinListing {
    pub pins: Vec<PinStatus>,
}

/// `designation.list`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct DesignationListing {
    pub designations: Vec<DesignationStatus>,
}

/// `designation.list_delegations`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct DelegationListing {
    pub delegations: Vec<DelegationStatus>,
}

/// `prune.list`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct PruneRootListing {
    pub roots: Vec<PruneRootStatus>,
}

/// `snapshot.list`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotListing {
    pub snapshots: Vec<SnapshotStatus>,
}

/// `snapshot.refs`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RefHashes {
    pub hashes: Vec<String>,
}

/// `cache.list`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct CacheEntryListing {
    pub entries: Vec<CacheEntryStatus>,
}

/// `view.list`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ViewListing {
    pub views: Vec<ViewInfo>,
}

/// `node.ops` (plan 31 §6.10): the operation watchdog's registry.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OpsParams {
    /// Only operations at least this old.
    #[serde(default)]
    pub min_age_s: Option<u64>,
    /// Only this view's operations.
    #[serde(default)]
    pub view: Option<u64>,
}

/// `node.ops`'s result.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct OpsReport {
    pub in_flight: u64,
    pub stalled: u64,
    pub oldest_s: u64,
    pub ops: Vec<StalledFuseRequest>,
}

/// The host lifecycle events of plan 31 §10, as a wire type
/// (`Suspending`'s deadline is relative: an `Instant` does not cross
/// processes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum LifecycleEventSpec {
    Foreground,
    Background,
    Suspending { deadline_in_ms: u64 },
    Resumed,
    NetworkChanged { reachable: bool, metered: bool },
    LowPower,
}

/// `node.lifecycle`: inject a host lifecycle event (harness / desktop).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LifecycleParams {
    pub event: LifecycleEventSpec,
}

/// `node.handoff` (plan 31 §6.11): drain the FUSE sessions of `views` (all
/// when empty), export their handle tables and send the session
/// descriptors to the process listening on the socket **attached to this
/// request** (over fd passing), which then `resume`s them. Requires a
/// transport with fd passing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HandoffParams {
    #[serde(default)]
    pub views: Vec<u64>,
    /// How long to wait for in-flight operations to drain.
    #[serde(default)]
    pub drain_timeout_ms: Option<u64>,
}

/// One session handed over.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HandedOffView {
    pub id: u64,
    pub mountpoint: String,
    /// Open handles exported with it.
    pub handles: u64,
}

/// `node.handoff`'s result.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HandoffReport {
    pub views: Vec<HandedOffView>,
}

/// One sample of `stats.subscribe`: named counters (monotonic) and gauges
/// (instantaneous), the same names `/metrics` exports.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StatsSample {
    pub unix_ms: u64,
    pub counters: BTreeMap<String, u64>,
    pub gauges: BTreeMap<String, f64>,
}

/// `stats.subscribe`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StatsSubscribeParams {
    /// Sampling period; the server clamps it to at least 100 ms.
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,
}

fn default_interval_ms() -> u64 {
    1000
}

impl Default for StatsSubscribeParams {
    fn default() -> Self {
        StatsSubscribeParams {
            interval_ms: default_interval_ms(),
        }
    }
}

/// `events.subscribe`: which topics to receive (empty: all). Topic names are
/// dotted, e.g. `view.mounted`, `peer.connected`, `lease.acquired`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EventsSubscribeParams {
    #[serde(default)]
    pub topics: Vec<String>,
}

/// One item of `events.subscribe`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ControlEvent {
    pub unix_ms: u64,
    pub topic: String,
    pub data: JsonValue,
}

// --- fs.* : the filesystem registry (`Manager`'s API) -----------------------

/// One registered filesystem.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FsInfo {
    pub uuid: String,
    #[serde(default)]
    pub name: Option<String>,
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub chunk_size: u32,
    #[serde(default)]
    pub compression: String,
    #[serde(default)]
    pub e2e: bool,
    #[serde(default)]
    pub write_mode: String,
    /// Whether this daemon has the credentials to open it (`fs.unlock`).
    #[serde(default)]
    pub unlocked: bool,
}

/// `fs.list`'s result.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FsListing {
    pub filesystems: Vec<FsInfo>,
}

/// `fs.create`, **idempotent** by `(bucket, prefix)`: matching parameters
/// return the existing uuid; different parameters for an existing
/// `(bucket, prefix)` fail with a `Code` (`EEXIST`/`EINVAL`), never silently
/// reuse or change the filesystem.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FsCreateParams {
    #[serde(default)]
    pub name: Option<String>,
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub chunk_size: Option<u32>,
    #[serde(default)]
    pub compression: Option<String>,
    #[serde(default)]
    pub e2e: bool,
    #[serde(default)]
    pub write_mode: Option<String>,
}

/// `fs.create`'s result.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FsCreated {
    pub uuid: String,
    /// False when the call matched an existing filesystem.
    pub created: bool,
}

/// `fs.import`: register a filesystem from a document `fs.export` made.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FsImportParams {
    pub document: String,
    /// Register under this name instead of the document's.
    #[serde(default)]
    pub name: Option<String>,
}

/// `fs.export`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FsExportParams {
    /// A registered name or a uuid.
    pub fs: String,
}

/// `fs.export`'s result: a self-contained registry document (TOML text).
/// Admin-only because it may name credentials sources.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FsExportDocument {
    pub document: String,
}

/// `fs.passwd`: change an end-to-end passphrase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FsPasswdParams {
    pub fs: String,
    pub old_passphrase: Secret,
    pub new_passphrase: Secret,
}

/// `fs.doctor`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FsDoctorParams {
    /// One filesystem, or all registered when absent.
    #[serde(default)]
    pub fs: Option<String>,
}

/// One registry consistency check.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FsDoctorCheck {
    pub fs: String,
    pub check: String,
    pub ok: bool,
    #[serde(default)]
    pub detail: String,
}

/// `fs.doctor`'s result.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FsDoctorReport {
    pub checks: Vec<FsDoctorCheck>,
}

/// Runtime credentials for `fs.unlock` (plan 31 §9.8). Every field is a
/// [`Secret`]; the daemon holds them in memory only.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct UnlockCredentials {
    #[serde(default)]
    pub access_key_id: Option<Secret>,
    #[serde(default)]
    pub secret_access_key: Option<Secret>,
    #[serde(default)]
    pub session_token: Option<Secret>,
    #[serde(default)]
    pub e2e_passphrase: Option<Secret>,
}

/// `fs.unlock`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FsUnlockParams {
    pub fs: String,
    pub credentials: UnlockCredentials,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{Blob, Encoding};

    fn round_trip<T>(value: &T)
    where
        T: Serialize + serde::de::DeserializeOwned + std::fmt::Debug,
    {
        for enc in crate::proto::SUPPORTED_ENCODINGS {
            let blob = Blob::encode(enc, value).unwrap();
            let back: T = blob.decode().unwrap();
            // Compare through JSON: some result types have no PartialEq.
            assert_eq!(
                serde_json::to_value(value).unwrap(),
                serde_json::to_value(&back).unwrap(),
                "{enc:?}"
            );
        }
    }

    #[test]
    fn rich_types_survive_both_encodings() {
        round_trip(&ViewMountParams {
            subtree: "/a".into(),
            source: MountSource::Path {
                mountpoint: "/mnt/x".into(),
                opts: MountViewOpts::default(),
            },
            labels: [("pv".to_string(), "pvc-1".to_string())].into(),
            qos: ViewQos {
                max_inflight_ops: Some(8),
                max_staging_bytes: None,
            },
            confine_links: true,
        });
        round_trip(&ViewMountParams {
            subtree: "/".into(),
            source: MountSource::PreopenedFd,
            labels: Default::default(),
            qos: Default::default(),
            confine_links: false,
        });
        round_trip(&BrowseWriteParams {
            path: "/f".into(),
            offset: 3,
            data: vec![0, 1, 2, 255].into(),
            create: true,
            create_mode: Some(0o600),
            truncate: false,
        });
        round_trip(&XattrParams {
            path: "/f".into(),
            op: XattrOp::Set {
                name: "user.x".into(),
                value: vec![9].into(),
            },
        });
        round_trip(&GcReport {
            report: JsonValue(serde_json::json!({"chunks": {"deleted": 3}, "ok": true})),
        });
        round_trip(&LifecycleParams {
            event: LifecycleEventSpec::NetworkChanged {
                reachable: true,
                metered: false,
            },
        });
        round_trip(&FsUnlockParams {
            fs: "f".into(),
            credentials: UnlockCredentials {
                access_key_id: Some(Secret::new("AKIA")),
                ..Default::default()
            },
        });
        round_trip(&ControlEvent {
            unix_ms: 1,
            topic: "view.mounted".into(),
            data: JsonValue(serde_json::json!({"id": 1})),
        });
    }

    #[test]
    fn status_report_survives_postcard() {
        // The largest type: build it from the minimal JSON a client with no
        // optional counters would send, then push it through postcard.
        let minimal = serde_json::json!({
            "fs_uuid": "u", "backend": "s3", "uptime_s": 5,
            "spool": {"journal_backlog": 0, "head_seq": 4},
            "cache": {"used_bytes": 0, "budget_bytes": 10, "chunks": 0},
            // A peer with a cooperative-cache source: the shape that once
            // carried `skip_serializing_if`, which postcard cannot read back.
            "p2p": {"peers": [{"node_id": 3, "coop": {"id": "s3"}}]}
        });
        let report: StatusReport = serde_json::from_value(minimal).unwrap();
        let blob = Blob::encode(Encoding::Postcard, &report).unwrap();
        let back: StatusReport = blob.decode().unwrap();
        assert_eq!(back.fs_uuid, "u");
        assert_eq!(back.spool.head_seq, 4);
        assert!(back.enrolled);
        let peer = &back.p2p.peers[0];
        assert_eq!(peer.node_id, 3);
        assert_eq!(peer.coop.as_ref().unwrap().id, "s3");
        let d = DelegationReport::default();
        let blob = Blob::encode(Encoding::Postcard, &d).unwrap();
        let _: DelegationReport = blob.decode().unwrap();
    }

    /// Postcard is positional: an attribute that changes which fields are
    /// written or how a variant is tagged corrupts the binary encoding.
    #[test]
    fn no_postcard_hostile_serde_attributes() {
        let source = include_str!("types.rs");
        let source = source.split("#[cfg(test)]").next().unwrap();
        for (n, line) in source.lines().enumerate() {
            let code = line.trim_start();
            if code.starts_with("//") {
                continue;
            }
            for bad in [
                "skip_serializing_if",
                "flatten",
                "serde(tag",
                "untagged",
                "serde(skip",
            ] {
                assert!(
                    !code.contains(bad),
                    "types.rs:{}: `{bad}` breaks postcard",
                    n + 1
                );
            }
        }
    }

    #[test]
    fn json_defaults_let_short_requests_through() {
        let p: ViewMountParams = serde_json::from_value(serde_json::json!({
            "subtree": "/", "source": "PreopenedFd"
        }))
        .unwrap();
        assert!(!p.confine_links && p.labels.is_empty());
        let s: StatsSubscribeParams = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(s.interval_ms, 1000);
    }
}
