//! Result types carried by the method table (plan 31 §9.2).
//!
//! Everything from `StatusReport` down to `DirectoryEntry` is copied from
//! the retired `crates/api/src/types.rs` (deleted in C5b, so this is the
//! surviving home, not a duplicate) with a `JsonSchema` derive added so the
//! schema document covers it. They keep today's field names.
//!
//! `#[serde(default)]` appears only on request types (`*Params` and what
//! they embed): a client may omit an optional parameter. Reply and event
//! types never carry it, because every daemon writes every field.
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
    /// Plan 38 §5: the FUSE transport this mount's connection negotiated
    /// — `dev_fuse`, `uring` or `uring_zc` — fixed for the connection's
    /// life. `None` on a host with no FUSE session behind the view (a
    /// control-only embedder, the engine's own fixtures) and in a report
    /// from a daemon older than plan 38 Z1b. The same value as this
    /// mount's [`FuseMountStatus::transport`] in [`StatusReport::fuse`],
    /// which carries the rest of plan 38 §5's per-mount transport state;
    /// kept here too because it describes the mount, and every mount
    /// listing (`view.mount`'s answer included) carries it.
    pub transport: Option<String>,
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
    pub manifest: Option<ManifestStatus>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ManifestStatus {
    pub chunk_size: u32,
    pub chunk_count: u64,
    pub spilled: bool,
    /// The manifest's own length (the inode's `size` can differ while a
    /// setattr and a manifest commit are apart).
    pub file_len: u64,
    /// BLAKE3 of the encoded manifest: two nodes serving the same file
    /// version agree on it.
    pub digest: String,
    /// `index:chunk hash` of the first chunks (or the spilled list's
    /// hash), for telling apart what two nodes serve.
    pub chunks: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct DoctorStatus {
    pub create_if_absent: bool,
    pub etag_cas: bool,
    /// Plan 30 §M4: what the provider answered at each CAS edge
    /// (`constellation_store_s3::probe`).
    pub cas_probes: Vec<CasProbeStatus>,
    /// Plan 30 §M4: bucket versioning as seen on a probe PUT
    /// (informational).
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

/// One snapshot, as `snapshot.list`/`snapshot.create`/`snapshot.hold`
/// report it. Everything after `created_unix_ms` is plan 32 §0.4's row
/// extension; a snapshot taken before it reports the defaults (`manual`,
/// not held, no owner, no size).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotStatus {
    pub id: String,
    pub path: String,
    pub name: String,
    pub root_hash: String,
    pub created_unix_ms: i64,
    /// `manual` or `auto` (a snapshot policy's own).
    pub origin: String,
    /// The directory inode carrying the owning policy; 0 for none.
    pub policy_ino: u64,
    /// A retention hold: `snapshot.delete` refuses it without `force`,
    /// and plan 32's expiry never considers it.
    pub held: bool,
    /// Who owns the hold: `user:<name>`, `csi:<VolumeSnapshotContent
    /// uid>`; absent for a plain hold with no recorded owner.
    pub held_by: Option<String>,
    /// The node that took it; 0 = unknown.
    pub creator: u64,
    /// REFER: the subtree's logical size when the snapshot was taken —
    /// what restoring it needs (plan 37's CSI `size_bytes`). Absent when
    /// it was not available at creation.
    pub refer_bytes: Option<u64>,
    /// The metadata commit the snapshot froze (its `root_hash`'s seq):
    /// with `created_unix_ms`, the order of a chain (plan 32 §0.2).
    /// Absent when the root does not parse.
    pub seq: Option<u64>,
    /// Plan 32 §6.1's sizes, logical bytes, from this node's accounting
    /// index, all "as of" the commit `as_of_seq` (`as_of_ms` when the
    /// index last caught up): `USED` (chunks nothing else references,
    /// live tree included), `WRITTEN` (new since the chain's previous
    /// snapshot), `REFER` (the distinct chunks it references — exact, so
    /// unlike `refer_bytes` it is deduplicated), `LSIZE` (apparent size).
    /// Present only with `size_state: ok`.
    pub used: Option<u64>,
    pub written: Option<u64>,
    pub refer: Option<u64>,
    pub lsize: Option<u64>,
    pub as_of_seq: Option<u64>,
    pub as_of_ms: Option<u64>,
    /// Whether the sizes above are there: `ok`, `building` (the index
    /// does not match the snapshots yet; never a partial number) or `off`
    /// (`CONSTELLATION_SNAPACCT=off`). Absent when the caller did not ask
    /// for sizes and the index had none ready.
    pub size_state: Option<SizeState>,
    /// With `size_state: building`: the share of snapshot rows applied.
    pub building_pct: Option<u8>,
    /// Plan 32 Step 5's `KEPT BY`, for an unheld auto snapshot of an
    /// *armed* policy root (parseable, not paused, directory present): the
    /// tiers that keep it (`5m`, `1h`, …), then `last`; `["grace"]` while
    /// only a grace window after a policy change keeps it; empty when the
    /// next expiry run deletes it. `null` for every other snapshot (manual,
    /// held — see `held`/`held_by` —, orphaned, paused). Always present.
    pub kept_by: Option<Vec<String>>,
    /// `EXPIRES`: when, if snapshots keep arriving on schedule, nothing
    /// keeps it any more (Unix ms; a forecast for display, never an input
    /// to expiry). `null` when `kept_by` is, when a `*` tier keeps it
    /// forever, or when it is due now (`kept_by` empty). Always present.
    pub expires_unix_ms: Option<i64>,
}

/// Whether a snapshot's sizes are available (plan 32 §6.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SizeState {
    Ok,
    Building,
    Off,
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
    pub designated: bool,
    /// Plan 30 §M12: `"<idx>/<count>"` for a hash range of the
    /// directory's names; empty for the whole directory.
    pub range: String,
}

/// Plan 30 §M11: this node's delegation state.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct DelegationReport {
    /// `CONSTELLATION_DELEGATION` is on (and P2P).
    pub enabled: bool,
    /// The live table.
    pub table: Vec<DelegationStatus>,
    /// Delegations this node holds: `(dir, gen, until_ms, stopped,
    /// streamed_through, executed, parked)`.
    pub mine: Vec<(u64, u64, i64, bool, u64, u64, usize)>,
    /// As the root: `(dir, node, gen, cursor, until_ms, recall, ended)`.
    pub gens: Vec<(u64, u64, u64, u64, i64, String, bool)>,
    pub executed: u64,
    /// Ops the FUSE fast path executed here as the delegate (not
    /// counted in `executed`, which is the core's).
    pub fast_path_executed: u64,
    /// Plan 30 §M12: ops the root's fast path sent through the core
    /// because a live delegation owned their keys.
    pub fast_path_routed: u64,
    pub forwarded_to_delegate: u64,
    pub deps_waits: u64,
    pub parked_expired: u64,
    /// Grants this delegate gave up unrenewed past the root's reclaim
    /// horizon (the root dead or cut off).
    pub lapsed: u64,
    pub not_owner: u64,
    pub installed: u64,
    pub streamed_txs: u64,
    pub stream_refused: u64,
    pub renewals: u64,
    pub renewals_refused: u64,
    pub recalls_received: u64,
    pub delegated: u64,
    pub appended_txs: u64,
    pub stream_refusals: u64,
    pub deps_unsatisfied_at_append: u64,
    pub cross_subtree: u64,
    pub recalls_sent: u64,
    pub recalls_drained: u64,
    pub recalls_expired: u64,
    pub reclaimed: u64,
    pub ended: u64,
    pub deps_overflow_to_root: u64,
    pub exec_parked: u64,
    /// Delegate rows stranded here by a recall (rolled back, replayed).
    pub stranded: u64,
    // ---- phase 2b ----
    /// As the root: `(gen, kind, backup)` per generation (kind: Manual,
    /// Placed, Designated).
    pub kinds: Vec<(u64, String, u64)>,
    /// As a delegate: `(gen, backup, backup_acked)`.
    pub backups: Vec<(u64, u64, u64)>,
    /// The placement's busiest subtrees, `(dir, node, node_ops,
    /// subtree_ops)` over the window (M12's input).
    pub placement: Vec<(u64, u64, u64, u64)>,
    pub inherited: u64,
    pub refused_designated: u64,
    pub designated: u64,
    pub redelegated: u64,
    pub seals_sent: u64,
    pub sealed_drained: u64,
    pub restreams: u64,
    pub backup_appends: u64,
    pub backup_acks: u64,
    pub acks_parked: u64,
    pub backup_persisted: u64,
    pub backup_seals: u64,
    pub place_evaluations: u64,
    pub place_delegated: u64,
    pub place_recalled: u64,
    pub place_skipped_cooldown: u64,
    pub place_skipped_unreachable: u64,
    /// Plan 30 §M12: hot directories split into hash ranges, and range
    /// generations recalled by the placement.
    pub place_splits: u64,
    pub place_range_recalls: u64,
    pub read_index_served: u64,
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

/// The FUSE request watchdog (`crate::fuse_watch`, EC2 campaign 7
/// B-2): requests in flight, and those unanswered past
/// `CONSTELLATION_FUSE_REQUEST_STALL_S`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct FuseRequestsStatus {
    /// Requests being handled right now.
    pub in_flight: u64,
    /// Of them, reported as stalled (older than the threshold).
    pub stalled: u64,
    /// Requests ever reported as stalled since the daemon started.
    pub stalled_total: u64,
    /// Of those, the ones that did complete eventually.
    pub stalled_completed: u64,
    /// The oldest request in flight, in seconds (blocking locks aside).
    pub oldest_s: u64,
    pub stall_threshold_s: u64,
    /// The stalled requests (and blocking lock waits past the
    /// threshold, marked `blocking`), oldest first.
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
    pub blocking: bool,
}

/// Plan 31 C4b: in-place upgrades of this daemon (`node.handoff` with
/// [`HandoffTarget::Exec`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HandoverStatus {
    /// How many handovers this daemon's process has been through (0: the
    /// image that started it).
    pub generation: u32,
    pub pid: u32,
    /// A handover is under way.
    pub upgrading: bool,
    /// Why the last attempt did not happen, if it did not.
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct StatusReport {
    /// Plan 31 C4b. First, as it was in the retired `crates/api` report:
    /// `constellation status` prints this struct, field order included.
    pub handover: HandoverStatus,
    pub fs_uuid: String,
    pub backend: String,
    /// Every view this daemon currently has mounted (plan 21, step 1).
    /// Was a single `mountpoint: String` field; a daemon serves exactly
    /// one view in every release before this one, so old single-view
    /// clients should read `mounts[0].mountpoint` if they need the
    /// scalar back, but the struct field itself is gone — multi-view is
    /// the whole point of this change and there is no sane single value
    /// to keep reporting once more than one view is live.
    pub mounts: Vec<MountInfo>,
    /// This node's cluster-unique id (scopes ino allocation, marks log
    /// segment origin).
    pub node_id: u64,
    /// Running binary version (`git describe` / package version).
    pub version: String,
    /// Whether this node's registry record is present and not retired.
    /// False after a successful `leave`, or when an admin retired us.
    pub enrolled: bool,
    pub uptime_s: u64,
    pub spool: SpoolStatus,
    pub cache: CacheStatus,
    /// Write authority for the one metadata stream (`p0`).
    pub lease: LeaseStatus,
    /// P2P fast path (M3.3). `enabled: false` on daemons without it, or
    /// when `CONSTELLATION_P2P=off`.
    pub p2p: P2pStatus,
    /// Locally pinned subtrees (phase 4a). Node-local, not replicated.
    pub pins: Vec<PinStatus>,
    /// Live offline designations visible to this node (phase 4a,
    /// DESIGN.md §5.2). Replicated via S3, so every node's view should
    /// agree modulo the periodic refresh lag.
    pub designations: Vec<DesignationStatus>,
    /// Plan 30 §M11.
    pub delegation: DelegationReport,
    /// Continuation epoch (phase 4b, DESIGN.md §5.3).
    pub epoch: EpochStatus,
    /// Stranded-journal reintegration (phase 4b).
    pub reintegration: ReintegrationStatus,
    /// Plan 30 §M3a speculation log and stranded-op recovery.
    pub speculation: SpeculationStatus,
    /// Plan 30 §M4: journal records held back behind unrecoverable
    /// pending chunks (everything else keeps shipping).
    pub held: HeldStatus,
    /// Plan 30 §M6: the session wait on local reads (read-your-writes,
    /// monotonic reads) and its latency distribution.
    pub session: SessionStatus,
    /// Plan 30 §M8: `cto=strict` and read delegations.
    pub cto: CtoStatus,
    /// Plan 30 §M14: `--locks` and cross-node lock grants.
    pub locks: LockStatus,
    /// Plan 30 §M9: acknowledgement policy, backups, seals.
    pub ack: AckStatus,
    /// Cooperative cache (phase 5, DESIGN.md §7).
    pub coop: CoopStatus,
    /// Adaptive sequential and directory readahead.
    pub prefetch: PrefetchStatus,
    /// Phase 5b write-back queue and adaptive upload policy.
    pub writeback: WritebackStatus,
    /// Plan 39: `fsync`s waiting out an unreachable S3, and how they ended.
    pub fsync: FsyncStatus,
    pub forwarded_ok: u64,
    pub forwarded_err: u64,
    pub forward_p50_ms: Option<u64>,
    /// Plan 30 §M7: the direct log stream (this node's subscription to
    /// the holder, or the subscribers it serves as the holder).
    pub log_stream: LogStreamStatus,
    /// Plan 30 §M2: forwarded requests the holder answered from
    /// `recent`/`completed` instead of re-executing (a retried rid).
    pub forward_dedup_hits: u64,
    /// Plan 30 §M2: same-rid forward retries this node's requester side
    /// made (same holder, or a redirected one) before falling back to
    /// the lease-acquisition path.
    pub forward_retries: u64,
    /// Plan 30 §M2: in-doubt ops the lease-path resolved against
    /// `completed` instead of re-executing (a genuine takeover finding
    /// the op already happened).
    pub forward_indoubt_resolved: u64,
    /// EC2 campaign 8 A-1: this node's own S3 path, and what its ops did
    /// without it.
    pub own_s3: OwnS3Status,
    /// Plan 30 §M13: the S3 inbox (forwarding without P2P).
    pub inbox: InboxStatus,
    pub placement_reason: Option<String>,
    /// Optional cluster-wide logical byte cap and current used bytes.
    pub quota: QuotaStatus,
    /// Read-time atime (plan 20). Default (`off`) leaves every counter
    /// zero and `mode` "off".
    pub atime: AtimeStatus,
    /// Retention pruning (plan 22). Default (no marked roots) leaves
    /// every counter zero.
    pub prune: PruneStatus,
    /// Automatic snapshot schedules (plan 32 Step 9). Every counter the
    /// plan names is present from M2 on; they stay zero until the
    /// scheduler (M3) and expiry (M4) run.
    pub snapsched: SnapSchedStatus,
    /// Plan 32 §6.3/Step 9: the space-accounting index. Default (`auto`,
    /// never asked) leaves everything zero and `maintaining` false.
    pub snapacct: SnapAcctStatus,
    /// The FUSE request watchdog (EC2 campaign 7 B-2).
    pub fuse_requests: FuseRequestsStatus,
    /// Plan 31 C8: the engine profile and the host's lifecycle.
    pub lifecycle: LifecycleStatus,
    /// Object-store requests this daemon has issued since it started
    /// (every filesystem it serves), by kind and by key area.
    pub s3: S3RequestStatus,
    /// The unified op metrics (plan 31 §6.10) `/metrics` exports as
    /// `constellation_vfs_ops_total` and `constellation_vfs_op_seconds`.
    pub vfs_ops: VfsOpsStatus,
    /// Plan 38 §5: the FUSE read-path transport, per mount and
    /// process-wide.
    pub fuse: FuseStatus,
}

/// Plan 38 §5's `fuse` section: what each FUSE mount's connection is
/// served over and what of the read path's fast lanes it uses, plus the
/// process-wide counters behind `constellation_fuse_*`.
///
/// `cache_verify` (`admit`/`always`, plan 38 §2.3), which decides whether
/// passthrough and zero-copy may be used at all, is node-wide and is
/// reported once, as [`CacheStatus::cache_verify`]; it is deliberately not
/// repeated here.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FuseStatus {
    /// One entry per mount with a FUSE session behind it, by mount id.
    pub mounts: Vec<FuseMountStatus>,
    /// Every transport fallback this process took, by (from, to, reason):
    /// `constellation_fuse_transport_fallbacks_total`. Process-wide, so
    /// it survives the unmount of the mount that took one.
    pub transport_fallbacks: Vec<FuseFallbackCount>,
    /// Reads served zero-copy by every session of this process
    /// (`constellation_fuse_zero_copy_reads_total`, plan 38 Z4b).
    pub zero_copy_reads_total: u64,
    /// Blocking lock requests (`F_SETLKW`, blocking `flock`) every ring
    /// session of this process served as non-blocking because their ring
    /// queue's lock-wait budget (`depth - 1` waiters) was spent — answered
    /// `ENOLCK` where the lock was contended
    /// (`constellation_fuse_lock_wait_downgrades_total`, plan 38 Z2c).
    pub lock_wait_downgrades_total: u64,
}

/// One mount of [`FuseStatus`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FuseMountStatus {
    /// The mount's id ([`MountInfo::id`]).
    pub id: u64,
    pub mountpoint: String,
    /// What the connection negotiated: `dev_fuse`, `uring` or `uring_zc`,
    /// fixed for the connection's life.
    pub transport: String,
    /// Ring entries per kernel queue; 0 on `dev_fuse` (no ring queues).
    pub uring_queue_depth: u32,
    pub passthrough: FusePassthroughStatus,
    /// Reads this mount served zero-copy: one `READ_FIXED` from a chunk
    /// file into the reader's pages (plan 38 Z4b; only on `uring_zc`).
    pub zero_copy_reads: u64,
    /// The transport fallback this mount's handshake took, if it took
    /// one (plan 38 §2.4: logged once, and visible here).
    pub last_fallback: Option<FuseFallback>,
    /// Blocking lock requests this mount's ring served as non-blocking
    /// (see [`FuseStatus::lock_wait_downgrades_total`]); always 0 on
    /// `dev_fuse`.
    pub lock_wait_downgrades: u64,
    /// Replies this mount's ring threads found queued with no wake-up and
    /// flushed only because their wait for the kernel is bounded (1 s): each
    /// one a lost wake-up that, before the bound, left its caller waiting in
    /// the kernel while the daemon had nothing in flight. Logged (at most
    /// once a minute per ring); always 0 on `dev_fuse`.
    pub ring_stranded_commits: u64,
    /// Ring entries this mount's userspace has held -- fetched from the
    /// kernel, not yet answered back -- for longer than the FUSE request
    /// stall threshold (`CONSTELLATION_FUSE_REQUEST_STALL_S`), blocking lock
    /// requests aside, as of the ring watchdog's last pass. Unlike
    /// `fuse_requests` this includes requests still queued for a worker,
    /// which the view has not been handed yet. Always 0 on `dev_fuse`.
    pub ring_entries_held_long: u64,
}

/// Passthrough opens on one mount (plan 38 §3(c)): the kernel reading a
/// single-chunk file's cached chunk directly.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FusePassthroughStatus {
    /// Eligible opens are offered a backing file.
    pub enabled: bool,
    /// Opens currently backed by a chunk file
    /// (`constellation_fuse_passthrough_opens`).
    pub opens: u64,
    /// Why `enabled` is false; `None` when it is true: `writable_mount`
    /// (the default for a writable mount: only read-only mounts use it
    /// unless `CONSTELLATION_FUSE_PASSTHROUGH=1`), `disabled`,
    /// `cache_verify_always`, `no_cap_sys_admin`, `kernel`, `backing_open`
    /// (the kernel refused the session's probe registration: a user
    /// namespace, a cache on overlayfs) or `platform` (plan 38 Z3b).
    pub unavailable_reason: Option<String>,
    /// Read-write opens refused (`ETXTBSY`) because the file was open in
    /// passthrough mode (plan 38 §3(c), Z3b).
    pub refused_opens: u64,
    /// Opens answered with a backing file since the session started (a
    /// lane can tell from it that passthrough actually served something).
    pub opens_total: u64,
}

/// One transport downgrade (plan 38 §2.4).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FuseFallback {
    /// What was asked for (`uring`).
    pub from: String,
    /// What the connection got (`dev_fuse`).
    pub to: String,
    /// The rung that refused, a fixed name: `no_io_uring_feature`,
    /// `kernel_not_offered`, `handover_capable`, `ring_setup_failed`.
    pub reason: String,
    /// What refused, as precisely as the daemon knows.
    pub detail: String,
    /// When the handshake recorded it, Unix milliseconds.
    pub at_unix_ms: u64,
}

/// One (from, to, reason) of [`FuseStatus::transport_fallbacks`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FuseFallbackCount {
    pub from: String,
    pub to: String,
    pub reason: String,
    pub count: u64,
}

/// Every frontend op counted so far, per (frontend, view, transport, op):
/// the numbers behind
/// `constellation_vfs_ops_total{frontend,view,op,outcome,transport}` and the
/// `constellation_vfs_op_seconds{frontend,view,op,transport}` histogram. Only series
/// with something counted are present; `view` is the allowlisted metric
/// label of a view (plan 31 §9.10), never its full label map.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct VfsOpsStatus {
    /// The histogram's bucket upper bounds, seconds, ascending; each
    /// series' last bucket is `+Inf`.
    pub bucket_bounds_s: Vec<f64>,
    pub series: Vec<VfsOpSeries>,
}

/// One (frontend, view, transport, op) of [`VfsOpsStatus`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct VfsOpSeries {
    pub frontend: String,
    /// The view's allowlisted metric label; absent for a view with none.
    pub view: Option<String>,
    /// Plan 38 §5: the kernel transport that carried the ops — `dev_fuse`,
    /// `uring` or `uring_zc` for the FUSE frontend, `n/a` for any other.
    pub transport: String,
    pub op: String,
    /// Ops per outcome: `ok`, or a `Code` name (`NotFound`, ...).
    pub outcomes: BTreeMap<String, u64>,
    /// Ops per latency bucket (not cumulative): one more than
    /// `bucket_bounds_s`, the last being `+Inf`.
    pub buckets: Vec<u64>,
    /// The latencies' sum, nanoseconds.
    pub sum_ns: u64,
}

/// Object-store requests by kind (one per call the daemon makes; a
/// request's own retries inside the S3 client are not counted again),
/// and by `<KIND> <area>`, the area being the key's first segment under
/// the filesystem prefix (`log`, `leases`, `nodes`, `chunks`, …).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct S3RequestStatus {
    pub get: u64,
    pub head: u64,
    pub put: u64,
    pub list: u64,
    pub delete: u64,
    pub copy: u64,
    pub by_area: std::collections::BTreeMap<String, u64>,
    /// Requests that failed other than as an error of the request itself
    /// (see `last_answered_unix_ms`): a transport error or timeout after
    /// the client's own retries, a 5xx, a refusal.
    pub unanswered: u64,
    /// When S3 last answered a request (unix ms; 0: never) — a success,
    /// or an error of the request itself (not found, a failed
    /// precondition), which proves the path works as well.
    pub last_answered_unix_ms: i64,
    /// When a request last went unanswered (unix ms; 0: never), and why.
    pub last_unanswered_unix_ms: i64,
    pub last_unanswered_error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct AtimeStatus {
    /// "off", "relatime", or "lazy".
    pub mode: String,
    pub queued: u64,
    pub coalesced: u64,
    pub applied: u64,
    pub dropped_cap: u64,
    pub forward_ok: u64,
    pub forward_err: u64,
    pub local_only: u64,
    pub skew_clamped: u64,
}

/// Retention-pruning counters (plan 22), surfaced in `constellation
/// status`, on the web UI, and in `/metrics`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct PruneStatus {
    pub runs: u64,
    pub roots: u64,
    pub armed_roots: u64,
    pub unparseable_roots: u64,
    pub inert_roots: u64,
    pub entries_examined: u64,
    pub selected: u64,
    pub deleted: u64,
    pub bytes_deleted: u64,
    pub bytes_freed: u64,
    pub skipped_reverify: u64,
    pub skipped_forward_err: u64,
    pub skipped_hardlink: u64,
    pub skipped_repartition: u64,
    pub leases_acquired: u64,
    pub refused_lag: u64,
    pub last_run_unix_ms: u64,
    /// Last setxattr policy rejection: `(expression, byte_offset, message)`.
    pub last_parse_error: Option<(String, usize, String)>,
}

/// How long after a refused scheduler tick the silent-failure warning stays up.
pub const SNAPSCHED_REFUSAL_WARN_MS: u64 = 10 * 60 * 1000;

impl SnapSchedStatus {
    /// A tick was refused within [`SNAPSCHED_REFUSAL_WARN_MS`] of `now_unix_ms`
    /// (a standing refusal gate refuses every tick, so it keeps this true).
    pub fn refusing_recently(&self) -> bool {
        self.last_refused_unix_ms > 0
            && self.now_unix_ms.saturating_sub(self.last_refused_unix_ms)
                < SNAPSCHED_REFUSAL_WARN_MS
    }
}

/// Snapshot-schedule counters (plan 32 Step 9), surfaced in
/// `constellation status`, on the web UI, and in `/metrics`.
///
/// `unparseable_roots`, `capped_roots` above zero, or a recent refusal
/// (`refusing_recently`), mean snapshots are silently not being taken; `create_failed` climbing while
/// `expired` stays at zero is the healthy outage signature (expiry
/// anchors on the newest snapshot, so an outage freezes it).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapSchedStatus {
    /// Scheduler ticks run on this node.
    pub ticks: u64,
    /// This node holds the scheduler's singleton lease.
    pub leader: bool,
    /// Policy roots at the last tick, and of them the paused, the
    /// unparseable (skipped: nothing created, nothing expired) and the
    /// capped ones (too many live auto snapshots to create more).
    pub roots: u64,
    pub paused_roots: u64,
    pub unparseable_roots: u64,
    pub capped_roots: u64,
    /// Auto snapshots whose root carries no parseable policy any more:
    /// kept, never expired automatically.
    pub orphaned_snapshots: u64,
    pub created: u64,
    pub skipped_empty: u64,
    pub create_failed: u64,
    pub expired: u64,
    /// Expiry victims whose row changed before the delete (now held,
    /// gone, or re-owned), and so were not deleted.
    pub skipped_reverify: u64,
    /// Expiry victims kept by the grace window after a policy change.
    pub skipped_grace: u64,
    pub budget_expired: u64,
    pub budget_stale: u64,
    pub refused_lag: u64,
    pub refused_state: u64,
    /// When the scheduler last refused a tick (0 = never), and this node's
    /// clock when the status was read. `refused_*` are cumulative; the
    /// silent-failure warning keys on this being recent instead.
    pub last_refused_unix_ms: u64,
    pub now_unix_ms: u64,
    pub last_create_unix_ms: u64,
    pub last_error: Option<String>,
    /// The last policy a setxattr refused: `(expression, byte_offset,
    /// message)`.
    pub last_parse_error: Option<(String, usize, String)>,
    /// The `_snapsched` lease epoch this node last took or renewed (0 =
    /// never). Two nodes reporting `leader` with the same epoch would be
    /// two leaders; a lower epoch is a predecessor that has not yet seen
    /// it was replaced.
    pub lease_epoch: u64,
    /// While `leader`: when the lease lapses by this node's last renewal
    /// (Unix ms; `leader` turns false at it, a stalled leader included);
    /// 0 otherwise.
    pub lease_until_unix_ms: u64,
}

/// Plan 32 Step 9's `SnapAcctStats`: the node-local space-accounting
/// index (advisory; never published, never consulted by GC).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SnapAcctStatus {
    /// `CONSTELLATION_SNAPACCT`: "auto", "on" or "off".
    pub mode: String,
    /// Whether the background task is keeping the index current now.
    pub maintaining: bool,
    /// The index does not match the snapshot rows (its first build, a
    /// pass cut short by its budget, or a stalled chain): queries answer
    /// "building", never partial numbers.
    pub building: bool,
    pub build_progress_pct: u64,
    /// Chunks held by at least one snapshot.
    pub indexed_chunks: u64,
    /// The index's on-disk tables.
    pub index_bytes: u64,
    /// The commit every number is "as of".
    pub as_of_seq: u64,
    /// The last live-tree refresh's duration.
    pub refresh_ms_last: u64,
    /// The last `--verify`'s mismatch count.
    pub verify_mismatches: u64,
    /// Chains the last pass could not apply (an operation failed twice);
    /// above zero, queries answer "building" until they can be.
    pub stalled_chains: u64,
    /// Live refreshes that found a newer commit this replica had not
    /// applied yet, and so kept the flags at the older one.
    pub refreshes_deferred: u64,
    /// Chunks whose live flags the next settled refresh reads again.
    pub live_rechecks: u64,
    /// A full recompute of the live flags is pending a settled moment:
    /// the space figures are an estimate until it runs.
    pub live_recheck_full: bool,
    pub passes: u64,
    pub errors: u64,
    pub last_error: Option<String>,
}

/// One marked prune root, for `constellation prune ls`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct PruneRootStatus {
    pub path: String,
    pub policy: String,
    pub armed: bool,
    pub valid: bool,
    /// A note when the policy is unparseable or inert.
    pub note: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct QuotaStatus {
    /// `None` = unlimited.
    pub max_bytes: Option<u64>,
    pub used_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct PrefetchStatus {
    pub inflight: u64,
    pub queued: u64,
    pub streams: u64,
    pub window_bytes: u64,
    pub stalls: u64,
    pub gate_target: u32,
    pub scan_ahead_files: u64,
    pub scan_ahead_bytes: u64,
    /// Times a stream's queued readahead was cancelled because the reader
    /// stopped consuming it (DESIGN.md §7 "abandoned reader").
    pub abandoned: u64,
    /// Chunks dropped from queues by those cancellations — GETs saved from
    /// readers that never came back for them.
    pub abandoned_chunks: u64,
}

/// Plan 39: the `fsync` policy (`hard` by default; `soft` with
/// `--fsync-timeout`) and the waits it caused.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct FsyncStatus {
    /// `hard` (wait until durable) or `soft` (`--fsync-timeout`).
    pub mode: String,
    /// The soft timeout in ms (0: none).
    pub timeout_ms: u64,
    /// The cap the kernel's FUSE request timeout imposes, in ms (0: none).
    pub kernel_cap_ms: u64,
    /// `fsync`s waiting now after a failed attempt.
    pub waiting: u64,
    /// How long the oldest of them has waited, in ms (0: none).
    pub longest_wait_ms: u64,
    /// The longest any `fsync` has waited since the node started, in ms.
    pub max_wait_ms: u64,
    /// `fsync`s that retried at least once.
    pub waited: u64,
    /// Attempts retried after a transient failure.
    pub retries: u64,
    /// `fsync`s answered `EIO` because the soft timeout (or the kernel cap)
    /// elapsed.
    pub timeouts: u64,
    /// `fsync`s answered `EIO` for a failure waiting does not fix.
    pub permanent_errors: u64,
    /// `fsync`s answered `EINTR`.
    pub interrupted: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct WritebackStatus {
    pub mode: String,
    pub dirty_bytes: u64,
    pub pending_uploads: u64,
    pub upload_concurrency: u32,
    pub remote_probe_enabled: bool,
    pub remote_probe_hit_rate: f64,
    pub existence_bloom_hits: u64,
    pub existence_chunk_ref_hits: u64,
    pub existence_misses: u64,
    pub existence_peer_hints: u64,
    /// Pending rows for chunks other nodes forwarded in a manifest while
    /// they were still uploading there (`--write-mode back` on a
    /// non-owner): what this node's ship waits for their reports on.
    pub remote_chunks_awaited: u64,
    /// The oldest of them, in seconds (0: none).
    pub remote_chunks_oldest_s: u64,
    /// EC2 finding 1: drains that handed their chunks to a peer because
    /// this node's own uploads made no progress (this node's S3 path
    /// was down), how many succeeded, and the chunks they covered.
    pub handoffs_sent: u64,
    pub handoffs_ok: u64,
    pub handoff_chunks: u64,
    /// Chunks this node fetched from a peer and uploaded for it.
    pub handoff_chunks_accepted: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct CoopStatus {
    pub peer_hits: u64,
    /// Soft negatives: every peer decline (busy, absent, recently
    /// removed); see `peer_false_positives` / `peer_stale_misses`.
    pub peer_misses: u64,
    /// Hard negatives: transport / hash failures against a peer.
    pub peer_errors: u64,
    pub s3_fetches: u64,
    /// Chunks fetched from another member of an open continuation epoch.
    pub epoch_member_fetches: u64,
    pub hedges_fired: u64,
    pub bytes_served_to_peers: u64,
    pub stale_digests_pruned: u64,
    pub digest_rebuilds: u64,
    pub digest_capacity_exceeded: bool,
    pub per_source: Vec<SourceStatus>,
    /// Plan 30 §M15: `exact` (mirrors + reconciliation) or `bloom`.
    pub digest_mode: String,
    /// Peer declined with `Absent` a chunk our digest said it held.
    pub peer_false_positives: u64,
    /// Peer had dropped the chunk within its recent-removal window.
    pub peer_stale_misses: u64,
    /// Fetches of a chunk no mirror listed yet from the node that wrote
    /// the manifest naming it (a file another node just wrote): served,
    /// and declined (the fetch then went on to S3).
    pub fresh_hint_hits: u64,
    pub fresh_hint_misses: u64,
    /// Digest-plane traffic (summaries, deltas, rounds, or blooms).
    pub digest_bytes_sent: u64,
    pub digest_bytes_received: u64,
    pub digest_messages: u64,
    /// Microseconds spent building, applying and answering digests.
    pub digest_cpu_us: u64,
    pub reconcile_sessions: u64,
    pub reconcile_rounds: u64,
    pub reconcile_failures: u64,
    /// Part of `digest_cpu_us` spent on reconciliation rounds.
    pub reconcile_cpu_us: u64,
    /// Keys in this node's published servable set.
    pub local_set_entries: u64,
    /// Entries held about peers' caches (mirror keys or bloom inserts).
    pub peer_set_entries: u64,
    pub peer_set_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SourceStatus {
    pub id: String,
    /// First-byte EWMA from successful transfers only.
    pub ttfb_ms_ewma: Option<f64>,
    /// Goodput EWMA from successful transfers only (per-stream body rate).
    pub goodput_mbps_ewma: Option<f64>,
    /// Aggregate path throughput across concurrent streams (wall-clock).
    /// Prefer this over `goodput_mbps_ewma` when displaying "S3 BW" to operators.
    pub aggregate_mbps_ewma: Option<f64>,
    /// Successful fetches (EWMA complementary to miss/err).
    pub hit_rate: f64,
    /// Soft negatives (declines). Previously folded into `err_rate`.
    pub miss_rate: f64,
    /// Hard negatives (transport/hash failures).
    pub err_rate: f64,
    /// Successful transfers contributing to lat/BW EWMAs.
    pub ok_samples: u64,
    pub transport_rtt_ms: Option<f64>,
    pub path: String,
}

/// Live continuation-epoch snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct EpochStatus {
    pub active: bool,
    pub epoch_id: Option<String>,
    pub members: Vec<u64>,
    /// Plan 30 §M10: `f` (the filesystem's `epoch_slack`).
    pub epoch_slack: u32,
    /// Plan 30 §M10: the node whose lease the current (or last) epoch
    /// carried.
    pub carrier: Option<u64>,
    /// Plan 30 §M10: this node's last issued heartbeat promise (unix ms;
    /// 0: none).
    pub promise_until_ms: i64,
    /// EC2 follow-up 3c: this node's S3 is failing while a live member's
    /// works — its own outage, not the bucket's: it proposes no epoch.
    pub own_s3_outage: bool,
    /// Proposals this node made (sent to its members).
    pub proposals: u64,
    /// Plan 30 §M10 counters (the core's): promises persisted and PUT;
    /// promise requests answered and refused; TTL-takeover promise checks
    /// run, takeovers refused for too few promises, flush re-claims
    /// exempt; activations that found this node's claim stale.
    pub promise_puts: u64,
    pub promise_requests_answered: u64,
    pub promise_requests_refused: u64,
    pub promise_checks: u64,
    pub takeovers_refused_promises: u64,
    pub promise_flush_exempt: u64,
    pub stale_claims: u64,
    /// Members keep following the hold owner's log stream during an
    /// epoch: journal transactions the hold owner streamed ahead, those
    /// this member installed, and this member's forwards the stream
    /// answered.
    pub streamed_ahead: u64,
    pub streamed_installed: u64,
    pub forwards_streamed: u64,
    /// Epoch hold transfers declined because the requester had not
    /// applied the holder's whole log.
    pub handoffs_behind: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ReintegrationStatus {
    pub stranded_records: u64,
    pub conflicts_materialized: u64,
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
    pub outstanding: u64,
    /// Stranded ops queued for replay by rid.
    pub pending_replay: u64,
    /// Speculative entries rolled back because a later epoch stranded
    /// them, since start.
    pub rolled_back: u64,
    /// Stranded ops replayed by rid and accepted, since start.
    pub stranded_replayed: u64,
    /// Stranded ops whose replay was refused and materialized as a
    /// `.constellation-conflict/` copy, since start.
    pub replay_conflicts: u64,
    /// Plan 30 §M3b: this node's own journaled transactions captured as
    /// speculation and not yet shipped (a holder's unshipped journal).
    /// Unlike `outstanding`, these do not stop a publish: the publisher
    /// substitutes their before-images.
    pub local: u64,
    /// Plan 30 §M3b: this node's own unshipped transactions rolled back
    /// because it was deposed (and queued for replay by rid), since start.
    pub local_rolled_back: u64,
    /// Plan 30 §M3b: deposition recoveries run (rollback plus replay, or
    /// the capture-off rebuild), since start.
    pub depositions: u64,
    /// Plan 30 §M3b: epoch-marker segments this node shipped right after a
    /// takeover, since start.
    pub epoch_markers: u64,
    /// Plan 30 §M3b: this node holds the lease but its takeover gate has
    /// not completed (a marker or a local replay failed); new mutations are
    /// refused until a sync round completes it.
    pub gate_pending: bool,
    /// Plan 30 §M4: refused replays whose `.constellation-conflict/` copy
    /// could not be made yet (retried with backoff; later ops are not held
    /// up), and those failing for at least 10 s — a stall worth looking
    /// at (the node then also asks for the lease to make them locally).
    pub copies_pending: u64,
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
    pub enabled: bool,
    pub submitted_batches: u64,
    pub submitted_ops: u64,
    /// Ops re-submitted by rid: under a newer epoch because a takeover
    /// stranded their batch, or in a new batch because one they shared
    /// was withdrawn.
    pub resubmitted_ops: u64,
    /// Requester: batches this node withdrew (overwrote with a tombstone)
    /// before forwarding their op over P2P or holding it back.
    pub withdrawn_ops: u64,
    /// Holder: withdrawn batches its polls read and stepped past.
    pub tombstones_read: u64,
    /// Forwards the inbox could not take (no live holder to leave them
    /// with, S3 refused the batch, or the in-doubt deadline passed); they
    /// took the lease path.
    pub unavailable: u64,
    /// Submitted ops still waiting for their outcome in the log.
    pub pending_ops: u64,
    /// This node's next batch number under its current epoch.
    pub next_n: u64,
    pub executed_ops: u64,
    pub refused_ops: u64,
    /// Batch positions answered without executing (rid already had an
    /// outcome, or the position was below the watermark).
    pub deduped_ops: u64,
    pub drained_batches: u64,
    pub drained_ops: u64,
    pub polls: u64,
    pub poll_hits: u64,
    pub gc_deleted: u64,
    pub tracked_requesters: u64,
    /// The write-eligible roster this node's authority core last read
    /// (the registry, refreshed periodically and at a holder's inbox
    /// tenure start): who a holder polls with P2P off. A bench waits for
    /// it to name every node before timing.
    pub roster: Vec<u64>,
    /// Round-2 instrumentation, requester side: mean time from queueing
    /// an op to its batch being durable, from durable to its outcome
    /// applied from the log, and their sum.
    pub avg_queue_wait_ms: f64,
    pub avg_outcome_wait_ms: f64,
    pub avg_round_trip_ms: f64,
    /// Holder side: mean time from a batch's submission stamp to its
    /// poll hit (requester and holder clocks), and per-hit execute time.
    pub avg_pickup_ms: f64,
    pub avg_execute_ms: f64,
    /// Requester side: ops per batch, mean and maximum.
    pub avg_batch_ops: f64,
    pub largest_batch_ops: u64,
    /// Plan 30 M13 round 3b (the hybrid): whether this node's inbox
    /// demand is currently sustained enough that it is asking for the
    /// lease; how many times it started asking; the lease requests it
    /// sent for that; ops it forwarded through the inbox vs. ops it
    /// executed locally as holder.
    pub escalated: bool,
    pub escalations: u64,
    pub lease_requests: u64,
    /// EC2 finding 2: rounds in which this node, holding the lease,
    /// kept it from wanters across a P2P partition from the nodes using
    /// it.
    pub leases_kept_for_p2p_side: u64,
    pub inbox_ops: u64,
    pub local_ops: u64,
}

/// P2P fast-path state. Purely observational: the filesystem is correct
/// with `enabled: false` and every peer disconnected, just slower.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct P2pStatus {
    pub enabled: bool,
    /// This node's dialable address, as published to the registry.
    pub node_addr: Option<String>,
    /// Active relay policy: `disabled`, `default`, or a custom URL label.
    pub relay: String,
    /// Every P2P dial has timed out for longer than twice the dial
    /// timeout (`constellation_p2p_dial_stalled`): this node's endpoint
    /// is likely stuck, not its peers.
    pub dial_stalled: bool,
    pub peers: Vec<PeerStatus>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct PeerStatus {
    pub node_id: u64,
    pub connected: bool,
    pub rtt_ms: Option<u64>,
    /// Milliseconds since this peer was last observed live.
    pub last_seen_ms: Option<u64>,
    pub hostname: Option<String>,
    /// Peer binary version from the registry (`git describe` / package).
    pub version: Option<String>,
    pub pubkey: Option<String>,
    pub endpoint_id: Option<String>,
    pub addrs: Vec<String>,
    pub created_unix: Option<i64>,
    pub p2p_updated_unix: Option<i64>,
    pub ro: bool,
    /// Whether this peer is a member of the active continuation epoch.
    pub epoch_member: bool,
    /// Offline designations this peer currently holds (paths).
    pub designations: Vec<String>,
    /// Cooperative-cache source stats for this peer, when available.
    pub coop: Option<SourceStatus>,
    /// Synthetic S3 backend row (`node_id` 0). Always listed first among
    /// [`P2pStatus::peers`] so the UI can compare lat/BW/hit% with peers.
    pub s3: bool,
    /// Connectivity path: `direct`, `relay`, `unknown`, or empty for S3.
    pub path: String,
    /// Plan 30 §M4: every open QUIC path to this peer right now.
    pub paths: PeerPathsStatus,
}

/// Plan 30 §M4: the open network paths of the pooled connection to one
/// peer (iroh 1.x on noq keeps several open at once: typically the relay
/// path and, once holepunching succeeds, a direct one).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct PeerPathsStatus {
    /// Kind of the path application data currently uses: `direct`,
    /// `relay`, or empty when no connection is pooled.
    pub selected: String,
    /// Open direct (IP) paths.
    pub direct: u32,
    /// Open relay paths.
    pub relay: u32,
    /// More than one path is open, so a failure of the selected one can
    /// fail over without a new handshake.
    pub multipath: bool,
    /// Round-trip estimate of each open path, `kind:ms`, selected first.
    pub rtts: Vec<String>,
}

/// Plan 30 §M6: the session wait FUSE reads go through
/// (`constellation_meta::session`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SessionStatus {
    /// `CONSTELLATION_SESSION_WAIT_MS` (0: disabled).
    pub budget_ms: u64,
    /// Reads checked, and how each ended: at once on the applied
    /// position, at once on covering speculation, after a wait, or
    /// degraded after the whole budget.
    pub reads: u64,
    pub fast: u64,
    pub covered: u64,
    pub waited: u64,
    pub timeouts: u64,
    /// Timeouts while M4 held-back rows existed (a held row stalls the
    /// shipped-through position).
    pub degraded_held: u64,
    /// Reads that waited for a queued replay of this node's own write.
    pub replay_blocked: u64,
    /// Waits by log2 milliseconds: `[0]` < 1 ms, `[i]` < 2^i ms.
    pub waits_ms: Vec<u64>,
    pub wait_ms_total: u64,
    /// Times the `observed` watermark rose (replies whose effects were
    /// not installed here).
    pub raised: u64,
    /// `CONSTELLATION_SESSION_WATERMARK_TTL_MS` (0: never dropped).
    pub watermark_ttl_ms: u64,
    /// Watermarks dropped after staying unreached for the TTL, and
    /// stream dependencies voided because the delegation table showed
    /// their generation ended before this incarnation (EC2 campaign 7
    /// B-2: either one left every read on the node degraded for good).
    pub abandoned: u64,
    pub voided_ended: u64,
}

/// Plan 30 §M8: `cto=strict` reads, read delegations and recalls.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct CtoStatus {
    /// This mount is `--cto strict`.
    pub strict: bool,
    /// This node, as sequencer, grants read delegations.
    pub grants_enabled: bool,
    // ---- reader side ----
    /// Strict opens, lookups and listings, and how each was answered: by
    /// this node as the sequencer, under a read delegation, after a
    /// ReadIndex round trip, by tailing S3 (no live sequencer, or no
    /// P2P), or degraded (no answer in the budget).
    pub strict_reads: u64,
    pub holder_local: u64,
    pub delegation_local: u64,
    pub read_index: u64,
    pub s3_tail: u64,
    pub degraded: u64,
    /// ReadIndex round trip plus the wait for its position, total ms and
    /// log2 histogram (`[0]` < 1 ms, `[i]` < 2^i ms).
    pub read_index_ms_total: u64,
    pub read_index_ms: Vec<u64>,
    pub renewals: u64,
    pub delegations_installed: u64,
    /// Grants not installed because a recall overtook their reply.
    pub delegations_raced: u64,
    /// Delegations held right now, and recalls received.
    pub delegations_held: u64,
    pub recalled: u64,
    // ---- sequencer side ----
    pub read_index_served: u64,
    pub read_index_refused: u64,
    pub grants: u64,
    pub live_grants: u64,
    /// Recalls sent, acked, and outwaited (TTL + margin: the delegate was
    /// unreachable).
    pub recalls_sent: u64,
    pub recalls_acked: u64,
    pub recalls_expired: u64,
    /// Acknowledgements that waited for recalls, and the total wait (ms).
    pub recall_waits: u64,
    pub recall_wait_ms_total: u64,
    /// Forwarded replies answered `Held` (and, as requester, retried).
    pub held_replies: u64,
    pub held_retries: u64,
    /// This node's own FUSE writes that waited for recalls, and how long.
    pub fuse_writes_recalled: u64,
    pub fuse_recall_wait_ms_total: u64,
    /// Parked acknowledgements and recalls in flight right now.
    pub parked_acks: u64,
    pub recalls_in_flight: u64,
}

/// Plan 30 §M14: `--locks cluster` — lock grants leased from the
/// owning sequencer.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct LockStatus {
    /// `cluster` or `local`.
    pub mode: String,
    // ---- this node as a lock holder ----
    /// Grants this node holds now (cached across unlocks).
    pub grants_held: u64,
    /// Local lock requests, answered under a held grant, refused by
    /// another local owner, granted by the sequencer, refused
    /// (`EAGAIN`), or unavailable (`ENOLCK`).
    pub requests: u64,
    pub local_hits: u64,
    pub local_conflicts: u64,
    pub granted: u64,
    pub would_block: u64,
    pub unavailable: u64,
    /// Grant round trips: total ms and log2 histogram (`[0]` < 1 ms).
    pub grant_ms_total: u64,
    pub grant_ms: Vec<u64>,
    pub renewals: u64,
    /// Grants the sequencer no longer knew (I/O under them is fenced).
    pub lost: u64,
    /// Recalls received (and how many found local locks), releases sent.
    pub recalled: u64,
    pub recalled_busy: u64,
    pub released: u64,
    /// I/O refused with `EIO` under a lapsed grant.
    pub fenced_io: u64,
    /// Lock owners fenced on this node (their grant lapsed under their
    /// local lock), and the ops of theirs refused with `EIO` for it, on
    /// any file.
    pub owners_fenced: u64,
    pub owner_fenced_ops: u64,
    /// Recalled grants given up before their first local lock (the
    /// requester gave up, or the first-use budget ran out).
    pub first_use_abandoned: u64,
    /// The lock owners fenced on this node now, with when each was fenced
    /// (this node's clock, unix ms): what a harness compares with when
    /// another node got the lock.
    pub fenced_owners: Vec<FencedOwnerStatus>,
    /// The bounded clock-skew margin the lease and lock machinery assume
    /// (ms): a grant's holder honours it `margin` less than its owner
    /// records it, and a fence that came `2 × margin` before another
    /// node's grant came first.
    pub margin_ms: u64,
    /// The grants this node holds now, `[minter, seq]` each (a harness
    /// matches them with the executors' log of tagged operations).
    pub held_grants: Vec<(u64, u64)>,
    /// Plan 30 §M14 phase 2 (the fencing token): mutations issued here
    /// tagged with their owner's grants, and recalled grants whose
    /// release waited for such a mutation to be answered.
    pub tagged_ops: u64,
    pub release_waits: u64,
    /// A new local lock that waited for an earlier turn's mutations
    /// tagged with the same grant (in flight, or in doubt inside their
    /// window) before it was taken.
    pub predecessor_waits: u64,
    /// Owner side: grant and renewal answers that waited for the restart
    /// horizon's durable write (run off the authority core), and those
    /// writes.
    pub horizon_held: u64,
    pub horizon_writes: u64,
    /// Grants whose floor this replica had not reached on arrival (the
    /// first read under the lock waited), how long those waits took in
    /// all, and the ones that timed out after the session budget: under
    /// such a grant the holder's reads were answered degraded, so the
    /// lock's visibility guarantee did not hold for that turn (logged at
    /// WARN too).
    pub grants_waited: u64,
    pub grant_wait_ms_total: u64,
    pub grants_degraded: u64,
    // ---- this node as a sequencer ----
    /// Live grants in this node's table.
    pub grants_table: u64,
    pub grants_made: u64,
    pub recalls_sent: u64,
    pub recalls_released: u64,
    pub recalls_expired: u64,
    pub reclaimed: u64,
    pub waiters_parked: u64,
    pub grace_refusals: u64,
    /// Plan 30 §M14 phase 2: tagged mutations this node refused as their
    /// executor because a grant they named was no longer live (`EIO` to
    /// their issuer; nothing journaled).
    pub token_rejections: u64,
    /// Waiters re-parked at their old queue position after a grant to
    /// them went unused, and releases that named a grant id this owner
    /// had replaced (both ended a grant that was otherwise outwaited).
    pub requeued_in_place: u64,
    pub released_superseded: u64,
    /// Peers this owner took for unreachable (a recall undeliverable, or
    /// pushed grants unused twice in a row; granted only over their own
    /// requests until one acknowledges a recall), and waiters of theirs
    /// passed over meanwhile (silent past a short wait for their next
    /// request).
    pub peers_unreachable: u64,
    pub unreachable_passed_over: u64,
    /// Waiters dropped because their node asked again under a new
    /// incarnation, and requests from an older incarnation than one
    /// already seen (answered, not served).
    pub incarnation_waiters_dropped: u64,
    pub stale_incarnation_requests: u64,
    /// Right now: requests in flight, parked waiters, recalls in flight.
    pub requests_in_flight: u64,
    pub waiters: u64,
    pub recalls_in_flight: u64,
}

/// One lock owner fenced on this node (`LockStatus::fenced_owners`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FencedOwnerStatus {
    /// The kernel's lock owner id.
    pub owner: u64,
    /// The process that took the lock (0: unknown).
    pub pid: u32,
    /// When it was fenced (unix ms, this node's clock).
    pub since_ms: i64,
    /// The grant whose end fenced it, `[minter, seq]`, when known: every
    /// op the owner issued under that lock carried it as its fencing
    /// token (an executor logs each one it lets through, target
    /// `constellation::token_exec`).
    pub grant: Option<(u64, u64)>,
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
    pub peers_reach_s3: Option<bool>,
    /// How long the stall has lasted, as far as the core knows.
    pub stalled_for_ms: Option<u64>,
    /// Forward retries past the ordinary budget, made instead of the
    /// lease path.
    pub retries: u64,
    /// Ops on the lease path forwarded again.
    pub forwards: u64,
    /// Ops that failed at the bound.
    pub deadlines: u64,
    /// Forwards this node answered `Held` while re-adopting the lease a
    /// previous incarnation of it left behind.
    pub readopted_for_forward: u64,
}

/// Plan 30 §M7: the direct log stream.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct LogStreamStatus {
    /// `CONSTELLATION_LOG_STREAMS` (and P2P) on.
    pub enabled: bool,
    /// The holder this node is subscribed to (0: none), whether a frame
    /// has arrived on the subscription, and segments waiting in its
    /// reorder buffer for a sequence S3 must supply.
    pub upstream: u64,
    pub live: bool,
    pub buffered: u64,
    /// Segments applied from the stream (no S3 GET), and rounds that
    /// skipped their S3 tail because the stream covered it.
    pub applied: u64,
    pub tail_skips: u64,
    /// Subscriptions made, and how they ended: refused (not the holder),
    /// ended by the holder (it let the lease go), a frame gap, lost at the
    /// transport, silent past the timeout, or a full reorder buffer.
    pub subscribes: u64,
    pub refused: u64,
    pub ended: u64,
    pub gaps: u64,
    pub lost: u64,
    pub timeouts: u64,
    pub overflows: u64,
    pub duplicates: u64,
    /// As the holder: subscribers served now, subscriptions accepted and
    /// declined, frames sent, and subscribers dropped for falling behind
    /// (their bounded buffer overflowed) or going away.
    pub serving: u64,
    pub served: u64,
    pub declined: u64,
    pub frames_sent: u64,
    pub subscribers_dropped: u64,
}

/// Plan 30 §M4: records held back behind unrecoverable pending chunks.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct HeldStatus {
    /// Journaled transactions held back (the seeds and everything that
    /// depends on them).
    pub transactions: u64,
    pub records: u64,
    /// The oldest held journal seq.
    pub oldest_seq: Option<u64>,
    /// An uncaptured transaction (holder capture off) was held, so every
    /// transaction after it is held too.
    pub opaque: bool,
    /// Plan 30 §M7: transactions deferred (not held) because a chunk their
    /// manifest names is still uploading; they ship once it is up.
    pub deferred: u64,
    /// Each inode with unrecoverable chunks: `constellation repair
    /// drop-held <ino>` discards its held records.
    pub inodes: Vec<HeldInodeStatus>,
    /// Pending chunks another node forwarded as still uploading there
    /// (`--write-mode back`, or any write inside a continuation epoch):
    /// the transactions naming them are deferred (counted in
    /// `deferred`) until that node uploads them, or the sequencer finds
    /// them in S3. If the node is gone for good, `constellation repair
    /// drop-held <ino> --remote` drops them.
    pub remote: Vec<RemoteChunkStatus>,
}

/// One chunk the sequencer waits for another node to upload.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct RemoteChunkStatus {
    pub ino: u64,
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
    pub path: Option<String>,
    /// Hex hashes of the pending chunks gone from the local cache.
    pub missing_chunks: Vec<String>,
    /// Held transactions whose manifest names them.
    pub seeds: u64,
}

/// Plan 30 §M9: the acknowledgement policy, backups and seals.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct AckStatus {
    /// This mount asks for `ack=s3`.
    pub ack_s3: bool,
    /// The policy of the lease this node holds: "local", "backup", "s3",
    /// or "-" when it holds none.
    pub policy: String,
    pub backups: Vec<u64>,
    pub candidate: Option<u64>,
    pub config_version: u64,
    /// The durable journal seq (min over the backups' acks, or the
    /// shipped-through seq under `s3`); `u64::MAX` when nothing gates.
    pub durable: u64,
    /// Acknowledgements parked for durability right now.
    pub parked_acks: u64,
    /// The fast path is closed (local writes go through the core).
    pub gated: bool,
    // ---- this node as a backup ----
    pub backing_holder: u64,
    pub backing_epoch: u64,
    pub backing_acked: u64,
    pub sealed_epoch: u64,
    // ---- counters ----
    pub backups_added: u64,
    pub backups_removed: u64,
    pub reconfig_cas: u64,
    pub backup_appends: u64,
    pub backup_acks: u64,
    pub backup_ack_timeouts: u64,
    pub acks_waited: u64,
    pub ack_wait_ms_total: u64,
    pub acks_aborted: u64,
    pub streamed_ahead: u64,
    pub streamed_installed: u64,
    pub streamed_dropped: u64,
    /// Accepted forwards of this node that could not install as a
    /// shadow (the holder ran unshipped work on their keys before them)
    /// and waited for their transaction, and of those, the ones the
    /// pre-S3 stream answered before the log did.
    pub awaited_log: u64,
    pub awaited_log_streamed: u64,
    /// Of `awaited_log_streamed`, a delegate's replies (answered from
    /// the root's stream of its append of the delegate's transaction).
    pub awaited_log_streamed_deleg: u64,
    /// Uploads of this node's pending chunks for its own forwarded `back`
    /// close, despite the upload hold, because the close waited for
    /// records only those chunks release: once when the sequencer answered
    /// that only this upload brings them back, and once per
    /// `CONSTELLATION_OWN_RECORD_WAIT_MS` the close then still waited, or
    /// waited past a stream the sequencer said would carry them.
    pub own_record_uploads: u64,
    /// Forward replies whose position this node observed less rows of its
    /// own whose effects it already carries (a `back` close's, deferred on
    /// the sequencer behind its held chunks): no wait and no upload for
    /// them.
    pub own_rows_excused: u64,
    /// As sequencer: forwarded replies answered `Held` at once because
    /// the acknowledgement waits for chunks only the requester can upload
    /// (it is asked to upload them).
    pub held_for_upload: u64,
    pub backup_persisted: u64,
    pub seals: u64,
    pub backup_takeovers: u64,
    pub backup_tail_applied: u64,
    pub s3_fast_takeovers: u64,
    /// Root takeovers started because an op this node waits on (accepted,
    /// waiting for the log) needed a root whose lease had run out.
    pub dead_root_acquires: u64,
    pub ack_floor_waits: u64,
    pub stale_liveness_refusals: u64,
    /// Plan 30 §M10's claim rule: continuation-epoch activations that
    /// did not carry this node's lease (an `ack=s3` lease, or a backup
    /// outside the epoch).
    pub epoch_carry_refused: u64,
    /// Plan 30 §M9: definitive refusals of forwarded ops journaled as
    /// outcomes, and refused replays of never-acknowledged ops.
    pub refusals_journaled: u64,
    pub unacked_replays_refused: u64,
    /// Holder-local reads that waited for durability of the unshipped
    /// rows they would have observed.
    pub reads_durability_blocked: u64,
}

/// Partition lease state (DESIGN.md §4). `held` is this node's own
/// authority; `holder`/`epoch` also describe the *foreign* holder once
/// this node has been deposed, which is what `lost` reports.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct LeaseStatus {
    pub held: bool,
    pub holder: u64,
    pub epoch: u64,
    pub expires_in_ms: i64,
    /// This node was deposed: it refuses to ship and its unshipped
    /// journal is stranded pending reintegration.
    pub lost: bool,
}

/// Spool observability (DESIGN.md §12): outstanding unflushed metadata.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SpoolStatus {
    /// Journal records not yet shipped to S3.
    pub journal_backlog: u64,
    /// Highest log segment sequence shipped or applied so far.
    pub head_seq: u64,
    /// Foreign records skipped because a pending local op won (phase 2
    /// leaseless conflict detection).
    pub conflicts: u64,
    /// Last sync error, if the most recent round failed (S3 outage).
    pub last_ship_error: Option<String>,
    /// `run_managed_sync_round` invocations that ran to completion (plan
    /// 30 M2b measurement counter — see `shipper::SpoolInfo`'s doc).
    pub ship_rounds_completed: u64,
    /// Rounds dropped mid-flight for a request that still cancels one
    /// (plan 30 M2b: `Mutate`/`Forward` no longer do — see
    /// `shipper::SpoolInfo`'s doc).
    pub ship_rounds_cancelled: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct CacheStatus {
    pub used_bytes: u64,
    pub budget_bytes: u64,
    pub chunks: u64,
    /// Bytes protected from eviction by local pins.
    pub pinned_bytes: u64,
    /// In-flight (unflushed) write bytes staged to local disk, bounded
    /// independently of the chunk cache (plan 07). Zero on daemons
    /// with no open dirty inode.
    pub staging_bytes: u64,
    pub staging_budget_bytes: u64,
    /// The chunk memory cache: verified chunk contents kept in RAM so a
    /// cached read neither re-reads nor re-hashes the disk copy
    /// (`CONSTELLATION_CHUNK_MEMCACHE_BYTES`). Budget `0`: off.
    pub memory_budget_bytes: u64,
    pub memory_used_bytes: u64,
    /// Chunks resident in the memory cache.
    pub memory_chunks: u64,
    /// Of `memory_used_bytes`, the protected (reused) segment.
    pub memory_protected_bytes: u64,
    /// Chunk reads served from memory.
    pub memory_hits: u64,
    /// Chunk reads that loaded and verified the disk copy.
    pub memory_misses: u64,
    /// Reads that waited for a concurrent load of the same chunk.
    pub memory_coalesced: u64,
    pub memory_evictions: u64,
    /// When a disk-cache read re-hashes the chunk file it read
    /// (`--cache-verify`, `CONSTELLATION_CACHE_VERIFY`; plan 38 §2.3):
    /// `"admit"` verifies a chunk once — in flight on the fetch, or on
    /// the first read of a file a restart found on disk — and trusts it
    /// afterwards; `"always"` re-hashes every disk read. Empty when it
    /// came from a daemon older than the knob — which behaved as
    /// `"always"`, but says so by the absence, not by the value.
    pub cache_verify: String,
    /// Holders of a chunk kept un-evictable for an open passthrough
    /// handle (plan 38 §3(c)'s pin-while-open), summed over chunks: equal
    /// to the mounts' `fuse.passthrough.opens` when every such handle sits
    /// on the chunk the engine offered it.
    pub open_pins: u64,
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

/// `snapshot.create`'s `hold` (plans 32/37), which has two spellings
/// because two plans wrote it down differently and both must work:
///
/// - `"hold": true` — take it held, with `held_by` naming the owner (or
///   nobody). The spelling the CLI's `--hold [--by ...]` sends.
/// - `"hold": "csi:<content-uid>"` — hold it, owned by that string. Plan
///   31 L1208 and plan 37 §5's `Controller.CreateSnapshot` row spell the
///   driver's call exactly this way, so it deserializes verbatim.
///
/// Hand-written codecs rather than a serde enum representation: the only
/// one that produces a bare `true`/`"owner"` is the tagless one, and
/// postcard — not self-describing — cannot read it back at all (see
/// `no_postcard_hostile_serde_attributes`). So the self-describing
/// encoding gets both spellings and a positional one carries the resolved
/// `(held, owner)` pair, which is lossless either way.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Hold {
    /// Whether a hold was asked for at all.
    pub held: bool,
    /// The owner, when the `"hold": "<owner>"` spelling named one.
    pub owner: Option<String>,
}

impl Hold {
    /// `"hold": true|false`.
    pub fn flag(held: bool) -> Hold {
        Hold { held, owner: None }
    }

    /// `"hold": "<owner>"` — plan 37's shorthand.
    pub fn owned_by(owner: impl Into<String>) -> Hold {
        Hold {
            held: true,
            owner: Some(owner.into()),
        }
    }
}

impl Serialize for Hold {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match (&self.owner, serializer.is_human_readable()) {
            (Some(owner), true) => serializer.serialize_str(owner),
            (None, true) => serializer.serialize_bool(self.held),
            (_, false) => {
                use serde::ser::SerializeTuple;
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&self.held)?;
                tuple.serialize_element(&self.owner)?;
                tuple.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Hold {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Hold, D::Error> {
        struct Either;
        impl serde::de::Visitor<'_> for Either {
            type Value = Hold;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a boolean, or the hold owner as a string")
            }

            fn visit_bool<E: serde::de::Error>(self, held: bool) -> Result<Hold, E> {
                Ok(Hold::flag(held))
            }

            fn visit_str<E: serde::de::Error>(self, owner: &str) -> Result<Hold, E> {
                // An empty string is "no owner named", not an owner whose
                // name is empty — same normalization as `held_by`.
                Ok(match owner.is_empty() {
                    true => Hold::flag(true),
                    false => Hold::owned_by(owner),
                })
            }
        }
        if deserializer.is_human_readable() {
            return deserializer.deserialize_any(Either);
        }
        let (held, owner) = <(bool, Option<String>)>::deserialize(deserializer)?;
        Ok(Hold { held, owner })
    }
}

impl JsonSchema for Hold {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Hold".into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "anyOf": [
                {"type": "boolean"},
                {"type": "string"}
            ],
            "description": "true/false, or the hold owner as a string (`csi:<uid>`), which implies a hold"
        })
    }
}

/// `snapshot.create`. `hold` (plans 32/37) takes the snapshot already
/// held, so it is never briefly exposed to pruning; `held_by` records who
/// owns that hold (`user:<name>`, `csi:<VolumeSnapshotContent uid>`;
/// `policy:` is reserved and refused). A `held_by` on its own implies
/// `hold`, so plan 37's driver can say only who it is.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotCreateParams {
    pub selector: String,
    #[serde(default)]
    pub hold: Hold,
    #[serde(default)]
    pub held_by: Option<String>,
}

impl SnapshotCreateParams {
    /// The two spellings resolved into `(held, owner)`. Naming an owner at
    /// all asks for the hold, so plan 37's driver can pass `held_by` alone
    /// or `hold: "csi:<uid>"` alone. Both spellings naming *different*
    /// owners is a client bug, not something to pick a winner for.
    pub fn hold_request(&self) -> Result<(bool, Option<&str>), String> {
        let explicit = self.held_by.as_deref().filter(|by| !by.is_empty());
        let shorthand = self.hold.owner.as_deref().filter(|by| !by.is_empty());
        let owner = match (explicit, shorthand) {
            (Some(a), Some(b)) if a != b => {
                return Err(format!(
                    "hold names the owner {b:?} but held_by names {a:?}; pass one of them"
                ))
            }
            (Some(by), _) | (None, Some(by)) => Some(by),
            (None, None) => None,
        };
        Ok((self.hold.held || owner.is_some(), owner))
    }
}

/// `snapshot.hold`: set (`held: true`) or release (`held: false`) a
/// snapshot's retention hold.
///
/// `id` is a snapshot id (`snapshot.list`'s `id`) or a `path@name`
/// selector. `by` is the owner namespace: releasing a hold recorded under
/// an owner requires naming that same owner, and taking over another
/// owner's hold is refused — `force` (admin only) overrides both.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotHoldParams {
    pub id: String,
    pub held: bool,
    #[serde(default)]
    pub by: Option<String>,
    #[serde(default)]
    pub force: bool,
}

/// `snapshot.list`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotListParams {
    #[serde(default)]
    pub path: Option<String>,
    /// Ask for every snapshot's sizes (plan 32 §6.3): under
    /// `CONSTELLATION_SNAPACCT=auto` this is the request that builds the
    /// accounting index, answering `size_state: building` meanwhile.
    /// Without it the sizes are filled only when the index is already
    /// current, and the listing causes no accounting work.
    #[serde(default)]
    pub sizes: bool,
}

/// `snapshot.delete`. A held snapshot is refused (the message names the
/// owner) unless `force`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotDeleteParams {
    pub selector: String,
    #[serde(default)]
    pub force: bool,
}

/// `snapshot.resolve`: plan 32 Step 5's selectors — `path@name`,
/// `path@a%b` (the chain of `path@a` from `a` to `b` inclusive),
/// `path@prefix*` (a single trailing glob), or a bare snapshot id.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotResolveParams {
    pub selectors: Vec<String>,
}

/// `snapshot.reclaim`: what deleting exactly the snapshots `selectors`
/// name (as `snapshot.resolve` resolves them) would give back.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotReclaimParams {
    pub selectors: Vec<String>,
    /// A test aid: also return the hashes of the chunks the estimate
    /// counts (`ReclaimEstimate::chunk_hashes`), so a test can compare the
    /// estimate with what GC deletes afterwards. Needs the operator role;
    /// refused when the estimate counts more than 100,000 chunks (one
    /// 64-character string each, inside one reply frame).
    #[serde(default)]
    pub list_chunks: bool,
}

/// `snapshot.space`: the breakdown for the whole filesystem, or for the
/// snapshots of directories at or under `path`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotSpaceParams {
    #[serde(default)]
    pub path: Option<String>,
}

/// `snapshot.delete_many`: resolve `selectors` (as `snapshot.resolve`
/// does) and delete them all in one batch at the lease holder. A held
/// snapshot is refused per item, naming its owner, unless `force`.
/// `dry_run` resolves and reports what would be refused, and deletes
/// nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotDeleteManyParams {
    pub selectors: Vec<String>,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub force: bool,
}

/// `clone.create`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CloneParams {
    pub selector: String,
    pub destination: String,
}

/// A policy root (plan 32 Step 3.1) as `snapshot.policy.list/show/pause`
/// report it: a directory carrying `user.constellation.snapshots`, or an
/// orphaned stream — auto snapshots whose `policy_ino` carries no
/// parseable policy (removed, or the directory is gone), which are never
/// expired automatically (Step 4.2).
///
/// `ino` is the root's identity (a rename changes `path`, nothing else).
/// `path` is where it is now, absent when the inode is no longer linked.
/// `expr` is the xattr exactly as stored (empty for a removed policy),
/// `canonical` its canonical form when it parses, `error` where it does
/// not. `auto_snapshots` counts every row with `origin=auto` and this
/// `policy_ino`, held ones included; `orphaned` is set when there are
/// some and no parseable policy owns them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapPolicyRoot {
    pub ino: u64,
    pub path: Option<String>,
    pub expr: String,
    pub canonical: Option<String>,
    pub paused: bool,
    pub error: Option<PolicyErrorInfo>,
    pub auto_snapshots: u32,
    pub orphaned: bool,
}

/// `snapshot.policy.list`: every policy root, then every orphaned
/// stream whose directory carries no policy at all, each ascending by
/// inode.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapPolicyListing {
    pub roots: Vec<SnapPolicyRoot>,
}

/// `snapshot.policy.show`: the root, and — when its policy parses — the
/// policy evaluated over the directory's real snapshots (the same shape
/// `snapshot.policy.check {against}` returns). A paused policy is still
/// evaluated: the verdicts are what it does once resumed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapPolicyShown {
    pub root: SnapPolicyRoot,
    pub verdicts: Option<SnapPolicyAgainst>,
}

/// `snapshot.policy.set`: bind `expr` to the directory `path`.
///
/// The new policy is evaluated over the directory's real snapshots
/// first. When it would expire any, the daemon writes only if
/// `confirm_expiring` equals that count — the previewed delta, so a
/// stale client cannot confirm a different one. `dry_run` never writes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapPolicySetParams {
    pub path: String,
    pub expr: String,
    #[serde(default)]
    pub confirm_expiring: Option<u32>,
    #[serde(default)]
    pub dry_run: bool,
}

/// `snapshot.policy.set`'s delta (plan 32 Step 5). Also the `details` of
/// the `conflict` refusal when an expiring change was not confirmed.
///
/// `canonical` is what is (or would be) stored. `previous` is the
/// directory's expression before, if it had one. `creates_every` is the
/// finest tier's interval (absent for a paused policy, which creates
/// nothing). `would_expire`/`would_expire_ids` are the snapshots the new
/// policy's verdict expires, oldest first. `grace_note` says when that
/// would happen. `written` is whether the xattr was written.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapPolicyDelta {
    pub path: String,
    pub ino: u64,
    pub canonical: String,
    pub previous: Option<String>,
    pub creates_every: Option<String>,
    pub would_expire: u32,
    pub would_expire_ids: Vec<String>,
    pub grace_note: String,
    pub warnings: Vec<String>,
    pub written: bool,
}

/// `snapshot.policy.remove`: unbind the directory's policy. Without
/// `expire` its auto snapshots become orphaned and are kept. With
/// `expire`, its unheld auto snapshots are deleted together with the
/// policy (plan 32 Step 4.2), and the call writes only when
/// `confirm_expiring` is exactly the count it would delete (the same
/// guard as `set`); `dry_run` previews that count and writes nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapPolicyRemoveParams {
    pub path: String,
    #[serde(default)]
    pub expire: bool,
    #[serde(default)]
    pub confirm_expiring: Option<u32>,
    #[serde(default)]
    pub dry_run: bool,
}

/// `snapshot.policy.remove`'s answer. `root` is the directory as it is
/// now. `would_expire`/`would_expire_ids` are the root's unheld auto
/// snapshots (what `expire` deletes; 0 without `expire`), oldest first.
/// `expired` are the ones deleted; `skipped` the ones that changed under
/// the call (held or gone meanwhile) and were kept; `error` is why the
/// deletion stopped early, if it did (the policy is removed regardless).
/// Also the `details` of the `conflict` refusal when `expire` was not
/// confirmed with the right count.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapPolicyRemoved {
    pub root: SnapPolicyRoot,
    pub would_expire: u32,
    pub would_expire_ids: Vec<String>,
    pub expired: Vec<String>,
    pub skipped: Vec<SnapPolicySkipped>,
    pub error: Option<String>,
    pub written: bool,
}

/// A snapshot `snapshot.policy.remove {expire}` kept after all, and why.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapPolicySkipped {
    pub id: String,
    pub reason: String,
}

/// `snapshot.policy.pause`: set (`paused: true`) or clear the policy's
/// `paused` flag.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapPolicyPauseParams {
    pub path: String,
    pub paused: bool,
}

/// `snapshot.sched.status`: this node's scheduler (plan 32 Step 3.2) —
/// its counters, its knobs, and every policy root as this node's replica
/// sees it now.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapSchedReport {
    pub node_id: u64,
    /// `CONSTELLATION_SNAPSCHED`: whether this node may lead at all.
    pub enabled: bool,
    pub tick_ms: u64,
    pub max_per_root: u64,
    pub stats: SnapSchedStatus,
    /// Ascending by inode.
    pub roots: Vec<SnapSchedRootState>,
}

/// One policy root in `snapshot.sched.status`.
///
/// `due`: no auto snapshot of the root (held or not) was created inside
/// the current finest bucket, and the root is armed (parseable, not
/// paused, under the cap, its directory still there). `next_due_unix_ms`
/// is when it is (or became) due: the start of the current bucket when it
/// is due now, the start of the next one otherwise; absent when it never
/// will be (paused, unparseable, gone). `error` is why the root is not
/// armed, or the last creation failure the scheduler saw for it on this
/// node.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapSchedRootState {
    pub ino: u64,
    /// `None` when the directory is gone.
    pub path: Option<String>,
    /// The xattr as stored.
    pub expr: String,
    /// `None` when the policy does not parse.
    pub canonical: Option<String>,
    pub paused: bool,
    pub due: bool,
    pub next_due_unix_ms: Option<i64>,
    /// The name the current bucket's snapshot has (or would have).
    pub bucket_name: Option<String>,
    /// Live auto snapshots of this root (held ones included).
    pub auto_snapshots: u64,
    pub last_created_unix_ms: Option<i64>,
    pub capped: bool,
    pub error: Option<String>,
    /// Σ `USED` of every snapshot of this directory (auto and manual),
    /// from the space-accounting index; `None` while the index is not
    /// current or accounting is off (a peek: asking never starts a build).
    /// Not what deleting them returns: `USED` does not sum.
    pub used_bytes: Option<u64>,
    /// The policy's `budget=` (plan 32 Step 8), logical bytes; `None`
    /// without one (or unparseable).
    pub budget_bytes: Option<u64>,
    /// What the budget measures — `reclaim` of the root's unheld auto
    /// snapshots the tier rule keeps, logical bytes — as this node's
    /// scheduler last measured it, less what that run's budget victims
    /// were to return: the figure it expects after the run (a victim a
    /// hold saved meanwhile is not added back). Never a scan of its own:
    /// `None` on a node that does not lead, before its first measuring
    /// run, after a run that could not measure (see `budget_note`), and
    /// without a budget. Physical bytes ≈ this × `snapshot.space`'s
    /// `physical_ratio` (stored bytes per logical byte).
    pub budget_used_bytes: Option<u64>,
    /// The budget step's last word on this root from this node's
    /// scheduler, when it could not do what the budget asks: over budget
    /// with only the `last` floor and held snapshots left (unmeetable,
    /// reported, not an error), the accounting index stale or building
    /// (`budget_stale`), a grace window holding it back, or the run's
    /// delete cap reached. `None` once a run finds the root within budget,
    /// and on a node that does not lead (cleared when leadership ends).
    pub budget_note: Option<String>,
}

/// `snapshot.sched.run`: run one scheduler tick now, on this node.
/// Without `dry_run` it takes the `_snapsched` lease if it is free (a
/// tick on a node another one leads is refused, with the reason), creates
/// what is due and runs expiry for the roots due one (plan 32 Step 4);
/// `dry_run` takes nothing, creates and deletes nothing, and reports what
/// a tick would create and expire from this node's replica.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapSchedRunParams {
    #[serde(default)]
    pub dry_run: bool,
}

/// What one `snapshot.sched.run` tick did. `refused`: why it did nothing
/// (more) at all (a refusal gate, another leader, the scheduler disabled
/// here, fenced mid-run). `error`: the creation batch failed as a whole
/// (the holder unreachable, S3 down; nothing was created), or the expiry
/// run stopped (the grace state unreadable or its CAS lost: nothing
/// deleted; a delete batch failed); the next tick retries.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapSchedRunResult {
    pub dry_run: bool,
    pub leader: bool,
    pub refused: Option<String>,
    pub error: Option<String>,
    /// The roots the tick asked a snapshot of, ascending by inode.
    pub roots: Vec<SnapSchedRunRoot>,
}

/// One due root, or one snapshot the expiry run handled, in a
/// `snapshot.sched.run` result. For a creation `outcome` is
/// `would_create` (dry run), `created`, `skipped_empty` (nothing under
/// the root changed since its newest snapshot), `already_exists` (the
/// bucket's name was taken already — by an earlier leader, typically;
/// success), or `failed` (with `error`). For expiry (`name` and `id` the
/// snapshot's) it is `would_expire` (dry run), `expired`, or
/// `skipped_reverify` (with why in `error`: held or gone meanwhile);
/// for the space budget (plan 32 Step 8) `would_budget_expire` (dry run)
/// or `budget_expired`, and in a dry run `budget_note` (no `name`/`id`;
/// `error` says why the budget decides nothing or is not met).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapSchedRunRoot {
    pub ino: u64,
    pub path: String,
    pub name: String,
    pub outcome: String,
    pub id: Option<String>,
    pub error: Option<String>,
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
///
/// Without `subtree` the cap is the filesystem-wide logical byte cap. With
/// one (an absolute path naming a directory, plan 37's
/// `quota.set{subtree, bytes}`) it caps that directory's subtree instead:
/// the bytes under it, enforced by every view mounted at it. `"/"` is the
/// filesystem-wide cap, spelled out.
///
/// A subtree `quota.set` returns the new cap with `used_bytes: 0`: the
/// bytes under a subtree cost a walk of all of it, which a cap change
/// (`DeleteVolume`'s release, an expansion) must not pay; `quota.get`
/// reports them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SetQuotaParams {
    pub max_bytes: Option<u64>,
    #[serde(default)]
    pub subtree: Option<String>,
}

/// `quota.get`: the filesystem-wide cap and usage, or (with `subtree`) one
/// directory subtree's cap and the logical bytes under it — `NotFound`
/// when no such directory exists.
///
/// A subtree's `used_bytes` is a walk of every entry under it (O(entries));
/// `cap_only` skips the walk and reports `used_bytes: 0`, for callers that
/// only need the cap or the directory's existence. The filesystem-wide
/// usage is a maintained counter and always reported.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct QuotaGetParams {
    #[serde(default)]
    pub subtree: Option<String>,
    #[serde(default)]
    pub cap_only: bool,
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
    PreopenedFd {
        /// Where the sender mounted it, as the sender sees it (plan 37's
        /// staging path): the name `view.list`/`view.stats`/`view.unmount`
        /// know the view by. It may not exist in this process's mount
        /// namespace, and this daemon never unmounts it — `view.unmount`
        /// ends the session and closes the connection, and whoever made
        /// the mount unmounts it. `None`: the view is known as `fd:<n>`.
        #[serde(default)]
        mountpoint: Option<PathBuf>,
        /// How the session serves it: `allow_other` (fuser admits every
        /// uid, which a mount other processes use needs), `fs_name`,
        /// `fuse_threads`, and the view options. The kernel's half of the
        /// mount options was fixed by whoever called `mount(2)`.
        #[serde(default)]
        opts: MountViewOpts,
    },
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
    pub labels: BTreeMap<String, String>,
    pub qos: ViewQos,
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
    pub value: Option<ByteBuf>,
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

/// `snapshot.create`'s result: the new snapshot's record, and the summary
/// `constellation snapshot create` prints.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotCreated {
    pub detail: String,
    pub snapshot: SnapshotStatus,
}

/// `snapshot.list`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotListing {
    pub snapshots: Vec<SnapshotStatus>,
}

/// One snapshot `snapshot.delete_many` did not delete, and why.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotRefusal {
    pub id: String,
    pub reason: String,
}

/// What deleting a set of snapshots would give back once GC has run (plan
/// 32 §6.1 `reclaim(D)`): the logical bytes and chunks no other snapshot
/// and not the live tree reference, as of the accounting index's commit
/// `as_of_seq`. While the index does not match the snapshots yet,
/// `building` is set (with `building_pct`) and `bytes`/`chunks` are 0 and
/// mean nothing: never a partial number.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReclaimEstimate {
    pub bytes: u64,
    pub chunks: u64,
    pub as_of_seq: u64,
    pub as_of_ms: u64,
    pub building: bool,
    pub building_pct: u8,
    /// With `snapshot.reclaim`'s `list_chunks` (a test aid), the counted
    /// chunks' hashes (lowercase hex, sorted); `null` otherwise and while
    /// building.
    pub chunk_hashes: Option<Vec<String>>,
}

/// Logical bytes and the distinct chunks they are in.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SpaceAmount {
    pub bytes: u64,
    pub chunks: u64,
}

/// `snapshot.space` (plan 32 Step 5, ZFS's `usedby*`), logical bytes.
/// While the index does not match the snapshots yet, `building` is set
/// and every number is 0 and means nothing.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SpaceBreakdown {
    /// The path asked about; absent for the whole filesystem.
    pub path: Option<String>,
    pub building: bool,
    pub building_pct: u8,
    /// Apparent size of the live tree (or of the subtree).
    pub live_logical: u64,
    /// `usedbysnapshots`: what deleting every snapshot in scope returns.
    pub snapshots_total: SpaceAmount,
    /// Σ `USED`: chunks exactly one snapshot holds. Not the total: chunks
    /// two or more snapshots share are in nobody's `USED`.
    pub unique: SpaceAmount,
    /// Not live, held by two or more snapshots.
    pub shared_snapshots_only: SpaceAmount,
    /// In a snapshot and in the live tree: costs nothing extra.
    pub shared_with_live: SpaceAmount,
    /// Freed by snapshot deletion and presumed not yet collected
    /// (filesystem-wide even with a path).
    pub awaiting_gc: SpaceAmount,
    /// How long GC keeps freed chunks (`CONSTELLATION_GC_HORIZON_S`):
    /// what "awaiting GC" waits for.
    pub gc_horizon_ms: u64,
    /// **Estimate**: stored bytes per logical byte (e.g. 0.61), from the
    /// last GC round's census of `chunks/`; absent before any round wrote
    /// one. Show physical figures derived from it with `≈`.
    pub physical_ratio: Option<f64>,
    /// **Estimate**: `snapshots_total` in stored bytes (logical ×
    /// `physical_ratio`).
    pub physical_estimate: Option<u64>,
    pub as_of_seq: u64,
    pub as_of_ms: u64,
    /// A recheck of the live flags is pending a settled moment (the
    /// replica never was at one commit's state): the "shared with live"
    /// and "reclaimable" figures are an estimate until it runs.
    pub estimate_pending: bool,
}

/// `snapshot.space.verify`: the accounting index against a brute-force
/// walk of every snapshot (plan 32 §6.3).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SpaceVerified {
    /// Numbers that differ; 0 means the index is exact.
    pub mismatches: u64,
    /// One line per mismatch (the first 200).
    pub details: Vec<String>,
    pub snapshots: u64,
    /// Distinct chunks the snapshots reference.
    pub chunks: u64,
    pub as_of_seq: u64,
}

/// `snapshot.delete_many`'s result. `resolved` is every snapshot the
/// selectors named, in chain order; `deleted` the ids actually deleted
/// (always empty for a dry run); `refused` the ones that were not, with
/// the reason. `reclaim` is filled for a dry run (over the snapshots it
/// would delete, not the refused ones) and `None` otherwise, or when
/// accounting is off.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotsDeleted {
    pub resolved: Vec<SnapshotStatus>,
    pub deleted: Vec<String>,
    pub refused: Vec<SnapshotRefusal>,
    pub reclaim: Option<ReclaimEstimate>,
}

/// `snapshot.hold`'s result: the summary line, and the snapshot as it now
/// stands.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotHeld {
    pub detail: String,
    pub snapshot: SnapshotStatus,
}

/// `snapshot.policy.check` (plan 32 Step 5): parse a snapshot-schedule
/// expression and say what it means, without writing it anywhere.
///
/// `against` is a directory path: the policy is evaluated over that
/// directory's real snapshot rows, as if it were the directory's policy
/// (`policy_ino` = the path's current inode). `simulate_ms` is the
/// simulation horizon; absent, the daemon simulates until the policy's
/// count settles (`constellation_meta::snapsched::check`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapPolicyCheckParams {
    pub expr: String,
    #[serde(default)]
    pub against: Option<String>,
    #[serde(default)]
    pub simulate_ms: Option<u64>,
}

/// Where a policy expression fails to parse: the byte offset of the
/// token at fault (for a caret under it) and why.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PolicyErrorInfo {
    pub offset: u64,
    pub message: String,
}

/// Why retention keeps a snapshot, in the uniform four-key shape
/// `constellation_meta::snapsched::Reason` serializes to: `kind` is
/// `tier` (with `every`, e.g. `"1h"`), `last` (with `last`, the `n` of
/// `last=n`), `held` (with `held_by`, the owner if recorded), `grace`, or
/// `not_candidate` (manual, or another root's).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapReason {
    pub kind: String,
    pub every: Option<String>,
    pub last: Option<u32>,
    pub held_by: Option<String>,
}

/// One real snapshot's fate under a checked policy: its identity, the
/// facts retention read (`origin`, `policy_ino`, `held`, `held_by`,
/// `created_unix_ms`), and the verdict. `keep: false` means the policy,
/// were it set on that directory, would expire it; `reasons` is empty
/// exactly then. `expires_unix_ms` is the display forecast (absent when
/// kept forever or already expired).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapVerdict {
    pub id: String,
    pub path: String,
    pub name: String,
    pub created_unix_ms: i64,
    pub origin: String,
    pub policy_ino: u64,
    pub held: bool,
    pub held_by: Option<String>,
    pub keep: bool,
    pub reasons: Vec<SnapReason>,
    pub expires_unix_ms: Option<i64>,
}

/// `snapshot.policy.check`'s `against`: the directory's snapshots, how
/// many the policy would expire, and every verdict, oldest first.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapPolicyAgainst {
    pub path: String,
    pub policy_ino: u64,
    pub snapshots: u32,
    pub would_expire: u32,
    pub verdicts: Vec<SnapVerdict>,
}

/// `snapshot.policy.check`'s result. A parse error is a *result*
/// (`ok: false`, `error` set, everything else empty), not a failed call:
/// checking an expression that turns out invalid is the method working.
///
/// `simulated_count` is the policy's own snapshots alive after
/// `simulate_horizon_ms` of simulated schedule (over the `against`
/// directory's history when given, an empty one otherwise);
/// `simulate_truncated` means the simulation hit its work limits before
/// the horizon (a policy that keeps most of what it creates, or a very
/// fine cadence), and the count is where it stopped:
/// `simulate_reached_ms` after the start, always set alongside
/// `simulated_count`. `steady_state_bound` is the plan's upper bound, absent for a
/// policy with a `*` tier.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapPolicyCheckResult {
    pub ok: bool,
    pub canonical: Option<String>,
    pub error: Option<PolicyErrorInfo>,
    pub warnings: Vec<String>,
    pub steady_state_bound: Option<u64>,
    pub simulated_count: Option<u64>,
    pub simulate_horizon_ms: Option<u64>,
    pub simulate_truncated: bool,
    pub simulate_reached_ms: Option<u64>,
    pub against: Option<SnapPolicyAgainst>,
}

/// `snapshot.policy.simulate` (plan 32 Step 2 "Simulation", Step 7.4):
/// run `expr` forward for `horizon_ms` from the daemon's clock, over the
/// snapshots of `path` (as that directory's policy) or over nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapPolicySimulateParams {
    #[serde(default)]
    pub path: Option<String>,
    pub expr: String,
    pub horizon_ms: u64,
}

/// `snapshot.policy.simulate`'s result: `constellation_meta::snapsched::
/// Timeline`, field for field (its docs are the reference). The daemon's
/// clock only places the synthetic future ticks; `now_unix_ms` says
/// which instant that was.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapTimeline {
    pub policy: String,
    pub policy_ino: u64,
    pub paused: bool,
    pub now_unix_ms: i64,
    pub horizon_unix_ms: i64,
    pub cadence: Option<String>,
    pub snapshots: Vec<SnapTimelineEntry>,
    pub counts: Vec<SnapCountPoint>,
    pub created: u32,
    pub expired: u32,
    pub final_count: u32,
    pub steady_state_bound: Option<u64>,
    pub truncated: bool,
}

/// One snapshot of a [`SnapTimeline`]: an existing one, or one the
/// simulation invented (`synthetic`, named as the scheduler would name
/// it).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapTimelineEntry {
    pub id: String,
    pub created_unix_ms: i64,
    pub synthetic: bool,
    pub candidate: bool,
    pub held: bool,
    pub held_by: Option<String>,
    pub keep: bool,
    pub reasons: Vec<SnapReason>,
    pub expires_unix_ms: Option<i64>,
    pub expired_unix_ms: Option<i64>,
}

/// One step of a [`SnapTimeline`]'s count-over-time series: every live
/// snapshot (`total`) and the policy's own (`candidates`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapCountPoint {
    pub at_unix_ms: i64,
    pub total: u32,
    pub candidates: u32,
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
    /// Only this view's operations (by id, as `view.list` shows it).
    #[serde(default)]
    pub view: Option<u64>,
}

/// `node.ops`'s result: the operations in flight (at least `min_age_s`
/// old, of `view` if named), oldest first, and the same per view.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OpsReport {
    /// Operations in flight (after the filters).
    pub in_flight: u64,
    /// Of them, reported as stalled (older than `stall_threshold_s`).
    pub stalled: u64,
    /// The age of the oldest non-blocking one.
    pub oldest_s: u64,
    pub stall_threshold_s: u64,
    /// The operations, oldest first, at most `OPS_LIST_MAX` of them
    /// (`truncated` says whether there were more).
    pub ops: Vec<OpEntry>,
    pub truncated: bool,
    /// Per view (by id), with its labels; operations of no view are under
    /// `view: null`. Counts cover all matching operations, not only the
    /// listed ones.
    pub views: Vec<ViewOps>,
}

/// The most operations `node.ops` lists (the counts are never capped).
pub const OPS_LIST_MAX: usize = 1000;

/// One operation in flight, as `node.ops` lists it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OpEntry {
    pub op: String,
    /// The inode it is about (0: a view-wide operation).
    pub ino: u64,
    pub age_s: u64,
    /// What the handler last noted it was waiting on.
    pub stage: String,
    /// The OS thread handling it.
    pub tid: u32,
    /// A blocking lock request: unbounded by design, not a stall.
    pub blocking: bool,
    /// The watchdog has reported it as stalled.
    pub stalled: bool,
    /// The view (by id) it belongs to.
    pub view: Option<u64>,
}

/// One view's share of `node.ops`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ViewOps {
    pub view: Option<u64>,
    /// The view's full label map (`view.list`'s).
    pub labels: BTreeMap<String, String>,
    pub in_flight: u64,
    pub stalled: u64,
    pub oldest_s: u64,
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

/// Plan 31 C8: the engine's lifecycle, as `node.status` reports it and
/// `node.lifecycle` answers: the profile it runs, what the host last said,
/// and what is in force because of both.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LifecycleStatus {
    /// The engine profile's modes: `p2p` (`listen`/`dial-only`/`off`),
    /// `leases` (`hold`/`forward-only`), `uploads`
    /// (`always`/`unmetered-only`), `background`
    /// (`continuous`/`on-demand`).
    pub profile: BTreeMap<String, String>,
    /// `foreground`, `background`, `suspending` or `suspended`.
    pub state: String,
    /// The host asked for reduced background work (until the next
    /// `Foreground` or `Resumed`).
    pub low_power: bool,
    /// The last `NetworkChanged` (reachable and unmetered until one says
    /// otherwise).
    pub network_reachable: bool,
    pub network_metered: bool,
    /// In force: the authority core never takes the lease from a live
    /// holder (the profile's `forward-only`, or a suspension).
    pub forward_only: bool,
    /// In force: the core takes no lease at all (a suspension).
    pub suspended: bool,
    /// In force: opportunistic chunk uploads hold (`unmetered-only` on a
    /// metered network), and how often the hold deferred one.
    pub uploads_held: bool,
    pub upload_deferrals: u64,
    /// In force: GC, prune, digests, pin refreshes, registry and
    /// designation polls are paused.
    pub background_paused: bool,
    /// The P2P endpoint admits inbound connections / takes part in
    /// gossip (both false without P2P; a suspension refuses both, and
    /// only this node's own requests dial out).
    pub p2p_accepts_inbound: bool,
    pub p2p_gossip: bool,
    /// Lifecycle events applied since the engine started, and the last.
    pub events: u64,
    pub last_event: Option<String>,
    pub last_suspend: Option<SuspendReport>,
    pub last_resume: Option<ResumeReport>,
}

/// What a `Suspending` achieved by its deadline.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SuspendReport {
    pub deadline_ms: u64,
    pub elapsed_ms: u64,
    /// Every step finished before the deadline.
    pub within_deadline: bool,
    /// Views open, and published by their `sync_view` barrier; one line
    /// per view that failed or did not finish in time.
    pub views: u64,
    pub views_synced: u64,
    pub view_errors: Vec<String>,
    /// The flush — every pending chunk up, the journal shipped, the lease
    /// released — finished; why not, when it did not.
    pub flushed: bool,
    pub flush_error: Option<String>,
    /// This node held no lease once the steps ran.
    pub lease_released: bool,
    /// What is left for after the resume (0 and 0 after a clean flush).
    pub journal_backlog: u64,
    pub pending_uploads: u64,
    /// P2P connections closed by the quiesce.
    pub p2p_connections_closed: u64,
}

/// What a `Resumed` did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ResumeReport {
    pub elapsed_ms: u64,
    /// The engine was suspended (a `Resumed` without one changes nothing).
    pub was_suspended: bool,
    /// The P2P endpoint admits connections again and re-dialed its peers.
    pub p2p_resumed: bool,
}

/// `node.lifecycle`'s answer: the event, applied, and the lifecycle it
/// left (with the suspension's or resumption's own report).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LifecycleReport {
    pub event: LifecycleEventSpec,
    /// The engine applied it before this answer (false: still applying
    /// well past a suspension's deadline). Lifecycle events are the
    /// host's, so an `EngineHost` delivers each to all its engines.
    pub applied: bool,
    pub status: LifecycleStatus,
}

/// Where `node.handoff` sends the FUSE sessions (plan 31 §6.11).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum HandoffTarget {
    /// The in-place upgrade of plan 31 C4b (`constellation daemon
    /// --upgrade`): the daemon `exec`s `binary` (default: the executable it
    /// was started from, as it is on disk now) with every session, its
    /// lock and its control listener inherited, under the same pid. Needs
    /// no descriptor; every mounted view goes (`views` must be empty) and
    /// the handover's own bounded drain applies (`drain_timeout_ms` must be
    /// absent).
    Exec {
        #[serde(default)]
        binary: Option<PathBuf>,
    },
    /// Plan 37 §8: hand the sessions of `views` (all when empty) to
    /// another process — a CSI engine pod's replacement — in the phases of
    /// [`HandoffPhase`], driven step by step by the node plugin. The sender
    /// is the serving daemon (`Prepare`, `Transfer`, `Commit`, `Abort`);
    /// the receiver a standby `constellation serve --handoff-socket`
    /// (`Receive`, `Seal`, `Status`). `Transfer` and `Receive` carry a
    /// descriptor (see [`HandoffPhase`]).
    Socket,
}

impl Default for HandoffTarget {
    fn default() -> Self {
        HandoffTarget::Exec { binary: None }
    }
}

/// The steps of a [`HandoffTarget::Socket`] handoff (plan 37 §8). Every
/// sender step before `Commit` is undone by `Abort`; the state dir's lock
/// decides which process serves, so the two never serve at once (the
/// receiver serves only once it holds the lock, the sender gives it up
/// only by exiting after `Commit`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HandoffPhase {
    /// Sender, §8 steps 1-3: stop reading every session, drain what was
    /// accepted (bounded by `drain_timeout_ms`), publish pending writes and
    /// export the handle tables. Nothing leaves the process; until
    /// `deadline_ms` passes without a `Commit` (then the sender aborts on
    /// its own: a plugin that died must not leave the mounts stalled).
    Prepare,
    /// Sender, §8 step 4: write one record per prepared view, each with its
    /// `/dev/fuse` descriptor, onto the unix stream socket attached to the
    /// request (`crate::handoff_wire`), then an end mark. The sender keeps
    /// its own copies until `Commit` or `Abort`.
    Transfer,
    /// Sender, §8 step 6: mark the state dir committed, close the views,
    /// stop the engine without draining it (the receiver is the same node
    /// on the same state dir: it ships the journal and the pending uploads
    /// and re-adopts the lease) and exit, which frees the state dir for the
    /// receiver. Refused once `Prepare`'s `deadline_ms` has passed. The
    /// point of no return.
    Commit,
    /// Sender: serve every prepared session again, in place. A no-op when
    /// nothing is prepared.
    Abort,
    /// Receiver: read records and their descriptors, exactly as a
    /// `Transfer` wrote them, from the unix stream socket attached to the
    /// request (`crate::handoff_wire`), and hold them without serving them.
    Receive,
    /// Receiver: every record is in; take the state dir as soon as the
    /// sender has freed it, start the node and resume every view (§8 step
    /// 5). Answered at once; `Status` follows it. Past `deadline_ms` with no
    /// commit by the sender, the receiver gives up; once the sender has
    /// committed it waits for the state dir without a bound.
    Seal,
    /// Receiver: where the adoption stands ([`HandoffReport::state`]).
    Status,
    /// Either side, before `Prepare` (37-k6a): the credentials an engine
    /// pod got through `fs.unlock` (plan 37 §9) cross to its standby, which
    /// has no other way to get them — nothing is in its pod spec or
    /// environment, and the node plugin may have restarted since it last
    /// held them. The sender writes them as one frame onto the unix stream
    /// socket attached to the request (`crate::handoff_wire::write_secret`;
    /// an empty frame when it holds none), the receiver reads that frame
    /// from the socket attached to its own request, checks the credentials
    /// against the bucket and pre-opens its backend with them. Nothing
    /// pauses: no session stops for it. A standby started `--await-unlock`
    /// refuses `Receive` until it has them.
    Credentials,
}

/// `node.handoff` (plan 31 §6.11). With [`HandoffTarget::Socket`] every
/// request names its [`HandoffPhase`], and `Transfer`/`Receive` carry a
/// descriptor and need a transport with fd passing; [`HandoffTarget::Exec`]
/// (the default) takes no phase and no descriptor.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HandoffParams {
    #[serde(default)]
    pub target: HandoffTarget,
    #[serde(default)]
    pub views: Vec<u64>,
    /// How long to wait for in-flight operations to drain.
    #[serde(default)]
    pub drain_timeout_ms: Option<u64>,
    /// `Socket` only: which step this request is.
    #[serde(default)]
    pub phase: Option<HandoffPhase>,
    /// `Prepare`: how long the sender stays prepared without a `Commit`
    /// before it aborts on its own (and after which it refuses one).
    /// `Seal`: how long the receiver waits for the sender to commit before
    /// it gives up (dropping what it received).
    #[serde(default)]
    pub deadline_ms: Option<u64>,
}

/// Where a [`HandoffTarget::Socket`] handoff stands, on either side.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HandoffState {
    /// Sender: serving, nothing prepared (also after an `Abort`).
    Serving,
    /// Sender: sessions stopped and exported, still held here.
    Prepared,
    /// Sender: records written; its copies still held.
    Transferred,
    /// Sender: committed, exiting.
    Committed,
    /// Receiver: waiting for records (`received` so far).
    Standby { received: u64 },
    /// Receiver: sealed, waiting for the state dir or starting the node.
    Sealed,
    /// Receiver: serving the views it resumed (failed ones listed in
    /// `failed`: their sessions ended).
    Resumed { failed: Vec<String> },
    /// Receiver: gave up (aborted, or the deadline passed); it exits.
    Failed { reason: String },
}

/// One session handed over.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HandedOffView {
    pub id: u64,
    pub mountpoint: String,
    /// Open handles exported with it.
    pub handles: u64,
    /// The transport its connection is served over (`dev_fuse`: the only
    /// one that can be handed over, plan 38 §3(e)).
    pub transport: String,
}

/// `node.handoff`'s result: answered once the sessions are detached; the
/// receiving image then serves them (for `Exec`, `node.status` reports
/// `handover.generation` one higher).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HandoffReport {
    /// A human-readable summary (the old `Upgrade` answer).
    pub detail: String,
    pub views: Vec<HandedOffView>,
    /// `Socket` only: the state the step left.
    pub state: Option<HandoffState>,
    /// How long the step took.
    pub elapsed_ms: u64,
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
    pub name: Option<String>,
    pub bucket: String,
    pub prefix: String,
    pub chunk_size: u32,
    pub compression: String,
    pub e2e: bool,
    pub write_mode: String,
    /// Whether this daemon has the credentials to open it (`fs.unlock`).
    pub unlocked: bool,
    /// The daemon's own filesystem only (0 elsewhere): how many times
    /// `fs.unlock` has set the S3 credentials its engine signs with (0:
    /// it signs with the environment's AWS chain), and the newest of those
    /// generations a signed S3 request has used. The two are equal once a
    /// rotation has reached the running S3 clients (plan 37 K6a).
    pub credentials_generation: u64,
    pub credentials_in_use: u64,
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
            source: MountSource::PreopenedFd {
                mountpoint: Some("/var/lib/kubelet/plugins/x/globalmount".into()),
                opts: MountViewOpts {
                    allow_other: true,
                    ..Default::default()
                },
            },
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
        // The largest type, with a peer carrying a cooperative-cache source:
        // the shape that once carried `skip_serializing_if`, which postcard
        // cannot read back.
        let mut report = StatusReport {
            fs_uuid: "u".into(),
            backend: "s3".into(),
            ..Default::default()
        };
        report.spool.head_seq = 4;
        report.p2p.peers.push(PeerStatus {
            node_id: 3,
            coop: Some(SourceStatus {
                id: "s3".into(),
                ..Default::default()
            }),
            ..Default::default()
        });
        let blob = Blob::encode(Encoding::Postcard, &report).unwrap();
        let back: StatusReport = blob.decode().unwrap();
        assert_eq!(back.fs_uuid, "u");
        assert_eq!(back.spool.head_seq, 4);
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

    /// Plan 31 L1208 and plan 37 §5 spell `snapshot.create`'s hold as the
    /// owner string; the CLI spells it as a flag plus `held_by`. Both
    /// decode, resolve to the same `(held, owner)`, and survive postcard.
    #[test]
    fn both_spellings_of_hold_decode_to_the_same_request() {
        let shorthand: SnapshotCreateParams = serde_json::from_value(serde_json::json!({
            "selector": "/vol@pvc-1", "hold": "csi:content-uid"
        }))
        .unwrap();
        assert_eq!(
            shorthand.hold_request().unwrap(),
            (true, Some("csi:content-uid"))
        );
        let spelled_out: SnapshotCreateParams = serde_json::from_value(serde_json::json!({
            "selector": "/vol@pvc-1", "hold": true, "held_by": "csi:content-uid"
        }))
        .unwrap();
        assert_eq!(
            spelled_out.hold_request().unwrap(),
            shorthand.hold_request().unwrap()
        );
        // `held_by` alone is still a hold, and so is nothing at all not.
        let owner_only: SnapshotCreateParams = serde_json::from_value(serde_json::json!({
            "selector": "/vol@pvc-1", "held_by": "user:attila"
        }))
        .unwrap();
        assert_eq!(
            owner_only.hold_request().unwrap(),
            (true, Some("user:attila"))
        );
        let plain: SnapshotCreateParams =
            serde_json::from_value(serde_json::json!({"selector": "/vol@pvc-1"})).unwrap();
        assert_eq!(plain.hold_request().unwrap(), (false, None));
        let flag: SnapshotCreateParams =
            serde_json::from_value(serde_json::json!({"selector": "/v@s", "hold": true})).unwrap();
        assert_eq!(flag.hold_request().unwrap(), (true, None));
        // Two owners that disagree is a client bug, not a coin flip.
        let conflict: SnapshotCreateParams = serde_json::from_value(serde_json::json!({
            "selector": "/v@s", "hold": "csi:a", "held_by": "user:b"
        }))
        .unwrap();
        assert!(conflict.hold_request().is_err());
        // The JSON spelling is what plan 37 documents, and the positional
        // encoding carries the same pair losslessly.
        assert_eq!(
            serde_json::to_value(&shorthand).unwrap()["hold"],
            serde_json::json!("csi:content-uid")
        );
        assert_eq!(
            serde_json::to_value(&flag).unwrap()["hold"],
            serde_json::json!(true)
        );
        round_trip(&shorthand);
        round_trip(&spelled_out);
        round_trip(&plain);
    }

    #[test]
    fn json_defaults_let_short_requests_through() {
        let p: ViewMountParams = serde_json::from_value(serde_json::json!({
            "subtree": "/", "source": {"PreopenedFd": {}}
        }))
        .unwrap();
        assert!(!p.confine_links && p.labels.is_empty());
        let s: StatsSubscribeParams = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(s.interval_ms, 1000);
    }
}
