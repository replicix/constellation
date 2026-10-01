//! [`ControlClient`]: the seam between the CSI services and a Constellation
//! engine pod's control socket (plan 37 §4's "control-protocol methods this
//! plan drives" table, §5's RPC mapping).
//!
//! `constellation-csi` never links against `constellation-engine` (plan 37
//! settled decision 1): every Controller/Node RPC reaches an engine pod only
//! through this trait, in the same typed shapes
//! `constellation-control::proto::types` already defines for the daemon's
//! own CLI and UI clients. The real implementation — wrapping
//! `constellation_control::Client` over `Transport::send_fd` — is 37-k2b's;
//! this K1 chunk ships the trait plus [`InMemoryControl`], an in-process
//! fake used by unit tests and by `csi-sanity` so the sanity suite never
//! touches real S3.

use async_trait::async_trait;
use constellation_control::proto::types::{
    Ack, CloneParams, FileStat, FsCreateParams, FsCreated, FsUnlockParams, HandoffParams,
    HandoffReport, LeaveParams, MkdirParams, Pong, QuotaStatus, RenameParams, SnapshotCreateParams,
    SnapshotCreated, SnapshotDeleteParams, SnapshotHeld, SnapshotHoldParams, SnapshotListParams,
    SnapshotListing, ViewInfo, ViewMountParams, ViewStatsParams, ViewStatsReport,
    ViewUnmountParams, XattrParams, XattrResult,
};
use constellation_control::proto::ControlError;
use std::sync::Arc;

/// §5's `quota.set{subtree, bytes}`: a byte cap on one directory subtree
/// (`/volumes/<pv>` for a pool volume, `/` for a dedicated one).
///
/// Not the control protocol's `SetQuotaParams` (`{max_bytes}`, a
/// filesystem-wide cap): the engine has no per-subtree quota yet, so this
/// is the one place the trait is shaped by what plan 37 needs rather than
/// by what `constellation-control` already carries. The real client
/// (37-k2b) maps `subtree: "/"` onto today's `quota.set{max_bytes}` and
/// must refuse any other subtree with `Unsupported` until the engine grows
/// subtree quotas — never silently widen a PV's cap to the whole pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubtreeQuotaParams {
    /// Absolute path inside the filesystem the client is bound to.
    pub subtree: String,
    /// `None` clears the cap (unlimited).
    pub max_bytes: Option<u64>,
}

/// An async client of one engine pod's control protocol, scoped to exactly
/// the methods plan 37's Controller/Node/Identity services need (§4, §5).
/// Every method name matches its control-protocol method 1:1
/// (`fs_create` ↔ `fs.create`, …) so a caller can read the RPC-mapping table
/// alongside the trait.
#[async_trait]
pub trait ControlClient: Send + Sync {
    async fn fs_create(&self, params: FsCreateParams) -> Result<FsCreated, ControlError>;
    async fn fs_unlock(&self, params: FsUnlockParams) -> Result<Ack, ControlError>;

    async fn browse_mkdir(&self, params: MkdirParams) -> Result<FileStat, ControlError>;
    async fn browse_xattr(&self, params: XattrParams) -> Result<XattrResult, ControlError>;
    async fn browse_rename(&self, params: RenameParams) -> Result<Ack, ControlError>;

    /// `quota.get` for `subtree` (see [`SubtreeQuotaParams`]). `NotFound`
    /// when the subtree does not exist — `ValidateVolumeCapabilities`'
    /// existence check (§5) relies on that.
    async fn quota_get(&self, subtree: &str) -> Result<QuotaStatus, ControlError>;
    async fn quota_set(&self, params: SubtreeQuotaParams) -> Result<QuotaStatus, ControlError>;

    async fn snapshot_create(
        &self,
        params: SnapshotCreateParams,
    ) -> Result<SnapshotCreated, ControlError>;
    async fn snapshot_delete(&self, params: SnapshotDeleteParams) -> Result<Ack, ControlError>;
    async fn snapshot_list(
        &self,
        params: SnapshotListParams,
    ) -> Result<SnapshotListing, ControlError>;
    async fn snapshot_hold(&self, params: SnapshotHoldParams)
        -> Result<SnapshotHeld, ControlError>;

    async fn clone_create(&self, params: CloneParams) -> Result<Ack, ControlError>;

    async fn view_mount(&self, params: ViewMountParams) -> Result<ViewInfo, ControlError>;
    async fn view_unmount(&self, params: ViewUnmountParams) -> Result<Ack, ControlError>;
    async fn view_stats(&self, params: ViewStatsParams) -> Result<ViewStatsReport, ControlError>;

    async fn node_ping(&self) -> Result<Pong, ControlError>;
    async fn node_handoff(&self, params: HandoffParams) -> Result<HandoffReport, ControlError>;
    async fn node_leave(&self, params: LeaveParams) -> Result<Ack, ControlError>;
}

/// How the controller reaches engine pods (plan 37 §4, §"Engine-pod
/// lifecycle"): one [`ControlClient`] per filesystem, because `browse.*` and
/// `quota.*` act on whichever filesystem the engine pod behind the client
/// serves and carry no filesystem selector of their own.
///
/// The controller is stateless (settled decision 7): it asks for a client
/// by filesystem uuid on every RPC — parsed out of `volume_id`, or returned
/// by `fs.create` — and never caches a volume-to-pod mapping itself. The
/// real implementation (controller-owned engine pods, 37-k2b) may cache
/// connections; [`InMemoryEngines`] backs the unit tests and `csi-sanity`.
#[async_trait]
pub trait Engines: Send + Sync {
    /// Any engine pod that can answer filesystem-registry calls
    /// (`fs.create`) — the pool's own pod need not exist yet.
    async fn registry(&self) -> Result<Arc<dyn ControlClient>, ControlError>;

    /// An engine pod serving filesystem `fs_uuid`. `NotFound` when no such
    /// filesystem is registered, which `DeleteVolume` reads as "already
    /// gone".
    async fn filesystem(&self, fs_uuid: &str) -> Result<Arc<dyn ControlClient>, ControlError>;
}

mod fake;
pub use fake::{InMemoryControl, InMemoryEngines};
