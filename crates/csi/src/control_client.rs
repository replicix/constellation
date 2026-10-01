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
    HandoffReport, LeaveParams, MkdirParams, Pong, QuotaStatus, RenameParams, SetQuotaParams,
    SnapshotCreateParams, SnapshotCreated, SnapshotDeleteParams, SnapshotHeld, SnapshotHoldParams,
    SnapshotListParams, SnapshotListing, ViewInfo, ViewMountParams, ViewStatsParams,
    ViewStatsReport, ViewUnmountParams, XattrParams, XattrResult,
};
use constellation_control::proto::ControlError;

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

    async fn quota_get(&self) -> Result<QuotaStatus, ControlError>;
    async fn quota_set(&self, params: SetQuotaParams) -> Result<QuotaStatus, ControlError>;

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

mod fake;
pub use fake::InMemoryControl;
