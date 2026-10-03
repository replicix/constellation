//! [`ControlClient`]: the seam between the CSI services and a Constellation
//! engine pod's control socket (plan 37 §4's "control-protocol methods this
//! plan drives" table, §5's RPC mapping).
//!
//! `constellation-csi` never links against `constellation-engine` (plan 37
//! settled decision 1): every Controller/Node RPC reaches an engine pod only
//! through this trait, in the same typed shapes
//! `constellation-control::proto::types` already defines for the daemon's
//! own CLI and UI clients. Two implementations: [`SocketControlClient`],
//! the real one over any control-protocol transport (an engine pod's unix
//! socket, or the controller's exec relay, [`crate::engine_pods`]), and
//! [`InMemoryControl`], an in-process fake used by unit tests and by
//! `csi-sanity` so the sanity suite never touches real S3.

use crate::params::ClassParams;
use async_trait::async_trait;
use constellation_control::fd::OwnedFd;
use constellation_control::proto::types::{
    Ack, CloneParams, FileStat, FsCreateParams, FsCreated, FsListing, FsUnlockParams,
    HandoffParams, HandoffReport, LeaveParams, MkdirParams, Pong, QuotaStatus, RenameParams,
    SnapshotCreateParams, SnapshotCreated, SnapshotDeleteParams, SnapshotHeld, SnapshotHoldParams,
    SnapshotListParams, SnapshotListing, ViewInfo, ViewListParams, ViewListing, ViewMountParams,
    ViewStatsParams, ViewStatsReport, ViewUnmountParams, XattrParams, XattrResult,
};
use constellation_control::proto::ControlError;
use std::collections::BTreeMap;
use std::sync::Arc;

/// §5's `quota.set{subtree, bytes}`: a byte cap on one directory subtree
/// (`/volumes/<pv>` for a pool volume, `/` for a dedicated one).
///
/// The control protocol's `SetQuotaParams{max_bytes, subtree}` with the
/// subtree always spelled out: `"/"` is the filesystem-wide cap (a
/// dedicated volume's), any other path that directory's own subtree cap
/// (plan 37 K2 added those to the engine). A client must never map a
/// subtree onto the filesystem-wide cap — that would widen a PV's cap to
/// the whole pool.
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
    /// `fs.list`: an engine pod's registry is empty, so its one unnamed
    /// entry is the filesystem it serves (how the node plugin checks that
    /// a pod serves the filesystem a `volume_id` names).
    async fn fs_list(&self) -> Result<FsListing, ControlError>;

    async fn browse_mkdir(&self, params: MkdirParams) -> Result<FileStat, ControlError>;
    async fn browse_xattr(&self, params: XattrParams) -> Result<XattrResult, ControlError>;
    async fn browse_rename(&self, params: RenameParams) -> Result<Ack, ControlError>;

    /// `quota.get` for `subtree` (see [`SubtreeQuotaParams`]): the cap and
    /// the bytes under it. `NotFound` when the subtree does not exist. For
    /// any subtree but `/` the usage is a walk of every entry under it
    /// (O(entries), seconds for a ten-million-file volume), so only callers
    /// that need the bytes use it — `NodeGetVolumeStats` (K3) and the
    /// adoption check on a directory with no record.
    async fn quota_get(&self, subtree: &str) -> Result<QuotaStatus, ControlError>;
    /// `quota.get{cap_only}`: the cap alone, with no usage walk — O(1)
    /// whatever the volume holds. `NotFound` when the subtree does not
    /// exist; `ValidateVolumeCapabilities` and `ControllerExpandVolume`
    /// use it as their existence check.
    async fn quota_cap(&self, subtree: &str) -> Result<Option<u64>, ControlError>;
    /// `quota.set` on `subtree`; the result's `used_bytes` is `0` for a
    /// subtree (no walk). A missing subtree should be `NotFound`, but
    /// callers must not depend on it for idempotency: an engine that
    /// reports the failure as `Failed` (as the filesystem-wide path did)
    /// would read as transient and be retried. `DeleteVolume` therefore
    /// checks existence first (`browse.xattr list`, O(1)).
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
    /// `view.mount{PreopenedFd}` with the `/dev/fuse` descriptor `fd`
    /// attached (`SCM_RIGHTS`): the node plugin's `NodeStageVolume`. The
    /// descriptor is consumed — closed here once sent, whatever the
    /// answer — so the engine pod ends up its only holder. `NotSupported`
    /// on a connection that cannot pass descriptors (the controller's
    /// exec relay).
    async fn view_mount_fd(
        &self,
        params: ViewMountParams,
        fd: OwnedFd,
    ) -> Result<ViewInfo, ControlError>;
    async fn view_list(&self, params: ViewListParams) -> Result<ViewListing, ControlError>;
    async fn view_unmount(&self, params: ViewUnmountParams) -> Result<Ack, ControlError>;
    async fn view_stats(&self, params: ViewStatsParams) -> Result<ViewStatsReport, ControlError>;

    async fn node_ping(&self) -> Result<Pong, ControlError>;
    async fn node_handoff(&self, params: HandoffParams) -> Result<HandoffReport, ControlError>;
    async fn node_leave(&self, params: LeaveParams) -> Result<Ack, ControlError>;
}

/// One pool filesystem (one shard of a pool `StorageClass`), as
/// `CreateVolume` knows it: where it lives, how to create it, and the
/// credentials the request carried (`req.secrets`, the class's
/// provisioner secret resolved by external-provisioner).
#[derive(Clone, PartialEq, Eq)]
pub struct PoolRef {
    pub class: ClassParams,
    pub shard: u32,
    /// Never logged: `Debug` prints the key names only.
    pub secrets: BTreeMap<String, String>,
}

impl PoolRef {
    /// The pool filesystem's bucket prefix.
    pub fn prefix(&self) -> String {
        self.class.pool_prefix(self.shard)
    }
}

impl std::fmt::Debug for PoolRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolRef")
            .field("bucket", &self.class.bucket)
            .field("prefix", &self.prefix())
            .field("shard", &self.shard)
            .field("secrets", &self.secrets.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// How the controller reaches engine pods (plan 37 §4, §"Engine-pod
/// lifecycle"): one [`ControlClient`] per filesystem, because `browse.*` and
/// `quota.*` act on whichever filesystem the engine pod behind the client
/// serves and carry no filesystem selector of their own.
///
/// The controller is stateless (settled decision 7): it asks for a client
/// on every RPC — by pool for `CreateVolume`, by the filesystem uuid parsed
/// out of `volume_id` for the rest — and never caches a volume-to-pod
/// mapping itself. [`crate::engine_pods::EnginePodManager`] (the
/// controller-owned engine pods) may cache connections; [`InMemoryEngines`]
/// backs the unit tests and `csi-sanity`.
#[async_trait]
pub trait Engines: Send + Sync {
    /// An engine pod serving `pool`, brought up first if need be (and the
    /// pool filesystem created with it): `fs.create` through it answers
    /// the pool's uuid, `browse.*`/`quota.*` act on the pool.
    async fn pool(&self, pool: &PoolRef) -> Result<Arc<dyn ControlClient>, ControlError>;

    /// An engine pod serving filesystem `fs_uuid`. `NotFound` only when the
    /// filesystem itself is known to be gone, which `DeleteVolume` reads as
    /// "already gone"; an implementation that merely cannot find a pod for
    /// it answers `Unavailable` (retryable), never `NotFound`.
    async fn filesystem(&self, fs_uuid: &str) -> Result<Arc<dyn ControlClient>, ControlError>;

    /// An engine pod serving `fs_uuid` **only if one is up already**: never
    /// created, recreated from a remembered spec, or rebuilt from the
    /// cluster. `None` when there is none. The deletes start here, so that
    /// a delete the CO repeats after the object is gone (external-provisioner
    /// does, seconds after deleting the PV) brings nothing back.
    async fn running(&self, fs_uuid: &str) -> Result<Option<Arc<dyn ControlClient>>, ControlError>;

    /// Whether the CO still has an object naming `handle`: a
    /// `PersistentVolume` of this driver with that `volumeHandle`, or a
    /// `VolumeSnapshotContent` with that snapshot handle. The sidecars
    /// delete their object only after the delete RPC succeeded, so `false`
    /// means an earlier delete of it already did. Without a CO (the
    /// in-memory backend, csi-sanity) every handle counts as named.
    async fn named(&self, handle: Handle<'_>) -> Result<bool, ControlError>;

    /// Stop the engine pod serving `fs_uuid`, which a delete brought up
    /// only for itself, unless something else is using it by now.
    async fn retire(&self, fs_uuid: &str) -> Result<(), ControlError>;

    /// Every filesystem an engine pod serves now (an unfiltered
    /// `ListSnapshots` lists their snapshots; it starts no pod).
    async fn running_filesystems(&self) -> Result<Vec<String>, ControlError>;
}

/// A CO object's handle, for [`Engines::named`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Handle<'a> {
    /// A `PersistentVolume`'s `spec.csi.volumeHandle`.
    Volume(&'a str),
    /// A `VolumeSnapshotContent`'s snapshot handle.
    Snapshot(&'a str),
}

mod fake;
mod socket;
pub use fake::{InMemoryControl, InMemoryEngines};
pub use socket::SocketControlClient;
