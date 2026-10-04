//! [`SocketControlClient`]: the real [`ControlClient`], one
//! `constellation_control::Client` connection to one engine pod's daemon.
//!
//! The connection's transport is the caller's choice: the node plugin (K3)
//! dials the engine pod's hostPath unix socket ([`Self::connect_unix`],
//! the only transport that can carry `view.mount`'s `/dev/fuse` descriptor),
//! while the controller reaches its controller-owned engine pods through a
//! Kubernetes exec relay ([`crate::engine_pods`]) and hands the resulting
//! stream here through [`Self::new`]. Either way the methods are the
//! control protocol's own, 1:1, with two adaptations:
//!
//! - **`quota.*` always names its subtree.** `"/"` maps to the
//!   filesystem-wide cap (`subtree: None` on the wire) and every other path
//!   to that directory's own cap — never the other way round, which would
//!   silently widen a volume's cap to its whole pool.
//! - **Every call is bounded** ([`Self::with_timeout`]): a relay or a pod
//!   that stops answering must fail the CSI RPC (which the sidecar
//!   retries), not park it until the gRPC deadline.

use super::{ControlClient, SubtreeQuotaParams};
use async_trait::async_trait;
use constellation_control::fd::OwnedFd;
use constellation_control::methods::{
    BrowseDelete, BrowseMkdir, BrowseReaddir, BrowseRename, BrowseStat, BrowseXattr, CloneCreate,
    FsCreate, FsList, FsUnlock, Method, NodeHandoff, NodeLeave, NodePing, NodeStatus, PeersList,
    QuotaGet, QuotaSet, SnapshotCreate, SnapshotDelete, SnapshotHold, SnapshotList, ViewList,
    ViewMount, ViewStats, ViewUnmount,
};
use constellation_control::proto::types::{
    Ack, CloneParams, DeleteParams, DirectoryListing, FileStat, FsCreateParams, FsCreated,
    FsListing, FsUnlockParams, HandoffParams, HandoffReport, LeaveParams, MkdirParams, PathParams,
    PeerListing, Pong, QuotaGetParams, QuotaStatus, RenameParams, SetQuotaParams,
    SnapshotCreateParams, SnapshotCreated, SnapshotDeleteParams, SnapshotHeld, SnapshotHoldParams,
    SnapshotListParams, SnapshotListing, ViewInfo, ViewListParams, ViewListing, ViewMountParams,
    ViewStatsParams, ViewStatsReport, ViewUnmountParams, XattrParams, XattrResult,
};
use constellation_control::proto::ControlError;
use constellation_control::Client;
use std::time::Duration;

/// The default bound on one call. `fs.create` reads `meta.json` from S3
/// and may wait out a slow store; nothing the CSI driver sends is a bulk
/// transfer.
pub const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(30);

pub struct SocketControlClient {
    client: Client,
    timeout: Duration,
}

impl SocketControlClient {
    /// Wrap a connected, handshaken client.
    pub fn new(client: Client) -> SocketControlClient {
        SocketControlClient {
            client,
            timeout: DEFAULT_CALL_TIMEOUT,
        }
    }

    /// Dial a daemon's unix control socket.
    pub async fn connect_unix(path: &std::path::Path) -> Result<SocketControlClient, ControlError> {
        Ok(SocketControlClient::new(Client::connect_unix(path).await?))
    }

    pub fn with_timeout(mut self, timeout: Duration) -> SocketControlClient {
        self.timeout = timeout;
        self
    }

    /// Whether the connection is still up; a dead one fails every call
    /// `Unavailable`, and its owner should dial again.
    pub fn is_connected(&self) -> bool {
        self.client.is_connected()
    }

    /// The underlying connection, for methods outside [`ControlClient`].
    pub fn raw(&self) -> &Client {
        &self.client
    }

    async fn call<M: Method>(&self, params: M::Params) -> Result<M::Result, ControlError> {
        self.client.call_bounded::<M>(params, self.timeout).await
    }

    /// [`Self::call`] with `fd` attached, under the same bound.
    async fn call_fd<M: Method>(
        &self,
        params: M::Params,
        fd: OwnedFd,
    ) -> Result<M::Result, ControlError> {
        match tokio::time::timeout(self.timeout, self.client.call_with_fd::<M>(params, fd)).await {
            Ok(result) => result,
            Err(_) => Err(ControlError::new(
                constellation_control::proto::ErrorKind::Timeout,
                format!("{} did not finish within {:?}", M::NAME, self.timeout),
            )
            .with_code(constellation_types::Code::TimedOut)),
        }
    }
}

/// `"/"` (and `""`) is the filesystem-wide cap; anything else a subtree.
fn wire_subtree(subtree: &str) -> Option<String> {
    let trimmed = subtree.trim_matches('/');
    (!trimmed.is_empty()).then(|| format!("/{trimmed}"))
}

#[async_trait]
impl ControlClient for SocketControlClient {
    async fn fs_create(&self, params: FsCreateParams) -> Result<FsCreated, ControlError> {
        self.call::<FsCreate>(params).await
    }

    async fn fs_unlock(&self, params: FsUnlockParams) -> Result<Ack, ControlError> {
        self.call::<FsUnlock>(params).await
    }

    async fn fs_list(&self) -> Result<FsListing, ControlError> {
        self.call::<FsList>(Default::default()).await
    }

    async fn browse_mkdir(&self, params: MkdirParams) -> Result<FileStat, ControlError> {
        self.call::<BrowseMkdir>(params).await
    }

    async fn browse_xattr(&self, params: XattrParams) -> Result<XattrResult, ControlError> {
        self.call::<BrowseXattr>(params).await
    }

    async fn browse_rename(&self, params: RenameParams) -> Result<Ack, ControlError> {
        self.call::<BrowseRename>(params).await
    }

    async fn browse_readdir(&self, path: &str) -> Result<DirectoryListing, ControlError> {
        self.call::<BrowseReaddir>(PathParams {
            path: path.to_string(),
        })
        .await
    }

    async fn browse_stat(&self, path: &str) -> Result<FileStat, ControlError> {
        self.call::<BrowseStat>(PathParams {
            path: path.to_string(),
        })
        .await
    }

    async fn browse_delete(&self, params: DeleteParams) -> Result<Ack, ControlError> {
        self.call::<BrowseDelete>(params).await
    }

    async fn quota_get(&self, subtree: &str) -> Result<QuotaStatus, ControlError> {
        self.call::<QuotaGet>(QuotaGetParams {
            subtree: wire_subtree(subtree),
            cap_only: false,
        })
        .await
    }

    async fn quota_cap(&self, subtree: &str) -> Result<Option<u64>, ControlError> {
        let status = self
            .call::<QuotaGet>(QuotaGetParams {
                subtree: wire_subtree(subtree),
                cap_only: true,
            })
            .await?;
        Ok(status.max_bytes)
    }

    async fn quota_set(&self, params: SubtreeQuotaParams) -> Result<QuotaStatus, ControlError> {
        self.call::<QuotaSet>(SetQuotaParams {
            max_bytes: params.max_bytes,
            subtree: wire_subtree(&params.subtree),
        })
        .await
    }

    async fn snapshot_create(
        &self,
        params: SnapshotCreateParams,
    ) -> Result<SnapshotCreated, ControlError> {
        self.call::<SnapshotCreate>(params).await
    }

    async fn snapshot_delete(&self, params: SnapshotDeleteParams) -> Result<Ack, ControlError> {
        self.call::<SnapshotDelete>(params).await
    }

    async fn snapshot_list(
        &self,
        params: SnapshotListParams,
    ) -> Result<SnapshotListing, ControlError> {
        self.call::<SnapshotList>(params).await
    }

    async fn snapshot_hold(
        &self,
        params: SnapshotHoldParams,
    ) -> Result<SnapshotHeld, ControlError> {
        self.call::<SnapshotHold>(params).await
    }

    async fn clone_create(&self, params: CloneParams) -> Result<Ack, ControlError> {
        self.call::<CloneCreate>(params).await
    }

    /// Without a descriptor: a `PreopenedFd` mount goes through
    /// [`Self::view_mount_fd`].
    async fn view_mount(&self, params: ViewMountParams) -> Result<ViewInfo, ControlError> {
        self.call::<ViewMount>(params).await
    }

    async fn view_mount_fd(
        &self,
        params: ViewMountParams,
        fd: OwnedFd,
    ) -> Result<ViewInfo, ControlError> {
        self.call_fd::<ViewMount>(params, fd).await
    }

    async fn view_list(&self, params: ViewListParams) -> Result<ViewListing, ControlError> {
        self.call::<ViewList>(params).await
    }

    async fn view_unmount(&self, params: ViewUnmountParams) -> Result<Ack, ControlError> {
        self.call::<ViewUnmount>(params).await
    }

    async fn view_stats(&self, params: ViewStatsParams) -> Result<ViewStatsReport, ControlError> {
        self.call::<ViewStats>(params).await
    }

    async fn node_ping(&self) -> Result<Pong, ControlError> {
        self.call::<NodePing>(Default::default()).await
    }

    async fn node_handoff(&self, params: HandoffParams) -> Result<HandoffReport, ControlError> {
        self.call::<NodeHandoff>(params).await
    }

    async fn node_handoff_fd(
        &self,
        params: HandoffParams,
        fd: OwnedFd,
    ) -> Result<HandoffReport, ControlError> {
        self.call_fd::<NodeHandoff>(params, fd).await
    }

    async fn node_leave(&self, params: LeaveParams) -> Result<Ack, ControlError> {
        self.call::<NodeLeave>(params).await
    }

    async fn node_id(&self) -> Result<u64, ControlError> {
        Ok(self.call::<NodeStatus>(Default::default()).await?.node_id)
    }

    async fn peers_list(&self) -> Result<PeerListing, ControlError> {
        self.call::<PeersList>(Default::default()).await
    }

    async fn node_enrolled(&self) -> Result<bool, ControlError> {
        Ok(self.call::<NodeStatus>(Default::default()).await?.enrolled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_root_is_the_filesystem_wide_cap() {
        assert_eq!(wire_subtree("/"), None);
        assert_eq!(wire_subtree(""), None);
        assert_eq!(wire_subtree("//"), None);
        assert_eq!(
            wire_subtree("/volumes/pvc-1"),
            Some("/volumes/pvc-1".into())
        );
        assert_eq!(
            wire_subtree("volumes/pvc-1/"),
            Some("/volumes/pvc-1".into())
        );
    }
}
