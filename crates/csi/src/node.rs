//! The Node service: wired so `csi-sanity`/kubelet can connect. Staging
//! and publishing (`fuse_mount_fd`/`view.mount`) are 37-K3's (§15), so
//! every RPC answers `UNIMPLEMENTED` except two whose correct answer is
//! already known while nothing can be published:
//!
//! - `NodeGetCapabilities`: none yet. K3 advertises `STAGE_UNSTAGE_VOLUME`
//!   and `GET_VOLUME_STATS` (§5) together with the RPCs behind them.
//! - `NodeUnpublishVolume`: `OK`. No RPC in this build publishes anything,
//!   so every target is "not mounted there", which §5 and the CSI spec
//!   answer with `OK` (idempotent).
//!
//! Both exist because `csi-sanity` cleans up every volume its Controller
//! tests create through the Node service too (`NodeUnpublishVolume`, then
//! `NodeGetCapabilities` to decide on `NodeUnstageVolume`), so the
//! Controller group cannot pass against a Node service that refuses them.

use crate::control_client::ControlClient;
use crate::proto::csi::v1::node_server::Node as NodeRpc;
use crate::proto::csi::v1::*;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct NodeService {
    /// The k8s node name (`NodeGetInfo.node_id`, K3). Unused until then.
    #[allow(dead_code)]
    node_id: String,
    /// Unused until K3 wires real RPCs against it.
    #[allow(dead_code)]
    engine: Option<Arc<dyn ControlClient>>,
}

impl NodeService {
    pub fn new(node_id: String, engine: Option<Arc<dyn ControlClient>>) -> NodeService {
        NodeService { node_id, engine }
    }
}

/// Stub every listed method of `$trait` on `$ty` with
/// `Err(Status::unimplemented)`, alongside the two real ones (module docs).
macro_rules! unimplemented_rpcs {
    ($ty:ty, $trait:ty { $($method:ident($req:ty) -> $resp:ty,)* }) => {
        #[tonic::async_trait]
        impl $trait for $ty {
            async fn node_unpublish_volume(
                &self,
                request: Request<NodeUnpublishVolumeRequest>,
            ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
                let req = request.into_inner();
                if req.volume_id.is_empty() || req.target_path.is_empty() {
                    return Err(Status::invalid_argument(
                        "volume_id and target_path are required",
                    ));
                }
                Ok(Response::new(NodeUnpublishVolumeResponse {}))
            }

            async fn node_get_capabilities(
                &self,
                _request: Request<NodeGetCapabilitiesRequest>,
            ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
                Ok(Response::new(NodeGetCapabilitiesResponse {
                    capabilities: Vec::new(),
                }))
            }
            $(
                async fn $method(
                    &self,
                    _request: Request<$req>,
                ) -> Result<Response<$resp>, Status> {
                    Err(Status::unimplemented(concat!(
                        stringify!($method),
                        ": plan 37 K3 implements the Node service"
                    )))
                }
            )*
        }
    };
}

unimplemented_rpcs!(NodeService, NodeRpc {
    node_stage_volume(NodeStageVolumeRequest) -> NodeStageVolumeResponse,
    node_unstage_volume(NodeUnstageVolumeRequest) -> NodeUnstageVolumeResponse,
    node_publish_volume(NodePublishVolumeRequest) -> NodePublishVolumeResponse,
    node_get_volume_stats(NodeGetVolumeStatsRequest) -> NodeGetVolumeStatsResponse,
    node_get_volume_health(NodeGetVolumeHealthRequest) -> NodeGetVolumeHealthResponse,
    node_get_storage_health(NodeGetStorageHealthRequest) -> NodeGetStorageHealthResponse,
    node_expand_volume(NodeExpandVolumeRequest) -> NodeExpandVolumeResponse,
    node_get_info(NodeGetInfoRequest) -> NodeGetInfoResponse,
});
