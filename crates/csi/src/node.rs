//! The Node service: wired so `csi-sanity`/kubelet can connect, but every
//! RPC answers `UNIMPLEMENTED` — `fuse_mount_fd`/`view.mount` staging is
//! 37-K3's (§15).

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

macro_rules! unimplemented_rpcs {
    ($ty:ty, $trait:ty { $($method:ident($req:ty) -> $resp:ty,)* }) => {
        #[tonic::async_trait]
        impl $trait for $ty {
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
    node_unpublish_volume(NodeUnpublishVolumeRequest) -> NodeUnpublishVolumeResponse,
    node_get_volume_stats(NodeGetVolumeStatsRequest) -> NodeGetVolumeStatsResponse,
    node_get_volume_health(NodeGetVolumeHealthRequest) -> NodeGetVolumeHealthResponse,
    node_get_storage_health(NodeGetStorageHealthRequest) -> NodeGetStorageHealthResponse,
    node_expand_volume(NodeExpandVolumeRequest) -> NodeExpandVolumeResponse,
    node_get_capabilities(NodeGetCapabilitiesRequest) -> NodeGetCapabilitiesResponse,
    node_get_info(NodeGetInfoRequest) -> NodeGetInfoResponse,
});
