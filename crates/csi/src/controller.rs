//! The Controller service: wired so `csi-sanity`/the sidecars can connect
//! and so `ControllerGetCapabilities` et al. have a real (if empty)
//! endpoint, but every RPC answers `UNIMPLEMENTED` — volume/expansion logic
//! is 37-K2's (§15).

use crate::control_client::ControlClient;
use crate::proto::csi::v1::controller_server::Controller as ControllerRpc;
use crate::proto::csi::v1::*;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct ControllerService {
    /// Unused until K2 wires real RPCs against it; held here so the
    /// constructor's shape does not change out from under K2.
    #[allow(dead_code)]
    engine: Option<Arc<dyn ControlClient>>,
}

impl ControllerService {
    pub fn new(engine: Option<Arc<dyn ControlClient>>) -> ControllerService {
        ControllerService { engine }
    }
}

/// Stub every method of `$trait` on `$ty` with `Err(Status::unimplemented)`,
/// naming the RPC in the message.
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
                        ": plan 37 K2/K4 implements the Controller service"
                    )))
                }
            )*
        }
    };
}

unimplemented_rpcs!(ControllerService, ControllerRpc {
    create_volume(CreateVolumeRequest) -> CreateVolumeResponse,
    delete_volume(DeleteVolumeRequest) -> DeleteVolumeResponse,
    controller_publish_volume(ControllerPublishVolumeRequest) -> ControllerPublishVolumeResponse,
    controller_unpublish_volume(ControllerUnpublishVolumeRequest) -> ControllerUnpublishVolumeResponse,
    validate_volume_capabilities(ValidateVolumeCapabilitiesRequest) -> ValidateVolumeCapabilitiesResponse,
    list_volumes(ListVolumesRequest) -> ListVolumesResponse,
    controller_list_volume_health(ControllerListVolumeHealthRequest) -> ControllerListVolumeHealthResponse,
    controller_get_volume_health(ControllerGetVolumeHealthRequest) -> ControllerGetVolumeHealthResponse,
    get_capacity(GetCapacityRequest) -> GetCapacityResponse,
    controller_get_capabilities(ControllerGetCapabilitiesRequest) -> ControllerGetCapabilitiesResponse,
    create_snapshot(CreateSnapshotRequest) -> CreateSnapshotResponse,
    delete_snapshot(DeleteSnapshotRequest) -> DeleteSnapshotResponse,
    list_snapshots(ListSnapshotsRequest) -> ListSnapshotsResponse,
    get_snapshot(GetSnapshotRequest) -> GetSnapshotResponse,
    controller_expand_volume(ControllerExpandVolumeRequest) -> ControllerExpandVolumeResponse,
    controller_get_volume(ControllerGetVolumeRequest) -> ControllerGetVolumeResponse,
    controller_modify_volume(ControllerModifyVolumeRequest) -> ControllerModifyVolumeResponse,
});
