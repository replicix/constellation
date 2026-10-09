//! The Identity service (CSI spec §4): the only service plan 37 K1 fully
//! implements. `GetPluginInfo`/`GetPluginCapabilities` are static;
//! `Probe` is the one RPC with real behavior, and it differs by which
//! binary entry point is serving it (plan 37 §4):
//!
//! - **Node**: local process health only — no engine pod to check from
//!   here (each PV's engine pod is a `NodeStageVolume` concern, K3).
//! - **Controller**: `node.ping` against a reachable engine pod, when one
//!   is configured; `ready: true` when none is (K1/K2: nothing has wired up
//!   engine-pod discovery yet, so "no engine pod expected" is the normal
//!   case, not a failure — see §"Engine-pod lifecycle").

use crate::control_client::ControlClient;
use crate::proto::csi::v1::identity_server::Identity as IdentityRpc;
use crate::proto::csi::v1::plugin_capability::{
    service::Type as ServiceCapability, Service, Type as CapabilityType,
};
use crate::proto::csi::v1::{
    GetPluginCapabilitiesRequest, GetPluginCapabilitiesResponse, GetPluginInfoRequest,
    GetPluginInfoResponse, PluginCapability, ProbeRequest, ProbeResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// `spec.driverName`, `GetPluginInfo.name`, and every `*.storage.k8s.io`
/// `driver`/`provisioner` field (plan 37 settled decision 2).
pub const DRIVER_NAME: &str = "constellation.csi.replicix.com";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    Controller,
    Node,
}

/// The Identity service, shared by both binary entry points (only `Probe`'s
/// behavior depends on which one is serving it).
pub struct IdentityService {
    mode: Mode,
    /// `Mode::Controller` only: a reachable engine pod, when one is
    /// configured. Always `None` in `Mode::Node` (node `Probe` never calls
    /// out — see the module docs).
    engine: Option<Arc<dyn ControlClient>>,
}

impl IdentityService {
    pub fn controller(engine: Option<Arc<dyn ControlClient>>) -> IdentityService {
        IdentityService {
            mode: Mode::Controller,
            engine,
        }
    }

    pub fn node() -> IdentityService {
        IdentityService {
            mode: Mode::Node,
            engine: None,
        }
    }
}

#[tonic::async_trait]
impl IdentityRpc for IdentityService {
    async fn get_plugin_info(
        &self,
        _request: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        Ok(Response::new(GetPluginInfoResponse {
            name: DRIVER_NAME.to_string(),
            vendor_version: env!("CARGO_PKG_VERSION").to_string(),
            manifest: Default::default(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _request: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        // `CONTROLLER_SERVICE` only: no `VOLUME_ACCESSIBILITY_CONSTRAINTS`
        // (every node reaches every filesystem's S3 bucket directly, plan
        // 37 settled decision — the Identity service's own capability list
        // in §4).
        Ok(Response::new(GetPluginCapabilitiesResponse {
            capabilities: vec![PluginCapability {
                r#type: Some(CapabilityType::Service(Service {
                    r#type: ServiceCapability::ControllerService as i32,
                })),
            }],
        }))
    }

    async fn probe(
        &self,
        _request: Request<ProbeRequest>,
    ) -> Result<Response<ProbeResponse>, Status> {
        let ready = match (self.mode, &self.engine) {
            (Mode::Node, _) => true,
            (Mode::Controller, None) => true,
            (Mode::Controller, Some(engine)) => match engine.node_ping().await {
                Ok(_) => true,
                Err(e) => {
                    tracing::warn!(error = %e, "controller probe: node.ping failed");
                    false
                }
            },
        };
        Ok(Response::new(ProbeResponse { ready: Some(ready) }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_client::InMemoryControl;

    /// `(Controller, Some(engine))` with a healthy engine: the arm nothing
    /// else exercises (`main.rs` always passes `None`, `tests/identity.rs`
    /// only builds `IdentityService::node()`, and `make csi-sanity` runs
    /// `--controller` with no engine configured, so it only ever hits the
    /// `(Controller, None)` arm).
    #[tokio::test]
    async fn controller_probe_ready_when_engine_answers() {
        let service = IdentityService::controller(Some(Arc::new(InMemoryControl::default())));
        let ready = service
            .probe(Request::new(ProbeRequest {}))
            .await
            .unwrap()
            .into_inner()
            .ready;
        assert_eq!(ready, Some(true));
    }

    /// The same arm with a dead engine pod: `ready: false`, not an RPC
    /// error (plan 37 §5) — this is the path that was previously
    /// unreachable in any test, so an inverted `is_ok()` here would have
    /// passed every gate in the chunk.
    #[tokio::test]
    async fn controller_probe_not_ready_when_engine_is_unreachable() {
        let engine = Arc::new(InMemoryControl::default());
        engine.mark_unreachable();
        let service = IdentityService::controller(Some(engine));
        let ready = service
            .probe(Request::new(ProbeRequest {}))
            .await
            .unwrap()
            .into_inner()
            .ready;
        assert_eq!(ready, Some(false));
    }
}
