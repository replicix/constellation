//! Starts the real gRPC server (Identity + Node) over a unix socket and
//! calls it through a tonic client — the Rust-side companion to
//! `tests/csi/sanity.sh`'s csi-sanity run (plan 37 K1 gate: "a Rust
//! integration test that at least starts the server and calls
//! `GetPluginInfo` through a tonic client").

use constellation_csi::identity::{IdentityService, DRIVER_NAME};
use constellation_csi::node::NodeService;
use constellation_csi::proto::csi::v1::identity_client::IdentityClient;
use constellation_csi::proto::csi::v1::identity_server::IdentityServer;
use constellation_csi::proto::csi::v1::node_server::NodeServer;
use constellation_csi::proto::csi::v1::{GetPluginInfoRequest, ProbeRequest};
use hyper_util::rt::TokioIo;
use std::time::Duration;
use tokio::net::{UnixListener, UnixStream};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Endpoint, Server, Uri};
use tower::service_fn;

#[tokio::test]
async fn get_plugin_info_and_probe_over_a_real_unix_socket() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("csi.sock");

    let listener = UnixListener::bind(&path).unwrap();
    let incoming = UnixListenerStream::new(listener);
    let serving = tokio::spawn(
        Server::builder()
            .add_service(IdentityServer::new(IdentityService::node()))
            .add_service(NodeServer::new(NodeService::new(
                "test-node".to_string(),
                None,
            )))
            .serve_with_incoming(incoming),
    );

    let connect_path = path.clone();
    let channel = Endpoint::try_from("http://[::]:50051")
        .unwrap()
        .connect_with_connector(service_fn(move |_: Uri| {
            let path = connect_path.clone();
            async move { UnixStream::connect(path).await.map(TokioIo::new) }
        }))
        .await
        .expect("connecting over the unix socket");
    let mut client = IdentityClient::new(channel);

    let info = client
        .get_plugin_info(GetPluginInfoRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.name, DRIVER_NAME);
    assert_eq!(info.vendor_version, env!("CARGO_PKG_VERSION"));

    // Node-mode Probe is always ready: local health only, no engine pod.
    let probe = client.probe(ProbeRequest {}).await.unwrap().into_inner();
    assert_eq!(probe.ready, Some(true));

    serving.abort();
    let _ = tokio::time::timeout(Duration::from_secs(1), serving).await;
}
