//! `constellation-csi`: entry point for both CSI binary roles. `--controller`
//! serves Identity+Controller; `--node` serves Identity+Node (plan 37 §4).

use anyhow::{bail, Context, Result};
use clap::Parser;
use constellation_csi::control_client::{Engines, InMemoryEngines};
use constellation_csi::controller::{ControllerConfig, ControllerService};
use constellation_csi::identity::IdentityService;
use constellation_csi::node::NodeService;
use constellation_csi::proto::csi::v1::controller_server::ControllerServer;
use constellation_csi::proto::csi::v1::identity_server::IdentityServer;
use constellation_csi::proto::csi::v1::node_server::NodeServer;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

#[derive(Parser)]
#[command(
    name = "constellation-csi",
    version = env!("CARGO_PKG_VERSION"),
    about = "Constellation's Kubernetes CSI driver: Identity + Controller or Identity + Node over gRPC"
)]
struct Cli {
    /// Serve the Controller service (with Identity).
    #[arg(long, conflicts_with = "node")]
    controller: bool,
    /// Serve the Node service (with Identity).
    #[arg(long, conflicts_with = "controller")]
    node: bool,
    /// Where to listen: `unix:///path/to/csi.sock` (the CSI sidecars'
    /// `--csi-address`/`CSI_ENDPOINT` convention).
    #[arg(long, env = "CSI_ENDPOINT")]
    endpoint: String,
    /// This node's name (`NodeGetInfo.node_id`); required with `--node`.
    #[arg(long)]
    node_id: Option<String>,
    /// hostPath root for engine-pod control sockets — the fd-passing
    /// rendezvous between this (privileged) node plugin and the
    /// unprivileged engine pods it starts (plan 37 §"hostPath layout").
    /// Recorded for K3's `NodeStageVolume`; unused until then.
    #[arg(long)]
    #[allow(dead_code)]
    control_socket_root: Option<PathBuf>,
    /// Back the Controller service with an in-process, in-memory engine
    /// fake instead of engine pods: volumes live only as long as this
    /// process. For `csi-sanity` (`tests/csi/sanity.sh`) and local testing
    /// only — never for a real cluster. With `--controller` only.
    #[arg(long, requires = "controller")]
    in_memory_backend: bool,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    match (cli.controller, cli.node) {
        (false, false) => bail!("one of --controller or --node is required"),
        (true, false) if cli.node_id.is_some() => {
            bail!("--node-id only applies to --node")
        }
        (false, true) if cli.node_id.is_none() => bail!("--node requires --node-id"),
        _ => {}
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(cli))
}

async fn run(cli: Cli) -> Result<()> {
    let path = socket_path(&cli.endpoint)?;
    if path.exists() {
        std::fs::remove_file(&path)
            .with_context(|| format!("removing the stale socket {}", path.display()))?;
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let listener = UnixListener::bind(&path)
        .with_context(|| format!("binding the CSI endpoint {}", path.display()))?;
    let incoming = UnixListenerStream::new(listener);

    if cli.controller {
        tracing::info!(endpoint = %path.display(), "constellation-csi starting (controller)");
        let config = ControllerConfig::from_env().map_err(anyhow::Error::msg)?;
        let engines: Option<Arc<dyn Engines>> = if cli.in_memory_backend {
            tracing::warn!(
                "--in-memory-backend: volumes are in-process fakes and vanish with this process"
            );
            Some(Arc::new(InMemoryEngines::default()))
        } else {
            // Controller-owned engine pods are 37-k2b's: until then every
            // volume RPC answers UNAVAILABLE.
            None
        };
        let identity = IdentityServer::new(IdentityService::controller(None));
        let controller = ControllerServer::new(ControllerService::new(engines, config));
        Server::builder()
            .add_service(identity)
            .add_service(controller)
            .serve_with_incoming(incoming)
            .await?;
    } else {
        let node_id = cli.node_id.expect("checked in main()");
        tracing::info!(endpoint = %path.display(), node_id, "constellation-csi starting (node)");
        let identity = IdentityServer::new(IdentityService::node());
        let node = NodeServer::new(NodeService::new(node_id, None));
        Server::builder()
            .add_service(identity)
            .add_service(node)
            .serve_with_incoming(incoming)
            .await?;
    }
    Ok(())
}

/// `unix:///path/to/csi.sock` (or a bare path) -> the filesystem path.
fn socket_path(endpoint: &str) -> Result<PathBuf> {
    let path = endpoint.strip_prefix("unix://").unwrap_or(endpoint);
    if path.is_empty() {
        bail!("--endpoint must name a unix socket path, e.g. unix:///path/to/csi.sock");
    }
    Ok(PathBuf::from(path))
}
