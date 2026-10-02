//! `constellation-csi`: entry point for both CSI binary roles. `--controller`
//! serves Identity+Controller; `--node` serves Identity+Node (plan 37 §4).

use anyhow::{bail, Context, Result};
use clap::Parser;
use constellation_csi::control_client::{Engines, InMemoryEngines};
use constellation_csi::controller::{ControllerConfig, ControllerService};
use constellation_csi::engine_pods::NodeEnginePods;
use constellation_csi::engine_pods::{EnginePodConfig, EnginePodManager};
use constellation_csi::identity::IdentityService;
use constellation_csi::node::state::StateStore;
use constellation_csi::node::{
    FakeMounter, InMemoryNodeEngines, LinuxMounter, Mounter, NodeEngines, NodeService,
};
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
    /// With `--node`: the node-local hostPath root of plan 37 §7's layout
    /// (`node-identity/<unit>/`, `sockets/<unit>/`), where this privileged
    /// plugin and the unprivileged engine pods it starts meet, and where
    /// the plugin keeps its staged-volume records (`volumes/`).
    #[arg(long, default_value = "/var/lib/constellation-csi")]
    host_root: PathBuf,
    /// Back the service with in-process fakes instead of engine pods (and,
    /// with `--node`, instead of real mounts: the directories are made, the
    /// mounts only recorded): volumes live only as long as this process.
    /// For `csi-sanity` (`tests/csi/sanity.sh`) and local testing only —
    /// never for a real cluster.
    #[arg(long)]
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
        } else if std::env::var_os("KUBERNETES_SERVICE_HOST").is_some() {
            // In a cluster: controller-owned engine pods (plan 37 §7).
            let config = EnginePodConfig::from_env().map_err(anyhow::Error::msg)?;
            let _ = rustls::crypto::ring::default_provider().install_default();
            let client = kube::Client::try_default()
                .await
                .context("connecting to the Kubernetes API")?;
            tracing::info!(
                namespace = %config.namespace,
                image = %config.image,
                "engine pods: controller-owned, reached by exec relay"
            );
            Some(Arc::new(EnginePodManager::new(client, config).await))
        } else {
            // Outside a cluster there is nowhere to start engine pods:
            // every volume RPC answers UNAVAILABLE.
            tracing::warn!(
                "not running in a Kubernetes cluster and no --in-memory-backend: volume RPCs \
                 are UNAVAILABLE"
            );
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
        let state = StateStore::open(&cli.host_root.join("volumes"))
            .with_context(|| format!("opening {}/volumes", cli.host_root.display()))?;
        let (engines, mounter): (Option<Arc<dyn NodeEngines>>, Arc<dyn Mounter>) =
            if cli.in_memory_backend {
                tracing::warn!(
                    "--in-memory-backend: engine pods are in-process fakes and mounts are only \
                     recorded"
                );
                (
                    Some(Arc::new(InMemoryNodeEngines::accepting_any_path(&node_id))),
                    Arc::new(FakeMounter::default()),
                )
            } else if std::env::var_os("KUBERNETES_SERVICE_HOST").is_some() {
                // In a cluster: this node's own engine pods (plan 37 §7).
                let mut config = EnginePodConfig::from_env().map_err(anyhow::Error::msg)?;
                config.host_root = cli.host_root.display().to_string();
                let _ = rustls::crypto::ring::default_provider().install_default();
                let client = kube::Client::try_default()
                    .await
                    .context("connecting to the Kubernetes API")?;
                tracing::info!(
                    namespace = %config.namespace,
                    image = %config.image,
                    host_root = %config.host_root,
                    "engine pods: node-owned, reached through their hostPath sockets"
                );
                (
                    Some(Arc::new(
                        NodeEnginePods::new(client, config, node_id.clone()).await,
                    )),
                    Arc::new(LinuxMounter),
                )
            } else {
                tracing::warn!(
                    "not running in a Kubernetes cluster and no --in-memory-backend: staging \
                     answers UNAVAILABLE"
                );
                (None, Arc::new(LinuxMounter))
            };
        let identity = IdentityServer::new(IdentityService::node());
        let node = NodeServer::new(NodeService::new(node_id, engines, mounter, state));
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
