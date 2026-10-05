//! `constellation-csi`: entry point for both CSI binary roles. `--controller`
//! serves Identity+Controller; `--node` serves Identity+Node (plan 37 §4).

use anyhow::{bail, Context, Result};
use clap::Parser;
use constellation_csi::control_client::{Engines, InMemoryEngines};
use constellation_csi::controller::{ControllerConfig, ControllerService};
use constellation_csi::credentials::{KubeSecrets, Refresher};
use constellation_csi::engine_pods::NodeEnginePods;
use constellation_csi::engine_pods::{EnginePodConfig, EnginePodManager};
use constellation_csi::identity::IdentityService;
use constellation_csi::node::gc::{self, GcConfig};
use constellation_csi::node::handoff::{HandoffConfig, HandoffMetrics};
use constellation_csi::node::state::StateStore;
use constellation_csi::node::{
    rollout, FakeMounter, InMemoryNodeEngines, LinuxMounter, Mounter, NodeEngines, NodeService,
};
use constellation_csi::proto::csi::v1::controller_server::ControllerServer;
use constellation_csi::proto::csi::v1::identity_server::IdentityServer;
use constellation_csi::proto::csi::v1::node_server::NodeServer;
use constellation_csi::purge::{Leader, LeaderTiming, PurgeConfig, PurgeWorker};
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
    /// With `--node`: the node plugin's `preStop` hook (plan 37 §7
    /// "Drain"). On a draining node, wait until the running plugin has
    /// collected every engine pod of the node (each leaves the pool's
    /// registry first), at most `CONSTELLATION_CSI_PRESTOP_TIMEOUT_S`
    /// (default 300); on any other node exit at once. Serves nothing.
    #[arg(long, requires = "node")]
    pre_stop: bool,
    /// Ask the Node service at `--endpoint` for one staged volume's health
    /// (`NodeGetVolumeHealth`), print it as JSON and exit. How an operator,
    /// or `harness k8s-scenario`, reads plan 37 §11's volume condition — a
    /// volume removed or its record changed outside Kubernetes (settled
    /// decision 18) — where kubelet does not surface it: `kubectl exec` it
    /// in the node plugin's container. Serves nothing.
    #[arg(long, value_name = "VOLUME_ID", conflicts_with_all = ["controller", "node"])]
    volume_health: Option<String>,
    /// With `--volume-health`: a publish path to check as well.
    #[arg(long, value_name = "PATH", requires = "volume_health")]
    volume_path: Option<String>,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    // Both plugins hold credentials in memory (the Secrets they pass to
    // engine pods with `fs.unlock`): never in a core dump.
    constellation_platform::forbid_core_dumps().context("disabling core dumps")?;
    match (cli.controller, cli.node) {
        (false, false) if cli.volume_health.is_some() => {}
        (false, false) => bail!("one of --controller or --node is required"),
        (true, false) if cli.node_id.is_some() => {
            bail!("--node-id only applies to --node")
        }
        (false, true) if cli.node_id.is_none() && !cli.pre_stop => {
            bail!("--node requires --node-id")
        }
        _ => {}
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(cli))
}

async fn run(cli: Cli) -> Result<()> {
    if cli.pre_stop {
        return pre_stop(cli).await;
    }
    if let Some(volume_id) = &cli.volume_health {
        return volume_health(&cli.endpoint, volume_id, cli.volume_path.as_deref()).await;
    }
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
        let mut config = ControllerConfig::from_env().map_err(anyhow::Error::msg)?;
        // The purge worker runs only against engine pods (in a cluster).
        let purge = PurgeConfig::from_env().map_err(anyhow::Error::msg)?;
        let purge = (!cli.in_memory_backend
            && std::env::var_os("KUBERNETES_SERVICE_HOST").is_some())
        .then_some(purge);
        config.purge_worker = purge.as_ref().is_some_and(|p| p.interval.is_some());
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
            // `credentialSource: refreshing` classes' Secrets, watched.
            let refresher = Refresher::new(Arc::new(KubeSecrets::new(client.clone())));
            let namespace = config.namespace.clone();
            let manager = EnginePodManager::new(client.clone(), config, refresher).await;
            // Plan 37 §"Deletion and purge": the purge worker, in the
            // replica holding the purge lease.
            if let Some(purge) = purge.clone().filter(|p| p.interval.is_some()) {
                let identity = std::env::var("POD_NAME")
                    .ok()
                    .filter(|n| !n.is_empty())
                    .unwrap_or_else(|| format!("constellation-csi-{}", std::process::id()));
                let timing = LeaderTiming::from_env().map_err(anyhow::Error::msg)?;
                tracing::info!(?purge, ?timing, identity, "purge worker on (when leading)");
                let leader = Leader::spawn(client, &namespace, identity, timing);
                PurgeWorker::new(manager.clone(), purge).spawn(leader.flag());
            } else {
                tracing::info!("purge worker off (CONSTELLATION_CSI_PURGE_INTERVAL=0)");
            }
            Some(manager)
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
            .serve_with_incoming_shutdown(incoming, stop_signal())
            .await?;
    } else {
        let node_id = cli.node_id.expect("checked in main()");
        tracing::info!(endpoint = %path.display(), node_id, "constellation-csi starting (node)");
        let state = StateStore::open(&cli.host_root.join("volumes"))
            .with_context(|| format!("opening {}/volumes", cli.host_root.display()))?;
        let mut refresher = None;
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
                refresher = Some(Refresher::new(Arc::new(KubeSecrets::new(client.clone()))));
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
        let rollouts = engines.is_some() && !cli.in_memory_backend;
        let handoff = HandoffConfig::from_env().map_err(anyhow::Error::msg)?;
        let mut service = NodeService::new(node_id, engines, mounter, state).with_handoff(handoff);
        if let Some(refresher) = refresher {
            service = service.with_refresher(refresher);
        }
        let service = Arc::new(service);
        // Plan 37 §8: roll engine pods whose spec drifted, by handover.
        match rollout::interval_from_env().map_err(anyhow::Error::msg)? {
            Some(interval) if rollouts => {
                tracing::info!(
                    ?interval,
                    ?handoff,
                    "engine-pod rollouts by session handoff"
                );
                NodeService::spawn_rollout(service.clone(), interval);
            }
            _ => tracing::info!("engine-pod rollouts are off"),
        }
        // Plan 37 §7: idle engine pods and a draining node's engine pods
        // leave the registry and go.
        if rollouts {
            let gc = GcConfig::from_env().map_err(anyhow::Error::msg)?;
            tracing::info!(?gc, "engine-pod idle GC and drain");
            NodeService::spawn_gc(service.clone(), gc);
        }
        if let Some(addr) = std::env::var("CONSTELLATION_CSI_METRICS_ADDR")
            .ok()
            .filter(|a| !a.trim().is_empty())
        {
            serve_metrics(&addr, service.handoff_metrics()).await?;
        }
        let identity = IdentityServer::new(IdentityService::node());
        let node = NodeServer::from_arc(service);
        Server::builder()
            .add_service(identity)
            .add_service(node)
            .serve_with_incoming_shutdown(incoming, stop_signal())
            .await?;
    }
    Ok(())
}

/// `SIGTERM` or `SIGINT`. Each plugin is its container's PID 1, for which
/// the kernel ignores any signal it installs no handler for: without this a
/// pod deletion waits out the whole grace period (the node plugin's covers
/// its `preStop` drain, minutes) before kubelet's `SIGKILL`. The mounts and
/// the engine pods do not depend on this process, so stopping at once is
/// safe (plan 37 §4).
async fn stop_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let (Ok(mut term), Ok(mut int)) = (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) else {
        tracing::warn!("cannot install the SIGTERM/SIGINT handlers");
        return std::future::pending().await;
    };
    let name = tokio::select! {
        _ = term.recv() => "SIGTERM",
        _ = int.recv() => "SIGINT",
    };
    tracing::info!(signal = name, "stopping");
}

/// `--pre-stop` (see the flag): the node plugin's `preStop` hook.
async fn pre_stop(cli: Cli) -> Result<()> {
    // A `preStop` exec command is not expanded like the container's
    // arguments: the node name comes from the environment there.
    let node_id = cli
        .node_id
        .clone()
        .filter(|n| !n.starts_with("$("))
        .or_else(|| std::env::var("NODE_NAME").ok().filter(|n| !n.is_empty()))
        .context("--pre-stop needs --node-id or $NODE_NAME")?;
    if std::env::var_os("KUBERNETES_SERVICE_HOST").is_none() {
        tracing::info!("preStop: not in a cluster; nothing to wait for");
        return Ok(());
    }
    let timeout = match std::env::var("CONSTELLATION_CSI_PRESTOP_TIMEOUT_S") {
        Ok(v) if !v.trim().is_empty() => std::time::Duration::from_secs(
            v.trim()
                .parse()
                .context("CONSTELLATION_CSI_PRESTOP_TIMEOUT_S must be seconds")?,
        ),
        _ => std::time::Duration::from_secs(300),
    };
    let mut config = EnginePodConfig::from_env().map_err(anyhow::Error::msg)?;
    config.host_root = cli.host_root.display().to_string();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = kube::Client::try_default()
        .await
        .context("connecting to the Kubernetes API")?;
    let engines = NodeEnginePods::new(client, config, node_id).await;
    gc::pre_stop(&engines, timeout)
        .await
        .map_err(anyhow::Error::msg)
}

/// `--volume-health` (see the flag): one `NodeGetVolumeHealth` call to the
/// Node service at `endpoint`, its answer on stdout as JSON
/// (`{"volume_id", "abnormal", "statuses": [{"status", "reason",
/// "message"}]}`; `status` is the CSI `VolumeHealthErrorType` name).
async fn volume_health(endpoint: &str, volume_id: &str, path: Option<&str>) -> Result<()> {
    use constellation_csi::proto::csi::v1::node_client::NodeClient;
    use constellation_csi::proto::csi::v1::{NodeGetVolumeHealthRequest, VolumeHealthErrorType};
    let socket = socket_path(endpoint)?;
    // The URI is a placeholder: the connector dials the socket.
    let channel = tonic::transport::Endpoint::try_from("http://[::]:50051")?
        .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
            let socket = socket.clone();
            async move {
                tokio::net::UnixStream::connect(socket)
                    .await
                    .map(hyper_util::rt::TokioIo::new)
            }
        }))
        .await
        .with_context(|| format!("connecting to {endpoint}"))?;
    let health = NodeClient::new(channel)
        .node_get_volume_health(NodeGetVolumeHealthRequest {
            volume_id: volume_id.to_string(),
            volume_publish_path: path.unwrap_or_default().to_string(),
            ..Default::default()
        })
        .await
        .map_err(|s| anyhow::anyhow!("NodeGetVolumeHealth: {:?}: {}", s.code(), s.message()))?
        .into_inner()
        .volume_health
        .unwrap_or_default();
    let statuses: Vec<serde_json::Value> = health
        .health_statuses
        .iter()
        .map(|e| {
            let status = VolumeHealthErrorType::try_from(e.status)
                .map(|t| t.as_str_name().to_string())
                .unwrap_or_else(|_| e.status.to_string());
            serde_json::json!({"status": status, "reason": e.reason, "message": e.message})
        })
        .collect();
    println!(
        "{}",
        serde_json::json!({
            "volume_id": volume_id,
            "abnormal": !statuses.is_empty(),
            "statuses": statuses,
        })
    );
    Ok(())
}

/// `CONSTELLATION_CSI_METRICS_ADDR` (e.g. `0.0.0.0:9810`): the node
/// plugin's handoff counters (plan 37 §10) as Prometheus text, at
/// `GET /metrics` (anything else: 404, or 405 for another method).
/// Minimal by design: one request, one answer, the connection closed.
async fn serve_metrics(addr: &str, metrics: Arc<HandoffMetrics>) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding the metrics endpoint {addr}"))?;
    tracing::info!(addr, "serving metrics");
    tokio::spawn(async move {
        loop {
            let Ok((mut conn, _)) = listener.accept().await else {
                continue;
            };
            let metrics = metrics.clone();
            tokio::spawn(async move {
                let mut request = [0u8; 1024];
                let n = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    conn.read(&mut request),
                )
                .await
                .ok()
                .and_then(Result::ok)
                .unwrap_or(0);
                let (status, kind, body) = metrics_answer(&request[..n], &metrics);
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: {kind}\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = conn.write_all(response.as_bytes()).await;
            });
        }
    });
    Ok(())
}

/// The metrics endpoint's answer to a request's first bytes: status line,
/// content type, body.
fn metrics_answer(
    request: &[u8],
    metrics: &HandoffMetrics,
) -> (&'static str, &'static str, String) {
    let line = request.split(|b| *b == b'\n').next().unwrap_or_default();
    let line = String::from_utf8_lossy(line);
    let mut parts = line.split_whitespace();
    let (method, path) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    let path = path.split('?').next().unwrap_or("");
    match (method, path) {
        ("GET", "/metrics") => ("200 OK", "text/plain; version=0.0.4", metrics.render()),
        (_, "/metrics") => ("405 Method Not Allowed", "text/plain", "GET only\n".into()),
        _ => (
            "404 Not Found",
            "text/plain",
            "metrics are at /metrics\n".into(),
        ),
    }
}

/// `unix:///path/to/csi.sock` (or a bare path) -> the filesystem path.
fn socket_path(endpoint: &str) -> Result<PathBuf> {
    let path = endpoint.strip_prefix("unix://").unwrap_or(endpoint);
    if path.is_empty() {
        bail!("--endpoint must name a unix socket path, e.g. unix:///path/to/csi.sock");
    }
    Ok(PathBuf::from(path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_answer_get_metrics_only() {
        let m = HandoffMetrics::default();
        let (status, _, body) = metrics_answer(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", &m);
        assert_eq!(status, "200 OK");
        assert!(body.contains("constellation_csi_handoff_total"));
        assert_eq!(
            metrics_answer(b"GET /metrics?x=1 HTTP/1.1\r\n", &m).0,
            "200 OK"
        );
        assert_eq!(
            metrics_answer(b"POST /metrics HTTP/1.1\r\n", &m).0,
            "405 Method Not Allowed"
        );
        assert_eq!(metrics_answer(b"GET / HTTP/1.1\r\n", &m).0, "404 Not Found");
        assert_eq!(metrics_answer(b"", &m).0, "404 Not Found");
    }
}
