//! `constellation serve`: a node with no FUSE mount of its own — plan 37's
//! engine pods (§4, §"Engine-pod lifecycle").
//!
//! An engine pod is unprivileged: it can never call `mount(2)`, so the
//! `mount` command's "become the daemon by mounting a first view" shape does
//! not fit it. `serve` starts the same node (`NodeRuntime`), binds the
//! control socket at an explicit path (a hostPath the node plugin reaches),
//! and serves the control API until a signal; views arrive later, if at
//! all, as `view.mount{PreopenedFd}` from the privileged node plugin (K3),
//! and the node keeps running when the last of them goes
//! ([`NodeConfig::persistent`](crate::node_runtime::NodeConfig)). The
//! controller-owned engine pod never gets a view: it exists only to answer
//! `fs.create`/`browse.*`/`quota.*`.
//!
//! `--create` makes the filesystem at `--s3` first when there is none (the
//! controller-owned pod of a pool nobody has provisioned from yet — its
//! daemon cannot start on a filesystem that does not exist, and the
//! controller's own `fs.create` needs a daemon to go through). It is the
//! `fs create` sequence without the registry: an engine pod names its
//! filesystem by URL and state dir, never by a registered name. A
//! filesystem already there is used as it is; the controller's `fs.create`
//! then compares its parameters with the class's and refuses a mismatch.
//!
//! No fork, no `daemon.lock` attach: a pod runs exactly one node per state
//! dir, and a second `serve` on the same one fails rather than attaching.
//! `daemon --upgrade` does not apply; an engine pod is replaced by
//! restarting it.
//!
//! `control-relay` (hidden) is the other half of how the CSI controller
//! reaches a controller-owned engine pod: an exec'd process that pipes its
//! stdin/stdout to the control socket, so the controller speaks the control
//! protocol over the Kubernetes exec stream (authorized by RBAC on
//! `pods/exec`) and the daemon sees an ordinary local peer — the pod's own
//! uid, its owner. `--ping` is the pod's readiness probe: one `node.ping`.

use anyhow::{bail, Context, Result};
use constellation_store_s3::{ChunkStore, CompressionSetting, FsMeta, StoreError};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{log_buffer, node_runtime, parallelism, startup};

/// `constellation serve`'s arguments.
pub struct ServeArgs {
    pub s3: String,
    pub state_dir: PathBuf,
    pub control_socket: PathBuf,
    pub create: bool,
    pub chunk_size: u32,
    pub compression: String,
    pub e2e: bool,
    pub cache_size: Option<u64>,
    pub write_mode: Option<String>,
}

pub fn cmd_serve(
    threads: parallelism::ThreadPlan,
    args: ServeArgs,
    log_buffer: log_buffer::LogBuffer,
) -> Result<()> {
    let ServeArgs {
        s3,
        state_dir,
        control_socket,
        create,
        chunk_size,
        compression,
        e2e,
        cache_size,
        write_mode,
    } = args;
    let initial_write_mode: crate::writeback::WriteMode = write_mode
        .as_deref()
        .unwrap_or("through")
        .parse()
        .map_err(anyhow::Error::msg)?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads.tokio)
        .max_blocking_threads(threads.blocking)
        .enable_all()
        .build()?;
    if create {
        startup::phase("creating the filesystem if missing");
        rt.block_on(create_if_missing(&s3, chunk_size, &compression, e2e))?;
    }
    if let Some(parent) = control_socket.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    startup::phase("taking daemon.lock");
    match crate::take_state_dir_lock(&state_dir)? {
        crate::LockOutcome::BecomeDaemon => {}
        crate::LockOutcome::Attach => bail!(
            "another daemon already serves {}; one `serve` per state dir",
            state_dir.display()
        ),
    }
    startup::phase("starting the node runtime");
    let node = node_runtime::NodeRuntime::start(
        node_runtime::NodeConfig {
            fs_id: constellation_engine::FsId::new(state_dir.display().to_string()),
            engine: constellation_engine::EngineConfig {
                state_dir: Some(state_dir.clone()),
                cache_size: cache_size.unwrap_or(1024 * 1024 * 1024),
                cto_strict: crate::cto::strict_from(None)?,
                locks: crate::locks::cluster_flag(None)?,
                initial_write_mode,
                atime_mode: crate::atime::AtimeMode::resolve(None),
                // An engine pod has no terminal: an E2E filesystem's
                // passphrase comes from the environment or not at all.
                passphrase: constellation_engine::PassphraseSource::Ask(Box::new(|| {
                    crate::passphrase("CONSTELLATION_PASSPHRASE", "")
                })),
                version: env!("CONSTELLATION_VERSION").to_string(),
                ..constellation_engine::EngineConfig::new(s3.clone())
            },
            web_ui: 0,
            log_buffer,
            resumed: None,
            // A headless node makes no mount of its own, but `view.mount`
            // with a path (control.rs) makes a plain one — CSI engine pods
            // included — and it follows the same transport policy as a
            // daemon's plain mount (plan 38 Z2c): `auto` unless the
            // environment or the profile says otherwise, with cluster-lock
            // mounts on /dev/fuse under `auto` anyway. `view.mount` on a
            // pre-opened descriptor is handover-capable and pinned to
            // /dev/fuse whatever this says.
            fuse_transport: constellation_frontend_fuse::TransportConfig::resolve(
                None,
                None,
                crate::node_runtime::profile_transport()?,
            )
            .map_err(anyhow::Error::msg)?,
            control_socket: Some(control_socket.clone()),
            persistent: true,
        },
        rt.handle().clone(),
    )?;
    startup::phase("starting the control API");
    if let Err(e) = node.serve_headless() {
        let _ = node.shutdown();
        return Err(e);
    }
    startup::done("serving (headless)");
    tracing::info!(
        socket = %control_socket.display(),
        fs = %node.engine().fsmeta().uuid,
        s3,
        "serving the control API with no FUSE mount"
    );
    node.wait_stopped();
    let failed = node.shutdown_error();
    drop(node);
    rt.shutdown_timeout(Duration::from_secs(10));
    match failed {
        Some(message) => bail!(message),
        None => Ok(()),
    }
}

/// `fs create`'s backend half (module docs): a no-op when `meta.json`
/// exists, a create otherwise. A concurrent creator winning the race is
/// success too — both then serve the one filesystem.
async fn create_if_missing(s3: &str, chunk_size: u32, compression: &str, e2e: bool) -> Result<()> {
    constellation_fs_core::validate_chunk_size(chunk_size)?;
    let setting: CompressionSetting = compression.parse().map_err(|e| anyhow::anyhow!("{e}"))?;
    let (backend, _) = crate::backend::open_backend_described(s3)
        .await
        .context("opening backend")?;
    let store = ChunkStore::new(backend);
    match store.load_fs_waiting(Duration::from_secs(2)).await {
        Ok(meta) => {
            tracing::info!(fs = %meta.uuid, s3, "filesystem exists; serving it");
            return Ok(());
        }
        Err(StoreError::NotFound) => {}
        Err(e) => return Err(e).context("reading meta.json"),
    }
    crate::preflight_backend(&store, s3).await?;
    let mut meta = FsMeta::new(chunk_size, &setting.to_string());
    meta.e2e = e2e;
    if e2e {
        meta.gossip_secret = None;
        let secret = crate::passphrase("CONSTELLATION_PASSPHRASE", "")
            .context("an e2e filesystem needs CONSTELLATION_PASSPHRASE")?;
        meta.keyring = Some(
            constellation_store_s3::create_keyring_block(&secret)
                .context("creating E2E keyring")?,
        );
    }
    match store.create_fs(&meta).await {
        Ok(()) => tracing::info!(fs = %meta.uuid, s3, "created the filesystem"),
        Err(StoreError::AlreadyExists) => tracing::info!(s3, "another creator won; serving theirs"),
        Err(e) => return Err(e).context("creating filesystem"),
    }
    Ok(())
}

/// `control-relay`: pipe stdin → `socket` and `socket` → stdout until
/// either side closes; with `ping`, one `node.ping` instead.
pub async fn control_relay(socket: &Path, ping: bool) -> Result<()> {
    if ping {
        let client = constellation_control::Client::connect_unix(socket).await?;
        client
            .call_bounded::<constellation_control::methods::NodePing>(
                Default::default(),
                Duration::from_secs(5),
            )
            .await?;
        return Ok(());
    }
    let stream = tokio::net::UnixStream::connect(socket)
        .await
        .with_context(|| format!("connecting to {}", socket.display()))?;
    let (mut from_daemon, mut to_daemon) = stream.into_split();
    let up = async move {
        let mut stdin = tokio::io::stdin();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = stdin.read(&mut buf).await?;
            if n == 0 {
                // The caller is done; let the daemon see EOF and finish.
                return to_daemon.shutdown().await;
            }
            to_daemon.write_all(&buf[..n]).await?;
        }
    };
    let down = async move {
        let mut stdout = tokio::io::stdout();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = from_daemon.read(&mut buf).await?;
            if n == 0 {
                return std::io::Result::Ok(());
            }
            // Frames must reach the caller as they come, not when a
            // buffer fills: the protocol is request/response.
            stdout.write_all(&buf[..n]).await?;
            stdout.flush().await?;
        }
    };
    tokio::pin!(down);
    tokio::select! {
        result = &mut down => result?,
        result = up => {
            result?;
            down.await?;
        }
    }
    Ok(())
}
