//! The daemon's end of the control protocol (plan 31 C5), and the CLI's.
//!
//! **Serving.** The engine implements the method table
//! ([`constellation_engine::control`]); what only this host can do — its
//! FUSE mounts, the in-place upgrade, detaching every view after a
//! `leave` — is [`DaemonHost`]. [`daemon_router`] puts both behind one
//! [`Router`] with the daemon's policy (its owner is admin; anyone else only
//! what `control-allow.toml` grants, plan 31 §9.5) and an audit log of
//! mutating calls in the state dir (`control-audit.jsonl`). The router is
//! served on the per-user runtime socket (`transport::socket_path_for_state_dir`)
//! and, when `--web-ui` asks, by the localhost HTTP adapter.
//!
//! **Finding a daemon.** The socket's path is recorded in the state dir
//! (`control.path`, see `constellation_control::transport` path docs); every
//! command that talks to "the daemon of this state dir" reads it through
//! [`connect`]/[`ping`], and there is no `control.sock` in the state dir.

use crate::node_runtime::{MountId, NodeRuntime, ViewConfig};
use anyhow::{bail, Result};
use constellation_control::fd::OwnedFd;
use constellation_control::methods::Method;
use constellation_control::proto::types::{
    HandedOffView, HandoffParams, HandoffReport, HandoffTarget, HandoverStatus, MountSource,
    ViewInfo, ViewMountParams,
};
use constellation_control::proto::{ControlError, ErrorKind};
use constellation_control::transport::locate_socket;
use constellation_control::{Client, ClientOptions, Policy, Router};
use constellation_engine::control::{ControlHost, EngineControl, HostView};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::Duration;

/// The allowlist of other users' roles (plan 31 §9.5):
/// `$CONSTELLATION_CONTROL_POLICY`, else `<config dir>/control-allow.toml`.
/// Absent: only the daemon's owner.
pub fn policy_path() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os("CONSTELLATION_CONTROL_POLICY") {
        return Some(PathBuf::from(p));
    }
    constellation_platform::native()
        .dirs
        .config_dir()
        .ok()
        .map(|d| d.join("control-allow.toml"))
}

/// The audit log of mutating control calls.
pub const AUDIT_FILE: &str = "control-audit.jsonl";

/// The daemon's router: every method of the engine's service, the owner +
/// allowlist policy, the state dir's audit log.
pub fn daemon_router(service: &Arc<EngineControl>, state_dir: &Path) -> Router {
    let (owner, _) = constellation_platform::native().process.effective_ids();
    let policy = match policy_path() {
        Some(path) => Policy::load(&path, Some(owner)).unwrap_or_else(|e| {
            // A broken authorization file must be loud, and must not widen
            // anything: the owner keeps admin, nobody else gets in.
            tracing::error!(path = %path.display(), error = %e,
                "the control allowlist is unreadable; only the daemon's owner may connect");
            Policy::owner_only(owner)
        }),
        None => Policy::owner_only(owner),
    };
    let mut router = constellation_engine::control::router(service).with_policy(policy);
    match constellation_control::FileAuditSink::open(&state_dir.join(AUDIT_FILE)) {
        Ok(sink) => router = router.with_audit(Arc::new(sink)),
        Err(e) => tracing::warn!(error = %e, "control audit log unavailable"),
    }
    router
}

/// [`ControlHost`] over this daemon's [`NodeRuntime`].
pub struct DaemonHost {
    pub(crate) node: Weak<NodeRuntime>,
}

impl DaemonHost {
    fn node(&self) -> Result<Arc<NodeRuntime>, ControlError> {
        self.node
            .upgrade()
            .ok_or_else(|| ControlError::unavailable("the daemon is shutting down"))
    }
}

fn failed(e: anyhow::Error) -> ControlError {
    ControlError::failed(format!("{e:#}"))
}

impl ControlHost for DaemonHost {
    fn views(&self) -> Vec<HostView> {
        let Some(node) = self.node.upgrade() else {
            return Vec::new();
        };
        node.mounts()
            .into_iter()
            .map(|m| HostView {
                id: m.id.as_u64(),
                subtree: m.subtree,
                mountpoint: m.mountpoint,
                since: m.since,
                labels: m.view.labels().clone(),
                qos: constellation_control::proto::types::ViewQos {
                    max_inflight_ops: m.qos.max_inflight_ops,
                    max_staging_bytes: m.qos.max_staging_bytes,
                },
                confine_links: m.confine_links,
                view: Some(m.view),
            })
            .collect()
    }

    fn mount(&self, p: &ViewMountParams, fd: Option<OwnedFd>) -> Result<ViewInfo, ControlError> {
        let node = self.node()?;
        let (mountpoint, opts) = match &p.source {
            MountSource::Path { mountpoint, opts } => (Some(mountpoint.clone()), opts.clone()),
            MountSource::PreopenedFd => (None, Default::default()),
        };
        // `fuse_threads` arrives straight from an API caller and flows into
        // fuser's `n_threads`, one OS thread each: an unbounded value is a
        // thread-spawn DoS. Reject anything outside a sane band rather than
        // clamp, so the caller learns the request was wrong instead of
        // silently getting a different mount. The CLI-driven mount path
        // derives its count from `thread_plan()` (already capped at
        // `FUSE_THREAD_HARD_MAX`), so only this API path needs the guard.
        let fuse_threads = match opts.fuse_threads {
            Some(n) if !(1..=crate::parallelism::FUSE_THREAD_HARD_MAX).contains(&n) => {
                return Err(ControlError::invalid(format!(
                    "fuse_threads must be between 1 and {}, got {n}",
                    crate::parallelism::FUSE_THREAD_HARD_MAX
                )));
            }
            Some(n) => n,
            None => crate::parallelism::thread_plan().fuse,
        };
        let config = ViewConfig {
            inner_path: p.subtree.clone(),
            mountpoint: mountpoint.clone().unwrap_or_default(),
            allow_other: opts.allow_other,
            fs_name: opts
                .fs_name
                .clone()
                .unwrap_or_else(|| "constellation".to_string()),
            fuse_threads,
            rw_snapshot: opts.rw,
            clone_name: opts.clone_name.clone(),
            ephemeral: opts.ephemeral,
            confine_links: p.confine_links,
            labels: p.labels.clone(),
            qos: constellation_engine::ViewQos {
                max_inflight_ops: p.qos.max_inflight_ops,
                max_staging_bytes: p.qos.max_staging_bytes,
            },
        };
        let id = match (mountpoint, fd) {
            (Some(_), _) => node.add_mount(config),
            (None, Some(fd)) => node.add_mount_fd(config, fd),
            (None, None) => {
                return Err(ControlError::invalid(
                    "view.mount with PreopenedFd needs a file descriptor attached",
                ))
            }
        }
        .map_err(failed)?;
        let view = node
            .mounts()
            .into_iter()
            .find(|m| m.id == id)
            .ok_or_else(|| ControlError::failed("the view ended as soon as it was mounted"))?;
        Ok(ViewInfo {
            id: id.as_u64(),
            subtree: view.subtree,
            mountpoint: view.mountpoint.display().to_string(),
            mounted_ms_ago: view.since.elapsed().as_millis() as u64,
            labels: view.view.labels().clone(),
            qos: p.qos.clone(),
            confine_links: view.confine_links,
        })
    }

    fn unmount(&self, mountpoint: &Path) -> Result<String, ControlError> {
        let node = self.node()?;
        let id: MountId = node
            .mounts()
            .into_iter()
            .find(|m| m.mountpoint == mountpoint)
            .map(|m| m.id)
            .ok_or_else(|| {
                ControlError::not_found(format!("no view mounted at {}", mountpoint.display()))
            })?;
        // `remove_mount` unmounts and then joins the session's OS thread,
        // which can take a while (draining in-flight requests): the router
        // runs this on a blocking thread, never a runtime worker.
        node.remove_mount(id).map_err(failed)?;
        Ok(format!("unmounted {}", mountpoint.display()))
    }

    fn detach_all(&self) {
        let Some(node) = self.node.upgrade() else {
            return;
        };
        for id in node.mounts().into_iter().map(|m| m.id) {
            if let Err(e) = node.remove_mount(id) {
                tracing::warn!(error = %e, "leave: detaching a view failed");
            }
        }
    }

    fn handover_status(&self) -> HandoverStatus {
        match self.node.upgrade() {
            Some(node) => node.handover.status(),
            None => HandoverStatus::default(),
        }
    }

    fn handoff(
        &self,
        p: &HandoffParams,
        _fd: Option<OwnedFd>,
    ) -> Result<HandoffReport, ControlError> {
        let binary = match &p.target {
            HandoffTarget::Exec { binary } => binary.clone(),
            HandoffTarget::Socket => {
                return Err(ControlError::unsupported(
                    "handing sessions to another process over a socket is plan 37's; \
                     this daemon hands over in place (`exec`)",
                )
                .with_remediation("use target Exec (`constellation daemon --upgrade`)"))
            }
        };
        if !p.views.is_empty() {
            return Err(ControlError::invalid(
                "an in-place upgrade hands every view over; `views` must be empty",
            ));
        }
        if p.drain_timeout_ms.is_some() {
            return Err(ControlError::invalid(
                "an in-place upgrade drains with its own bound; omit drain_timeout_ms",
            ));
        }
        let node = self.node()?;
        let views: Vec<HandedOffView> = node
            .mounts()
            .into_iter()
            .map(|m| HandedOffView {
                id: m.id.as_u64(),
                mountpoint: m.mountpoint.display().to_string(),
                handles: 0,
            })
            .collect();
        let detail =
            crate::handover::upgrade(&node, binary.as_deref()).map_err(ControlError::failed)?;
        Ok(HandoffReport { detail, views })
    }
}

// ---------------------------------------------------------------------------
// The CLI's side
// ---------------------------------------------------------------------------

/// A connection to the daemon serving `state_dir`.
pub async fn connect(state_dir: &Path) -> Result<Client> {
    let Some(sock) = locate_socket(state_dir) else {
        bail!(
            "no daemon is serving {} (is the mount running?)",
            state_dir.display()
        );
    };
    Client::connect_unix_with(
        &sock,
        ClientOptions::default().named("constellation", env!("CONSTELLATION_VERSION")),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{} (is the mount running?)", e.message))
}

/// Whether `e` means "nobody is listening" (as opposed to a daemon that
/// answered, or one that accepted and went mute).
pub fn is_unreachable(e: &ControlError) -> bool {
    e.kind == ErrorKind::Unavailable && e.message.starts_with("connecting to ")
}

/// Is the daemon of `state_dir` alive *and answering*? A bare `connect`
/// succeeding proves only that a listener exists (a wedged process keeps
/// its listener); a `node.ping` answered within `within` proves the
/// daemon's runtime is serving. `Ok(false)` when there is no listener at
/// all (no recorded socket, or a stale one nobody listens on).
pub async fn ping(state_dir: &Path, within: Duration) -> Result<bool> {
    let Some(sock) = locate_socket(state_dir) else {
        return Ok(false);
    };
    let attempt = async {
        let client = Client::connect_unix(&sock).await?;
        client
            .call::<constellation_control::methods::NodePing>(Default::default())
            .await
    };
    match tokio::time::timeout(within, attempt).await {
        Ok(Ok(_)) => Ok(true),
        Ok(Err(e)) if is_unreachable(&e) => Ok(false),
        Ok(Err(e)) => Err(anyhow::anyhow!("{e}")),
        Err(_) => bail!(
            "the daemon at {} accepted the connection but did not answer a ping within {within:?}",
            sock.display()
        ),
    }
}

/// Call `M` on the daemon of `state_dir`, unbounded (some calls — prune,
/// GC, fsck, leave — are answered only when the work is done). A refusal
/// becomes an error whose text is the daemon's message.
pub async fn call<M: Method>(state_dir: &Path, params: M::Params) -> Result<M::Result> {
    let client = connect(state_dir).await?;
    client
        .call::<M>(params)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e.message))
}

/// [`call`] with a deadline on the whole exchange (connect, handshake,
/// answer): a daemon whose listener is open but that never answers (the
/// campaign 6 B-1 shape) must not park the caller forever.
pub async fn call_bounded<M: Method>(
    state_dir: &Path,
    params: M::Params,
    within: Duration,
) -> Result<M::Result> {
    match tokio::time::timeout(within, call::<M>(state_dir, params)).await {
        Ok(result) => result,
        Err(_) => bail!(
            "the daemon of {} did not answer within {within:?}",
            state_dir.display()
        ),
    }
}

/// Like [`call`], keeping the daemon's structured refusal apart from
/// failing to reach it at all (the outer error): the attach flow and
/// `export` act on which one it was. `within` bounds the whole exchange.
pub async fn try_call<M: Method>(
    state_dir: &Path,
    params: M::Params,
    within: Option<Duration>,
) -> Result<std::result::Result<M::Result, ControlError>> {
    let attempt = async {
        let client = connect(state_dir).await?;
        Ok::<_, anyhow::Error>(client.call::<M>(params).await)
    };
    let Some(within) = within else {
        return attempt.await;
    };
    match tokio::time::timeout(within, attempt).await {
        Ok(result) => result,
        Err(_) => bail!(
            "the daemon of {} did not answer within {within:?}",
            state_dir.display()
        ),
    }
}

/// Whether the daemon of `state_dir` still has its socket (it removes it,
/// and the locator, only at the very end of a clean shutdown).
pub fn socket_exists(state_dir: &Path) -> bool {
    locate_socket(state_dir).is_some_and(|sock| sock.exists())
}
