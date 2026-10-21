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
    self as api, HandedOffView, HandoffParams, HandoffReport, HandoffTarget, HandoverStatus,
    MountSource, ViewInfo, ViewMountParams,
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
///
/// `bound_socket` is this daemon's own listening control socket,
/// canonicalised by the caller (it just bound it, so canonicalisation
/// cannot fail the way a config-file path can) — `None` when binding the
/// socket itself failed, in which case no `kind = "service"` grant (plan 33
/// U1) can ever match since nobody is listening anyway.
pub fn daemon_router(
    service: &Arc<EngineControl>,
    state_dir: &Path,
    bound_socket: Option<PathBuf>,
) -> Router {
    with_daemon_policy(
        constellation_engine::control::router(service),
        state_dir,
        bound_socket,
    )
}

/// `router` under this daemon's allowlist and audit log — the ones
/// [`daemon_router`] uses, so `serve --await-unlock`'s credential gate
/// (which answers before the engine exists) admits and records exactly
/// who the daemon will.
pub fn with_daemon_policy(
    router: Router,
    state_dir: &Path,
    bound_socket: Option<PathBuf>,
) -> Router {
    let (owner, _) = constellation_platform::native().process.effective_ids();
    let policy = load_policy(policy_path().as_deref(), owner, bound_socket);
    let mut router = router.with_policy(policy);
    match constellation_control::FileAuditSink::open(&state_dir.join(AUDIT_FILE)) {
        Ok(sink) => router = router.with_audit(Arc::new(sink)),
        Err(e) => tracing::warn!(error = %e, "control audit log unavailable"),
    }
    router
}

/// The policy half of [`daemon_router`], separated so a test can drive the
/// allowlist file and the bound socket without an engine: load `path`, fall
/// back to owner-only on any error, and tell the result which socket this
/// daemon actually bound (plan 33 U1).
pub(crate) fn load_policy(
    path: Option<&Path>,
    owner: u32,
    bound_socket: Option<PathBuf>,
) -> Policy {
    let mut policy = match path {
        Some(path) => Policy::load(path, Some(owner)).unwrap_or_else(|e| {
            // A broken authorization file must be loud, and must not widen
            // anything: the owner keeps admin, nobody else gets in. There
            // is no reload yet, so this is also what a restart with a
            // broken file gives — not the previously loaded grants.
            tracing::error!(path = %path.display(), error = %e,
                "the control allowlist is unreadable; only the daemon's owner may connect");
            Policy::owner_only(owner)
        }),
        None => Policy::owner_only(owner),
    };
    if let Some(bound_socket) = bound_socket {
        policy = policy.with_bound_socket(bound_socket);
    }
    policy
}

/// How long `view.unmount` waits for the view to end before it answers
/// that the view, detached, is still in use.
const VIEW_UNMOUNT_WAIT: Duration = Duration::from_secs(5);

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

/// A FUSE session's transport state in the protocol's shape (plan 38
/// §5), its passthrough section included (Z3b: the session knows what the
/// kernel agreed and how many handles it serves from a backing file). The
/// engine fills in the mount's id and mountpoint.
fn fuse_mount_status(stats: &constellation_frontend_fuse::SessionStats) -> api::FuseMountStatus {
    let p = stats.passthrough();
    api::FuseMountStatus {
        passthrough: api::FusePassthroughStatus {
            enabled: p.enabled,
            opens: p.opens,
            unavailable_reason: p.unavailable_reason,
            refused_opens: p.refused_opens,
            opens_total: p.opens_total,
        },
        transport: stats.transport().name().to_string(),
        uring_queue_depth: stats.uring_queue_depth(),
        zero_copy_reads: stats.zero_copy_reads(),
        lock_wait_downgrades: stats.lock_wait_downgrades(),
        ring_stranded_commits: stats.ring_stranded_commits(),
        ring_entries_held_long: stats.ring_entries_held_long(),
        kernel_refs: stats.kernel_refs(),
        forgets_final: stats.forgets_final(),
        forgets_partial: stats.forgets_partial(),
        forgets_unknown: stats.forgets_unknown(),
        forgets_underflow: stats.forgets_underflow(),
        kernel_cache: kernel_cache_status(&stats.kernel_cache()),
        kernel_workarounds: kernel_workarounds_status(&stats.kernel_workarounds()),
        last_fallback: stats.last_fallback().map(|f| api::FuseFallback {
            from: f.from.name().to_string(),
            to: f.to.name().to_string(),
            reason: f.reason.as_str().to_string(),
            detail: f.detail.clone(),
            at_unix_ms: f.at_unix_ms,
        }),
        ..Default::default()
    }
}

/// Plan 42 §5.7: a session's kernel-cache counters, in the protocol's
/// shape.
fn kernel_cache_status(
    c: &constellation_frontend_fuse::PageCacheCounts,
) -> api::FuseKernelCacheStatus {
    let by = |v: &[(&'static str, u64)]| v.iter().map(|(k, n)| (k.to_string(), *n)).collect();
    api::FuseKernelCacheStatus {
        keep_cache_opens: c.keep_cache_opens,
        dropping_opens: by(&c.dropping_opens),
        suspect_marks: by(&c.suspect_marks),
        repairs: c.repairs,
        repair_pages: c.repair_pages,
        repairs_late: c.repairs_late,
        repairs_released: c.repairs_released,
        repair_wait_ms: c.repair_wait_ms,
        repair_wait_max_ms: c.repair_wait_max_ms,
        ambiguous_writes: c.ambiguous_writes,
        concurrent_writes: c.concurrent_writes,
        dir_attr_notices: c.dir_attr_notices,
    }
}

/// Plan 42 §5.4: a session's workaround settings and the kernel's
/// verdicts, in the protocol's shape.
fn kernel_workarounds_status(
    w: &constellation_frontend_fuse::pagecache::KernelWorkaroundsStatus,
) -> api::FuseKernelWorkarounds {
    api::FuseKernelWorkarounds {
        keep_cache: w.keep_cache.to_string(),
        write_repair: w.write_repair.to_string(),
        write_repair_on: w.write_repair_on,
        dir_attr_ttl: w.dir_attr_ttl.to_string(),
        dir_attr_ttl_zero: w.dir_attr_ttl_zero,
        dir_change_notify: w.dir_change_notify.to_string(),
        dir_change_notify_on: w.dir_change_notify_on,
        verdicts: w
            .verdicts
            .iter()
            .map(|(fix, verdict, source)| {
                (
                    fix.to_string(),
                    api::FuseKernelVerdict {
                        verdict: verdict.to_string(),
                        source: source.to_string(),
                    },
                )
            })
            .collect(),
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
                fuse: Some(fuse_mount_status(&m.fuse)),
                view: Some(m.view),
            })
            .collect()
    }

    fn fuse_counters(&self) -> api::FuseStatus {
        api::FuseStatus {
            mounts: Vec::new(),
            transport_fallbacks: constellation_frontend_fuse::stats::fallback_counts()
                .into_iter()
                .map(|(from, to, reason, count)| api::FuseFallbackCount {
                    from: from.into(),
                    to: to.into(),
                    reason: reason.into(),
                    count,
                })
                .collect(),
            zero_copy_reads_total: constellation_frontend_fuse::stats::zero_copy_reads_total(),
            lock_wait_downgrades_total:
                constellation_frontend_fuse::stats::lock_wait_downgrades_total(),
        }
    }

    fn mount(&self, p: &ViewMountParams, fd: Option<OwnedFd>) -> Result<ViewInfo, ControlError> {
        let node = self.node()?;
        let (mountpoint, opts, preopened_at) = match &p.source {
            MountSource::Path { mountpoint, opts } => {
                (Some(mountpoint.clone()), opts.clone(), None)
            }
            MountSource::PreopenedFd { mountpoint, opts } => {
                (None, opts.clone(), mountpoint.clone())
            }
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
            (None, Some(fd)) => node.add_mount_fd(config, fd, preopened_at),
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
        // `detach_view` unmounts and then waits for the session's thread
        // to have torn the view down, which can take a while (draining
        // in-flight requests): the router runs this on a blocking thread,
        // never a runtime worker. It does not wait for the node's own
        // drain when this was its last view: the caller watches that
        // (`constellation umount` reports its progress until the daemon
        // has exited).
        Ok(
            match node
                .detach_view(id, Some(VIEW_UNMOUNT_WAIT))
                .map_err(failed)?
            {
                Some(true) => format!(
                    "unmounted {}; that was the daemon's last view: it drains and exits",
                    mountpoint.display()
                ),
                Some(false) if node.is_persistent() && node.mounts().is_empty() => format!(
                    "unmounted {}; the daemon keeps running without views",
                    mountpoint.display()
                ),
                Some(false) => format!("unmounted {}", mountpoint.display()),
                // Still listed: a caller that needs it gone waits for that.
                None => format!(
                    "detached {}, still in use (files open on it or a working directory in \
                     it): the view ends when they are closed",
                    mountpoint.display()
                ),
            },
        )
    }

    /// A node already gone (its last view ended, its drain over, the
    /// process on its way out: the control server outlives the node by up
    /// to the runtime's shutdown grace) is exiting, whatever kind it was:
    /// `umount` must wait for that exit and go by its recorded outcome,
    /// not take the daemon for a persistent one that keeps serving
    /// without views and report success over an unshipped drain.
    fn exits_when_empty(&self) -> bool {
        self.node.upgrade().is_none_or(|node| !node.is_persistent())
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

    fn handoff_secrets(&self) -> Option<constellation_platform::EphemeralSecretStore> {
        self.node.upgrade()?.handoff_secrets()
    }

    fn handoff(
        &self,
        p: &HandoffParams,
        _fd: Option<OwnedFd>,
        service: Option<&constellation_control::authz::ServiceMatch>,
    ) -> Result<HandoffReport, ControlError> {
        let binary = match &p.target {
            HandoffTarget::Exec { binary } => binary.clone(),
            HandoffTarget::Socket => {
                return crate::handoff_socket::sender(&self.node()?, p, _fd, service);
            }
        };
        if p.phase.is_some() || p.deadline_ms.is_some() {
            return Err(ControlError::invalid(
                "an in-place upgrade has no phases: phase and deadline_ms are for a handoff to a \
                 socket",
            ));
        }
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
                transport: m.fuse.transport().name().to_string(),
            })
            .collect();
        let detail =
            crate::handover::upgrade(&node, binary.as_deref()).map_err(ControlError::failed)?;
        Ok(HandoffReport {
            detail,
            views,
            ..Default::default()
        })
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

/// Whether `e`, a call's error, says the daemon went away while the call
/// was out rather than that it refused the call: the connection ended
/// before the answer came (the client's own `Unavailable`: the daemon
/// exited), the daemon cancelled the call as it shut down its control
/// server (`Cancelled`; this client cancels nothing it waits for without
/// a bound), or it answered that it is shutting down (`Unavailable`).
/// `umount` goes by the daemon's record for such a call, and
/// `daemon --upgrade` by its `status` (the `exec` of the handover closes
/// the call's connection, close-on-exec, before the answer may be out).
pub fn went_away(e: &ControlError) -> bool {
    matches!(e.kind, ErrorKind::Unavailable | ErrorKind::Cancelled)
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

/// How the filesystem manager's `export` reaches the daemon of a state
/// dir: this client, bounded as every liveness probe is
/// (`CONSTELLATION_CONTROL_TIMEOUT_MS`).
pub struct CliDaemons;

impl constellation_engine::manager::Daemons for CliDaemons {
    fn answers<'a>(&'a self, state_dir: &'a Path) -> futures::future::BoxFuture<'a, bool> {
        Box::pin(async move {
            matches!(
                ping(state_dir, crate::daemon_lock::control_timeout()).await,
                Ok(true)
            )
        })
    }

    fn leave<'a>(
        &'a self,
        state_dir: &'a Path,
        force: bool,
    ) -> futures::future::BoxFuture<'a, Result<Result<String, ControlError>, ControlError>> {
        Box::pin(async move {
            let params = api::LeaveParams {
                node_id: None,
                force,
            };
            match try_call::<constellation_control::methods::NodeLeave>(state_dir, params, None)
                .await
            {
                Ok(answer) => Ok(answer.map(|ack| ack.detail)),
                Err(e) => Err(ControlError::unavailable(format!("{e:#}"))),
            }
        })
    }

    fn socket_exists(&self, state_dir: &Path) -> bool {
        socket_exists(state_dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_control::authz::{Principal, Role};

    /// A `view.list` answered after the daemon's node is gone (a drain
    /// over at once, the node dropped, the control server still serving
    /// until the process exits) says the daemon exits, as it does: it used
    /// to say it keeps running without views, and `umount` returned success
    /// over a drain that had left a chunk unshipped (`umount_drain`'s
    /// `umount_fails_when_the_drain_cannot_ship_everything`, once under
    /// load).
    #[test]
    fn a_daemon_whose_node_is_gone_exits_when_empty() {
        let host = DaemonHost { node: Weak::new() };
        assert!(host.views().is_empty());
        assert!(
            constellation_engine::control::ControlHost::exits_when_empty(&host),
            "a daemon whose node is gone was reported as one that keeps serving"
        );
    }

    fn unix(uid: u32) -> Principal {
        Principal::Unix {
            uid,
            gids: vec![uid],
            pid: Some(1),
        }
    }

    /// The daemon's own policy wiring (plan 33 U1): the bound socket a
    /// `kind = "service"` grant needs reaches the policy, and the grant is
    /// inert without it. `daemon_router` adds only the engine's method
    /// table and the audit sink on top of this.
    #[test]
    fn the_bound_socket_reaches_the_loaded_allowlist() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("control.sock");
        std::fs::write(&sock, b"").unwrap();
        let allowlist = dir.path().join("control-allow.toml");
        std::fs::write(
            &allowlist,
            format!(
                "[[grant]]\nkind = \"service\"\nprincipal = \"uid:4242\"\nsocket = {:?}\nrole = \"operator\"\nlabel = \"csi-node-plugin\"\n",
                sock.display().to_string()
            ),
        )
        .unwrap();
        let bound = std::fs::canonicalize(&sock).unwrap();

        let p = load_policy(Some(&allowlist), 1000, Some(bound.clone()));
        let resolved = p.resolve(&unix(4242)).expect("the service grant matches");
        assert_eq!(resolved.role, Role::Operator);
        assert_eq!(
            resolved.service.map(|s| s.label),
            Some("csi-node-plugin".to_string())
        );

        // No bound socket (binding failed): fails closed, owner still admin.
        let p = load_policy(Some(&allowlist), 1000, None);
        assert_eq!(p.role_of(&unix(4242)), None);
        assert_eq!(p.role_of(&unix(1000)), Some(Role::Admin));

        // A daemon bound elsewhere: no match.
        let p = load_policy(Some(&allowlist), 1000, Some(dir.path().join("other.sock")));
        assert_eq!(p.role_of(&unix(4242)), None);

        // A broken allowlist is owner-only-admin, not the previous grants
        // (there is no reload path) and not fail-open.
        std::fs::write(&allowlist, "[[grant]]\nuid = \"not-a-number\"\n").unwrap();
        let p = load_policy(Some(&allowlist), 1000, Some(bound));
        assert_eq!(p.role_of(&unix(4242)), None);
        assert_eq!(p.role_of(&unix(1000)), Some(Role::Admin));
        assert!(p.grants().is_empty());

        // No allowlist configured at all: same owner-only policy.
        let p = load_policy(None, 1000, None);
        assert_eq!(p.role_of(&unix(1000)), Some(Role::Admin));
    }
}
