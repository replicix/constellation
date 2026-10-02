//! The engine's control service (plan 31 §9.7, C5): every method of the
//! control protocol's table ([`constellation_control::METHODS`]) bound to
//! an [`Engine`], served by whatever host runs it.
//!
//! ## Engine here, host there
//!
//! [`EngineControl`] is the one object behind the table — the daemon
//! serves it over its unix socket and the web adapter, a test or an
//! embedding host calls it in-process through the same [`Router`]. It lives
//! in the engine, not in the CLI, because every host of an engine (the
//! desktop daemon today; plan 37's CSI engine pod, plan 36's Android
//! service) needs the same 60 methods with the same semantics, and
//! everything they touch — the metadata replica, the sync task, the
//! snapshot manager, the registry, the op watchdog — is the engine's.
//!
//! What only a host can do comes through [`ControlHost`]: the kernel mounts
//! (`view.mount`/`view.unmount` and the mount list every status carries),
//! the session handover (`node.handoff`), and detaching every view after
//! a self-`leave`. The daemon implements it over its FUSE sessions; a test
//! implements it with bare [`View`]s. (Host lifecycle events are the
//! engine's own: `node.lifecycle` pushes into the host services' manual
//! lifecycle source, which the engine subscribes to — [`lifecycle`].)
//!
//! ## The 37 old methods keep their semantics
//!
//! [`service`] holds the retired `StatusSource`'s bodies, moved from the
//! daemon unchanged: the same report, the same refusal strings (now the
//! `message` of a `Failed` [`ControlError`]), the same waits on the sync
//! task. They are synchronous (they park on the engine's runtime), so the
//! router runs each on a blocking thread ([`blocking`]).
//!
//! ## What C5 adds
//!
//! - `browse.stat/read/write/mkdir/rename/delete/xattr` through
//!   [`ControlVfs`], a `Vfs` client of a view this service opens for itself
//!   (the whole filesystem), as the calling principal.
//! - `fs.*`: the registry ([`fs`]).
//! - `view.stats` (a view's `statfs` and recursive size/count), `view.list`
//!   label filters, `peers.list`, `node.ops` (the watchdog's registry).
//! - `stats.subscribe` (periodic samples of `/metrics`'s gauges) and
//!   `events.subscribe` (view, lease and peer transitions, observed by a
//!   watcher started with the first subscriber).
//! - `node.lifecycle` (plan 31 C8): a host lifecycle event, applied by the
//!   engine before the answer ([`lifecycle`]).
//!
//! Plan 32's `snapshot.policy.*` methods live in [`snapsched`].

mod browse;
mod fs;
mod lifecycle;
mod ops;
mod service;
pub mod snapsched;
mod streams;

#[cfg(test)]
mod parity_tests;

pub use browse::ControlVfs;

use crate::view::View;
use crate::Engine;
use constellation_control::fd::OwnedFd;
use constellation_control::methods::*;
use constellation_control::proto::types as api;
use constellation_control::proto::types::{
    Ack, CacheEntryListing, CachePruneResult, DelegationListing, DesignationListing,
    DirectoryListing, FileStat, FsckReport, GcReport, HandoffParams, HandoffReport, HandoverStatus,
    PeerListing, PinListing, Pong, PruneRootListing, QuotaStatus, RefHashes, SnapshotCreated,
    SnapshotHeld, SnapshotListing, ViewInfo, ViewListing, ViewMountParams, ViewStatsReport,
};
use constellation_control::proto::{ControlError, JsonValue};
use constellation_control::{CallCtx, Principal, Router};
use constellation_vfs::Caller;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// One view a host serves, as `view.list`, `view.stats` and `node.status`
/// see it.
#[derive(Clone)]
pub struct HostView {
    pub id: u64,
    /// The subtree or selector as mounted (`/`, `/data`, `/data@snap`).
    pub subtree: String,
    pub mountpoint: PathBuf,
    pub since: Instant,
    pub labels: BTreeMap<String, String>,
    pub qos: api::ViewQos,
    pub confine_links: bool,
    /// Plan 38 §5: the FUSE transport this view's session negotiated
    /// (`dev_fuse`/`uring`/`uring_zc`), as `node.status` reports it per
    /// mount. A `String` rather than the frontend's `Transport`: this
    /// crate names no frontend types, and a host with no FUSE session
    /// behind the view (a control-only embedder) has `None`.
    pub transport: Option<String>,
    /// The engine view behind it (`view.stats` asks it).
    pub view: Option<Arc<View>>,
}

/// What only the host of an engine can do (see the module docs). Every
/// method may block (a FUSE unmount joins its session thread); the router
/// calls them on blocking threads.
pub trait ControlHost: Send + Sync + 'static {
    /// The views the host serves.
    fn views(&self) -> Vec<HostView>;

    /// `view.mount`: `fd` is the request's descriptor for
    /// `MountSource::PreopenedFd` (the router has checked it is there).
    fn mount(
        &self,
        params: &ViewMountParams,
        fd: Option<OwnedFd>,
    ) -> Result<ViewInfo, ControlError>;

    /// `view.unmount`: the summary line the CLI prints.
    fn unmount(&self, mountpoint: &Path) -> Result<String, ControlError>;

    /// Detach every view (after a self-`leave` has been answered).
    fn detach_all(&self);

    /// Where an in-place upgrade stands.
    fn handover_status(&self) -> HandoverStatus;

    /// `node.handoff`.
    fn handoff(
        &self,
        params: &HandoffParams,
        fd: Option<OwnedFd>,
    ) -> Result<HandoffReport, ControlError>;
}

/// The engine's control service. See the module docs.
pub struct EngineControl {
    pub(crate) engine: Arc<Engine>,
    pub(crate) host: Arc<dyn ControlHost>,
    pub(crate) version: String,
    pub(crate) log_buffer: crate::log_buffer::LogBuffer,
    // The engine's handles, as the old `DaemonStatus` held them (the moved
    // bodies in `service` address them by these names).
    pub(crate) meta: Arc<constellation_meta::Meta>,
    pub(crate) cache: Arc<constellation_fs_core::cache::DiskCache>,
    pub(crate) staging_budget: Arc<crate::staging::StagingBudget>,
    pub(crate) core: Arc<Mutex<crate::authority_driver::CoreStatus>>,
    pub(crate) lease: Arc<crate::lease::LeaseView>,
    pub(crate) fs_uuid: String,
    pub(crate) backend: String,
    pub(crate) node_id: u64,
    pub(crate) started: Instant,
    pub(crate) peers: constellation_net::Peers,
    pub(crate) pins: Arc<crate::pin::PinManager>,
    pub(crate) designations: Arc<crate::designation::DesignationManager>,
    pub(crate) epochs: Arc<crate::epoch::EpochManager>,
    pub(crate) reintegration: Arc<crate::reintegrate::ReintegrationState>,
    pub(crate) sync_tx: tokio::sync::mpsc::UnboundedSender<crate::sync::SyncRequest>,
    pub(crate) store: Arc<dyn object_store::ObjectStore>,
    pub(crate) departed: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) rt: tokio::runtime::Handle,
    pub(crate) coop: Arc<crate::coop::Coop>,
    pub(crate) prefetch_stats: Arc<crate::prefetch::PrefetchStats>,
    pub(crate) write_mode: Arc<crate::writeback::WriteModeState>,
    pub(crate) upload: Arc<crate::upload::UploadRuntime>,
    pub(crate) snapshots: Arc<crate::snapshot::SnapshotManager>,
    pub(crate) forward: Arc<crate::forward::ForwardState>,
    pub(crate) placement: Arc<crate::placement::Placement>,
    pub(crate) atime: Arc<crate::atime::AtimeAccumulator>,
    pub(crate) prune_stats: Arc<crate::prune::PruneStats>,
    pub(crate) lease_mode: constellation_store_s3::LeaseMode,
    pub(crate) read_only_member: bool,
    pub(crate) last_sync_ms: Arc<std::sync::atomic::AtomicU64>,
    pub(crate) state_dir: PathBuf,
    pub(crate) compression: constellation_store_s3::CompressionSetting,
    /// The whole-filesystem view `browse.*` goes through, opened on first
    /// use and kept for the engine's life.
    browse: Mutex<Option<Arc<View>>>,
    /// `fs.unlock`'s credentials, by the name (or uuid) they were given for.
    pub(crate) unlocked: Mutex<HashMap<String, Arc<constellation_platform::CredentialSource>>>,
    pub(crate) events: streams::EventBus,
}

impl EngineControl {
    /// The service for `engine`, hosted by `host`. `prefetch_stats` are the
    /// view whose readahead `status` reports (the first mounted one, as
    /// before); `version` is what `status` reports as the binary's.
    pub fn new(
        engine: Arc<Engine>,
        host: Arc<dyn ControlHost>,
        log_buffer: crate::log_buffer::LogBuffer,
        prefetch_stats: Arc<crate::prefetch::PrefetchStats>,
        version: impl Into<String>,
    ) -> Arc<EngineControl> {
        let e = &engine;
        Arc::new(EngineControl {
            meta: e.meta().clone(),
            cache: e.cache().clone(),
            staging_budget: e.staging_budget().clone(),
            core: e.core_status().clone(),
            lease: e.lease().clone(),
            fs_uuid: e.fsmeta().uuid.to_string(),
            backend: e.backend_url().to_string(),
            node_id: e.node_id(),
            started: e.started(),
            peers: e.peers().clone(),
            pins: e.pins().clone(),
            designations: e.designations().clone(),
            epochs: e.epochs().clone(),
            reintegration: e.reintegration().clone(),
            sync_tx: e.sync_tx().clone(),
            store: e.store().inner().clone(),
            departed: e.departed().clone(),
            rt: e.runtime().clone(),
            coop: e.coop().clone(),
            prefetch_stats,
            write_mode: e.write_mode().clone(),
            upload: e.upload().clone(),
            snapshots: e.snapshots().clone(),
            forward: e.forward().clone(),
            placement: e.placement().clone(),
            atime: e.atime().clone(),
            prune_stats: e.prune_stats().clone(),
            lease_mode: e.lease_mode(),
            read_only_member: e.read_only_member(),
            last_sync_ms: e.last_sync_ms().clone(),
            state_dir: e.state_dir().to_path_buf(),
            compression: e.compression(),
            engine,
            host,
            version: version.into(),
            log_buffer,
            browse: Mutex::new(None),
            unlocked: Mutex::new(HashMap::new()),
            events: streams::EventBus::new(),
        })
    }

    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    /// Publish a daemon event to `events.subscribe`rs (a host's own
    /// transitions; the watcher adds the ones it observes).
    pub fn publish(&self, topic: &str, data: serde_json::Value) {
        self.events.publish(topic, data);
    }

    /// The host's views in the report's shape.
    pub(crate) fn mount_infos(&self) -> Vec<api::MountInfo> {
        let mut views = self.host.views();
        views.sort_by_key(|v| v.id);
        views
            .into_iter()
            .map(|v| api::MountInfo {
                id: v.id,
                subtree: v.subtree,
                mountpoint: v.mountpoint.display().to_string(),
                mounted_ms_ago: v.since.elapsed().as_millis() as u64,
                transport: v.transport,
            })
            .collect()
    }

    /// A `ControlVfs` over the service's own whole-filesystem view, as
    /// `principal`.
    fn browser(&self, principal: &Principal) -> Result<ControlVfs, ControlError> {
        let view = {
            let mut slot = self.browse.lock().unwrap();
            match &*slot {
                Some(view) => view.clone(),
                None => {
                    let caps = constellation_vfs::FrontendCaps {
                        // Nothing caches on the far side of a control call.
                        push_inval: constellation_vfs::PushInval::None,
                        cluster_locks: false,
                        max_io: 1024 * 1024,
                        ..constellation_vfs::FrontendCaps::linux_fuse(false)
                    };
                    let view = self
                        .engine
                        .open_view(
                            crate::ViewSpec::new("/"),
                            caps,
                            crate::DeferredEvents::new(),
                        )
                        .map_err(|e| ControlError::unavailable(format!("{e:#}")))?;
                    *slot = Some(view.clone());
                    view
                }
            }
        };
        Ok(ControlVfs::new(view, self.caller_of(principal)))
    }

    /// The `Caller` a control principal acts as: a unix peer as itself;
    /// anyone else (in-process, the web adapter) as the daemon's own user.
    fn caller_of(&self, principal: &Principal) -> Caller {
        match principal {
            Principal::Unix { uid, gids, pid } => {
                let gid = gids.first().copied().unwrap_or(*uid);
                let mut caller = Caller::with_groups(*uid, gid, gids);
                caller.pid = *pid;
                caller
            }
            _ => {
                let (uid, gid) = self.engine.host().process.effective_ids();
                Caller::with_groups(uid, gid, &[gid])
            }
        }
    }

    fn view_named(
        &self,
        id: Option<u64>,
        mountpoint: Option<&Path>,
    ) -> Result<HostView, ControlError> {
        if id.is_some() == mountpoint.is_some() {
            return Err(ControlError::invalid(
                "name the view by exactly one of id and mountpoint",
            ));
        }
        self.host
            .views()
            .into_iter()
            .find(|v| Some(v.id) == id || Some(v.mountpoint.as_path()) == mountpoint)
            .ok_or_else(|| ControlError::not_found("no such view"))
    }
}

/// A peer's addresses as strings.
pub(crate) fn peer_addr_strings(addr: &constellation_net::EndpointAddr) -> Vec<String> {
    addr.addrs.iter().map(|a| a.to_string()).collect()
}

/// `/a//b/` → `/a/b`; empty → `/`.
pub(crate) fn normalize_control_path(path: &str) -> String {
    let parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    if parts.is_empty() {
        "/".into()
    } else {
        format!("/{}", parts.join("/"))
    }
}

/// The request watchdog's view (`constellation_vfs::watch`), as `status`
/// reports it (`fuse_requests`).
pub fn fuse_requests_status(
    s: &constellation_vfs::watch::WatchSnapshot,
) -> api::FuseRequestsStatus {
    api::FuseRequestsStatus {
        in_flight: s.in_flight,
        stalled: s.stalled,
        stalled_total: s.stalled_total,
        stalled_completed: s.stalled_completed,
        oldest_s: s.oldest_s,
        stall_threshold_s: s.stall_threshold_s,
        stalled_requests: s
            .stalled_ops
            .iter()
            .map(|op| api::StalledFuseRequest {
                op: op.op.to_string(),
                ino: op.ino,
                age_s: op.age_s,
                stage: op.stage.to_string(),
                tid: op.tid as u32,
                blocking: op.blocking,
            })
            .collect(),
    }
}

/// The old refusals were strings; they arrive as a `Failed` error whose
/// message is that string.
fn failed(message: String) -> ControlError {
    ControlError::failed(message)
}

/// Run `f` on a blocking thread (the moved bodies park on the runtime).
pub(crate) async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, ControlError> + Send + 'static,
) -> Result<T, ControlError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| ControlError::failed(format!("the handler failed: {e}")))?
}

/// What a blocking handler receives besides its params.
pub(crate) struct Call {
    principal: Principal,
    /// The role that admitted the caller: a handler whose *arguments*
    /// need more than the method's minimum role checks it itself
    /// (`snapshot.hold`'s `force`).
    role: constellation_control::authz::Role,
    fd: Option<OwnedFd>,
}

type Body<M> =
    fn(&EngineControl, Call, <M as Method>::Params) -> Result<<M as Method>::Result, ControlError>;

/// Register `M` as a blocking body over `svc`.
fn unary<M: Method>(router: &mut Router, svc: &Arc<EngineControl>, body: Body<M>) {
    let svc = svc.clone();
    router.register::<M, _, _>(move |mut ctx: CallCtx, params| {
        let svc = svc.clone();
        let call = Call {
            principal: ctx.principal.clone(),
            role: ctx.role,
            fd: ctx.take_fd(),
        };
        async move { blocking(move || body(&svc, call, params)).await }
    });
}

fn ack(r: Result<String, String>) -> Result<Ack, ControlError> {
    r.map(Ack::new).map_err(failed)
}

/// A router serving `svc` for every method in the table.
pub fn router(svc: &Arc<EngineControl>) -> Router {
    let mut r = Router::new();
    register(&mut r, svc);
    r
}

/// Register every method of the table on `router`, served by `svc`.
pub fn register(r: &mut Router, svc: &Arc<EngineControl>) {
    // ---- node ----
    r.register::<NodePing, _, _>(|_c, _p| async { Ok(Pong {}) });
    unary::<NodeStatus>(r, svc, |s, _, _| Ok(s.status()));
    unary::<NodeReintegrate>(r, svc, |s, _, _| ack(s.reintegrate()));
    unary::<NodeLeave>(r, svc, |s, _, p| ack(s.leave(p.node_id, p.force)));
    unary::<NodeSetWriteMode>(r, svc, |s, _, p| ack(s.set_write_mode(&p.mode)));
    streams::register_logs_tail(r, svc);
    unary::<NodeDoctor>(r, svc, |s, _, _| s.doctor().map_err(failed));
    unary::<NodeOps>(r, svc, |s, _, p| s.node_ops(&p));
    unary::<NodeHandoff>(r, svc, |s, call, p| s.host.handoff(&p, call.fd));
    unary::<NodeLifecycle>(r, svc, |s, _, p| s.lifecycle(&p));

    // ---- pin / designation ----
    unary::<PinAdd>(r, svc, |s, _, p| ack(s.pin(&p.path)));
    unary::<PinRemove>(r, svc, |s, _, p| ack(s.unpin(&p.path)));
    unary::<PinList>(r, svc, |s, _, _| {
        Ok(PinListing {
            pins: s.list_pins(),
        })
    });
    unary::<DesignationOffline>(r, svc, |s, _, p| ack(s.offline(&p.path, p.read_only)));
    unary::<DesignationDelegate>(r, svc, |s, _, p| {
        ack(s.delegate(&p.path, p.node, p.range.as_deref()))
    });
    unary::<DesignationUndelegate>(r, svc, |s, _, p| ack(s.undelegate(&p.path)));
    unary::<DesignationListDelegations>(r, svc, |s, _, _| {
        Ok(DelegationListing {
            delegations: s.list_delegations(),
        })
    });
    unary::<DesignationOnline>(r, svc, |s, _, p| ack(s.online(&p.path)));
    unary::<DesignationList>(r, svc, |s, _, _| {
        Ok(DesignationListing {
            designations: s.list_designations(),
        })
    });

    // ---- prune / gc / fsck ----
    unary::<PruneRun>(r, svc, |s, _, p| {
        ack(s.prune_run(p.path.as_deref(), p.dry_run))
    });
    unary::<PruneList>(r, svc, |s, _, _| {
        s.prune_ls()
            .map(|roots| PruneRootListing { roots })
            .map_err(failed)
    });
    unary::<GcRun>(r, svc, |s, _, p| {
        s.gc_run(p.verify_only)
            .map(|report| GcReport {
                report: JsonValue(report),
            })
            .map_err(failed)
    });
    unary::<FsckRun>(r, svc, |s, _, p| {
        s.fsck_run(p.repair, p.force_release.as_deref())
            .map(|report| FsckReport {
                report: JsonValue(report),
            })
            .map_err(failed)
    });

    // ---- snapshot / clone ----
    unary::<SnapshotCreate>(r, svc, |s, _, p| {
        // Plan 32 §0.4 / plan 37 §5: `hold: true` with `held_by`, and the
        // shorthand `hold: "csi:<uid>"`, mean the same thing; naming an
        // owner at all is asking for a hold.
        let (held, held_by) = p.hold_request().map_err(ControlError::invalid)?;
        if let Some(by) = held_by {
            crate::snapshot::validate_owner(by)
                .map_err(|e| ControlError::invalid(format!("{e:#}")))?;
        }
        let options = crate::snapshot::SnapshotOptions {
            held,
            held_by: held_by.map(str::to_string),
            ..Default::default()
        };
        let (detail, snapshot) = s.snapshot_create(&p.selector, &options).map_err(failed)?;
        Ok(SnapshotCreated { detail, snapshot })
    });
    unary::<SnapshotList>(r, svc, |s, _, p| {
        s.snapshot_list(p.path.as_deref())
            .map(|snapshots| SnapshotListing { snapshots })
            .map_err(failed)
    });
    unary::<SnapshotDelete>(r, svc, |s, _, p| {
        ack(s.snapshot_delete(&p.selector, p.force))
    });
    unary::<SnapshotHold>(r, svc, |s, c, p| {
        // Overriding another owner's hold is a destructive act on someone
        // else's state: operator may hold and release its own, only admin
        // may force.
        if p.force && c.role < constellation_control::authz::Role::Admin {
            return Err(ControlError::denied(
                "forcing a hold past its recorded owner needs the admin role",
            )
            .with_remediation("release it as its owner (`--by`), or ask an admin"));
        }
        let by = p.by.as_deref().filter(|by| !by.is_empty());
        if let Some(by) = by {
            crate::snapshot::validate_owner(by)
                .map_err(|e| ControlError::invalid(format!("{e:#}")))?;
        }
        let (detail, snapshot) = s
            .snapshot_hold(&p.id, p.held, by, p.force)
            .map_err(failed)?;
        Ok(SnapshotHeld { detail, snapshot })
    });
    unary::<SnapshotRefs>(r, svc, |s, _, p| {
        s.snap_refs(&p.id)
            .map(|hashes| RefHashes { hashes })
            .map_err(failed)
    });
    unary::<CloneCreate>(r, svc, |s, _, p| {
        ack(s.clone_snapshot(&p.selector, &p.destination))
    });
    snapsched::register(r, svc);

    // ---- browse ----
    unary::<BrowseReaddir>(r, svc, |s, _, p| {
        s.read_dir(&p.path)
            .map(|entries| DirectoryListing {
                path: p.path,
                entries,
            })
            .map_err(failed)
    });
    unary::<BrowseInspect>(r, svc, |s, _, p| s.inspect(&p.path).map_err(failed));
    unary::<BrowseStat>(r, svc, |s, c, p| -> Result<FileStat, ControlError> {
        s.browser(&c.principal)?.stat(&p.path)
    });
    streams::register_browse_read(r, svc);
    unary::<BrowseWrite>(r, svc, |s, c, p| {
        s.browser(&c.principal)?.write(
            &p.path,
            p.offset,
            &p.data.0,
            p.create,
            p.create_mode,
            p.truncate,
        )
    });
    unary::<BrowseMkdir>(r, svc, |s, c, p| {
        s.browser(&c.principal)?.mkdir(&p.path, p.mode, p.parents)
    });
    unary::<BrowseRename>(r, svc, |s, c, p| {
        s.browser(&c.principal)?
            .rename(&p.from, &p.to, p.overwrite)?;
        Ok(Ack::new(format!("renamed {} to {}", p.from, p.to)))
    });
    unary::<BrowseDelete>(r, svc, |s, c, p| {
        s.browser(&c.principal)?.delete(&p.path, p.recursive)?;
        Ok(Ack::new(format!("deleted {}", p.path)))
    });
    unary::<BrowseXattr>(r, svc, |s, c, p| {
        s.browser(&c.principal)?.xattr(&p.path, &p.op)
    });

    // ---- locks / cache / quota ----
    unary::<LocksForceRelease>(r, svc, |s, _, p| ack(s.force_release(&p.part)));
    unary::<LocksDropHeld>(r, svc, |s, _, p| ack(s.drop_held(p.ino, p.remote)));
    unary::<CacheList>(r, svc, |s, _, _| {
        Ok(CacheEntryListing {
            entries: s.cache_list(),
        })
    });
    unary::<CachePrune>(r, svc, |s, _, p| {
        let report = s
            .cache
            .prune_to(p.target_bytes)
            .map_err(|e| failed(e.to_string()))?;
        Ok(CachePruneResult {
            freed_bytes: report.freed_bytes,
            remaining_bytes: report.used_bytes,
            detail: format!(
                "pruned {} chunks ({} bytes); {} bytes remain \
                 ({} pinned, {} dirty, {} entries)",
                report.freed_chunks,
                report.freed_bytes,
                report.used_bytes,
                report.pinned_bytes,
                report.dirty_bytes,
                report.entries
            ),
        })
    });
    unary::<QuotaSet>(r, svc, |s, _, p| {
        s.set_quota(p.max_bytes).map_err(failed)?;
        let (max_bytes, used_bytes) = s.get_quota().map_err(failed)?;
        Ok(QuotaStatus {
            max_bytes,
            used_bytes,
        })
    });
    unary::<QuotaGet>(r, svc, |s, _, _| {
        let (max_bytes, used_bytes) = s.get_quota().map_err(failed)?;
        Ok(QuotaStatus {
            max_bytes,
            used_bytes,
        })
    });

    // ---- view ----
    unary::<ViewMount>(r, svc, |s, call, p| s.host.mount(&p, call.fd));
    unary::<ViewUnmount>(r, svc, |s, _, p| {
        s.host.unmount(&p.mountpoint).map(Ack::new)
    });
    unary::<ViewList>(r, svc, |s, _, p| {
        let mut views: Vec<ViewInfo> = s
            .host
            .views()
            .into_iter()
            .filter(|v| p.labels.iter().all(|(k, val)| v.labels.get(k) == Some(val)))
            .map(|v| ViewInfo {
                id: v.id,
                subtree: v.subtree,
                mountpoint: v.mountpoint.display().to_string(),
                mounted_ms_ago: v.since.elapsed().as_millis() as u64,
                labels: v.labels,
                qos: v.qos,
                confine_links: v.confine_links,
            })
            .collect();
        views.sort_by_key(|v| v.id);
        Ok(ViewListing { views })
    });
    unary::<ViewStats>(r, svc, |s, c, p| {
        let hv = s.view_named(p.id, p.mountpoint.as_deref())?;
        let view = hv
            .view
            .clone()
            .ok_or_else(|| ControlError::unavailable("the view is not open in this engine"))?;
        let (fs, rsize, rcount) = ControlVfs::new(view, s.caller_of(&c.principal)).stats()?;
        let block = fs.frsize as u64;
        Ok(ViewStatsReport {
            id: hv.id,
            block_size: fs.frsize,
            total_bytes: fs.blocks.saturating_mul(block),
            used_bytes: fs.blocks.saturating_sub(fs.bfree).saturating_mul(block),
            available_bytes: fs.bavail.saturating_mul(block),
            inodes_total: fs.files.saturating_add(fs.ffree),
            inodes_used: fs.files,
            rsize,
            rcount,
        })
    });

    // ---- peers / streams ----
    unary::<PeersList>(r, svc, |s, _, _| {
        Ok(PeerListing {
            peers: s.status().p2p.peers,
        })
    });
    streams::register_subscriptions(r, svc);

    // ---- fs registry ----
    {
        let svc = svc.clone();
        r.register::<FsList, _, _>(move |_c, _p| {
            let svc = svc.clone();
            async move { svc.fs_list().await }
        });
    }
    {
        let svc = svc.clone();
        r.register::<FsCreate, _, _>(move |_c, p| {
            let svc = svc.clone();
            async move { svc.fs_create(p).await }
        });
    }
    unary::<FsImport>(r, svc, |s, _, p| s.fs_import(p));
    unary::<FsExport>(r, svc, |s, _, p| s.fs_export(&p.fs));
    {
        let svc = svc.clone();
        r.register::<FsPasswd, _, _>(move |_c, p| {
            let svc = svc.clone();
            async move {
                let fs = p.fs.clone();
                svc.fs_passwd(p).await?;
                Ok(Ack::new(format!(
                    "{fs}: passphrase changed; data-encryption keys were not rotated"
                )))
            }
        });
    }
    {
        let svc = svc.clone();
        r.register::<FsDoctor, _, _>(move |_c, p| {
            let svc = svc.clone();
            async move { svc.fs_doctor(p.fs).await }
        });
    }
    unary::<FsUnlock>(r, svc, |s, _, p| s.fs_unlock(p).map(Ack::new));
}
