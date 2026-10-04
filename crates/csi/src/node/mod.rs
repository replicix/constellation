//! The Node service (plan 37 §4, §5, K3): staging a volume is a FUSE mount
//! this privileged plugin makes and an engine pod serves; publishing it is a
//! bind mount.
//!
//! **`NodeStageVolume`** (settled decision 5): the `volume_id` names the
//! filesystem and subtree ([`crate::volume_id`]), the `volume_context` names
//! where the filesystem lives (the class parameters the controller put
//! there, [`crate::params::volume_context`], or a static PV's
//! `volumeAttributes`). From those the plugin
//!
//! 1. brings up this node's engine pod of that pool filesystem
//!    (`constellation-engine-<pool>[-shard-k]-<node>`, [`NodeEngines`]) and
//!    checks it serves the filesystem the id names (`fs.list`);
//!    Its credentials go with the bring-up ([`crate::credentials`]): a pod
//!    of a class that needs them starts with `--await-unlock` and answers
//!    nothing until `fs.unlock` brings them, which [`NodeEngines::engine`]
//!    sends once per pod incarnation (a new pod, or a restart of its
//!    engine container) and again whenever they differ from what that
//!    incarnation got. They come from the request's node-stage secret, or
//!    for a `refreshing` class from the watched Secret, or — for a restage
//!    whose request carries none — from what this plugin last sent the
//!    unit, held in memory only. A `refreshing` class's watch also pushes
//!    every change of the Secret to the running pod at once: rotation
//!    with no remount;
//! 2. reads the volume directory's record (`user.constellation.csi.*`),
//!    which fails `NOT_FOUND` for a volume that is not there;
//! 3. mounts FUSE at the staging path (`fuse_mount_fd`, [`Mounter`]) and
//!    sends the descriptor with `view.mount{PreopenedFd}` over the pod's
//!    hostPath control socket, naming the staging path as the view's
//!    mountpoint; the plugin's copy of the descriptor is closed as soon as
//!    it is sent, so the engine pod is the connection's only holder;
//! 4. records the volume ([`StateStore`]) and the pod's view count.
//!
//! The session is `/dev/fuse` by construction: a `view.mount{PreopenedFd}`
//! is served handover-capable, which pins the transport whatever the engine
//! pod's knob says (plan 38 §3(e); `MountOptions::handover_capable`), and
//! the request carries no transport to ask for anything else.
//!
//! **`NodeUnstageVolume`** unmounts the staging path — the plugin made the
//! mount, the plugin unmounts it, which also ends the engine's session —
//! then asks the engine for `view.unmount` by the same name, which answers
//! at once whether the session already ended or not (it ends a session it
//! did not mount rather than unmounting it), and records the pod's view
//! count. An idle pod leaves the pool's registry and is deleted by the idle
//! GC ([`gc`], 37-k6b), which also collects a draining node's pods.
//!
//! **`NodePublishVolume`** checks the staging mount on every call
//! (`requiresRepublish: true`, settled decision 12): alive, it bind-mounts
//! it at the target (read-only for a reader-only mode or a `readonly`
//! publish) unless that bind is already there; dead (`ENOTCONN`: the engine
//! pod crashed and took the connection with it), it restages first against
//! the pod's next incarnation, from the volume context and the recorded
//! state — with the credentials this plugin last sent the unit (or the
//! watched Secret's), since a publish carries no node-stage secret; a
//! plugin restarted since then has none for a `static-ephemeral` class,
//! and the restage fails `UNAVAILABLE` saying so until a request brings
//! the secret again. Containers started before the crash keep their dead
//! bind until they restart: a new mount at the target reaches only new
//! mounts of it.
//!
//! **`NodeGetVolumeStats`** is `view.stats` (the subtree's recursive bytes
//! and entries) plus the subtree's quota cap (`quota.get{cap_only}`, so the
//! subtree is walked once, not twice). **Volume condition**: CSI 1.13 (the
//! vendored spec) removed `VolumeCondition` from `NodeGetVolumeStats`
//! (field 2 is reserved) in favour of `NodeGetVolumeHealth`
//! (`GET_VOLUME_HEALTH`), so the abnormal-volume signal of plan 37 §11 is
//! answered there: the staging mount dead or gone, the engine pod
//! unreachable, the volume directory removed, or its record changed behind
//! the driver's back (settled decision 18). Neither RPC enforces anything.
//!
//! **Concurrency.** One operation per volume at a time in this process
//! (`ABORTED` for a second, as the controller does); one bring-up per
//! engine pod ([`NodeEngines`]).

pub mod engines;
pub mod gc;
pub mod handoff;
pub mod mounter;
pub mod rollout;
pub mod state;

use crate::control_client::PoolRef;
use crate::controller::{read_record, status, VolumeLocks, X_NAMESPACE, X_PV, X_PVC};
use crate::credentials::{is_awaiting_unlock, unlock_params, Refresher, Secrets};
use crate::engine_pods::pool_label;
use crate::params::{ClassParams, CredentialMode, SecretRef, SHARD_KEY};
use crate::proto::csi::v1::node_server::Node as NodeRpc;
use crate::proto::csi::v1::node_service_capability::rpc::Type as RpcType;
use crate::proto::csi::v1::node_service_capability::{Rpc, Type as CapabilityType};
use crate::proto::csi::v1::volume_capability::access_mode::Mode;
use crate::proto::csi::v1::volume_capability::AccessType;
use crate::proto::csi::v1::volume_health::VolumeHealthEntry;
use crate::proto::csi::v1::volume_usage::Unit;
use crate::proto::csi::v1::*;
use crate::volume_id::VolumeId;
use constellation_control::proto::types::{
    MountSource, MountViewOpts, ViewListParams, ViewMountParams, ViewStatsParams, ViewUnmountParams,
};
use constellation_control::proto::ErrorKind;
pub use engines::{
    Drain, Drift, InMemoryNodeEngines, NodeEngine, NodeEngines, Replacement, UnitPod,
};
pub use mounter::{FakeMounter, FuseMountOptions, LinuxMounter, MountState, Mounter};
use state::{StateStore, VolumeRecord};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::{Request, Response, Status};

/// The engine image's uid/gid, which the staging mount is made for.
pub const ENGINE_UID: u32 = 65532;
/// The view label carrying the staged `volume_id`, by which a restarted
/// plugin recognises a view it staged before it lost its record.
pub const LABEL_VOLUME_ID: &str = "volume-id";

/// How long a `stat` of a mount may take before it counts as wedged.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long recording an engine pod's view count (a PATCH to the API
/// server) may take. A slow API server must not hold up the end of every
/// later stage and unstage of the pod's unit; the count a timed-out patch
/// missed is recorded by the unit's next stage or unstage.
const ANNOTATION_TIMEOUT: Duration = Duration::from_secs(10);

/// What a bounded look at a mount found ([`NodeService::probe`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Probe {
    State(MountState),
    /// The `stat` did not come back: a live but wedged FUSE server.
    Wedged,
}

impl Probe {
    /// Mounted and served, if slowly: nothing to restage.
    fn served(self) -> bool {
        matches!(self, Probe::State(MountState::Alive { .. }) | Probe::Wedged)
    }
}

pub struct NodeService {
    /// The k8s node name, reported as `NodeGetInfo.node_id`.
    node_id: String,
    /// `None` outside a cluster without `--in-memory-backend`: staging
    /// answers `UNAVAILABLE` (nowhere to start an engine pod).
    engines: Option<Arc<dyn NodeEngines>>,
    mounter: Arc<dyn Mounter>,
    state: StateStore,
    locks: VolumeLocks,
    /// The `refreshing` classes' Secret watch (`None`: outside a cluster).
    refresher: Option<Arc<Refresher>>,
    /// unit → the credentials last sent its engine pod, in memory only:
    /// what unlocks a replacement pod for a restage (module docs).
    remembered: Arc<Mutex<HashMap<String, Secrets>>>,
    /// Units whose watched Secret is pushed to their pod on every change,
    /// and the task doing it.
    rotating: Mutex<HashMap<String, (SecretRef, tokio::task::AbortHandle)>>,
    /// One per engine pod unit, held across recording a staged volume and
    /// keeping its credentials, and across forgetting them: stages of
    /// different volumes of one unit run concurrently, and an unstage's
    /// "no volume left" check must not interleave with their record.
    unit_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// Held from counting an engine pod's views to the end of the patch
    /// that records the count. Stages and unstages of different volumes
    /// run concurrently; without it two of them count 24 and 25, and their
    /// patches can land in the other order, leaving the pod annotated
    /// with a count it no longer has — or `0` and idle while it serves a
    /// view (37-k3b's `csi-many-pvs-one-pool` saw 23 of 25). One lock
    /// per engine pod unit: units are independent, and a slow patch of
    /// one must not hold up the others.
    views_annotation: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// [`ANNOTATION_TIMEOUT`]; tests shorten it.
    annotation_timeout: Duration,
    /// Plan 37 §8: one gate per engine-pod unit. Stages and unstages hold
    /// it shared; a rollout's handoff holds it alone, so no view comes or
    /// goes while the unit's sessions change hands ([`rollout`]). A
    /// `refreshing` class's rotation push holds it shared too, so a
    /// rotation never lands on the pod a handoff is retiring.
    ///
    /// **Lock order** (37-k6a): a unit gate first, then the unit's
    /// bring-up lock (`NodeEngines`), then [`Self::unit_locks`]; nothing
    /// takes a unit gate while it holds one of the others. The unit lock
    /// is a plain mutex never held across an `await`, and the handoff
    /// (which holds the gate alone) takes neither of the others.
    unit_gates: Arc<Mutex<HashMap<String, Arc<tokio::sync::RwLock<()>>>>>,
    /// The handoff's bounds and the rollouts' bookkeeping.
    handoff: handoff::HandoffConfig,
    rollout: rollout::RolloutState,
}

impl NodeService {
    pub fn new(
        node_id: String,
        engines: Option<Arc<dyn NodeEngines>>,
        mounter: Arc<dyn Mounter>,
        state: StateStore,
    ) -> NodeService {
        NodeService {
            node_id,
            engines,
            mounter,
            state,
            locks: VolumeLocks::default(),
            refresher: None,
            remembered: Arc::default(),
            rotating: Mutex::default(),
            unit_locks: Mutex::default(),
            views_annotation: Mutex::default(),
            annotation_timeout: ANNOTATION_TIMEOUT,
            unit_gates: Arc::default(),
            handoff: handoff::HandoffConfig::default(),
            rollout: rollout::RolloutState::default(),
        }
    }

    /// Rollouts hand sessions over within `cfg` ([`handoff::HandoffConfig`]).
    pub fn with_handoff(mut self, cfg: handoff::HandoffConfig) -> NodeService {
        self.handoff = cfg;
        self
    }

    /// The handoff counters (the node plugin's metrics endpoint).
    pub fn handoff_metrics(&self) -> Arc<handoff::HandoffMetrics> {
        self.rollout.metrics.clone()
    }

    fn unit_gate(&self, unit: &str) -> Arc<tokio::sync::RwLock<()>> {
        gate_of(&self.unit_gates, unit)
    }

    /// `unit`'s gate, shared: what every RPC that touches a staging mount
    /// or its engine holds, so none of them meets a session that is
    /// changing hands (its mount would not answer — a `stat` of the
    /// staging path blocks until the handoff ends) — it waits for the
    /// handoff instead ([`rollout`]). Taken once per RPC: a second shared
    /// acquisition behind a waiting rollout would deadlock.
    async fn shared_gate(&self, unit: &str) -> tokio::sync::OwnedRwLockReadGuard<()> {
        self.unit_gate(unit).read_owned().await
    }

    /// Watch the Secrets `refreshing` classes name (module docs).
    pub fn with_refresher(mut self, refresher: Arc<Refresher>) -> NodeService {
        self.refresher = Some(refresher);
        self
    }

    fn engines(&self) -> Result<&Arc<dyn NodeEngines>, Status> {
        self.engines.as_ref().ok_or_else(|| {
            Status::unavailable(
                "no engine backend: the node plugin runs outside a Kubernetes cluster without \
                 --in-memory-backend",
            )
        })
    }

    /// What is mounted at `path`, bounded by [`PROBE_TIMEOUT`] (a `stat`
    /// of a FUSE mount waits for its server).
    async fn probe(&self, path: &Path) -> Probe {
        let mounter = self.mounter.clone();
        let path = path.to_path_buf();
        let look = tokio::task::spawn_blocking(move || mounter.state(&path));
        match tokio::time::timeout(PROBE_TIMEOUT, look).await {
            Ok(Ok(state)) => Probe::State(state),
            Ok(Err(e)) => {
                tracing::error!(error = %e, "the mount probe panicked");
                Probe::State(MountState::NotMounted)
            }
            Err(_) => Probe::Wedged,
        }
    }

    async fn blocking<T: Send + 'static>(
        &self,
        what: &str,
        f: impl FnOnce(&dyn Mounter) -> std::io::Result<T> + Send + 'static,
    ) -> Result<T, Status> {
        let mounter = self.mounter.clone();
        tokio::task::spawn_blocking(move || f(&*mounter))
            .await
            .map_err(|e| Status::internal(format!("{what}: {e}")))?
            .map_err(|e| Status::internal(format!("{what}: {e}")))
    }

    async fn unmount(&self, path: &Path) -> Result<bool, Status> {
        let target = path.to_path_buf();
        self.blocking(&format!("unmounting {}", path.display()), move |m| {
            m.unmount(&target)
        })
        .await
    }

    async fn record_views(&self, engines: &dyn NodeEngines, unit: &str, pod: &str) {
        let serial = self
            .views_annotation
            .lock()
            .unwrap()
            .entry(unit.to_string())
            .or_default()
            .clone();
        let _serial = serial.lock().await;
        let views = self.state.views_of(pod);
        match tokio::time::timeout(self.annotation_timeout, engines.set_view_count(unit, views))
            .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::warn!(pod, views, error = %e, "recording the engine pod's view count")
            }
            Err(_) => tracing::warn!(
                pod,
                views,
                timeout = ?self.annotation_timeout,
                "recording the engine pod's view count timed out; the unit's next stage or \
                 unstage records it"
            ),
        }
    }

    /// The credentials for `unit`'s engine pod (module docs, step 1): a
    /// `refreshing` class's watched Secret, else the request's, else what
    /// this plugin last sent the unit.
    async fn credentials_for(&self, unit: &str, pool: &PoolRef) -> Option<Secrets> {
        let from_request = unlock_params("", &pool.secrets).map(|_| pool.secrets.clone());
        if let (CredentialMode::Refreshing, Some(secret), Some(refresher)) = (
            pool.class.credentials,
            &pool.class.credential_secret,
            &self.refresher,
        ) {
            self.watch_rotations(unit, secret);
            // A request that carries the secret need not wait for the
            // watch's first read; one that does not may.
            let wait = if from_request.is_some() {
                Duration::ZERO
            } else {
                Duration::from_secs(10)
            };
            if let Some(data) = refresher.current(secret, wait).await {
                return Some((*data).clone());
            }
        }
        from_request.or_else(|| self.remembered.lock().unwrap().get(unit).cloned())
    }

    /// Push each change of `secret` to `unit`'s engine pod (once per unit).
    fn watch_rotations(&self, unit: &str, secret: &SecretRef) {
        let (Some(refresher), Some(engines)) = (&self.refresher, &self.engines) else {
            return;
        };
        let mut rotating = self.rotating.lock().unwrap();
        if rotating.contains_key(unit) {
            return;
        }
        let mut latest = refresher.subscribe(secret);
        latest.borrow_and_update();
        let engines = engines.clone();
        let remembered = self.remembered.clone();
        let gates = self.unit_gates.clone();
        let (pushed_to, landed_for) = (unit.to_string(), unit.to_string());
        let task = tokio::spawn(crate::credentials::follow_rotations(
            latest,
            secret.clone(),
            unit.to_string(),
            move |data| {
                let engines = engines.clone();
                let unit = pushed_to.clone();
                let gate = gate_of(&gates, &unit);
                async move {
                    // Never into a handoff: it lands on the pod that serves
                    // once the handoff is over (a rotation sent to the old
                    // pod after the standby took its credentials would be
                    // lost with it).
                    let _gate = gate.read_owned().await;
                    engines.rotate(&unit, &data).await
                }
            },
            move |data| {
                remembered
                    .lock()
                    .unwrap()
                    .insert(landed_for.clone(), data.clone());
            },
        ));
        rotating.insert(unit.to_string(), (secret.clone(), task.abort_handle()));
    }

    fn unit_lock(&self, unit: &str) -> Arc<Mutex<()>> {
        self.unit_locks
            .lock()
            .unwrap()
            .entry(unit.to_string())
            .or_default()
            .clone()
    }

    /// Record `record` and keep what unlocks its unit's engine pod, as one
    /// step with respect to [`Self::forget_credentials`].
    fn record_and_keep(
        &self,
        record: VolumeRecord,
        pool: &PoolRef,
        credentials: Option<Secrets>,
    ) -> Result<(), Status> {
        let unit = record.unit.clone();
        let lock = self.unit_lock(&unit);
        let _held = lock.lock().unwrap();
        self.state
            .put(record)
            .map_err(internal("recording the volume"))?;
        self.keep_credentials(&unit, pool, credentials);
        Ok(())
    }

    /// Keep what unlocks `unit`'s engine pod (`credentials`, and its
    /// Secret's watch for a `refreshing` class) while it serves a volume
    /// of this node: called once a volume of it is recorded.
    fn keep_credentials(&self, unit: &str, pool: &PoolRef, credentials: Option<Secrets>) {
        if let Some(credentials) = credentials {
            self.remembered
                .lock()
                .unwrap()
                .insert(unit.to_string(), credentials);
        }
        if let (CredentialMode::Refreshing, Some(secret)) =
            (pool.class.credentials, &pool.class.credential_secret)
        {
            self.watch_rotations(unit, secret);
        }
    }

    /// Drop what [`Self::keep_credentials`] kept for `unit` once no volume
    /// of this node is staged through it any more: the plugin holds no
    /// credentials it has no use for.
    fn forget_credentials(&self, unit: &str) {
        let lock = self.unit_lock(unit);
        let _held = lock.lock().unwrap();
        if self.state.all().iter().any(|r| r.unit == unit) {
            return;
        }
        self.remembered.lock().unwrap().remove(unit);
        let stopped = self.rotating.lock().unwrap().remove(unit);
        if let Some((secret, task)) = stopped {
            task.abort();
            if let Some(refresher) = &self.refresher {
                refresher.release(&secret);
            }
        }
        tracing::debug!(
            unit,
            "no volume left on the engine pod: its credentials forgotten"
        );
    }

    /// Stage `id` at `staging` (module docs, steps 1-4) and record it.
    /// The caller holds the pool's unit gate ([`Self::shared_gate`]).
    #[allow(clippy::too_many_arguments)]
    async fn stage(
        &self,
        engines: &dyn NodeEngines,
        id: &VolumeId,
        volume_id: &str,
        staging: &Path,
        pool: PoolRef,
        context: BTreeMap<String, String>,
        published: std::collections::BTreeSet<PathBuf>,
    ) -> Result<(), Status> {
        let (unit, _) = engines.names(&pool);
        let staged = self
            .stage_inner(engines, id, volume_id, staging, pool, context, published)
            .await;
        if staged.is_err() {
            // A failed stage must not leave its unit's Secret watch behind.
            self.forget_credentials(&unit);
        }
        staged
    }

    #[allow(clippy::too_many_arguments)]
    async fn stage_inner(
        &self,
        engines: &dyn NodeEngines,
        id: &VolumeId,
        volume_id: &str,
        staging: &Path,
        pool: PoolRef,
        context: BTreeMap<String, String>,
        published: std::collections::BTreeSet<PathBuf>,
    ) -> Result<(), Status> {
        let fs_uuid = id.fs_uuid();
        let (unit, _) = engines.names(&pool);
        let credentials = self.credentials_for(&unit, &pool).await;
        let engine = engines
            .engine(&pool, fs_uuid, credentials.as_ref())
            .await
            .map_err(|e| {
                status(
                    &format!("bringing up the engine pod of {}", pool.prefix()),
                    e,
                )
            })?;

        // 1. The pod serves the filesystem the id names.
        let listing = engine.client.fs_list().await.map_err(|e| {
            if is_awaiting_unlock(&e) {
                Status::unavailable(format!(
                    "engine pod {} waits for its credentials, and this plugin has none for it: \
                     the request carries no node-stage secret, and none was sent to this unit \
                     since the plugin started (a static-ephemeral class's engine pod that \
                     restarted after the plugin did; name the secret in the class's \
                     csi.storage.k8s.io/node-publish-secret-name too, so a publish carries \
                     it, or recreate the pod using the volume, so kubelet stages it again \
                     with its secret)",
                    engine.pod
                ))
            } else {
                status(&format!("fs.list on {}", engine.pod), e)
            }
        })?;
        match listing.filesystems.iter().find(|f| f.name.is_none()) {
            Some(own) if own.uuid == fs_uuid => {}
            Some(own) => {
                return Err(Status::not_found(format!(
                    "volume {volume_id} names filesystem {fs_uuid}, but its volume context \
                     locates s3://{}/{}, which holds filesystem {}",
                    pool.class.bucket,
                    pool.prefix(),
                    own.uuid
                )))
            }
            None => {
                return Err(Status::internal(format!(
                    "engine pod {} reports no filesystem of its own",
                    engine.pod
                )))
            }
        }
        // 2. The volume, and its record as of now.
        let subtree = id.subtree();
        let xattrs = read_record(engine.client.as_ref(), &subtree)
            .await
            .map_err(|s| match s.code() {
                tonic::Code::NotFound => Status::not_found(format!(
                    "volume {volume_id}: {subtree} does not exist in filesystem {fs_uuid}"
                )),
                _ => s,
            })?;

        let mut record = VolumeRecord::new(volume_id, staging);
        record.unit = engine.unit.clone();
        record.pod = engine.pod.clone();
        record.fs_uuid = fs_uuid.to_string();
        record.subtree = subtree.clone();
        record.xattrs = xattrs.clone();
        record.context = context;
        record.published = published;

        // A view already known by this staging path: ours from before a
        // lost record, still served — adopt it; anything else there is
        // stale and goes.
        let views = engine
            .client
            .view_list(ViewListParams::default())
            .await
            .map_err(|e| status(&format!("view.list on {}", engine.pod), e))?;
        if let Some(view) = views
            .views
            .iter()
            .find(|v| Path::new(&v.mountpoint) == staging)
        {
            let ours = view.labels.get(LABEL_VOLUME_ID).map(String::as_str) == Some(volume_id);
            if ours && self.probe(staging).await.served() {
                tracing::info!(volume_id, staging = %staging.display(), pod = %engine.pod,
                    "adopting a view staged before this plugin lost its record");
                self.record_and_keep(record, &pool, credentials)?;
                self.record_views(engines, &engine.unit, &engine.pod).await;
                return Ok(());
            }
            match engine
                .client
                .view_unmount(ViewUnmountParams {
                    mountpoint: staging.to_path_buf(),
                })
                .await
            {
                Ok(_) => {}
                Err(e) if e.kind == ErrorKind::NotFound => {}
                Err(e) => return Err(status("ending a stale view at the staging path", e)),
            }
        }

        // 3. The mount, and the view on it.
        self.unmount(staging).await?;
        let target = staging.to_path_buf();
        let fd = self
            .blocking(
                &format!("mounting FUSE at {}", staging.display()),
                move |m| {
                    std::fs::create_dir_all(&target)?;
                    m.fuse_mount(
                        &target,
                        &FuseMountOptions {
                            fs_name: "constellation".into(),
                            uid: ENGINE_UID,
                            gid: ENGINE_UID,
                        },
                    )
                },
            )
            .await?;
        let mut labels = BTreeMap::from([
            (LABEL_VOLUME_ID.to_string(), volume_id.to_string()),
            ("pool".to_string(), pool_label(&pool)),
            ("shard".to_string(), pool.shard.to_string()),
        ]);
        for (label, key) in [("pv", X_PV), ("pvc", X_PVC), ("namespace", X_NAMESPACE)] {
            if let Some(value) = xattrs.get(key).filter(|v| !v.is_empty()) {
                labels.insert(label.to_string(), value.clone());
            }
        }
        let mounted = engine
            .client
            .view_mount_fd(
                ViewMountParams {
                    subtree,
                    source: MountSource::PreopenedFd {
                        mountpoint: Some(staging.to_path_buf()),
                        opts: MountViewOpts {
                            allow_other: true,
                            fs_name: Some("constellation".into()),
                            ..Default::default()
                        },
                    },
                    labels,
                    qos: Default::default(),
                    // §3 settled decision 16: a PV's mount never links out
                    // of its subtree.
                    confine_links: true,
                },
                fd,
            )
            .await;
        if let Err(e) = mounted {
            // The mount waits for a `FUSE_INIT` nobody will answer; the
            // descriptor is closed, so it is already aborted.
            if let Err(u) = self.unmount(staging).await {
                tracing::warn!(error = %u, "unmounting the failed staging mount");
            }
            return Err(status(&format!("view.mount on {}", engine.pod), e));
        }
        // The engine took the view; its session must answer the mount.
        if let Probe::State(state @ (MountState::Dead | MountState::NotMounted)) =
            self.probe(staging).await
        {
            if let Err(u) = self.unmount(staging).await {
                tracing::warn!(error = %u, "unmounting the dead staging mount");
            }
            let _ = engine
                .client
                .view_unmount(ViewUnmountParams {
                    mountpoint: staging.to_path_buf(),
                })
                .await;
            return Err(Status::internal(format!(
                "engine pod {} accepted the view of {volume_id} but its FUSE session ended at \
                 once (the staging mount is {state:?}); see the pod's log",
                engine.pod
            )));
        }
        // 4. Recorded.
        self.record_and_keep(record, &pool, credentials)?;
        self.record_views(engines, &engine.unit, &engine.pod).await;
        tracing::info!(volume_id, staging = %staging.display(), pod = %engine.pod, "staged");
        Ok(())
    }
}

/// `unit`'s gate in `gates` ([`NodeService::unit_gates`]).
fn gate_of(
    gates: &Mutex<HashMap<String, Arc<tokio::sync::RwLock<()>>>>,
    unit: &str,
) -> Arc<tokio::sync::RwLock<()>> {
    gates
        .lock()
        .unwrap()
        .entry(unit.to_string())
        .or_default()
        .clone()
}

fn internal(what: &'static str) -> impl Fn(std::io::Error) -> Status {
    move |e| Status::internal(format!("{what}: {e}"))
}

/// The access mode of a request's capability: required, a mount (never
/// block), and a known mode — every one of settled decision 14's is served.
fn access_mode(cap: Option<&VolumeCapability>) -> Result<Mode, Status> {
    let cap = cap.ok_or_else(|| Status::invalid_argument("volume_capability is required"))?;
    match cap.access_type {
        Some(AccessType::Mount(_)) => {}
        Some(AccessType::Block(_)) => {
            return Err(Status::invalid_argument(
                "block access is not supported: volumes are filesystem mounts",
            ))
        }
        None => {
            return Err(Status::invalid_argument(
                "a volume capability must set an access type",
            ))
        }
    }
    match cap
        .access_mode
        .as_ref()
        .and_then(|m| Mode::try_from(m.mode).ok())
        .unwrap_or(Mode::Unknown)
    {
        Mode::Unknown => Err(Status::invalid_argument(
            "a volume capability must set a known access mode",
        )),
        mode => Ok(mode),
    }
}

fn reader_only(mode: Mode) -> bool {
    matches!(mode, Mode::SingleNodeReaderOnly | Mode::MultiNodeReaderOnly)
}

/// The pool filesystem a volume lives in, from its context (module docs):
/// the class parameters, with the shard from the id (or, for a static
/// handle, the context's `shard`).
fn location(
    context: &HashMap<String, String>,
    id: &VolumeId,
    secrets: &HashMap<String, String>,
) -> Result<PoolRef, Status> {
    // Prefixed keys are the CO's (`storage.kubernetes.io/csiProvisionerIdentity`
    // from external-provisioner, `csi.storage.k8s.io/*` pod information):
    // no class parameter has a `/`.
    let params: HashMap<String, String> = context
        .iter()
        .filter(|(k, _)| k.as_str() != SHARD_KEY && !k.contains('/'))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let class = ClassParams::parse(&params).map_err(|e| {
        Status::invalid_argument(format!(
            "the volume context does not locate a pool filesystem ({e}); a statically \
             provisioned PersistentVolume names its pool in volumeAttributes (bucket, prefix, \
             endpoint, …, and shard for a sharded pool)"
        ))
    })?;
    let shard = match id {
        VolumeId::Pool { shard, .. } => *shard,
        VolumeId::Dedicated { .. } => 0,
        VolumeId::Static { .. } => match context.get(SHARD_KEY) {
            None => 0,
            Some(s) => s.trim().parse().map_err(|_| {
                Status::invalid_argument(format!("volume context shard {s:?} is not a number"))
            })?,
        },
    };
    if shard >= class.shards {
        return Err(Status::invalid_argument(format!(
            "the volume names shard {shard} of a pool with {} shard(s)",
            class.shards
        )));
    }
    Ok(PoolRef {
        class,
        shard,
        secrets: secrets
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    })
}

/// What of a request's volume context is worth recording: the location,
/// not the CO's prefixed keys (pod information changes per publish).
fn recordable(context: &HashMap<String, String>) -> BTreeMap<String, String> {
    context
        .iter()
        .filter(|(k, _)| !k.contains('/'))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn parse_id(volume_id: &str) -> Result<VolumeId, Status> {
    VolumeId::parse(volume_id).map_err(|e| Status::not_found(e.to_string()))
}

fn required(value: &str, what: &str) -> Result<(), Status> {
    if value.is_empty() {
        return Err(Status::invalid_argument(format!("{what} is required")));
    }
    Ok(())
}

fn to_i64(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

fn health(status: VolumeHealthErrorType, reason: &str, message: String) -> VolumeHealthEntry {
    VolumeHealthEntry {
        status: status as i32,
        reason: reason.to_string(),
        message,
    }
}

#[tonic::async_trait]
impl NodeRpc for NodeService {
    async fn node_stage_volume(
        &self,
        request: Request<NodeStageVolumeRequest>,
    ) -> Result<Response<NodeStageVolumeResponse>, Status> {
        let req = request.into_inner();
        let who = crate::controller::attribution(&req.volume_id);
        constellation_control::client::on_behalf_of(who, async move {
            required(&req.volume_id, "volume_id")?;
            required(&req.staging_target_path, "staging_target_path")?;
            access_mode(req.volume_capability.as_ref())?;
            let id = parse_id(&req.volume_id)?;
            let engines = self.engines()?.clone();
            let _lock = self.locks.try_lock(req.volume_id.clone())?;
            let staging = PathBuf::from(&req.staging_target_path);
            let pool = location(&req.volume_context, &id, &req.secrets)?;
            // Never while the unit's sessions change hands (`rollout`).
            let _gate = self.shared_gate(&engines.names(&pool).0).await;

            let mut published = Default::default();
            if let Some(record) = self.state.get(&req.volume_id) {
                let served = self.probe(&record.staging_path).await.served();
                match (record.staging_path == staging, served) {
                    (true, true) => return Ok(Response::new(NodeStageVolumeResponse {})),
                    (false, true) => {
                        return Err(Status::already_exists(format!(
                            "volume {} is already staged at {}",
                            req.volume_id,
                            record.staging_path.display()
                        )))
                    }
                    // Dead or gone: staged again below, its publishes kept.
                    (same, false) => {
                        if !same {
                            self.unmount(&record.staging_path).await?;
                        }
                        published = record.published;
                    }
                }
            }
            self.stage(
                engines.as_ref(),
                &id,
                &req.volume_id,
                &staging,
                pool,
                recordable(&req.volume_context),
                published,
            )
            .await?;
            Ok(Response::new(NodeStageVolumeResponse {}))
        })
        .await
    }

    async fn node_unstage_volume(
        &self,
        request: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        let req = request.into_inner();
        let who = crate::controller::attribution(&req.volume_id);
        constellation_control::client::on_behalf_of(who, async move {
            required(&req.volume_id, "volume_id")?;
            required(&req.staging_target_path, "staging_target_path")?;
            let _lock = self.locks.try_lock(req.volume_id.clone())?;
            let staging = PathBuf::from(&req.staging_target_path);
            // Never while the unit's sessions change hands (`rollout`).
            let unit = self.state.get(&req.volume_id).map(|r| r.unit);
            let _gate = match unit {
                Some(unit) => Some(self.shared_gate(&unit).await),
                None => None,
            };
            // The plugin made the mount; unmounting it also ends the engine's
            // session on it (the kernel aborts the connection).
            self.unmount(&staging).await?;
            let Some(record) = self
                .state
                .get(&req.volume_id)
                .filter(|r| r.staging_path == staging)
            else {
                return Ok(Response::new(NodeUnstageVolumeResponse {}));
            };
            if let Some(engines) = &self.engines {
                // And the view goes from the engine — at once, ended or not.
                match engines.existing(&record.unit).await {
                    Ok(Some(engine)) => match engine
                        .client
                        .view_unmount(ViewUnmountParams {
                            mountpoint: staging.clone(),
                        })
                        .await
                    {
                        Ok(_) => {}
                        Err(e) if e.kind == ErrorKind::NotFound => {}
                        Err(e) => return Err(status(&format!("view.unmount on {}", engine.pod), e)),
                    },
                    // A pod that is gone took its views with it.
                    Ok(None) => {}
                    Err(e) => tracing::warn!(pod = %record.pod, error = %e,
                        "the engine pod is unreachable; its view of the unstaged volume goes with it"),
                }
                self.state
                    .remove(&req.volume_id)
                    .map_err(internal("removing the volume's record"))?;
                self.forget_credentials(&record.unit);
                self.record_views(engines.as_ref(), &record.unit, &record.pod)
                    .await;
            } else {
                self.state
                    .remove(&req.volume_id)
                    .map_err(internal("removing the volume's record"))?;
            }
            tracing::info!(volume_id = %req.volume_id, staging = %staging.display(), "unstaged");
            Ok(Response::new(NodeUnstageVolumeResponse {}))
        })
        .await
    }

    async fn node_publish_volume(
        &self,
        request: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        let req = request.into_inner();
        let who = crate::controller::attribution(&req.volume_id);
        constellation_control::client::on_behalf_of(who, async move {
            required(&req.volume_id, "volume_id")?;
            required(&req.target_path, "target_path")?;
            let mode = access_mode(req.volume_capability.as_ref())?;
            required(&req.staging_target_path, "staging_target_path")?;
            let id = parse_id(&req.volume_id)?;
            let _lock = self.locks.try_lock(req.volume_id.clone())?;
            let staging = PathBuf::from(&req.staging_target_path);
            let target = PathBuf::from(&req.target_path);
            let mut record = self
                .state
                .get(&req.volume_id)
                .filter(|r| r.staging_path == staging)
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "volume {} is not staged at {}",
                        req.volume_id,
                        staging.display()
                    ))
                })?;
            // Never while the unit's sessions change hands (`rollout`): the
            // staging mount's probe would wait for it anyway.
            let _gate = self.shared_gate(&record.unit).await;
            if mode == Mode::SingleNodeSingleWriter {
                if let Some(other) = record.published.iter().find(|p| **p != target) {
                    return Err(Status::failed_precondition(format!(
                        "volume {} is single-writer and already published at {}",
                        req.volume_id,
                        other.display()
                    )));
                }
            }

            // Settled decision 12: every publish checks the staging mount.
            let staged = match self.probe(&staging).await {
                Probe::State(MountState::Alive { dev, .. }) => Some(dev),
                Probe::Wedged => None,
                Probe::State(state) => {
                    tracing::warn!(volume_id = %req.volume_id, ?state,
                        "the staging mount is not served (its engine pod died?): restaging");
                    let engines = self.engines()?.clone();
                    let context: HashMap<String, String> =
                        if recordable(&req.volume_context).is_empty() {
                            record.context.clone().into_iter().collect()
                        } else {
                            req.volume_context.clone()
                        };
                    // The publish secret, when the class names one
                    // (`csi.storage.k8s.io/node-publish-secret-name`):
                    // what unlocks a replacement engine pod after this
                    // plugin restarted and forgot the stage secret.
                    let pool = location(&context, &id, &req.secrets)?;
                    self.stage(
                        engines.as_ref(),
                        &id,
                        &req.volume_id,
                        &staging,
                        pool,
                        recordable(&context),
                        record.published.clone(),
                    )
                    .await?;
                    record = self
                        .state
                        .get(&req.volume_id)
                        .ok_or_else(|| Status::internal("the restaged volume has no record"))?;
                    match self.probe(&staging).await {
                        Probe::State(MountState::Alive { dev, .. }) => Some(dev),
                        _ => None,
                    }
                }
            };

            let read_only = req.readonly || reader_only(mode);
            match (self.probe(&target).await, staged) {
                // Already this volume's bind (the same device as the staging
                // mount), as asked: nothing to do. Bound the other way round
                // (read-only, asked read-write, or the reverse): the CSI spec's
                // incompatible republish.
                (Probe::State(MountState::Alive { dev, read_only: ro }), Some(staged))
                    if dev == staged =>
                {
                    if ro != read_only {
                        return Err(Status::already_exists(format!(
                            "volume {} is already published at {} {}, not {}",
                            req.volume_id,
                            target.display(),
                            if ro { "read-only" } else { "read-write" },
                            if read_only { "read-only" } else { "read-write" },
                        )));
                    }
                }
                (Probe::State(MountState::NotMounted), _) => {
                    self.bind(&staging, &target, read_only).await?;
                }
                (Probe::Wedged, None) => {
                    return Err(Status::unavailable(format!(
                        "the staging mount {} does not answer",
                        staging.display()
                    )))
                }
                // A dead bind of an earlier session, or something else.
                _ => {
                    self.unmount(&target).await?;
                    self.bind(&staging, &target, read_only).await?;
                }
            }
            if record.published.insert(target.clone()) {
                self.state
                    .put(record)
                    .map_err(internal("recording the publish"))?;
            }
            Ok(Response::new(NodePublishVolumeResponse {}))
        })
        .await
    }

    async fn node_unpublish_volume(
        &self,
        request: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let req = request.into_inner();
        let who = crate::controller::attribution(&req.volume_id);
        constellation_control::client::on_behalf_of(who, async move {
            required(&req.volume_id, "volume_id")?;
            required(&req.target_path, "target_path")?;
            let _lock = self.locks.try_lock(req.volume_id.clone())?;
            let target = PathBuf::from(&req.target_path);
            // Never while the unit's sessions change hands (`rollout`).
            let unit = self.state.get(&req.volume_id).map(|r| r.unit);
            let _gate = match unit {
                Some(unit) => Some(self.shared_gate(&unit).await),
                None => None,
            };
            self.unmount(&target).await?;
            // The CSI spec: the plugin removes what it created at the path.
            match std::fs::remove_dir(&target) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(Status::internal(format!(
                        "removing {}: {e}",
                        target.display()
                    )))
                }
            }
            if let Some(mut record) = self.state.get(&req.volume_id) {
                if record.published.remove(&target) {
                    self.state
                        .put(record)
                        .map_err(internal("recording the unpublish"))?;
                }
            }
            Ok(Response::new(NodeUnpublishVolumeResponse {}))
        })
        .await
    }

    async fn node_get_volume_stats(
        &self,
        request: Request<NodeGetVolumeStatsRequest>,
    ) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        let req = request.into_inner();
        let who = crate::controller::attribution(&req.volume_id);
        constellation_control::client::on_behalf_of(who, async move {
            required(&req.volume_id, "volume_id")?;
            required(&req.volume_path, "volume_path")?;
            let record = self.state.get(&req.volume_id).ok_or_else(|| {
                Status::not_found(format!("volume {} is not staged here", req.volume_id))
            })?;
            // Never while the unit's sessions change hands (`rollout`).
            let _gate = self.shared_gate(&record.unit).await;
            let path = PathBuf::from(&req.volume_path);
            if path != record.staging_path && !record.published.contains(&path) {
                return Err(Status::not_found(format!(
                    "volume {} is not mounted at {}",
                    req.volume_id,
                    path.display()
                )));
            }
            let engines = self.engines()?;
            let engine = engines
                .existing(&record.unit)
                .await
                .map_err(|e| status(&format!("reaching {}", record.pod), e))?
                .ok_or_else(|| {
                    Status::unavailable(format!(
                        "engine pod {} is not running; the volume is restaged on its next publish",
                        record.pod
                    ))
                })?;
            let stats = engine
                .client
                .view_stats(ViewStatsParams {
                    id: None,
                    mountpoint: Some(record.staging_path.clone()),
                })
                .await
                .map_err(|e| status(&format!("view.stats on {}", engine.pod), e))?;
            let cap = match engine.client.quota_cap(&record.subtree).await {
                Ok(cap) => cap,
                Err(e) if e.kind == ErrorKind::NotFound => None,
                Err(e) => return Err(status(&format!("quota.get {}", record.subtree), e)),
            };
            // §11: capacity is the subtree's quota; without one, what the
            // view's statfs reports.
            let used = stats.rsize;
            let (total, available) = match cap {
                Some(cap) => (cap, cap.saturating_sub(used)),
                None => (stats.total_bytes, stats.available_bytes),
            };
            let inodes_total = stats.inodes_total.max(stats.rcount);
            Ok(Response::new(NodeGetVolumeStatsResponse {
                usage: vec![
                    VolumeUsage {
                        available: to_i64(available),
                        total: to_i64(total),
                        used: to_i64(used),
                        unit: Unit::Bytes as i32,
                    },
                    VolumeUsage {
                        available: to_i64(inodes_total.saturating_sub(stats.rcount)),
                        total: to_i64(inodes_total),
                        used: to_i64(stats.rcount),
                        unit: Unit::Inodes as i32,
                    },
                ],
            }))
        })
        .await
    }

    async fn node_get_volume_health(
        &self,
        request: Request<NodeGetVolumeHealthRequest>,
    ) -> Result<Response<NodeGetVolumeHealthResponse>, Status> {
        let req = request.into_inner();
        required(&req.volume_id, "volume_id")?;
        let record = self.state.get(&req.volume_id).ok_or_else(|| {
            Status::not_found(format!("volume {} is not staged here", req.volume_id))
        })?;
        // Never while the unit's sessions change hands (`rollout`).
        let _gate = self.shared_gate(&record.unit).await;
        let mut statuses = Vec::new();
        match self.probe(&record.staging_path).await {
            Probe::State(MountState::Alive { .. }) => {}
            Probe::State(MountState::Dead) => statuses.push(health(
                VolumeHealthErrorType::Inaccessible,
                "EngineGone",
                format!(
                    "the staging mount {} lost its engine pod ({}); the next publish restages it",
                    record.staging_path.display(),
                    record.pod
                ),
            )),
            Probe::State(MountState::NotMounted) => statuses.push(health(
                VolumeHealthErrorType::Inaccessible,
                "NotStaged",
                format!("nothing is mounted at {}", record.staging_path.display()),
            )),
            Probe::Wedged => statuses.push(health(
                VolumeHealthErrorType::Degraded,
                "Unresponsive",
                format!(
                    "the staging mount {} did not answer within {PROBE_TIMEOUT:?}",
                    record.staging_path.display()
                ),
            )),
        }
        if !req.volume_publish_path.is_empty()
            && self.probe(Path::new(&req.volume_publish_path)).await
                == Probe::State(MountState::NotMounted)
        {
            statuses.push(health(
                VolumeHealthErrorType::Inaccessible,
                "NotPublished",
                format!("nothing is mounted at {}", req.volume_publish_path),
            ));
        }
        let engine = match &self.engines {
            Some(engines) => engines.existing(&record.unit).await.ok().flatten(),
            None => None,
        };
        match engine {
            None => statuses.push(health(
                VolumeHealthErrorType::Inaccessible,
                "EngineUnavailable",
                format!("engine pod {} is not running", record.pod),
            )),
            // Settled decision 18: the volume changed behind the driver's
            // back. Detected and reported, never enforced.
            Some(engine) => match read_record(engine.client.as_ref(), &record.subtree).await {
                Err(s) if s.code() == tonic::Code::NotFound => statuses.push(health(
                    VolumeHealthErrorType::DataLoss,
                    "VolumeRemoved",
                    format!(
                        "{} no longer exists in filesystem {}: it was removed outside \
                         Kubernetes",
                        record.subtree, record.fs_uuid
                    ),
                )),
                Err(s) => statuses.push(health(
                    VolumeHealthErrorType::Degraded,
                    "RecordUnreadable",
                    format!("reading {}'s record: {}", record.subtree, s.message()),
                )),
                Ok(now) if now != record.xattrs => {
                    let changed: Vec<&str> = record
                        .xattrs
                        .keys()
                        .chain(now.keys())
                        .filter(|k| record.xattrs.get(*k) != now.get(*k))
                        .map(String::as_str)
                        .collect::<std::collections::BTreeSet<_>>()
                        .into_iter()
                        .collect();
                    statuses.push(health(
                        VolumeHealthErrorType::Degraded,
                        "VolumeRecordChanged",
                        format!(
                            "{}'s volume record changed since it was staged ({}): edited \
                             outside Kubernetes",
                            record.subtree,
                            changed.join(", ")
                        ),
                    ))
                }
                Ok(_) => {}
            },
        }
        Ok(Response::new(NodeGetVolumeHealthResponse {
            volume_health: Some(VolumeHealth {
                volume_id: req.volume_id,
                health_statuses: statuses,
            }),
        }))
    }

    async fn node_get_storage_health(
        &self,
        _request: Request<NodeGetStorageHealthRequest>,
    ) -> Result<Response<NodeGetStorageHealthResponse>, Status> {
        Err(Status::unimplemented(
            "NodeGetStorageHealth is not supported (GET_STORAGE_HEALTH is not advertised)",
        ))
    }

    /// Settled decision 6: the quota is metadata, so there is nothing to
    /// grow on a node (`EXPAND_VOLUME` is not advertised here; this answers
    /// a CO that calls anyway).
    async fn node_expand_volume(
        &self,
        request: Request<NodeExpandVolumeRequest>,
    ) -> Result<Response<NodeExpandVolumeResponse>, Status> {
        let req = request.into_inner();
        required(&req.volume_id, "volume_id")?;
        required(&req.volume_path, "volume_path")?;
        if self.state.get(&req.volume_id).is_none() {
            return Err(Status::not_found(format!(
                "volume {} is not staged here",
                req.volume_id
            )));
        }
        Ok(Response::new(NodeExpandVolumeResponse {
            capacity_bytes: req
                .capacity_range
                .map(|r| r.required_bytes)
                .unwrap_or_default(),
        }))
    }

    async fn node_get_capabilities(
        &self,
        _request: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        // §5, plus SINGLE_NODE_MULTI_WRITER (settled decision 14: RWOP is
        // SINGLE_NODE_SINGLE_WRITER, enforced at publish) and
        // GET_VOLUME_HEALTH (the volume condition, module docs). Not
        // EXPAND_VOLUME (settled decision 6), not VOLUME_MOUNT_GROUP.
        let capabilities = [
            RpcType::StageUnstageVolume,
            RpcType::GetVolumeStats,
            RpcType::SingleNodeMultiWriter,
            RpcType::GetVolumeHealth,
        ]
        .into_iter()
        .map(|t| NodeServiceCapability {
            r#type: Some(CapabilityType::Rpc(Rpc { r#type: t as i32 })),
        })
        .collect();
        Ok(Response::new(NodeGetCapabilitiesResponse { capabilities }))
    }

    async fn node_get_info(
        &self,
        _request: Request<NodeGetInfoRequest>,
    ) -> Result<Response<NodeGetInfoResponse>, Status> {
        // No topology: every node reaches S3 (plan 37 §4), so volumes carry
        // no node affinity. max_volumes_per_node 0 = the CO decides.
        Ok(Response::new(NodeGetInfoResponse {
            node_id: self.node_id.clone(),
            ..Default::default()
        }))
    }
}

impl NodeService {
    async fn bind(&self, staging: &Path, target: &Path, read_only: bool) -> Result<(), Status> {
        let (source, target) = (staging.to_path_buf(), target.to_path_buf());
        self.blocking(
            &format!("bind-mounting {} at {}", source.display(), target.display()),
            move |m| m.bind_mount(&source, &target, read_only),
        )
        .await
    }
}

#[cfg(test)]
mod tests;
