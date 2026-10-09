//! [`EnginePodManager`]: the controller-owned engine pods of plan 37
//! §"Engine-pod lifecycle", and how the controller reaches them — the real
//! [`Engines`] behind the Controller service.
//!
//! **One pod per (pool, shard).** `CreateVolume` can arrive for a pool no
//! node has mounted, so the controller keeps exactly one engine pod per
//! pool filesystem itself: `constellation-engine-<pool>[-shard-<k>]-controller`,
//! a bare `Pod` running `constellation serve` from the driver's own image,
//! **not** `nodeName`-pinned (the scheduler places it), unprivileged
//! (`runAsNonRoot`, no capabilities, `RuntimeDefault` seccomp, read-only
//! root), `restartPolicy: OnFailure`. `<pool>` is a DNS label derived from
//! the pool's S3 location — a readable slug of the class prefix and 40 bits
//! of BLAKE3 over `(endpoint, bucket, class prefix)`, the same for every
//! shard of the pool, which `-shard-<k>` then tells apart — so every
//! controller replica, before and after a restart, names the same pod for
//! the same filesystem, and two classes sharing a bucket but not a prefix
//! never share a pod. `constellation.replicix.com/pool` carries `<pool>` (one value
//! per pool, §7) and `constellation.replicix.com/shard` the shard. Its labels say what it is (`app.kubernetes.io/component:
//! engine`, `constellation.replicix.com/pool`, `constellation.replicix.com/shard`,
//! `constellation.replicix.com/owner: controller`) and, once the controller has
//! asked it, which filesystem it serves (`constellation.replicix.com/fs-uuid`), which
//! is how `DeleteVolume`/`ControllerExpandVolume` — knowing only the uuid in
//! `volume_id` — find it again. Its `ownerReference` is the controller
//! `Deployment` (when the chart names it), so uninstalling the driver
//! garbage-collects the pods, while a controller *pod* restart or a leader
//! change leaves them running. A pool's pod is reaped once the pool is
//! empty, and replaced when its engine settings drift, by the purge worker
//! ([`crate::purge`], 37-k6b; the [`crate::purge::PurgeBackend`] impl
//! below); either way it leaves the pool's registry before it is deleted.
//!
//! **No hostPath, no init container.** The controller-owned pod never
//! holds a view and nobody but the controller reaches it (through the exec
//! relay below), so its state dir (the meta store) and its control socket
//! are `emptyDir`s: they survive a container restart (`OnFailure`), a
//! rescheduled pod starts empty and rejoins as a fresh Constellation node
//! that replays the pool's log — harmless for a node that never serves a
//! view. That leaves nothing for a root init container to `chown`, and
//! makes the pod PodSecurity-`restricted` as a whole (plan 37 §9, K6a):
//! non-root, every capability dropped, no privilege escalation, read-only
//! root, `RuntimeDefault` seccomp, only `emptyDir` volumes. (A node-owned
//! pod needs hostPaths, which no PodSecurity level below `privileged`
//! admits; its node plugin makes them, owned by the engine's uid —
//! [`NodeEnginePods`].)
//!
//! **Readiness** is the daemon's own: an exec probe running
//! `constellation control-relay --ping` (one `node.ping` over the socket).
//! The manager waits for the pod's `Ready` condition with exponential
//! backoff (200 ms doubling to 3 s, bounded by
//! `CONSTELLATION_CSI_ENGINE_READY_TIMEOUT_S`, default 100 s, under the
//! sidecars' 120 s RPC timeout so the error names the pod's waiting reason); a pod that
//! has terminated (phase `Failed`/`Succeeded`) is deleted and recreated from
//! its own spec.
//!
//! **Reaching the pod: an exec relay, not a Service.** The control protocol
//! has no network transport — only the unix socket, whose peer credentials
//! *are* its authentication — and the controller cannot reach a hostPath
//! socket on another node. The manager therefore runs `constellation
//! control-relay --socket …` inside the engine container through the
//! Kubernetes exec API and speaks the control protocol over that stream
//! ([`StreamTransport`]). The authorization boundary is Kubernetes RBAC
//! (`create` on `pods/exec`, namespace-scoped, granted only to the
//! controller's ServiceAccount); the daemon sees the relay as a local
//! process of its own uid. No listener is opened on the pod network, so
//! nothing else in the cluster can talk to an engine pod. One long-lived
//! relay per pod is kept and multiplexes every call; a dead one is
//! re-dialled on the next RPC.
//!
//! **A pod the controller has no spec for** (the review's lost-pod case:
//! after `helm uninstall` + reinstall the owner GC took the pods and their
//! Secrets, or a controller restart forgot the spec of a pod someone then
//! deleted). `DeleteVolume`/`ControllerExpandVolume` know only the
//! filesystem uuid, so [`EnginePodManager::rebuild_pool`] finds a
//! `PersistentVolume` of this driver whose handle names that uuid, reads
//! its `StorageClass`, and rebuilds the pool exactly as `CreateVolume`
//! would ([`pool_from_pv`]): class parameters, the shard from the handle,
//! and the provisioner secret named by the class (its `${pvc.namespace}`,
//! `${pvc.name}` and `${pv.name}` templates resolved from the PV) — read
//! with the controller's own ServiceAccount, so only from the driver's
//! namespace or a Secret the chart lets it watch (failing that the pod
//! comes up and waits, and the RPC is `UNAVAILABLE` until one carries the
//! secret). The rebuilt pod must turn out to serve that uuid. That needs `list` on
//! `persistentvolumes` and `get` on `storageclasses` (the chart's
//! controller ClusterRole).
//!
//! **Deletes start nothing they do not need** (37-k4, the 37-k3b review's
//! bug: a `DeleteVolume` the provisioner repeated after the PV was gone
//! recreated the pool's pod from its remembered spec). [`Engines::running`]
//! reaches a pod only if it exists and is neither terminating nor
//! terminated, never creating, respawning or rebuilding one;
//! [`Engines::named`] lists `PersistentVolume`s / `VolumeSnapshotContent`s
//! for the handle (`list` on both, the chart's controller ClusterRole); and
//! [`Engines::retire`] deletes a pod a delete started for itself, unless
//! another RPC holds a client into it by then (every client shares the
//! cached relay, so the relay map's reference is then the only one).
//!
//! **Credentials** ([`crate::credentials`]). Nothing in a pod spec: an
//! engine pod of a class that needs credentials runs `constellation serve
//! --await-unlock` and waits for them on its control socket. The manager
//! sends `fs.unlock` (naming the pod's `--s3` URL) with the request's
//! secrets (`req.secrets`: the class's provisioner, controller-expand or
//! snapshotter secret, resolved by the sidecars) whenever it reaches a new
//! incarnation of the pod — over a one-shot relay, since the gate answers
//! nothing else and closes its connections once the engine runs — and
//! again on the live relay when a request brings different secrets (a
//! rotation, in place). It keeps the last secrets per pod in memory to
//! unlock a replacement no request has secrets for yet; for a
//! `refreshing` class it also watches the class's Secret and pushes every
//! change at once. A pod it can unlock with nothing is `UNAVAILABLE`,
//! saying so. A class on `aws-default-chain` (and not E2E) starts on the
//! SDK's chain and is never unlocked. The endpoint and region come from
//! the class as `AWS_ENDPOINT`/`AWS_REGION` (not secrets), which is why
//! `fs.create` through the pod must not name them again (the daemon
//! refuses an `fs.create` that does). The controller therefore needs no
//! write access to Secrets at all — only what external-provisioner itself
//! reads, and `get`/`list`/`watch` on the Secrets `refreshing` classes
//! name (the chart's `credentials.watchedSecrets`).

use crate::control_client::{ControlClient, Engines, Handle, PoolRef, SocketControlClient};
use crate::credentials::{fingerprint, is_awaiting_unlock, unlock_params, Refresher, Secrets};
use crate::node::{Drift, NodeEngine, NodeEngines, Replacement};
use crate::params::{ClassParams, CredentialMode, SecretRef};
use crate::volume_id::VolumeId;
use async_trait::async_trait;
use constellation_control::fd::OwnedFd;
use constellation_control::methods::FsList;
use constellation_control::proto::types::{
    Ack, CloneParams, DeleteParams as BrowseDeleteParams, DirectoryListing, FileStat,
    FsCreateParams, FsCreated, FsListing, FsUnlockParams, HandoffParams, HandoffReport,
    LeaveParams, MkdirParams, PeerListing, Pong, QuotaStatus, RenameParams, SnapshotCreateParams,
    SnapshotCreated, SnapshotDeleteParams, SnapshotHeld, SnapshotHoldParams, SnapshotListParams,
    SnapshotListing, ViewInfo, ViewListParams, ViewListing, ViewMountParams, ViewStatsParams,
    ViewStatsReport, ViewUnmountParams, XattrParams, XattrResult,
};
use constellation_control::proto::{ControlError, ErrorKind};
use constellation_control::transport::StreamTransport;
use constellation_control::{Client, ClientOptions, Principal};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment};
use k8s_openapi::api::core::v1::{
    Capabilities, ConfigMap, Container, EmptyDirVolumeSource, EnvVar, ExecAction,
    HostPathVolumeSource, PersistentVolume, Pod, PodSecurityContext, PodSpec, Probe,
    ResourceRequirements, SeccompProfile, Secret, SecurityContext, Toleration, Volume, VolumeMount,
};
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::api::{
    Api, AttachParams, DeleteParams, ListParams, Patch, PatchParams, PostParams, Preconditions,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncBufReadExt;

mod controller_pods;
pub use controller_pods::{
    hold_key, live_holds, retiring_by, PoolRecord, ANNOTATION_HELD_PREFIX, ANNOTATION_RETIRING,
    POOLS_CONFIGMAP,
};
use controller_pods::{Held, Hold, HoldState};

/// The engine container's name in every engine pod.
pub const ENGINE_CONTAINER: &str = "engine";
/// Where the engine binary lives in the image.
const ENGINE_BIN: &str = "/usr/local/bin/constellation";
/// The control socket inside the engine container (the sockets hostPath
/// is mounted on its directory).
pub const POD_SOCKET: &str = "/run/constellation-csi/control.sock";
const POD_SOCKET_DIR: &str = "/run/constellation-csi";
const POD_STATE_DIR: &str = "/var/lib/constellation/state";
/// The engine's uid/gid (the image's non-root user).
const ENGINE_UID: i64 = 65532;

pub const LABEL_COMPONENT: &str = "app.kubernetes.io/component";
pub const LABEL_POOL: &str = "constellation.replicix.com/pool";
pub const LABEL_SHARD: &str = "constellation.replicix.com/shard";
pub const LABEL_OWNER: &str = "constellation.replicix.com/owner";
pub const LABEL_FS_UUID: &str = "constellation.replicix.com/fs-uuid";
pub const LABEL_NODE: &str = "constellation.replicix.com/node";
/// A node-owned pod's `<unit>`: what its name, hostPaths and credentials
/// `Secret` are derived from, which the node half of the chart's
/// pod-access policy holds them to.
pub const LABEL_UNIT: &str = "constellation.replicix.com/unit";
/// §7: how many views a node-owned engine pod serves, as its node plugin
/// last counted them (the idle GC, 37-k6b, reads it), and since when it
/// has served none.
pub const ANNOTATION_VIEWS: &str = "constellation.replicix.com/last-view-count";
pub const ANNOTATION_IDLE_SINCE: &str = "constellation.replicix.com/idle-since";
/// Plan 37 §8: a node-owned pod's place in its unit's chain of
/// replacements (`0` for the first). Its name and its sockets follow from
/// it ([`node_pod_name_gen`], [`control_socket_file`]).
pub const LABEL_GENERATION: &str = "constellation.replicix.com/generation";
/// The fingerprint of the engine settings a node-owned pod was created
/// from ([`engine_config_fingerprint`]): a pod whose fingerprint is not
/// the plugin's own drifted, and is rolled (`crate::node::rollout`).
pub const ANNOTATION_ENGINE_CONFIG: &str = "constellation.replicix.com/engine-config";
/// Why a rollout gave up on this pod (§8 "Failure handling").
pub const ANNOTATION_HANDOFF_FALLBACK: &str = "constellation.replicix.com/handoff-fallback";
const ANNOTATION_S3: &str = "constellation.replicix.com/s3";
const ANNOTATION_ENDPOINT: &str = "constellation.replicix.com/endpoint";

/// How engine pods are made. Read from the controller's environment
/// ([`Self::from_env`]); the chart sets every variable.
#[derive(Debug, Clone, PartialEq)]
pub struct EnginePodConfig {
    /// The driver's namespace: engine pods and their secrets live here.
    pub namespace: String,
    pub image: String,
    pub image_pull_policy: Option<String>,
    /// hostPath root of §7's layout.
    pub host_root: String,
    /// The controller `Deployment` its engine pods are owned by.
    pub owner_deployment: Option<String>,
    /// The node plugin `DaemonSet` its engine pods are owned by.
    pub owner_daemonset: Option<String>,
    pub resources: Option<ResourceRequirements>,
    pub log_level: String,
    pub ready_timeout: Duration,
    pub service_account: Option<String>,
}

impl EnginePodConfig {
    /// `CONSTELLATION_CSI_NAMESPACE` (required), `CONSTELLATION_CSI_ENGINE_IMAGE`
    /// (required), `CONSTELLATION_CSI_ENGINE_PULL_POLICY`,
    /// `CONSTELLATION_CSI_HOST_ROOT` (default `/var/lib/constellation-csi`),
    /// `CONSTELLATION_CSI_OWNER_DEPLOYMENT`, `CONSTELLATION_CSI_OWNER_DAEMONSET`,
    /// `CONSTELLATION_CSI_ENGINE_RESOURCES` (a JSON `ResourceRequirements`),
    /// `CONSTELLATION_CSI_ENGINE_LOG_LEVEL` (default `info`),
    /// `CONSTELLATION_CSI_ENGINE_READY_TIMEOUT_S` (default 100),
    /// `CONSTELLATION_CSI_ENGINE_SERVICE_ACCOUNT`.
    pub fn from_env() -> Result<EnginePodConfig, String> {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty());
        let required = |k: &str| var(k).ok_or_else(|| format!("{k} must be set"));
        let resources = match var("CONSTELLATION_CSI_ENGINE_RESOURCES") {
            Some(json) => Some(
                serde_json::from_str(&json)
                    .map_err(|e| format!("CONSTELLATION_CSI_ENGINE_RESOURCES: {e}"))?,
            ),
            None => None,
        };
        let ready_timeout = match var("CONSTELLATION_CSI_ENGINE_READY_TIMEOUT_S") {
            Some(v) => Duration::from_secs(v.trim().parse().map_err(|_| {
                format!("CONSTELLATION_CSI_ENGINE_READY_TIMEOUT_S={v:?} must be seconds")
            })?),
            // Below the sidecars' 120 s RPC timeout (the chart's
            // `sidecars.timeout`), so the RPC reports why the pod is not
            // ready before the sidecar gives up on it.
            None => Duration::from_secs(100),
        };
        Ok(EnginePodConfig {
            namespace: required("CONSTELLATION_CSI_NAMESPACE")?,
            image: required("CONSTELLATION_CSI_ENGINE_IMAGE")?,
            image_pull_policy: var("CONSTELLATION_CSI_ENGINE_PULL_POLICY"),
            host_root: var("CONSTELLATION_CSI_HOST_ROOT")
                .unwrap_or_else(|| "/var/lib/constellation-csi".into()),
            owner_deployment: var("CONSTELLATION_CSI_OWNER_DEPLOYMENT"),
            owner_daemonset: var("CONSTELLATION_CSI_OWNER_DAEMONSET"),
            resources,
            log_level: var("CONSTELLATION_CSI_ENGINE_LOG_LEVEL").unwrap_or_else(|| "info".into()),
            ready_timeout,
            service_account: var("CONSTELLATION_CSI_ENGINE_SERVICE_ACCOUNT"),
        })
    }
}

/// `<slug>-<10 hex>`: the pool part of the pod name, shared by every
/// shard (module docs).
pub fn pool_label(pool: &PoolRef) -> String {
    let class = &pool.class;
    let mut hasher = blake3::Hasher::new();
    for part in [
        class.endpoint.as_deref().unwrap_or(""),
        &class.bucket,
        &class.prefix,
    ] {
        hasher.update(part.as_bytes());
        hasher.update(&[0]);
    }
    let hash = hasher.finalize().to_hex();
    let base = class
        .prefix
        .rsplit('/')
        .find(|c| !c.is_empty())
        .unwrap_or(&class.bucket);
    let slug: String = base
        .chars()
        .map(|c| c.to_ascii_lowercase())
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .take(16)
        .collect();
    let slug = slug.trim_matches('-');
    let slug = if slug.is_empty() { "pool" } else { slug };
    format!("{slug}-{}", &hash[..10])
}

/// `<pool>[-shard-<k>]`: the per-filesystem unit.
pub fn unit_name(pool: &PoolRef) -> String {
    let label = pool_label(pool);
    if pool.class.shards > 1 {
        format!("{label}-shard-{}", pool.shard)
    } else {
        label
    }
}

/// `constellation-engine-<unit>-controller`.
pub fn pod_name(pool: &PoolRef) -> String {
    format!("constellation-engine-{}-controller", unit_name(pool))
}

/// `constellation-engine-<unit>-<node>`, a pod name (a DNS subdomain, at
/// most 253 bytes): node names may be 253 bytes themselves, so a longer
/// one keeps its head and gets a stable BLAKE3 suffix of the whole name.
/// The unit (at most ~45 bytes) is always kept whole.
pub fn node_pod_name(unit: &str, node: &str) -> String {
    const MAX: usize = 253;
    let full = format!("constellation-engine-{unit}-{node}");
    if full.len() <= MAX {
        return full;
    }
    let hash = blake3::hash(full.as_bytes()).to_hex();
    let head = full[..MAX - 11].trim_end_matches(['-', '.']);
    format!("{head}-{}", &hash[..10])
}

/// The prefix of every node-owned engine pod's hostname
/// ([`node_engine_hostname`]).
pub const NODE_ENGINE_HOST_PREFIX: &str = "csi-node-";

/// A node-owned engine pod's hostname (`spec.hostname`): what its registry
/// record carries, so the controller's registry sweep (`crate::purge`) can
/// tell a record of a node that is gone from one of a node that is not.
/// The default hostname, the pod name cut to 63 bytes, loses the node in
/// most names. `csi-node-<readable head of the node>-<10 hex of BLAKE3 of
/// it>`: a DNS label (at most 56 bytes), the same for every pod of every
/// unit on the node.
pub fn node_engine_hostname(node: &str) -> String {
    let head: String = node
        .chars()
        .map(|c| c.to_ascii_lowercase())
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .take(35)
        .collect();
    let head = head.trim_matches('-');
    let hash = blake3::hash(node.as_bytes()).to_hex();
    if head.is_empty() {
        format!("{NODE_ENGINE_HOST_PREFIX}{}", &hash[..10])
    } else {
        format!("{NODE_ENGINE_HOST_PREFIX}{head}-{}", &hash[..10])
    }
}

/// The hostname kubelet gives a pod named `name` that sets none: the name,
/// cut to 63 bytes, without a trailing `-` or `.`.
pub fn pod_hostname(name: &str) -> String {
    let cut = &name[..name.len().min(63)];
    cut.trim_end_matches(['-', '.']).to_string()
}

/// Who owns an engine pod and where it runs (plan 37 §7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineRole {
    /// `constellation-engine-<unit>-controller`: the controller's, placed by
    /// the scheduler, never serving a view (this module's docs).
    Controller,
    /// `constellation-engine-<unit>-<node>`: a node plugin's, pinned to
    /// `node`, serving that node's staged volumes of the unit
    /// ([`NodeEnginePods`]).
    Node { node: String },
}

impl EngineRole {
    /// The pod's name for `pool`.
    pub fn pod_name(&self, pool: &PoolRef) -> String {
        match self {
            EngineRole::Controller => pod_name(pool),
            EngineRole::Node { node } => node_pod_name(&unit_name(pool), node),
        }
    }

    fn owner_label(&self) -> &'static str {
        match self {
            EngineRole::Controller => "controller",
            EngineRole::Node { .. } => "node",
        }
    }
}

/// A Kubernetes label value for `s`: itself when it is one (≤ 63 bytes of
/// `[A-Za-z0-9._-]`, alphanumeric at both ends), else a readable prefix
/// and 10 hex digits of BLAKE3 — node names are DNS subdomains, up to 253
/// bytes.
pub fn label_value(s: &str) -> String {
    let valid = s.len() <= 63
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        && s.bytes().next().is_none_or(|b| b.is_ascii_alphanumeric())
        && s.bytes().last().is_none_or(|b| b.is_ascii_alphanumeric());
    if valid {
        return s.to_string();
    }
    let hash = blake3::hash(s.as_bytes()).to_hex();
    let head: String = s
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
        .take(40)
        .collect();
    let head = head.trim_matches('-');
    format!("{head}-{}", &hash[..10])
        .trim_start_matches('-')
        .to_string()
}

/// The control-socket allowlist a node-owned engine pod reads
/// (`CONSTELLATION_CONTROL_POLICY`, plan 33 U1's `kind = "service"`
/// grant): the node plugin — uid 0, the only privileged component (plan
/// 37 settled decision 10) — is admin on this pod's socket and nowhere
/// else (admin, not operator: `view.mount`/`view.unmount` are Admin-only
/// in the method table); the engine's own uid stays admin as the
/// daemon's owner. Written by the node plugin into
/// `<hostRoot>/policy/<unit>/`, which the pod mounts read-only.
///
/// One grant per socket a pod of the unit may bind: the control socket and
/// the handoff standby's socket (§8) of both generation slots — the
/// serving pod and its replacement share the unit's sockets directory, and
/// this one file.
pub fn node_engine_policy() -> String {
    let mut policy =
        String::from("# Written by the constellation-csi node plugin (plan 37 §7, §8).\n");
    for slot in 0..2 {
        for file in [control_socket_file(slot), handoff_socket_file(slot)] {
            policy.push_str(&format!(
                "[[grant]]\n\
                 kind = \"service\"\n\
                 principal = \"uid:0\"\n\
                 socket = \"{POD_SOCKET_DIR}/{file}\"\n\
                 role = \"admin\"\n\
                 label = \"csi-node-plugin\"\n"
            ));
        }
    }
    policy
}

/// The control socket's file name of a generation-`generation` node pod:
/// two slots, alternating, so a pod and its replacement never share one.
pub fn control_socket_file(generation: u32) -> &'static str {
    if generation.is_multiple_of(2) {
        "control.sock"
    } else {
        "control-b.sock"
    }
}

/// Where a generation-`generation` node pod waits as a handoff standby.
pub fn handoff_socket_file(generation: u32) -> &'static str {
    if generation.is_multiple_of(2) {
        "handoff.sock"
    } else {
        "handoff-b.sock"
    }
}

/// `constellation-engine-<unit>-<node>` for generation 0, with `-g<n>`
/// after the node for a later one (still a pod name under the same
/// limits, still `constellation-engine-<unit>-…`).
pub fn node_pod_name_gen(unit: &str, node: &str, generation: u32) -> String {
    match generation {
        0 => node_pod_name(unit, node),
        g => node_pod_name(unit, &format!("{node}-g{g}")),
    }
}

/// Bump when the node pods' template changes in a way running pods must
/// be rolled for (the fingerprint then differs for every pod).
const POD_TEMPLATE_REVISION: u32 = 1;

/// The fingerprint of the engine settings node pods are created from
/// ([`ANNOTATION_ENGINE_CONFIG`]): image, pull policy, resources, log
/// level and the template revision.
pub fn engine_config_fingerprint(cfg: &EnginePodConfig) -> String {
    let settings = serde_json::json!({
        "image": cfg.image,
        "pull": cfg.image_pull_policy,
        "resources": cfg.resources,
        "log": cfg.log_level,
        "template": POD_TEMPLATE_REVISION,
    });
    blake3::hash(settings.to_string().as_bytes()).to_hex()[..16].to_string()
}

/// Point `pod` (a node pod) at generation `generation`: its name, its
/// generation label, and its control and handoff sockets (arguments and
/// probes) in that generation's slot.
fn set_generation(pod: &mut Pod, unit: &str, node: &str, generation: u32) {
    pod.metadata.name = Some(node_pod_name_gen(unit, node, generation));
    pod.metadata
        .labels
        .get_or_insert_with(BTreeMap::new)
        .insert(LABEL_GENERATION.to_string(), generation.to_string());
    let control = format!("{POD_SOCKET_DIR}/{}", control_socket_file(generation));
    let handoff = format!("{POD_SOCKET_DIR}/{}", handoff_socket_file(generation));
    let Some(container) = pod.spec.as_mut().and_then(|s| s.containers.first_mut()) else {
        return;
    };
    let args = container.args.get_or_insert_with(Vec::new);
    for (flag, value) in [
        ("--control-socket", &control),
        ("--handoff-socket", &handoff),
    ] {
        match args.iter().position(|a| a == flag) {
            Some(i) if i + 1 < args.len() => args[i + 1] = value.clone(),
            _ => args.extend([flag.to_string(), value.clone()]),
        }
    }
    // Readiness asks the control socket only: a pod is `Ready` once it
    // serves. Startup and liveness also take the handoff socket's answer
    // (plan 37 §8): a replacement waits there as a standby, without a
    // control socket, for as long as the handoff takes — kubelet must not
    // kill it for that (it would restart into another standby, and the
    // sessions it holds would be gone).
    let ping = |or: Option<&str>| {
        let mut command = vec![
            ENGINE_BIN.to_string(),
            "control-relay".into(),
            "--ping".into(),
            "--socket".into(),
            control.clone(),
        ];
        if let Some(or) = or {
            command.extend(["--or-socket".to_string(), or.to_string()]);
        }
        command
    };
    for (probe, or) in [
        (container.startup_probe.as_mut(), Some(handoff.as_str())),
        (container.readiness_probe.as_mut(), None),
        (container.liveness_probe.as_mut(), Some(handoff.as_str())),
    ] {
        if let Some(exec) = probe.and_then(|p| p.exec.as_mut()) {
            exec.command = Some(ping(or));
        }
    }
}

/// The replacement of a running node pod (`current`, its unit's
/// generation `generation - 1`): its spec — the pool, credentials and
/// hostPaths it serves — with the engine settings of `cfg` (image, pull
/// policy, resources, log level), in generation `generation`'s name and
/// sockets. Pure, so it is unit-tested.
pub fn replacement_pod(current: &Pod, cfg: &EnginePodConfig, generation: u32) -> Option<Pod> {
    let labels = current.metadata.labels.clone().unwrap_or_default();
    let unit = labels.get(LABEL_UNIT)?.clone();
    let mut spec = current.spec.clone()?;
    let node = spec.node_name.clone()?;
    for c in spec
        .containers
        .iter_mut()
        .chain(spec.init_containers.iter_mut().flatten())
    {
        c.image = Some(cfg.image.clone());
        c.image_pull_policy = cfg.image_pull_policy.clone();
    }
    if let Some(engine) = spec.containers.first_mut() {
        engine.resources = cfg.resources.clone();
        let envs = engine.env.get_or_insert_with(Vec::new);
        envs.retain(|e| e.name != "RUST_LOG");
        envs.insert(0, env("RUST_LOG", cfg.log_level.clone()));
    }
    let mut annotations = current.metadata.annotations.clone().unwrap_or_default();
    for gone in [
        ANNOTATION_VIEWS,
        ANNOTATION_IDLE_SINCE,
        ANNOTATION_HANDOFF_FALLBACK,
    ] {
        annotations.remove(gone);
    }
    annotations.insert(
        ANNOTATION_ENGINE_CONFIG.to_string(),
        engine_config_fingerprint(cfg),
    );
    let mut pod = Pod {
        metadata: ObjectMeta {
            namespace: current.metadata.namespace.clone(),
            labels: Some(labels),
            annotations: Some(annotations),
            owner_references: current.metadata.owner_references.clone(),
            ..Default::default()
        },
        spec: Some(spec),
        status: None,
    };
    set_generation(&mut pod, &unit, &node, generation);
    Some(pod)
}

/// A node pod's generation ([`LABEL_GENERATION`]; `0` without one).
pub fn pod_generation(pod: &Pod) -> u32 {
    pod.metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(LABEL_GENERATION))
        .and_then(|g| g.parse().ok())
        .unwrap_or(0)
}

/// The control-socket allowlist of a controller-owned engine pod: the
/// controller's relay, exec'd into the pod as the engine's own uid, is the
/// `csi-controller` service on the pod's socket (admin, as the node
/// plugin's grant), so the audit log names the service rather than the
/// bare owner uid. Baked into the image
/// (`deploy/docker/controller-engine-control-allow.toml`, at
/// [`CONTROLLER_POD_POLICY`]): the controller-owned pod has no hostPath to
/// be handed one through.
pub fn controller_engine_policy() -> String {
    format!(
        "[[grant]]\n\
         kind = \"service\"\n\
         principal = \"uid:{ENGINE_UID}\"\n\
         socket = \"{POD_SOCKET}\"\n\
         role = \"admin\"\n\
         label = \"csi-controller\"\n"
    )
}

/// Where the image holds [`controller_engine_policy`].
pub const CONTROLLER_POD_POLICY: &str =
    "/etc/constellation-csi/controller-engine/control-allow.toml";

/// Where a node-owned engine pod reads [`node_engine_policy`] from: its
/// `<hostRoot>/policy/<unit>/` directory, mounted read-only.
pub const POD_POLICY: &str = "/etc/constellation-csi/policy/control-allow.toml";
const POD_POLICY_DIR: &str = "/etc/constellation-csi/policy";
/// Its name inside `<hostRoot>/policy/<unit>/`.
pub const POLICY_FILE: &str = "control-allow.toml";
/// `<hostRoot>/policy/`: root-owned, never mounted writable into a pod.
pub const POLICY_DIR: &str = "policy";

fn path_component(s: &str) -> std::io::Result<std::ffi::CString> {
    use std::io::{Error, ErrorKind};
    if s.is_empty() || s == "." || s == ".." || s.contains('/') {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("{s:?} is not a single path component"),
        ));
    }
    std::ffi::CString::new(s).map_err(|e| Error::new(ErrorKind::InvalidInput, e))
}

fn cvt(rc: libc::c_int) -> std::io::Result<libc::c_int> {
    if rc < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(rc)
    }
}

/// Open directory `path` (relative to `at`) without following a link; if
/// `private`, it must be owned by this process's uid and writable by
/// nobody else.
fn open_dir_nofollow(
    at: libc::c_int,
    path: &std::ffi::CStr,
    shown: &dyn std::fmt::Display,
    private: bool,
) -> std::io::Result<std::os::fd::OwnedFd> {
    use std::io::{Error, ErrorKind};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    // SAFETY: a valid NUL-terminated path; the result is checked.
    let fd = cvt(unsafe {
        libc::openat(
            at,
            path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    })
    .map_err(|e| Error::new(e.kind(), format!("opening {shown}: {e}")))?;
    // SAFETY: a descriptor we just opened and own.
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    if private {
        // SAFETY: no preconditions.
        let me = unsafe { libc::geteuid() };
        // SAFETY: `st` is written by fstat before it is read.
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        cvt(unsafe { libc::fstat(fd.as_raw_fd(), &mut st) })?;
        if st.st_uid != me || st.st_mode & 0o022 != 0 {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                format!(
                    "{shown} is owned by uid {} with mode {:o}: refusing to write into a \
                     directory anyone but uid {me} can change",
                    st.st_uid,
                    st.st_mode & 0o7777
                ),
            ));
        }
    }
    Ok(fd)
}

/// `<root>/<dirs…>`, each created if missing (mode 0755) and opened with
/// `O_NOFOLLOW`, each owned by this process's uid and writable by nobody
/// else — so nobody else can have put a symlink or a hard link in any of
/// them. The last one's descriptor.
fn private_dir_chain(
    root: &std::path::Path,
    dirs: &[&str],
) -> std::io::Result<std::os::fd::OwnedFd> {
    use std::io::{Error, ErrorKind};
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;
    std::fs::create_dir_all(root)?;
    let root_c = std::ffi::CString::new(root.as_os_str().as_bytes())
        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
    let mut dir = open_dir_nofollow(libc::AT_FDCWD, &root_c, &root.display(), true)?;
    let mut shown = root.to_path_buf();
    for d in dirs {
        let c = path_component(d)?;
        shown.push(d);
        // SAFETY: a valid NUL-terminated name relative to an open dir.
        match cvt(unsafe { libc::mkdirat(dir.as_raw_fd(), c.as_ptr(), 0o755) }) {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
            Err(e) => {
                return Err(Error::new(
                    e.kind(),
                    format!("mkdir {}: {e}", shown.display()),
                ))
            }
        }
        dir = open_dir_nofollow(dir.as_raw_fd(), &c, &shown.display(), true)?;
    }
    Ok(dir)
}

/// Write `contents` to `<root>/<dirs…>/<file>` as root (the node plugin)
/// without trusting anything below `root` that someone else could have
/// planted (plan 37 settled decision 10: the engine pod is unprivileged,
/// and a privileged writer must not become its tool). Every directory is
/// created if missing and checked as [`private_dir_chain`] does; the file
/// is written to a fresh `O_EXCL|O_NOFOLLOW` temporary next to it, synced
/// and renamed over the old one within the same directory descriptor.
/// Unchanged contents are not rewritten.
pub fn write_private_file(
    root: &std::path::Path,
    dirs: &[&str],
    file: &str,
    contents: &[u8],
) -> std::io::Result<()> {
    use std::io::{ErrorKind, Read, Write};
    use std::os::fd::{AsRawFd, FromRawFd};

    let dir = private_dir_chain(root, dirs)?;
    let file_c = path_component(file)?;
    // Unchanged: nothing to do (read without following a link).
    // SAFETY: a valid NUL-terminated name relative to an open dir.
    let have = unsafe {
        libc::openat(
            dir.as_raw_fd(),
            file_c.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
        )
    };
    if have >= 0 {
        // SAFETY: a descriptor we just opened and own.
        let mut f = unsafe { std::fs::File::from_raw_fd(have) };
        let mut buf = Vec::new();
        if f.metadata().is_ok_and(|m| m.is_file())
            && f.read_to_end(&mut buf).is_ok()
            && buf == contents
        {
            return Ok(());
        }
    }
    let tmp_c = path_component(&format!(".{file}.tmp"))?;
    // A leftover temporary (ours, from a crash) goes first; O_EXCL below
    // then guarantees a fresh inode of our own.
    // SAFETY: a valid NUL-terminated name relative to an open dir.
    if let Err(e) = cvt(unsafe { libc::unlinkat(dir.as_raw_fd(), tmp_c.as_ptr(), 0) }) {
        if e.kind() != ErrorKind::NotFound {
            return Err(e);
        }
    }
    // SAFETY: a valid NUL-terminated name relative to an open dir.
    let fd = cvt(unsafe {
        libc::openat(
            dir.as_raw_fd(),
            tmp_c.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o644 as libc::c_uint,
        )
    })?;
    // SAFETY: a descriptor we just opened and own.
    let mut f = unsafe { std::fs::File::from_raw_fd(fd) };
    f.write_all(contents)?;
    f.sync_all()?;
    drop(f);
    // SAFETY: valid NUL-terminated names relative to an open dir.
    cvt(unsafe {
        libc::renameat(
            dir.as_raw_fd(),
            tmp_c.as_ptr(),
            dir.as_raw_fd(),
            file_c.as_ptr(),
        )
    })?;
    // SAFETY: an open descriptor.
    cvt(unsafe { libc::fsync(dir.as_raw_fd()) })?;
    Ok(())
}

/// The node-owned engine pod's own hostPath directories,
/// `<root>/<sub>/<unit>/` for each of `subs`, made by the node plugin
/// (root) before it creates the pod, owned by the engine's `uid:gid`,
/// mode 0700 — which is what lets the pod mount them `type: Directory`
/// with no root init container to `chown` what kubelet would create
/// root-owned (plan 37 §9, K6a). `<root>` and `<root>/<sub>` are checked
/// as [`private_dir_chain`] does, so the unit directory's parent is one
/// only root can change; the unit directory itself is opened with
/// `O_NOFOLLOW` (a link there is refused, never followed) and changed
/// through its descriptor.
pub fn prepare_unit_dirs(
    root: &std::path::Path,
    subs: &[&str],
    unit: &str,
    uid: u32,
    gid: u32,
) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    use std::os::fd::AsRawFd;
    let unit_c = path_component(unit)?;
    for sub in subs {
        let parent = private_dir_chain(root, &[sub])?;
        let shown = root.join(sub).join(unit);
        // SAFETY: a valid NUL-terminated name relative to an open dir.
        match cvt(unsafe { libc::mkdirat(parent.as_raw_fd(), unit_c.as_ptr(), 0o700) }) {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
            Err(e) => {
                return Err(Error::new(
                    e.kind(),
                    format!("mkdir {}: {e}", shown.display()),
                ))
            }
        }
        let dir = open_dir_nofollow(parent.as_raw_fd(), &unit_c, &shown.display(), false)?;
        // SAFETY: an open descriptor.
        cvt(unsafe { libc::fchown(dir.as_raw_fd(), uid, gid) })
            .map_err(|e| Error::new(e.kind(), format!("chown {}: {e}", shown.display())))?;
        // SAFETY: an open descriptor.
        cvt(unsafe { libc::fchmod(dir.as_raw_fd(), 0o700) })
            .map_err(|e| Error::new(e.kind(), format!("chmod {}: {e}", shown.display())))?;
    }
    Ok(())
}

/// The backend URL `constellation serve --s3` gets.
fn s3_url(pool: &PoolRef) -> String {
    let prefix = pool.prefix();
    if prefix.is_empty() {
        format!("s3://{}", pool.class.bucket)
    } else {
        format!("s3://{}/{prefix}", pool.class.bucket)
    }
}

fn env(name: &str, value: impl Into<String>) -> EnvVar {
    EnvVar {
        name: name.into(),
        value: Some(value.into()),
        ..Default::default()
    }
}

/// The controller-owned engine pod of `pool` (module docs). Pure: no API
/// call, so the spec is unit-tested.
pub fn engine_pod(pool: &PoolRef, cfg: &EnginePodConfig, owner: Option<&OwnerReference>) -> Pod {
    let mut pod = build_engine_pod(pool, cfg, owner, &EngineRole::Controller);
    // What the purge worker's roll compares (`crate::purge`, 37-k6b).
    pod.metadata
        .annotations
        .get_or_insert_with(BTreeMap::new)
        .insert(
            ANNOTATION_ENGINE_CONFIG.to_string(),
            engine_config_fingerprint(cfg),
        );
    pod
}

/// The replacement of controller-owned pod `current` whose engine settings
/// drifted from `cfg` (37-k6b: K5a's handoff rolls only node-owned pods;
/// this one serves no view, so the purge worker replaces it plainly): its
/// own spec — pool, arguments, volumes — under the same name, with `cfg`'s
/// image, pull policy, resources and log level, placed by the scheduler
/// anew. Pure, so it is unit-tested.
pub fn controller_replacement(current: &Pod, cfg: &EnginePodConfig) -> Pod {
    let mut pod = respawn(current);
    if let Some(spec) = pod.spec.as_mut() {
        for c in spec.containers.iter_mut() {
            c.image = Some(cfg.image.clone());
            c.image_pull_policy = cfg.image_pull_policy.clone();
        }
        if let Some(engine) = spec.containers.first_mut() {
            engine.resources = cfg.resources.clone();
            let envs = engine.env.get_or_insert_with(Vec::new);
            envs.retain(|e| e.name != "RUST_LOG");
            envs.insert(0, env("RUST_LOG", cfg.log_level.clone()));
        }
    }
    pod.metadata
        .annotations
        .get_or_insert_with(BTreeMap::new)
        .insert(
            ANNOTATION_ENGINE_CONFIG.to_string(),
            engine_config_fingerprint(cfg),
        );
    pod
}

/// Whether engine pod `pod` was made from settings other than `cfg`'s
/// (its [`ANNOTATION_ENGINE_CONFIG`]; none counts as drifted).
pub fn drifted_from(pod: &Pod, cfg: &EnginePodConfig) -> bool {
    pod.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(ANNOTATION_ENGINE_CONFIG))
        .map(String::as_str)
        != Some(engine_config_fingerprint(cfg).as_str())
}

/// The node-owned engine pod of `pool` on `node` (plan 37 §7), owned by
/// the node plugin's `DaemonSet` (`owner`). `fs_uuid`: the filesystem the
/// volume that brings it up names, for the `constellation.replicix.com/fs-uuid`
/// label. Pure, so the spec is unit-tested.
pub fn node_engine_pod(
    pool: &PoolRef,
    cfg: &EnginePodConfig,
    node: &str,
    owner: Option<&OwnerReference>,
    fs_uuid: &str,
) -> Pod {
    let role = EngineRole::Node {
        node: node.to_string(),
    };
    let mut pod = build_engine_pod(pool, cfg, owner, &role);
    if let Some(spec) = pod.spec.as_mut() {
        spec.hostname = Some(node_engine_hostname(node));
    }
    if let Some(labels) = pod.metadata.labels.as_mut() {
        labels.insert(LABEL_NODE.to_string(), label_value(node));
        labels.insert(LABEL_UNIT.to_string(), unit_name(pool));
        labels.insert(LABEL_FS_UUID.to_string(), label_value(fs_uuid));
    }
    pod.metadata
        .annotations
        .get_or_insert_with(BTreeMap::new)
        .insert(
            ANNOTATION_ENGINE_CONFIG.to_string(),
            engine_config_fingerprint(cfg),
        );
    // Every node pod can wait as a handoff standby (§8): it does when the
    // state dir is held, i.e. only as a replacement.
    set_generation(&mut pod, &unit_name(pool), node, 0);
    pod
}

/// [`node_engine_pod`] in generation `generation` (a unit whose serving
/// pod is a replacement's replacement).
pub fn node_engine_pod_gen(
    pool: &PoolRef,
    cfg: &EnginePodConfig,
    node: &str,
    owner: Option<&OwnerReference>,
    fs_uuid: &str,
    generation: u32,
) -> Pod {
    let mut pod = node_engine_pod(pool, cfg, node, owner, fs_uuid);
    set_generation(&mut pod, &unit_name(pool), node, generation);
    pod
}

/// The `fs` an engine pod's `fs.unlock` names: its `--s3` URL, which its
/// credential gate checks (it knows no uuid before it can read the
/// bucket) and the running daemon resolves to itself.
pub fn unlock_target(pool: &PoolRef) -> String {
    s3_url(pool)
}

/// Whether `pod` runs its engine with `--await-unlock` (from its own
/// spec, so a pod made by an older plugin generation is read as it is).
pub fn awaits_unlock(pod: &Pod) -> bool {
    pod.spec
        .as_ref()
        .and_then(|s| s.containers.iter().find(|c| c.name == ENGINE_CONTAINER))
        .and_then(|c| c.args.as_ref())
        .is_some_and(|a| a.iter().any(|x| x == "--await-unlock"))
}

/// Both roles' engine pod (module docs, [`node_engine_pod`]).
fn build_engine_pod(
    pool: &PoolRef,
    cfg: &EnginePodConfig,
    owner: Option<&OwnerReference>,
    role: &EngineRole,
) -> Pod {
    let name = role.pod_name(pool);
    // A node-owned pod's directory under `<hostRoot>/{node-identity,
    // sockets,policy}/` (§7's `<unit>`); the controller's has none.
    let unit = unit_name(pool);
    let class = &pool.class;
    let mut labels = BTreeMap::from([
        (
            "app.kubernetes.io/name".to_string(),
            "constellation-csi".to_string(),
        ),
        (LABEL_COMPONENT.to_string(), "engine".to_string()),
        (LABEL_POOL.to_string(), pool_label(pool)),
        (LABEL_SHARD.to_string(), pool.shard.to_string()),
        (LABEL_OWNER.to_string(), role.owner_label().to_string()),
    ]);
    labels.retain(|_, v| !v.is_empty());
    let mut annotations = BTreeMap::from([(ANNOTATION_S3.to_string(), s3_url(pool))]);
    if let Some(endpoint) = &class.endpoint {
        annotations.insert(ANNOTATION_ENDPOINT.to_string(), endpoint.clone());
    }

    let mut args = vec![
        "serve".to_string(),
        "--s3".into(),
        s3_url(pool),
        "--state-dir".into(),
        POD_STATE_DIR.into(),
        "--control-socket".into(),
        POD_SOCKET.into(),
    ];
    // The controller's pod may be the first to see a pool, and makes its
    // filesystem; a node's never does — a volume names a filesystem that
    // exists, and a wrong location must fail rather than create a stray.
    if *role == EngineRole::Controller {
        args.push("--create".into());
    }
    if let Some(chunk) = class.chunk_size {
        args.extend(["--chunk-size".into(), chunk.to_string()]);
    }
    if let Some(compression) = &class.compression {
        args.extend(["--compression".into(), compression.clone()]);
    }
    if class.e2e {
        args.push("--e2e".into());
    }
    // Plan 37 §9: the credentials (and an E2E passphrase) arrive only as
    // `fs.unlock` over the control socket (`crate::credentials`).
    if class.awaits_unlock() {
        args.push("--await-unlock".into());
    }
    // The class's `writeMode` is not passed: it is a property of a mounted
    // view (`view.mount`'s write mode), and a controller-owned pod mounts
    // none. The node-owned pods that do mount get it from K3 on; `fs.create`
    // ignores it (it names no registry entry).

    let mut envs = vec![
        env("RUST_LOG", cfg.log_level.clone()),
        // A read-only root: everything the CLI writes besides the state
        // dir (registry, runtime dir) goes to the scratch emptyDir.
        env("HOME", "/tmp"),
        env("XDG_CONFIG_HOME", "/tmp/config"),
        env("XDG_CACHE_HOME", "/tmp/cache"),
        env("XDG_DATA_HOME", "/tmp/data"),
        env("XDG_RUNTIME_DIR", "/tmp/run"),
        // §6: "server" is the only engine profile through K6.
        env("CONSTELLATION_PROFILE", "server"),
    ];
    envs.push(env(
        "CONSTELLATION_CONTROL_POLICY",
        match role {
            // The node plugin's grant on this pod's socket
            // (`node_engine_policy`).
            EngineRole::Node { .. } => POD_POLICY,
            // The controller's (`controller_engine_policy`), in the image.
            EngineRole::Controller => CONTROLLER_POD_POLICY,
        },
    ));
    if let Some(endpoint) = &class.endpoint {
        envs.push(env("AWS_ENDPOINT", endpoint.clone()));
        envs.push(env("AWS_ENDPOINT_URL", endpoint.clone()));
        if endpoint.starts_with("http://") {
            envs.push(env("AWS_ALLOW_HTTP", "true"));
        }
    }
    if let Some(region) = &class.region {
        envs.push(env("AWS_REGION", region.clone()));
        envs.push(env("AWS_DEFAULT_REGION", region.clone()));
    }
    let ping = Probe {
        exec: Some(ExecAction {
            command: Some(vec![
                ENGINE_BIN.into(),
                "control-relay".into(),
                "--ping".into(),
                "--socket".into(),
                POD_SOCKET.into(),
            ]),
        }),
        timeout_seconds: Some(5),
        ..Default::default()
    };
    // A node-owned pod's state and socket are §7's hostPaths, which its
    // plugin makes owned by the engine's uid before it creates the pod
    // (`prepare_unit_dirs`): no root init container to `chown` them. The
    // controller-owned pod's are `emptyDir`s (module docs).
    let host_root = cfg.host_root.trim_end_matches('/');
    let unit_dir = |sub: &str| Volume {
        name: sub.into(),
        host_path: match role {
            EngineRole::Node { .. } => Some(HostPathVolumeSource {
                path: format!("{host_root}/{sub}/{unit}"),
                type_: Some("Directory".into()),
            }),
            EngineRole::Controller => None,
        },
        empty_dir: match role {
            EngineRole::Node { .. } => None,
            EngineRole::Controller => Some(EmptyDirVolumeSource::default()),
        },
        ..Default::default()
    };
    let mut mounts = vec![
        VolumeMount {
            name: "node-identity".into(),
            mount_path: POD_STATE_DIR.into(),
            ..Default::default()
        },
        VolumeMount {
            name: "sockets".into(),
            mount_path: POD_SOCKET_DIR.into(),
            ..Default::default()
        },
        VolumeMount {
            name: "scratch".into(),
            mount_path: "/tmp".into(),
            ..Default::default()
        },
    ];
    let mut volumes = vec![
        unit_dir("node-identity"),
        unit_dir("sockets"),
        Volume {
            name: "scratch".into(),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Default::default()
        },
    ];
    if let EngineRole::Node { .. } = role {
        // The node plugin's grant (`node_engine_policy`): root-owned on
        // the host, read-only here, so the engine cannot rewrite it — nor
        // plant anything the privileged plugin would then write through.
        mounts.push(VolumeMount {
            name: POLICY_DIR.into(),
            mount_path: POD_POLICY_DIR.into(),
            read_only: Some(true),
            ..Default::default()
        });
        volumes.push(Volume {
            name: POLICY_DIR.into(),
            host_path: Some(HostPathVolumeSource {
                path: format!("{host_root}/{POLICY_DIR}/{unit}"),
                // Made by the plugin before it creates the pod.
                type_: Some("Directory".into()),
            }),
            ..Default::default()
        });
    }
    // PodSecurity `restricted`'s container rules, whole (plan 37 §9).
    let unprivileged = SecurityContext {
        allow_privilege_escalation: Some(false),
        capabilities: Some(Capabilities {
            drop: Some(vec!["ALL".into()]),
            add: None,
        }),
        read_only_root_filesystem: Some(true),
        run_as_non_root: Some(true),
        seccomp_profile: Some(SeccompProfile {
            type_: "RuntimeDefault".into(),
            localhost_profile: None,
        }),
        ..Default::default()
    };

    Pod {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(cfg.namespace.clone()),
            labels: Some(labels),
            annotations: Some(annotations),
            owner_references: owner.map(|o| vec![o.clone()]),
            ..Default::default()
        },
        spec: Some(PodSpec {
            // §7: a node's pod is pinned (no scheduler round trip; its
            // identity is the node's) and tolerates whatever the node
            // plugin itself is scheduled through.
            node_name: match role {
                EngineRole::Node { node } => Some(node.clone()),
                EngineRole::Controller => None,
            },
            tolerations: match role {
                EngineRole::Node { .. } => Some(vec![Toleration {
                    operator: Some("Exists".into()),
                    ..Default::default()
                }]),
                EngineRole::Controller => None,
            },
            restart_policy: Some("OnFailure".into()),
            // §7's drain discipline: the daemon ignores SIGTERM while it
            // serves a view (`serve`'s deferred signal) and exits once the
            // last view is unmounted, so this grace bounds a deletion the
            // node plugin did not drain. A pod with no view exits at once.
            termination_grace_period_seconds: Some(600),
            service_account_name: cfg.service_account.clone(),
            automount_service_account_token: Some(false),
            node_selector: Some(BTreeMap::from([(
                "kubernetes.io/os".to_string(),
                "linux".to_string(),
            )])),
            security_context: Some(PodSecurityContext {
                run_as_non_root: Some(true),
                run_as_user: Some(ENGINE_UID),
                run_as_group: Some(ENGINE_UID),
                seccomp_profile: Some(SeccompProfile {
                    type_: "RuntimeDefault".into(),
                    localhost_profile: None,
                }),
                ..Default::default()
            }),
            containers: vec![Container {
                name: ENGINE_CONTAINER.into(),
                image: Some(cfg.image.clone()),
                image_pull_policy: cfg.image_pull_policy.clone(),
                command: Some(vec![ENGINE_BIN.into()]),
                args: Some(args),
                env: Some(envs),
                security_context: Some(unprivileged),
                resources: cfg.resources.clone(),
                startup_probe: Some(Probe {
                    period_seconds: Some(2),
                    failure_threshold: Some(150),
                    ..ping.clone()
                }),
                readiness_probe: Some(Probe {
                    period_seconds: Some(5),
                    failure_threshold: Some(2),
                    ..ping.clone()
                }),
                liveness_probe: Some(Probe {
                    period_seconds: Some(20),
                    failure_threshold: Some(6),
                    ..ping
                }),
                volume_mounts: Some(mounts),
                ..Default::default()
            }],
            volumes: Some(volumes),
            ..Default::default()
        }),
        status: None,
    }
}

/// The provisioner secret a class names, its templates resolved for `pv`
/// (`csi.storage.k8s.io/provisioner-secret-{name,namespace}`, external-
/// provisioner's own substitutions). `None`: the class names none.
fn provisioner_secret_ref(
    params: &BTreeMap<String, String>,
    pv: &PersistentVolume,
) -> Option<(String, String)> {
    let name = params.get("csi.storage.k8s.io/provisioner-secret-name")?;
    let namespace = params.get("csi.storage.k8s.io/provisioner-secret-namespace")?;
    let claim = pv.spec.as_ref().and_then(|s| s.claim_ref.as_ref());
    let resolve = |t: &str| {
        t.replace(
            "${pvc.namespace}",
            claim.and_then(|c| c.namespace.as_deref()).unwrap_or(""),
        )
        .replace(
            "${pvc.name}",
            claim.and_then(|c| c.name.as_deref()).unwrap_or(""),
        )
        .replace("${pv.name}", pv.metadata.name.as_deref().unwrap_or(""))
    };
    Some((resolve(namespace), resolve(name)))
}

/// Whether `pv` is a pool volume of this driver inside filesystem
/// `fs_uuid`; its shard if so.
fn pool_shard_of(pv: &PersistentVolume, fs_uuid: &str) -> Option<u32> {
    let csi = pv.spec.as_ref()?.csi.as_ref()?;
    if csi.driver != crate::identity::DRIVER_NAME {
        return None;
    }
    match VolumeId::parse(&csi.volume_handle).ok()? {
        VolumeId::Pool {
            shard, fs_uuid: u, ..
        } if u == fs_uuid => Some(shard),
        VolumeId::Dedicated { fs_uuid: u } if u == fs_uuid => Some(0),
        _ => None,
    }
}

/// The pool `pv` lives in, rebuilt from its `StorageClass` (module docs);
/// secrets empty, and the class's provisioner secret reference resolved.
pub fn pool_from_pv(
    pv: &PersistentVolume,
    class: &StorageClass,
    shard: u32,
) -> Result<(PoolRef, Option<(String, String)>), String> {
    let params = class.parameters.clone().unwrap_or_default();
    let mut class_params = ClassParams::parse(&params.clone().into_iter().collect())
        .map_err(|e| format!("StorageClass {:?}: {e}", class.metadata.name))?;
    if class_params.layout == crate::params::Layout::Dedicated {
        // The volume's own filesystem: the prefix its context recorded,
        // else the one `CreateVolume` derives from the PV's name.
        let recorded = pv
            .spec
            .as_ref()
            .and_then(|s| s.csi.as_ref())
            .and_then(|c| c.volume_attributes.as_ref())
            .and_then(|a| a.get("prefix").cloned());
        class_params.prefix = match recorded {
            Some(prefix) => prefix,
            None => class_params.dedicated_prefix(pv.metadata.name.as_deref().unwrap_or("")),
        };
    }
    if shard >= class_params.shards {
        return Err(format!(
            "StorageClass {:?} has {} shard(s); the volume names shard {shard}",
            class.metadata.name, class_params.shards
        ));
    }
    Ok((
        PoolRef {
            class: class_params,
            shard,
            secrets: BTreeMap::new(),
        },
        provisioner_secret_ref(&params, pv),
    ))
}

fn kube_err(what: &str, e: kube::Error) -> ControlError {
    ControlError::unavailable(format!("{what}: {e}"))
}

fn is_status(e: &kube::Error, code: u16) -> bool {
    matches!(e, kube::Error::Api(s) if s.code == code)
}

fn pod_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|c| c.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
}

fn pod_phase(pod: &Pod) -> &str {
    pod.status
        .as_ref()
        .and_then(|s| s.phase.as_deref())
        .unwrap_or("Pending")
}

/// What a not-yet-ready pod is waiting on, for the error a timeout returns.
fn pod_waiting_on(pod: &Pod) -> String {
    let status = pod.status.as_ref();
    let reasons: Vec<String> = status
        .into_iter()
        .flat_map(|s| {
            s.init_container_statuses
                .iter()
                .flatten()
                .chain(s.container_statuses.iter().flatten())
        })
        .filter_map(|c| {
            let waiting = c.state.as_ref()?.waiting.as_ref()?;
            Some(format!(
                "{}: {}",
                c.name,
                waiting.reason.as_deref().unwrap_or("waiting")
            ))
        })
        .collect();
    if reasons.is_empty() {
        format!("phase {}", pod_phase(pod))
    } else {
        reasons.join(", ")
    }
}

/// Pod `name`, created from `spec` when absent and recreated (from `spec`,
/// else from its own) when it has terminated or its engine left the
/// registry ([`controller_pods::ANNOTATION_LEFT`]), once it is `Ready`: polled
/// with exponential backoff (200 ms doubling to 3 s) for at most
/// `ready_timeout`. The timeout is `Timeout`, naming what the pod waits on.
async fn ensure_pod_ready(
    pods: &Api<Pod>,
    name: &str,
    spec: Option<&Pod>,
    ready_timeout: Duration,
) -> Result<Pod, ControlError> {
    let deadline = Instant::now() + ready_timeout;
    let mut backoff = Duration::from_millis(200);
    let mut last: Option<Pod> = None;
    loop {
        let pod = pods
            .get_opt(name)
            .await
            .map_err(|e| kube_err("reading the engine pod", e))?;
        match pod {
            None => {
                let fresh = match (spec, &last) {
                    (Some(spec), _) => spec.clone(),
                    // Recreate a pod we just deleted from its own spec.
                    (None, Some(old)) => respawn(old),
                    (None, None) => {
                        return Err(ControlError::unavailable(format!(
                            "engine pod {name} does not exist"
                        )))
                    }
                };
                match pods.create(&PostParams::default(), &fresh).await {
                    Ok(_) => tracing::info!(pod = name, "created engine pod"),
                    Err(e) if is_status(&e, 409) => {}
                    Err(e) => return Err(kube_err("creating the engine pod", e)),
                }
            }
            Some(pod) if pod.metadata.deletion_timestamp.is_some() => {
                last = Some(pod);
            }
            Some(pod) if matches!(pod_phase(&pod), "Failed" | "Succeeded") => {
                tracing::warn!(
                    pod = name,
                    phase = pod_phase(&pod),
                    "engine pod terminated; replacing it"
                );
                let _ = pods.delete(name, &DeleteParams::default()).await;
                last = Some(pod);
            }
            // Its engine left the registry (`controller_pods`): never
            // served from again.
            Some(pod) if controller_pods::has_left(&pod) => {
                controller_pods::delete_left(pods, name, &pod).await?;
                last = Some(pod);
            }
            Some(pod) if pod_ready(&pod) => return Ok(pod),
            Some(pod) => last = Some(pod),
        }
        if Instant::now() >= deadline {
            let why = last.as_ref().map(pod_waiting_on).unwrap_or_default();
            return Err(ControlError::new(
                ErrorKind::Timeout,
                format!("engine pod {name} is not ready after {ready_timeout:?} ({why})"),
            ));
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(3));
    }
}

/// Which run of a node-owned engine pod's daemon this is: the pod's uid
/// and its engine container's id, so a container restart (`OnFailure`),
/// which loses the daemon's views and `fs.unlock` credentials as surely as
/// a new pod does, counts as a new incarnation too.
fn incarnation(pod: &Pod) -> String {
    let uid = pod.metadata.uid.as_deref().unwrap_or_default();
    let container = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_ref())
        .and_then(|c| c.iter().find(|c| c.name == ENGINE_CONTAINER))
        .and_then(|c| c.container_id.as_deref())
        .unwrap_or_default();
    format!("{uid}/{container}")
}

/// One live relay into one pod incarnation.
struct Relay {
    uid: String,
    client: Arc<SocketControlClient>,
    process: kube::api::AttachedProcess,
    /// The fingerprint of the credentials this process last unlocked the
    /// incarnation with ([`crate::credentials::fingerprint`]); `None`:
    /// not by this process (it may need none, or another controller
    /// process did it).
    unlocked: Mutex<Option<[u8; 32]>>,
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.process.abort();
    }
}

/// The real [`Engines`] (module docs).
pub struct EnginePodManager {
    client: kube::Client,
    pods: Api<Pod>,
    cfg: EnginePodConfig,
    owner: Option<OwnerReference>,
    relays: Mutex<HashMap<String, Arc<Relay>>>,
    /// fs uuid → pod name, learned from `fs.list` (and the pod label).
    by_uuid: Mutex<HashMap<String, String>>,
    /// pod name → the spec this process last created it from, so a pod
    /// deleted behind the controller's back is recreated by the next RPC
    /// that needs it, not only by the next `CreateVolume`.
    specs: Mutex<HashMap<String, Pod>>,
    /// Serializes bringing up one pod (create, wait, dial) per pod name.
    bringup: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// pod name → the last secrets a request (or the watch) brought for
    /// it, in memory only: what unlocks a replacement incarnation no
    /// request has secrets for yet (module docs).
    secrets: Mutex<HashMap<String, Secrets>>,
    /// pod name → its pool's watched Secret (`refreshing` classes).
    watched: Mutex<HashMap<String, SecretRef>>,
    refresher: Arc<Refresher>,
    /// This replica's hold annotation ([`hold_key`] of its pod name).
    hold_key: String,
    /// pod name → this process's uses of it (`controller_pods`).
    holds: Mutex<HashMap<String, HoldState>>,
    /// pod name → since when, and in how many purge passes in a row, its
    /// recorded pool failed to come up (`controller_pods`).
    bringup_failures: Mutex<HashMap<String, (Instant, u32)>>,
    this: std::sync::Weak<EnginePodManager>,
}

impl EnginePodManager {
    /// Resolves the owner `Deployment` once (its uid goes into every
    /// `ownerReference`); a missing one is logged and engine pods are then
    /// unowned.
    pub async fn new(
        client: kube::Client,
        cfg: EnginePodConfig,
        refresher: Arc<Refresher>,
    ) -> Arc<EnginePodManager> {
        let owner = match &cfg.owner_deployment {
            Some(name) => {
                let deployments: Api<Deployment> = Api::namespaced(client.clone(), &cfg.namespace);
                match deployments.get(name).await {
                    Ok(d) => d.metadata.uid.map(|uid| OwnerReference {
                        api_version: "apps/v1".into(),
                        kind: "Deployment".into(),
                        name: name.clone(),
                        uid,
                        controller: Some(false),
                        block_owner_deletion: None,
                    }),
                    Err(e) => {
                        tracing::warn!(deployment = %name, error = %e,
                            "cannot read the owner Deployment; engine pods will be unowned");
                        None
                    }
                }
            }
            None => None,
        };
        // The replica's name (the chart passes the pod's), else one of
        // this process's own: what its holds on engine pods are keyed by.
        let identity = std::env::var("POD_NAME")
            .ok()
            .filter(|n| !n.is_empty())
            .unwrap_or_else(|| format!("constellation-csi-{}", std::process::id()));
        let manager = Arc::new_cyclic(|this| EnginePodManager {
            pods: Api::namespaced(client.clone(), &cfg.namespace),
            client,
            cfg,
            owner,
            relays: Mutex::default(),
            by_uuid: Mutex::default(),
            specs: Mutex::default(),
            bringup: Mutex::default(),
            secrets: Mutex::default(),
            watched: Mutex::default(),
            refresher,
            hold_key: hold_key(&identity),
            holds: Mutex::default(),
            bringup_failures: Mutex::default(),
            this: this.clone(),
        });
        Self::spawn_hold_renewal(Arc::downgrade(&manager));
        manager
    }

    fn bringup_lock(&self, name: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.bringup
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_default()
            .clone()
    }

    /// The pod, created from `spec` when absent and recreated when it has
    /// terminated, once it is `Ready`.
    async fn ensure_ready(&self, name: &str, spec: Option<&Pod>) -> Result<Pod, ControlError> {
        ensure_pod_ready(&self.pods, name, spec, self.cfg.ready_timeout).await
    }

    /// The cached relay into `pod`'s incarnation, if it is still up.
    fn live_relay(&self, pod: &Pod) -> Option<Arc<Relay>> {
        let name = pod.metadata.name.as_deref().unwrap_or_default();
        let uid = pod.metadata.uid.as_deref().unwrap_or_default();
        self.relays
            .lock()
            .unwrap()
            .get(name)
            .filter(|r| r.uid == uid && r.client.is_connected())
            .cloned()
    }

    /// A new relay into `pod` (`constellation control-relay` through the
    /// exec API), bounded by `timeout` per call; not cached.
    async fn exec_relay(&self, pod: &Pod, timeout: Duration) -> Result<Relay, ControlError> {
        let name = pod.metadata.name.clone().unwrap_or_default();
        let uid = pod.metadata.uid.clone().unwrap_or_default();
        let attach = AttachParams {
            container: Some(ENGINE_CONTAINER.into()),
            stdin: true,
            stdout: true,
            stderr: true,
            tty: false,
            ..Default::default()
        };
        let mut process = self
            .pods
            .exec(
                &name,
                [ENGINE_BIN, "control-relay", "--socket", POD_SOCKET],
                &attach,
            )
            .await
            .map_err(|e| kube_err("starting the control relay", e))?;
        let (Some(stdout), Some(stdin)) = (process.stdout(), process.stdin()) else {
            return Err(ControlError::unavailable("the exec stream has no stdio"));
        };
        if let Some(stderr) = process.stderr() {
            let pod_name = name.clone();
            tokio::spawn(async move {
                let mut lines = tokio::io::BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::debug!(pod = %pod_name, "control-relay: {line}");
                }
            });
        }
        let transport = Arc::new(StreamTransport::new(
            tokio::io::join(stdout, stdin),
            Principal::InProcess,
        ));
        let client = Client::from_transport(transport, ClientOptions::default()).await?;
        Ok(Relay {
            uid,
            client: Arc::new(SocketControlClient::new(client).with_timeout(timeout)),
            process,
            unlocked: Mutex::new(None),
        })
    }

    /// A relay for every later call, cached.
    async fn dial_relay(&self, pod: &Pod) -> Result<Arc<Relay>, ControlError> {
        let name = pod.metadata.name.clone().unwrap_or_default();
        let relay = Arc::new(
            self.exec_relay(pod, crate::control_client::DEFAULT_CALL_TIMEOUT)
                .await?,
        );
        self.relays
            .lock()
            .unwrap()
            .insert(name.clone(), relay.clone());
        tracing::debug!(pod = %name, "control relay connected");
        Ok(relay)
    }

    /// The credentials to unlock pod `name` with when no request brought
    /// any: the last ones seen, else its watched Secret's.
    async fn known_secrets(&self, name: &str) -> Option<Secrets> {
        if let Some(have) = self.secrets.lock().unwrap().get(name).cloned() {
            return Some(have);
        }
        let watched = self.watched.lock().unwrap().get(name).cloned()?;
        let data = self
            .refresher
            .current(&watched, Duration::from_secs(10))
            .await?;
        Some((*data).clone())
    }

    /// `fs.unlock` over a relay of its own: the first one of an
    /// incarnation reaches its credential gate, which answers once the
    /// engine runs and then closes its connections (module docs). Bounded
    /// by the ready timeout: the engine starts before the answer.
    async fn unlock_once(&self, pod: &Pod, secrets: &Secrets) -> Result<(), ControlError> {
        let name = pod.metadata.name.as_deref().unwrap_or_default();
        let Some(params) = unlock_params(&pod_unlock_target(pod), secrets) else {
            return Err(ControlError::unavailable(format!(
                "engine pod {name} waits for its credentials, and the secret this request \
                 carries holds neither aws_access_key_id/aws_secret_access_key nor \
                 e2e_passphrase"
            )));
        };
        let relay = self.exec_relay(pod, self.cfg.ready_timeout).await?;
        relay.client.fs_unlock(params).await.map_err(|e| {
            ControlError::new(
                e.kind,
                format!("fs.unlock on engine pod {name}: {}", e.message),
            )
        })?;
        tracing::info!(pod = name, "engine pod unlocked");
        Ok(())
    }

    /// Bring `name` up (from `spec` if it must be created), unlock it if it
    /// waits for its credentials, push `secrets` to it if they are new to
    /// it, and connect (module docs).
    ///
    /// The RPC holds the pod while it uses the client (`controller_pods`):
    /// a pod another replica is retiring is waited out (bounded by the
    /// ready timeout), then brought up again.
    async fn connect(
        &self,
        name: &str,
        spec: Option<&Pod>,
        secrets: &Secrets,
    ) -> Result<PoolClient, ControlError> {
        let lock = self.bringup_lock(name);
        let _guard = lock.lock().await;
        self.remember(name, secrets);
        let deadline = Instant::now() + self.cfg.ready_timeout;
        loop {
            let pod = self.ensure_ready(name, spec).await?;
            match self.hold(name, &pod).await? {
                Held::Yes(hold) => {
                    let relay = self.attach(name, &pod, secrets).await?;
                    return Ok(PoolClient {
                        relay,
                        _hold: Some(hold),
                    });
                }
                Held::Retiring if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_secs(1)).await
                }
                Held::Retiring => return Err(retiring_error(name)),
            }
        }
    }

    /// Keep `secrets` (if they hold credentials) as pod `name`'s, to unlock
    /// a later incarnation with.
    fn remember(&self, name: &str, secrets: &Secrets) {
        if unlock_params("", secrets).is_some() {
            self.secrets
                .lock()
                .unwrap()
                .insert(name.to_string(), secrets.clone());
        }
    }

    /// [`Self::connect`] to `pod`, `Ready` already; the caller holds its
    /// bring-up lock.
    async fn attach(
        &self,
        name: &str,
        pod: &Pod,
        secrets: &Secrets,
    ) -> Result<Arc<Relay>, ControlError> {
        let pod = pod.clone();
        let awaits = awaits_unlock(&pod);
        let relay = match self.live_relay(&pod) {
            Some(relay) => relay,
            None if awaits => match self.known_secrets(name).await {
                Some(known) => {
                    self.unlock_once(&pod, &known).await?;
                    let relay = self.dial_relay(&pod).await?;
                    *relay.unlocked.lock().unwrap() = Some(fingerprint(&known));
                    relay
                }
                None => {
                    // Perhaps unlocked by an earlier controller process:
                    // the daemon answers; a gate says it waits.
                    let relay = self.dial_relay(&pod).await?;
                    if let Err(e) = relay.client.fs_list().await {
                        self.relays.lock().unwrap().remove(name);
                        return Err(if is_awaiting_unlock(&e) {
                            ControlError::unavailable(format!(
                                "engine pod {name} waits for its credentials, and neither this \
                                 request nor this controller has them (the class's \
                                 provisioner/controller-expand secret, or its watched Secret)"
                            ))
                        } else {
                            e
                        });
                    }
                    relay
                }
            },
            None => self.dial_relay(&pod).await?,
        };
        // A request with other credentials than the incarnation has: a
        // rotation, in place (the daemon swaps its credential source).
        if awaits && unlock_params("", secrets).is_some() {
            let fp = fingerprint(secrets);
            if *relay.unlocked.lock().unwrap() != Some(fp) {
                if let Some(params) = unlock_params(&pod_unlock_target(&pod), secrets) {
                    relay.client.fs_unlock(params).await?;
                    *relay.unlocked.lock().unwrap() = Some(fp);
                }
            }
        }
        self.learn_uuid(&pod, &relay).await?;
        Ok(relay)
    }

    /// For a `refreshing` class: remember pod `name`'s Secret and push each
    /// change of it to the pod, from a task of its own (once per pod).
    fn watch_rotations(&self, name: &str, pool: &PoolRef) {
        let Some(secret) = pool.class.credential_secret.clone() else {
            return;
        };
        if pool.class.credentials != CredentialMode::Refreshing {
            return;
        }
        let first = self
            .watched
            .lock()
            .unwrap()
            .insert(name.to_string(), secret.clone())
            .is_none();
        if !first {
            return;
        }
        let latest = self.refresher.subscribe(&secret);
        let (this, landed_in) = (self.this.clone(), self.this.clone());
        let (pod, landed_pod) = (name.to_string(), name.to_string());
        tokio::spawn(crate::credentials::follow_rotations(
            latest,
            secret,
            name.to_string(),
            move |data| {
                let this = this.clone();
                let pod = pod.clone();
                async move {
                    match this.upgrade() {
                        Some(manager) => manager.rotate(&pod, &data).await,
                        None => Ok(false),
                    }
                }
            },
            move |data| {
                if let Some(manager) = landed_in.upgrade() {
                    manager.remember(&landed_pod, data);
                }
            },
        ));
    }

    /// Push `data` to pod `name` now, if it is up and has other
    /// credentials (`Ok(true)`); otherwise (`Ok(false)`) it is what the
    /// next incarnation gets. An error is the engine's refusal, or no
    /// answer.
    async fn rotate(&self, name: &str, data: &Secrets) -> Result<bool, ControlError> {
        if unlock_params("", data).is_none() {
            return Ok(false);
        }
        let relay = self.relays.lock().unwrap().get(name).cloned();
        let Some(relay) = relay.filter(|r| r.client.is_connected()) else {
            return Ok(false);
        };
        let fp = fingerprint(data);
        if *relay.unlocked.lock().unwrap() == Some(fp) {
            return Ok(false);
        }
        let target = match self.pods.get_opt(name).await {
            Ok(Some(pod)) if awaits_unlock(&pod) => pod_unlock_target(&pod),
            Ok(_) => return Ok(false),
            Err(e) => {
                return Err(kube_err(
                    "reading the engine pod to rotate its credentials",
                    e,
                ))
            }
        };
        let Some(params) = unlock_params(&target, data) else {
            return Ok(false);
        };
        relay.client.fs_unlock(params).await?;
        *relay.unlocked.lock().unwrap() = Some(fp);
        Ok(true)
    }

    /// Learn (and label) the filesystem uuid `pod` serves, once per pod.
    async fn learn_uuid(&self, pod: &Pod, relay: &Relay) -> Result<(), ControlError> {
        let name = pod.metadata.name.clone().unwrap_or_default();
        let labelled = pod
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(LABEL_FS_UUID))
            .cloned();
        let uuid = match labelled {
            Some(uuid) => uuid,
            None => {
                let listing = relay
                    .client
                    .raw()
                    .call_bounded::<FsList>(Default::default(), Duration::from_secs(30))
                    .await?;
                // An engine pod's registry is empty: the one entry is the
                // filesystem the daemon serves.
                let Some(own) = listing.filesystems.iter().find(|f| f.name.is_none()) else {
                    return Err(ControlError::failed(format!(
                        "engine pod {name} reports no filesystem of its own"
                    )));
                };
                let patch = serde_json::json!({
                    "metadata": { "labels": { LABEL_FS_UUID: own.uuid } }
                });
                self.pods
                    .patch(&name, &PatchParams::default(), &Patch::Merge(&patch))
                    .await
                    .map_err(|e| kube_err("labelling the engine pod", e))?;
                own.uuid.clone()
            }
        };
        self.by_uuid.lock().unwrap().insert(uuid, name);
        Ok(())
    }

    /// The controller-owned pod that served `fs_uuid` last (cached, else by
    /// its label); it may be gone since.
    async fn pod_for(&self, fs_uuid: &str) -> Result<Option<String>, ControlError> {
        if let Some(name) = self.by_uuid.lock().unwrap().get(fs_uuid).cloned() {
            return Ok(Some(name));
        }
        // The controller's own pods only: node-owned ones carry the label
        // too, and are no business of the controller's.
        let selector =
            format!("{LABEL_COMPONENT}=engine,{LABEL_OWNER}=controller,{LABEL_FS_UUID}={fs_uuid}");
        let pods = self
            .pods
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(|e| kube_err("listing engine pods", e))?;
        Ok(pods.items.into_iter().filter_map(|p| p.metadata.name).min())
    }

    /// Whether a `PersistentVolume` of this driver has `handle`.
    async fn pv_named(&self, handle: &str) -> Result<bool, ControlError> {
        let pvs: Api<PersistentVolume> = Api::all(self.client.clone());
        let mut params = ListParams::default().limit(500);
        loop {
            let page = pvs
                .list(&params)
                .await
                .map_err(|e| kube_err("listing PersistentVolumes", e))?;
            if page.items.iter().any(|pv| {
                pv.spec
                    .as_ref()
                    .and_then(|s| s.csi.as_ref())
                    .is_some_and(|c| {
                        c.driver == crate::identity::DRIVER_NAME && c.volume_handle == handle
                    })
            }) {
                return Ok(true);
            }
            match page.metadata.continue_ {
                Some(token) if !token.is_empty() => params = params.continue_token(&token),
                _ => return Ok(false),
            }
        }
    }

    /// Whether a `VolumeSnapshotContent` of this driver has `handle` (as
    /// the snapshot it made, or as a pre-provisioned one's source). No
    /// snapshot CRDs installed: no such object can exist.
    async fn content_named(&self, handle: &str) -> Result<bool, ControlError> {
        let resource = kube::core::ApiResource {
            group: "snapshot.storage.k8s.io".into(),
            version: "v1".into(),
            api_version: "snapshot.storage.k8s.io/v1".into(),
            kind: "VolumeSnapshotContent".into(),
            plural: "volumesnapshotcontents".into(),
        };
        let contents: Api<kube::core::DynamicObject> =
            Api::all_with(self.client.clone(), &resource);
        let mut params = ListParams::default().limit(500);
        loop {
            let page = match contents.list(&params).await {
                Ok(page) => page,
                Err(e) if is_status(&e, 404) => return Ok(false),
                Err(e) => return Err(kube_err("listing VolumeSnapshotContents", e)),
            };
            if page.items.iter().any(|c| {
                let d = &c.data;
                d["spec"]["driver"] == crate::identity::DRIVER_NAME
                    && (d["status"]["snapshotHandle"] == handle
                        || d["spec"]["source"]["snapshotHandle"] == handle)
            }) {
                return Ok(true);
            }
            match page.metadata.continue_ {
                Some(token) if !token.is_empty() => params = params.continue_token(&token),
                _ => return Ok(false),
            }
        }
    }

    /// The pool serving `fs_uuid`, rebuilt from the cluster's PVs and
    /// `StorageClass`es, with its provisioner secret's data when the
    /// controller may read it (module docs).
    async fn rebuild_pool(&self, fs_uuid: &str) -> Result<PoolRef, ControlError> {
        let pvs: Api<PersistentVolume> = Api::all(self.client.clone());
        let mut params = ListParams::default().limit(500);
        let found = loop {
            let page = pvs
                .list(&params)
                .await
                .map_err(|e| kube_err("listing PersistentVolumes", e))?;
            if let Some(hit) = page
                .items
                .into_iter()
                .find_map(|pv| pool_shard_of(&pv, fs_uuid).map(|shard| (pv, shard)))
            {
                break Some(hit);
            }
            match page.metadata.continue_ {
                Some(token) if !token.is_empty() => params = params.continue_token(&token),
                _ => break None,
            }
        };
        let Some((pv, shard)) = found else {
            // Nothing names this filesystem any more: retryable, never a
            // silent "deleted" (the filesystem may well still hold data).
            return Err(ControlError::unavailable(format!(
                "no engine pod serves filesystem {fs_uuid}, and no PersistentVolume of this \
                 driver names it to rebuild one from"
            )));
        };
        let pv_name = pv.metadata.name.clone().unwrap_or_default();
        let class_name = pv
            .spec
            .as_ref()
            .and_then(|s| s.storage_class_name.clone())
            .filter(|n| !n.is_empty())
            .ok_or_else(|| {
                ControlError::unavailable(format!(
                    "PersistentVolume {pv_name} names no StorageClass to rebuild filesystem \
                     {fs_uuid}'s engine pod from"
                ))
            })?;
        let classes: Api<StorageClass> = Api::all(self.client.clone());
        let class = classes
            .get(&class_name)
            .await
            .map_err(|e| kube_err(&format!("reading StorageClass {class_name}"), e))?;
        let (mut pool, secret_ref) = pool_from_pv(&pv, &class, shard).map_err(|why| {
            ControlError::unavailable(format!("rebuilding {pv_name}'s pool: {why}"))
        })?;
        if let Some((namespace, name)) = secret_ref {
            if let Some(data) = self.read_secret(&namespace, &name, fs_uuid).await {
                pool.secrets = data;
            }
        }
        tracing::info!(fs_uuid, pv = %pv_name, class = %class_name,
            "rebuilding a lost engine pod's spec from its PersistentVolume");
        Ok(pool)
    }

    /// A rebuilt pool's provisioner secret's data, if the controller may
    /// read it. Not readable (another namespace, plan §9) or gone: the pod
    /// comes up and waits, and a request that carries the secret unlocks
    /// it.
    async fn read_secret(&self, namespace: &str, name: &str, fs_uuid: &str) -> Option<Secrets> {
        let secrets: Api<Secret> = Api::namespaced(self.client.clone(), namespace);
        match secrets.get_opt(name).await {
            Ok(Some(secret)) => Some(
                secret
                    .data
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(k, v)| (k, String::from_utf8_lossy(&v.0).into_owned()))
                    .collect(),
            ),
            Ok(None) => {
                tracing::warn!(fs_uuid, secret = %format!("{namespace}/{name}"),
                    "the rebuilt pool's provisioner secret does not exist");
                None
            }
            Err(e) => {
                tracing::warn!(fs_uuid, secret = %format!("{namespace}/{name}"),
                    error = %e, "cannot read the rebuilt pool's provisioner secret");
                None
            }
        }
    }
}

fn retiring_error(name: &str) -> ControlError {
    ControlError::unavailable(format!(
        "engine pod {name} is being retired (reaped or replaced) by another controller replica; \
         retry"
    ))
}

/// The `fs.unlock` target of a running engine pod: the `--s3` URL its
/// annotation records ([`unlock_target`] for its pool).
fn pod_unlock_target(pod: &Pod) -> String {
    pod.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(ANNOTATION_S3))
        .cloned()
        .unwrap_or_default()
}

/// A terminated pod's spec, as a fresh pod object.
fn respawn(old: &Pod) -> Pod {
    Pod {
        metadata: ObjectMeta {
            name: old.metadata.name.clone(),
            namespace: old.metadata.namespace.clone(),
            labels: old.metadata.labels.clone(),
            annotations: controller_pods::without_holds(old.metadata.annotations.clone()),
            owner_references: old.metadata.owner_references.clone(),
            ..Default::default()
        },
        spec: old.spec.clone().map(|mut spec| {
            // The scheduler chose a node for the old pod; let it choose
            // again (§7: never pinned).
            spec.node_name = None;
            spec
        }),
        status: None,
    }
}

#[async_trait]
impl Engines for EnginePodManager {
    async fn pool(&self, pool: &PoolRef) -> Result<Arc<dyn ControlClient>, ControlError> {
        let spec = engine_pod(pool, &self.cfg, self.owner.as_ref());
        let name = pod_name(pool);
        self.specs
            .lock()
            .unwrap()
            .insert(name.clone(), spec.clone());
        self.watch_rotations(&name, pool);
        Ok(Arc::new(
            self.connect(&name, Some(&spec), &pool.secrets).await?,
        ))
    }

    async fn filesystem(
        &self,
        fs_uuid: &str,
        secrets: &Secrets,
    ) -> Result<Arc<dyn ControlClient>, ControlError> {
        let name = self.pod_for(fs_uuid).await?;
        if let Some(name) = name {
            let spec = self.specs.lock().unwrap().get(&name).cloned();
            let present = spec.is_some()
                || self
                    .pods
                    .get_opt(&name)
                    .await
                    .map_err(|e| kube_err("reading the engine pod", e))?
                    .is_some();
            if present {
                return Ok(Arc::new(self.connect(&name, spec.as_ref(), secrets).await?));
            }
        }
        // No pod and no spec: rebuild both from the cluster (module docs).
        let mut pool = self.rebuild_pool(fs_uuid).await?;
        if unlock_params("", secrets).is_some() {
            pool.secrets = secrets.clone();
        }
        let spec = engine_pod(&pool, &self.cfg, self.owner.as_ref());
        let name = pod_name(&pool);
        self.specs
            .lock()
            .unwrap()
            .insert(name.clone(), spec.clone());
        self.watch_rotations(&name, &pool);
        let client = self.connect(&name, Some(&spec), &pool.secrets).await?;
        let serves = self.by_uuid.lock().unwrap().get(fs_uuid).cloned();
        if serves.as_deref() != Some(name.as_str()) {
            return Err(ControlError::failed(format!(
                "the engine pod {name} rebuilt for filesystem {fs_uuid} serves another \
                 filesystem: its pool's S3 prefix no longer holds {fs_uuid}"
            )));
        }
        Ok(Arc::new(client))
    }

    async fn running(
        &self,
        fs_uuid: &str,
        secrets: &Secrets,
    ) -> Result<Option<Arc<dyn ControlClient>>, ControlError> {
        let Some(name) = self.pod_for(fs_uuid).await? else {
            return Ok(None);
        };
        let lock = self.bringup_lock(&name);
        let _guard = lock.lock().await;
        let deadline = Instant::now() + self.cfg.ready_timeout;
        loop {
            let Some(pod) = wait_existing_ready(&self.pods, &name, self.cfg.ready_timeout).await?
            else {
                return Ok(None);
            };
            // Held for the delete's calls; a pod being retired by another
            // replica is waited out (gone: none runs).
            match self.hold(&name, &pod).await? {
                Held::Yes(hold) => {
                    self.remember(&name, secrets);
                    let relay = self.attach(&name, &pod, secrets).await?;
                    return Ok(Some(Arc::new(PoolClient {
                        relay,
                        _hold: Some(hold),
                    })));
                }
                Held::Retiring if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_secs(1)).await
                }
                Held::Retiring => return Err(retiring_error(&name)),
            }
        }
    }

    async fn named(&self, handle: Handle<'_>) -> Result<bool, ControlError> {
        match handle {
            Handle::Volume(h) => self.pv_named(h).await,
            Handle::Snapshot(h) => self.content_named(h).await,
        }
    }

    /// Deleted only when no RPC of this or another controller replica
    /// uses it (`controller_pods`: holds and the retiring mark), after it
    /// left the pool's registry.
    async fn retire(&self, fs_uuid: &str) -> Result<(), ControlError> {
        let Some(name) = self.pod_for(fs_uuid).await? else {
            return Ok(());
        };
        self.retire_started(&name).await
    }

    async fn record_trash(&self, fs_uuid: &str, volume_id: &str) -> Result<(), ControlError> {
        self.record_pool_of(fs_uuid, volume_id).await
    }

    async fn running_filesystems(&self) -> Result<Vec<String>, ControlError> {
        let selector = format!("{LABEL_COMPONENT}=engine,{LABEL_OWNER}=controller");
        let pods = self
            .pods
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(|e| kube_err("listing engine pods", e))?;
        let mut uuids: Vec<String> = pods
            .items
            .iter()
            .filter(|p| p.metadata.deletion_timestamp.is_none())
            .filter(|p| !matches!(pod_phase(p), "Failed" | "Succeeded"))
            .filter_map(|p| p.metadata.labels.as_ref()?.get(LABEL_FS_UUID).cloned())
            .collect();
        uuids.sort();
        uuids.dedup();
        Ok(uuids)
    }
}

/// Taints that say a node is going away (plan 37 §7 "Drain"): the
/// cluster autoscaler's, before it drains a node to delete it, and the
/// out-of-service taint of a node that is down for good.
const DRAIN_TAINTS: [&str; 2] = [
    "ToBeDeletedByClusterAutoscaler",
    "node.kubernetes.io/out-of-service",
];

/// Whether and how `node` is being drained: deleted or tainted to go
/// (condemned), else cordoned.
pub fn node_drain(node: &k8s_openapi::api::core::v1::Node) -> crate::node::Drain {
    use crate::node::Drain;
    let spec = node.spec.as_ref();
    if node.metadata.deletion_timestamp.is_some()
        || spec
            .and_then(|s| s.taints.as_ref())
            .is_some_and(|t| t.iter().any(|t| DRAIN_TAINTS.contains(&t.key.as_str())))
    {
        Drain::Condemned
    } else if spec.and_then(|s| s.unschedulable).unwrap_or(false) {
        Drain::Cordoned
    } else {
        Drain::No
    }
}

/// Where a unit's node identity waits while its engine leaves the pool's
/// registry ([`NodeEngines::set_identity_aside`]).
fn leaving_name(unit: &str) -> String {
    format!(".leaving-{unit}")
}

fn unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// A controller-owned pod about to be deleted on purpose leaves the
/// pool's registry first (its state is an `emptyDir`: whatever runs next
/// under its name is a new node). Best effort: a refusal (an open epoch, a
/// stranded journal) is logged and the pod goes anyway; the purge worker's
/// registry sweep retires the record later. Whether the engine left.
async fn leave_before_delete(name: &str, client: &SocketControlClient) -> bool {
    match client
        .node_leave(LeaveParams {
            node_id: None,
            force: false,
        })
        .await
    {
        Ok(_) => {
            tracing::info!(pod = name, "engine pod left the pool's registry");
            true
        }
        Err(e) => {
            tracing::warn!(pod = name, error = %e.message,
                "engine pod could not leave the registry; its record is swept later");
            false
        }
    }
}

/// Pod `name` once `Ready`, if it exists and is neither terminating nor
/// terminated; `None` otherwise. Never creates or replaces it (what
/// [`Engines::running`] promises). Polled like [`ensure_pod_ready`].
async fn wait_existing_ready(
    pods: &Api<Pod>,
    name: &str,
    ready_timeout: Duration,
) -> Result<Option<Pod>, ControlError> {
    let deadline = Instant::now() + ready_timeout;
    let mut backoff = Duration::from_millis(200);
    loop {
        let pod = pods
            .get_opt(name)
            .await
            .map_err(|e| kube_err("reading the engine pod", e))?;
        let Some(pod) = pod else { return Ok(None) };
        if pod.metadata.deletion_timestamp.is_some()
            || matches!(pod_phase(&pod), "Failed" | "Succeeded")
        {
            return Ok(None);
        }
        if pod_ready(&pod) {
            return Ok(Some(pod));
        }
        if Instant::now() >= deadline {
            return Err(ControlError::new(
                ErrorKind::Timeout,
                format!(
                    "engine pod {name} is not ready after {ready_timeout:?} ({})",
                    pod_waiting_on(&pod)
                ),
            ));
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(3));
    }
}

/// The node plugin's own engine pods (plan 37 §7 "Creation"): the real
/// [`NodeEngines`].
///
/// One pod per (pool filesystem [shard], node), `constellation-engine-<unit>-<node>`,
/// built like the controller's ([`node_engine_pod`]) but **pinned** to this
/// node, never `--create`-ing a filesystem, labelled `constellation.replicix.com/owner:
/// node` and `constellation.replicix.com/node`, and owned by the node plugin's
/// `DaemonSet` — not by the plugin *pod*: the plan's §7 asks for the pod, so
/// that an engine pod cannot outlive the plugin generation that made it,
/// but every rolling update of the plugin would then delete every engine
/// pod on the node and every mount with it, which is the outage §2.1 puts
/// engine pods in their own pods to avoid. Uninstalling the driver still
/// takes them (the `DaemonSet` goes). Its hostPaths are §7's own,
/// `<hostRoot>/{node-identity,sockets}/<unit>/`, made by the plugin owned
/// by the engine's uid before the pod is created ([`prepare_unit_dirs`]):
/// the meta store survives a container restart, and the node identity
/// with it.
///
/// **Reaching it.** The plugin is on the same node, so it dials the pod's
/// control socket directly through the shared hostPath — the only
/// transport that carries `view.mount`'s descriptor. The pod runs as the
/// engine's uid and the plugin as root, so the daemon would refuse it;
/// the plugin writes a service grant for uid 0 on that socket
/// ([`node_engine_policy`]) into `<hostRoot>/policy/<unit>/` — root-owned,
/// mounted read-only into the pod, which reads it through
/// `CONSTELLATION_CONTROL_POLICY` — and never into `sockets/<unit>/`, which
/// the engine owns and could plant a symlink in ([`write_private_file`]).
///
/// **Credentials** ([`crate::credentials`]). The node plugin has no
/// permission on Secrets but the ones `refreshing` classes name: what
/// unlocks a node's engine pod is the request's node-stage secret
/// (kubelet resolves it), sent with `fs.unlock` over a connection of its
/// own once per incarnation, and again whenever the plugin is handed other
/// credentials for it (a later stage, a watched Secret's change).
pub struct NodeEnginePods {
    pods: Api<Pod>,
    cfg: EnginePodConfig,
    node: String,
    owner: Option<OwnerReference>,
    /// pod name → (incarnation, connection) of the incarnation last dialled.
    clients: Mutex<HashMap<String, (String, Arc<SocketControlClient>)>>,
    /// pod name → (incarnation, fingerprint) of the last `fs.unlock` this
    /// process sent it.
    unlocked: Mutex<HashMap<String, (String, [u8; 32])>>,
    bringup: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl NodeEnginePods {
    /// Resolves the owner `DaemonSet` once; a missing one is logged and the
    /// engine pods are then unowned.
    pub async fn new(client: kube::Client, cfg: EnginePodConfig, node: String) -> NodeEnginePods {
        let owner = match &cfg.owner_daemonset {
            Some(name) => {
                let sets: Api<DaemonSet> = Api::namespaced(client.clone(), &cfg.namespace);
                match sets.get(name).await {
                    Ok(d) => d.metadata.uid.map(|uid| OwnerReference {
                        api_version: "apps/v1".into(),
                        kind: "DaemonSet".into(),
                        name: name.clone(),
                        uid,
                        controller: Some(false),
                        block_owner_deletion: None,
                    }),
                    Err(e) => {
                        tracing::warn!(daemonset = %name, error = %e,
                            "cannot read the owner DaemonSet; engine pods will be unowned");
                        None
                    }
                }
            }
            None => None,
        };
        NodeEnginePods {
            pods: Api::namespaced(client, &cfg.namespace),
            cfg,
            node,
            owner,
            clients: Mutex::default(),
            unlocked: Mutex::default(),
            bringup: Mutex::default(),
        }
    }

    /// Every live (not terminated, not being deleted) node pod of `unit`
    /// on this node, by generation.
    async fn unit_pods(&self, unit: &str) -> Result<Vec<Pod>, ControlError> {
        let selector = format!(
            "{LABEL_OWNER}=node,{LABEL_UNIT}={unit},{LABEL_NODE}={}",
            label_value(&self.node)
        );
        let mut pods: Vec<Pod> = self
            .pods
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(|e| kube_err("listing the engine pods", e))?
            .items
            .into_iter()
            .filter(|p| p.spec.as_ref().and_then(|s| s.node_name.as_deref()) == Some(&self.node))
            .collect();
        pods.sort_by_key(pod_generation);
        Ok(pods)
    }

    /// The pod serving `unit` on this node: the newest ready one. During a
    /// handoff the replacement is not ready (it binds its control socket
    /// only once it serves), so this is the old pod until the cutover.
    async fn serving(&self, unit: &str) -> Result<Option<Pod>, ControlError> {
        Ok(self
            .unit_pods(unit)
            .await?
            .into_iter()
            .rev()
            .find(|p| pod_ready(p) && p.metadata.deletion_timestamp.is_none()))
    }

    /// The generation `unit`'s pod is (or is to be) created in: the
    /// serving pod's, else the newest live one's, else 0.
    async fn generation_of(&self, unit: &str) -> Result<u32, ControlError> {
        if let Some(pod) = self.serving(unit).await? {
            return Ok(pod_generation(&pod));
        }
        Ok(self
            .unit_pods(unit)
            .await?
            .iter()
            .filter(|p| {
                p.metadata.deletion_timestamp.is_none()
                    && !matches!(pod_phase(p), "Failed" | "Succeeded")
            })
            .map(pod_generation)
            .next()
            .unwrap_or(0))
    }

    /// `<hostRoot>/node-identity/`.
    fn identities_dir(&self) -> std::path::PathBuf {
        std::path::Path::new(&self.cfg.host_root).join("node-identity")
    }

    fn sockets_dir(&self, unit: &str) -> std::path::PathBuf {
        std::path::Path::new(&self.cfg.host_root)
            .join("sockets")
            .join(unit)
    }

    /// The pod's allowlist, in `<hostRoot>/policy/<unit>/` — a directory
    /// the engine (uid 65532) cannot write: the pod mounts it read-only,
    /// and [`write_private_file`] refuses one anybody but root could have
    /// changed. Never into `sockets/<unit>/`, which the engine owns.
    fn write_policy(&self, unit: &str) -> Result<(), ControlError> {
        let root = std::path::Path::new(&self.cfg.host_root);
        write_private_file(
            root,
            &[POLICY_DIR, unit],
            POLICY_FILE,
            node_engine_policy().as_bytes(),
        )
        .map_err(|e| {
            ControlError::failed(format!(
                "writing the engine pod's allowlist {}: {e}",
                root.join(POLICY_DIR).join(unit).join(POLICY_FILE).display()
            ))
        })?;
        prepare_unit_dirs(
            root,
            &["node-identity", "sockets"],
            unit,
            ENGINE_UID as u32,
            ENGINE_UID as u32,
        )
        .map_err(|e| ControlError::failed(format!("preparing the engine pod's directories: {e}")))
    }

    fn bringup_lock(&self, name: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.bringup
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_default()
            .clone()
    }

    /// `fs.unlock` to `pod` over a connection of its own (module docs:
    /// the gate answers nothing else and closes its connections), unless
    /// this incarnation already has exactly these credentials from us.
    async fn unlock_pod(
        &self,
        pod: &Pod,
        unit: &str,
        secrets: &Secrets,
    ) -> Result<(), ControlError> {
        let name = pod.metadata.name.clone().unwrap_or_default();
        let inc = incarnation(pod);
        let fp = fingerprint(secrets);
        let had = self.unlocked.lock().unwrap().get(&name).cloned();
        if had.as_ref() == Some(&(inc.clone(), fp)) {
            return Ok(());
        }
        let Some(params) = unlock_params(&pod_unlock_target(pod), secrets) else {
            return Ok(());
        };
        let socket = self
            .sockets_dir(unit)
            .join(control_socket_file(pod_generation(pod)));
        let client = SocketControlClient::connect_unix(&socket)
            .await
            .map_err(|e| {
                ControlError::unavailable(format!(
                    "dialling engine pod {name} at {}: {}",
                    socket.display(),
                    e.message
                ))
            })?
            .with_timeout(self.cfg.ready_timeout);
        client.fs_unlock(params).await.map_err(|e| {
            ControlError::new(
                e.kind,
                format!("fs.unlock on engine pod {name}: {}", e.message),
            )
        })?;
        let first = had.as_ref().is_none_or(|(i, _)| *i != inc);
        if first {
            // A connection made before it started was the gate's.
            self.clients.lock().unwrap().remove(&name);
        }
        self.unlocked
            .lock()
            .unwrap()
            .insert(name.clone(), (inc, fp));
        tracing::info!(pod = %name, rotation = !first, "engine pod unlocked");
        Ok(())
    }

    /// A connection to `pod` (ready), dialled anew for a new incarnation or
    /// a dead connection.
    async fn dial(&self, pod: &Pod, unit: &str) -> Result<NodeEngine, ControlError> {
        let name = pod.metadata.name.clone().unwrap_or_default();
        let uid = incarnation(pod);
        let cached = self.clients.lock().unwrap().get(&name).cloned();
        let client = match cached {
            Some((have, client)) if have == uid && client.is_connected() => client,
            _ => {
                let socket = self
                    .sockets_dir(unit)
                    .join(control_socket_file(pod_generation(pod)));
                let client = Arc::new(SocketControlClient::connect_unix(&socket).await.map_err(
                    |e| {
                        ControlError::unavailable(format!(
                            "dialling engine pod {name} at {}: {}",
                            socket.display(),
                            e.message
                        ))
                    },
                )?);
                self.clients
                    .lock()
                    .unwrap()
                    .insert(name.clone(), (uid.clone(), client.clone()));
                client
            }
        };
        Ok(NodeEngine {
            client,
            unit: unit.to_string(),
            pod: name,
            incarnation: uid,
        })
    }

    async fn delete_pod(&self, name: &str) {
        match self.pods.delete(name, &DeleteParams::default()).await {
            Ok(_) => tracing::info!(pod = name, "deleted engine pod"),
            Err(e) if is_status(&e, 404) => {}
            Err(e) => tracing::warn!(pod = name, error = %e, "deleting an engine pod"),
        }
        self.clients.lock().unwrap().remove(name);
    }

    /// Wait until pod `name` is gone (bounded by the ready timeout).
    async fn wait_gone(&self, name: &str) -> Result<(), ControlError> {
        let deadline = Instant::now() + self.cfg.ready_timeout;
        loop {
            match self.pods.get_opt(name).await {
                Ok(None) => return Ok(()),
                Ok(Some(_)) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(500)).await
                }
                Ok(Some(_)) => {
                    return Err(ControlError::unavailable(format!(
                        "engine pod {name} is still being deleted"
                    )))
                }
                Err(e) => return Err(kube_err("reading the engine pod", e)),
            }
        }
    }

    /// The desired engine settings, as a fingerprint.
    fn desired(&self) -> String {
        engine_config_fingerprint(&self.cfg)
    }

    /// An event on engine pod `pod` (best effort).
    async fn event(&self, pod: &Pod, reason: &str, message: &str, warning: bool) {
        use k8s_openapi::api::core::v1::{Event, ObjectReference};
        let name = pod.metadata.name.clone().unwrap_or_default();
        let now = k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
            k8s_openapi::jiff::Timestamp::now(),
        );
        let event = Event {
            metadata: ObjectMeta {
                generate_name: Some(format!("{name}.")),
                namespace: Some(self.cfg.namespace.clone()),
                ..Default::default()
            },
            involved_object: ObjectReference {
                api_version: Some("v1".into()),
                kind: Some("Pod".into()),
                name: Some(name.clone()),
                namespace: Some(self.cfg.namespace.clone()),
                uid: pod.metadata.uid.clone(),
                ..Default::default()
            },
            reason: Some(reason.into()),
            message: Some(message.into()),
            type_: Some(if warning { "Warning" } else { "Normal" }.into()),
            first_timestamp: Some(now.clone()),
            last_timestamp: Some(now),
            count: Some(1),
            source: Some(k8s_openapi::api::core::v1::EventSource {
                component: Some("constellation-csi-node".into()),
                host: Some(self.node.clone()),
            }),
            ..Default::default()
        };
        let events: Api<Event> =
            Api::namespaced(self.pods.clone().into_client(), &self.cfg.namespace);
        if let Err(e) = events.create(&PostParams::default(), &event).await {
            tracing::warn!(pod = %name, reason, error = %e, "recording an event");
        }
    }
}

#[async_trait]
impl NodeEngines for NodeEnginePods {
    fn names(&self, pool: &PoolRef) -> (String, String) {
        let unit = unit_name(pool);
        let pod = node_pod_name(&unit, &self.node);
        (unit, pod)
    }

    async fn engine(
        &self,
        pool: &PoolRef,
        fs_uuid: &str,
        unlock: Option<&Secrets>,
    ) -> Result<NodeEngine, ControlError> {
        let unit = unit_name(pool);
        // One bring-up per unit, whatever generation its pod is.
        let lock = self.bringup_lock(&unit);
        let _guard = lock.lock().await;
        self.write_policy(&unit)?;
        let generation = self.generation_of(&unit).await?;
        let spec = node_engine_pod_gen(
            pool,
            &self.cfg,
            &self.node,
            self.owner.as_ref(),
            fs_uuid,
            generation,
        );
        let name = node_pod_name_gen(&unit, &self.node, generation);
        let pod = ensure_pod_ready(&self.pods, &name, Some(&spec), self.cfg.ready_timeout).await?;
        if let (true, Some(secrets)) = (awaits_unlock(&pod), unlock) {
            self.unlock_pod(&pod, &unit, secrets).await?;
        }
        self.dial(&pod, &unit).await
    }

    async fn existing(&self, unit: &str) -> Result<Option<NodeEngine>, ControlError> {
        match self.serving(unit).await? {
            Some(pod) => self.dial(&pod, unit).await.map(Some),
            None => Ok(None),
        }
    }

    async fn rotate(&self, unit: &str, secrets: &Secrets) -> Result<bool, ControlError> {
        // The bring-up's lock (one per unit, whatever generation its pod
        // is): a rotation never meets a pod half brought up.
        let lock = self.bringup_lock(unit);
        let _guard = lock.lock().await;
        match self.serving(unit).await? {
            Some(pod) if awaits_unlock(&pod) => {
                self.unlock_pod(&pod, unit, secrets).await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn set_view_count(&self, unit: &str, views: usize) -> Result<(), ControlError> {
        let Some(pod) = self.serving(unit).await? else {
            return Ok(());
        };
        let name = pod.metadata.name.clone().unwrap_or_default();
        let idle_since = (views == 0).then(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs().to_string())
                .unwrap_or_default()
        });
        let patch = serde_json::json!({
            "metadata": { "annotations": {
                ANNOTATION_VIEWS: views.to_string(),
                ANNOTATION_IDLE_SINCE: idle_since,
            } }
        });
        match self
            .pods
            .patch(&name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if is_status(&e, 404) => Ok(()),
            Err(e) => Err(kube_err("annotating the engine pod", e)),
        }
    }

    async fn drifted(&self) -> Result<Vec<Drift>, ControlError> {
        let selector = format!(
            "{LABEL_OWNER}=node,{LABEL_NODE}={}",
            label_value(&self.node)
        );
        let pods = self
            .pods
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(|e| kube_err("listing the engine pods", e))?
            .items;
        let desired = self.desired();
        let mut units: BTreeMap<String, Pod> = BTreeMap::new();
        for pod in pods.into_iter().filter(|p| {
            pod_ready(p)
                && p.metadata.deletion_timestamp.is_none()
                && p.spec.as_ref().and_then(|s| s.node_name.as_deref()) == Some(&self.node)
        }) {
            let Some(unit) = pod
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(LABEL_UNIT))
                .cloned()
            else {
                continue;
            };
            // The newest ready pod is the one serving (`serving`).
            match units.get(&unit) {
                Some(have) if pod_generation(have) >= pod_generation(&pod) => {}
                _ => {
                    units.insert(unit, pod);
                }
            }
        }
        Ok(units
            .into_iter()
            .filter_map(|(unit, pod)| {
                let have = pod
                    .metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get(ANNOTATION_ENGINE_CONFIG))
                    .cloned();
                (have.as_deref() != Some(desired.as_str())).then(|| {
                    let image = pod
                        .spec
                        .as_ref()
                        .and_then(|s| s.containers.first())
                        .and_then(|c| c.image.clone())
                        .unwrap_or_default();
                    Drift {
                        unit,
                        pod: pod.metadata.name.clone().unwrap_or_default(),
                        desired: desired.clone(),
                        why: format!(
                            "engine settings {} -> {desired} (image {image} -> {})",
                            have.as_deref().unwrap_or("(none)"),
                            self.cfg.image
                        ),
                    }
                })
            })
            .collect())
    }

    async fn units(&self) -> Result<Vec<crate::node::UnitPod>, ControlError> {
        let selector = format!(
            "{LABEL_OWNER}=node,{LABEL_NODE}={}",
            label_value(&self.node)
        );
        let pods = self
            .pods
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(|e| kube_err("listing the engine pods", e))?
            .items;
        let mut units: BTreeMap<String, Pod> = BTreeMap::new();
        for pod in pods.into_iter().filter(|p| {
            p.metadata.deletion_timestamp.is_none()
                && p.spec.as_ref().and_then(|s| s.node_name.as_deref()) == Some(&self.node)
        }) {
            let Some(unit) = pod
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(LABEL_UNIT))
                .cloned()
            else {
                continue;
            };
            match units.get(&unit) {
                Some(have) if pod_generation(have) >= pod_generation(&pod) => {}
                _ => {
                    units.insert(unit, pod);
                }
            }
        }
        Ok(units
            .into_iter()
            .map(|(unit, pod)| {
                let annotation = |k: &str| {
                    pod.metadata
                        .annotations
                        .as_ref()
                        .and_then(|a| a.get(k))
                        .and_then(|v| v.parse::<u64>().ok())
                };
                crate::node::UnitPod {
                    unit,
                    pod: pod.metadata.name.clone().unwrap_or_default(),
                    ready: pod_ready(&pod),
                    views: annotation(ANNOTATION_VIEWS),
                    idle_since: annotation(ANNOTATION_IDLE_SINCE),
                    created: pod
                        .metadata
                        .creation_timestamp
                        .as_ref()
                        .map(|t| t.0.as_second().max(0) as u64),
                }
            })
            .collect())
    }

    async fn draining(&self) -> Result<crate::node::Drain, ControlError> {
        use k8s_openapi::api::core::v1::Node;
        let nodes: Api<Node> = Api::all(self.pods.clone().into_client());
        let node = nodes
            .get(&self.node)
            .await
            .map_err(|e| kube_err("reading this node", e))?;
        Ok(node_drain(&node))
    }

    async fn set_identity_aside(&self, unit: &str) -> Result<(), ControlError> {
        let identities = self.identities_dir();
        let (current, leaving) = (identities.join(unit), identities.join(leaving_name(unit)));
        if current.symlink_metadata().is_err() || leaving.symlink_metadata().is_ok() {
            // Aside already (a pass that ended early), or never made.
            return Ok(());
        }
        std::fs::rename(&current, &leaving).map_err(|e| {
            ControlError::failed(format!(
                "moving the node identity {} aside: {e}",
                current.display()
            ))
        })?;
        // A rename keeps the directory's mtime, which
        // `identities_aside` reads as "aside since": stamp it now.
        if let Err(e) = std::fs::File::open(&leaving)
            .and_then(|dir| dir.set_modified(std::time::SystemTime::now()))
        {
            tracing::warn!(unit, error = %e, "stamping the node identity set aside");
        }
        Ok(())
    }

    async fn restore_identity(&self, unit: &str) -> Result<(), ControlError> {
        let identities = self.identities_dir();
        let (current, leaving) = (identities.join(unit), identities.join(leaving_name(unit)));
        if leaving.symlink_metadata().is_err() {
            return Ok(());
        }
        if current.symlink_metadata().is_ok() {
            return Err(ControlError::failed(format!(
                "a fresh node identity {} is in place; the one aside cannot go back",
                current.display()
            )));
        }
        std::fs::rename(&leaving, &current).map_err(|e| {
            ControlError::failed(format!(
                "moving the node identity {} back: {e}",
                current.display()
            ))
        })
    }

    async fn discard_superseded_aside(&self, unit: &str) -> Result<bool, ControlError> {
        let identities = self.identities_dir();
        let (current, leaving) = (identities.join(unit), identities.join(leaving_name(unit)));
        if current.symlink_metadata().is_err() {
            return Ok(false);
        }
        let Ok(meta) = leaving.symlink_metadata() else {
            return Ok(false);
        };
        let since = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |d| d.as_secs() as i64);
        // Out of the way first (no restore can pick it up any more), then
        // removed unless a pod of the unit older than the move may still
        // mount it: `forget_unit` removes it after that pod then.
        let spent = identities.join(format!(".left-{unit}-{}-s", unix_ms()));
        std::fs::rename(&leaving, &spent).map_err(|e| {
            ControlError::failed(format!(
                "retiring the superseded node identity {}: {e}",
                leaving.display()
            ))
        })?;
        tracing::warn!(
            unit,
            "a node identity set aside for a leave was superseded by a fresh one; retired. \
             If its engine never ran the leave, its registry record stays behind"
        );
        let older = self.unit_pods(unit).await?.iter().any(|p| {
            p.metadata
                .creation_timestamp
                .as_ref()
                .is_none_or(|t| t.0.as_second() <= since)
        });
        if !older {
            if let Err(e) = std::fs::remove_dir_all(&spent) {
                tracing::warn!(path = %spent.display(), error = %e,
                    "removing a superseded node identity");
            }
        }
        Ok(true)
    }

    async fn identities_aside(&self) -> Result<Vec<(String, u64)>, ControlError> {
        let Ok(dir) = std::fs::read_dir(self.identities_dir()) else {
            return Ok(Vec::new());
        };
        Ok(dir
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                let unit = name.strip_prefix(".leaving-")?.to_string();
                let since = entry
                    .metadata()
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map_or(0, |d| d.as_secs());
                Some((unit, since))
            })
            .collect())
    }

    async fn forget_unit(&self, unit: &str) -> Result<(), ControlError> {
        let identities = self.identities_dir();
        // Aside first, atomically (a rename inside the root-owned
        // `node-identity/`): from here on no pod can start on the spent
        // state, whatever fails below. It may be aside for the leave
        // already (`.leaving-<unit>`).
        let at = unix_ms();
        for (i, spent) in [identities.join(unit), identities.join(leaving_name(unit))]
            .into_iter()
            .enumerate()
        {
            if spent.symlink_metadata().is_ok() {
                let aside = identities.join(format!(".left-{unit}-{at}-{i}"));
                std::fs::rename(&spent, &aside).map_err(|e| {
                    ControlError::failed(format!(
                        "moving the spent node identity {} aside: {e}",
                        spent.display()
                    ))
                })?;
            }
        }
        let pods = self.unit_pods(unit).await?;
        for pod in &pods {
            self.delete_pod(&pod.metadata.name.clone().unwrap_or_default())
                .await;
        }
        for pod in &pods {
            self.wait_gone(&pod.metadata.name.clone().unwrap_or_default())
                .await?;
        }
        self.unlocked.lock().unwrap().retain(|name, _| {
            !pods
                .iter()
                .any(|p| p.metadata.name.as_deref() == Some(name))
        });
        // The old identities of this unit, now that no pod mounts them
        // (`remove_dir_all` never follows a link it meets).
        let prefix = format!(".left-{unit}-");
        if let Ok(dir) = std::fs::read_dir(&identities) {
            for entry in dir.flatten() {
                if entry.file_name().to_string_lossy().starts_with(&prefix) {
                    if let Err(e) = std::fs::remove_dir_all(entry.path()) {
                        tracing::warn!(path = %entry.path().display(), error = %e,
                            "removing a spent node identity");
                    }
                }
            }
        }
        tracing::info!(
            unit,
            "node identity retired; the unit's next pod is a new node"
        );
        Ok(())
    }

    async fn start_replacement(&self, unit: &str) -> Result<Replacement, ControlError> {
        let current = self
            .serving(unit)
            .await?
            .ok_or_else(|| ControlError::unavailable(format!("no engine pod serves {unit}")))?;
        self.write_policy(unit)?;
        let generation = pod_generation(&current) + 1;
        let spec = replacement_pod(&current, &self.cfg, generation).ok_or_else(|| {
            ControlError::failed(format!(
                "engine pod {} is not a node pod this plugin can replace",
                current.metadata.name.as_deref().unwrap_or_default()
            ))
        })?;
        let name = spec.metadata.name.clone().unwrap_or_default();
        // A pod of that name is a leftover of an earlier attempt.
        if self
            .pods
            .get_opt(&name)
            .await
            .map_err(|e| kube_err("reading the engine pod", e))?
            .is_some()
        {
            self.delete_pod(&name).await;
            self.wait_gone(&name).await?;
        }
        self.pods
            .create(&PostParams::default(), &spec)
            .await
            .map_err(|e| kube_err("creating the replacement engine pod", e))?;
        tracing::info!(pod = %name, unit, generation, image = %self.cfg.image,
            "created a replacement engine pod");
        // Up once its handoff socket answers as a standby.
        let socket = self.sockets_dir(unit).join(handoff_socket_file(generation));
        let deadline = Instant::now() + self.cfg.ready_timeout;
        let mut why: String;
        loop {
            match self.pods.get_opt(&name).await {
                Ok(Some(pod)) if matches!(pod_phase(&pod), "Failed" | "Succeeded") => {
                    why = format!("it ended ({}) before waiting as a standby", pod_phase(&pod));
                    break;
                }
                Ok(Some(pod)) => why = pod_waiting_on(&pod),
                Ok(None) => why = "it was deleted".into(),
                Err(e) => why = e.to_string(),
            }
            if let Ok(client) = SocketControlClient::connect_unix(&socket).await {
                let client = client.with_timeout(Duration::from_secs(10));
                let status = client
                    .node_handoff(constellation_control::proto::types::HandoffParams {
                        target: constellation_control::proto::types::HandoffTarget::Socket,
                        phase: Some(constellation_control::proto::types::HandoffPhase::Status),
                        ..Default::default()
                    })
                    .await;
                match status.map(|r| r.state) {
                    Ok(Some(constellation_control::proto::types::HandoffState::Standby {
                        ..
                    })) => {
                        return Ok(Replacement {
                            pod: name,
                            handoff: Arc::new(client),
                        })
                    }
                    Ok(state) => why = format!("its handoff socket answers {state:?}"),
                    Err(e) => why = e.message,
                }
            }
            if Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        self.delete_pod(&name).await;
        Err(ControlError::unavailable(format!(
            "replacement engine pod {name} did not wait as a handoff standby: {why}"
        )))
    }

    async fn adopt_replacement(
        &self,
        unit: &str,
        replacement: &Replacement,
    ) -> Result<NodeEngine, ControlError> {
        let pod =
            ensure_pod_ready(&self.pods, &replacement.pod, None, self.cfg.ready_timeout).await?;
        let engine = self.dial(&pod, unit).await?;
        // The old pod exited after its commit; its object goes now.
        for old in self.unit_pods(unit).await? {
            let name = old.metadata.name.clone().unwrap_or_default();
            if name != replacement.pod {
                self.delete_pod(&name).await;
            }
        }
        self.event(
            &pod,
            "EngineHandoff",
            &format!("serving {unit}'s volumes, handed over without unmounting them (plan 37 §8)"),
            false,
        )
        .await;
        Ok(engine)
    }

    async fn discard_replacement(&self, _unit: &str, replacement: Replacement) {
        self.delete_pod(&replacement.pod).await;
    }

    async fn replacement_ended(&self, _unit: &str, replacement: &Replacement) -> Option<String> {
        let pod = match self.pods.get_opt(&replacement.pod).await {
            Ok(Some(pod)) => pod,
            Ok(None) => return Some(format!("replacement pod {} was deleted", replacement.pod)),
            // The API server, not the pod: ask again later.
            Err(_) => return None,
        };
        if pod.metadata.deletion_timestamp.is_some() {
            return Some(format!(
                "replacement pod {} is being deleted",
                replacement.pod
            ));
        }
        if matches!(pod_phase(&pod), "Failed" | "Succeeded") {
            return Some(format!(
                "replacement pod {} ended ({})",
                replacement.pod,
                pod_phase(&pod)
            ));
        }
        let restarts: i32 = pod
            .status
            .as_ref()
            .and_then(|s| s.container_statuses.as_ref())
            .map(|cs| cs.iter().map(|c| c.restart_count).sum())
            .unwrap_or(0);
        (restarts > 0).then(|| {
            format!(
                "replacement pod {}'s container restarted: the sessions it held are gone",
                replacement.pod
            )
        })
    }

    async fn retire(&self, unit: &str) -> Result<(), ControlError> {
        for pod in self.unit_pods(unit).await? {
            self.delete_pod(&pod.metadata.name.clone().unwrap_or_default())
                .await;
        }
        Ok(())
    }

    async fn report_fallback(&self, unit: &str, why: &str) {
        let pod = match self.serving(unit).await {
            Ok(Some(pod)) => pod,
            _ => return,
        };
        let name = pod.metadata.name.clone().unwrap_or_default();
        let patch = serde_json::json!({
            "metadata": { "annotations": { ANNOTATION_HANDOFF_FALLBACK: why } }
        });
        if let Err(e) = self
            .pods
            .patch(&name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
        {
            tracing::warn!(pod = %name, error = %e, "annotating the handoff fallback");
        }
        self.event(&pod, "EngineHandoffFallback", why, true).await;
    }
}

/// A pool's client: the relay's, with `fs.create` stripped of the endpoint
/// and region the pod already has in its environment (module docs). Holds
/// the relay, so the stream outlives every RPC using it, and the RPC's
/// hold on the pod (`controller_pods`; none for the purge worker's own).
struct PoolClient {
    relay: Arc<Relay>,
    _hold: Option<Hold>,
}

impl PoolClient {
    fn c(&self) -> &SocketControlClient {
        &self.relay.client
    }
}

#[async_trait]
impl ControlClient for PoolClient {
    async fn fs_create(&self, mut params: FsCreateParams) -> Result<FsCreated, ControlError> {
        params.endpoint = None;
        params.region = None;
        self.c().fs_create(params).await
    }
    async fn fs_unlock(&self, params: FsUnlockParams) -> Result<Ack, ControlError> {
        self.c().fs_unlock(params).await
    }
    async fn fs_list(&self) -> Result<FsListing, ControlError> {
        self.c().fs_list().await
    }
    async fn browse_mkdir(&self, params: MkdirParams) -> Result<FileStat, ControlError> {
        self.c().browse_mkdir(params).await
    }
    async fn browse_xattr(&self, params: XattrParams) -> Result<XattrResult, ControlError> {
        self.c().browse_xattr(params).await
    }
    async fn browse_rename(&self, params: RenameParams) -> Result<Ack, ControlError> {
        self.c().browse_rename(params).await
    }
    async fn browse_readdir(&self, path: &str) -> Result<DirectoryListing, ControlError> {
        self.c().browse_readdir(path).await
    }
    async fn browse_stat(&self, path: &str) -> Result<FileStat, ControlError> {
        self.c().browse_stat(path).await
    }
    async fn browse_delete(&self, params: BrowseDeleteParams) -> Result<Ack, ControlError> {
        self.c().browse_delete(params).await
    }
    async fn quota_get(&self, subtree: &str) -> Result<QuotaStatus, ControlError> {
        self.c().quota_get(subtree).await
    }
    async fn quota_cap(&self, subtree: &str) -> Result<Option<u64>, ControlError> {
        self.c().quota_cap(subtree).await
    }
    async fn quota_set(
        &self,
        params: crate::control_client::SubtreeQuotaParams,
    ) -> Result<QuotaStatus, ControlError> {
        self.c().quota_set(params).await
    }
    async fn snapshot_create(
        &self,
        params: SnapshotCreateParams,
    ) -> Result<SnapshotCreated, ControlError> {
        self.c().snapshot_create(params).await
    }
    async fn snapshot_delete(&self, params: SnapshotDeleteParams) -> Result<Ack, ControlError> {
        self.c().snapshot_delete(params).await
    }
    async fn snapshot_list(
        &self,
        params: SnapshotListParams,
    ) -> Result<SnapshotListing, ControlError> {
        self.c().snapshot_list(params).await
    }
    async fn snapshot_hold(
        &self,
        params: SnapshotHoldParams,
    ) -> Result<SnapshotHeld, ControlError> {
        self.c().snapshot_hold(params).await
    }
    async fn clone_create(&self, params: CloneParams) -> Result<Ack, ControlError> {
        self.c().clone_create(params).await
    }
    async fn view_mount(&self, params: ViewMountParams) -> Result<ViewInfo, ControlError> {
        self.c().view_mount(params).await
    }
    async fn view_mount_fd(
        &self,
        params: ViewMountParams,
        fd: OwnedFd,
    ) -> Result<ViewInfo, ControlError> {
        // An exec stream carries no descriptor: `NotSupported`, at once.
        self.c().view_mount_fd(params, fd).await
    }
    async fn view_list(&self, params: ViewListParams) -> Result<ViewListing, ControlError> {
        self.c().view_list(params).await
    }
    async fn view_unmount(&self, params: ViewUnmountParams) -> Result<Ack, ControlError> {
        self.c().view_unmount(params).await
    }
    async fn view_stats(&self, params: ViewStatsParams) -> Result<ViewStatsReport, ControlError> {
        self.c().view_stats(params).await
    }
    async fn node_ping(&self) -> Result<Pong, ControlError> {
        self.c().node_ping().await
    }
    async fn node_handoff(&self, params: HandoffParams) -> Result<HandoffReport, ControlError> {
        self.c().node_handoff(params).await
    }
    async fn node_leave(&self, params: LeaveParams) -> Result<Ack, ControlError> {
        self.c().node_leave(params).await
    }
    async fn node_id(&self) -> Result<u64, ControlError> {
        self.c().node_id().await
    }
    async fn peers_list(&self) -> Result<PeerListing, ControlError> {
        self.c().peers_list().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::ClassParams;

    fn pool(kv: &[(&str, &str)], shard: u32) -> PoolRef {
        let params: HashMap<String, String> = kv
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        PoolRef {
            class: ClassParams::parse(&params).unwrap(),
            shard,
            secrets: BTreeMap::new(),
        }
    }

    fn cfg() -> EnginePodConfig {
        EnginePodConfig {
            namespace: "constellation-csi".into(),
            image: "constellation-csi:dev".into(),
            image_pull_policy: Some("IfNotPresent".into()),
            host_root: "/var/lib/constellation-csi".into(),
            owner_deployment: None,
            owner_daemonset: None,
            resources: None,
            log_level: "info".into(),
            ready_timeout: Duration::from_secs(1),
            service_account: None,
        }
    }

    fn is_dns_label(s: &str) -> bool {
        !s.is_empty()
            && s.len() <= 63
            && s.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            && !s.starts_with('-')
            && !s.ends_with('-')
    }

    #[test]
    fn names_are_stable_distinct_and_valid() {
        let a = pool(&[("bucket", "b"), ("prefix", "team/Data_Pool")], 0);
        let label = pool_label(&a);
        assert!(label.starts_with("data-pool-"), "{label}");
        assert!(is_dns_label(&label), "{label}");
        // Pinned: a change renames every existing pool's pod, so it must be
        // a deliberate, versioned change.
        assert_eq!(label, pool_label(&a.clone()));
        assert_eq!(
            pod_name(&a),
            format!("constellation-engine-{label}-controller")
        );
        // Another prefix, bucket or endpoint is another pool.
        for other in [
            pool(&[("bucket", "b"), ("prefix", "team/data_pool2")], 0),
            pool(&[("bucket", "c"), ("prefix", "team/Data_Pool")], 0),
            pool(
                &[
                    ("bucket", "b"),
                    ("prefix", "team/Data_Pool"),
                    ("endpoint", "http://s3:4566"),
                ],
                0,
            ),
        ] {
            assert_ne!(pool_label(&other), label);
        }
        // Shards of one class: one pool label (§7's
        // `constellation.replicix.com/pool`), one pod per shard.
        let s0 = pool(&[("bucket", "b"), ("shards", "4")], 0);
        let s3 = pool(&[("bucket", "b"), ("shards", "4")], 3);
        assert_eq!(pool_label(&s0), pool_label(&s3));
        assert_ne!(pod_name(&s0), pod_name(&s3), "distinct filesystems");
        assert!(unit_name(&s3).ends_with("-shard-3"));
        // A long, odd prefix still yields a label and a short socket path.
        let long = pool(&[("bucket", "b"), ("prefix", &"Ü".repeat(80))], 63);
        assert!(is_dns_label(&pool_label(&long)), "{}", pool_label(&long));
        let socket = format!(
            "/var/lib/constellation-csi/sockets/{}/control.sock",
            unit_name(&long)
        );
        assert!(socket.len() < 108, "{socket}");
    }

    #[test]
    fn the_pod_is_unprivileged_unpinned_and_self_describing() {
        let mut p = pool(
            &[
                ("bucket", "b"),
                ("prefix", "pool"),
                ("endpoint", "http://floci:4566"),
                ("region", "us-east-1"),
                ("chunkSize", "1MiB"),
            ],
            0,
        );
        p.secrets
            .insert("aws_access_key_id".into(), "AKIA-not-in-the-spec".into());
        let pod = engine_pod(&p, &cfg(), None);
        let json = serde_json::to_string(&pod).unwrap();
        assert!(
            !json.contains("AKIA-not-in-the-spec"),
            "credentials never enter a pod spec"
        );
        let spec = pod.spec.as_ref().unwrap();
        assert_eq!(spec.node_name, None, "§7: never nodeName-pinned");
        assert_eq!(spec.restart_policy.as_deref(), Some("OnFailure"));
        let psc = spec.security_context.as_ref().unwrap();
        assert_eq!(psc.run_as_non_root, Some(true));
        let engine = &spec.containers[0];
        assert_restricted(engine);
        let args = engine.args.as_ref().unwrap();
        assert_eq!(args[..3], ["serve", "--s3", "s3://b/pool"]);
        assert!(args.windows(2).any(|w| w == ["--chunk-size", "1048576"]));
        // §9: the credentials come by fs.unlock, never from the environment.
        assert!(args.contains(&"--await-unlock".to_string()));
        assert!(awaits_unlock(&pod));
        let envs = engine.env.as_ref().unwrap();
        let value = |n: &str| {
            envs.iter()
                .find(|e| e.name == n)
                .and_then(|e| e.value.clone())
        };
        assert_eq!(value("AWS_ENDPOINT").as_deref(), Some("http://floci:4566"));
        assert_eq!(value("AWS_ALLOW_HTTP").as_deref(), Some("true"));
        assert_eq!(value("AWS_REGION").as_deref(), Some("us-east-1"));
        assert!(envs.iter().all(|e| e.value_from.is_none()), "{envs:?}");
        for var in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "CONSTELLATION_PASSPHRASE",
        ] {
            assert!(
                envs.iter().all(|e| e.name != var),
                "{var} in the environment"
            );
        }
        assert!(engine.readiness_probe.as_ref().unwrap().exec.is_some());
        let labels = pod.metadata.labels.as_ref().unwrap();
        assert_eq!(labels[LABEL_COMPONENT], "engine");
        assert_eq!(labels[LABEL_OWNER], "controller");
        assert_eq!(labels[LABEL_SHARD], "0");
        assert_eq!(
            pod_unlock_target(&pod),
            unlock_target(&p),
            "fs.unlock names the --s3 URL"
        );
        // PodSecurity `restricted` as a whole: emptyDirs only, no init
        // container (nothing to chown), no hostPath.
        assert!(spec.init_containers.is_none());
        let volumes = spec.volumes.as_ref().unwrap();
        assert!(
            volumes
                .iter()
                .all(|v| v.empty_dir.is_some() && v.host_path.is_none()),
            "{volumes:?}"
        );
        let names: Vec<&str> = volumes.iter().map(|v| v.name.as_str()).collect();
        assert_eq!(names, ["node-identity", "sockets", "scratch"]);
        // A class on the AWS chain does not wait; an E2E one still does.
        let chain = pool(
            &[("bucket", "b"), ("credentialSource", "aws-default-chain")],
            0,
        );
        assert!(!awaits_unlock(&engine_pod(&chain, &cfg(), None)));
        let chain_e2e = pool(
            &[
                ("bucket", "b"),
                ("credentialSource", "aws-default-chain"),
                ("e2e", "true"),
            ],
            0,
        );
        assert!(awaits_unlock(&engine_pod(&chain_e2e, &cfg(), None)));
    }

    /// PodSecurity `restricted`'s container rules (plan 37 §9).
    /// 37-k6b: a controller-owned pod records its settings, and its
    /// replacement keeps its spec (pool, arguments, volumes, name) with the
    /// new settings, unpinned again.
    #[test]
    fn a_drifted_controller_pod_is_replaced_from_its_own_spec() {
        let p = pool(&[("bucket", "b"), ("prefix", "pool")], 0);
        let old_cfg = cfg();
        let mut pod = engine_pod(&p, &old_cfg, None);
        assert!(!drifted_from(&pod, &old_cfg));
        pod.spec.as_mut().unwrap().node_name = Some("w1".into());
        pod.metadata
            .labels
            .as_mut()
            .unwrap()
            .insert(LABEL_FS_UUID.into(), "uuid-1".into());
        let mut new_cfg = cfg();
        new_cfg.image = "constellation-csi:next".into();
        new_cfg.log_level = "debug".into();
        assert!(drifted_from(&pod, &new_cfg));
        let next = controller_replacement(&pod, &new_cfg);
        assert!(!drifted_from(&next, &new_cfg));
        assert_eq!(next.metadata.name, pod.metadata.name);
        assert_eq!(
            next.metadata.labels.as_ref().unwrap()[LABEL_FS_UUID],
            "uuid-1"
        );
        let (old_spec, spec) = (pod.spec.as_ref().unwrap(), next.spec.as_ref().unwrap());
        assert_eq!(spec.node_name, None, "placed by the scheduler anew");
        assert_eq!(spec.volumes, old_spec.volumes);
        let (old_c, c) = (&old_spec.containers[0], &spec.containers[0]);
        assert_eq!(c.args, old_c.args);
        assert_eq!(c.image.as_deref(), Some("constellation-csi:next"));
        let log = c
            .env
            .as_ref()
            .unwrap()
            .iter()
            .filter(|e| e.name == "RUST_LOG")
            .map(|e| e.value.clone().unwrap_or_default())
            .collect::<Vec<_>>();
        assert_eq!(log, ["debug"]);
        assert_restricted(c);
        // An unannotated pod (made before K6b) counts as drifted.
        pod.metadata.annotations = None;
        assert!(drifted_from(&pod, &old_cfg));
    }

    #[test]
    fn engine_hostnames_are_dns_labels_that_keep_their_node() {
        let is_label = |s: &str| {
            !s.is_empty()
                && s.len() <= 63
                && s.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                && !s.starts_with('-')
                && !s.ends_with('-')
        };
        for node in [
            "kind-37-k6b-worker",
            "ip-10-0-12-34.us-west-2.compute.internal",
            &"n".repeat(253),
            "...",
        ] {
            let h = node_engine_hostname(node);
            assert!(is_label(&h), "{h:?}");
            assert!(h.starts_with(NODE_ENGINE_HOST_PREFIX));
            assert_eq!(h, node_engine_hostname(node), "stable");
        }
        assert_ne!(node_engine_hostname("w1"), node_engine_hostname("w2"));
        let p = pool(&[("bucket", "b"), ("prefix", "pool")], 0);
        let pod = node_engine_pod(&p, &cfg(), "w1", None, "uuid");
        assert_eq!(pod.spec.unwrap().hostname, Some(node_engine_hostname("w1")));
        // kubelet's own default.
        let long = "constellation-engine-csi-node-drain-d6123c0dab-kind-37-k6b-worker";
        assert_eq!(
            pod_hostname(long),
            "constellation-engine-csi-node-drain-d6123c0dab-kind-37-k6b-work"
        );
        assert_eq!(pod_hostname("a-b"), "a-b");
        assert_eq!(
            pod_hostname(&format!("{}-x", "a".repeat(62))),
            "a".repeat(62)
        );
    }

    #[test]
    fn a_cordoned_or_condemned_node_is_draining() {
        use k8s_openapi::api::core::v1::{Node, NodeSpec, Taint};
        let node = |spec: NodeSpec| Node {
            spec: Some(spec),
            ..Default::default()
        };
        use crate::node::Drain;
        assert_eq!(node_drain(&node(NodeSpec::default())), Drain::No);
        assert_eq!(
            node_drain(&node(NodeSpec {
                unschedulable: Some(true),
                ..Default::default()
            })),
            Drain::Cordoned
        );
        let tainted = |key: &str| {
            node(NodeSpec {
                taints: Some(vec![Taint {
                    key: key.into(),
                    effect: "NoSchedule".into(),
                    ..Default::default()
                }]),
                ..Default::default()
            })
        };
        assert_eq!(
            node_drain(&tainted("ToBeDeletedByClusterAutoscaler")),
            Drain::Condemned
        );
        assert_eq!(
            node_drain(&tainted("node.kubernetes.io/out-of-service")),
            Drain::Condemned
        );
        assert_eq!(node_drain(&tainted("dedicated")), Drain::No);
        let mut deleted = node(NodeSpec::default());
        deleted.metadata.deletion_timestamp =
            Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                k8s_openapi::jiff::Timestamp::now(),
            ));
        assert_eq!(node_drain(&deleted), Drain::Condemned);
    }

    fn assert_restricted(c: &Container) {
        let sc = c.security_context.as_ref().unwrap();
        assert_eq!(sc.allow_privilege_escalation, Some(false));
        assert_eq!(sc.privileged, None);
        assert_eq!(sc.run_as_non_root, Some(true));
        assert_eq!(sc.read_only_root_filesystem, Some(true));
        assert_eq!(
            sc.seccomp_profile.as_ref().map(|p| p.type_.as_str()),
            Some("RuntimeDefault")
        );
        let caps = sc.capabilities.as_ref().unwrap();
        assert_eq!(caps.drop.as_deref(), Some(&["ALL".to_string()][..]));
        assert!(caps.add.is_none());
    }

    /// Plan 37 §7: a node's engine pod is the controller's shape, pinned,
    /// never creating a filesystem, granting the node plugin its socket,
    /// and on §7's own hostPaths (no `-controller` suffix).
    #[test]
    fn a_node_engine_pod_is_pinned_and_grants_the_node_plugin() {
        let mut p = pool(&[("bucket", "b"), ("prefix", "pool"), ("shards", "2")], 1);
        p.secrets
            .insert("aws_access_key_id".into(), "AKIA-not-in-the-spec".into());
        let node = "ip-10-0-0-1.ec2.internal";
        let pod = node_engine_pod(&p, &cfg(), node, None, "uuid-1");
        assert!(!serde_json::to_string(&pod)
            .unwrap()
            .contains("AKIA-not-in-the-spec"));
        let unit = unit_name(&p);
        assert_eq!(
            pod.metadata.name.as_deref(),
            Some(format!("constellation-engine-{unit}-{node}").as_str())
        );
        let labels = pod.metadata.labels.as_ref().unwrap();
        assert_eq!(labels[LABEL_OWNER], "node");
        assert_eq!(labels[LABEL_NODE], node);
        assert_eq!(labels[LABEL_UNIT], unit);
        assert_eq!(labels[LABEL_FS_UUID], "uuid-1");
        assert_eq!(labels[LABEL_SHARD], "1");
        let spec = pod.spec.as_ref().unwrap();
        assert_eq!(spec.node_name.as_deref(), Some(node), "§7: nodeName-pinned");
        assert_eq!(spec.restart_policy.as_deref(), Some("OnFailure"));
        assert_eq!(spec.termination_grace_period_seconds, Some(600));
        assert!(spec
            .tolerations
            .as_ref()
            .is_some_and(|t| t[0].operator.as_deref() == Some("Exists")));
        let engine = &spec.containers[0];
        let args = engine.args.as_ref().unwrap();
        assert_eq!(args[..3], ["serve", "--s3", "s3://b/pool/shard-1"]);
        assert!(
            !args.contains(&"--create".to_string()),
            "never creates a filesystem"
        );
        let envs = engine.env.as_ref().unwrap();
        assert!(envs
            .iter()
            .any(|e| e.name == "CONSTELLATION_CONTROL_POLICY"
                && e.value.as_deref() == Some(POD_POLICY)));
        assert!(envs.iter().all(|e| e.value_from.is_none()), "{envs:?}");
        assert!(args.contains(&"--await-unlock".to_string()));
        assert_restricted(engine);
        assert!(spec.init_containers.is_none(), "no root init container");
        // The plugin makes the unit's directories (`prepare_unit_dirs`):
        // the pod requires them to exist rather than have kubelet create
        // them root-owned.
        assert!(spec.volumes.as_ref().unwrap().iter().all(|v| v
            .host_path
            .as_ref()
            .is_none_or(|h| h.type_.as_deref() == Some("Directory"))));
        let paths: Vec<String> = spec
            .volumes
            .as_ref()
            .unwrap()
            .iter()
            .filter_map(|v| v.host_path.as_ref().map(|h| h.path.clone()))
            .collect();
        assert_eq!(
            paths,
            [
                format!("/var/lib/constellation-csi/node-identity/{unit}"),
                format!("/var/lib/constellation-csi/sockets/{unit}"),
                format!("/var/lib/constellation-csi/policy/{unit}"),
            ]
        );
        // The grant is read from the root-owned policy directory, mounted
        // read-only, never from the sockets directory the engine owns.
        let policy = engine
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .find(|m| m.name == POLICY_DIR)
            .unwrap();
        assert_eq!(policy.read_only, Some(true));
        assert!(POD_POLICY.starts_with(&format!("{}/", policy.mount_path)));
        assert!(!POD_POLICY.starts_with(POD_SOCKET_DIR));
        // The controller's pod of the same pool stays apart.
        let controller = engine_pod(&p, &cfg(), None);
        assert_ne!(controller.metadata.name, pod.metadata.name);
        assert!(controller.spec.unwrap().containers[0]
            .args
            .as_ref()
            .unwrap()
            .contains(&"--create".to_string()));
    }

    /// Plan 37 §8: a replacement is its predecessor's pod (pool, hostPaths,
    /// credentials) with the desired engine settings, in the next
    /// generation's name and socket slot; every node pod can wait as a
    /// standby; a settings change shows as a different fingerprint.
    #[test]
    fn a_replacement_keeps_the_pod_and_moves_to_the_next_slot() {
        let p = pool(&[("bucket", "b"), ("prefix", "pool")], 0);
        let node = "w1";
        let unit = unit_name(&p);
        let first = node_engine_pod(&p, &cfg(), node, None, "uuid-1");
        let args = |pod: &Pod| {
            pod.spec.as_ref().unwrap().containers[0]
                .args
                .clone()
                .unwrap()
        };
        let flag = |pod: &Pod, f: &str| {
            let a = args(pod);
            a[a.iter().position(|x| x == f).unwrap() + 1].clone()
        };
        assert_eq!(pod_generation(&first), 0);
        assert_eq!(flag(&first, "--control-socket"), POD_SOCKET);
        assert_eq!(
            flag(&first, "--handoff-socket"),
            "/run/constellation-csi/handoff.sock"
        );
        let fingerprint = engine_config_fingerprint(&cfg());
        assert_eq!(
            first.metadata.annotations.as_ref().unwrap()[ANNOTATION_ENGINE_CONFIG],
            fingerprint
        );

        let mut upgraded = cfg();
        upgraded.image = "constellation-csi:next".into();
        upgraded.log_level = "debug".into();
        assert_ne!(engine_config_fingerprint(&upgraded), fingerprint);
        let mut running = first.clone();
        running.metadata.uid = Some("uid-old".into());
        running
            .metadata
            .annotations
            .as_mut()
            .unwrap()
            .insert(ANNOTATION_VIEWS.into(), "3".into());
        let next = replacement_pod(&running, &upgraded, 1).unwrap();
        assert_eq!(
            next.metadata.name.as_deref(),
            Some(format!("constellation-engine-{unit}-{node}-g1").as_str())
        );
        assert_eq!(next.metadata.uid, None);
        assert_eq!(pod_generation(&next), 1);
        let annotations = next.metadata.annotations.as_ref().unwrap();
        assert_eq!(
            annotations[ANNOTATION_ENGINE_CONFIG],
            engine_config_fingerprint(&upgraded)
        );
        assert!(!annotations.contains_key(ANNOTATION_VIEWS));
        assert_eq!(
            flag(&next, "--control-socket"),
            "/run/constellation-csi/control-b.sock"
        );
        assert_eq!(
            flag(&next, "--handoff-socket"),
            "/run/constellation-csi/handoff-b.sock"
        );
        // Each flag once, the pool's own arguments untouched.
        assert_eq!(
            args(&next)
                .iter()
                .filter(|a| *a == "--control-socket")
                .count(),
            1
        );
        assert_eq!(args(&next)[..3], args(&first)[..3]);
        let spec = next.spec.as_ref().unwrap();
        let engine = &spec.containers[0];
        assert_eq!(engine.image.as_deref(), Some("constellation-csi:next"));
        assert!(
            spec.init_containers.is_none(),
            "no root init container (37-k6a)"
        );
        // 37-k6a: the replacement waits for its credentials as its
        // predecessor did — they come over the handoff, never in its spec.
        assert!(awaits_unlock(&first));
        assert!(awaits_unlock(&next));
        assert!(engine.env.iter().flatten().all(|e| e.value_from.is_none()));
        let probe = engine.readiness_probe.as_ref().unwrap();
        let command = probe.exec.as_ref().unwrap().command.as_ref().unwrap();
        assert_eq!(
            command.last().map(String::as_str),
            Some("/run/constellation-csi/control-b.sock")
        );
        // A standby answers on its handoff socket only: startup and
        // liveness take that answer too (should-fix 6 of 37-k5a's review),
        // readiness does not.
        assert!(!command.contains(&"--or-socket".to_string()));
        for probe in [&engine.startup_probe, &engine.liveness_probe] {
            let command = probe
                .as_ref()
                .unwrap()
                .exec
                .as_ref()
                .unwrap()
                .command
                .clone();
            let command = command.unwrap();
            assert_eq!(
                command[command.len() - 4..],
                [
                    "--socket",
                    "/run/constellation-csi/control-b.sock",
                    "--or-socket",
                    "/run/constellation-csi/handoff-b.sock"
                ]
            );
        }
        let log = engine
            .env
            .as_ref()
            .unwrap()
            .iter()
            .filter(|e| e.name == "RUST_LOG");
        assert_eq!(
            log.map(|e| e.value.clone().unwrap()).collect::<Vec<_>>(),
            ["debug"]
        );
        assert_eq!(spec.node_name.as_deref(), Some(node));
        assert_eq!(
            spec.volumes,
            first.spec.as_ref().unwrap().volumes,
            "the same hostPaths"
        );
        // And back to the first slot for the next one.
        let third = replacement_pod(&next, &cfg(), 2).unwrap();
        assert_eq!(flag(&third, "--control-socket"), POD_SOCKET);
        assert_eq!(
            third.metadata.name.as_deref(),
            Some(format!("constellation-engine-{unit}-{node}-g2").as_str())
        );
        // Every socket either slot binds is granted to the node plugin.
        let policy = node_engine_policy();
        for g in 0..2 {
            for file in [control_socket_file(g), handoff_socket_file(g)] {
                assert!(policy.contains(&format!("socket = \"{POD_SOCKET_DIR}/{file}\"")));
            }
        }
    }

    /// Must-fix 37-k6a: the controller-owned pod's allowlist is one
    /// service grant, for the engine's uid on the pod's socket, labelled
    /// `csi-controller`; the image's copy is the same text.
    #[test]
    fn the_controllers_grant_is_in_the_image_and_named_by_the_pod() {
        let policy = controller_engine_policy();
        let baked = include_str!("../../../deploy/docker/controller-engine-control-allow.toml");
        let grants = |text: &str| {
            text.lines()
                .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        };
        assert_eq!(grants(baked), grants(&policy));
        assert!(policy.contains("principal = \"uid:65532\""));
        assert!(policy.contains("label = \"csi-controller\""));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(POLICY_FILE);
        std::fs::write(&path, baked).unwrap();
        let loaded =
            constellation_control::Policy::load(&path, Some(65532)).expect("a loadable allowlist");
        assert_eq!(loaded.grants().len(), 1);
        let pod = engine_pod(
            &pool(&[("bucket", "b"), ("prefix", "pool")], 0),
            &cfg(),
            None,
        );
        let envs = pod.spec.unwrap().containers[0].env.clone().unwrap();
        assert!(envs.iter().any(|e| e.name == "CONSTELLATION_CONTROL_POLICY"
            && e.value.as_deref() == Some(CONTROLLER_POD_POLICY)));
        let dockerfile = include_str!("../../../deploy/docker/constellation-csi.Dockerfile");
        assert!(
            dockerfile.contains(CONTROLLER_POD_POLICY),
            "the image does not install {CONTROLLER_POD_POLICY}"
        );
    }

    #[test]
    fn the_node_plugins_grant_parses_as_a_service_grant() {
        let policy = node_engine_policy();
        assert!(policy.contains("principal = \"uid:0\""));
        assert!(policy.contains(&format!("socket = \"{POD_SOCKET}\"")));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(POLICY_FILE);
        std::fs::write(&path, &policy).unwrap();
        constellation_control::Policy::load(&path, Some(65532)).expect("a loadable allowlist");
    }

    #[test]
    fn node_pod_names_stay_valid_whatever_the_node_name() {
        let unit = "pool-0123456789-shard-3";
        assert_eq!(
            node_pod_name(unit, "w1"),
            format!("constellation-engine-{unit}-w1")
        );
        let long = format!("{}.example.internal", "n".repeat(230));
        let a = node_pod_name(unit, &long);
        assert!(a.len() <= 253, "{}", a.len());
        assert!(a.starts_with(&format!("constellation-engine-{unit}-")));
        assert_eq!(a, node_pod_name(unit, &long), "stable");
        let other = format!("{}.example.internal", "n".repeat(231));
        assert_ne!(a, node_pod_name(unit, &other), "distinct");
        assert!(a
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.'));
    }

    /// Must-fix 37-k3a: the privileged plugin writes the engine's grant
    /// without following anything the unprivileged engine could plant.
    #[test]
    fn the_policy_writer_never_follows_a_planted_link() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        let victim = dir.path().join("victim");
        std::fs::write(&victim, "precious").unwrap();
        let unit_dir = root.join(POLICY_DIR).join("u");
        std::fs::create_dir_all(&unit_dir).unwrap();
        std::fs::set_permissions(
            root.join(POLICY_DIR),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        std::fs::set_permissions(&unit_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        // The reviewer's exploit: the temporary and the file itself are
        // symlinks to a file the writer may write.
        symlink(&victim, unit_dir.join(format!(".{POLICY_FILE}.tmp"))).unwrap();
        symlink(&victim, unit_dir.join(POLICY_FILE)).unwrap();
        write_private_file(&root, &[POLICY_DIR, "u"], POLICY_FILE, b"grant").unwrap();
        assert_eq!(std::fs::read_to_string(&victim).unwrap(), "precious");
        let written = unit_dir.join(POLICY_FILE);
        assert!(std::fs::symlink_metadata(&written).unwrap().is_file());
        assert_eq!(std::fs::read_to_string(&written).unwrap(), "grant");
        assert!(!unit_dir.join(format!(".{POLICY_FILE}.tmp")).exists());
        // Unchanged contents: not rewritten.
        let inode = |p: &std::path::Path| {
            std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(p).unwrap())
        };
        let before = inode(&written);
        write_private_file(&root, &[POLICY_DIR, "u"], POLICY_FILE, b"grant").unwrap();
        assert_eq!(inode(&written), before);

        // A directory in the path that is a symlink is refused…
        let elsewhere = dir.path().join("elsewhere");
        std::fs::create_dir(&elsewhere).unwrap();
        symlink(&elsewhere, root.join(POLICY_DIR).join("linked")).unwrap();
        assert!(write_private_file(&root, &[POLICY_DIR, "linked"], POLICY_FILE, b"x").is_err());
        assert!(!elsewhere.join(POLICY_FILE).exists());
        // …and so is one somebody else may write into.
        let open = root.join(POLICY_DIR).join("open");
        std::fs::create_dir(&open).unwrap();
        std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o777)).unwrap();
        let err = write_private_file(&root, &[POLICY_DIR, "open"], POLICY_FILE, b"x").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied, "{err}");
        assert!(!open.join(POLICY_FILE).exists());
        // No component may climb out.
        assert!(write_private_file(&root, &[POLICY_DIR, ".."], POLICY_FILE, b"x").is_err());
        assert!(write_private_file(&root, &["a/b"], POLICY_FILE, b"x").is_err());
    }

    /// The node plugin makes a node-owned pod's hostPath directories,
    /// owned by the engine (so no root init container chowns them), and
    /// never follows a link planted where one of them goes.
    #[test]
    fn unit_dirs_are_made_for_the_engine_and_never_through_a_link() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        // SAFETY: no preconditions.
        let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
        prepare_unit_dirs(&root, &["node-identity", "sockets"], "unit-a", uid, gid).unwrap();
        for sub in ["node-identity", "sockets"] {
            let meta = std::fs::symlink_metadata(root.join(sub).join("unit-a")).unwrap();
            assert!(meta.is_dir());
            assert_eq!((meta.uid(), meta.gid()), (uid, gid));
            assert_eq!(meta.permissions().mode() & 0o777, 0o700);
        }
        // Idempotent, and it repairs a mode.
        std::fs::set_permissions(
            root.join("sockets/unit-a"),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        prepare_unit_dirs(&root, &["sockets"], "unit-a", uid, gid).unwrap();
        let mode = std::fs::metadata(root.join("sockets/unit-a"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700);
        // A link where a unit directory goes: refused, its target untouched.
        let target = dir.path().join("elsewhere");
        std::fs::create_dir(&target).unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::os::unix::fs::symlink(&target, root.join("sockets/unit-b")).unwrap();
        let err = prepare_unit_dirs(&root, &["sockets"], "unit-b", uid, gid).unwrap_err();
        assert!(err.to_string().contains("unit-b"), "{err}");
        let mode = std::fs::metadata(&target).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o755, "followed the link");
        // A parent anyone could change is refused.
        std::fs::set_permissions(root.join("sockets"), std::fs::Permissions::from_mode(0o777))
            .unwrap();
        assert!(prepare_unit_dirs(&root, &["sockets"], "unit-a", uid, gid).is_err());
        // Not a path component: refused.
        assert!(prepare_unit_dirs(&root, &["sockets"], "../x", uid, gid).is_err());
    }

    #[test]
    fn label_values_are_valid_whatever_the_node_name() {
        assert_eq!(label_value("worker-1"), "worker-1");
        assert_eq!(
            label_value("ip-10-0-0-1.ec2.internal"),
            "ip-10-0-0-1.ec2.internal"
        );
        let long = format!("{}.example.com", "a".repeat(80));
        let v = label_value(&long);
        assert!(v.len() <= 63, "{v}");
        assert_ne!(v, label_value(&format!("{}.example.org", "a".repeat(80))));
        assert!(v.bytes().next().unwrap().is_ascii_alphanumeric());
    }

    fn pv(handle: &str, driver: &str, class: &str) -> PersistentVolume {
        use k8s_openapi::api::core::v1::{
            CSIPersistentVolumeSource, ObjectReference, PersistentVolumeSpec,
        };
        PersistentVolume {
            metadata: ObjectMeta {
                name: Some("pvc-0123".into()),
                ..Default::default()
            },
            spec: Some(PersistentVolumeSpec {
                csi: Some(CSIPersistentVolumeSource {
                    driver: driver.into(),
                    volume_handle: handle.into(),
                    ..Default::default()
                }),
                storage_class_name: Some(class.into()),
                claim_ref: Some(ObjectReference {
                    namespace: Some("tenant-a".into()),
                    name: Some("data".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            status: None,
        }
    }

    fn class(kv: &[(&str, &str)]) -> StorageClass {
        StorageClass {
            metadata: ObjectMeta {
                name: Some("pool".into()),
                ..Default::default()
            },
            provisioner: crate::identity::DRIVER_NAME.into(),
            parameters: Some(
                kv.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
            ..Default::default()
        }
    }

    /// Should-fix 2 of the 37-K2b review: a pod lost along with the
    /// controller's spec is rebuilt from a PV and its class — the same pod
    /// name, so the same hostPaths, as `CreateVolume` made.
    #[test]
    fn a_lost_pool_is_rebuilt_from_its_pv_and_class() {
        let handle = "v1/pool/2/0f5e-uuid/volumes/pvc-0123";
        let p = pv(handle, crate::identity::DRIVER_NAME, "pool");
        assert_eq!(pool_shard_of(&p, "0f5e-uuid"), Some(2));
        assert_eq!(pool_shard_of(&p, "other-uuid"), None);
        let foreign = pv(handle, "ebs.csi.aws.com", "pool");
        assert_eq!(pool_shard_of(&foreign, "0f5e-uuid"), None);
        let static_pv = pv(
            "v1/static/0f5e-uuid/data",
            crate::identity::DRIVER_NAME,
            "pool",
        );
        assert_eq!(pool_shard_of(&static_pv, "0f5e-uuid"), None);

        let kv = [
            ("bucket", "b"),
            ("prefix", "team/data"),
            ("shards", "4"),
            ("endpoint", "http://floci:4566"),
            (
                "csi.storage.k8s.io/provisioner-secret-name",
                "s3-${pvc.name}",
            ),
            (
                "csi.storage.k8s.io/provisioner-secret-namespace",
                "${pvc.namespace}",
            ),
        ];
        let (pool_ref, secret) = pool_from_pv(&p, &class(&kv), 2).unwrap();
        let created = pool(&kv, 2);
        assert_eq!(pod_name(&pool_ref), pod_name(&created));
        assert_eq!(
            engine_pod(&pool_ref, &cfg(), None),
            engine_pod(&created, &cfg(), None)
        );
        assert_eq!(secret, Some(("tenant-a".into(), "s3-data".into())));
        // Secrets never shape the pod: one with and one without are one spec.
        let mut with = created.clone();
        with.secrets.insert("aws_access_key_id".into(), "x".into());
        assert_eq!(
            engine_pod(&pool_ref, &cfg(), None),
            engine_pod(&with, &cfg(), None)
        );
        // A class with no secret, and a class that cannot hold the shard.
        let (_, none) = pool_from_pv(&p, &class(&[("bucket", "b"), ("shards", "4")]), 2).unwrap();
        assert_eq!(none, None);
        assert!(pool_from_pv(&p, &class(&[("bucket", "b")]), 2).is_err());
        assert!(pool_from_pv(&p, &class(&[("bucket", "b"), ("bogus", "1")]), 0).is_err());
    }

    /// A dedicated volume's pod is rebuilt for its own filesystem: the
    /// prefix its context recorded, else `<class prefix>/<pv name>`.
    #[test]
    fn a_lost_dedicated_filesystem_is_rebuilt_from_its_pv() {
        let mut p = pv("v1/dedicated/d-uuid/", crate::identity::DRIVER_NAME, "iso");
        assert_eq!(pool_shard_of(&p, "d-uuid"), Some(0));
        assert_eq!(pool_shard_of(&p, "other"), None);
        let kv = [("bucket", "b"), ("prefix", "iso"), ("layout", "dedicated")];
        let (rebuilt, _) = pool_from_pv(&p, &class(&kv), 0).unwrap();
        assert_eq!(rebuilt.prefix(), "iso/pvc-0123");
        p.spec
            .as_mut()
            .unwrap()
            .csi
            .as_mut()
            .unwrap()
            .volume_attributes = Some(BTreeMap::from([(
            "prefix".to_string(),
            "iso/elsewhere".to_string(),
        )]));
        let (rebuilt, _) = pool_from_pv(&p, &class(&kv), 0).unwrap();
        assert_eq!(rebuilt.prefix(), "iso/elsewhere");
    }

    #[test]
    fn debug_never_prints_secret_values() {
        let mut p = pool(&[("bucket", "b")], 0);
        p.secrets
            .insert("aws_secret_access_key".into(), "hunter2".into());
        let shown = format!("{p:?}");
        assert!(shown.contains("aws_secret_access_key"));
        assert!(!shown.contains("hunter2"));
    }
}
