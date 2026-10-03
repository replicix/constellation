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
//! never share a pod. `constellation.dev/pool` carries `<pool>` (one value
//! per pool, §7) and `constellation.dev/shard` the shard. Its labels say what it is (`app.kubernetes.io/component:
//! engine`, `constellation.dev/pool`, `constellation.dev/shard`,
//! `constellation.dev/owner: controller`) and, once the controller has
//! asked it, which filesystem it serves (`constellation.dev/fs-uuid`), which
//! is how `DeleteVolume`/`ControllerExpandVolume` — knowing only the uuid in
//! `volume_id` — find it again. Its `ownerReference` is the controller
//! `Deployment` (when the chart names it), so uninstalling the driver
//! garbage-collects the pods, while a controller *pod* restart or a leader
//! change leaves them running. Reaping a pool's pod once the pool is empty
//! is 37-k6b's (with the purge worker), not this module's.
//!
//! **hostPath layout.** The pod's state dir (the meta store: durable state
//! that should survive a container restart) is
//! `<hostRoot>/node-identity/<unit>-controller/` and its control socket
//! `<hostRoot>/sockets/<unit>-controller/control.sock`, both
//! `DirectoryOrCreate` — §7's tree, with a `-controller` suffix so the
//! controller-owned pod never collides with a node-owned pod of the same
//! pool on the same node (K3). kubelet creates those directories root-owned;
//! a one-shot init container (uid 0, `CAP_CHOWN` only) hands them to the
//! engine's uid. A rescheduled pod starts with an empty state dir on its new
//! node — harmless for a node that never holds a view: it rejoins as a fresh
//! Constellation node and replays the pool's log.
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
//! namespace; elsewhere the pool's surviving credentials `Secret` is
//! reused, and failing both the RPC stays `UNAVAILABLE` and says why. The
//! rebuilt pod must turn out to serve that uuid. That needs `list` on
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
//! **Credentials.** An engine pod opens S3 at start, before any control
//! call could hand it credentials, so `fs.unlock`-style per-request
//! delivery (§9, 37-k6a) cannot start it. The request's secrets
//! (`req.secrets`, the class's provisioner secret resolved by
//! external-provisioner) are written to one `Secret` per pool in the
//! driver's namespace — `constellation-engine-<unit>-controller-credentials`, owned
//! like the pod — and the pod reads them as environment variables
//! (`secretKeyRef`, so they never appear in the pod spec). That needs
//! `secrets` get/create/update in the driver's namespace only (K1 granted
//! none); a class with no secret (`credentialSource: aws-default-chain`)
//! leaves the pod on the SDK's default chain (IRSA, EKS Pod Identity). The
//! endpoint and region come from the class as `AWS_ENDPOINT`/`AWS_REGION`,
//! which is why `fs.create` through the pod must not name them again
//! (the daemon refuses an `fs.create` that does).

use crate::control_client::{ControlClient, Engines, Handle, PoolRef, SocketControlClient};
use crate::node::{NodeEngine, NodeEngines};
use crate::params::ClassParams;
use crate::volume_id::VolumeId;
use async_trait::async_trait;
use constellation_control::fd::OwnedFd;
use constellation_control::methods::FsList;
use constellation_control::proto::types::{
    Ack, CloneParams, FileStat, FsCreateParams, FsCreated, FsListing, FsUnlockParams,
    HandoffParams, HandoffReport, LeaveParams, MkdirParams, Pong, QuotaStatus, RenameParams,
    SnapshotCreateParams, SnapshotCreated, SnapshotDeleteParams, SnapshotHeld, SnapshotHoldParams,
    SnapshotListParams, SnapshotListing, ViewInfo, ViewListParams, ViewListing, ViewMountParams,
    ViewStatsParams, ViewStatsReport, ViewUnmountParams, XattrParams, XattrResult,
};
use constellation_control::proto::{ControlError, ErrorKind};
use constellation_control::transport::StreamTransport;
use constellation_control::{Client, ClientOptions, Principal};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment};
use k8s_openapi::api::core::v1::{
    Capabilities, Container, EmptyDirVolumeSource, EnvVar, EnvVarSource, ExecAction,
    HostPathVolumeSource, PersistentVolume, Pod, PodSecurityContext, PodSpec, Probe,
    ResourceRequirements, SeccompProfile, Secret, SecretKeySelector, SecurityContext, Toleration,
    Volume, VolumeMount,
};
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use k8s_openapi::ByteString;
use kube::api::{Api, AttachParams, DeleteParams, ListParams, Patch, PatchParams, PostParams};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncBufReadExt;

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
pub const LABEL_POOL: &str = "constellation.dev/pool";
pub const LABEL_SHARD: &str = "constellation.dev/shard";
pub const LABEL_OWNER: &str = "constellation.dev/owner";
pub const LABEL_FS_UUID: &str = "constellation.dev/fs-uuid";
pub const LABEL_NODE: &str = "constellation.dev/node";
/// A node-owned pod's `<unit>`: what its name, hostPaths and credentials
/// `Secret` are derived from, which the node half of the chart's
/// pod-access policy holds them to.
pub const LABEL_UNIT: &str = "constellation.dev/unit";
/// §7: how many views a node-owned engine pod serves, as its node plugin
/// last counted them (the idle GC, 37-k6b, reads it), and since when it
/// has served none.
pub const ANNOTATION_VIEWS: &str = "constellation.dev/last-view-count";
pub const ANNOTATION_IDLE_SINCE: &str = "constellation.dev/idle-since";
const ANNOTATION_S3: &str = "constellation.dev/s3";
const ANNOTATION_ENDPOINT: &str = "constellation.dev/endpoint";

/// The secret keys an engine pod reads, and the variables they become
/// (plan 37 §6's `Secret` layout).
const SECRET_ENV: [(&str, &str); 4] = [
    ("aws_access_key_id", "AWS_ACCESS_KEY_ID"),
    ("aws_secret_access_key", "AWS_SECRET_ACCESS_KEY"),
    ("aws_session_token", "AWS_SESSION_TOKEN"),
    ("e2e_passphrase", "CONSTELLATION_PASSPHRASE"),
];

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

fn secret_name(pod: &str) -> String {
    format!("{pod}-credentials")
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

    /// The pod's directory under `<hostRoot>/{node-identity,sockets}/`:
    /// §7's `<unit>`, with a `-controller` suffix for the controller's own
    /// so the two never share a meta store or a socket on one node.
    pub fn host_dir(&self, pool: &PoolRef) -> String {
        match self {
            EngineRole::Controller => format!("{}-controller", unit_name(pool)),
            EngineRole::Node { .. } => unit_name(pool),
        }
    }

    /// The credentials `Secret` the pod reads: the pool's, which only the
    /// controller writes (from `CreateVolume`'s provisioner secret) — a
    /// node's pod references it, and the node plugin holds no permission
    /// on Secrets at all (plan 37 §9; [`NodeEnginePods`]).
    pub fn secret_name(&self, pool: &PoolRef) -> String {
        secret_name(&pod_name(pool))
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
pub fn node_engine_policy() -> String {
    format!(
        "# Written by the constellation-csi node plugin (plan 37 §7).\n\
         [[grant]]\n\
         kind = \"service\"\n\
         principal = \"uid:0\"\n\
         socket = \"{POD_SOCKET}\"\n\
         role = \"admin\"\n\
         label = \"csi-node-plugin\"\n"
    )
}

/// Where a node-owned engine pod reads [`node_engine_policy`] from: its
/// `<hostRoot>/policy/<unit>/` directory, mounted read-only.
pub const POD_POLICY: &str = "/etc/constellation-csi/policy/control-allow.toml";
const POD_POLICY_DIR: &str = "/etc/constellation-csi/policy";
/// Its name inside `<hostRoot>/policy/<unit>/`.
pub const POLICY_FILE: &str = "control-allow.toml";
/// `<hostRoot>/policy/`: root-owned, never mounted writable into a pod.
pub const POLICY_DIR: &str = "policy";

/// Write `contents` to `<root>/<dirs…>/<file>` as root (the node plugin)
/// without trusting anything below `root` that someone else could have
/// planted (plan 37 settled decision 10: the engine pod is unprivileged,
/// and a privileged writer must not become its tool). Every directory is
/// created if missing (mode 0755) and opened with `O_NOFOLLOW`, and must
/// be owned by this process's uid and writable by nobody else — so nobody
/// else can have put a symlink or a hard link in it; the file is written
/// to a fresh `O_EXCL|O_NOFOLLOW` temporary next to it, synced and renamed
/// over the old one within the same directory descriptor. Unchanged
/// contents are not rewritten.
pub fn write_private_file(
    root: &std::path::Path,
    dirs: &[&str],
    file: &str,
    contents: &[u8],
) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::io::{Error, ErrorKind, Read, Write};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;

    let name = |s: &str| -> std::io::Result<CString> {
        if s.is_empty() || s == "." || s == ".." || s.contains('/') {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("{s:?} is not a single path component"),
            ));
        }
        CString::new(s).map_err(|e| Error::new(ErrorKind::InvalidInput, e))
    };
    let cvt = |rc: libc::c_int| {
        if rc < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(rc)
        }
    };
    // SAFETY: no preconditions.
    let me = unsafe { libc::geteuid() };
    let open_dir = |at: libc::c_int, path: &CString, shown: &dyn std::fmt::Display| {
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
        Ok::<OwnedFd, Error>(fd)
    };

    std::fs::create_dir_all(root)?;
    let root_c = CString::new(root.as_os_str().as_bytes())
        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
    let mut dir = open_dir(libc::AT_FDCWD, &root_c, &root.display())?;
    let mut shown = root.to_path_buf();
    for d in dirs {
        let c = name(d)?;
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
        dir = open_dir(dir.as_raw_fd(), &c, &shown.display())?;
    }
    let file_c = name(file)?;
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
    let tmp_c = name(&format!(".{file}.tmp"))?;
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
    engine_pod_with(pool, cfg, owner, !pool.secrets.is_empty())
}

/// The node-owned engine pod of `pool` on `node` (plan 37 §7), owned by
/// the node plugin's `DaemonSet` (`owner`). `fs_uuid`: the filesystem the
/// volume that brings it up names, for the `constellation.dev/fs-uuid`
/// label. Pure, so the spec is unit-tested.
pub fn node_engine_pod(
    pool: &PoolRef,
    cfg: &EnginePodConfig,
    node: &str,
    owner: Option<&OwnerReference>,
    reads_secret: bool,
    fs_uuid: &str,
) -> Pod {
    let role = EngineRole::Node {
        node: node.to_string(),
    };
    let mut pod = build_engine_pod(pool, cfg, owner, reads_secret, &role);
    if let Some(labels) = pod.metadata.labels.as_mut() {
        labels.insert(LABEL_NODE.to_string(), label_value(node));
        labels.insert(LABEL_UNIT.to_string(), unit_name(pool));
        labels.insert(LABEL_FS_UUID.to_string(), label_value(fs_uuid));
    }
    pod
}

/// [`engine_pod`], with whether the pod reads the pool's credentials
/// `Secret` decided by the caller: a rebuilt pool may reuse a surviving
/// one whose bytes the controller does not hold.
fn engine_pod_with(
    pool: &PoolRef,
    cfg: &EnginePodConfig,
    owner: Option<&OwnerReference>,
    reads_secret: bool,
) -> Pod {
    build_engine_pod(pool, cfg, owner, reads_secret, &EngineRole::Controller)
}

/// Both roles' engine pod (module docs, [`node_engine_pod`]).
fn build_engine_pod(
    pool: &PoolRef,
    cfg: &EnginePodConfig,
    owner: Option<&OwnerReference>,
    reads_secret: bool,
    role: &EngineRole,
) -> Pod {
    let name = role.pod_name(pool);
    let unit = role.host_dir(pool);
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
    if let EngineRole::Node { .. } = role {
        // The node plugin's grant on this pod's socket (`node_engine_policy`).
        envs.push(env("CONSTELLATION_CONTROL_POLICY", POD_POLICY));
    }
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
    if reads_secret {
        for (key, var) in SECRET_ENV {
            envs.push(EnvVar {
                name: var.into(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: role.secret_name(pool),
                        key: key.into(),
                        optional: Some(true),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
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
    let host_dir = |sub: &str| Volume {
        name: sub.into(),
        host_path: Some(HostPathVolumeSource {
            path: format!("{}/{sub}/{unit}", cfg.host_root.trim_end_matches('/')),
            type_: Some("DirectoryOrCreate".into()),
        }),
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
        host_dir("node-identity"),
        host_dir("sockets"),
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
                path: format!(
                    "{}/{POLICY_DIR}/{unit}",
                    cfg.host_root.trim_end_matches('/')
                ),
                // Made by the plugin before it creates the pod.
                type_: Some("Directory".into()),
            }),
            ..Default::default()
        });
    }
    let unprivileged = SecurityContext {
        allow_privilege_escalation: Some(false),
        capabilities: Some(Capabilities {
            drop: Some(vec!["ALL".into()]),
            add: None,
        }),
        read_only_root_filesystem: Some(true),
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
            init_containers: Some(vec![Container {
                // kubelet creates `DirectoryOrCreate` hostPaths root-owned.
                name: "own-host-dirs".into(),
                image: Some(cfg.image.clone()),
                image_pull_policy: cfg.image_pull_policy.clone(),
                command: Some(vec![
                    "chown".into(),
                    format!("{ENGINE_UID}:{ENGINE_UID}"),
                    POD_STATE_DIR.into(),
                    POD_SOCKET_DIR.into(),
                ]),
                security_context: Some(SecurityContext {
                    run_as_user: Some(0),
                    run_as_non_root: Some(false),
                    capabilities: Some(Capabilities {
                        drop: Some(vec!["ALL".into()]),
                        add: Some(vec!["CHOWN".into()]),
                    }),
                    ..unprivileged.clone()
                }),
                volume_mounts: Some(mounts[..2].to_vec()),
                ..Default::default()
            }]),
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

/// The pool's credentials `Secret` for `role`'s pods: only the keys an
/// engine pod reads.
fn credentials_secret(
    pool: &PoolRef,
    cfg: &EnginePodConfig,
    owner: Option<&OwnerReference>,
    role: &EngineRole,
) -> Secret {
    let data = SECRET_ENV
        .iter()
        .filter_map(|(key, _)| {
            pool.secrets
                .get(*key)
                .map(|v| (key.to_string(), ByteString(v.as_bytes().to_vec())))
        })
        .collect();
    Secret {
        metadata: ObjectMeta {
            name: Some(role.secret_name(pool)),
            namespace: Some(cfg.namespace.clone()),
            labels: Some(BTreeMap::from([
                (
                    "app.kubernetes.io/name".to_string(),
                    "constellation-csi".to_string(),
                ),
                (LABEL_COMPONENT.to_string(), "engine".to_string()),
                (LABEL_POOL.to_string(), pool_label(pool)),
            ])),
            owner_references: owner.map(|o| vec![o.clone()]),
            ..Default::default()
        },
        type_: Some("Opaque".into()),
        data: Some(data),
        ..Default::default()
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

/// Create or refresh a credentials `Secret` to hold exactly `wanted`'s
/// data.
async fn ensure_credentials(secrets: &Api<Secret>, wanted: Secret) -> Result<(), ControlError> {
    let name = wanted.metadata.name.clone().unwrap_or_default();
    match secrets.get_opt(&name).await {
        Ok(Some(existing)) if existing.data == wanted.data => Ok(()),
        Ok(Some(_)) => {
            // Rotation reaches a running pod only at its next start
            // (environment variables); a node-owned pod also gets the new
            // bytes at once through `fs.unlock` (`crate::node`).
            let patch = serde_json::json!({ "data": wanted.data });
            secrets
                .patch(&name, &PatchParams::default(), &Patch::Merge(&patch))
                .await
                .map(drop)
                .map_err(|e| kube_err("updating the engine credentials secret", e))
        }
        Ok(None) => match secrets.create(&PostParams::default(), &wanted).await {
            Ok(_) => Ok(()),
            Err(e) if is_status(&e, 409) => Ok(()),
            Err(e) => Err(kube_err("creating the engine credentials secret", e)),
        },
        Err(e) => Err(kube_err("reading the engine credentials secret", e)),
    }
}

/// Pod `name`, created from `spec` when absent and recreated (from `spec`,
/// else from its own) when it has terminated, once it is `Ready`: polled
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
    secrets: Api<Secret>,
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
}

impl EnginePodManager {
    /// Resolves the owner `Deployment` once (its uid goes into every
    /// `ownerReference`); a missing one is logged and engine pods are then
    /// unowned.
    pub async fn new(client: kube::Client, cfg: EnginePodConfig) -> EnginePodManager {
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
        EnginePodManager {
            pods: Api::namespaced(client.clone(), &cfg.namespace),
            secrets: Api::namespaced(client.clone(), &cfg.namespace),
            client,
            cfg,
            owner,
            relays: Mutex::default(),
            by_uuid: Mutex::default(),
            specs: Mutex::default(),
            bringup: Mutex::default(),
        }
    }

    fn bringup_lock(&self, name: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.bringup
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_default()
            .clone()
    }

    /// Create or refresh the pool's credentials `Secret`.
    async fn ensure_secret(&self, pool: &PoolRef) -> Result<(), ControlError> {
        let wanted = credentials_secret(
            pool,
            &self.cfg,
            self.owner.as_ref(),
            &EngineRole::Controller,
        );
        ensure_credentials(&self.secrets, wanted).await
    }

    /// The pod, created from `spec` when absent and recreated when it has
    /// terminated, once it is `Ready`.
    async fn ensure_ready(&self, name: &str, spec: Option<&Pod>) -> Result<Pod, ControlError> {
        ensure_pod_ready(&self.pods, name, spec, self.cfg.ready_timeout).await
    }

    /// The live relay into `pod`, dialled anew when the pod was replaced
    /// or the old relay died.
    async fn relay(&self, pod: &Pod) -> Result<Arc<Relay>, ControlError> {
        let name = pod.metadata.name.clone().unwrap_or_default();
        let uid = pod.metadata.uid.clone().unwrap_or_default();
        if let Some(relay) = self.relays.lock().unwrap().get(&name) {
            if relay.uid == uid && relay.client.is_connected() {
                return Ok(relay.clone());
            }
        }
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
        let relay = Arc::new(Relay {
            uid,
            client: Arc::new(SocketControlClient::new(client)),
            process,
        });
        self.relays
            .lock()
            .unwrap()
            .insert(name.clone(), relay.clone());
        tracing::debug!(pod = %name, "control relay connected");
        Ok(relay)
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

    /// The pool serving `fs_uuid`, rebuilt from the cluster's PVs and
    /// `StorageClass`es with its credentials (module docs), and whether its
    /// pod reads a credentials `Secret` (`pool.secrets` may be empty when a
    /// surviving one is reused).
    async fn rebuild_pool(&self, fs_uuid: &str) -> Result<(PoolRef, bool), ControlError> {
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
                "no engine pod serves filesystem {fs_uuid}, and no PersistentVolume of this                  driver names it to rebuild one from"
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
                    "PersistentVolume {pv_name} names no StorageClass to rebuild filesystem                      {fs_uuid}'s engine pod from"
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
        let mut reads_secret = false;
        if let Some((namespace, name)) = secret_ref {
            reads_secret = true;
            let secrets: Api<Secret> = Api::namespaced(self.client.clone(), &namespace);
            match secrets.get_opt(&name).await {
                Ok(Some(secret)) => {
                    pool.secrets = secret
                        .data
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(k, v)| (k, String::from_utf8_lossy(&v.0).into_owned()))
                        .collect();
                }
                // Not readable by the controller (another namespace, plan
                // §9) or gone: the pool's own copy may have survived.
                other => {
                    let own = secret_name(&pod_name(&pool));
                    let survived = self
                        .secrets
                        .get_opt(&own)
                        .await
                        .map_err(|e| kube_err("reading the engine credentials secret", e))?
                        .is_some();
                    if !survived {
                        let why = match other {
                            Err(e) => e.to_string(),
                            _ => "not found".into(),
                        };
                        return Err(ControlError::unavailable(format!(
                            "rebuilding filesystem {fs_uuid}'s engine pod needs the provisioner                              secret {namespace}/{name} ({why}), and the pool's own copy {own}                              is gone too"
                        )));
                    }
                }
            }
        }
        tracing::info!(fs_uuid, pv = %pv_name, class = %class_name,
            "rebuilding a lost engine pod's spec from its PersistentVolume");
        Ok((pool, reads_secret))
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

    /// Bring `name` up (from `spec` if it must be created) and connect.
    async fn connect(&self, name: &str, spec: Option<&Pod>) -> Result<Arc<Relay>, ControlError> {
        let lock = self.bringup_lock(name);
        let _guard = lock.lock().await;
        let pod = self.ensure_ready(name, spec).await?;
        let relay = self.relay(&pod).await?;
        self.learn_uuid(&pod, &relay).await?;
        Ok(relay)
    }
}

/// A terminated pod's spec, as a fresh pod object.
fn respawn(old: &Pod) -> Pod {
    Pod {
        metadata: ObjectMeta {
            name: old.metadata.name.clone(),
            namespace: old.metadata.namespace.clone(),
            labels: old.metadata.labels.clone(),
            annotations: old.metadata.annotations.clone(),
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
        if !pool.secrets.is_empty() {
            self.ensure_secret(pool).await?;
        }
        let spec = engine_pod(pool, &self.cfg, self.owner.as_ref());
        let name = pod_name(pool);
        self.specs
            .lock()
            .unwrap()
            .insert(name.clone(), spec.clone());
        let relay = self.connect(&name, Some(&spec)).await?;
        Ok(Arc::new(PoolClient(relay)))
    }

    async fn filesystem(&self, fs_uuid: &str) -> Result<Arc<dyn ControlClient>, ControlError> {
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
                let relay = self.connect(&name, spec.as_ref()).await?;
                return Ok(Arc::new(PoolClient(relay)));
            }
        }
        // No pod and no spec: rebuild both from the cluster (module docs).
        let (pool, reads_secret) = self.rebuild_pool(fs_uuid).await?;
        if !pool.secrets.is_empty() {
            self.ensure_secret(&pool).await?;
        }
        let spec = engine_pod_with(&pool, &self.cfg, self.owner.as_ref(), reads_secret);
        let name = pod_name(&pool);
        self.specs
            .lock()
            .unwrap()
            .insert(name.clone(), spec.clone());
        let relay = self.connect(&name, Some(&spec)).await?;
        let serves = self.by_uuid.lock().unwrap().get(fs_uuid).cloned();
        if serves.as_deref() != Some(name.as_str()) {
            return Err(ControlError::failed(format!(
                "the engine pod {name} rebuilt for filesystem {fs_uuid} serves another \
                 filesystem: its pool's S3 prefix no longer holds {fs_uuid}"
            )));
        }
        Ok(Arc::new(PoolClient(relay)))
    }

    async fn running(&self, fs_uuid: &str) -> Result<Option<Arc<dyn ControlClient>>, ControlError> {
        let Some(name) = self.pod_for(fs_uuid).await? else {
            return Ok(None);
        };
        let lock = self.bringup_lock(&name);
        let _guard = lock.lock().await;
        let Some(pod) = wait_existing_ready(&self.pods, &name, self.cfg.ready_timeout).await?
        else {
            return Ok(None);
        };
        let relay = self.relay(&pod).await?;
        self.learn_uuid(&pod, &relay).await?;
        Ok(Some(Arc::new(PoolClient(relay))))
    }

    async fn named(&self, handle: Handle<'_>) -> Result<bool, ControlError> {
        match handle {
            Handle::Volume(h) => self.pv_named(h).await,
            Handle::Snapshot(h) => self.content_named(h).await,
        }
    }

    /// Deleted only when no other caller holds a client into it: every
    /// [`PoolClient`] shares the cached relay, so the relay map's own
    /// reference is then the only one left.
    async fn retire(&self, fs_uuid: &str) -> Result<(), ControlError> {
        let Some(name) = self.pod_for(fs_uuid).await? else {
            return Ok(());
        };
        let lock = self.bringup_lock(&name);
        let _guard = lock.lock().await;
        {
            let mut relays = self.relays.lock().unwrap();
            if let Some(relay) = relays.get(&name) {
                if Arc::strong_count(relay) > 1 {
                    tracing::debug!(pod = %name, "engine pod in use; not retiring it");
                    return Ok(());
                }
            }
            relays.remove(&name);
        }
        match self.pods.delete(&name, &DeleteParams::default()).await {
            Ok(_) => {
                tracing::info!(pod = %name, "stopped the engine pod a delete started");
                Ok(())
            }
            Err(e) if is_status(&e, 404) => Ok(()),
            Err(e) => Err(kube_err("deleting the engine pod", e)),
        }
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
/// node, never `--create`-ing a filesystem, labelled `constellation.dev/owner:
/// node` and `constellation.dev/node`, and owned by the node plugin's
/// `DaemonSet` — not by the plugin *pod*: the plan's §7 asks for the pod, so
/// that an engine pod cannot outlive the plugin generation that made it,
/// but every rolling update of the plugin would then delete every engine
/// pod on the node and every mount with it, which is the outage §2.1 puts
/// engine pods in their own pods to avoid. Uninstalling the driver still
/// takes them (the `DaemonSet` goes). Its hostPaths are §7's own,
/// `<hostRoot>/{node-identity,sockets}/<unit>/`: the meta store survives
/// a container restart, and the node identity with it.
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
/// **Credentials.** The node plugin has no permission on Secrets (plan 37
/// §9): an engine opens S3 at start, so the pod references the pool's
/// credentials `Secret` the *controller* writes on `CreateVolume`
/// (`constellation-engine-<unit>-controller-credentials`, [`EngineRole::secret_name`]),
/// and kubelet resolves it (the node authorizer admits a pod's own
/// `secretKeyRef`s); the request's node-stage secret then reaches the
/// running pod through `fs.unlock` (`crate::node`), which is also how a
/// rotation does. A pool no `CreateVolume` of this driver has seen (only
/// static PVs) has no such `Secret`: its node pods start on the SDK's
/// default chain (IRSA, EKS Pod Identity) or not at all.
pub struct NodeEnginePods {
    pods: Api<Pod>,
    cfg: EnginePodConfig,
    node: String,
    owner: Option<OwnerReference>,
    /// pod name → (incarnation, connection) of the incarnation last dialled.
    clients: Mutex<HashMap<String, (String, Arc<SocketControlClient>)>>,
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
            bringup: Mutex::default(),
        }
    }

    fn pod_of(&self, unit: &str) -> String {
        node_pod_name(unit, &self.node)
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
        })
    }

    fn bringup_lock(&self, name: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.bringup
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_default()
            .clone()
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
                let socket = self.sockets_dir(unit).join("control.sock");
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
}

#[async_trait]
impl NodeEngines for NodeEnginePods {
    fn names(&self, pool: &PoolRef) -> (String, String) {
        let unit = unit_name(pool);
        let pod = self.pod_of(&unit);
        (unit, pod)
    }

    async fn engine(
        &self,
        pool: &PoolRef,
        fs_uuid: &str,
        reads_secret: bool,
    ) -> Result<NodeEngine, ControlError> {
        let (unit, name) = self.names(pool);
        let lock = self.bringup_lock(&name);
        let _guard = lock.lock().await;
        self.write_policy(&unit)?;
        let spec = node_engine_pod(
            pool,
            &self.cfg,
            &self.node,
            self.owner.as_ref(),
            reads_secret,
            fs_uuid,
        );
        let pod = ensure_pod_ready(&self.pods, &name, Some(&spec), self.cfg.ready_timeout).await?;
        self.dial(&pod, &unit).await
    }

    async fn existing(&self, unit: &str) -> Result<Option<NodeEngine>, ControlError> {
        let name = self.pod_of(unit);
        let pod = self
            .pods
            .get_opt(&name)
            .await
            .map_err(|e| kube_err("reading the engine pod", e))?;
        match pod {
            Some(pod) if pod_ready(&pod) && pod.metadata.deletion_timestamp.is_none() => {
                self.dial(&pod, unit).await.map(Some)
            }
            _ => Ok(None),
        }
    }

    async fn set_view_count(&self, unit: &str, views: usize) -> Result<(), ControlError> {
        let name = self.pod_of(unit);
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
}

/// A pool's client: the relay's, with `fs.create` stripped of the endpoint
/// and region the pod already has in its environment (module docs). Holds
/// the relay, so the stream outlives every RPC using it.
struct PoolClient(Arc<Relay>);

impl PoolClient {
    fn c(&self) -> &SocketControlClient {
        &self.0.client
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
        // `constellation.dev/pool`), one pod per shard.
        let s0 = pool(&[("bucket", "b"), ("shards", "4")], 0);
        let s3 = pool(&[("bucket", "b"), ("shards", "4")], 3);
        assert_eq!(pool_label(&s0), pool_label(&s3));
        assert_ne!(pod_name(&s0), pod_name(&s3), "distinct filesystems");
        assert!(unit_name(&s3).ends_with("-shard-3"));
        // A long, odd prefix still yields a label and a short socket path.
        let long = pool(&[("bucket", "b"), ("prefix", &"Ü".repeat(80))], 63);
        assert!(is_dns_label(&pool_label(&long)), "{}", pool_label(&long));
        let socket = format!(
            "/var/lib/constellation-csi/sockets/{}-controller/control.sock",
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
            "secrets stay in the Secret"
        );
        let spec = pod.spec.as_ref().unwrap();
        assert_eq!(spec.node_name, None, "§7: never nodeName-pinned");
        assert_eq!(spec.restart_policy.as_deref(), Some("OnFailure"));
        let psc = spec.security_context.as_ref().unwrap();
        assert_eq!(psc.run_as_non_root, Some(true));
        let engine = &spec.containers[0];
        let sc = engine.security_context.as_ref().unwrap();
        assert_eq!(sc.allow_privilege_escalation, Some(false));
        assert_eq!(sc.privileged, None);
        assert_eq!(
            sc.capabilities.as_ref().unwrap().drop.as_deref(),
            Some(&["ALL".to_string()][..])
        );
        assert!(sc.capabilities.as_ref().unwrap().add.is_none());
        let args = engine.args.as_ref().unwrap();
        assert_eq!(args[..3], ["serve", "--s3", "s3://b/pool"]);
        assert!(args.windows(2).any(|w| w == ["--chunk-size", "1048576"]));
        let envs = engine.env.as_ref().unwrap();
        let value = |n: &str| {
            envs.iter()
                .find(|e| e.name == n)
                .and_then(|e| e.value.clone())
        };
        assert_eq!(value("AWS_ENDPOINT").as_deref(), Some("http://floci:4566"));
        assert_eq!(value("AWS_ALLOW_HTTP").as_deref(), Some("true"));
        assert_eq!(value("AWS_REGION").as_deref(), Some("us-east-1"));
        let key = envs.iter().find(|e| e.name == "AWS_ACCESS_KEY_ID").unwrap();
        let sel = key
            .value_from
            .as_ref()
            .unwrap()
            .secret_key_ref
            .as_ref()
            .unwrap();
        assert_eq!(sel.name, format!("{}-credentials", pod_name(&p)));
        assert!(engine.readiness_probe.as_ref().unwrap().exec.is_some());
        let labels = pod.metadata.labels.as_ref().unwrap();
        assert_eq!(labels[LABEL_COMPONENT], "engine");
        assert_eq!(labels[LABEL_OWNER], "controller");
        assert_eq!(labels[LABEL_SHARD], "0");
        let paths: Vec<String> = spec
            .volumes
            .as_ref()
            .unwrap()
            .iter()
            .filter_map(|v| v.host_path.as_ref().map(|h| h.path.clone()))
            .collect();
        let unit = unit_name(&p);
        assert_eq!(
            paths,
            [
                format!("/var/lib/constellation-csi/node-identity/{unit}-controller"),
                format!("/var/lib/constellation-csi/sockets/{unit}-controller"),
            ]
        );
        // The only privilege anywhere: the init container's CHOWN.
        let init = &spec.init_containers.as_ref().unwrap()[0];
        let caps = init
            .security_context
            .as_ref()
            .unwrap()
            .capabilities
            .as_ref()
            .unwrap();
        assert_eq!(caps.add.as_deref(), Some(&["CHOWN".to_string()][..]));
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
        let pod = node_engine_pod(&p, &cfg(), node, None, true, "uuid-1");
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
        let key = envs.iter().find(|e| e.name == "AWS_ACCESS_KEY_ID").unwrap();
        let sel = key
            .value_from
            .as_ref()
            .unwrap()
            .secret_key_ref
            .as_ref()
            .unwrap();
        assert_eq!(
            sel.name,
            format!("constellation-engine-{unit}-controller-credentials"),
            "the pool's Secret, which only the controller writes"
        );
        let sc = engine.security_context.as_ref().unwrap();
        assert_eq!(sc.privileged, None);
        assert_eq!(sc.allow_privilege_escalation, Some(false));
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
        let init = &spec.init_containers.as_ref().unwrap()[0];
        assert!(init
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .all(|m| m.name != POLICY_DIR));
        // The controller's pod of the same pool stays apart.
        let controller = engine_pod(&p, &cfg(), None);
        assert_ne!(controller.metadata.name, pod.metadata.name);
        assert!(controller.spec.unwrap().containers[0]
            .args
            .as_ref()
            .unwrap()
            .contains(&"--create".to_string()));
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

    #[test]
    fn the_credentials_secret_carries_only_known_keys() {
        let mut p = pool(&[("bucket", "b")], 0);
        p.secrets.insert("aws_access_key_id".into(), "id".into());
        p.secrets
            .insert("aws_secret_access_key".into(), "key".into());
        p.secrets.insert("unrelated".into(), "x".into());
        let secret = credentials_secret(&p, &cfg(), None, &EngineRole::Controller);
        let keys: Vec<&String> = secret.data.as_ref().unwrap().keys().collect();
        assert_eq!(keys, ["aws_access_key_id", "aws_secret_access_key"]);
        // And a pool with no secret gets no secret references at all.
        let bare = engine_pod(&pool(&[("bucket", "b")], 0), &cfg(), None);
        let envs = bare.spec.unwrap().containers[0].env.clone().unwrap();
        assert!(envs.iter().all(|e| e.value_from.is_none()));
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
        // A reused surviving Secret is referenced exactly as a fresh one.
        let mut with = created.clone();
        with.secrets.insert("aws_access_key_id".into(), "x".into());
        assert_eq!(
            engine_pod_with(&pool_ref, &cfg(), None, true),
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
