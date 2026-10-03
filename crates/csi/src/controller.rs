//! The Controller service (plan 37 §5, K2, K4): `CreateVolume` (also from
//! a snapshot or a volume), `DeleteVolume`, `ControllerExpandVolume`,
//! `ValidateVolumeCapabilities`, `ControllerGetCapabilities`, and the
//! snapshot RPCs ([`snapshots`]), driven entirely through [`Engines`] /
//! [`ControlClient`]. Every RPC not listed here answers `UNIMPLEMENTED` and
//! is not advertised.
//!
//! **Stateless** (settled decision 7). A volume *is* a directory
//! `/volumes/<name>` in a pool filesystem, its record is the
//! `user.constellation.csi.*` xattrs on that directory, and its `volume_id`
//! names the filesystem and subtree ([`crate::volume_id`]). Every RPC
//! re-derives its answer from the pool; nothing here survives a restart
//! except the in-flight bookkeeping below, which is only about *this*
//! process's concurrent calls.
//!
//! **The volume record and idempotency.** `CreateVolume` writes, in order:
//! `mkdir /volumes/<name>`; the `pv`, `pvc`, `namespace`, `capacity` and
//! `source` xattrs; `quota.set{/volumes/<name>, capacity}`; and finally the
//! `created` xattr. `created` is the commit mark: a directory carrying it is
//! a finished volume, and a retried `CreateVolume` compares its record
//! against the request (same name, compatible capacity, same source →
//! `OK` with the stored capacity; anything else → `ALREADY_EXISTS`). A
//! directory without it is an earlier attempt that died part-way — most
//! often at `quota.set`, see below — and is completed in place with the
//! current request's values. A directory whose `pv` names a different
//! volume is never adopted, and neither is one with no `pv` at all that
//! already holds data: an attempt that died before its first xattr left an
//! empty directory, so a non-empty one without a record is somebody
//! else's (a human's `mkdir` + copy), and `ALREADY_EXISTS` says so rather
//! than handing its contents to a new PV.
//!
//! **`layout: dedicated`** (§2.3) is the same record on a filesystem of
//! its own: `CreateVolume` makes `<class prefix>/<name>` with `fs.create`
//! through that filesystem's own controller-owned engine pod, writes the
//! record on its root and the quota as its filesystem-wide cap
//! (`quota.set{/}`), and puts the filesystem's prefix in the volume
//! context. `DeleteVolume` of one is `FAILED_PRECONDITION` until plan 37
//! K6b's purge primitive exists: dropping a whole filesystem is not a
//! rename into `/.trash`.
//!
//! **A delete never brings an engine pod back.** external-provisioner may
//! call `DeleteVolume` again after it deleted the PV (seen 18 s later, after
//! a teardown had removed the pool's engine pods), and a delete that
//! recreated the pool's pod to answer "already gone" would leave a pod
//! nobody stops. So `DeleteVolume` (and `DeleteSnapshot`) work through an
//! engine pod that is up already ([`Engines::running`]); with none, they
//! ask the cluster whether the PV (the `VolumeSnapshotContent`) still
//! exists — the sidecar deletes it only after the RPC succeeded — and
//! answer `OK` at once if not. Only a delete that genuinely needs the
//! engine starts one, and stops it again afterwards ([`Engines::retire`]).
//!
//! **`DeleteVolume` checks existence first** (`browse.xattr list` →
//! `NotFound` → `OK`), before releasing the quota: a retried delete of a
//! volume already in `/.trash` must succeed whatever error kind the engine
//! gives a `quota.set` on a missing subtree — a `Failed` would read as
//! transient, be retried eight times and end `ABORTED`, leaving the PV
//! stuck deleting.
//!
//! **No RPC walks a volume** (plan 37 §"Deletion and purge": the same
//! latency for one file or ten million). The bytes under a subtree cost a
//! walk of every entry (`quota.get`, about 0.45 µs per entry warm), so
//! existence checks use an xattr list or `quota.get{cap_only}`, and
//! `quota.set{subtree}` returns no usage. Only `CreateVolume`'s adoption
//! check on a directory with no record (somebody else's?) and an
//! expansion that puts the first cap on an uncapped volume ask for the
//! bytes.
//!
//! **`quota.set` fails transiently, so it is retried** (plan 37 "K0 results"
//! Track B). Under concurrent metadata load the engine's `quota.set` used to
//! fail with `journal not shipped: no lease` at rates from a fraction of a
//! percent (c=4) to most calls (c=64), host-load dependent: its handler
//! waited for the node's *whole* journal to drain inside one sync round.
//! The engine no longer drains the journal there, but the controller still
//! treats `Failed`/`Unavailable`/`Timeout` from `quota.set` as transient and
//! retries with jittered exponential backoff (bounded, [`QuotaRetry`]):
//! `quota.set` is idempotent and, by the order above, everything before it
//! is already committed. An exhausted retry is `ABORTED` — retryable for
//! the sidecar, which then re-runs the whole idempotent `CreateVolume`.
//! As a secondary measure, in-flight `CreateVolume`s are bounded per pool
//! filesystem ([`ControllerConfig::pool_create_concurrency`]); excess calls
//! queue rather than fail, for as long as the caller's deadline allows.
//!
//! **Concurrency guard** (§5). A second call for a volume that already has
//! one in flight in this process is `ABORTED` at once ("an operation with
//! the given Volume ID … is already in progress"), the standard CSI
//! pattern; the sidecars retry it. Pool volumes are keyed by their name
//! whichever RPC names them (`req.name` for create, the name inside
//! `volume_id` for the rest), so a delete can never interleave with a
//! still-running create of the same volume.

use crate::control_client::{ControlClient, Engines, Handle, PoolRef, SubtreeQuotaParams};
use crate::params::{ClassParams, Layout, PVC_NAMESPACE_KEY, PVC_NAME_KEY};
use crate::proto::csi::v1::controller_server::Controller as ControllerRpc;
use crate::proto::csi::v1::controller_service_capability::rpc::Type as RpcType;
use crate::proto::csi::v1::controller_service_capability::{Rpc, Type as CapabilityType};
use crate::proto::csi::v1::volume_capability::access_mode::Mode;
use crate::proto::csi::v1::volume_capability::AccessType;
use crate::proto::csi::v1::*;
use crate::volume_id::{validate_name, VolumeId, TRASH_DIR, VOLUMES_DIR};
use constellation_control::proto::types::{
    MkdirParams, QuotaStatus, RenameParams, XattrOp, XattrParams,
};
use constellation_control::proto::{ControlError, ErrorKind};
use constellation_types::Code;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status};

/// The volume record's xattr namespace (settled decision 7).
pub const XATTR_PREFIX: &str = "user.constellation.csi.";
pub(crate) const X_PV: &str = "user.constellation.csi.pv";
pub(crate) const X_PVC: &str = "user.constellation.csi.pvc";
pub(crate) const X_NAMESPACE: &str = "user.constellation.csi.namespace";
const X_CAPACITY: &str = "user.constellation.csi.capacity";
const X_SOURCE: &str = "user.constellation.csi.source";
/// Written last: the record's commit mark (module docs).
const X_CREATED: &str = "user.constellation.csi.created";

/// Bounded retry of `quota.set` (module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaRetry {
    /// Total calls, the first included. `1` disables retrying.
    pub attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for QuotaRetry {
    /// Eight calls, 25 ms doubling to a 1 s cap: ~3 s of backoff in the
    /// worst case, inside external-provisioner's default 10 s `--timeout`
    /// even with the hundreds-of-ms sequence latency Track B measured at
    /// high concurrency. A failing call itself returned in 10-60 ms there.
    fn default() -> QuotaRetry {
        QuotaRetry {
            attempts: 8,
            initial_backoff: Duration::from_millis(25),
            max_backoff: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerConfig {
    /// In-flight `CreateVolume`s per pool filesystem; more queue. Default 4
    /// (the coordinator's ≤4 after Track B's review: no level above 1 was
    /// clean on every host, so this is a secondary bound behind the retry).
    pub pool_create_concurrency: usize,
    pub quota_retry: QuotaRetry,
}

impl Default for ControllerConfig {
    fn default() -> ControllerConfig {
        ControllerConfig {
            pool_create_concurrency: 4,
            quota_retry: QuotaRetry::default(),
        }
    }
}

impl ControllerConfig {
    /// The defaults, overridden by `CONSTELLATION_CSI_POOL_CREATE_CONCURRENCY`
    /// (≥ 1) and `CONSTELLATION_CSI_QUOTA_SET_ATTEMPTS` (≥ 1). An
    /// unparseable value is an error rather than a silent default.
    pub fn from_env() -> Result<ControllerConfig, String> {
        let mut config = ControllerConfig::default();
        let read = |key: &str| -> Result<Option<u64>, String> {
            match std::env::var(key) {
                Ok(v) => v
                    .trim()
                    .parse::<u64>()
                    .ok()
                    .filter(|n| *n >= 1)
                    .map(Some)
                    .ok_or_else(|| format!("{key}={v:?} must be an integer >= 1")),
                Err(_) => Ok(None),
            }
        };
        if let Some(n) = read("CONSTELLATION_CSI_POOL_CREATE_CONCURRENCY")? {
            config.pool_create_concurrency = n as usize;
        }
        if let Some(n) = read("CONSTELLATION_CSI_QUOTA_SET_ATTEMPTS")? {
            config.quota_retry.attempts = u32::try_from(n).unwrap_or(u32::MAX);
        }
        Ok(config)
    }
}

/// The in-process per-volume guard (module docs): a set of keys with an
/// operation in flight, released when the [`VolumeLock`] drops — including
/// when tonic drops the RPC's future because its caller went away.
#[derive(Default)]
pub(crate) struct VolumeLocks(Mutex<HashSet<String>>);

pub(crate) struct VolumeLock<'a> {
    locks: &'a VolumeLocks,
    key: String,
}

impl VolumeLocks {
    pub(crate) fn try_lock(&self, key: String) -> Result<VolumeLock<'_>, Status> {
        if !self.0.lock().unwrap().insert(key.clone()) {
            return Err(Status::aborted(format!(
                "an operation with the given Volume ID {key} is already in progress"
            )));
        }
        Ok(VolumeLock { locks: self, key })
    }
}

impl Drop for VolumeLock<'_> {
    fn drop(&mut self) {
        self.locks.0.lock().unwrap().remove(&self.key);
    }
}

pub struct ControllerService {
    /// `None` when no engine backend is configured (outside a cluster,
    /// without `--in-memory-backend`); every volume RPC is then
    /// `UNAVAILABLE`.
    engines: Option<Arc<dyn Engines>>,
    config: ControllerConfig,
    locks: VolumeLocks,
    /// The same guard for snapshots, keyed by snapshot name.
    snapshot_locks: VolumeLocks,
    /// `(bucket, pool prefix)` → that pool filesystem's create gate.
    pool_gates: Mutex<HashMap<(String, String), Arc<Semaphore>>>,
    /// Pool volume ids this process created and has not deleted since: a
    /// `DeleteVolume` of one is a real delete even when the CO names no PV
    /// (the provisioner cleaning up a PV it failed to save), so it may start
    /// the engine. A delete repeated after success finds nothing here.
    created: Mutex<std::collections::HashSet<String>>,
}

impl ControllerService {
    pub fn new(engines: Option<Arc<dyn Engines>>, config: ControllerConfig) -> ControllerService {
        ControllerService {
            engines,
            config,
            locks: VolumeLocks::default(),
            snapshot_locks: VolumeLocks::default(),
            pool_gates: Mutex::default(),
            created: Mutex::default(),
        }
    }

    fn engines(&self) -> Result<&Arc<dyn Engines>, Status> {
        self.engines.as_ref().ok_or_else(|| {
            Status::unavailable(
                "no engine backend is configured for the controller (not running in a \
                 Kubernetes cluster, and no --in-memory-backend)",
            )
        })
    }

    fn pool_gate(&self, bucket: &str, prefix: &str) -> Arc<Semaphore> {
        self.pool_gates
            .lock()
            .unwrap()
            .entry((bucket.to_string(), prefix.to_string()))
            .or_insert_with(|| Arc::new(Semaphore::new(self.config.pool_create_concurrency.max(1))))
            .clone()
    }

    /// `quota.set` with the bounded retry the module docs describe. Only
    /// transient kinds are retried; `NotFound`, `Invalid` and the like come
    /// back on the first failure.
    async fn set_quota(
        &self,
        fs: &dyn ControlClient,
        subtree: &str,
        max_bytes: Option<u64>,
    ) -> Result<QuotaStatus, ControlError> {
        let policy = self.config.quota_retry;
        let mut backoff = policy.initial_backoff;
        let mut attempt = 1;
        loop {
            let result = fs
                .quota_set(SubtreeQuotaParams {
                    subtree: subtree.to_string(),
                    max_bytes,
                })
                .await;
            match result {
                Err(e) if is_transient(&e) && attempt < policy.attempts => {
                    tracing::debug!(subtree, attempt, error = %e, "quota.set failed; retrying");
                    tokio::time::sleep(jittered(backoff)).await;
                    backoff = (backoff * 2).min(policy.max_backoff);
                    attempt += 1;
                }
                other => return other,
            }
        }
    }

    /// The pool shard `pool`'s filesystem uuid: `fs.create` through its
    /// engine pod, idempotent, so the first volume creates the pool.
    async fn pool_uuid(
        &self,
        engines: &Arc<dyn Engines>,
        pool: &PoolRef,
    ) -> Result<String, Status> {
        let prefix = pool.prefix();
        let registry = engines
            .pool(pool)
            .await
            .map_err(|e| status("reaching the pool's engine", e))?;
        let created = registry
            .fs_create(pool.class.fs_create(prefix.clone()))
            .await
            .map_err(|e| match e.kind {
                ErrorKind::Conflict | ErrorKind::Invalid => Status::invalid_argument(format!(
                    "the StorageClass parameters do not match the existing pool filesystem at \
                     s3://{}/{prefix}: {}",
                    pool.class.bucket, e.message
                )),
                _ => status("fs.create", e),
            })?;
        Ok(created.uuid)
    }

    async fn create_pool_volume(
        &self,
        engines: &Arc<dyn Engines>,
        class: &ClassParams,
        req: &CreateVolumeRequest,
        capacity: u64,
    ) -> Result<Volume, Status> {
        let name = req.name.as_str();
        let shard = class.shard_for(name);
        let prefix = class.pool_prefix(shard);
        let gate = self.pool_gate(&class.bucket, &prefix);
        let _permit = gate
            .acquire_owned()
            .await
            .map_err(|_| Status::internal("pool create gate closed"))?;

        let pool_ref = PoolRef {
            class: class.clone(),
            shard,
            secrets: req
                .secrets
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };
        let pool_uuid = self.pool_uuid(engines, &pool_ref).await?;
        let fs = engines
            .filesystem(&pool_uuid, &pool_ref.secrets)
            .await
            .map_err(|e| status("reaching the pool's engine", e))?;
        let id = VolumeId::Pool {
            shard,
            fs_uuid: pool_uuid,
            name: name.to_string(),
        };
        let subtree = id.subtree();

        fs.browse_mkdir(MkdirParams {
            path: VOLUMES_DIR.to_string(),
            mode: None,
            parents: true,
        })
        .await
        .map_err(|e| status("browse.mkdir /volumes", e))?;

        let (existing, fresh) = match fs
            .browse_mkdir(MkdirParams {
                path: subtree.clone(),
                mode: Some(0o755),
                parents: false,
            })
            .await
        {
            Ok(_) => (BTreeMap::new(), true),
            Err(e) if e.kind == ErrorKind::Conflict => {
                (read_record(fs.as_ref(), &subtree).await?, false)
            }
            Err(e) => return Err(status("browse.mkdir", e)),
        };

        self.commit_volume(
            fs.as_ref(),
            &id,
            req,
            capacity,
            existing,
            fresh,
            &req.parameters,
            "",
        )
        .await
    }

    /// `layout: dedicated` (§2.3): the volume is a whole filesystem of its
    /// own at `<class prefix>/<name>`, created through its own
    /// controller-owned engine pod (`fs.create`, idempotent by location),
    /// its record on the root and its quota the filesystem-wide cap
    /// (`quota.set{/}`). The volume context names that filesystem's
    /// prefix, so `NodeStageVolume` brings up an engine pod for it.
    async fn create_dedicated_volume(
        &self,
        engines: &Arc<dyn Engines>,
        class: &ClassParams,
        req: &CreateVolumeRequest,
        capacity: u64,
    ) -> Result<Volume, Status> {
        let prefix = class.dedicated_prefix(&req.name);
        let gate = self.pool_gate(&class.bucket, &prefix);
        let _permit = gate
            .acquire_owned()
            .await
            .map_err(|_| Status::internal("create gate closed"))?;
        let own = ClassParams {
            prefix: prefix.clone(),
            ..class.clone()
        };
        let fs_ref = PoolRef {
            class: own.clone(),
            shard: 0,
            secrets: req
                .secrets
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };
        let registry = engines
            .pool(&fs_ref)
            .await
            .map_err(|e| status("reaching the volume's engine", e))?;
        let created = registry
            .fs_create(own.fs_create(prefix.clone()))
            .await
            .map_err(|e| match e.kind {
                ErrorKind::Conflict | ErrorKind::Invalid => Status::invalid_argument(format!(
                    "the StorageClass parameters do not match the existing filesystem at \
                     s3://{}/{prefix}: {}",
                    class.bucket, e.message
                )),
                _ => status("fs.create", e),
            })?;
        let fs = engines
            .filesystem(&created.uuid, &fs_ref.secrets)
            .await
            .map_err(|e| status("reaching the volume's engine", e))?;
        let id = VolumeId::Dedicated {
            fs_uuid: created.uuid.clone(),
        };
        let existing = read_record(fs.as_ref(), &id.subtree()).await?;
        let mut context = req.parameters.clone();
        context.insert("prefix".into(), prefix);
        self.commit_volume(
            fs.as_ref(),
            &id,
            req,
            capacity,
            existing,
            false,
            &context,
            "",
        )
        .await
    }

    /// The common tail of both layouts' `CreateVolume`, and of a clone
    /// (module docs): the volume's directory (`id.subtree()`) exists in
    /// `fs` and carries `existing` as its record (`fresh`: this call just
    /// made it). Adopt, compare or complete the record, then the quota,
    /// then the mark. `source` is the record's content source (`""` for an
    /// empty volume, [`snapshots`]' `snapshot:<id>`/`volume:<id>`).
    #[allow(clippy::too_many_arguments)]
    async fn commit_volume(
        &self,
        fs: &dyn ControlClient,
        id: &VolumeId,
        req: &CreateVolumeRequest,
        capacity: u64,
        existing: BTreeMap<String, String>,
        fresh: bool,
        context: &HashMap<String, String>,
        source: &str,
    ) -> Result<Volume, Status> {
        let name = req.name.as_str();
        let subtree = id.subtree();
        let source = source.to_string();
        let fs_uuid = id.fs_uuid();
        match existing.get(X_PV) {
            Some(pv) if pv != name => {
                return Err(Status::already_exists(format!(
                    "{subtree} in filesystem {fs_uuid} belongs to volume {pv:?}, not {name:?}"
                )));
            }
            Some(_) => {}
            // A fresh directory has no record either; only one that was
            // already there needs the "holds no data" check (module docs).
            None if !existing.is_empty() || fresh => {}
            None => {
                let used = fs
                    .quota_get(&subtree)
                    .await
                    .map_err(|e| status(&format!("quota.get {subtree}"), e))?
                    .used_bytes;
                if used > 0 {
                    return Err(Status::already_exists(format!(
                        "{subtree} in filesystem {fs_uuid} already exists, holds {used} bytes \
                         and carries no volume record: not adopting a directory this driver \
                         did not create"
                    )));
                }
            }
        }
        if existing.contains_key(X_CREATED) {
            let stored: u64 = existing
                .get(X_CAPACITY)
                .and_then(|c| c.parse().ok())
                .ok_or_else(|| {
                    Status::internal(format!("{subtree}: the volume record has no capacity"))
                })?;
            let stored_source = existing.get(X_SOURCE).cloned().unwrap_or_default();
            if stored_source != source {
                return Err(Status::already_exists(format!(
                    "volume {name} exists with content source {stored_source:?}"
                )));
            }
            if !capacity_compatible(stored, req.capacity_range.as_ref()) {
                return Err(Status::already_exists(format!(
                    "volume {name} exists with capacity {stored} bytes, incompatible with the \
                     requested range"
                )));
            }
            return Ok(volume(id, stored, context));
        }

        // A new directory, or an earlier attempt that never reached its
        // commit mark: (re)write the whole record, then the quota, then the
        // mark.
        let params = &req.parameters;
        let record = [
            (X_PV, name.to_string()),
            (X_PVC, params.get(PVC_NAME_KEY).cloned().unwrap_or_default()),
            (
                X_NAMESPACE,
                params.get(PVC_NAMESPACE_KEY).cloned().unwrap_or_default(),
            ),
            (X_CAPACITY, capacity.to_string()),
            (X_SOURCE, source),
        ];
        for (key, value) in record {
            set_xattr(fs, &subtree, key, &value).await?;
        }
        self.set_quota(fs, &subtree, quota_of(capacity))
            .await
            .map_err(|e| quota_status(&subtree, e))?;
        set_xattr(fs, &subtree, X_CREATED, &unix_ms().to_string()).await?;
        tracing::info!(volume_id = %id, capacity, "created volume");
        Ok(volume(id, capacity, context))
    }

    /// `DeleteVolume` of a pool volume through `fs` (module docs): gone
    /// already → `OK`; else release the quota, then rename into `/.trash`.
    async fn trash_volume(
        &self,
        fs: &dyn ControlClient,
        id: &VolumeId,
        name: &str,
        subtree: &str,
    ) -> Result<(), Status> {
        // Gone already (trashed by an earlier attempt, or never made):
        // decided here, not by how `quota.set` fails, and in O(1) — never
        // a walk of the volume (module docs).
        match fs
            .browse_xattr(XattrParams {
                path: subtree.to_string(),
                op: XattrOp::List,
            })
            .await
        {
            Ok(_) => {}
            Err(e) if e.kind == ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(status(&format!("browse.xattr list {subtree}"), e)),
        }
        // Release the quota first (§"Deletion and purge"), then trash. A
        // `NotFound` now is a concurrent delete outside this process.
        match self.set_quota(fs, subtree, Some(0)).await {
            Ok(_) => {}
            Err(e) if e.kind == ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(quota_status(subtree, e)),
        }
        fs.browse_mkdir(MkdirParams {
            path: TRASH_DIR.to_string(),
            mode: None,
            parents: true,
        })
        .await
        .map_err(|e| status("browse.mkdir /.trash", e))?;
        let trashed = format!("{TRASH_DIR}/{name}-{}", unix_ms());
        match fs
            .browse_rename(RenameParams {
                from: subtree.to_string(),
                to: trashed.clone(),
                overwrite: false,
            })
            .await
        {
            Ok(_) => tracing::info!(volume_id = %id, trashed, "deleted pool volume"),
            Err(e) if e.kind == ErrorKind::NotFound => {}
            Err(e) => return Err(status("browse.rename to trash", e)),
        }
        Ok(())
    }
}

/// Transient control failures: worth retrying an idempotent call over.
fn is_transient(e: &ControlError) -> bool {
    matches!(
        e.kind,
        ErrorKind::Failed | ErrorKind::Unavailable | ErrorKind::Timeout
    )
}

/// A 0-50% stretch, so concurrent retriers do not stay in lockstep (the
/// same reasoning as the engine's write-gate backoff).
fn jittered(backoff: Duration) -> Duration {
    use std::hash::{BuildHasher, Hasher};
    let bits = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    let fraction = (bits >> 11) as f64 / (1u64 << 53) as f64;
    backoff + backoff.mul_f64(fraction * 0.5)
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `0` is "no cap" in the record and in `capacity_bytes` (CSI: 0 =
/// unknown); the quota itself is then cleared rather than set to zero.
fn quota_of(capacity: u64) -> Option<u64> {
    (capacity > 0).then_some(capacity)
}

fn volume(id: &VolumeId, capacity: u64, parameters: &HashMap<String, String>) -> Volume {
    Volume {
        capacity_bytes: i64::try_from(capacity).unwrap_or(i64::MAX),
        volume_id: id.to_string(),
        volume_context: crate::params::volume_context(parameters),
        content_source: None,
        accessible_topology: Vec::new(),
    }
}

/// The capacity a request asks for: `required_bytes` when set, else the
/// largest the caller allows (`limit_bytes`), else `0` (no cap). Negative
/// or inverted ranges are `INVALID_ARGUMENT`.
fn requested_capacity(range: Option<&CapacityRange>) -> Result<u64, Status> {
    let Some(range) = range else { return Ok(0) };
    let (required, limit) = (range.required_bytes, range.limit_bytes);
    if required < 0 || limit < 0 {
        return Err(Status::invalid_argument(
            "capacity_range must not be negative",
        ));
    }
    if limit > 0 && required > limit {
        return Err(Status::invalid_argument(format!(
            "capacity_range: required_bytes {required} exceeds limit_bytes {limit}"
        )));
    }
    Ok(if required > 0 { required } else { limit } as u64)
}

/// Whether a stored capacity satisfies a (repeated) request's range.
fn capacity_compatible(stored: u64, range: Option<&CapacityRange>) -> bool {
    let Some(range) = range else { return true };
    let (required, limit) = (range.required_bytes as u64, range.limit_bytes as u64);
    if stored == 0 {
        // Unlimited satisfies only a request that also asked for no cap.
        return required == 0 && limit == 0;
    }
    stored >= required && (limit == 0 || stored <= limit)
}

/// The access modes of settled decision 14 (all of them; RWX is the
/// headline) over a mount — never a raw block device.
fn capability_supported(cap: &VolumeCapability) -> Result<(), String> {
    match cap.access_type {
        Some(AccessType::Mount(_)) => {}
        Some(AccessType::Block(_)) => {
            return Err("block access is not supported: volumes are filesystem mounts".into())
        }
        None => return Err("a volume capability must set an access type".into()),
    }
    let mode = cap
        .access_mode
        .as_ref()
        .and_then(|m| Mode::try_from(m.mode).ok())
        .unwrap_or(Mode::Unknown);
    match mode {
        Mode::Unknown => Err("a volume capability must set a known access mode".into()),
        _ => Ok(()),
    }
}

fn check_capabilities(caps: &[VolumeCapability]) -> Result<(), String> {
    if caps.is_empty() {
        return Err("volume_capabilities must not be empty".into());
    }
    caps.iter().try_for_each(capability_supported)
}

/// `ControlError` → gRPC status for a failed step `what`. Callers map the
/// kinds that mean something specific at their step (a `Conflict` from
/// `fs.create`, a `NotFound` that means "already deleted") themselves.
pub(crate) fn status(what: &str, e: ControlError) -> Status {
    let message = format!("{what}: {}", e.message);
    if e.code == Some(Code::NoSpace) {
        return Status::resource_exhausted(message);
    }
    match e.kind {
        ErrorKind::NotFound => Status::not_found(message),
        ErrorKind::Denied => Status::permission_denied(message),
        ErrorKind::Invalid => Status::invalid_argument(message),
        ErrorKind::Unsupported => Status::unimplemented(message),
        ErrorKind::Failed => Status::internal(message),
        ErrorKind::Cancelled => Status::cancelled(message),
        ErrorKind::Unavailable => Status::unavailable(message),
        ErrorKind::Conflict => Status::failed_precondition(message),
        ErrorKind::Timeout => Status::deadline_exceeded(message),
    }
}

/// A `quota.set` that still fails after its retries. A transient failure is
/// `ABORTED`: everything before it is committed and idempotent, so the
/// sidecar's retry of the whole RPC resumes where this one stopped.
fn quota_status(subtree: &str, e: ControlError) -> Status {
    if is_transient(&e) {
        Status::aborted(format!(
            "quota.set {subtree} kept failing transiently ({}); retry the request",
            e.message
        ))
    } else {
        status(&format!("quota.set {subtree}"), e)
    }
}

async fn set_xattr(
    fs: &dyn ControlClient,
    path: &str,
    name: &str,
    value: &str,
) -> Result<(), Status> {
    fs.browse_xattr(XattrParams {
        path: path.to_string(),
        op: XattrOp::Set {
            name: name.to_string(),
            value: value.as_bytes().to_vec().into(),
        },
    })
    .await
    .map(drop)
    .map_err(|e| status(&format!("browse.xattr set {name} on {path}"), e))
}

/// The `user.constellation.csi.*` xattrs on `path`. Lists first and only
/// reads names that exist: the engine reports a missing attribute as
/// `NotFound` (`ENODATA`), indistinguishable from a missing path.
pub(crate) async fn read_record(
    fs: &dyn ControlClient,
    path: &str,
) -> Result<BTreeMap<String, String>, Status> {
    let names = fs
        .browse_xattr(XattrParams {
            path: path.to_string(),
            op: XattrOp::List,
        })
        .await
        .map_err(|e| status(&format!("browse.xattr list {path}"), e))?
        .names;
    let mut record = BTreeMap::new();
    for name in names.into_iter().filter(|n| n.starts_with(XATTR_PREFIX)) {
        let value = fs
            .browse_xattr(XattrParams {
                path: path.to_string(),
                op: XattrOp::Get { name: name.clone() },
            })
            .await
            .map_err(|e| status(&format!("browse.xattr get {name} on {path}"), e))?
            .value
            .map(|v| String::from_utf8_lossy(&v.0).into_owned())
            .unwrap_or_default();
        record.insert(name, value);
    }
    Ok(record)
}

/// The lock key for an existing volume (module docs): a pool volume's name,
/// so create (keyed by `req.name`) and delete/expand serialize.
fn lock_key(id: &VolumeId, raw: &str) -> String {
    match id {
        VolumeId::Pool { name, .. } => name.clone(),
        _ => raw.to_string(),
    }
}

/// A request's `secrets` as [`crate::credentials::Secrets`].
pub(crate) fn secrets_of(
    secrets: &std::collections::HashMap<String, String>,
) -> crate::credentials::Secrets {
    secrets
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Whom a CSI call is for, as every engine audit line it causes records
/// it (`on_behalf_of`, plan 37 K6a): the PersistentVolume's name — a pool
/// volume's id carries it — else the volume id itself (a dedicated or a
/// static volume), squeezed into the protocol's token alphabet.
pub(crate) fn attribution(volume_id: &str) -> String {
    match VolumeId::parse(volume_id) {
        Ok(id) => attribution_of_volume(&id),
        Err(_) => attribution_of(volume_id),
    }
}

/// [`attribution`] of a parsed volume id.
pub(crate) fn attribution_of_volume(id: &VolumeId) -> String {
    match id {
        VolumeId::Pool { name, .. } => attribution_of(name),
        _ => attribution_of(&id.to_string()),
    }
}

/// `name` in `on_behalf_of`'s alphabet and length.
pub(crate) fn attribution_of(name: &str) -> String {
    let mut out: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '/' | '@' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    out.truncate(constellation_control::proto::ON_BEHALF_OF_MAX);
    if out.is_empty() {
        out.push('-');
    }
    out
}

#[tonic::async_trait]
impl ControllerRpc for ControllerService {
    async fn create_volume(
        &self,
        request: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let req = request.into_inner();
        let who = attribution_of(&req.name);
        constellation_control::client::on_behalf_of(who, async move {
            validate_name(&req.name).map_err(Status::invalid_argument)?;
            check_capabilities(&req.volume_capabilities).map_err(Status::invalid_argument)?;
            let capacity = requested_capacity(req.capacity_range.as_ref())?;
            let class = ClassParams::parse(&req.parameters).map_err(Status::invalid_argument)?;
            let engines = self.engines()?;
            let _lock = self.locks.try_lock(req.name.clone())?;
            let volume = match (&req.volume_content_source, class.layout) {
                (Some(content), _) => {
                    self.create_from_source(engines, &class, &req, capacity, content)
                        .await?
                }
                (None, Layout::Pool) => {
                    self.create_pool_volume(engines, &class, &req, capacity)
                        .await?
                }
                (None, Layout::Dedicated) => {
                    self.create_dedicated_volume(engines, &class, &req, capacity)
                        .await?
                }
            };
            self.created
                .lock()
                .unwrap()
                .insert(volume.volume_id.clone());
            Ok(Response::new(CreateVolumeResponse {
                volume: Some(volume),
            }))
        })
        .await
    }

    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let req = request.into_inner();
        let who = attribution(&req.volume_id);
        constellation_control::client::on_behalf_of(who, async move {
            if req.volume_id.is_empty() {
                return Err(Status::invalid_argument("volume_id is required"));
            }
            let engines = self.engines()?;
            let id = match VolumeId::parse(&req.volume_id) {
                Ok(id) => id,
                // Not an id this driver ever minted, so no such volume: CSI
                // makes deleting a nonexistent volume `OK`.
                Err(e) => {
                    tracing::info!(error = %e, "DeleteVolume of an unparseable id: nothing to do");
                    return Ok(Response::new(DeleteVolumeResponse {}));
                }
            };
            let (name, subtree) = match &id {
                VolumeId::Pool { name, .. } => (name.clone(), id.subtree()),
                VolumeId::Static { .. } => {
                    return Err(Status::failed_precondition(
                        "statically provisioned volumes are never deleted by this driver; use \
                     persistentVolumeReclaimPolicy: Retain (plan 37 settled decision 17)",
                    ))
                }
                VolumeId::Dedicated { fs_uuid } => {
                    return Err(Status::failed_precondition(format!(
                        "volume {} is a whole filesystem ({fs_uuid}, layout \"dedicated\"), and \
                     deleting one needs the purge primitive of plan 37 K6b, which does not exist \
                     yet: retain it (persistentVolumeReclaimPolicy: Retain), or remove the \
                     filesystem's bucket prefix by hand once its PV is gone",
                        req.volume_id
                    )))
                }
            };
            let _lock = self.locks.try_lock(lock_key(&id, &req.volume_id))?;
            let uuid = id.fs_uuid().to_string();
            let remembered = self.created.lock().unwrap().contains(&req.volume_id);
            let Some((fs, started)) = self
                .engine_for_delete(
                    engines,
                    &uuid,
                    Handle::Volume(&req.volume_id),
                    remembered,
                    &secrets_of(&req.secrets),
                )
                .await?
            else {
                self.created.lock().unwrap().remove(&req.volume_id);
                return Ok(Response::new(DeleteVolumeResponse {}));
            };
            let result = self.trash_volume(fs.as_ref(), &id, &name, &subtree).await;
            self.retire_if_started(engines, fs, &uuid, started).await;
            if result.is_ok() {
                self.created.lock().unwrap().remove(&req.volume_id);
            }
            result.map(|()| Response::new(DeleteVolumeResponse {}))
        })
        .await
    }

    async fn controller_expand_volume(
        &self,
        request: Request<ControllerExpandVolumeRequest>,
    ) -> Result<Response<ControllerExpandVolumeResponse>, Status> {
        let req = request.into_inner();
        let who = attribution(&req.volume_id);
        constellation_control::client::on_behalf_of(who, async move {
            if req.volume_id.is_empty() {
                return Err(Status::invalid_argument("volume_id is required"));
            }
            if req.capacity_range.is_none() {
                return Err(Status::invalid_argument("capacity_range is required"));
            }
            let wanted = requested_capacity(req.capacity_range.as_ref())?;
            if let Some(cap) = &req.volume_capability {
                capability_supported(cap).map_err(Status::invalid_argument)?;
            }
            let engines = self.engines()?;
            let id =
                VolumeId::parse(&req.volume_id).map_err(|e| Status::not_found(e.to_string()))?;
            if let VolumeId::Static { .. } = id {
                return Err(Status::invalid_argument(
                    "statically provisioned volumes have no driver-managed capacity",
                ));
            }
            let subtree = id.subtree();
            let _lock = self.locks.try_lock(lock_key(&id, &req.volume_id))?;
            let fs = engines
                .filesystem(id.fs_uuid(), &secrets_of(&req.secrets))
                .await
                .map_err(|e| status("reaching the volume's engine", e))?;
            // The cap alone: raising a cap never walks the volume (module
            // docs) — above the old cap is above what the cap admitted.
            let current = fs
                .quota_cap(&subtree)
                .await
                .map_err(|e| status(&format!("quota.get {subtree}"), e))?;
            // Only capping a volume that had none can land below its contents,
            // and only that (rare) case pays for the walk.
            if current.is_none() && wanted > 0 {
                let used = fs
                    .quota_get(&subtree)
                    .await
                    .map_err(|e| status(&format!("quota.get {subtree}"), e))?
                    .used_bytes;
                if wanted < used {
                    return Err(Status::out_of_range(format!(
                        "{wanted} bytes is below the {used} bytes already used"
                    )));
                }
            }
            // Grow only: a request at or below the current cap is already
            // satisfied (and `0`, "no particular size", leaves it alone).
            let capacity = match current {
                Some(cap) if cap >= wanted => cap,
                None if wanted == 0 => 0,
                _ => {
                    self.set_quota(fs.as_ref(), &subtree, Some(wanted))
                        .await
                        .map_err(|e| quota_status(&subtree, e))?;
                    wanted
                }
            };
            if let VolumeId::Pool { .. } = id {
                set_xattr(fs.as_ref(), &subtree, X_CAPACITY, &capacity.to_string()).await?;
            }
            Ok(Response::new(ControllerExpandVolumeResponse {
                capacity_bytes: i64::try_from(capacity).unwrap_or(i64::MAX),
                // Settled decision 6: the quota is the whole size.
                node_expansion_required: false,
            }))
        })
        .await
    }

    async fn validate_volume_capabilities(
        &self,
        request: Request<ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<ValidateVolumeCapabilitiesResponse>, Status> {
        let req = request.into_inner();
        let who = attribution(&req.volume_id);
        constellation_control::client::on_behalf_of(who, async move {
            if req.volume_id.is_empty() {
                return Err(Status::invalid_argument("volume_id is required"));
            }
            if req.volume_capabilities.is_empty() {
                return Err(Status::invalid_argument("volume_capabilities is required"));
            }
            let engines = self.engines()?;
            let id =
                VolumeId::parse(&req.volume_id).map_err(|e| Status::not_found(e.to_string()))?;
            let fs = engines
                .filesystem(id.fs_uuid(), &secrets_of(&req.secrets))
                .await
                .map_err(|e| status("reaching the volume's engine", e))?;
            fs.quota_cap(&id.subtree())
                .await
                .map_err(|e| status(&format!("quota.get {}", id.subtree()), e))?;
            let response = match check_capabilities(&req.volume_capabilities) {
                Ok(()) => ValidateVolumeCapabilitiesResponse {
                    confirmed: Some(validate_volume_capabilities_response::Confirmed {
                        volume_context: req.volume_context,
                        volume_capabilities: req.volume_capabilities,
                        parameters: req.parameters,
                        mutable_parameters: req.mutable_parameters,
                    }),
                    message: String::new(),
                },
                Err(why) => ValidateVolumeCapabilitiesResponse {
                    confirmed: None,
                    message: why,
                },
            };
            Ok(Response::new(response))
        })
        .await
    }

    async fn controller_get_capabilities(
        &self,
        _request: Request<ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<ControllerGetCapabilitiesResponse>, Status> {
        // Only what works: csi-sanity exercises whatever is advertised.
        // Not GET_CAPACITY (§11: an S3 bucket's capacity is meaningless to
        // the scheduler), not PUBLISH_UNPUBLISH_VOLUME (settled decision
        // 4), not LIST_VOLUMES.
        let capabilities = [
            RpcType::CreateDeleteVolume,
            RpcType::ExpandVolume,
            RpcType::CreateDeleteSnapshot,
            RpcType::ListSnapshots,
            RpcType::CloneVolume,
            // Alpha in the spec; ListSnapshots by id with NOT_FOUND.
            RpcType::GetSnapshot,
            // Settled decision 14 accepts SINGLE_NODE_{SINGLE,MULTI}_WRITER.
            RpcType::SingleNodeMultiWriter,
        ]
        .into_iter()
        .map(|t| ControllerServiceCapability {
            r#type: Some(CapabilityType::Rpc(Rpc { r#type: t as i32 })),
        })
        .collect();
        Ok(Response::new(ControllerGetCapabilitiesResponse {
            capabilities,
        }))
    }

    async fn controller_publish_volume(
        &self,
        _request: Request<ControllerPublishVolumeRequest>,
    ) -> Result<Response<ControllerPublishVolumeResponse>, Status> {
        Err(unimplemented(
            "ControllerPublishVolume (settled decision 4)",
        ))
    }

    async fn controller_unpublish_volume(
        &self,
        _request: Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<ControllerUnpublishVolumeResponse>, Status> {
        Err(unimplemented(
            "ControllerUnpublishVolume (settled decision 4)",
        ))
    }

    async fn list_volumes(
        &self,
        _request: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        Err(unimplemented("ListVolumes"))
    }

    async fn controller_list_volume_health(
        &self,
        _request: Request<ControllerListVolumeHealthRequest>,
    ) -> Result<Response<ControllerListVolumeHealthResponse>, Status> {
        Err(unimplemented("ControllerListVolumeHealth"))
    }

    async fn controller_get_volume_health(
        &self,
        _request: Request<ControllerGetVolumeHealthRequest>,
    ) -> Result<Response<ControllerGetVolumeHealthResponse>, Status> {
        Err(unimplemented("ControllerGetVolumeHealth"))
    }

    async fn get_capacity(
        &self,
        _request: Request<GetCapacityRequest>,
    ) -> Result<Response<GetCapacityResponse>, Status> {
        Err(unimplemented("GetCapacity (§11)"))
    }

    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let req = request.into_inner();
        let who = attribution(&req.source_volume_id);
        let snapshot =
            constellation_control::client::on_behalf_of(who, self.create_snapshot_rpc(req)).await?;
        Ok(Response::new(CreateSnapshotResponse {
            snapshot: Some(snapshot),
        }))
    }

    async fn delete_snapshot(
        &self,
        request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        let req = request.into_inner();
        let who = match crate::volume_id::SnapshotId::parse(&req.snapshot_id) {
            Ok(id) => attribution_of_volume(&id.volume),
            Err(_) => attribution_of(&req.snapshot_id),
        };
        constellation_control::client::on_behalf_of(who, self.delete_snapshot_rpc(req)).await?;
        Ok(Response::new(DeleteSnapshotResponse {}))
    }

    async fn list_snapshots(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        Ok(Response::new(
            self.list_snapshots_rpc(request.into_inner()).await?,
        ))
    }

    async fn get_snapshot(
        &self,
        request: Request<GetSnapshotRequest>,
    ) -> Result<Response<GetSnapshotResponse>, Status> {
        let snapshot = self.get_snapshot_rpc(request.into_inner()).await?;
        Ok(Response::new(GetSnapshotResponse {
            snapshot: Some(snapshot),
        }))
    }

    async fn controller_get_volume(
        &self,
        _request: Request<ControllerGetVolumeRequest>,
    ) -> Result<Response<ControllerGetVolumeResponse>, Status> {
        Err(unimplemented("ControllerGetVolume"))
    }

    async fn controller_modify_volume(
        &self,
        _request: Request<ControllerModifyVolumeRequest>,
    ) -> Result<Response<ControllerModifyVolumeResponse>, Status> {
        Err(unimplemented("ControllerModifyVolume"))
    }
}

fn unimplemented(rpc: &str) -> Status {
    Status::unimplemented(format!("{rpc} is not implemented and not advertised"))
}

mod snapshots;

#[cfg(test)]
mod tests;
