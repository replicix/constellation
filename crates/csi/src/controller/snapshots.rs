//! Snapshots and clones (plan 37 §5, K4): `CreateSnapshot`,
//! `DeleteSnapshot`, `ListSnapshots`, and `CreateVolume` from a
//! `VolumeContentSource`, all on plan 32's held snapshots (settled decision
//! 8). This driver keeps no snapshot storage or table of its own.
//!
//! **A snapshot is an engine snapshot of the source volume's subtree**, named
//! `req.name` and born held: `snapshot.create{selector:
//! <subtree>@<req.name>, held_by: "csi:<snapshot_id>"}`. Its `snapshot_id`
//! is `<source volume_id>@<req.name>` ([`SnapshotId`]), so it names the
//! filesystem, the shard, the subtree and the snapshot, and every RPC finds
//! the snapshot from the id alone.
//!
//! **The hold's owner is `csi:<snapshot_id>`.** Plan 37 §16 writes
//! `csi:<VolumeSnapshotContent uid>`, but no CSI request carries that uid:
//! external-snapshotter sends `req.name` (`snapshot-<VolumeSnapshot uid>`)
//! and, with `--extra-create-metadata`, the content's *name*. The snapshot
//! id is what the content records as `status.snapshotHandle`, so the owner
//! still names the Kubernetes object holding the snapshot, and it makes a
//! listing self-describing: every `csi:`-held row carries the id
//! `ListSnapshots` must report, the source volume id included. (A row's
//! path alone cannot say which shard its filesystem is, nor whether a
//! static handle named it.)
//!
//! **Idempotency** (§5, keyed by `req.name`): an existing snapshot of the
//! same subtree with that name is the same snapshot (`OK`, the same
//! answer); a `csi:`-held snapshot with that name of another volume is
//! `ALREADY_EXISTS`. CSI names are unique per driver, so the check looks at
//! the source's filesystem and at every filesystem an engine pod serves
//! now (other shards, other pools), not at filesystems with no engine up:
//! that would mean starting every pool's pod for each snapshot.
//!
//! **`DeleteSnapshot`** releases this driver's hold (`snapshot.hold{held:
//! false, by}`) and deletes the snapshot only if no other hold remains: a
//! snapshot a human held as well (`snapshot hold --force` moved the hold to
//! `user:…`, or a plain hold) is left in place and the RPC succeeds — the
//! Kubernetes object goes, the snapshot stays the human's. A missing
//! snapshot, an id this driver never minted and an already-deleted one are
//! all `OK`.
//!
//! **`ListSnapshots`** lists `csi:`-held snapshots only, except when asked for
//! one `snapshot_id`: a pre-provisioned `VolumeSnapshotContent` may name any
//! snapshot by `<volume_id>@<name>` (an import of a human's snapshot), and
//! external-snapshotter asks for exactly that id. Unfiltered, it covers the
//! filesystems an engine pod serves now ([`Engines::running_filesystems`]):
//! listing starts no pod. Entries are ordered by snapshot id, and the
//! pagination token is the last id returned (`ABORTED` for anything that is
//! not such a token), so a snapshot deleted between pages shifts nothing.
//!
//! **`size_bytes`** (the content's `restoreSize`) is the accounting index's
//! live `REFER` (plan 32 §6.1: the distinct chunks the snapshot references)
//! whenever the engine's index answers `ok`, and the row's creation-time
//! `refer_bytes` while it is `building` or `off` (coordinator decision,
//! 2026-10-04). No CSI call asks for sizes (`sizes: false`): the engine only
//! peeks at its index, so the live figure appears once the index is
//! current, and no call waits for the index or fails on it. A
//! fresh `CreateSnapshot`'s answer is the engine's new row, which the index
//! has not seen: `refer_bytes`, and the live figure from the next listing
//! on.
//!
//! **Known gaps.** An unfiltered `ListSnapshots` covers only filesystems with
//! an engine pod up, so snapshots of idle pools are not listed (a CSI
//! semantics gap; asking for one `snapshot_id` or source volume reaches it).
//! A `DeleteVolume` for a volume the cluster names no PV for, when no engine
//! pod is up and this process did not create the volume (a restart in
//! between), is answered `OK` without trashing it (logged at warn): the
//! volume and its quota stay until removed by hand. A volume this process
//! created and has not deleted starts the engine and is trashed.
//!
//! **Clone and restore** (`CreateVolume` with a content source) are
//! `clone.create` inside the source's own filesystem: metadata only, no
//! data is copied (the new tree's files reference the snapshot's chunks).
//! The destination is the class's pool shard *the source lives in* — the
//! source's shard, not `hash(req.name)`'s, or a clone in a sharded pool
//! would land elsewhere three times in four. That filesystem must be the
//! source's: a source in another pool, another shard of this one (a class
//! with fewer shards), a dedicated filesystem, or a dedicated destination
//! class, is `INVALID_ARGUMENT` naming both filesystems (settled decision 8:
//! never a silent full copy).
//!
//! `clone.create` clones snapshots only, so a volume clone (`FromVolume`)
//! takes a transient, unheld snapshot of the source first
//! (`csi-clone-<hash of the new name>`, the same on every retry), clones it
//! and deletes it. The clone carries the source's root xattrs — its volume
//! record, `created` mark included — so the record is rewritten before the
//! quota and the new mark; a retry that finds the raw copy (the source's
//! `pv` in the record) completes it in place.

use super::{read_record, secrets_of, status, ControllerService, X_CREATED, X_PV};
use crate::control_client::{ControlClient, Engines, Handle, PoolRef};
use crate::credentials::Secrets;
use crate::params::{ClassParams, Layout};
use crate::proto::csi::v1::volume_content_source::Type as SourceType;
use crate::proto::csi::v1::*;
use crate::volume_id::{validate_snapshot_name, SnapshotId, VolumeId, VOLUMES_DIR};
use constellation_control::proto::types::{
    CloneParams, MkdirParams, SizeState, SnapshotCreateParams, SnapshotDeleteParams,
    SnapshotHoldParams, SnapshotListParams, SnapshotStatus, XattrOp, XattrParams,
};
use constellation_control::proto::ErrorKind;
use std::sync::Arc;
use tonic::Status;

/// The hold owner prefix of this driver's snapshots (plan 32 §0.4).
pub(crate) const OWNER_PREFIX: &str = "csi:";
/// What `ListSnapshots`' `next_token` starts with; the rest is the last
/// snapshot id returned.
const TOKEN_PREFIX: &str = "after:";

/// The hold owner of snapshot `id` (module docs).
pub(crate) fn owner_of(id: &SnapshotId) -> String {
    format!("{OWNER_PREFIX}{id}")
}

/// A content source, parsed: what `CreateVolume` clones from.
enum Source {
    Volume(VolumeId),
    Snapshot(SnapshotId),
}

impl Source {
    fn volume(&self) -> &VolumeId {
        match self {
            Source::Volume(v) => v,
            Source::Snapshot(s) => &s.volume,
        }
    }

    /// The volume record's `source` value.
    fn record(&self) -> String {
        match self {
            Source::Volume(v) => format!("volume:{v}"),
            Source::Snapshot(s) => format!("snapshot:{s}"),
        }
    }
}

/// The transient snapshot a volume clone is made through (module docs).
fn transient_name(new_volume: &str) -> String {
    let hash = blake3::hash(new_volume.as_bytes()).to_hex();
    format!("csi-clone-{}", &hash[..32])
}

fn creation_time(row: &SnapshotStatus) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: row.created_unix_ms.div_euclid(1000),
        nanos: (row.created_unix_ms.rem_euclid(1000) * 1_000_000) as i32,
    }
}

/// A snapshot's `size_bytes`: the accounting index's live `REFER` (plan 32
/// §6.1, deduplicated, as of the index's last catch-up) when the index
/// answers `ok`, else the row's creation-time `refer_bytes` — while the
/// index is `building` or `off`, and on `CreateSnapshot`'s own answer,
/// which the index has not seen yet. Reads what a peek found; never a wait.
fn size_of(row: &SnapshotStatus) -> i64 {
    let live = match row.size_state {
        Some(SizeState::Ok) => row.refer,
        _ => None,
    };
    live.or(row.refer_bytes)
        .map(|b| i64::try_from(b).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn snapshot_of(id: &SnapshotId, row: &SnapshotStatus) -> Snapshot {
    Snapshot {
        size_bytes: size_of(row),
        snapshot_id: id.to_string(),
        source_volume_id: id.volume.to_string(),
        creation_time: Some(creation_time(row)),
        // A snapshot is a frozen metadata root: usable the moment it exists.
        ready_to_use: true,
        ..Default::default()
    }
}

/// The snapshot id a row's `csi:` hold names, when it is this driver's.
fn csi_id(row: &SnapshotStatus) -> Option<SnapshotId> {
    let owner = row.held_by.as_deref()?.strip_prefix(OWNER_PREFIX)?;
    SnapshotId::parse(owner).ok()
}

/// Rows of `fs` at exactly `path` (`None`: all of them). Never asks for
/// sizes (`sizes: false`): the engine then only peeks at its accounting
/// index, filling `refer` when the index is current and never waiting for
/// it ([`size_of`]).
async fn list(fs: &dyn ControlClient, path: Option<&str>) -> Result<Vec<SnapshotStatus>, Status> {
    fs.snapshot_list(SnapshotListParams {
        path: path.map(str::to_string),
        sizes: false,
    })
    .await
    .map(|l| l.snapshots)
    .map_err(|e| status("snapshot.list", e))
}

/// The row `<path>@<name>` of `fs`, if any.
async fn find(
    fs: &dyn ControlClient,
    path: &str,
    name: &str,
) -> Result<Option<SnapshotStatus>, Status> {
    Ok(list(fs, Some(path))
        .await?
        .into_iter()
        .find(|r| r.name == name))
}

/// Whether `path` exists in `fs` (an O(1) xattr list, never a walk).
async fn exists(fs: &dyn ControlClient, path: &str) -> Result<bool, Status> {
    match fs
        .browse_xattr(XattrParams {
            path: path.to_string(),
            op: XattrOp::List,
        })
        .await
    {
        Ok(_) => Ok(true),
        Err(e) if e.kind == ErrorKind::NotFound => Ok(false),
        Err(e) => Err(status(&format!("browse.xattr list {path}"), e)),
    }
}

impl ControllerService {
    /// The engine a delete of something in `fs_uuid` works through, and
    /// whether this call started it (the caller then [`Engines::retire`]s
    /// it). `None`: nothing to delete, and nothing was started — no engine
    /// pod serves the filesystem and the CO no longer has an object naming
    /// `handle` (a delete repeated after it succeeded), or the filesystem
    /// is gone.
    pub(super) async fn engine_for_delete(
        &self,
        engines: &Arc<dyn Engines>,
        fs_uuid: &str,
        handle: Handle<'_>,
        remembered: bool,
        secrets: &Secrets,
    ) -> Result<Option<(Arc<dyn ControlClient>, bool)>, Status> {
        if let Some(fs) = engines
            .running(fs_uuid, secrets)
            .await
            .map_err(|e| status("reaching the filesystem's engine", e))?
        {
            return Ok(Some((fs, false)));
        }
        let named = engines
            .named(handle)
            .await
            .map_err(|e| status("asking the cluster whether the object still exists", e))?;
        if !named && !remembered {
            tracing::warn!(
                ?handle,
                fs_uuid,
                "delete of an object the cluster has no record of, with no engine running \
                 and no create of it in this process: assumed deleted already, nothing \
                 started (if the volume was never saved as a PV, it stays until removed \
                 by hand)"
            );
            return Ok(None);
        }
        match engines.filesystem(fs_uuid, secrets).await {
            Ok(fs) => Ok(Some((fs, true))),
            Err(e) if e.kind == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(status("reaching the filesystem's engine", e)),
        }
    }

    /// Stop an engine a delete started for itself (`started`), once its
    /// client is dropped. A failure only leaves a pod running: logged.
    pub(super) async fn retire_if_started(
        &self,
        engines: &Arc<dyn Engines>,
        fs: Arc<dyn ControlClient>,
        fs_uuid: &str,
        started: bool,
    ) {
        drop(fs);
        if started {
            if let Err(e) = engines.retire(fs_uuid).await {
                tracing::warn!(fs_uuid, error = %e, "could not stop the engine pod a delete started");
            }
        }
    }

    pub(super) async fn create_snapshot_rpc(
        &self,
        req: CreateSnapshotRequest,
    ) -> Result<Snapshot, Status> {
        if req.name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }
        if req.source_volume_id.is_empty() {
            return Err(Status::invalid_argument("source_volume_id is required"));
        }
        validate_snapshot_name(&req.name).map_err(Status::invalid_argument)?;
        let engines = self.engines()?;
        let secrets = secrets_of(&req.secrets);
        let volume = VolumeId::parse(&req.source_volume_id)
            .map_err(|e| Status::not_found(format!("source volume: {e}")))?;
        let _lock = self.snapshot_locks.try_lock(req.name.clone())?;
        let id = SnapshotId {
            volume,
            name: req.name.clone(),
        };
        let fs = engines
            .filesystem(id.volume.fs_uuid(), &secrets)
            .await
            .map_err(|e| status("reaching the source volume's engine", e))?;
        let path = id.volume.subtree();
        if let Some(row) = find(fs.as_ref(), &path, &id.name).await? {
            return Ok(snapshot_of(&id, &row));
        }
        // The same name taken by a snapshot of another volume (module docs):
        // in the source's filesystem, and in every other one an engine
        // serves now (another shard, another pool).
        let mut others = engines
            .running_filesystems()
            .await
            .map_err(|e| status("listing the running engines", e))?;
        others.retain(|u| u != id.volume.fs_uuid());
        let mut taken = taken_by_other(fs.as_ref(), &id).await?;
        for uuid in others {
            if taken.is_some() {
                break;
            }
            if let Some(other_fs) = reach(engines, &uuid, &Secrets::new()).await? {
                taken = taken_by_other(other_fs.as_ref(), &id).await?;
            }
        }
        if let Some(other) = taken {
            return Err(Status::already_exists(format!(
                "snapshot {} exists already, of volume {}",
                id.name, other.volume
            )));
        }
        if !exists(fs.as_ref(), &path).await? {
            return Err(Status::not_found(format!(
                "source volume {} does not exist",
                id.volume
            )));
        }
        let created = fs
            .snapshot_create(SnapshotCreateParams {
                selector: id.selector(),
                held_by: Some(owner_of(&id)),
                ..Default::default()
            })
            .await;
        let row = match created {
            Ok(c) => c.snapshot,
            // The engine says "already exists" (or anything) as `Failed`: a
            // create that raced another process's is the same snapshot.
            Err(e) => match find(fs.as_ref(), &path, &id.name).await? {
                Some(row) => row,
                None => return Err(status(&format!("snapshot.create {}", id.selector()), e)),
            },
        };
        tracing::info!(snapshot_id = %id, "created snapshot");
        Ok(snapshot_of(&id, &row))
    }

    pub(super) async fn delete_snapshot_rpc(
        &self,
        req: DeleteSnapshotRequest,
    ) -> Result<(), Status> {
        if req.snapshot_id.is_empty() {
            return Err(Status::invalid_argument("snapshot_id is required"));
        }
        let engines = self.engines()?;
        let id = match SnapshotId::parse(&req.snapshot_id) {
            Ok(id) => id,
            Err(e) => {
                tracing::info!(error = %e, "DeleteSnapshot of an unparseable id: nothing to do");
                return Ok(());
            }
        };
        let _lock = self.snapshot_locks.try_lock(id.name.clone())?;
        let uuid = id.volume.fs_uuid().to_string();
        let Some((fs, started)) = self
            .engine_for_delete(
                engines,
                &uuid,
                Handle::Snapshot(&req.snapshot_id),
                false,
                &secrets_of(&req.secrets),
            )
            .await?
        else {
            return Ok(());
        };
        let result = delete_held(fs.as_ref(), &id).await;
        self.retire_if_started(engines, fs, &uuid, started).await;
        result
    }

    pub(super) async fn list_snapshots_rpc(
        &self,
        req: ListSnapshotsRequest,
    ) -> Result<ListSnapshotsResponse, Status> {
        if req.max_entries < 0 {
            return Err(Status::invalid_argument("max_entries must not be negative"));
        }
        let after = match req.starting_token.as_str() {
            "" => None,
            token => Some(
                token
                    .strip_prefix(TOKEN_PREFIX)
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| {
                        Status::aborted(format!("starting_token {token:?} is not a token of ours"))
                    })?
                    .to_string(),
            ),
        };
        let engines = self.engines()?;
        let secrets = secrets_of(&req.secrets);
        let mut found: Vec<Snapshot> = Vec::new();
        if !req.snapshot_id.is_empty() {
            let Ok(id) = SnapshotId::parse(&req.snapshot_id) else {
                return Ok(ListSnapshotsResponse::default());
            };
            if let Some(fs) = reach(engines, id.volume.fs_uuid(), &secrets).await? {
                if let Some(row) = find(fs.as_ref(), &id.volume.subtree(), &id.name).await? {
                    found.push(snapshot_of(&id, &row));
                }
            }
        } else if !req.source_volume_id.is_empty() {
            let Ok(volume) = VolumeId::parse(&req.source_volume_id) else {
                return Ok(ListSnapshotsResponse::default());
            };
            if let Some(fs) = reach(engines, volume.fs_uuid(), &secrets).await? {
                for row in list(fs.as_ref(), Some(&volume.subtree())).await? {
                    if let Some(id) = csi_id(&row).filter(|id| id.volume == volume) {
                        found.push(snapshot_of(&id, &row));
                    }
                }
            }
        } else {
            let uuids = engines
                .running_filesystems()
                .await
                .map_err(|e| status("listing the running engines", e))?;
            for uuid in uuids {
                // An unfiltered list that cannot reach one engine is an error,
                // not a silently partial list; it names the filesystem
                // (pool) it is waiting for.
                let reached = reach(engines, &uuid, &Secrets::new()).await.map_err(|s| {
                    Status::new(
                        s.code(),
                        format!(
                            "listing snapshots: filesystem {uuid} is not reachable yet: {}",
                            s.message()
                        ),
                    )
                })?;
                let Some(fs) = reached else {
                    continue;
                };
                for row in list(fs.as_ref(), None).await? {
                    if let Some(id) = csi_id(&row).filter(|id| id.volume.fs_uuid() == uuid) {
                        found.push(snapshot_of(&id, &row));
                    }
                }
            }
        }
        found.sort_by(|a, b| a.snapshot_id.cmp(&b.snapshot_id));
        found.dedup_by(|a, b| a.snapshot_id == b.snapshot_id);
        if let Some(after) = &after {
            found.retain(|s| s.snapshot_id.as_str() > after.as_str());
        }
        let mut next_token = String::new();
        if req.max_entries > 0 && found.len() > req.max_entries as usize {
            found.truncate(req.max_entries as usize);
            next_token = format!(
                "{TOKEN_PREFIX}{}",
                found.last().expect("non-empty").snapshot_id
            );
        }
        Ok(ListSnapshotsResponse {
            entries: found
                .into_iter()
                .map(|snapshot| list_snapshots_response::Entry {
                    snapshot: Some(snapshot),
                })
                .collect(),
            next_token,
        })
    }

    /// `GetSnapshot` (alpha): `ListSnapshots` by id, `NOT_FOUND` for a
    /// snapshot that is not there (or an id this driver never minted).
    pub(super) async fn get_snapshot_rpc(
        &self,
        req: GetSnapshotRequest,
    ) -> Result<Snapshot, Status> {
        if req.snapshot_id.is_empty() {
            return Err(Status::invalid_argument("snapshot_id is required"));
        }
        let id =
            SnapshotId::parse(&req.snapshot_id).map_err(|e| Status::not_found(e.to_string()))?;
        let engines = self.engines()?;
        let secrets = secrets_of(&req.secrets);
        let fs = reach(engines, id.volume.fs_uuid(), &secrets)
            .await?
            .ok_or_else(|| Status::not_found(format!("no filesystem {}", id.volume.fs_uuid())))?;
        let row = find(fs.as_ref(), &id.volume.subtree(), &id.name)
            .await?
            .ok_or_else(|| Status::not_found(format!("snapshot {id} does not exist")))?;
        Ok(snapshot_of(&id, &row))
    }

    /// `CreateVolume` with a content source (module docs). The caller holds
    /// the volume lock on `req.name`.
    pub(super) async fn create_from_source(
        &self,
        engines: &Arc<dyn Engines>,
        class: &ClassParams,
        req: &CreateVolumeRequest,
        capacity: u64,
        content: &VolumeContentSource,
    ) -> Result<Volume, Status> {
        let source = match &content.r#type {
            Some(SourceType::Snapshot(s)) => Source::Snapshot(
                SnapshotId::parse(&s.snapshot_id)
                    .map_err(|e| Status::not_found(format!("source snapshot: {e}")))?,
            ),
            Some(SourceType::Volume(v)) => Source::Volume(
                VolumeId::parse(&v.volume_id)
                    .map_err(|e| Status::not_found(format!("source volume: {e}")))?,
            ),
            None => {
                return Err(Status::invalid_argument(
                    "volume_content_source names neither a snapshot nor a volume",
                ))
            }
        };
        let name = req.name.as_str();
        let source_uuid = source.volume().fs_uuid().to_string();
        let refuse = |why: String| {
            Status::invalid_argument(format!(
                "cannot create {name} from {}: {why}. A clone or restore is a metadata-only \
                 copy inside one filesystem (plan 37 settled decision 8); create it with a \
                 StorageClass of the source's own pool",
                source.record()
            ))
        };
        if class.layout == Layout::Dedicated {
            return Err(refuse(format!(
                "the StorageClass's layout \"dedicated\" makes a new filesystem for every \
                 volume, and the source lives in filesystem {source_uuid}"
            )));
        }
        let shard = match source.volume() {
            VolumeId::Pool { shard, .. } => *shard,
            VolumeId::Static { .. } => class.shard_for(name),
            VolumeId::Dedicated { fs_uuid } => {
                return Err(refuse(format!(
                    "the source is a dedicated volume, a filesystem of its own ({fs_uuid})"
                )))
            }
        };
        if shard >= class.shards {
            return Err(refuse(format!(
                "the source lives in shard {shard} (filesystem {source_uuid}), and the \
                 StorageClass's pool has {} shard(s)",
                class.shards
            )));
        }
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
        if pool_uuid != source_uuid {
            return Err(refuse(format!(
                "the source lives in filesystem {source_uuid}, and the StorageClass's pool \
                 shard {shard} is filesystem {pool_uuid} (s3://{}/{prefix})",
                class.bucket
            )));
        }
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
        let record_source = source.record();
        // The source volume's own `pv`, which a raw clone carries.
        let source_pv = match source.volume() {
            VolumeId::Pool { name, .. } => Some(name.clone()),
            _ => None,
        };

        let made = async {
            let existing = match exists(fs.as_ref(), &subtree).await? {
                true => Some(read_record(fs.as_ref(), &subtree).await?),
                false => None,
            };
            let mut fresh = false;
            let existing = match existing {
                // Ours: finished (compared in `commit_record`) or part-way.
                Some(record) if record.get(X_PV).map(String::as_str) == Some(name) => record,
                // The raw copy an earlier attempt's clone left (module docs).
                Some(record) if source_pv.is_some() && record.get(X_PV) == source_pv.as_ref() => {
                    fresh = true;
                    record
                }
                Some(_) => {
                    return Err(Status::already_exists(format!(
                        "{subtree} in filesystem {} exists and is not volume {name}",
                        id.fs_uuid()
                    )))
                }
                None => {
                    self.clone_into(fs.as_ref(), &source, name, &subtree)
                        .await?;
                    fresh = true;
                    read_record(fs.as_ref(), &subtree).await?
                }
            };
            let existing = if fresh {
                // The source's record came along: its commit mark first, so a
                // crash from here on leaves a part-way volume, never a
                // finished-looking one with the source's capacity.
                if existing.contains_key(X_CREATED) {
                    fs.browse_xattr(XattrParams {
                        path: subtree.clone(),
                        op: XattrOp::Remove {
                            name: X_CREATED.to_string(),
                        },
                    })
                    .await
                    .map_err(|e| {
                        status(&format!("browse.xattr remove {X_CREATED} on {subtree}"), e)
                    })?;
                }
                Default::default()
            } else {
                existing
            };
            let volume = self
                .commit_volume(
                    fs.as_ref(),
                    &id,
                    req,
                    capacity,
                    existing,
                    true,
                    &req.parameters,
                    &record_source,
                )
                .await?;
            Ok::<_, Status>(volume)
        }
        .await;
        // Best effort: a leftover only costs the metadata it pins and is
        // never expired (manual, unheld), so it goes on failure too — a
        // clone abandoned with its PVC would leak it for good.
        if let Source::Volume(v) = &source {
            drop_transient(fs.as_ref(), v, name).await;
        }
        let mut volume = made?;
        volume.content_source = Some(content.clone());
        Ok(volume)
    }

    /// `clone.create` of `source` to `subtree` (which does not exist yet).
    async fn clone_into(
        &self,
        fs: &dyn ControlClient,
        source: &Source,
        name: &str,
        subtree: &str,
    ) -> Result<(), Status> {
        let selector = match source {
            Source::Snapshot(s) => {
                if find(fs, &s.volume.subtree(), &s.name).await?.is_none() {
                    return Err(Status::not_found(format!(
                        "source snapshot {s} does not exist"
                    )));
                }
                s.selector()
            }
            Source::Volume(v) => {
                let path = v.subtree();
                if !exists(fs, &path).await? {
                    return Err(Status::not_found(format!(
                        "source volume {v} does not exist"
                    )));
                }
                let transient = transient_name(name);
                if find(fs, &path, &transient).await?.is_none() {
                    let selector = format!("{path}@{transient}");
                    if let Err(e) = fs
                        .snapshot_create(SnapshotCreateParams {
                            selector: selector.clone(),
                            ..Default::default()
                        })
                        .await
                    {
                        if find(fs, &path, &transient).await?.is_none() {
                            return Err(status(&format!("snapshot.create {selector}"), e));
                        }
                    }
                }
                format!("{path}@{transient}")
            }
        };
        fs.browse_mkdir(MkdirParams {
            path: VOLUMES_DIR.to_string(),
            mode: None,
            parents: true,
        })
        .await
        .map_err(|e| status("browse.mkdir /volumes", e))?;
        let started = std::time::Instant::now();
        fs.clone_create(CloneParams {
            selector: selector.clone(),
            destination: subtree.to_string(),
        })
        .await
        .map_err(|e| status(&format!("clone.create {selector} -> {subtree}"), e))?;
        tracing::info!(%selector, %subtree, ms = started.elapsed().as_millis() as u64,
            "cloned (metadata only)");
        Ok(())
    }
}

/// Delete the transient snapshot of a volume clone `new_volume`, if there is
/// one. Best effort, logged on failure.
async fn drop_transient(fs: &dyn ControlClient, source: &VolumeId, new_volume: &str) {
    let path = source.subtree();
    let name = transient_name(new_volume);
    if find(fs, &path, &name).await.ok().flatten().is_none() {
        return;
    }
    let selector = format!("{path}@{name}");
    if let Err(e) = fs
        .snapshot_delete(SnapshotDeleteParams {
            selector: selector.clone(),
            force: false,
        })
        .await
    {
        tracing::warn!(snapshot = %selector, error = %e,
            "could not delete a clone's transient snapshot");
    }
}

/// A `csi:`-held snapshot in `fs` named like `id` but not `id`. `snapshot.list`
/// has no name filter, so this reads every snapshot of the filesystem (plan 32's
/// automatic ones too): O(snapshots) per new `CreateSnapshot`.
async fn taken_by_other(
    fs: &dyn ControlClient,
    id: &SnapshotId,
) -> Result<Option<SnapshotId>, Status> {
    Ok(list(fs, None)
        .await?
        .iter()
        .filter(|r| r.name == id.name)
        .filter_map(csi_id)
        .find(|other| other != id))
}

/// Release this driver's hold on `id` and delete it, unless another hold
/// remains (module docs). Missing → `OK`.
async fn delete_held(fs: &dyn ControlClient, id: &SnapshotId) -> Result<(), Status> {
    let path = id.volume.subtree();
    let Some(row) = find(fs, &path, &id.name).await? else {
        return Ok(());
    };
    let owner = owner_of(id);
    if row.held {
        if row.held_by.as_deref() != Some(owner.as_str()) {
            tracing::info!(snapshot_id = %id, held_by = ?row.held_by,
                "snapshot held by another owner too: kept, only the CSI object goes");
            return Ok(());
        }
        if let Err(e) = fs
            .snapshot_hold(SnapshotHoldParams {
                id: id.selector(),
                held: false,
                by: Some(owner.clone()),
                force: false,
            })
            .await
        {
            // Gone meanwhile, or taken over by a human: both end here.
            return match find(fs, &path, &id.name).await? {
                None => Ok(()),
                Some(r) if r.held && r.held_by.as_deref() != Some(owner.as_str()) => Ok(()),
                Some(_) => Err(status(
                    &format!("snapshot.hold release {}", id.selector()),
                    e,
                )),
            };
        }
    }
    match fs
        .snapshot_delete(SnapshotDeleteParams {
            selector: id.selector(),
            force: false,
        })
        .await
    {
        Ok(_) => {
            tracing::info!(snapshot_id = %id, "deleted snapshot");
            Ok(())
        }
        Err(e) => match find(fs, &path, &id.name).await? {
            None => Ok(()),
            // Held again between the release and the delete, by someone
            // else: theirs now.
            Some(r) if r.held => Ok(()),
            Some(_) => Err(status(&format!("snapshot.delete {}", id.selector()), e)),
        },
    }
}

/// The engine of `fs_uuid` for a read: `None` when the filesystem is
/// unknown (`NotFound`), so a listing of something gone is empty.
/// `secrets`: the request's, only when they belong to that filesystem's
/// pool — another pool's engine is reached with none (it runs unlocked
/// already, or the read fails `UNAVAILABLE`), never with credentials that
/// would then be pushed to it as a rotation.
async fn reach(
    engines: &Arc<dyn Engines>,
    fs_uuid: &str,
    secrets: &Secrets,
) -> Result<Option<Arc<dyn ControlClient>>, Status> {
    match engines.filesystem(fs_uuid, secrets).await {
        Ok(fs) => Ok(Some(fs)),
        Err(e) if e.kind == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(status("reaching the filesystem's engine", e)),
    }
}
