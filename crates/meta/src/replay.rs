//! Convergent replay: applying a batch of [`LogRecord`]s — from a
//! shipped segment, a bootstrap replay of the whole log, or a
//! reintegration decision — to the local replica.
//!
//! The whole batch (every record plus the trailing ino-counter
//! recompute) is one fjall write transaction, exactly as it was one
//! SQLite transaction: a crash mid-batch must leave either the fully
//! applied batch or none of it. Idempotency and "skip, don't error" are
//! load-bearing here — the same segment can be tailed twice (at-least-
//! once delivery), and a record whose target was already removed by an
//! earlier-in-the-same-batch conflict must not abort the rest.

use crate::error::MetaError;
use crate::record::{CloneNode, LogRecord};
use crate::store::{atime, misc, ns, reclaim_ino_counter, Meta};
use constellation_fs_core::{Ino, InodeKind};
use constellation_mtree::keys;
use constellation_mtree::record::{self, Attrs, DentryRecord, Kind};
use fjall::SingleWriterWriteTx;
use std::collections::HashSet;

/// Which dentries/inos a batch of records touches, used to suppress a
/// foreign record that collides with the caller's own not-yet-shipped
/// work. Atime is deliberately invisible here (see [`LogRecord::Atime`]):
/// it records neither a dentry nor an ino, so it can neither suppress
/// nor be suppressed.
#[derive(Default)]
pub struct TouchSet {
    pub dentries: HashSet<(u64, String)>,
    pub inos: HashSet<u64>,
}

impl TouchSet {
    pub fn from_records<'a>(records: impl Iterator<Item = &'a LogRecord>) -> Self {
        let mut set = TouchSet::default();
        for rec in records {
            set.add(rec);
        }
        set
    }

    pub fn add(&mut self, rec: &LogRecord) {
        match rec {
            LogRecord::Mkdir {
                parent, name, ino, ..
            }
            | LogRecord::Create {
                parent, name, ino, ..
            }
            | LogRecord::Symlink {
                parent, name, ino, ..
            }
            | LogRecord::Mknod {
                parent, name, ino, ..
            }
            | LogRecord::Link {
                parent, name, ino, ..
            } => {
                self.dentries.insert((*parent, name.clone()));
                self.inos.insert(*ino);
            }
            LogRecord::Unlink { parent, name, .. } | LogRecord::Rmdir { parent, name, .. } => {
                self.dentries.insert((*parent, name.clone()));
            }
            LogRecord::Rename {
                parent,
                name,
                new_parent,
                new_name,
                ..
            } => {
                self.dentries.insert((*parent, name.clone()));
                self.dentries.insert((*new_parent, new_name.clone()));
            }
            LogRecord::Setattr { ino, .. }
            | LogRecord::WriteManifest { ino, .. }
            | LogRecord::SetXattr { ino, .. }
            | LogRecord::RemoveXattr { ino, .. } => {
                self.inos.insert(*ino);
            }
            LogRecord::SnapCreate { .. }
            | LogRecord::SnapDelete { .. }
            | LogRecord::SetQuota { .. }
            | LogRecord::Atime { .. }
            // Touches no dentry/ino: it can neither suppress nor be
            // suppressed, exactly like `Atime` (see this type's doc).
            | LogRecord::Completed { .. } => {}
            LogRecord::Clone { nodes, .. } => {
                for node in nodes {
                    self.dentries.insert((node.parent, node.name.clone()));
                    self.inos.insert(node.ino);
                }
            }
        }
    }

    pub fn conflicts(&self, rec: &LogRecord) -> bool {
        let touched = TouchSet::from_records(std::iter::once(rec));
        touched.dentries.iter().any(|d| self.dentries.contains(d))
            || touched.inos.iter().any(|i| self.inos.contains(i))
    }
}

enum Applied {
    Done,
    Skipped(&'static str),
}

impl Meta {
    pub fn apply_records(&self, records: &[LogRecord]) -> Result<(), MetaError> {
        self.apply_foreign(records, &TouchSet::default())?;
        Ok(())
    }

    /// Apply `records`, skipping any that collide with `pending` (the
    /// caller's own unshipped local journal — see the module doc).
    /// Returns how many were skipped (conflict or a downstream cascade).
    pub fn apply_foreign(
        &self,
        records: &[LogRecord],
        pending: &TouchSet,
    ) -> Result<usize, MetaError> {
        let mut tx = self.db.write_tx();
        let staged = crate::store::UsageTracker::staging();
        let mut skipped = 0usize;
        for rec in records {
            if pending.conflicts(rec) {
                tracing::warn!(
                    ?rec,
                    "replay: skipping record that conflicts with pending local work"
                );
                skipped += 1;
                continue;
            }
            match apply_one(&mut tx, self, rec, &staged)? {
                Applied::Done => {}
                Applied::Skipped(why) => {
                    tracing::warn!(why, ?rec, "replay: skipped");
                    skipped += 1;
                }
            }
        }
        for rec in records {
            if let Some(ino) = primary_ino(rec) {
                reclaim_ino_counter(&mut tx, &self.local, ino)?;
            }
        }
        let (staged_bytes, staged_files) = staged.raw_delta();
        crate::store::adjust_usage_tx(&mut tx, &self.local, staged_bytes, staged_files)?;
        tx.commit()?;
        staged.drain_into(self.usage_tracker());
        Ok(skipped)
    }
}

fn primary_ino(rec: &LogRecord) -> Option<Ino> {
    match rec {
        LogRecord::Mkdir { ino, .. }
        | LogRecord::Create { ino, .. }
        | LogRecord::Symlink { ino, .. }
        | LogRecord::Mknod { ino, .. } => Some(*ino),
        _ => None,
    }
}

#[allow(clippy::too_many_lines)]
fn apply_one(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    rec: &LogRecord,
    staged: &crate::store::UsageTracker,
) -> Result<Applied, MetaError> {
    match rec {
        LogRecord::Mkdir {
            parent,
            name,
            ino,
            mode,
            uid,
            gid,
            time_ns,
        } => insert_node(
            tx,
            meta,
            *parent,
            name,
            *ino,
            Kind::Dir,
            *mode,
            *uid,
            *gid,
            0,
            2,
            0,
            None,
            *time_ns,
            staged,
        ),
        LogRecord::Create {
            parent,
            name,
            ino,
            mode,
            uid,
            gid,
            time_ns,
        } => insert_node(
            tx,
            meta,
            *parent,
            name,
            *ino,
            Kind::File,
            *mode,
            *uid,
            *gid,
            0,
            1,
            0,
            None,
            *time_ns,
            staged,
        ),
        LogRecord::Symlink {
            parent,
            name,
            ino,
            target,
            uid,
            gid,
            time_ns,
        } => insert_node(
            tx,
            meta,
            *parent,
            name,
            *ino,
            Kind::Symlink,
            0o777,
            *uid,
            *gid,
            0,
            1,
            target.len() as u64,
            Some(target.clone()),
            *time_ns,
            staged,
        ),
        LogRecord::Mknod {
            parent,
            name,
            ino,
            kind,
            mode,
            uid,
            gid,
            rdev,
            time_ns,
        } => {
            let Some(kind) = InodeKind::from_u8(*kind) else {
                return Err(MetaError::Invalid(format!("unknown mknod kind {kind}")));
            };
            insert_node(
                tx,
                meta,
                *parent,
                name,
                *ino,
                ns::kind_to_mtree(kind),
                *mode,
                *uid,
                *gid,
                *rdev,
                1,
                0,
                None,
                *time_ns,
                staged,
            )
        }
        LogRecord::Link {
            ino,
            parent,
            name,
            time_ns,
        } => apply_link(tx, meta, *ino, *parent, name, *time_ns),
        LogRecord::Unlink {
            parent,
            name,
            time_ns,
        } => apply_unlink(tx, meta, *parent, name, *time_ns),
        LogRecord::Rmdir {
            parent,
            name,
            time_ns,
        } => apply_rmdir(tx, meta, *parent, name, *time_ns),
        LogRecord::Rename {
            parent,
            name,
            new_parent,
            new_name,
            time_ns,
        } => apply_rename(tx, meta, *parent, name, *new_parent, new_name, *time_ns),
        LogRecord::Setattr {
            ino,
            mode,
            uid,
            gid,
            size,
            atime_ns,
            mtime_ns,
            time_ns,
        } => apply_setattr(
            tx, meta, *ino, *mode, *uid, *gid, *size, *atime_ns, *mtime_ns, *time_ns, staged,
        ),
        LogRecord::WriteManifest {
            ino,
            manifest,
            size,
            time_ns,
            ..
        } => apply_write_manifest(tx, meta, *ino, manifest, *size, *time_ns, staged),
        LogRecord::SetXattr {
            ino,
            name,
            value,
            time_ns,
        } => apply_set_xattr(tx, meta, *ino, name, value, *time_ns),
        LogRecord::RemoveXattr { ino, name, time_ns } => {
            apply_remove_xattr(tx, meta, *ino, name, *time_ns)
        }
        LogRecord::SnapCreate {
            id,
            path,
            name,
            root_hash,
            created_unix_ms,
        } => {
            ns::ns_insert(
                tx,
                &meta.ns,
                meta.dirty_for_ns(),
                keys::subsystem(keys::Subsystem::Snapshot, id.as_bytes()),
                crate::store::snapshot_record(&crate::SnapshotRow {
                    id: id.clone(),
                    path: path.clone(),
                    name: name.clone(),
                    root_hash: root_hash.clone(),
                    created_unix_ms: *created_unix_ms,
                }),
            )?;
            Ok(Applied::Done)
        }
        LogRecord::SnapDelete { id, .. } => {
            ns::ns_remove(
                tx,
                &meta.ns,
                meta.dirty_for_ns(),
                keys::subsystem(keys::Subsystem::Snapshot, id.as_bytes()),
            )?;
            Ok(Applied::Done)
        }
        LogRecord::Clone { nodes, .. } => apply_clone(tx, meta, nodes, staged),
        LogRecord::SetQuota { max_logical_bytes } => {
            ns::ns_insert(
                tx,
                &meta.ns,
                meta.dirty_for_ns(),
                keys::subsystem(keys::Subsystem::Quota, b""),
                crate::store::quota_record(*max_logical_bytes),
            )?;
            Ok(Applied::Done)
        }
        LogRecord::Atime {
            ino,
            atime_ns,
            time_ns,
        } => {
            let skew = atime::atime_skew_tolerance_ns();
            atime::apply_atime_one(
                tx,
                &meta.ns,
                &meta.orphans,
                &meta.atime,
                *ino,
                *atime_ns,
                *time_ns,
                skew,
            )?;
            Ok(Applied::Done)
        }
        // Plan 30 §M2: node-local and unpublished, exactly like `Atime`
        // above — writes directly to `completed`, never through
        // `ns`/`dirty`. Idempotent: replaying the same segment twice (or
        // a rid whose row this replica already has, e.g. because it was
        // the holder that recorded it in `recent` before shipping) just
        // overwrites the same key with the same value.
        LogRecord::Completed { rid } => {
            // The position stored here is informational only (nothing
            // in the retry-resolution path needs anything but presence,
            // `Meta::completed_position` is a diagnostic), so a replica
            // applying a *tailed* segment (as opposed to the writer's
            // own `journal::append_tx`, which knows its real seq) just
            // records 0 rather than opening a second, MVCC-snapshotted
            // read transaction nested inside this write transaction for
            // a value nothing consults.
            let now_ms = constellation_fs_core::types::now_ns() / 1_000_000;
            tx.insert(
                &meta.completed,
                rid.to_key(),
                crate::store::Meta::encode_completed_row(0, now_ms),
            );
            Ok(Applied::Done)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn insert_node(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    parent: Ino,
    name: &str,
    ino: Ino,
    kind: Kind,
    mode: u32,
    uid: u32,
    gid: u32,
    rdev: u64,
    nlink: u32,
    size: u64,
    target: Option<String>,
    t: i64,
    staged: &crate::store::UsageTracker,
) -> Result<Applied, MetaError> {
    if ns::get_inode_record(tx, &meta.ns, parent)?.is_none() {
        return Ok(Applied::Skipped("parent does not exist"));
    }
    if let Some(existing) = ns::get_dentry_record(tx, &meta.ns, parent, name)? {
        if existing.ino == ino {
            return Ok(Applied::Done);
        }
        match evict_dentry(tx, meta, parent, name, existing.ino, t)? {
            Applied::Done => {}
            skip => return Ok(skip),
        }
    }
    let prior_file =
        ns::get_inode_record(tx, &meta.ns, ino)?.is_some_and(|r| r.attrs.kind == Kind::File);
    let attrs = Attrs {
        kind,
        mode: mode & 0o7777,
        uid,
        gid,
        nlink,
        size,
        mtime_ns: t,
        ctime_ns: t,
        rdev,
    };
    ns::put_inode(
        tx,
        &meta.ns,
        meta.dirty_for_ns(),
        &meta.blobs,
        ino,
        attrs,
        None,
        target.map(String::into_bytes),
        &[],
    )?;
    let is_new_dentry = ns::get_dentry_record(tx, &meta.ns, parent, name)?.is_none();
    ns::put_dentry(tx, &meta.ns, meta.dirty_for_ns(), parent, name, ino, attrs)?;
    atime::set_atime_tx(tx, &meta.atime, ino, t);
    if is_new_dentry && kind == Kind::Dir {
        misc::bump_nlink_tx(tx, &meta.ns, meta.dirty_for_ns(), parent, 1, t)?;
    }
    misc::touch_times_tx(tx, &meta.ns, meta.dirty_for_ns(), parent, t)?;
    if kind == Kind::File && !prior_file {
        // A fresh file always starts at 0 bytes (Create/Mkdir/Symlink/
        // Mknod never carry file content), but the file *count* still
        // moves, mirroring the local `create()` path's `adjust(0, 1)`.
        staged.adjust(0, 1);
    }
    Ok(Applied::Done)
}

/// Evict whatever currently sits at `(parent, name)` so a later insert
/// can claim the name. Mirrors the old engine's rules: a non-empty
/// directory is left alone (skip, don't cascade-delete a subtree), an
/// empty directory or a dropped-to-zero non-directory is removed
/// outright (an orphan reaped later, exactly like a local `unlink`).
fn evict_dentry(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    parent: Ino,
    name: &str,
    ino: Ino,
    t: i64,
) -> Result<Applied, MetaError> {
    let Some(rec) = ns::get_inode_record(tx, &meta.ns, ino)? else {
        ns::remove_dentry(tx, &meta.ns, meta.dirty_for_ns(), parent, name, ino)?;
        return Ok(Applied::Done);
    };
    if rec.attrs.kind == Kind::Dir {
        if ns::has_children(tx, &meta.ns, ino)? {
            return Ok(Applied::Skipped("name held by non-empty directory"));
        }
        ns::remove_dentry(tx, &meta.ns, meta.dirty_for_ns(), parent, name, ino)?;
        ns::ns_remove(tx, &meta.ns, meta.dirty_for_ns(), keys::inode(ino))?;
        ns::clear_spilled_xattrs(tx, &meta.ns, meta.dirty_for_ns(), ino)?;
        let names: Vec<Vec<u8>> = ns::all_xattrs(tx, &meta.ns, &meta.blobs, &rec, ino)?
            .into_iter()
            .map(|(n, _)| n.into_bytes())
            .collect();
        misc::xattr_by_name_del_all_tx(tx, &meta.xattr_by_name, ino, names);
        atime::remove_atime_tx(tx, &meta.atime, ino);
    } else {
        ns::remove_dentry(tx, &meta.ns, meta.dirty_for_ns(), parent, name, ino)?;
        if rec.attrs.nlink <= 1 {
            let manifest_bytes = rec
                .manifest
                .as_ref()
                .map(|p| ns::resolve_payload(tx, &meta.blobs, p))
                .transpose()?;
            ns::clear_spilled_xattrs(tx, &meta.ns, meta.dirty_for_ns(), ino)?;
            let names: Vec<Vec<u8>> = ns::all_xattrs(tx, &meta.ns, &meta.blobs, &rec, ino)?
                .into_iter()
                .map(|(n, _)| n.into_bytes())
                .collect();
            misc::xattr_by_name_del_all_tx(tx, &meta.xattr_by_name, ino, names);
            misc::track_manifest_transition_tx(
                tx,
                &meta.chunk_ref,
                &meta.chunk_ref_by_ino,
                ino,
                manifest_bytes.as_deref(),
                None,
            )?;
            let mut orphan_rec = rec.clone();
            orphan_rec.attrs.nlink = 0;
            orphan_rec.attrs.ctime_ns = t;
            orphan_rec.xattrs.clear();
            tx.insert(
                &meta.orphans,
                ino.to_be_bytes().to_vec(),
                orphan_rec.encode(),
            );
            ns::ns_remove(tx, &meta.ns, meta.dirty_for_ns(), keys::inode(ino))?;
        } else {
            misc::bump_nlink_tx(tx, &meta.ns, meta.dirty_for_ns(), ino, -1, t)?;
        }
    }
    Ok(Applied::Done)
}

fn apply_link(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    ino: Ino,
    parent: Ino,
    name: &str,
    t: i64,
) -> Result<Applied, MetaError> {
    if ns::get_inode_record(tx, &meta.ns, ino)?.is_none()
        || ns::get_inode_record(tx, &meta.ns, parent)?.is_none()
    {
        return Ok(Applied::Skipped("link endpoint missing"));
    }
    if let Some(existing) = ns::get_dentry_record(tx, &meta.ns, parent, name)? {
        if existing.ino == ino {
            return Ok(Applied::Done);
        }
        match evict_dentry(tx, meta, parent, name, existing.ino, t)? {
            Applied::Done => {}
            skip => return Ok(skip),
        }
    }
    let rec =
        misc::bump_nlink_tx(tx, &meta.ns, meta.dirty_for_ns(), ino, 1, t)?.expect("checked above");
    ns::put_dentry(
        tx,
        &meta.ns,
        meta.dirty_for_ns(),
        parent,
        name,
        ino,
        rec.attrs,
    )?;
    misc::touch_times_tx(tx, &meta.ns, meta.dirty_for_ns(), parent, t)?;
    Ok(Applied::Done)
}

fn apply_unlink(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    parent: Ino,
    name: &str,
    t: i64,
) -> Result<Applied, MetaError> {
    let Some(d) = ns::get_dentry_record(tx, &meta.ns, parent, name)? else {
        return Ok(Applied::Done);
    };
    let result = evict_dentry(tx, meta, parent, name, d.ino, t)?;
    misc::touch_times_tx(tx, &meta.ns, meta.dirty_for_ns(), parent, t)?;
    Ok(result)
}

fn apply_rmdir(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    parent: Ino,
    name: &str,
    t: i64,
) -> Result<Applied, MetaError> {
    let Some(d) = ns::get_dentry_record(tx, &meta.ns, parent, name)? else {
        return Ok(Applied::Done);
    };
    let Some(rec) = ns::get_inode_record(tx, &meta.ns, d.ino)? else {
        ns::remove_dentry(tx, &meta.ns, meta.dirty_for_ns(), parent, name, d.ino)?;
        return Ok(Applied::Done);
    };
    if rec.attrs.kind != Kind::Dir {
        return Ok(Applied::Skipped("rmdir target is not a directory"));
    }
    if ns::has_children(tx, &meta.ns, d.ino)? {
        return Ok(Applied::Skipped("directory not empty locally"));
    }
    ns::remove_dentry(tx, &meta.ns, meta.dirty_for_ns(), parent, name, d.ino)?;
    ns::ns_remove(tx, &meta.ns, meta.dirty_for_ns(), keys::inode(d.ino))?;
    ns::clear_spilled_xattrs(tx, &meta.ns, meta.dirty_for_ns(), d.ino)?;
    atime::remove_atime_tx(tx, &meta.atime, d.ino);
    misc::bump_nlink_tx(tx, &meta.ns, meta.dirty_for_ns(), parent, -1, t)?;
    Ok(Applied::Done)
}

fn apply_rename(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    parent: Ino,
    name: &str,
    new_parent: Ino,
    new_name: &str,
    t: i64,
) -> Result<Applied, MetaError> {
    let Some(src) = ns::get_dentry_record(tx, &meta.ns, parent, name)? else {
        return Ok(Applied::Done);
    };
    let ino = src.ino;
    if ns::get_inode_record(tx, &meta.ns, new_parent)?.is_none() {
        return Ok(Applied::Skipped("rename destination parent missing"));
    }
    let Some(src_rec) = ns::get_inode_record(tx, &meta.ns, ino)? else {
        return Ok(Applied::Done);
    };
    if let Some(existing) = ns::get_dentry_record(tx, &meta.ns, new_parent, new_name)? {
        if existing.ino == ino {
            return Ok(Applied::Done);
        }
        match evict_dentry(tx, meta, new_parent, new_name, existing.ino, t)? {
            Applied::Done => {}
            skip => return Ok(skip),
        }
    }
    ns::ns_remove(
        tx,
        &meta.ns,
        meta.dirty_for_ns(),
        keys::dentry(parent, name.as_bytes()),
    )?;
    ns::ns_remove(
        tx,
        &meta.ns,
        meta.dirty_for_ns(),
        keys::rdentry(ino, parent, name.as_bytes()),
    )?;
    ns::ns_insert(
        tx,
        &meta.ns,
        meta.dirty_for_ns(),
        keys::dentry(new_parent, new_name.as_bytes()),
        DentryRecord::new(ino, src.attrs).encode(),
    )?;
    ns::ns_insert(
        tx,
        &meta.ns,
        meta.dirty_for_ns(),
        keys::rdentry(ino, new_parent, new_name.as_bytes()),
        record::RDENTRY_VALUE.to_vec(),
    )?;
    if src_rec.attrs.kind == Kind::Dir && parent != new_parent {
        misc::bump_nlink_tx(tx, &meta.ns, meta.dirty_for_ns(), parent, -1, t)?;
        misc::bump_nlink_tx(tx, &meta.ns, meta.dirty_for_ns(), new_parent, 1, t)?;
    }
    misc::touch_times_tx(tx, &meta.ns, meta.dirty_for_ns(), parent, t)?;
    misc::touch_times_tx(tx, &meta.ns, meta.dirty_for_ns(), new_parent, t)?;
    Ok(Applied::Done)
}

#[allow(clippy::too_many_arguments)]
fn apply_setattr(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    ino: Ino,
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    size: Option<u64>,
    atime_ns: Option<i64>,
    mtime_ns: Option<i64>,
    t: i64,
    staged: &crate::store::UsageTracker,
) -> Result<Applied, MetaError> {
    let Some(mut rec) = ns::get_inode_record(tx, &meta.ns, ino)? else {
        return Ok(Applied::Done);
    };
    let old_size = rec.attrs.size;
    if let Some(m) = mode {
        rec.attrs.mode = m & 0o7777;
    }
    if let Some(u) = uid {
        rec.attrs.uid = u;
    }
    if let Some(g) = gid {
        rec.attrs.gid = g;
    }
    if let Some(s) = size {
        rec.attrs.size = s;
        rec.attrs.mtime_ns = t;
    }
    if let Some(m) = mtime_ns {
        rec.attrs.mtime_ns = m;
    }
    rec.attrs.ctime_ns = t;
    let attrs = rec.attrs;
    ns::ns_insert(
        tx,
        &meta.ns,
        meta.dirty_for_ns(),
        keys::inode(ino),
        rec.encode(),
    )?;
    for (parent, name) in ns::links_of(tx, &meta.ns, ino)? {
        ns::ns_insert(
            tx,
            &meta.ns,
            meta.dirty_for_ns(),
            keys::dentry(parent, name.as_bytes()),
            DentryRecord::new(ino, attrs).encode(),
        )?;
    }
    if let Some(a) = atime_ns {
        atime::set_atime_tx(tx, &meta.atime, ino, a);
    }
    if size.is_some() && attrs.kind == Kind::File {
        staged.adjust(attrs.size as i64 - old_size as i64, 0);
    }
    Ok(Applied::Done)
}

fn apply_write_manifest(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    ino: Ino,
    manifest: &[u8],
    size: u64,
    t: i64,
    staged: &crate::store::UsageTracker,
) -> Result<Applied, MetaError> {
    let Some(rec) = ns::get_inode_record(tx, &meta.ns, ino)? else {
        return Ok(Applied::Done);
    };
    let old_size = rec.attrs.size;
    let current = rec
        .manifest
        .as_ref()
        .map(|p| ns::resolve_payload(tx, &meta.blobs, p))
        .transpose()?;
    let mut attrs = rec.attrs;
    attrs.size = size;
    attrs.mtime_ns = t;
    attrs.ctime_ns = t;
    let xattrs = rec.xattrs.clone();
    ns::put_inode(
        tx,
        &meta.ns,
        meta.dirty_for_ns(),
        &meta.blobs,
        ino,
        attrs,
        Some(manifest.to_vec()),
        rec.symlink_target
            .as_ref()
            .map(|p| ns::resolve_payload(tx, &meta.blobs, p))
            .transpose()?,
        &xattrs,
    )?;
    for (parent, name) in ns::links_of(tx, &meta.ns, ino)? {
        ns::ns_insert(
            tx,
            &meta.ns,
            meta.dirty_for_ns(),
            keys::dentry(parent, name.as_bytes()),
            DentryRecord::new(ino, attrs).encode(),
        )?;
    }
    misc::track_manifest_transition_tx(
        tx,
        &meta.chunk_ref,
        &meta.chunk_ref_by_ino,
        ino,
        current.as_deref(),
        Some(manifest),
    )?;
    staged.adjust(size as i64 - old_size as i64, 0);
    Ok(Applied::Done)
}

fn apply_set_xattr(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    ino: Ino,
    name: &str,
    value: &[u8],
    t: i64,
) -> Result<Applied, MetaError> {
    let Some(rec) = ns::get_inode_record(tx, &meta.ns, ino)? else {
        return Ok(Applied::Done);
    };
    let mut xattrs = ns::all_xattrs(tx, &meta.ns, &meta.blobs, &rec, ino)?;
    if let Some(slot) = xattrs.iter_mut().find(|(n, _)| n == name) {
        slot.1 = value.to_vec();
    } else {
        xattrs.push((name.to_string(), value.to_vec()));
    }
    let mut attrs = rec.attrs;
    attrs.ctime_ns = t;
    let xattr_pairs: Vec<(Vec<u8>, Vec<u8>)> = xattrs
        .into_iter()
        .map(|(n, v)| (n.into_bytes(), v))
        .collect();
    let manifest = rec
        .manifest
        .as_ref()
        .map(|p| ns::resolve_payload(tx, &meta.blobs, p))
        .transpose()?;
    let target = rec
        .symlink_target
        .as_ref()
        .map(|p| ns::resolve_payload(tx, &meta.blobs, p))
        .transpose()?;
    ns::put_inode(
        tx,
        &meta.ns,
        meta.dirty_for_ns(),
        &meta.blobs,
        ino,
        attrs,
        manifest,
        target,
        &xattr_pairs,
    )?;
    misc::xattr_by_name_put_tx(tx, &meta.xattr_by_name, name, ino, value);
    Ok(Applied::Done)
}

fn apply_remove_xattr(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    ino: Ino,
    name: &str,
    t: i64,
) -> Result<Applied, MetaError> {
    let Some(rec) = ns::get_inode_record(tx, &meta.ns, ino)? else {
        return Ok(Applied::Done);
    };
    let mut xattrs = ns::all_xattrs(tx, &meta.ns, &meta.blobs, &rec, ino)?;
    xattrs.retain(|(n, _)| n != name);
    let mut attrs = rec.attrs;
    attrs.ctime_ns = t;
    let xattr_pairs: Vec<(Vec<u8>, Vec<u8>)> = xattrs
        .into_iter()
        .map(|(n, v)| (n.into_bytes(), v))
        .collect();
    let manifest = rec
        .manifest
        .as_ref()
        .map(|p| ns::resolve_payload(tx, &meta.blobs, p))
        .transpose()?;
    let target = rec
        .symlink_target
        .as_ref()
        .map(|p| ns::resolve_payload(tx, &meta.blobs, p))
        .transpose()?;
    ns::put_inode(
        tx,
        &meta.ns,
        meta.dirty_for_ns(),
        &meta.blobs,
        ino,
        attrs,
        manifest,
        target,
        &xattr_pairs,
    )?;
    misc::xattr_by_name_del_tx(tx, &meta.xattr_by_name, name, ino);
    Ok(Applied::Done)
}

fn apply_clone(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    nodes: &[CloneNode],
    staged: &crate::store::UsageTracker,
) -> Result<Applied, MetaError> {
    for node in nodes {
        let Some(kind) = InodeKind::from_u8(node.kind) else {
            return Err(MetaError::Invalid(format!(
                "unknown clone node kind {}",
                node.kind
            )));
        };
        if ns::get_inode_record(tx, &meta.ns, node.parent)?.is_none() {
            return Ok(Applied::Skipped("clone parent does not exist"));
        }
        let prior = ns::get_inode_record(tx, &meta.ns, node.ino)?;
        let (prior_file, prior_size) = prior.as_ref().map_or((false, 0u64), |r| {
            (r.attrs.kind == Kind::File, r.attrs.size)
        });
        let attrs = Attrs {
            kind: ns::kind_to_mtree(kind),
            mode: node.mode,
            uid: node.uid,
            gid: node.gid,
            nlink: if kind == InodeKind::Dir { 2 } else { 1 },
            size: node.size,
            mtime_ns: node.mtime_ns,
            ctime_ns: node.mtime_ns,
            rdev: node.rdev,
        };
        let xattrs: Vec<(Vec<u8>, Vec<u8>)> = node
            .xattrs
            .iter()
            .map(|(n, v)| (n.as_bytes().to_vec(), v.clone()))
            .collect();
        ns::put_inode(
            tx,
            &meta.ns,
            meta.dirty_for_ns(),
            &meta.blobs,
            node.ino,
            attrs,
            node.manifest.clone(),
            node.target.clone().map(String::into_bytes),
            &xattrs,
        )?;
        ns::put_dentry(
            tx,
            &meta.ns,
            meta.dirty_for_ns(),
            node.parent,
            &node.name,
            node.ino,
            attrs,
        )?;
        atime::set_atime_tx(tx, &meta.atime, node.ino, node.mtime_ns);
        misc::track_manifest_transition_tx(
            tx,
            &meta.chunk_ref,
            &meta.chunk_ref_by_ino,
            node.ino,
            None,
            node.manifest.as_deref(),
        )?;
        for (n, v) in &node.xattrs {
            misc::xattr_by_name_put_tx(tx, &meta.xattr_by_name, n, node.ino, v);
        }
        if kind == InodeKind::Dir {
            misc::bump_nlink_tx(
                tx,
                &meta.ns,
                meta.dirty_for_ns(),
                node.parent,
                1,
                node.mtime_ns,
            )?;
        }
        let is_file = kind == InodeKind::File;
        match (prior_file, is_file) {
            (false, true) => staged.adjust(node.size as i64, 1),
            (true, false) => staged.adjust(-(prior_size as i64), -1),
            (true, true) => staged.adjust(node.size as i64 - prior_size as i64, 0),
            (false, false) => {}
        }
    }
    Ok(Applied::Done)
}
