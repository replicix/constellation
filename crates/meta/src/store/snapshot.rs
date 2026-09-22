//! Snapshot rows and the replicated quota, stored as `0x30` subsystem
//! records directly in `ns` — so the local replica's bytes are *exactly*
//! the tree's bytes for these ranges (no local-vs-published translation,
//! unlike the old SQLite `snapshot` table / `quota_max_bytes` kv row).
//! The codecs are the ones `cli/src/mtree_read.rs` used to own; they
//! move here so both sides share one implementation.
//!
//! Clone (`eager_clone`) also lives here: it materializes a whole
//! subtree directly (fresh inos, ordinary `0x01`/`0x02`/`0x03`/`0x04`
//! writes) and journals one `LogRecord::Clone` covering the batch.

use crate::error::MetaError;
use crate::record::{CloneNode, LogRecord};
use crate::store::{alloc_ino_tx, journal, misc, ns, Meta};
use crate::{CloneSpec, SnapshotRow};
use constellation_fs_core::{Ino, InodeKind};
use constellation_mtree::keys::{self, Subsystem};
use constellation_mtree::record;
use fjall::Readable;

/// `record::encode_fields([path, name, root_hash, created_unix_ms])`.
pub fn snapshot_record(row: &SnapshotRow) -> Vec<u8> {
    record::encode_fields(&[
        row.path.as_bytes(),
        row.name.as_bytes(),
        row.root_hash.as_bytes(),
        &row.created_unix_ms.to_le_bytes(),
    ])
}

pub fn parse_snapshot_record(id: &str, value: &[u8]) -> Result<SnapshotRow, MetaError> {
    let fields = record::decode_fields(value)?;
    if fields.len() != 4 {
        return Err(MetaError::Invalid(format!(
            "snapshot record has {} fields",
            fields.len()
        )));
    }
    let text = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
    let created_unix_ms = i64::from_le_bytes(
        fields[3]
            .try_into()
            .map_err(|_| MetaError::Invalid("snapshot created_unix_ms".into()))?,
    );
    Ok(SnapshotRow {
        id: id.to_string(),
        path: text(fields[0]),
        name: text(fields[1]),
        root_hash: text(fields[2]),
        created_unix_ms,
    })
}

/// `None` = no quota record at all (creation-time cap applies);
/// `Some(None)` = an explicit, empty record (unlimited); `Some(Some(n))`
/// = a cap of `n` bytes.
pub fn quota_record(max_bytes: Option<u64>) -> Vec<u8> {
    match max_bytes {
        Some(n) => record::encode_fields(&[&n.to_le_bytes()]),
        None => record::encode_fields(&[]),
    }
}

pub fn parse_quota_record(value: &[u8]) -> Result<Option<u64>, MetaError> {
    let fields = record::decode_fields(value)?;
    match fields.first() {
        None => Ok(None),
        Some(f) => Ok(Some(u64::from_le_bytes(
            (*f).try_into()
                .map_err(|_| MetaError::Invalid("quota record".into()))?,
        ))),
    }
}

impl Meta {
    // -------------------------------------------------------- snapshots

    pub fn record_snapshot(&self, row: &SnapshotRow) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        ns::ns_insert(
            &mut tx,
            &self.ns,
            self.dirty_for_ns(),
            keys::subsystem(Subsystem::Snapshot, row.id.as_bytes()),
            snapshot_record(row),
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &LogRecord::SnapCreate {
                id: row.id.clone(),
                path: row.path.clone(),
                name: row.name.clone(),
                root_hash: row.root_hash.clone(),
                created_unix_ms: row.created_unix_ms,
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn delete_snapshot(&self, path: &str, name: &str) -> Result<bool, MetaError> {
        let mut tx = self.db.write_tx();
        let range = keys::records_of(Subsystem::Snapshot);
        let mut found: Option<String> = None;
        for guard in tx.range(&self.ns, ns::key_range_bounds(&range)) {
            let (k, v) = guard.into_inner()?;
            let id = match constellation_mtree::keys::Key::parse(&k)? {
                constellation_mtree::keys::Key::Subsystem { id, .. } => {
                    String::from_utf8_lossy(id).into_owned()
                }
                _ => continue,
            };
            let row = parse_snapshot_record(&id, &v)?;
            if row.path == path && row.name == name {
                found = Some(id);
                break;
            }
        }
        let Some(id) = found else { return Ok(false) };
        ns::ns_remove(
            &mut tx,
            &self.ns,
            self.dirty_for_ns(),
            keys::subsystem(Subsystem::Snapshot, id.as_bytes()),
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &LogRecord::SnapDelete {
                id,
                path: path.to_string(),
                name: name.to_string(),
            },
        )?;
        tx.commit()?;
        Ok(true)
    }

    pub fn snapshots(&self, path: Option<&str>) -> Result<Vec<SnapshotRow>, MetaError> {
        let r = self.db.read_tx();
        self.snapshots_at(&r, path)
    }

    pub fn snapshots_at(
        &self,
        r: &impl Readable,
        path: Option<&str>,
    ) -> Result<Vec<SnapshotRow>, MetaError> {
        let range = keys::records_of(Subsystem::Snapshot);
        let mut out = Vec::new();
        for guard in r.range(&self.ns, ns::key_range_bounds(&range)) {
            let (k, v) = guard.into_inner()?;
            let id = match constellation_mtree::keys::Key::parse(&k)? {
                constellation_mtree::keys::Key::Subsystem { id, .. } => {
                    String::from_utf8_lossy(id).into_owned()
                }
                _ => continue,
            };
            let row = parse_snapshot_record(&id, &v)?;
            if path.is_none_or(|p| row.path == p) {
                out.push(row);
            }
        }
        out.sort_by(|a, b| {
            (a.path.as_str(), a.name.as_str()).cmp(&(b.path.as_str(), b.name.as_str()))
        });
        Ok(out)
    }

    // ------------------------------------------------------------ quota

    pub fn read_quota(&self) -> Result<Option<u64>, MetaError> {
        let r = self.db.read_tx();
        if let Some(v) = r.get(&self.ns, keys::subsystem(Subsystem::Quota, b""))? {
            return parse_quota_record(&v);
        }
        match crate::store::kv_get_tx(&r, &self.local, crate::store::QUOTA_CREATION_KV_KEY)? {
            None => Ok(None),
            Some(s) if s.is_empty() => Ok(None),
            Some(s) => s
                .parse()
                .map(Some)
                .map_err(|_| MetaError::Invalid("quota_creation_bytes".into())),
        }
    }

    /// The raw `0x30` Quota subsystem record bytes, straight from `ns` —
    /// `ns` already holds this in exactly the shape the published tree
    /// wants, so the publisher can copy it verbatim rather than
    /// round-tripping it through the decimal-string `kv` mirror.
    pub fn replicated_quota_record_at(
        &self,
        r: &impl Readable,
    ) -> Result<Option<Vec<u8>>, MetaError> {
        Ok(r.get(&self.ns, keys::subsystem(Subsystem::Quota, b""))?
            .map(|v| v.to_vec()))
    }

    pub fn write_quota(&self, max_logical_bytes: Option<u64>) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        ns::ns_insert(
            &mut tx,
            &self.ns,
            self.dirty_for_ns(),
            keys::subsystem(Subsystem::Quota, b""),
            quota_record(max_logical_bytes),
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &LogRecord::SetQuota { max_logical_bytes },
        )?;
        tx.commit()?;
        Ok(())
    }

    // ------------------------------------------------------------ clone

    pub fn eager_clone(
        &self,
        source_path: &str,
        snapshot: &str,
        root_hash: &str,
        destination: &str,
        specs: &[CloneSpec],
    ) -> Result<(), MetaError> {
        if specs.first().is_none_or(|s| s.parent_index.is_some()) {
            return Err(MetaError::Invalid("clone tree has no root".into()));
        }
        for (i, spec) in specs.iter().enumerate() {
            if let Some(pi) = spec.parent_index {
                if pi >= i {
                    return Err(MetaError::Invalid(
                        "clone nodes are not parent-first".into(),
                    ));
                }
            }
        }
        let mut components: Vec<&str> = destination.split('/').filter(|s| !s.is_empty()).collect();
        let final_name = components
            .pop()
            .ok_or_else(|| MetaError::Invalid("empty clone destination".into()))?;

        let mut tx = self.db.write_tx();
        let mut parent = constellation_fs_core::types::ROOT_INO;
        for comp in &components {
            parent = ns::child_ino(&tx, &self.ns, parent, comp)?.ok_or(MetaError::NoEntry)?;
        }
        if ns::child_ino(&tx, &self.ns, parent, final_name)?.is_some() {
            return Err(MetaError::Exists);
        }

        let mut inos: Vec<Ino> = Vec::with_capacity(specs.len());
        let mut nodes: Vec<CloneNode> = Vec::with_capacity(specs.len());
        let mut delta_bytes: i64 = 0;
        let mut delta_files: i64 = 0;
        for spec in specs.iter() {
            let (node_parent, name): (Ino, &str) = match spec.parent_index {
                None => (parent, final_name),
                Some(pi) => (inos[pi], spec.name.as_str()),
            };
            let ino = alloc_ino_tx(&mut tx, &self.local, &self.ino_alloc, node_parent)?;
            inos.push(ino);
            let attrs = record::Attrs {
                kind: ns::kind_to_mtree(spec.kind),
                mode: spec.mode,
                uid: spec.uid,
                gid: spec.gid,
                nlink: if spec.kind == InodeKind::Dir { 2 } else { 1 },
                size: spec.size,
                mtime_ns: spec.mtime_ns,
                ctime_ns: spec.mtime_ns,
                rdev: 0,
            };
            let xattrs: Vec<(Vec<u8>, Vec<u8>)> = spec
                .xattrs
                .iter()
                .map(|(n, v)| (n.as_bytes().to_vec(), v.clone()))
                .collect();
            ns::put_inode(
                &mut tx,
                &self.ns,
                self.dirty_for_ns(),
                &self.blobs,
                ino,
                attrs,
                spec.manifest.clone(),
                spec.target.clone().map(String::into_bytes),
                &xattrs,
            )?;
            ns::put_dentry(
                &mut tx,
                &self.ns,
                self.dirty_for_ns(),
                node_parent,
                name,
                ino,
                attrs,
            )?;
            crate::store::atime::set_atime_tx(&mut tx, &self.atime, ino, spec.mtime_ns);
            misc::track_manifest_transition_tx(
                &mut tx,
                &self.chunk_ref,
                &self.chunk_ref_by_ino,
                ino,
                None,
                spec.manifest.as_deref(),
            )?;
            for (n, v) in &spec.xattrs {
                misc::xattr_by_name_put_tx(&mut tx, &self.xattr_by_name, n, ino, v);
            }
            if spec.kind == InodeKind::Dir {
                misc::bump_nlink_tx(
                    &mut tx,
                    &self.ns,
                    self.dirty_for_ns(),
                    node_parent,
                    1,
                    spec.mtime_ns,
                )?;
            }
            if spec.kind == InodeKind::File {
                delta_bytes += spec.size as i64;
                delta_files += 1;
            }
            nodes.push(CloneNode {
                parent: node_parent,
                name: name.to_string(),
                ino,
                kind: spec.kind.as_u8(),
                mode: spec.mode,
                uid: spec.uid,
                gid: spec.gid,
                size: spec.size,
                mtime_ns: spec.mtime_ns,
                rdev: 0,
                target: spec.target.clone(),
                manifest: spec.manifest.clone(),
                xattrs: spec.xattrs.clone(),
            });
        }
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &LogRecord::Clone {
                source_path: source_path.to_string(),
                snapshot: snapshot.to_string(),
                root_hash: root_hash.to_string(),
                nodes,
            },
        )?;
        crate::store::adjust_usage_tx(&mut tx, &self.local, delta_bytes, delta_files)?;
        tx.commit()?;
        self.usage_tracker().adjust(delta_bytes, delta_files);
        Ok(())
    }
}
