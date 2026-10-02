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

/// `record::encode_fields([path, name, root_hash, created_unix_ms,
/// origin, policy_ino, held, creator, held_by, refer_bytes])`.
///
/// Plan 32 §0.4: the first four fields are the pre-plan-32 record and
/// keep their meaning; everything after them is optional on decode, so a
/// row written by an older build still parses. Encoding always writes the
/// whole tuple — these bytes are the published tree's bytes, so the
/// mapping row → bytes must be a pure function of the row, not of what
/// happens to be non-default.
///
/// `held_by` and `refer_bytes` encode their absence as an empty field
/// (`held_by: Some("")` is *no owner*, the same as `None`).
pub fn snapshot_record(row: &SnapshotRow) -> Vec<u8> {
    let owner = row.owner().unwrap_or("");
    let refer = row.refer_bytes.map(u64::to_le_bytes);
    record::encode_fields(&[
        row.path.as_bytes(),
        row.name.as_bytes(),
        row.root_hash.as_bytes(),
        &row.created_unix_ms.to_le_bytes(),
        &[row.origin],
        &row.policy_ino.to_le_bytes(),
        &[u8::from(row.held)],
        &row.creator.to_le_bytes(),
        owner.as_bytes(),
        refer.as_ref().map(|b| &b[..]).unwrap_or(&[]),
    ])
}

pub fn parse_snapshot_record(id: &str, value: &[u8]) -> Result<SnapshotRow, MetaError> {
    let fields = record::decode_fields(value)?;
    if fields.len() < 4 {
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
    let byte = |i: usize| match fields.get(i) {
        None | Some([]) => Ok(0u8),
        Some([b]) => Ok(*b),
        Some(_) => Err(MetaError::Invalid(format!("snapshot record field {i}"))),
    };
    let word = |i: usize| match fields.get(i) {
        None | Some([]) => Ok(0u64),
        Some(f) => f
            .as_ref()
            .try_into()
            .map(u64::from_le_bytes)
            .map_err(|_| MetaError::Invalid(format!("snapshot record field {i}"))),
    };
    Ok(SnapshotRow {
        id: id.to_string(),
        path: text(fields[0]),
        name: text(fields[1]),
        root_hash: text(fields[2]),
        created_unix_ms,
        origin: byte(4)?,
        policy_ino: word(5)?,
        held: byte(6)? != 0,
        creator: word(7)?,
        held_by: match fields.get(8) {
            None | Some([]) => None,
            Some(f) => Some(text(f)),
        },
        refer_bytes: match fields.get(9) {
            None | Some([]) => None,
            Some(_) => Some(word(9)?),
        },
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
        // An unheld snapshot has no owner ([`Meta::set_snapshot_hold`]).
        // Enforced here rather than trusted, because the hold rides in its
        // own record: a row that claimed an owner without a hold would
        // replay (from `SnapHold { held: false }`) as *unowned*, and the
        // writer and its replicas would disagree on the row's bytes.
        let owned = SnapshotRow {
            held_by: row.held.then(|| row.owner().map(str::to_string)).flatten(),
            ..row.clone()
        };
        let row = &owned;
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        ns::ns_insert(
            &mut tx,
            &self.ns,
            dirty,
            keys::subsystem(Subsystem::Snapshot, row.id.as_bytes()),
            snapshot_record(row),
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::SnapCreate2 {
                id: row.id.clone(),
                path: row.path.clone(),
                name: row.name.clone(),
                root_hash: row.root_hash.clone(),
                created_unix_ms: row.created_unix_ms,
                origin: row.origin,
                policy_ino: row.policy_ino,
                creator: row.creator,
                refer_bytes: row.refer_bytes,
            },
        )?;
        // A snapshot born held (plan 37's `snapshot.create{hold}`) gets
        // its hold in the same transaction, so a replica never sees the
        // snapshot exist unheld.
        if row.held {
            journal::append_tx(
                &mut tx,
                &self.journal_ks,
                &self.local,
                &self.completed,
                &LogRecord::SnapHold {
                    id: row.id.clone(),
                    held: row.held,
                    by: row.owner().map(str::to_string),
                },
            )?;
        }
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        self.note_snapshot_change();
        Ok(())
    }

    /// One snapshot row by its id (`blake3(path@name)`), without the scan
    /// [`Meta::snapshots`] does.
    pub fn snapshot_by_id(&self, id: &str) -> Result<Option<SnapshotRow>, MetaError> {
        let r = self.db.read_tx();
        match r.get(
            &self.ns,
            keys::subsystem(Subsystem::Snapshot, id.as_bytes()),
        )? {
            None => Ok(None),
            Some(v) => parse_snapshot_record(id, &v).map(Some),
        }
    }

    /// Plan 32 §0.4: set or release the retention hold on `id`, recording
    /// `by` as the owner (`None` = a plain hold with no owner). Returns
    /// the row as it now stands, or `None` when there is no such snapshot.
    ///
    /// Ownership is by metadata (plan 32 L6): a hold recorded under an
    /// owner may only be released by that same owner, and may not be
    /// silently taken over by another. `force` overrides both — the
    /// control layer is what restricts *who* may ask for it (admin).
    /// The comparison lives here, inside the write transaction that
    /// applies the result, because anywhere else it is a TOCTOU: two
    /// concurrent holds on the same unheld snapshot would both pass a
    /// check made against an earlier read transaction, and the loser
    /// would be told it owns a hold it does not.
    pub fn set_snapshot_hold(
        &self,
        id: &str,
        held: bool,
        by: Option<&str>,
        force: bool,
    ) -> Result<Option<SnapshotRow>, MetaError> {
        let by = by.filter(|by| !by.is_empty());
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let key = keys::subsystem(Subsystem::Snapshot, id.as_bytes());
        let Some(value) = tx.get(&self.ns, &key)? else {
            return Ok(None);
        };
        let mut row = parse_snapshot_record(id, &value)?;
        if !force && row.held && row.owner() != by {
            return Err(MetaError::Invalid(format!(
                "snapshot {}@{} is held by {}, not by {}; pass --force (admin) to override",
                row.path,
                row.name,
                row.owner().unwrap_or("nobody in particular"),
                by.unwrap_or("no owner")
            )));
        }
        row.held = held;
        // An unheld snapshot has no hold, so it has no owner: releasing
        // forgets who held it. (The audit log is where "who released it"
        // lives; the row is state, not history.)
        row.held_by = match held {
            true => by.map(str::to_string),
            false => None,
        };
        ns::ns_insert(&mut tx, &self.ns, dirty, key, snapshot_record(&row))?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::SnapHold {
                id: id.to_string(),
                held,
                by: row.held_by.clone(),
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(Some(row))
    }

    /// Remove `path@name`'s row. The row key is computed from the id
    /// (plan 32 §0.3), so this is a point lookup, not a scan.
    pub fn delete_snapshot(&self, path: &str, name: &str) -> Result<bool, MetaError> {
        self.delete_snapshot_by_id(&keys::snapshot_id(path, name))
    }

    /// Remove snapshot `id`'s row; `false` when there is none. The
    /// `SnapDelete` record carries the path and name the row recorded.
    pub fn delete_snapshot_by_id(&self, id: &str) -> Result<bool, MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let key = keys::subsystem(Subsystem::Snapshot, id.as_bytes());
        let Some(value) = tx.get(&self.ns, &key)? else {
            return Ok(false);
        };
        let row = parse_snapshot_record(id, &value)?;
        ns::ns_remove(&mut tx, &self.ns, dirty, key)?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::SnapDelete {
                id: id.to_string(),
                path: row.path,
                name: row.name,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        self.note_snapshot_change();
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
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        ns::ns_insert(
            &mut tx,
            &self.ns,
            dirty,
            keys::subsystem(Subsystem::Quota, b""),
            quota_record(max_logical_bytes),
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::SetQuota { max_logical_bytes },
        )?;
        self.finish_local(&mut tx, local)?;
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
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
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
                rdev: constellation_types::Rdev::default(),
            };
            let xattrs: Vec<(Vec<u8>, Vec<u8>)> = spec
                .xattrs
                .iter()
                .map(|(n, v)| (n.as_bytes().to_vec(), v.clone()))
                .collect();
            ns::put_inode(
                &mut tx,
                &self.ns,
                dirty,
                &self.blobs,
                ino,
                attrs,
                spec.manifest.clone(),
                spec.target.clone().map(String::into_bytes),
                &xattrs,
            )?;
            ns::put_dentry(&mut tx, &self.ns, dirty, node_parent, name, ino, attrs)?;
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
                misc::bump_nlink_tx(&mut tx, &self.ns, dirty, node_parent, 1, spec.mtime_ns)?;
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
                rdev: constellation_types::Rdev::default(),
                target: spec.target.clone(),
                manifest: spec.manifest.clone(),
                xattrs: spec.xattrs.clone(),
            });
        }
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Clone {
                source_path: source_path.to_string(),
                snapshot: snapshot.to_string(),
                root_hash: root_hash.to_string(),
                nodes,
            },
        )?;
        crate::store::adjust_usage_tx(&mut tx, &self.local, delta_bytes, delta_files)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        self.usage_tracker().adjust(delta_bytes, delta_files);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_mtree::record;

    fn row() -> SnapshotRow {
        SnapshotRow {
            id: "id".into(),
            path: "/data".into(),
            name: "daily".into(),
            root_hash: "mtree:3:ab:9".into(),
            created_unix_ms: 1_700_000_000_123,
            origin: 1,
            policy_ino: 4096,
            held: true,
            creator: 7,
            held_by: Some("csi:1f2e".into()),
            refer_bytes: Some(1 << 40),
        }
    }

    #[test]
    fn a_full_row_round_trips() {
        let row = row();
        let encoded = snapshot_record(&row);
        assert_eq!(parse_snapshot_record("id", &encoded).unwrap(), row);
    }

    /// Plan 32 §0.4: a row written before the extension has exactly four
    /// fields, and must still parse — at the documented defaults.
    #[test]
    fn a_pre_plan_32_four_field_row_parses_with_defaults() {
        let old = record::encode_fields(&[
            b"/data",
            b"daily",
            b"mtree:3:ab:9",
            &1_700_000_000_123i64.to_le_bytes(),
        ]);
        let parsed = parse_snapshot_record("id", &old).unwrap();
        assert_eq!(
            parsed,
            SnapshotRow::new("id", "/data", "daily", "mtree:3:ab:9", 1_700_000_000_123)
        );
        assert_eq!(parsed.origin, 0);
        assert_eq!(parsed.policy_ino, 0);
        assert!(!parsed.held);
        assert_eq!(parsed.creator, 0);
        assert_eq!(parsed.owner(), None);
        assert_eq!(parsed.refer_bytes, None);
        // Re-encoding it is lossless in meaning, and a row that says
        // nothing is a row that says the defaults: parsing our own
        // encoding of the parsed value gives the same thing back.
        assert_eq!(
            parse_snapshot_record("id", &snapshot_record(&parsed)).unwrap(),
            parsed
        );
    }

    /// Anything between 4 and the full tuple parses too: a reader of a row
    /// written by a build that had only some of the trailing fields.
    #[test]
    fn a_partially_extended_row_parses() {
        let partial = record::encode_fields(&[
            b"/data",
            b"daily",
            b"h",
            &7i64.to_le_bytes(),
            &[1],
            &99u64.to_le_bytes(),
        ]);
        let parsed = parse_snapshot_record("id", &partial).unwrap();
        assert_eq!(parsed.origin, 1);
        assert_eq!(parsed.policy_ino, 99);
        assert!(!parsed.held);
        assert_eq!(parsed.refer_bytes, None);
    }

    /// Absence is spelled with an empty field, so `None` and `Some("")`
    /// (no owner) and a missing `refer_bytes` all survive a round trip as
    /// themselves.
    #[test]
    fn an_empty_owner_is_no_owner() {
        let mut row = row();
        row.held_by = Some(String::new());
        row.refer_bytes = None;
        let parsed = parse_snapshot_record("id", &snapshot_record(&row)).unwrap();
        assert_eq!(parsed.held_by, None);
        assert_eq!(parsed.refer_bytes, None);
        assert!(parsed.held, "the hold survives losing its owner");
        // `refer_bytes: Some(0)` is a real answer ("this subtree is
        // empty"), not an absence.
        row.refer_bytes = Some(0);
        assert_eq!(
            parse_snapshot_record("id", &snapshot_record(&row))
                .unwrap()
                .refer_bytes,
            Some(0)
        );
    }

    #[test]
    fn a_row_with_fewer_than_four_fields_is_refused() {
        let short = record::encode_fields(&[b"/data", b"daily", b"h"]);
        assert!(parse_snapshot_record("id", &short).is_err());
    }
}
