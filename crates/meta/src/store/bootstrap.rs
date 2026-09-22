//! The plan 28 S6 bootstrap-from-commit loader (`load_tree_rows`,
//! `load_tree_subsystems`, `finish_tree_load`) and reintegration's
//! atomic namespace swap (`commit_reintegration_batch`).

use crate::error::MetaError;
use crate::record::LogRecord;
use crate::store::snapshot::{quota_record, snapshot_record};
use crate::store::{journal, misc, ns, Meta, KV_APPLIED_SEQ};
use crate::{SnapshotRow, TreeInode};
use constellation_fs_core::Ino;
use constellation_mtree::keys::{self, Subsystem};
use constellation_mtree::record::InodeRecord;
use fjall::{Readable, SingleWriterTxKeyspace, SingleWriterWriteTx};

/// Wholesale clear of a keyspace, inside the caller's write transaction.
fn clear_tx(tx: &mut SingleWriterWriteTx, ks: &SingleWriterTxKeyspace) -> Result<(), MetaError> {
    let keys: Vec<Vec<u8>> = tx
        .iter(ks)
        .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
        .collect::<Result<_, _>>()?;
    for k in keys {
        tx.remove(ks, k);
    }
    Ok(())
}

/// Rebuild `chunk_ref`/`chunk_ref_by_ino`/`xattr_by_name` from whatever
/// is currently in `ns` — the fjall equivalent of the old engine's
/// `DELETE FROM chunk_ref; rebuild_chunk_ref(&tx)`, extended to the
/// `xattr_by_name` index this engine also maintains.
fn rebuild_indexes_tx(
    tx: &mut SingleWriterWriteTx,
    ns_ks: &SingleWriterTxKeyspace,
    blobs: &SingleWriterTxKeyspace,
    chunk_ref: &SingleWriterTxKeyspace,
    chunk_ref_by_ino: &SingleWriterTxKeyspace,
    xattr_by_name: &SingleWriterTxKeyspace,
) -> Result<(), MetaError> {
    clear_tx(tx, chunk_ref)?;
    clear_tx(tx, chunk_ref_by_ino)?;
    clear_tx(tx, xattr_by_name)?;
    let range = keys::whole_range(keys::RANGE_INODE);
    let rows: Vec<(Ino, InodeRecord)> = tx
        .range(ns_ks, ns::key_range_bounds(&range))
        .map(|g| {
            g.into_inner().map_err(MetaError::from).and_then(|(k, v)| {
                let ino = match keys::Key::parse(&k)? {
                    keys::Key::Inode { ino } => ino,
                    _ => return Err(MetaError::Invalid("expected an inode key".into())),
                };
                Ok((ino, InodeRecord::decode(&v)?))
            })
        })
        .collect::<Result<_, MetaError>>()?;
    for (ino, rec) in &rows {
        let manifest = rec
            .manifest
            .as_ref()
            .map(|p| ns::resolve_payload(tx, blobs, p))
            .transpose()?;
        misc::track_manifest_transition_tx(
            tx,
            chunk_ref,
            chunk_ref_by_ino,
            *ino,
            None,
            manifest.as_deref(),
        )?;
        for (name, value) in ns::all_xattrs(tx, ns_ks, blobs, rec, *ino)? {
            misc::xattr_by_name_put_tx(tx, xattr_by_name, &name, *ino, &value);
        }
    }
    Ok(())
}

impl Meta {
    /// Bulk upsert of one page of a tree walk's inode+xattr+dentry rows.
    /// Never journals: the rows *are* the shared state at the commit's
    /// vector. `0x01` records are visited before any `0x02` in key
    /// order, so a dentry's owning inode is always already committed by
    /// the time this is called for it (paged across multiple calls or
    /// not).
    pub fn load_tree_rows(
        &self,
        inodes: &[TreeInode],
        dentries: &[(Ino, String, Ino)],
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        for ti in inodes {
            let attrs = ns::fileattr_to_attrs(&ti.attr);
            let xattrs: Vec<(Vec<u8>, Vec<u8>)> = ti
                .xattrs
                .iter()
                .map(|(n, v)| (n.as_bytes().to_vec(), v.clone()))
                .collect();
            ns::put_inode(
                &mut tx,
                &self.ns,
                &self.blobs,
                ti.attr.ino,
                attrs,
                ti.manifest.clone(),
                ti.target.clone().map(String::into_bytes),
                &xattrs,
            )?;
            crate::store::atime::set_atime_tx(&mut tx, &self.atime, ti.attr.ino, ti.attr.atime_ns);
        }
        for (parent, name, ino) in dentries {
            if let Some(rec) = ns::get_inode_record(&tx, &self.ns, *ino)? {
                ns::put_dentry(&mut tx, &self.ns, *parent, name, *ino, rec.attrs);
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn load_tree_subsystems(
        &self,
        snapshots: &[SnapshotRow],
        quota: Option<Option<u64>>,
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let range = keys::records_of(Subsystem::Snapshot);
        let existing: Vec<Vec<u8>> = tx
            .range(&self.ns, ns::key_range_bounds(&range))
            .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;
        for k in existing {
            tx.remove(&self.ns, k);
        }
        for row in snapshots {
            tx.insert(
                &self.ns,
                keys::subsystem(Subsystem::Snapshot, row.id.as_bytes()),
                snapshot_record(row),
            );
        }
        if let Some(q) = quota {
            tx.insert(
                &self.ns,
                keys::subsystem(Subsystem::Quota, b""),
                quota_record(q),
            );
        }
        tx.commit()?;
        Ok(())
    }

    pub fn finish_tree_load(&self) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        rebuild_indexes_tx(
            &mut tx,
            &self.ns,
            &self.blobs,
            &self.chunk_ref,
            &self.chunk_ref_by_ino,
            &self.xattr_by_name,
        )?;
        tx.commit()?;
        Ok(())
    }

    /// The crash-safety boundary for reintegration: after commit the
    /// namespace and disposition ledger cannot disagree.
    ///
    /// Re-expressed from the old engine's `ATTACH DATABASE` + `INSERT
    /// ... SELECT` (fjall has no cross-database transaction — the
    /// reconciled state instead arrives as an already-open side `Meta`,
    /// typically bootstrapped into its own directory/tempdir from the
    /// shared log). One `write_tx` on `self`: wipe and bulk-copy `ns`
    /// and `orphans` from a snapshot of `side`, rebuild the derived
    /// `chunk_ref`/`xattr_by_name` indexes from the new `ns` (the old
    /// engine's "namespace replaced wholesale, so every reverse-index
    /// row is stale" reasoning still holds), adopt only `side`'s
    /// `applied_seq`, then mark every reconciled journal row's
    /// disposition and delete it, and journal the reconciliation's own
    /// `output` records — all inside the one transaction, so the
    /// namespace/disposition/journal surgery is one atomic unit exactly
    /// as it was under SQLite.
    pub fn commit_reintegration_batch(
        &self,
        side: &Meta,
        dispositions: &[(u64, String, String)],
        output: &[LogRecord],
    ) -> Result<(), MetaError> {
        let side_snap = side.db.read_tx();
        let mut tx = self.db.write_tx();
        clear_tx(&mut tx, &self.ns)?;
        clear_tx(&mut tx, &self.orphans)?;
        clear_tx(&mut tx, &self.atime)?;
        for guard in side_snap.iter(&side.ns) {
            let (k, v) = guard.into_inner()?;
            tx.insert(&self.ns, k.to_vec(), v.to_vec());
        }
        for guard in side_snap.iter(&side.orphans) {
            let (k, v) = guard.into_inner()?;
            tx.insert(&self.orphans, k.to_vec(), v.to_vec());
        }
        for guard in side_snap.iter(&side.atime) {
            let (k, v) = guard.into_inner()?;
            tx.insert(&self.atime, k.to_vec(), v.to_vec());
        }
        // `side`'s `ns` may reference `Payload::Spilled` bodies that only
        // exist in `side.blobs`; without this, resolving them against
        // `self.blobs` after the swap would silently come back empty.
        // Content-addressed, so a plain union copy is safe — any body
        // this replica no longer references becomes an ordinary orphan
        // (no local blob GC yet, same as everywhere else in M1).
        for guard in side_snap.iter(&side.blobs) {
            let (k, v) = guard.into_inner()?;
            tx.insert(&self.blobs, k.to_vec(), v.to_vec());
        }
        rebuild_indexes_tx(
            &mut tx,
            &self.ns,
            &self.blobs,
            &self.chunk_ref,
            &self.chunk_ref_by_ino,
            &self.xattr_by_name,
        )?;
        if let Some(v) = side_snap.get(&side.local, KV_APPLIED_SEQ.as_bytes())? {
            tx.insert(&self.local, KV_APPLIED_SEQ.as_bytes().to_vec(), v.to_vec());
        }
        for (seq, disposition, detail) in dispositions {
            journal::mark_disposition_tx(&mut tx, &self.reintegration, *seq, disposition, detail)?;
            tx.remove(&self.journal_ks, seq.to_be_bytes().to_vec());
        }
        for record in output {
            journal::append_tx(&mut tx, &self.journal_ks, &self.local, record)?;
        }
        tx.commit()?;
        Ok(())
    }
}
