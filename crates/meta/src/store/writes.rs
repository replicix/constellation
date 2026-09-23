//! `impl MetaStore for Meta`, the `*_at` (pre-allocated-ino) variants
//! used by replay/reintegration, and `publish_file` (scratch → shared
//! publish).

use crate::error::MetaError;
use crate::record::LogRecord;
use crate::store::{alloc_ino_tx, atime, journal, misc, ns, Meta};
use crate::{DirEntry, MetaStore, SetXattrMode};
use constellation_fs_core::types::{now_ns, ROOT_INO};
use constellation_fs_core::{FileAttr, Ino, InodeKind};
use constellation_mtree::keys;
use constellation_mtree::record::{self, Attrs, DentryRecord, InodeRecord, Kind};
use fjall::{Readable, SingleWriterTxKeyspace, SingleWriterWriteTx};

fn mask_mode(mode: u32) -> u32 {
    mode & 0o7777
}

/// The shared body of `mkdir`/`create`/`symlink`/`mknod` (and their
/// `_at` twins): existence checks, the `0x01`/`0x02`/`0x04` writes, the
/// `atime` seed, and the parent-directory touch (nlink+1 for a new
/// subdirectory, mtime/ctime only otherwise).
#[allow(clippy::too_many_arguments)]
fn insert_new_node(
    tx: &mut SingleWriterWriteTx,
    ns_ks: &SingleWriterTxKeyspace,
    dirty: ns::Dirty,
    atime_ks: &SingleWriterTxKeyspace,
    blobs: &SingleWriterTxKeyspace,
    parent: Ino,
    name: &str,
    ino: Ino,
    attrs: Attrs,
    manifest: Option<Vec<u8>>,
    symlink_target: Option<Vec<u8>>,
) -> Result<(), MetaError> {
    ns::require_dir(tx, ns_ks, parent)?;
    if ns::child_ino(tx, ns_ks, parent, name)?.is_some() {
        return Err(MetaError::Exists);
    }
    if ns::get_inode_record(tx, ns_ks, ino)?.is_some() {
        return Err(MetaError::Exists);
    }
    ns::put_inode(
        tx,
        ns_ks,
        dirty,
        blobs,
        ino,
        attrs,
        manifest,
        symlink_target,
        &[],
    )?;
    ns::put_dentry(tx, ns_ks, dirty, parent, name, ino, attrs)?;
    atime::set_atime_tx(tx, atime_ks, ino, attrs.mtime_ns);
    if attrs.kind == Kind::Dir {
        misc::bump_nlink_tx(tx, ns_ks, dirty, parent, 1, attrs.mtime_ns)?;
    } else {
        misc::touch_times_tx(tx, ns_ks, dirty, parent, attrs.mtime_ns)?;
    }
    Ok(())
}

impl Meta {
    pub fn max_journal_seq(&self) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        crate::store::journal::max_seq(&r, &self.journal_ks)
    }

    pub fn peek_journal_after(&self, after_seq: u64) -> Result<Vec<(u64, LogRecord)>, MetaError> {
        let r = self.db.read_tx();
        crate::store::journal::peek_after(&r, &self.journal_ks, after_seq)
    }

    /// The journal rows `journal_seqs` shipped in the segment at
    /// `applied_seq`: delete them, advance `applied_seq`, and retire the
    /// transactions they complete (plan 30 §M3b: their `journal_tx` rows,
    /// and their `Local` speculation — see `spec::retire_local_tx`).
    pub fn ack_journal_rows_at(
        &self,
        journal_seqs: &[u64],
        applied_seq: u64,
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        if let Some(&upto) = journal_seqs.iter().max() {
            crate::store::spec::retire_local_tx(&mut tx, self, upto, applied_seq)?;
        }
        crate::store::journal::ack_rows_at(
            &mut tx,
            &self.journal_ks,
            &self.local,
            journal_seqs,
            applied_seq,
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Plan 29 M0a removed namespace partitions, so there is exactly one
    /// implicit partition (`"p0"`) and this degenerates to `[("p0",
    /// batch)]` (or `[]` if the journal is empty) — kept for shipper
    /// call-site compatibility.
    ///
    /// Plan 30 §M3b: the batch always ends on a transaction boundary (it
    /// may run past `max_per_part` to reach one), so a caller shipping it
    /// whole never splits an op's records from its `Completed { rid }`.
    pub fn take_journal_grouped(
        &self,
        max_per_part: usize,
    ) -> Result<Vec<(String, crate::store::JournalBatch)>, MetaError> {
        let batch = self.take_journal_whole_txs(max_per_part)?;
        if batch.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(vec![("p0".to_string(), batch)])
        }
    }

    // --------------------------------------------------- *_at (fixed ino)

    pub fn mkdir_at(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let t = now_ns();
        let attrs = Attrs {
            kind: Kind::Dir,
            mode: mask_mode(mode),
            uid,
            gid,
            nlink: 2,
            size: 0,
            mtime_ns: t,
            ctime_ns: t,
            rdev: 0,
        };
        insert_new_node(
            &mut tx,
            &self.ns,
            dirty,
            &self.atime,
            &self.blobs,
            parent,
            name,
            ino,
            attrs,
            None,
            None,
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Mkdir {
                parent,
                name: name.to_string(),
                ino,
                mode: attrs.mode,
                uid,
                gid,
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(ns::attrs_to_fileattr(ino, &attrs, t))
    }

    pub fn create_at(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let t = now_ns();
        let attrs = Attrs {
            kind: Kind::File,
            mode: mask_mode(mode),
            uid,
            gid,
            nlink: 1,
            size: 0,
            mtime_ns: t,
            ctime_ns: t,
            rdev: 0,
        };
        insert_new_node(
            &mut tx,
            &self.ns,
            dirty,
            &self.atime,
            &self.blobs,
            parent,
            name,
            ino,
            attrs,
            None,
            None,
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Create {
                parent,
                name: name.to_string(),
                ino,
                mode: attrs.mode,
                uid,
                gid,
                time_ns: t,
            },
        )?;
        crate::store::adjust_usage_tx(&mut tx, &self.local, 0, 1)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        self.usage_tracker().adjust(0, 1);
        Ok(ns::attrs_to_fileattr(ino, &attrs, t))
    }

    pub fn symlink_at(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
        target: &str,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let t = now_ns();
        let attrs = Attrs {
            kind: Kind::Symlink,
            mode: 0o777,
            uid,
            gid,
            nlink: 1,
            size: target.len() as u64,
            mtime_ns: t,
            ctime_ns: t,
            rdev: 0,
        };
        insert_new_node(
            &mut tx,
            &self.ns,
            dirty,
            &self.atime,
            &self.blobs,
            parent,
            name,
            ino,
            attrs,
            None,
            Some(target.as_bytes().to_vec()),
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Symlink {
                parent,
                name: name.to_string(),
                ino,
                target: target.to_string(),
                uid,
                gid,
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(ns::attrs_to_fileattr(ino, &attrs, t))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn mknod_at(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
        kind: InodeKind,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: u64,
    ) -> Result<FileAttr, MetaError> {
        if !kind.is_special() {
            return Err(MetaError::Invalid("mknod kind".into()));
        }
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let t = now_ns();
        let attrs = Attrs {
            kind: ns::kind_to_mtree(kind),
            mode: mask_mode(mode),
            uid,
            gid,
            nlink: 1,
            size: 0,
            mtime_ns: t,
            ctime_ns: t,
            rdev,
        };
        insert_new_node(
            &mut tx,
            &self.ns,
            dirty,
            &self.atime,
            &self.blobs,
            parent,
            name,
            ino,
            attrs,
            None,
            None,
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Mknod {
                parent,
                name: name.to_string(),
                ino,
                kind: kind.as_u8(),
                mode: attrs.mode,
                uid,
                gid,
                rdev,
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(ns::attrs_to_fileattr(ino, &attrs, t))
    }

    // -------------------------------------------------------- manifest

    #[allow(clippy::too_many_arguments)]
    fn set_manifest_tx(
        tx: &mut SingleWriterWriteTx,
        ns_ks: &SingleWriterTxKeyspace,
        dirty: ns::Dirty,
        blobs: &SingleWriterTxKeyspace,
        chunk_ref: &SingleWriterTxKeyspace,
        chunk_ref_by_ino: &SingleWriterTxKeyspace,
        journal_ks: &SingleWriterTxKeyspace,
        local: &SingleWriterTxKeyspace,
        completed: &SingleWriterTxKeyspace,
        ino: Ino,
        expected_base: Option<&[u8]>,
        manifest: &[u8],
        size: u64,
    ) -> Result<i64, MetaError> {
        let Some(rec) = ns::get_inode_record(tx, ns_ks, ino)? else {
            return Err(MetaError::NoEnt(ino));
        };
        let current = rec
            .manifest
            .as_ref()
            .map(|p| ns::resolve_payload(tx, blobs, p))
            .transpose()?;
        if let (Some(base), Some(cur)) = (expected_base, current.as_deref()) {
            if cur != base && cur != manifest {
                return Err(MetaError::Conflict);
            }
        }
        let journal_base = expected_base
            .map(<[u8]>::to_vec)
            .or_else(|| current.clone());
        let t = now_ns();
        let old_size = rec.attrs.size as i64;
        let mut attrs = rec.attrs;
        attrs.size = size;
        attrs.mtime_ns = t;
        attrs.ctime_ns = t;
        let xattrs: Vec<(Vec<u8>, Vec<u8>)> = rec.xattrs.clone();
        ns::put_inode(
            tx,
            ns_ks,
            dirty,
            blobs,
            ino,
            attrs,
            Some(manifest.to_vec()),
            rec.symlink_target
                .as_ref()
                .map(|p| ns::resolve_payload(tx, blobs, p))
                .transpose()?,
            &xattrs,
        )?;
        misc::track_manifest_transition_tx(
            tx,
            chunk_ref,
            chunk_ref_by_ino,
            ino,
            current.as_deref(),
            Some(manifest),
        )?;
        journal::append_tx(
            tx,
            journal_ks,
            local,
            completed,
            &LogRecord::WriteManifest {
                ino,
                base_manifest: journal_base,
                manifest: manifest.to_vec(),
                size,
                time_ns: t,
            },
        )?;
        Ok(size as i64 - old_size)
    }

    pub fn set_manifest_dirty(
        &self,
        ino: Ino,
        base_manifest: Option<&[u8]>,
        manifest: &[u8],
        size: u64,
        dirty_hashes: &[constellation_fs_core::ChunkHash],
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let delta = Self::set_manifest_tx(
            &mut tx,
            &self.ns,
            dirty,
            &self.blobs,
            &self.chunk_ref,
            &self.chunk_ref_by_ino,
            &self.journal_ks,
            &self.local,
            &self.completed,
            ino,
            base_manifest,
            manifest,
            size,
        )?;
        for hash in dirty_hashes {
            let mut k = hash.0.to_vec();
            k.extend_from_slice(&ino.to_be_bytes());
            tx.insert(&self.pending_upload, k, Vec::new());
        }
        crate::store::adjust_usage_tx(&mut tx, &self.local, delta, 0)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        self.usage_tracker().adjust(delta, 0);
        Ok(())
    }

    pub fn set_manifest_with_base(
        &self,
        ino: Ino,
        base_manifest: Option<&[u8]>,
        manifest: &[u8],
        size: u64,
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let delta = Self::set_manifest_tx(
            &mut tx,
            &self.ns,
            dirty,
            &self.blobs,
            &self.chunk_ref,
            &self.chunk_ref_by_ino,
            &self.journal_ks,
            &self.local,
            &self.completed,
            ino,
            base_manifest,
            manifest,
            size,
        )?;
        crate::store::adjust_usage_tx(&mut tx, &self.local, delta, 0)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        self.usage_tracker().adjust(delta, 0);
        Ok(())
    }

    /// Atomic scratch → shared publish: creates `parent/name`, commits
    /// its manifest and xattrs, in one transaction.
    #[allow(clippy::too_many_arguments)]
    pub fn publish_file(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
        mtime_ns: i64,
        manifest: &[u8],
        size: u64,
        xattrs: &[(String, Vec<u8>)],
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        ns::require_dir(&tx, &self.ns, parent)?;
        let mut delta_bytes: i64 = 0;
        let mut delta_files: i64 = 0;
        if let Some(old) = ns::get_dentry_record(&tx, &self.ns, parent, name)? {
            let Some(old_rec) = ns::get_inode_record(&tx, &self.ns, old.ino)? else {
                return Err(MetaError::NoEnt(old.ino));
            };
            if old_rec.attrs.kind == Kind::Dir {
                return Err(MetaError::IsDir);
            }
            let old_manifest = old_rec
                .manifest
                .as_ref()
                .map(|p| ns::resolve_payload(&tx, &self.blobs, p))
                .transpose()?;
            if old_manifest.as_deref() == Some(manifest) {
                return Ok(());
            }
            let t = now_ns();
            ns::remove_dentry(&mut tx, &self.ns, dirty, parent, name, old.ino)?;
            if old_rec.attrs.nlink <= 1 {
                ns::clear_spilled_xattrs(&mut tx, &self.ns, dirty, old.ino)?;
                let names: Vec<Vec<u8>> =
                    ns::all_xattrs(&tx, &self.ns, &self.blobs, &old_rec, old.ino)?
                        .into_iter()
                        .map(|(n, _)| n.into_bytes())
                        .collect();
                misc::xattr_by_name_del_all_tx(&mut tx, &self.xattr_by_name, old.ino, names);
                misc::track_manifest_transition_tx(
                    &mut tx,
                    &self.chunk_ref,
                    &self.chunk_ref_by_ino,
                    old.ino,
                    old_manifest.as_deref(),
                    None,
                )?;
                let mut orphan_rec = old_rec.clone();
                orphan_rec.attrs.nlink = 0;
                orphan_rec.attrs.ctime_ns = t;
                orphan_rec.xattrs.clear();
                tx.insert(
                    &self.orphans,
                    old.ino.to_be_bytes().to_vec(),
                    orphan_rec.encode(),
                );
                ns::ns_remove(&mut tx, &self.ns, dirty, keys::inode(old.ino))?;
                if old_rec.attrs.kind == Kind::File {
                    delta_bytes -= old_rec.attrs.size as i64;
                    delta_files -= 1;
                }
            } else {
                misc::bump_nlink_tx(&mut tx, &self.ns, dirty, old.ino, -1, t)?;
            }
            journal::append_tx(
                &mut tx,
                &self.journal_ks,
                &self.local,
                &self.completed,
                &LogRecord::Unlink {
                    parent,
                    name: name.to_string(),
                    time_ns: t,
                },
            )?;
        }
        if ns::get_inode_record(&tx, &self.ns, ino)?.is_some() {
            return Err(MetaError::Exists);
        }
        let ctime_ns = now_ns();
        let attrs = Attrs {
            kind: Kind::File,
            mode: mask_mode(mode),
            uid,
            gid,
            nlink: 1,
            size,
            mtime_ns,
            ctime_ns,
            rdev: 0,
        };
        ns::put_dentry(&mut tx, &self.ns, dirty, parent, name, ino, attrs)?;
        misc::touch_times_tx(&mut tx, &self.ns, dirty, parent, ctime_ns)?;
        atime::set_atime_tx(&mut tx, &self.atime, ino, mtime_ns);
        let xattr_pairs: Vec<(Vec<u8>, Vec<u8>)> = xattrs
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
            Some(manifest.to_vec()),
            None,
            &xattr_pairs,
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Create {
                parent,
                name: name.to_string(),
                ino,
                mode: attrs.mode,
                uid,
                gid,
                time_ns: mtime_ns,
            },
        )?;
        misc::track_manifest_transition_tx(
            &mut tx,
            &self.chunk_ref,
            &self.chunk_ref_by_ino,
            ino,
            None,
            Some(manifest),
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::WriteManifest {
                ino,
                base_manifest: None,
                manifest: manifest.to_vec(),
                size,
                time_ns: mtime_ns,
            },
        )?;
        for (name, value) in xattrs {
            misc::xattr_by_name_put_tx(&mut tx, &self.xattr_by_name, name, ino, value);
            journal::append_tx(
                &mut tx,
                &self.journal_ks,
                &self.local,
                &self.completed,
                &LogRecord::SetXattr {
                    ino,
                    name: name.clone(),
                    value: value.clone(),
                    time_ns: mtime_ns,
                },
            )?;
        }
        delta_bytes += size as i64;
        delta_files += 1;
        crate::store::adjust_usage_tx(&mut tx, &self.local, delta_bytes, delta_files)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        self.usage_tracker().adjust(delta_bytes, delta_files);
        Ok(())
    }
}

// ------------------------------------------------------------- rename

#[allow(clippy::too_many_arguments)]
fn rename_in_tx(
    tx: &mut SingleWriterWriteTx,
    ns_ks: &SingleWriterTxKeyspace,
    dirty: ns::Dirty,
    atime_ks: &SingleWriterTxKeyspace,
    orphans_ks: &SingleWriterTxKeyspace,
    xattr_by_name_ks: &SingleWriterTxKeyspace,
    chunk_ref: &SingleWriterTxKeyspace,
    chunk_ref_by_ino: &SingleWriterTxKeyspace,
    blobs: &SingleWriterTxKeyspace,
    parent: Ino,
    name: &str,
    new_parent: Ino,
    new_name: &str,
) -> Result<Option<(Ino, i64, i64, i64)>, MetaError> {
    let Some(src) = ns::get_dentry_record(tx, ns_ks, parent, name)? else {
        return Err(MetaError::NoEntry);
    };
    let ino = src.ino;
    let Some(src_rec) = ns::get_inode_record(tx, ns_ks, ino)? else {
        return Err(MetaError::NoEnt(ino));
    };
    ns::require_dir(tx, ns_ks, new_parent)?;

    if src_rec.attrs.kind == Kind::Dir {
        let mut cursor = new_parent;
        loop {
            if cursor == ino {
                return Err(MetaError::Invalid("rename into own subtree".into()));
            }
            if cursor == ROOT_INO {
                break;
            }
            match ns::parent_of(tx, ns_ks, cursor)? {
                Some(p) => cursor = p,
                None => break,
            }
        }
    }

    let t = now_ns();
    let mut delta = (0i64, 0i64);
    if let Some(existing) = ns::get_dentry_record(tx, ns_ks, new_parent, new_name)? {
        if existing.ino == ino {
            return Ok(None);
        }
        let Some(existing_rec) = ns::get_inode_record(tx, ns_ks, existing.ino)? else {
            return Err(MetaError::NoEnt(existing.ino));
        };
        if src_rec.attrs.kind == Kind::Dir && existing_rec.attrs.kind != Kind::Dir {
            return Err(MetaError::NotDir);
        }
        if src_rec.attrs.kind != Kind::Dir && existing_rec.attrs.kind == Kind::Dir {
            return Err(MetaError::IsDir);
        }
        if existing_rec.attrs.kind == Kind::Dir {
            if ns::has_children(tx, ns_ks, existing.ino)? {
                return Err(MetaError::NotEmpty);
            }
            ns::ns_remove(tx, ns_ks, dirty, keys::inode(existing.ino))?;
            ns::clear_spilled_xattrs(tx, ns_ks, dirty, existing.ino)?;
            let names: Vec<Vec<u8>> =
                ns::all_xattrs(tx, ns_ks, blobs, &existing_rec, existing.ino)?
                    .into_iter()
                    .map(|(n, _)| n.into_bytes())
                    .collect();
            misc::xattr_by_name_del_all_tx(tx, xattr_by_name_ks, existing.ino, names);
            atime::remove_atime_tx(tx, atime_ks, existing.ino);
            misc::bump_nlink_tx(tx, ns_ks, dirty, new_parent, -1, t)?;
        } else if existing_rec.attrs.nlink <= 1 {
            ns::clear_spilled_xattrs(tx, ns_ks, dirty, existing.ino)?;
            let names: Vec<Vec<u8>> =
                ns::all_xattrs(tx, ns_ks, blobs, &existing_rec, existing.ino)?
                    .into_iter()
                    .map(|(n, _)| n.into_bytes())
                    .collect();
            misc::xattr_by_name_del_all_tx(tx, xattr_by_name_ks, existing.ino, names);
            let manifest_bytes = existing_rec
                .manifest
                .as_ref()
                .map(|p| ns::resolve_payload(tx, blobs, p))
                .transpose()?;
            misc::track_manifest_transition_tx(
                tx,
                chunk_ref,
                chunk_ref_by_ino,
                existing.ino,
                manifest_bytes.as_deref(),
                None,
            )?;
            let mut orphan_rec = existing_rec.clone();
            orphan_rec.attrs.nlink = 0;
            orphan_rec.attrs.ctime_ns = t;
            orphan_rec.xattrs.clear();
            tx.insert(
                orphans_ks,
                existing.ino.to_be_bytes().to_vec(),
                orphan_rec.encode(),
            );
            ns::ns_remove(tx, ns_ks, dirty, keys::inode(existing.ino))?;
            if existing_rec.attrs.kind == Kind::File {
                delta.0 -= existing_rec.attrs.size as i64;
                delta.1 -= 1;
            }
        } else {
            misc::bump_nlink_tx(tx, ns_ks, dirty, existing.ino, -1, t)?;
        }
        ns::remove_dentry(tx, ns_ks, dirty, new_parent, new_name, existing.ino)?;
    }

    ns::ns_remove(tx, ns_ks, dirty, keys::dentry(parent, name.as_bytes()))?;
    ns::ns_remove(
        tx,
        ns_ks,
        dirty,
        keys::rdentry(ino, parent, name.as_bytes()),
    )?;
    ns::ns_insert(
        tx,
        ns_ks,
        dirty,
        keys::dentry(new_parent, new_name.as_bytes()),
        DentryRecord::new(ino, src.attrs).encode(),
    )?;
    ns::ns_insert(
        tx,
        ns_ks,
        dirty,
        keys::rdentry(ino, new_parent, new_name.as_bytes()),
        record::RDENTRY_VALUE.to_vec(),
    )?;

    if src_rec.attrs.kind == Kind::Dir && parent != new_parent {
        misc::bump_nlink_tx(tx, ns_ks, dirty, parent, -1, t)?;
        misc::bump_nlink_tx(tx, ns_ks, dirty, new_parent, 1, t)?;
    }
    misc::touch_times_tx(tx, ns_ks, dirty, parent, t)?;
    misc::touch_times_tx(tx, ns_ks, dirty, new_parent, t)?;
    Ok(Some((ino, t, delta.0, delta.1)))
}

// ----------------------------------------------------------- MetaStore

impl MetaStore for Meta {
    fn lookup(&self, parent: Ino, name: &str) -> Result<Option<FileAttr>, MetaError> {
        let r = self.db.read_tx();
        let Some(d) = ns::get_dentry_record(&r, &self.ns, parent, name)? else {
            return Ok(None);
        };
        let at = atime::get_atime(&r, &self.atime, d.ino)?;
        Ok(Some(ns::attrs_to_fileattr(d.ino, &d.attrs, at)))
    }

    fn getattr(&self, ino: Ino) -> Result<Option<FileAttr>, MetaError> {
        let r = self.db.read_tx();
        let Some(rec) = ns::get_inode_any(&r, &self.ns, &self.orphans, ino)? else {
            return Ok(None);
        };
        let at = atime::get_atime(&r, &self.atime, ino)?;
        Ok(Some(ns::attrs_to_fileattr(ino, &rec.attrs, at)))
    }

    fn readdir(&self, parent: Ino) -> Result<Vec<DirEntry>, MetaError> {
        let r = self.db.read_tx();
        ns::require_dir(&r, &self.ns, parent)?;
        ns::readdir_entries(&r, &self.ns, parent)
    }

    fn readlink(&self, ino: Ino) -> Result<Option<String>, MetaError> {
        let r = self.db.read_tx();
        let Some(rec) = ns::get_inode_any(&r, &self.ns, &self.orphans, ino)? else {
            return Ok(None);
        };
        match &rec.symlink_target {
            Some(p) => Ok(Some(
                String::from_utf8_lossy(&ns::resolve_payload(&r, &self.blobs, p)?).into_owned(),
            )),
            None => Ok(None),
        }
    }

    fn manifest(&self, ino: Ino) -> Result<Option<Vec<u8>>, MetaError> {
        let r = self.db.read_tx();
        let Some(rec) = ns::get_inode_any(&r, &self.ns, &self.orphans, ino)? else {
            return Ok(None);
        };
        match &rec.manifest {
            Some(p) => Ok(Some(ns::resolve_payload(&r, &self.blobs, p)?)),
            None => Ok(None),
        }
    }

    fn get_xattr(&self, ino: Ino, name: &str) -> Result<Option<Vec<u8>>, MetaError> {
        let r = self.db.read_tx();
        let Some(rec) = ns::get_inode_any(&r, &self.ns, &self.orphans, ino)? else {
            return Err(MetaError::NoEnt(ino));
        };
        if !rec.xattrs.is_empty() {
            return Ok(rec
                .xattrs
                .iter()
                .find(|(n, _)| n == name.as_bytes())
                .map(|(_, v)| v.clone()));
        }
        match r.get(&self.ns, keys::xattr(ino, name.as_bytes()))? {
            Some(v) => Ok(Some(ns::resolve_payload(
                &r,
                &self.blobs,
                &record::Payload::decode(&v)?,
            )?)),
            None => Ok(None),
        }
    }

    fn list_xattrs(&self, ino: Ino) -> Result<Vec<String>, MetaError> {
        let r = self.db.read_tx();
        let Some(rec) = ns::get_inode_any(&r, &self.ns, &self.orphans, ino)? else {
            return Err(MetaError::NoEnt(ino));
        };
        Ok(ns::all_xattrs(&r, &self.ns, &self.blobs, &rec, ino)?
            .into_iter()
            .map(|(n, _)| n)
            .collect())
    }

    fn recursive_size(&self, ino: Ino) -> Result<(u64, u64), MetaError> {
        Meta::recursive_size(self, ino)
    }

    fn usage(&self) -> (u64, u64) {
        self.usage_bytes_files()
    }

    fn quota(&self) -> Result<Option<u64>, MetaError> {
        self.read_quota()
    }

    fn set_quota(&self, max_logical_bytes: Option<u64>) -> Result<(), MetaError> {
        self.write_quota(max_logical_bytes)
    }

    fn mkdir(
        &self,
        parent: Ino,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let ino = alloc_ino_tx(&mut tx, &self.local, &self.ino_alloc, parent)?;
        let t = now_ns();
        let attrs = Attrs {
            kind: Kind::Dir,
            mode: mask_mode(mode),
            uid,
            gid,
            nlink: 2,
            size: 0,
            mtime_ns: t,
            ctime_ns: t,
            rdev: 0,
        };
        insert_new_node(
            &mut tx,
            &self.ns,
            dirty,
            &self.atime,
            &self.blobs,
            parent,
            name,
            ino,
            attrs,
            None,
            None,
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Mkdir {
                parent,
                name: name.to_string(),
                ino,
                mode: attrs.mode,
                uid,
                gid,
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(ns::attrs_to_fileattr(ino, &attrs, t))
    }

    fn create(
        &self,
        parent: Ino,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let ino = alloc_ino_tx(&mut tx, &self.local, &self.ino_alloc, parent)?;
        let t = now_ns();
        let attrs = Attrs {
            kind: Kind::File,
            mode: mask_mode(mode),
            uid,
            gid,
            nlink: 1,
            size: 0,
            mtime_ns: t,
            ctime_ns: t,
            rdev: 0,
        };
        insert_new_node(
            &mut tx,
            &self.ns,
            dirty,
            &self.atime,
            &self.blobs,
            parent,
            name,
            ino,
            attrs,
            None,
            None,
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Create {
                parent,
                name: name.to_string(),
                ino,
                mode: attrs.mode,
                uid,
                gid,
                time_ns: t,
            },
        )?;
        crate::store::adjust_usage_tx(&mut tx, &self.local, 0, 1)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        self.usage_tracker().adjust(0, 1);
        Ok(ns::attrs_to_fileattr(ino, &attrs, t))
    }

    fn symlink(
        &self,
        parent: Ino,
        name: &str,
        target: &str,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let ino = alloc_ino_tx(&mut tx, &self.local, &self.ino_alloc, parent)?;
        let t = now_ns();
        let attrs = Attrs {
            kind: Kind::Symlink,
            mode: 0o777,
            uid,
            gid,
            nlink: 1,
            size: target.len() as u64,
            mtime_ns: t,
            ctime_ns: t,
            rdev: 0,
        };
        insert_new_node(
            &mut tx,
            &self.ns,
            dirty,
            &self.atime,
            &self.blobs,
            parent,
            name,
            ino,
            attrs,
            None,
            Some(target.as_bytes().to_vec()),
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Symlink {
                parent,
                name: name.to_string(),
                ino,
                target: target.to_string(),
                uid,
                gid,
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(ns::attrs_to_fileattr(ino, &attrs, t))
    }

    fn mknod(
        &self,
        parent: Ino,
        name: &str,
        kind: InodeKind,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: u64,
    ) -> Result<FileAttr, MetaError> {
        if !kind.is_special() {
            return Err(MetaError::Invalid("mknod kind".into()));
        }
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let ino = alloc_ino_tx(&mut tx, &self.local, &self.ino_alloc, parent)?;
        let t = now_ns();
        let attrs = Attrs {
            kind: ns::kind_to_mtree(kind),
            mode: mask_mode(mode),
            uid,
            gid,
            nlink: 1,
            size: 0,
            mtime_ns: t,
            ctime_ns: t,
            rdev,
        };
        insert_new_node(
            &mut tx,
            &self.ns,
            dirty,
            &self.atime,
            &self.blobs,
            parent,
            name,
            ino,
            attrs,
            None,
            None,
        )?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Mknod {
                parent,
                name: name.to_string(),
                ino,
                kind: kind.as_u8(),
                mode: attrs.mode,
                uid,
                gid,
                rdev,
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(ns::attrs_to_fileattr(ino, &attrs, t))
    }

    fn link(&self, ino: Ino, parent: Ino, name: &str) -> Result<FileAttr, MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        ns::require_dir(&tx, &self.ns, parent)?;
        let Some(rec) = ns::get_inode_record(&tx, &self.ns, ino)? else {
            return Err(MetaError::NoEnt(ino));
        };
        if rec.attrs.kind == Kind::Dir {
            return Err(MetaError::IsDir);
        }
        let t = now_ns();
        let at = atime::get_atime(&tx, &self.atime, ino)?;
        let rec2 =
            misc::bump_nlink_tx(&mut tx, &self.ns, dirty, ino, 1, t)?.expect("checked above");
        ns::put_dentry(&mut tx, &self.ns, dirty, parent, name, ino, rec2.attrs)?;
        misc::touch_times_tx(&mut tx, &self.ns, dirty, parent, t)?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Link {
                ino,
                parent,
                name: name.to_string(),
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(ns::attrs_to_fileattr(ino, &rec2.attrs, at))
    }

    fn unlink(&self, parent: Ino, name: &str) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let Some(d) = ns::get_dentry_record(&tx, &self.ns, parent, name)? else {
            return Err(MetaError::NoEntry);
        };
        let ino = d.ino;
        let Some(rec) = ns::get_inode_record(&tx, &self.ns, ino)? else {
            ns::remove_dentry(&mut tx, &self.ns, dirty, parent, name, ino)?;
            self.finish_local(&mut tx, local)?;
            tx.commit()?;
            return Ok(());
        };
        if rec.attrs.kind == Kind::Dir {
            return Err(MetaError::IsDir);
        }
        let t = now_ns();
        ns::remove_dentry(&mut tx, &self.ns, dirty, parent, name, ino)?;
        let mut usage_delta = (0i64, 0i64);
        if rec.attrs.nlink <= 1 {
            let manifest_bytes = rec
                .manifest
                .as_ref()
                .map(|p| ns::resolve_payload(&tx, &self.blobs, p))
                .transpose()?;
            let names: Vec<Vec<u8>> = ns::all_xattrs(&tx, &self.ns, &self.blobs, &rec, ino)?
                .into_iter()
                .map(|(n, _)| n.into_bytes())
                .collect();
            ns::clear_spilled_xattrs(&mut tx, &self.ns, dirty, ino)?;
            misc::xattr_by_name_del_all_tx(&mut tx, &self.xattr_by_name, ino, names);
            misc::track_manifest_transition_tx(
                &mut tx,
                &self.chunk_ref,
                &self.chunk_ref_by_ino,
                ino,
                manifest_bytes.as_deref(),
                None,
            )?;
            let mut orphan_rec = rec.clone();
            orphan_rec.attrs.nlink = 0;
            orphan_rec.attrs.ctime_ns = t;
            orphan_rec.xattrs.clear();
            tx.insert(
                &self.orphans,
                ino.to_be_bytes().to_vec(),
                orphan_rec.encode(),
            );
            ns::ns_remove(&mut tx, &self.ns, dirty, keys::inode(ino))?;
            if rec.attrs.kind == Kind::File {
                usage_delta = (-(rec.attrs.size as i64), -1);
            }
        } else {
            misc::bump_nlink_tx(&mut tx, &self.ns, dirty, ino, -1, t)?;
        }
        misc::touch_times_tx(&mut tx, &self.ns, dirty, parent, t)?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Unlink {
                parent,
                name: name.to_string(),
                time_ns: t,
            },
        )?;
        crate::store::adjust_usage_tx(&mut tx, &self.local, usage_delta.0, usage_delta.1)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        self.usage_tracker().adjust(usage_delta.0, usage_delta.1);
        Ok(())
    }

    fn rmdir(&self, parent: Ino, name: &str) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let Some(d) = ns::get_dentry_record(&tx, &self.ns, parent, name)? else {
            return Err(MetaError::NoEntry);
        };
        let ino = d.ino;
        let Some(rec) = ns::get_inode_record(&tx, &self.ns, ino)? else {
            return Err(MetaError::NoEnt(ino));
        };
        if rec.attrs.kind != Kind::Dir {
            return Err(MetaError::NotDir);
        }
        if ns::has_children(&tx, &self.ns, ino)? {
            return Err(MetaError::NotEmpty);
        }
        let t = now_ns();
        ns::remove_dentry(&mut tx, &self.ns, dirty, parent, name, ino)?;
        ns::ns_remove(&mut tx, &self.ns, dirty, keys::inode(ino))?;
        let names: Vec<Vec<u8>> = ns::all_xattrs(&tx, &self.ns, &self.blobs, &rec, ino)?
            .into_iter()
            .map(|(n, _)| n.into_bytes())
            .collect();
        ns::clear_spilled_xattrs(&mut tx, &self.ns, dirty, ino)?;
        misc::xattr_by_name_del_all_tx(&mut tx, &self.xattr_by_name, ino, names);
        atime::remove_atime_tx(&mut tx, &self.atime, ino);
        misc::bump_nlink_tx(&mut tx, &self.ns, dirty, parent, -1, t)?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Rmdir {
                parent,
                name: name.to_string(),
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(())
    }

    fn rename(
        &self,
        parent: Ino,
        name: &str,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let result = rename_in_tx(
            &mut tx,
            &self.ns,
            dirty,
            &self.atime,
            &self.orphans,
            &self.xattr_by_name,
            &self.chunk_ref,
            &self.chunk_ref_by_ino,
            &self.blobs,
            parent,
            name,
            new_parent,
            new_name,
        )?;
        if let Some((_ino, t, db, df)) = result {
            journal::append_tx(
                &mut tx,
                &self.journal_ks,
                &self.local,
                &self.completed,
                &LogRecord::Rename {
                    parent,
                    name: name.to_string(),
                    new_parent,
                    new_name: new_name.to_string(),
                    time_ns: t,
                },
            )?;
            crate::store::adjust_usage_tx(&mut tx, &self.local, db, df)?;
        }
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        if let Some((_, _, db, df)) = result {
            self.usage_tracker().adjust(db, df);
        }
        Ok(())
    }

    fn setattr(
        &self,
        ino: Ino,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime_ns: Option<i64>,
        mtime_ns: Option<i64>,
    ) -> Result<FileAttr, MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let Some(mut rec) = ns::get_inode_record(&tx, &self.ns, ino)? else {
            return Err(MetaError::NoEnt(ino));
        };
        let t = now_ns();
        let old_size = rec.attrs.size;
        if let Some(m) = mode {
            rec.attrs.mode = mask_mode(m);
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
        ns::put_inode_record(&mut tx, &self.ns, dirty, ino, &rec)?;
        if let Some(a) = atime_ns {
            atime::set_atime_tx(&mut tx, &self.atime, ino, a);
        }
        let at = atime::get_atime(&tx, &self.atime, ino)?;
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Setattr {
                ino,
                mode,
                uid,
                gid,
                size,
                atime_ns,
                mtime_ns,
                time_ns: t,
            },
        )?;
        // Persisted with the change like every other usage move (it used
        // to be in-memory only), so a rollback of this transaction (plan
        // 30 §M3b) restores the counter it moved.
        let size_delta = if size.is_some() && attrs.kind == Kind::File {
            attrs.size as i64 - old_size as i64
        } else {
            0
        };
        crate::store::adjust_usage_tx(&mut tx, &self.local, size_delta, 0)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        self.usage_tracker().adjust(size_delta, 0);
        Ok(ns::attrs_to_fileattr(ino, &attrs, at))
    }

    fn set_manifest(&self, ino: Ino, manifest: &[u8], size: u64) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let delta = Self::set_manifest_tx(
            &mut tx,
            &self.ns,
            dirty,
            &self.blobs,
            &self.chunk_ref,
            &self.chunk_ref_by_ino,
            &self.journal_ks,
            &self.local,
            &self.completed,
            ino,
            None,
            manifest,
            size,
        )?;
        crate::store::adjust_usage_tx(&mut tx, &self.local, delta, 0)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        self.usage_tracker().adjust(delta, 0);
        Ok(())
    }

    fn set_xattr(
        &self,
        ino: Ino,
        name: &str,
        value: &[u8],
        mode: SetXattrMode,
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let Some(rec) = ns::get_inode_record(&tx, &self.ns, ino)? else {
            return Err(MetaError::NoEnt(ino));
        };
        let existing = misc::get_one_xattr(&tx, &self.ns, &self.blobs, ino, name)?;
        match mode {
            SetXattrMode::Create if existing.is_some() => return Err(MetaError::Exists),
            SetXattrMode::Replace if existing.is_none() => return Err(MetaError::NoData),
            _ => {}
        }
        let t = now_ns();
        let mut xattrs = ns::all_xattrs(&tx, &self.ns, &self.blobs, &rec, ino)?;
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
            .map(|p| ns::resolve_payload(&tx, &self.blobs, p))
            .transpose()?;
        let target = rec
            .symlink_target
            .as_ref()
            .map(|p| ns::resolve_payload(&tx, &self.blobs, p))
            .transpose()?;
        ns::put_inode(
            &mut tx,
            &self.ns,
            dirty,
            &self.blobs,
            ino,
            attrs,
            manifest,
            target,
            &xattr_pairs,
        )?;
        misc::xattr_by_name_put_tx(&mut tx, &self.xattr_by_name, name, ino, value);
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::SetXattr {
                ino,
                name: name.to_string(),
                value: value.to_vec(),
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(())
    }

    fn remove_xattr(&self, ino: Ino, name: &str) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let dirty = local.dirty(self);
        let Some(rec) = ns::get_inode_record(&tx, &self.ns, ino)? else {
            return Err(MetaError::NoEnt(ino));
        };
        let existing = misc::get_one_xattr(&tx, &self.ns, &self.blobs, ino, name)?;
        if existing.is_none() {
            return Err(MetaError::NoData);
        }
        let t = now_ns();
        let mut xattrs = ns::all_xattrs(&tx, &self.ns, &self.blobs, &rec, ino)?;
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
            .map(|p| ns::resolve_payload(&tx, &self.blobs, p))
            .transpose()?;
        let target = rec
            .symlink_target
            .as_ref()
            .map(|p| ns::resolve_payload(&tx, &self.blobs, p))
            .transpose()?;
        ns::put_inode(
            &mut tx,
            &self.ns,
            dirty,
            &self.blobs,
            ino,
            attrs,
            manifest,
            target,
            &xattr_pairs,
        )?;
        misc::xattr_by_name_del_tx(&mut tx, &self.xattr_by_name, name, ino);
        journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::RemoveXattr {
                ino,
                name: name.to_string(),
                time_ns: t,
            },
        )?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(())
    }

    fn reap_orphan(&self, ino: Ino) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        if let Some(v) = tx.get(&self.orphans, ino.to_be_bytes())? {
            let rec = InodeRecord::decode(&v)?;
            let manifest = rec
                .manifest
                .as_ref()
                .map(|p| ns::resolve_payload(&tx, &self.blobs, p))
                .transpose()?;
            misc::track_manifest_transition_tx(
                &mut tx,
                &self.chunk_ref,
                &self.chunk_ref_by_ino,
                ino,
                manifest.as_deref(),
                None,
            )?;
            tx.remove(&self.orphans, ino.to_be_bytes());
            atime::remove_atime_tx(&mut tx, &self.atime, ino);
        }
        tx.commit()?;
        Ok(())
    }

    fn orphans(&self) -> Result<Vec<Ino>, MetaError> {
        let r = self.db.read_tx();
        let mut out = Vec::new();
        for guard in r.iter(&self.orphans) {
            let (k, _) = guard.into_inner()?;
            if k.len() == 8 {
                out.push(u64::from_be_bytes(k.as_ref().try_into().unwrap()));
            }
        }
        Ok(out)
    }

    fn take_journal(&self, max: usize) -> Result<Vec<(u64, LogRecord)>, MetaError> {
        let r = self.db.read_tx();
        journal::take(&r, &self.journal_ks, &self.local, max)
    }

    fn ack_journal(&self, upto_seq: u64) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let applied = crate::store::applied_seq_at(&tx, &self.local)?;
        crate::store::spec::retire_local_tx(&mut tx, self, upto_seq, applied)?;
        journal::ack_upto(&mut tx, &self.journal_ks, &self.local, upto_seq)?;
        tx.commit()?;
        Ok(())
    }

    fn journal_len(&self) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        journal::len(&r, &self.journal_ks, &self.local)
    }

    fn apply_atime(&self, bumps: &[(Ino, i64, i64)]) -> Result<(u64, u64), MetaError> {
        let mut tx = self.db.write_tx();
        let skew = atime::atime_skew_tolerance_ns();
        let mut applied = 0u64;
        let mut clamped = 0u64;
        for (ino, atime_ns, time_ns) in bumps {
            let (did_apply, was_clamped) = atime::apply_atime_one(
                &mut tx,
                &self.ns,
                &self.orphans,
                &self.atime,
                *ino,
                *atime_ns,
                *time_ns,
                skew,
            )?;
            if did_apply {
                applied += 1;
            }
            if was_clamped {
                clamped += 1;
            }
        }
        tx.commit()?;
        Ok((applied, clamped))
    }

    fn queue_atime(&self, bumps: &[(Ino, i64, i64)]) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        for (ino, atime_ns, time_ns) in bumps {
            atime::queue_atime_tx(&mut tx, &self.atime_journal, *ino, *atime_ns, *time_ns)?;
        }
        tx.commit()?;
        Ok(())
    }

    fn atime_backlog_of(&self, _part: &str) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        atime::atime_backlog(&r, &self.atime_journal)
    }

    fn atime_oldest_pending_ns(&self, _part: &str) -> Result<Option<i64>, MetaError> {
        let r = self.db.read_tx();
        atime::oldest_pending_time_ns(&r, &self.atime_journal)
    }

    fn take_atime_of(&self, _part: &str, max: usize) -> Result<Vec<(Ino, i64, i64)>, MetaError> {
        let r = self.db.read_tx();
        atime::take_atime(&r, &self.atime_journal, max)
    }

    fn clear_atime(&self, _part: &str, inos: &[Ino]) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        atime::clear_atime_tx(&mut tx, &self.atime_journal, inos);
        tx.commit()?;
        Ok(())
    }

    fn drop_atime_of(&self, _part: &str) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        atime::drop_atime_all(&mut tx, &self.atime_journal)?;
        tx.commit()?;
        Ok(())
    }
}
