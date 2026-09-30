//! The scratch-directory node-local namespace: private staging content
//! under a directory marked with [`crate::store::SCRATCH_XATTR`]. Never
//! journaled, never replicated, but stored in the same
//! `constellation_mtree` key/value encoding as `ns` (so a scratch → shared
//! publish, `Meta::publish_file`, is a record copy rather than a
//! translation).
//!
//! Two small, deliberate fixes over the old SQLite engine (documented
//! since "no backwards compatibility" applies): `scratch_mkdir` now
//! pre-checks for an existing ino the way `scratch_create` always did,
//! and `scratch_rename` now rejects an occupied destination with
//! `MetaError::Exists` instead of silently overwriting it (the old code
//! surfaced an unmapped SQLite constraint violation there).

use crate::error::MetaError;
use crate::store::{ns, Meta};
use crate::{DirEntry, MetaStore, SetXattrMode};
use constellation_fs_core::types::now_ns;
use constellation_fs_core::{FileAttr, Ino};
use constellation_mtree::keys;
use constellation_mtree::record::{Attrs, Kind};
use fjall::Readable;

impl Meta {
    pub fn is_scratch_dir(&self, ino: Ino) -> Result<bool, MetaError> {
        Ok(self.get_xattr(ino, crate::store::SCRATCH_XATTR)?.as_deref() == Some(b"1"))
    }

    pub fn scratch_create(
        &self,
        _parent: Ino,
        name: &str,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        self.scratch_new(_parent, name, ino, Kind::File, mode, uid, gid, 1)
    }

    pub fn scratch_mkdir(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        self.scratch_new(parent, name, ino, Kind::Dir, mode, uid, gid, 2)
    }

    #[allow(clippy::too_many_arguments)]
    fn scratch_new(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
        kind: Kind,
        mode: u32,
        uid: u32,
        gid: u32,
        nlink: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut tx = self.db.write_tx();
        if ns::get_inode_record(&tx, &self.scratch, ino)?.is_some() {
            return Err(MetaError::Exists);
        }
        if ns::child_ino(&tx, &self.scratch, parent, name)?.is_some() {
            return Err(MetaError::Exists);
        }
        let t = now_ns();
        let attrs = Attrs {
            kind,
            mode: mode & 0o7777,
            uid,
            gid,
            nlink,
            size: 0,
            mtime_ns: t,
            ctime_ns: t,
            rdev: constellation_types::Rdev::default(),
        };
        ns::put_inode(
            &mut tx,
            &self.scratch,
            ns::Dirty::Untracked,
            &self.blobs,
            ino,
            attrs,
            None,
            None,
            &[],
        )?;
        ns::put_dentry(
            &mut tx,
            &self.scratch,
            ns::Dirty::Untracked,
            parent,
            name,
            ino,
            attrs,
        )?;
        tx.commit()?;
        Ok(ns::attrs_to_fileattr(ino, &attrs, t))
    }

    pub fn scratch_lookup(&self, parent: Ino, name: &str) -> Result<Option<FileAttr>, MetaError> {
        let r = self.db.read_tx();
        let Some(d) = ns::get_dentry_record(&r, &self.scratch, parent, name)? else {
            return Ok(None);
        };
        Ok(Some(ns::attrs_to_fileattr(
            d.ino,
            &d.attrs,
            d.attrs.mtime_ns,
        )))
    }

    pub fn scratch_unlink(&self, parent: Ino, name: &str) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let Some(d) = ns::get_dentry_record(&tx, &self.scratch, parent, name)? else {
            return Err(MetaError::NoEntry);
        };
        ns::remove_dentry(
            &mut tx,
            &self.scratch,
            ns::Dirty::Untracked,
            parent,
            name,
            d.ino,
        )?;
        tx.remove(&self.scratch, keys::inode(d.ino));
        ns::clear_spilled_xattrs(&mut tx, &self.scratch, ns::Dirty::Untracked, d.ino)?;
        tx.commit()?;
        Ok(())
    }

    pub fn scratch_rename(
        &self,
        parent: Ino,
        name: &str,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let Some(d) = ns::get_dentry_record(&tx, &self.scratch, parent, name)? else {
            return Err(MetaError::NoEntry);
        };
        if ns::child_ino(&tx, &self.scratch, new_parent, new_name)?.is_some() {
            return Err(MetaError::Exists);
        }
        ns::remove_dentry(
            &mut tx,
            &self.scratch,
            ns::Dirty::Untracked,
            parent,
            name,
            d.ino,
        )?;
        ns::put_dentry(
            &mut tx,
            &self.scratch,
            ns::Dirty::Untracked,
            new_parent,
            new_name,
            d.ino,
            d.attrs,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn scratch_readdir(&self, parent: Ino) -> Result<Vec<DirEntry>, MetaError> {
        let r = self.db.read_tx();
        ns::readdir_entries(&r, &self.scratch, parent)
    }

    pub fn scratch_getattr(&self, ino: Ino) -> Result<Option<FileAttr>, MetaError> {
        let r = self.db.read_tx();
        let Some(rec) = ns::get_inode_record(&r, &self.scratch, ino)? else {
            return Ok(None);
        };
        Ok(Some(ns::attrs_to_fileattr(
            ino,
            &rec.attrs,
            rec.attrs.mtime_ns,
        )))
    }

    pub fn scratch_set_manifest(
        &self,
        ino: Ino,
        manifest: &[u8],
        size: u64,
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let Some(rec) = ns::get_inode_record(&tx, &self.scratch, ino)? else {
            return Err(MetaError::NoEnt(ino));
        };
        let mut attrs = rec.attrs;
        let t = now_ns();
        attrs.size = size;
        attrs.mtime_ns = t;
        attrs.ctime_ns = t;
        let xattrs = rec.xattrs.clone();
        let target = rec
            .symlink_target
            .as_ref()
            .map(|p| ns::resolve_payload(&tx, &self.blobs, p))
            .transpose()?;
        ns::put_inode(
            &mut tx,
            &self.scratch,
            ns::Dirty::Untracked,
            &self.blobs,
            ino,
            attrs,
            Some(manifest.to_vec()),
            target,
            &xattrs,
        )?;
        for (parent, name) in ns::links_of(&tx, &self.scratch, ino)? {
            tx.insert(
                &self.scratch,
                keys::dentry(parent, name.as_bytes()),
                constellation_mtree::record::DentryRecord::new(ino, attrs).encode(),
            );
        }
        tx.commit()?;
        Ok(())
    }

    pub fn scratch_manifest(&self, ino: Ino) -> Result<Option<Vec<u8>>, MetaError> {
        let r = self.db.read_tx();
        let Some(rec) = ns::get_inode_record(&r, &self.scratch, ino)? else {
            return Ok(None);
        };
        match &rec.manifest {
            Some(p) => Ok(Some(ns::resolve_payload(&r, &self.blobs, p)?)),
            None => Ok(None),
        }
    }

    fn scratch_exists(&self, ino: Ino) -> Result<bool, MetaError> {
        let r = self.db.read_tx();
        Ok(ns::get_inode_record(&r, &self.scratch, ino)?.is_some())
    }

    pub fn scratch_get_xattr(&self, ino: Ino, name: &str) -> Result<Option<Vec<u8>>, MetaError> {
        if !self.scratch_exists(ino)? {
            return Err(MetaError::NoEnt(ino));
        }
        let r = self.db.read_tx();
        crate::store::misc::get_one_xattr(&r, &self.scratch, &self.blobs, ino, name)
    }

    pub fn scratch_list_xattrs(&self, ino: Ino) -> Result<Vec<String>, MetaError> {
        if !self.scratch_exists(ino)? {
            return Err(MetaError::NoEnt(ino));
        }
        let r = self.db.read_tx();
        let rec = ns::get_inode_record(&r, &self.scratch, ino)?.unwrap();
        Ok(ns::all_xattrs(&r, &self.scratch, &self.blobs, &rec, ino)?
            .into_iter()
            .map(|(n, _)| n)
            .collect())
    }

    pub fn scratch_xattrs(&self, ino: Ino) -> Result<Vec<(String, Vec<u8>)>, MetaError> {
        let r = self.db.read_tx();
        let Some(rec) = ns::get_inode_record(&r, &self.scratch, ino)? else {
            return Ok(Vec::new());
        };
        ns::all_xattrs(&r, &self.scratch, &self.blobs, &rec, ino)
    }

    pub fn scratch_set_xattr(
        &self,
        ino: Ino,
        name: &str,
        value: &[u8],
        mode: SetXattrMode,
    ) -> Result<(), MetaError> {
        if !self.scratch_exists(ino)? {
            return Err(MetaError::NoEnt(ino));
        }
        let mut tx = self.db.write_tx();
        let rec = ns::get_inode_record(&tx, &self.scratch, ino)?.unwrap();
        let existing =
            crate::store::misc::get_one_xattr(&tx, &self.scratch, &self.blobs, ino, name)?;
        match mode {
            SetXattrMode::Create if existing.is_some() => return Err(MetaError::Exists),
            SetXattrMode::Replace if existing.is_none() => return Err(MetaError::NoData),
            _ => {}
        }
        let t = now_ns();
        let mut xattrs = ns::all_xattrs(&tx, &self.scratch, &self.blobs, &rec, ino)?;
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
            &self.scratch,
            ns::Dirty::Untracked,
            &self.blobs,
            ino,
            attrs,
            manifest,
            target,
            &xattr_pairs,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn scratch_remove_xattr(&self, ino: Ino, name: &str) -> Result<(), MetaError> {
        if !self.scratch_exists(ino)? {
            return Err(MetaError::NoEnt(ino));
        }
        let mut tx = self.db.write_tx();
        let rec = ns::get_inode_record(&tx, &self.scratch, ino)?.unwrap();
        let existing =
            crate::store::misc::get_one_xattr(&tx, &self.scratch, &self.blobs, ino, name)?;
        if existing.is_none() {
            return Err(MetaError::NoData);
        }
        let t = now_ns();
        let mut xattrs = ns::all_xattrs(&tx, &self.scratch, &self.blobs, &rec, ino)?;
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
            &self.scratch,
            ns::Dirty::Untracked,
            &self.blobs,
            ino,
            attrs,
            manifest,
            target,
            &xattr_pairs,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn scratch_purge_all(&self) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let keys: Vec<Vec<u8>> = tx
            .iter(&self.scratch)
            .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;
        for k in keys {
            tx.remove(&self.scratch, k);
        }
        tx.commit()?;
        Ok(())
    }
}
