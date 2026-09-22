//! Path resolution, the plan 28 §11 tree-builder read surface
//! (`tree_inode`, `links_of`, `scan_inos`, `dump_replicated`), and
//! `recursive_size` (a DFS over `0x02` since there is no maintained
//! per-directory counter yet — that is plan 29 M3).

use crate::error::MetaError;
use crate::store::{ns, Meta};
use crate::TreeInode;
use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::Ino;
use constellation_mtree::keys;
use fjall::Readable;

impl Meta {
    pub fn resolve_path(&self, path: &str) -> Result<Option<Ino>, MetaError> {
        let r = self.db.read_tx();
        let mut cur = ROOT_INO;
        for seg in path.split('/').filter(|s| !s.is_empty()) {
            match ns::child_ino(&r, &self.ns, cur, seg)? {
                Some(next) => cur = next,
                None => return Ok(None),
            }
        }
        Ok(Some(cur))
    }

    pub fn parent_of(&self, ino: Ino) -> Result<Option<Ino>, MetaError> {
        let r = self.db.read_tx();
        self.parent_of_at(&r, ino)
    }

    pub(crate) fn parent_of_at(
        &self,
        r: &impl Readable,
        ino: Ino,
    ) -> Result<Option<Ino>, MetaError> {
        ns::parent_of(r, &self.ns, ino)
    }

    /// Upward walk to the root via the `0x04` reverse index. Silently
    /// stops (returning a partial path) on a broken parent chain,
    /// matching the old engine.
    pub fn path_of(&self, ino: Ino) -> Result<String, MetaError> {
        if ino == ROOT_INO {
            return Ok("/".to_string());
        }
        let r = self.db.read_tx();
        let mut names = Vec::new();
        let mut cur = ino;
        loop {
            let range = keys::names_of(cur);
            let Some(guard) = r.range(&self.ns, ns::key_range_bounds(&range)).next() else {
                break;
            };
            let (k, _) = guard.into_inner()?;
            let keys::Key::RDentry {
                parent_ino, name, ..
            } = keys::Key::parse(&k)?
            else {
                break;
            };
            names.push(String::from_utf8_lossy(name).into_owned());
            if parent_ino == ROOT_INO {
                break;
            }
            cur = parent_ino;
        }
        names.reverse();
        Ok(format!("/{}", names.join("/")))
    }

    pub fn child_ino(&self, parent: Ino, name: &str) -> Result<Option<Ino>, MetaError> {
        let r = self.db.read_tx();
        ns::child_ino(&r, &self.ns, parent, name)
    }

    pub fn child_ino_reader(&self, parent: Ino, name: &str) -> Result<Option<Ino>, MetaError> {
        self.child_ino(parent, name)
    }

    pub fn child_ino_at(
        &self,
        r: &impl Readable,
        parent: Ino,
        name: &str,
    ) -> Result<Option<Ino>, MetaError> {
        ns::child_ino(r, &self.ns, parent, name)
    }

    // ------------------------------------------------------- tree builder

    pub fn tree_inode(&self, ino: Ino) -> Result<Option<TreeInode>, MetaError> {
        let r = self.db.read_tx();
        self.tree_inode_at(&r, ino)
    }

    pub fn tree_inode_at(
        &self,
        r: &impl Readable,
        ino: Ino,
    ) -> Result<Option<TreeInode>, MetaError> {
        let Some(rec) = ns::get_inode_record(r, &self.ns, ino)? else {
            return Ok(None);
        };
        let attr = ns::attrs_to_fileattr(
            ino,
            &rec.attrs,
            crate::store::atime::get_atime(r, &self.atime, ino)?,
        );
        let target = match &rec.symlink_target {
            Some(p) => {
                Some(String::from_utf8_lossy(&ns::resolve_payload(r, &self.blobs, p)?).into_owned())
            }
            None => None,
        };
        let manifest = match &rec.manifest {
            Some(p) => Some(ns::resolve_payload(r, &self.blobs, p)?),
            None => None,
        };
        let xattrs = ns::all_xattrs(r, &self.ns, &self.blobs, &rec, ino)?;
        Ok(Some(TreeInode {
            attr,
            target,
            manifest,
            xattrs,
        }))
    }

    pub fn links_of(&self, ino: Ino) -> Result<Vec<(Ino, String)>, MetaError> {
        let r = self.db.read_tx();
        ns::links_of(&r, &self.ns, ino)
    }

    pub fn links_of_at(
        &self,
        r: &impl Readable,
        ino: Ino,
    ) -> Result<Vec<(Ino, String)>, MetaError> {
        ns::links_of(r, &self.ns, ino)
    }

    pub fn scan_inos(&self, after: Ino, limit: usize) -> Result<Vec<Ino>, MetaError> {
        let r = self.db.read_tx();
        self.scan_inos_at(&r, after, limit)
    }

    pub fn scan_inos_at(
        &self,
        r: &impl Readable,
        after: Ino,
        limit: usize,
    ) -> Result<Vec<Ino>, MetaError> {
        let start = keys::inode(after.saturating_add(1));
        let end = keys::whole_range(keys::RANGE_INODE).end().to_vec();
        let mut out = Vec::new();
        for guard in r.range(&self.ns, start..end) {
            if out.len() >= limit {
                break;
            }
            let (k, _) = guard.into_inner()?;
            if let keys::Key::Inode { ino } = keys::Key::parse(&k)? {
                out.push(ino);
            }
        }
        Ok(out)
    }

    /// DFS over `0x02` in one snapshot: no maintained per-directory
    /// counter yet (plan 29 M3), so this walks the subtree exactly like
    /// the old `WITH RECURSIVE` CTE did.
    pub fn recursive_size(&self, ino: Ino) -> Result<(u64, u64), MetaError> {
        let r = self.db.read_tx();
        if ns::get_inode_record(&r, &self.ns, ino)?.is_none() {
            return Err(MetaError::NoEnt(ino));
        }
        let (mut bytes, mut files) = (0u64, 0u64);
        if let Some(rec) = ns::get_inode_record(&r, &self.ns, ino)? {
            if rec.attrs.kind == constellation_mtree::record::Kind::File {
                bytes += rec.attrs.size;
                files += 1;
            }
        }
        let mut stack = vec![ino];
        while let Some(dir) = stack.pop() {
            let range = keys::dentries_of(dir);
            for guard in r.range(&self.ns, ns::key_range_bounds(&range)) {
                let (_, v) = guard.into_inner()?;
                let (child_ino, kind) =
                    constellation_mtree::record::DentryRecord::ino_and_kind(&v)?;
                if kind == constellation_mtree::record::Kind::File {
                    if let Some(rec) = ns::get_inode_record(&r, &self.ns, child_ino)? {
                        bytes += rec.attrs.size;
                        files += 1;
                    }
                } else if kind == constellation_mtree::record::Kind::Dir {
                    stack.push(child_ino);
                }
            }
        }
        Ok((bytes, files))
    }

    /// Every file's manifest under `ino` (including `ino` itself if it
    /// is a file): `(ino, manifest_bytes, logical_size)`. Used by pin
    /// admission to compute a subtree's chunk footprint.
    pub fn subtree_manifests(&self, ino: Ino) -> Result<Vec<(Ino, Vec<u8>, u64)>, MetaError> {
        let r = self.db.read_tx();
        let mut out = Vec::new();
        let push_if_file = |out: &mut Vec<(Ino, Vec<u8>, u64)>,
                            ino: Ino,
                            rec: &constellation_mtree::record::InodeRecord|
         -> Result<(), MetaError> {
            if rec.attrs.kind == constellation_mtree::record::Kind::File {
                if let Some(p) = &rec.manifest {
                    out.push((
                        ino,
                        ns::resolve_payload(&r, &self.blobs, p)?,
                        rec.attrs.size,
                    ));
                }
            }
            Ok(())
        };
        let Some(root_rec) = ns::get_inode_record(&r, &self.ns, ino)? else {
            return Err(MetaError::NoEnt(ino));
        };
        push_if_file(&mut out, ino, &root_rec)?;
        let mut stack = vec![ino];
        while let Some(dir) = stack.pop() {
            let range = keys::dentries_of(dir);
            for guard in r.range(&self.ns, ns::key_range_bounds(&range)) {
                let (_, v) = guard.into_inner()?;
                let (child_ino, kind) =
                    constellation_mtree::record::DentryRecord::ino_and_kind(&v)?;
                if kind == constellation_mtree::record::Kind::Dir {
                    stack.push(child_ino);
                } else if let Some(rec) = ns::get_inode_record(&r, &self.ns, child_ino)? {
                    push_if_file(&mut out, child_ino, &rec)?;
                }
            }
        }
        Ok(out)
    }

    /// Every raw key currently in `ns`, in key order. Exposed for tests
    /// and tooling that want to compare the local replica's key set
    /// against an independently built tree (plan 28 §P6: `ns`'s key set
    /// must equal the published tree's, modulo spilling).
    pub fn ns_keys(&self) -> Result<Vec<Vec<u8>>, MetaError> {
        let r = self.db.read_tx();
        let mut out = Vec::new();
        for guard in r.iter(&self.ns) {
            let (k, _) = guard.into_inner()?;
            out.push(k.to_vec());
        }
        Ok(out)
    }

    /// Sorted debug dump of the replicated namespace (`ns`'s inodes with
    /// `nlink > 0` — everything in `ns` already satisfies that — plus
    /// dentries, xattrs and subsystem records), for cross-replica
    /// equality checks after a tree rebuild.
    pub fn dump_replicated(&self) -> Result<Vec<String>, MetaError> {
        let r = self.db.read_tx();
        let mut out = Vec::new();
        for guard in r.range(
            &self.ns,
            ns::key_range_bounds(&keys::whole_range(keys::RANGE_INODE)),
        ) {
            let (k, v) = guard.into_inner()?;
            let keys::Key::Inode { ino } = keys::Key::parse(&k)? else {
                continue;
            };
            let rec = constellation_mtree::record::InodeRecord::decode(&v)?;
            out.push(format!(
                "inode {ino} {:?} {} {} {} {} {}",
                rec.attrs.kind,
                rec.attrs.size,
                rec.attrs.mode,
                rec.attrs.uid,
                rec.attrs.gid,
                rec.attrs.nlink
            ));
        }
        for guard in r.range(
            &self.ns,
            ns::key_range_bounds(&keys::whole_range(keys::RANGE_DENTRY)),
        ) {
            let (k, v) = guard.into_inner()?;
            let keys::Key::Dentry { parent_ino, name } = keys::Key::parse(&k)? else {
                continue;
            };
            let (ino, _) = constellation_mtree::record::DentryRecord::ino_and_kind(&v)?;
            out.push(format!(
                "dentry {parent_ino} {} {ino}",
                String::from_utf8_lossy(name)
            ));
        }
        for guard in r.range(
            &self.ns,
            ns::key_range_bounds(&keys::whole_range(keys::RANGE_INODE)),
        ) {
            let (k, v) = guard.into_inner()?;
            let keys::Key::Inode { ino } = keys::Key::parse(&k)? else {
                continue;
            };
            let rec = constellation_mtree::record::InodeRecord::decode(&v)?;
            for (name, value) in ns::all_xattrs(&r, &self.ns, &self.blobs, &rec, ino)? {
                out.push(format!("xattr {ino} {name} {}", hex(&value)));
            }
        }
        for row in self.snapshots_at(&r, None)? {
            out.push(format!(
                "snapshot {} {} {} {} {}",
                row.id, row.path, row.name, row.root_hash, row.created_unix_ms
            ));
        }
        if let Some(v) = r.get(&self.ns, keys::subsystem(keys::Subsystem::Quota, b""))? {
            out.push(format!("kv quota_max_bytes {}", hex(&v)));
        }
        out.sort();
        Ok(out)
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
