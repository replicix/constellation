//! The synthetic `.constellation` tree (snapshots, plan 09): lookup-only
//! nodes mirroring frozen content, numbered above `SYNTHETIC_INO_BIT`.

use super::*;

pub(super) const SYNTHETIC_INO_BIT: u64 = 1 << 63;

/// Serializable: a session handover carries a view's synthetic numbering
/// to the next process (`view::handoff`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) enum SyntheticNode {
    /// `<dir>/.constellation` of the directory with inode `dir`, wherever
    /// it sits now: the node is interned once per parent inode and kept,
    /// so it must not capture the path (plan 32 §0.5 — a renamed
    /// directory keeps listing its own snapshots, not its old path's).
    ConstellationOf { dir: Ino },
    /// `<dir>/.constellation/snapshot` (see [`Self::ConstellationOf`]).
    SnapshotsOf { dir: Ino },
    Frozen {
        snapshot_id: String,
        kind: InodeKind,
        mode: u32,
        uid: u32,
        gid: u32,
        size: u64,
        mtime_ns: i64,
        target: Option<String>,
        object: Option<crate::snapshot::FrozenObject>,
        xattrs: Vec<(String, Vec<u8>)>,
    },
}

pub(super) struct SyntheticRegistry {
    pub(super) nodes: HashMap<Ino, SyntheticNode>,
    pub(super) keys: HashMap<String, Ino>,
    pub(super) next: Ino,
}

impl View {
    pub(crate) fn is_synthetic(ino: Ino) -> bool {
        ino & SYNTHETIC_INO_BIT != 0
    }

    pub(crate) fn synthetic_node(&self, ino: Ino) -> Option<SyntheticNode> {
        self.synthetic.lock().unwrap().nodes.get(&ino).cloned()
    }

    pub(crate) fn synthetic_active(&self, node: &SyntheticNode) -> bool {
        let SyntheticNode::Frozen { snapshot_id, .. } = node else {
            return true;
        };
        self.meta
            .snapshots(None)
            .map(|rows| rows.iter().any(|row| &row.id == snapshot_id))
            .unwrap_or(false)
    }

    pub(crate) fn intern_synthetic(&self, key: String, node: SyntheticNode) -> Ino {
        let mut registry = self.synthetic.lock().unwrap();
        if let Some(ino) = registry.keys.get(&key) {
            return *ino;
        }
        let ino = registry.next;
        registry.next += 1;
        registry.nodes.insert(ino, node);
        registry.keys.insert(key, ino);
        ino
    }

    pub(crate) fn synthetic_attr(&self, ino: Ino, node: &SyntheticNode) -> FileAttr {
        let (kind, mode, uid, gid, size, mtime_ns) = match node {
            SyntheticNode::ConstellationOf { .. } | SyntheticNode::SnapshotsOf { .. } => {
                (InodeKind::Dir, 0o555, 0, 0, 0, 0)
            }
            SyntheticNode::Frozen {
                kind,
                mode,
                uid,
                gid,
                size,
                mtime_ns,
                ..
            } => (*kind, *mode & !0o222, *uid, *gid, *size, *mtime_ns),
        };
        FileAttr {
            ino,
            kind,
            size,
            mode,
            uid,
            gid,
            nlink: if kind == InodeKind::Dir { 2 } else { 1 },
            atime_ns: mtime_ns,
            mtime_ns,
            ctime_ns: mtime_ns,
            rdev: Default::default(),
        }
    }

    pub(crate) fn synthetic_xattrs(&self, ino: Ino) -> Result<Vec<(String, Vec<u8>)>, Code> {
        let node = self.synthetic_node(ino).ok_or(Code::Stale)?;
        if !self.synthetic_active(&node) {
            return Err(Code::Stale);
        }
        match node {
            SyntheticNode::Frozen {
                kind: InodeKind::Dir,
                object: Some(tree_hash),
                xattrs,
                ..
            } if xattrs.is_empty() => Ok(self.snapshot_tree(tree_hash)?.xattrs),
            SyntheticNode::Frozen { xattrs, .. } => Ok(xattrs),
            _ => Ok(Vec::new()),
        }
    }

    pub(crate) fn synthetic_recursive_size(&self, ino: Ino) -> Result<(u64, u64), Code> {
        let node = self.synthetic_node(ino).ok_or(Code::Stale)?;
        if !self.synthetic_active(&node) {
            return Err(Code::Stale);
        }
        self.synthetic_node_recursive_size(&node)
    }

    pub(super) fn synthetic_node_recursive_size(
        &self,
        node: &SyntheticNode,
    ) -> Result<(u64, u64), Code> {
        match node {
            SyntheticNode::Frozen {
                kind: InodeKind::File,
                size,
                ..
            } => Ok((*size, 1)),
            SyntheticNode::Frozen {
                kind: InodeKind::Dir,
                object: Some(tree_hash),
                ..
            } => {
                let mut total = (0u64, 0u64);
                for entry in self.snapshot_tree(*tree_hash)?.entries {
                    match entry.kind {
                        InodeKind::File => {
                            total.0 = total.0.saturating_add(entry.size);
                            total.1 = total.1.saturating_add(1);
                        }
                        InodeKind::Dir => {
                            let child = SyntheticNode::Frozen {
                                snapshot_id: String::new(),
                                kind: entry.kind,
                                mode: entry.mode,
                                uid: entry.uid,
                                gid: entry.gid,
                                size: entry.size,
                                mtime_ns: entry.mtime_ns,
                                target: entry.target,
                                object: entry.object,
                                xattrs: entry.xattrs,
                            };
                            let subtotal = self.synthetic_node_recursive_size(&child)?;
                            total.0 = total.0.saturating_add(subtotal.0);
                            total.1 = total.1.saturating_add(subtotal.1);
                        }
                        _ => {}
                    }
                }
                Ok(total)
            }
            _ => Ok((0, 0)),
        }
    }

    pub(crate) fn snapshot_tree(
        &self,
        hash: crate::snapshot::FrozenObject,
    ) -> Result<crate::snapshot::FrozenDir, Code> {
        {
            let cache = self.tree_cache.lock().unwrap();
            if let Some(tree) = cache.0.get(&hash) {
                return Ok(tree.clone());
            }
        }
        let tree = self
            .rt
            .block_on(async {
                constellation_vfs::watch::stage("snapshot tree load");
                self.snapshots.list_frozen(&hash).await
            })
            .map_err(|_| Code::Io)?;
        let mut cache = self.tree_cache.lock().unwrap();
        if cache.0.len() >= 128 {
            if let Some(oldest) = cache.1.pop_front() {
                cache.0.remove(&oldest);
            }
        }
        cache.0.insert(hash, tree.clone());
        cache.1.push_back(hash);
        Ok(tree)
    }

    /// The snapshots `<dir>/.constellation/snapshot` lists.
    fn covering(
        &self,
        dir: Ino,
    ) -> Result<
        Vec<(
            constellation_meta::SnapshotRow,
            crate::snapshot::FrozenObject,
        )>,
        Code,
    > {
        self.rt
            .block_on(self.snapshots.covering(dir))
            .map_err(|error| {
                tracing::debug!(dir, %error, "snapshot listing failed");
                Code::Io
            })
    }

    pub(crate) fn lookup_synthetic(
        &self,
        parent: Ino,
        name: &str,
    ) -> Result<Option<(Ino, FileAttr)>, Code> {
        let node = if !Self::is_synthetic(parent) {
            if name != ".constellation" {
                return Ok(None);
            }
            let attr = self
                .meta
                .getattr(parent)
                .map_err(|error| error.code())?
                .ok_or(Code::NotFound)?;
            if attr.kind != InodeKind::Dir {
                return Ok(None);
            }
            SyntheticNode::ConstellationOf { dir: parent }
        } else {
            let parent_node = self.synthetic_node(parent).ok_or(Code::Stale)?;
            if !self.synthetic_active(&parent_node) {
                return Err(Code::Stale);
            }
            match parent_node {
                SyntheticNode::ConstellationOf { dir } if name == "snapshot" => {
                    SyntheticNode::SnapshotsOf { dir }
                }
                SyntheticNode::SnapshotsOf { dir } => {
                    let (row, root) = self
                        .covering(dir)?
                        .into_iter()
                        .find(|(row, _)| row.name == name)
                        .ok_or(Code::NotFound)?;
                    SyntheticNode::Frozen {
                        snapshot_id: row.id,
                        kind: InodeKind::Dir,
                        mode: 0o555,
                        uid: 0,
                        gid: 0,
                        size: 0,
                        mtime_ns: row.created_unix_ms * 1_000_000,
                        target: None,
                        object: Some(root),
                        xattrs: Vec::new(),
                    }
                }
                SyntheticNode::Frozen {
                    snapshot_id,
                    kind: InodeKind::Dir,
                    object: Some(tree_hash),
                    ..
                } => {
                    let tree = self.snapshot_tree(tree_hash)?;
                    let Some(entry) = tree.entries.into_iter().find(|entry| entry.name == name)
                    else {
                        return Ok(None);
                    };
                    SyntheticNode::Frozen {
                        snapshot_id,
                        kind: entry.kind,
                        mode: entry.mode,
                        uid: entry.uid,
                        gid: entry.gid,
                        size: entry.size,
                        mtime_ns: entry.mtime_ns,
                        target: entry.target,
                        object: entry.object,
                        xattrs: entry.xattrs,
                    }
                }
                _ => return Ok(None),
            }
        };
        let key = format!("{parent}/{name}");
        let ino = self.intern_synthetic(key, node.clone());
        Ok(Some((ino, self.synthetic_attr(ino, &node))))
    }

    pub(crate) fn synthetic_entries(
        &self,
        ino: Ino,
    ) -> Result<Vec<(Ino, InodeKind, String)>, Code> {
        let node = self.synthetic_node(ino).ok_or(Code::Stale)?;
        if !self.synthetic_active(&node) {
            return Err(Code::Stale);
        }
        let children: Vec<(String, SyntheticNode)> = match node {
            SyntheticNode::ConstellationOf { dir } => {
                vec![("snapshot".into(), SyntheticNode::SnapshotsOf { dir })]
            }
            SyntheticNode::SnapshotsOf { dir } => self
                .covering(dir)?
                .into_iter()
                .map(|(row, root)| {
                    Ok((
                        row.name,
                        SyntheticNode::Frozen {
                            snapshot_id: row.id,
                            kind: InodeKind::Dir,
                            mode: 0o555,
                            uid: 0,
                            gid: 0,
                            size: 0,
                            mtime_ns: row.created_unix_ms * 1_000_000,
                            target: None,
                            object: Some(root),
                            xattrs: Vec::new(),
                        },
                    ))
                })
                .collect::<Result<_, Code>>()?,
            SyntheticNode::Frozen {
                snapshot_id,
                kind: InodeKind::Dir,
                object: Some(tree_hash),
                ..
            } => self
                .snapshot_tree(tree_hash)?
                .entries
                .into_iter()
                .map(|entry| {
                    (
                        entry.name,
                        SyntheticNode::Frozen {
                            snapshot_id: snapshot_id.clone(),
                            kind: entry.kind,
                            mode: entry.mode,
                            uid: entry.uid,
                            gid: entry.gid,
                            size: entry.size,
                            mtime_ns: entry.mtime_ns,
                            target: entry.target,
                            object: entry.object,
                            xattrs: entry.xattrs,
                        },
                    )
                })
                .collect(),
            _ => return Err(Code::NotDir),
        };
        Ok(children
            .into_iter()
            .map(|(name, node)| {
                let child = self.intern_synthetic(format!("{ino}/{name}"), node.clone());
                let kind = self.synthetic_attr(child, &node).kind;
                (child, kind, name)
            })
            .collect())
    }

    /// The manifest of the frozen file `object`: from the view's cache
    /// ([`frozen_manifests`]), or loaded from the snapshot's tree (under
    /// the watchdog stage `stage`) and kept there.
    pub(super) fn frozen_manifest(
        &self,
        object: &crate::snapshot::FrozenObject,
        stage: &'static str,
    ) -> anyhow::Result<Arc<Manifest>> {
        if let Some(manifest) = self.frozen_manifests.get(object) {
            return Ok(manifest);
        }
        let manifest = Arc::new(self.rt.block_on(async {
            constellation_vfs::watch::stage(stage);
            self.snapshots.load_manifest(object).await
        })?);
        self.frozen_manifests.insert(*object, Arc::clone(&manifest));
        Ok(manifest)
    }

    pub(crate) fn read_frozen(&self, ino: Ino, offset: u64, size: u64) -> Result<Vec<u8>, Code> {
        let node = self.synthetic_node(ino).ok_or(Code::Stale)?;
        if !self.synthetic_active(&node) {
            return Err(Code::Stale);
        }
        let SyntheticNode::Frozen {
            kind: InodeKind::File,
            object: Some(manifest_hash),
            ..
        } = node
        else {
            return Err(Code::IsDir);
        };
        let manifest = self
            .frozen_manifest(&manifest_hash, "snapshot manifest load")
            .map_err(|error| {
                tracing::debug!(%error, ino, "frozen read: manifest load failed");
                Code::Io
            })?;
        let hashes = self.chunk_list(&manifest)?;
        tracing::debug!(
            ino,
            file_len = manifest.file_len,
            chunks = hashes.len(),
            "frozen read: manifest loaded"
        );
        if offset >= manifest.file_len {
            return Ok(Vec::new());
        }
        let len = size.min(manifest.file_len - offset);
        let mut out = Vec::with_capacity(len as usize);
        for slice in manifest.layout.slices(offset, len) {
            let chunk = self
                .read_committed_chunk(ino, &hashes, slice.index)
                .inspect_err(|code| {
                    tracing::debug!(
                        ino,
                        index = slice.index,
                        %code,
                        "frozen read: chunk read failed"
                    )
                })?;
            let start = slice.offset as usize;
            let end = (slice.offset + slice.len) as usize;
            if chunk.len() < end {
                return Err(Code::Io);
            }
            out.extend_from_slice(&chunk[start..end]);
        }
        Ok(out)
    }
}
