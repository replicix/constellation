//! Snapshot construction, eager clones, and GC-root enumeration.
//!
//! The immutable object model belongs in `fs-core`; this daemon-side module
//! joins the live SQLite replica to the chunk store.  Building performs one
//! indexed query per directory and conditional PUTs every manifest/tree blob.
//! Clones deliberately use the plan's correctness-first eager fallback:
//! metadata is inserted in one transaction and one `clone` record while data
//! chunks remain shared.

use anyhow::{bail, Context, Result};
use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest};
use constellation_fs_core::{ChunkHash, InodeKind, Tree, TreeEntry};
use constellation_meta::{CloneSpec, MetaStore, SnapshotRow, SqliteMeta};
use constellation_store_s3::{CompressionSetting, SnapshotRecord, SnapshotStore, StoreError};
use futures::future::BoxFuture;
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Clone)]
pub struct SnapshotManager {
    meta: Arc<SqliteMeta>,
    chunks: Arc<constellation_store_s3::ChunkStore>,
    records: SnapshotStore,
    compression: CompressionSetting,
    chunk_size: u32,
    creator: u64,
}

impl SnapshotManager {
    pub fn new(
        meta: Arc<SqliteMeta>,
        chunks: Arc<constellation_store_s3::ChunkStore>,
        compression: CompressionSetting,
        chunk_size: u32,
        creator: u64,
    ) -> Self {
        Self {
            records: SnapshotStore::new(chunks.inner().clone()),
            meta,
            chunks,
            compression,
            chunk_size,
            creator,
        }
    }

    pub async fn create(&self, path: &str, name: &str) -> Result<String> {
        validate_name(name)?;
        let path = normalize_path(path);
        let ino = self
            .meta
            .resolve_path(&path)?
            .with_context(|| format!("snapshot path {path} does not exist"))?;
        let attr = self
            .meta
            .getattr(ino)?
            .context("snapshot root disappeared")?;
        if attr.kind != InodeKind::Dir {
            bail!("snapshot path must be a directory");
        }
        let (root, _) = self.build_tree(ino).await?;
        let record = SnapshotRecord::new(&path, name, self.creator, root);
        match self.records.create(&record).await {
            Ok(()) => {}
            Err(StoreError::AlreadyExists) => bail!("snapshot {path}@{name} already exists"),
            Err(error) => return Err(error.into()),
        }
        self.meta.record_snapshot(&SnapshotRow {
            id: record.id(),
            path,
            name: name.to_string(),
            root_hash: root.to_hex(),
            created_unix_ms: record.created_unix_ms,
        })?;
        Ok(format!(
            "created snapshot {}@{} ({})",
            record.path,
            record.name,
            record.id()
        ))
    }

    fn build_tree(&self, ino: u64) -> BoxFuture<'_, Result<(ChunkHash, usize)>> {
        Box::pin(async move {
            let children = self.meta.snapshot_children(ino)?;
            let mut entries = Vec::with_capacity(children.len());
            let mut uploaded = 0usize;
            for child in children {
                let reference = match child.attr.kind {
                    InodeKind::Dir => {
                        let (hash, count) = self.build_tree(child.attr.ino).await?;
                        uploaded += count;
                        Some(hash)
                    }
                    InodeKind::File => {
                        let manifest = child
                            .manifest
                            .unwrap_or_else(|| Manifest::empty(self.chunk_size).encode());
                        let hash = self.chunks.hash(&manifest);
                        if !self
                            .chunks
                            .put_chunk_mode(
                                &hash,
                                &manifest,
                                self.compression,
                                constellation_store_s3::ChunkPutMode::Create,
                            )
                            .await?
                            .existed
                        {
                            uploaded += 1;
                        }
                        Some(hash)
                    }
                    _ => None,
                };
                entries.push(TreeEntry {
                    name: child.name,
                    kind: child.attr.kind,
                    mode: child.attr.mode,
                    uid: child.attr.uid,
                    gid: child.attr.gid,
                    size: child.attr.size,
                    mtime_ns: child.attr.mtime_ns,
                    target: child.target,
                    manifest_or_tree_hash: reference,
                });
            }
            let blob = Tree::new(entries)?.encode();
            let hash = self.chunks.hash(&blob);
            if !self
                .chunks
                .put_chunk_mode(
                    &hash,
                    &blob,
                    self.compression,
                    constellation_store_s3::ChunkPutMode::Create,
                )
                .await?
                .existed
            {
                uploaded += 1;
            }
            Ok((hash, uploaded))
        })
    }

    pub fn list(&self, path: Option<&str>) -> Result<Vec<SnapshotRow>> {
        let normalized = path.map(normalize_path);
        Ok(self.meta.snapshots(normalized.as_deref())?)
    }

    /// Snapshots whose frozen subtree contains `directory`, paired with the
    /// tree object corresponding to that directory.  Component-aware prefix
    /// matching avoids treating `/project-old` as a child of `/project`.
    pub async fn covering(&self, directory: &str) -> Result<Vec<(SnapshotRow, ChunkHash)>> {
        let directory = normalize_path(directory);
        let mut covered = Vec::new();
        for row in self.meta.snapshots(None)? {
            let relative = if directory == row.path {
                Some("")
            } else {
                directory
                    .strip_prefix(&row.path)
                    .and_then(|rest| rest.strip_prefix('/'))
            };
            let Some(relative) = relative else { continue };
            let mut hash = parse_hash(&row.root_hash)?;
            let mut exists = true;
            for component in relative.split('/').filter(|part| !part.is_empty()) {
                let tree = self.load_tree(hash).await?;
                match tree
                    .entries
                    .into_iter()
                    .find(|entry| entry.name == component && entry.kind == InodeKind::Dir)
                    .and_then(|entry| entry.manifest_or_tree_hash)
                {
                    Some(next) => hash = next,
                    None => {
                        exists = false;
                        break;
                    }
                }
            }
            if exists {
                covered.push((row, hash));
            }
        }
        // An exact-path name wins over the same name inherited from an
        // ancestor; otherwise the nearest ancestor wins.
        covered.sort_by_key(|(row, _)| std::cmp::Reverse(row.path.len()));
        let mut names = BTreeSet::new();
        covered.retain(|(row, _)| names.insert(row.name.clone()));
        covered.sort_by(|(a, _), (b, _)| a.name.cmp(&b.name));
        Ok(covered)
    }

    pub async fn delete(&self, path: &str, name: &str) -> Result<String> {
        let path = normalize_path(path);
        if !self.meta.delete_snapshot(&path, name)? {
            bail!("snapshot {path}@{name} does not exist");
        }
        self.records.delete(&path, name).await?;
        Ok(format!("deleted snapshot {path}@{name}"))
    }

    pub async fn clone_to(&self, path: &str, name: &str, destination: &str) -> Result<String> {
        let path = normalize_path(path);
        let row = self
            .meta
            .snapshots(Some(&path))?
            .into_iter()
            .find(|row| row.name == name)
            .with_context(|| format!("snapshot {path}@{name} does not exist"))?;
        let root = parse_hash(&row.root_hash)?;
        let mut specs = vec![CloneSpec {
            parent_index: None,
            name: String::new(),
            kind: InodeKind::Dir,
            mode: 0o755,
            // The synthetic snapshot root has no persisted owner of its own.
            // Make the ordinary writable clone belong to the daemon's mount
            // user; hard-coding root makes root-level clone entries
            // undeletable on an unprivileged mount.
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
            size: 0,
            mtime_ns: row.created_unix_ms * 1_000_000,
            target: None,
            manifest: None,
        }];
        self.flatten_tree(root, 0, &mut specs).await?;
        self.meta.eager_clone(
            &path,
            name,
            &row.root_hash,
            &normalize_path(destination),
            &specs,
        )?;
        Ok(format!(
            "cloned {path}@{name} to {}",
            normalize_path(destination)
        ))
    }

    fn flatten_tree<'a>(
        &'a self,
        hash: ChunkHash,
        parent_index: usize,
        specs: &'a mut Vec<CloneSpec>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let tree = Tree::decode(&self.chunks.get_chunk(&hash).await?)?;
            for entry in tree.entries {
                let manifest = if entry.kind == InodeKind::File {
                    let hash = entry
                        .manifest_or_tree_hash
                        .context("snapshot file has no manifest reference")?;
                    Some(self.chunks.get_chunk(&hash).await?)
                } else {
                    None
                };
                let child_tree = (entry.kind == InodeKind::Dir)
                    .then_some(entry.manifest_or_tree_hash)
                    .flatten();
                let index = specs.len();
                specs.push(CloneSpec {
                    parent_index: Some(parent_index),
                    name: entry.name,
                    kind: entry.kind,
                    mode: entry.mode,
                    uid: entry.uid,
                    gid: entry.gid,
                    size: entry.size,
                    mtime_ns: entry.mtime_ns,
                    target: entry.target,
                    manifest,
                });
                if let Some(child_tree) = child_tree {
                    self.flatten_tree(child_tree, index, specs).await?;
                }
            }
            Ok(())
        })
    }

    pub async fn refs(&self, id: &str) -> Result<Vec<String>> {
        let record = self
            .records
            .get(id)
            .await?
            .with_context(|| format!("snapshot {id} does not exist"))?;
        let mut refs = BTreeSet::new();
        self.walk_refs(record.root, &mut refs).await?;
        Ok(refs.into_iter().collect())
    }

    fn walk_refs<'a>(
        &'a self,
        tree_hash: ChunkHash,
        refs: &'a mut BTreeSet<String>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            refs.insert(tree_hash.to_hex());
            let tree = Tree::decode(&self.chunks.get_chunk(&tree_hash).await?)?;
            for entry in tree.entries {
                let Some(hash) = entry.manifest_or_tree_hash else {
                    continue;
                };
                if entry.kind == InodeKind::Dir {
                    self.walk_refs(hash, refs).await?;
                } else if entry.kind == InodeKind::File {
                    refs.insert(hash.to_hex());
                    let manifest = Manifest::decode(&self.chunks.get_chunk(&hash).await?)?;
                    let chunks = match manifest.chunks {
                        ChunkInfo::Inline(chunks) => chunks,
                        ChunkInfo::Spilled(spill) => {
                            refs.insert(spill.to_hex());
                            decode_chunk_list(&self.chunks.get_chunk(&spill).await?)?
                        }
                    };
                    refs.extend(chunks.into_values().map(|hash| hash.to_hex()));
                }
            }
            Ok(())
        })
    }

    pub async fn load_tree(&self, hash: ChunkHash) -> Result<Tree> {
        Ok(Tree::decode(&self.chunks.get_chunk(&hash).await?)?)
    }

    pub async fn load_manifest(&self, hash: ChunkHash) -> Result<Manifest> {
        Ok(Manifest::decode(&self.chunks.get_chunk(&hash).await?)?)
    }
}

pub fn split_selector(selector: &str) -> Result<(String, String)> {
    let (path, name) = selector
        .rsplit_once('@')
        .with_context(|| format!("snapshot selector {selector:?} must be <path>@<name>"))?;
    validate_name(name)?;
    Ok((normalize_path(path), name.to_string()))
}

pub fn normalize_path(path: &str) -> String {
    let parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    format!("/{}", parts.join("/"))
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.contains('/') || name.contains('@') {
        bail!("snapshot name must be non-empty and contain neither '/' nor '@'");
    }
    Ok(())
}

pub fn parse_hash(hex: &str) -> Result<ChunkHash> {
    if hex.len() != 64 {
        bail!("invalid 32-byte hash {hex:?}");
    }
    let mut bytes = [0u8; 32];
    for (index, pair) in hex.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        bytes[index] = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(ChunkHash(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_fs_core::DEFAULT_CHUNK_SIZE;
    use object_store::memory::InMemory;

    #[test]
    fn selector_uses_the_last_at_sign() {
        assert_eq!(
            split_selector("/projects@friday").unwrap(),
            ("/projects".into(), "friday".into())
        );
        assert!(split_selector("/projects").is_err());
    }

    #[tokio::test]
    async fn eager_clone_write_cannot_mutate_snapshot_tree() {
        let meta = Arc::new(SqliteMeta::open_in_memory().unwrap());
        let source = meta.mkdir(1, "source", 0o755, 1, 1).unwrap();
        let file = meta.create(source.ino, "file", 0o644, 1, 1).unwrap();
        let original = Manifest::from_chunks(
            DEFAULT_CHUNK_SIZE,
            3,
            vec![ChunkHash::of(b"old")],
            constellation_fs_core::INLINE_CHUNKS_MAX,
        )
        .0
        .encode();
        meta.set_manifest(file.ino, &original, 3).unwrap();
        let chunks = Arc::new(constellation_store_s3::ChunkStore::new(Arc::new(
            InMemory::new(),
        )));
        let manager = SnapshotManager::new(
            meta.clone(),
            chunks.clone(),
            CompressionSetting::RAW,
            DEFAULT_CHUNK_SIZE,
            1,
        );
        manager.create("/source", "before").await.unwrap();
        let row = manager.list(Some("/source")).unwrap().remove(0);
        let root = parse_hash(&row.root_hash).unwrap();
        let frozen_before = chunks.get_chunk(&root).await.unwrap();

        manager
            .clone_to("/source", "before", "/copy")
            .await
            .unwrap();
        let clone_root = meta.resolve_path("/copy").unwrap().unwrap();
        let clone_attr = meta.getattr(clone_root).unwrap().unwrap();
        assert_eq!(clone_attr.uid, unsafe { libc::geteuid() });
        assert_eq!(clone_attr.gid, unsafe { libc::getegid() });
        let clone_file = meta.resolve_path("/copy/file").unwrap().unwrap();
        let changed = Manifest::from_chunks(
            DEFAULT_CHUNK_SIZE,
            3,
            vec![ChunkHash::of(b"new")],
            constellation_fs_core::INLINE_CHUNKS_MAX,
        )
        .0
        .encode();
        meta.set_manifest(clone_file, &changed, 3).unwrap();

        assert_eq!(chunks.get_chunk(&root).await.unwrap(), frozen_before);
        let tree = manager.load_tree(root).await.unwrap();
        let manifest_hash = tree.entries[0].manifest_or_tree_hash.unwrap();
        assert_eq!(
            manager.load_manifest(manifest_hash).await.unwrap(),
            Manifest::decode(&original).unwrap()
        );
        assert_eq!(meta.manifest(file.ino).unwrap().unwrap(), original);
        assert_eq!(meta.manifest(clone_file).unwrap().unwrap(), changed);
    }
}
