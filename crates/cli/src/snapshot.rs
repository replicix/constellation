//! Snapshots, eager clones, and GC-root enumeration (DESIGN.md §13).
//!
//! Since plan 28 a snapshot is **a retained root hash**: `(commit seq,
//! mtree root, dir ino)`. Taking one forces a metadata publish (so the
//! tree reflects everything this node has written under the path) and
//! records where the directory sits in that tree. Nothing else is
//! written — the nodes are already on the bucket, shared structurally
//! with every commit around them — which is what replaced the old
//! `build_tree`, a walk that re-uploaded one `fs-core` `Tree` blob per
//! directory and one manifest blob per file.
//!
//! Reading goes through [`FrozenObject`] — an inode of a retained root —
//! and [`crate::mtree_read::TreeReader`], for the FUSE `.constellation/
//! snapshot` view, `clone_to` and GC alike. Clones deliberately keep the
//! correctness-first eager copy: metadata is inserted in one transaction
//! and one `clone` record while data chunks remain shared.

use crate::mtree_read::{Resolver, TreeReader};
use anyhow::{bail, Context, Result};
use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest};
use constellation_fs_core::{ChunkHash, Ino, InodeKind};
use constellation_meta::{CloneSpec, Meta, MetaStore, SnapshotRow};
use constellation_mtree::NodeHash;
use constellation_store_s3::{
    BlobStore, NodeCache, SnapshotRecord, SnapshotStore, SnapshotTreeRoot, StoreError,
};
use futures::future::BoxFuture;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

/// Force a metadata publish and return the commit that now reflects this
/// node's state: `(seq, root)`. In the daemon it runs a sync round and a
/// publish on the sync task; tests hand in a publisher directly.
pub type PublishHook = Arc<dyn Fn() -> BoxFuture<'static, Result<(u64, NodeHash)>> + Send + Sync>;

/// How to read the metadata tree: the node cache, the tree config (the
/// hasher is part of it), and the blob store for spilled values.
#[derive(Clone)]
pub struct TreeAccess {
    pub nodes: Arc<NodeCache>,
    pub config: constellation_mtree::Config,
    pub blobs: BlobStore,
}

impl TreeAccess {
    pub fn from_reader(reader: crate::mtree_read::ChainReader) -> TreeAccess {
        TreeAccess {
            nodes: reader.cache,
            config: reader.config,
            blobs: reader.blobs,
        }
    }

    fn reader(&self, root: NodeHash) -> Result<TreeReader<Arc<NodeCache>>> {
        Ok(TreeReader::new(
            constellation_mtree::Tree::with_config(self.nodes.clone(), self.config)?,
            root,
        ))
    }

    /// Run a synchronous tree read off the runtime. The node cache
    /// bridges to async I/O with `block_in_place`, which a blocking
    /// thread may do on any runtime flavour.
    async fn read<T, F>(&self, root: NodeHash, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&TreeReader<Arc<NodeCache>>, &Resolver<'_>) -> Result<T> + Send + 'static,
    {
        let reader = self.reader(root)?;
        let blobs = self.blobs.clone();
        let handle = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            let resolver = Resolver {
                blobs: &blobs,
                handle: &handle,
            };
            f(&reader, &resolver)
        })
        .await
        .context("metadata tree read task")?
    }
}

/// What a snapshot row's `root_hash` column names: directory `ino` of
/// metadata tree `root`, published as commit `seq`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotRoot {
    pub seq: u64,
    pub root: NodeHash,
    pub ino: Ino,
}

/// `root_hash` spelling of a [`SnapshotRoot`]: `mtree:<seq>:<root>:<ino>`.
const TREE_ROOT_PREFIX: &str = "mtree:";

impl SnapshotRoot {
    pub fn parse(value: &str) -> Result<SnapshotRoot> {
        let rest = value
            .strip_prefix(TREE_ROOT_PREFIX)
            .with_context(|| format!("snapshot root {value:?} is not a metadata tree root"))?;
        let mut fields = rest.split(':');
        let (Some(seq), Some(root), Some(ino), None) =
            (fields.next(), fields.next(), fields.next(), fields.next())
        else {
            bail!("malformed snapshot root {value:?}");
        };
        Ok(SnapshotRoot {
            seq: seq.parse().context("snapshot commit seq")?,
            root: NodeHash::from_hex(root).context("snapshot tree root")?,
            ino: ino.parse().context("snapshot directory ino")?,
        })
    }

    pub fn encode(&self) -> String {
        format!(
            "{TREE_ROOT_PREFIX}{}:{}:{}",
            self.seq,
            self.root.to_hex(),
            self.ino
        )
    }

    /// The frozen directory this root names.
    pub fn object(&self) -> FrozenObject {
        FrozenObject {
            root: self.root,
            ino: self.ino,
        }
    }

    pub fn of_record(record: &SnapshotRecord) -> Result<SnapshotRoot> {
        Ok(SnapshotRoot {
            seq: record.tree.seq,
            root: NodeHash::from_hex(&record.tree.root).context("snapshot tree root")?,
            ino: record.tree.ino,
        })
    }
}

/// A directory or file inside a snapshot: an inode of a retained tree.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FrozenObject {
    pub root: NodeHash,
    pub ino: Ino,
}

/// One child of a frozen directory.
#[derive(Clone, Debug)]
pub struct FrozenEntry {
    pub name: String,
    pub kind: InodeKind,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub mtime_ns: i64,
    pub target: Option<String>,
    /// Set for directories and regular files.
    pub object: Option<FrozenObject>,
    pub xattrs: Vec<(String, Vec<u8>)>,
}

/// A frozen directory's children and its own xattrs.
#[derive(Clone, Debug, Default)]
pub struct FrozenDir {
    pub entries: Vec<FrozenEntry>,
    pub xattrs: Vec<(String, Vec<u8>)>,
}

#[derive(Clone)]
pub struct SnapshotManager {
    meta: Arc<Meta>,
    chunks: Arc<constellation_store_s3::ChunkStore>,
    records: SnapshotStore,
    chunk_size: u32,
    creator: u64,
    tree: Option<TreeAccess>,
    publish: Option<PublishHook>,
}

impl SnapshotManager {
    pub fn new(
        meta: Arc<Meta>,
        chunks: Arc<constellation_store_s3::ChunkStore>,
        chunk_size: u32,
        creator: u64,
    ) -> Self {
        Self {
            records: SnapshotStore::new(chunks.inner().clone()),
            meta,
            chunks,
            chunk_size,
            creator,
            tree: None,
            publish: None,
        }
    }

    /// Read version-2 snapshots through `access`.
    pub fn with_tree(mut self, access: TreeAccess) -> Self {
        self.tree = Some(access);
        self
    }

    /// Take snapshots by forcing a publish through `hook`. Without one
    /// this manager can read snapshots but not create them.
    pub fn with_publisher(mut self, hook: PublishHook) -> Self {
        self.publish = Some(hook);
        self
    }

    fn tree(&self) -> Result<&TreeAccess> {
        self.tree
            .as_ref()
            .context("this mount has no metadata tree reader")
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
        let publish = self
            .publish
            .as_ref()
            .context("this mount cannot publish a metadata commit, so it cannot take snapshots")?;
        let (seq, root) = publish().await?;
        // The commit must actually hold the directory: a path created
        // after the last shipped segment would otherwise freeze nothing.
        let kind = self
            .tree()?
            .read(root, move |reader, _| {
                Ok(reader.getattr(ino)?.map(|rec| rec.attrs.kind.as_u8()))
            })
            .await?;
        if kind != Some(InodeKind::Dir.as_u8()) {
            bail!(
                "snapshot path {path} (ino {ino}) is not in metadata commit {seq} yet \
                 (found kind {kind:?}, {} journal rows unshipped); retry",
                self.meta.journal_len().unwrap_or(0)
            );
        }
        let snapshot = SnapshotRoot { seq, root, ino };
        let record = SnapshotRecord::new(
            &path,
            name,
            self.creator,
            SnapshotTreeRoot {
                seq,
                root: root.to_hex(),
                ino,
            },
        );
        match self.records.create(&record).await {
            Ok(()) => {}
            Err(StoreError::AlreadyExists) => bail!("snapshot {path}@{name} already exists"),
            Err(error) => return Err(error.into()),
        }
        self.meta.record_snapshot(&SnapshotRow {
            id: record.id(),
            path,
            name: name.to_string(),
            root_hash: snapshot.encode(),
            created_unix_ms: record.created_unix_ms,
        })?;
        Ok(format!(
            "created snapshot {}@{} ({}, metadata commit {seq})",
            record.path,
            record.name,
            record.id()
        ))
    }

    pub fn list(&self, path: Option<&str>) -> Result<Vec<SnapshotRow>> {
        let normalized = path.map(normalize_path);
        Ok(self.meta.snapshots(normalized.as_deref())?)
    }

    /// Snapshots whose frozen subtree contains `directory`, paired with the
    /// frozen object corresponding to that directory.  Component-aware
    /// prefix matching avoids treating `/project-old` as a child of
    /// `/project`.
    pub async fn covering(&self, directory: &str) -> Result<Vec<(SnapshotRow, FrozenObject)>> {
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
            let components: Vec<String> = relative
                .split('/')
                .filter(|part| !part.is_empty())
                .map(str::to_string)
                .collect();
            let SnapshotRoot { root, ino, .. } = SnapshotRoot::parse(&row.root_hash)?;
            let found = self
                .tree()?
                .read(root, move |reader, _| {
                    let mut ino = ino;
                    for component in &components {
                        match reader.lookup(ino, component.as_bytes())? {
                            Some(entry) if entry.attrs.kind.as_u8() == InodeKind::Dir.as_u8() => {
                                ino = entry.ino
                            }
                            _ => return Ok(None),
                        }
                    }
                    Ok(Some(FrozenObject { root, ino }))
                })
                .await?;
            if let Some(object) = found {
                covered.push((row, object));
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

    /// A frozen directory's children, in name order.
    pub async fn list_frozen(&self, dir: &FrozenObject) -> Result<FrozenDir> {
        let FrozenObject { root, ino } = *dir;
        self.tree()?
            .read(root, move |reader, resolver| {
                let xattrs = reader
                    .inode_row(ino, resolver)?
                    .map(|row| row.xattrs)
                    .unwrap_or_default();
                let mut entries = Vec::new();
                for child in reader.readdir(ino, None, usize::MAX)? {
                    let row = reader
                        .inode_row(child.ino, resolver)?
                        .with_context(|| format!("dentry to missing inode {}", child.ino))?;
                    let kind = row.attr.kind;
                    entries.push(FrozenEntry {
                        name: String::from_utf8(child.name)
                            .context("snapshot entry name is not UTF-8")?,
                        kind,
                        mode: row.attr.mode,
                        uid: row.attr.uid,
                        gid: row.attr.gid,
                        size: row.attr.size,
                        mtime_ns: row.attr.mtime_ns,
                        target: row.target,
                        object: matches!(kind, InodeKind::Dir | InodeKind::File).then_some(
                            FrozenObject {
                                root,
                                ino: child.ino,
                            },
                        ),
                        xattrs: row.xattrs,
                    });
                }
                Ok(FrozenDir { entries, xattrs })
            })
            .await
    }

    /// A frozen file's encoded manifest, `None` for a file that never had
    /// content written.
    async fn frozen_manifest_bytes(&self, file: &FrozenObject) -> Result<Option<Vec<u8>>> {
        let FrozenObject { root, ino } = *file;
        self.tree()?
            .read(root, move |reader, resolver| {
                Ok(reader
                    .inode_row(ino, resolver)?
                    .with_context(|| format!("snapshot file {ino} has no inode record"))?
                    .manifest)
            })
            .await
    }

    pub async fn load_manifest(&self, file: &FrozenObject) -> Result<Manifest> {
        match self.frozen_manifest_bytes(file).await? {
            Some(bytes) => Ok(Manifest::decode(&bytes)?),
            None => Ok(Manifest::empty(self.chunk_size)),
        }
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
        let root = SnapshotRoot::parse(&row.root_hash)?;
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
            xattrs: Vec::new(),
        }];
        self.flatten(root.object(), 0, &mut specs).await?;
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

    fn flatten<'a>(
        &'a self,
        dir: FrozenObject,
        parent_index: usize,
        specs: &'a mut Vec<CloneSpec>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let listing = self.list_frozen(&dir).await?;
            specs[parent_index].xattrs = listing.xattrs;
            for entry in listing.entries {
                let manifest = match (entry.kind, entry.object) {
                    (InodeKind::File, Some(object)) => self.frozen_manifest_bytes(&object).await?,
                    (InodeKind::File, None) => bail!("snapshot file has no manifest reference"),
                    _ => None,
                };
                let child_dir = (entry.kind == InodeKind::Dir)
                    .then_some(entry.object)
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
                    xattrs: entry.xattrs,
                });
                if let Some(child_dir) = child_dir {
                    self.flatten(child_dir, index, specs).await?;
                }
            }
            Ok(())
        })
    }

    /// Every chunk a snapshot keeps alive, as hex keys.
    pub async fn refs(&self, id: &str) -> Result<Vec<String>> {
        let record = self
            .records
            .get(id)
            .await?
            .with_context(|| format!("snapshot {id} does not exist"))?;
        let refs = snapshot_chunk_refs(
            &self.chunks,
            self.tree()?,
            &SnapshotRoot::of_record(&record)?,
        )
        .await?;
        let mut hex: Vec<String> = refs.into_iter().map(|hash| hash.to_hex()).collect();
        hex.sort();
        Ok(hex)
    }
}

/// Every chunk object a snapshot references: the data chunks (and
/// spilled chunk lists) of every file under its directory. Chunk GC's
/// snapshot roots, and `snap refs`.
pub async fn snapshot_chunk_refs(
    chunks: &constellation_store_s3::ChunkStore,
    tree: &TreeAccess,
    snapshot: &SnapshotRoot,
) -> Result<HashSet<ChunkHash>> {
    let SnapshotRoot { root, ino, .. } = *snapshot;
    let manifests = tree
        .read(root, move |reader, resolver| {
            let mut manifests = Vec::new();
            let mut stack = vec![ino];
            while let Some(dir) = stack.pop() {
                for child in reader.readdir(dir, None, usize::MAX)? {
                    match child.attrs.kind.as_u8() {
                        k if k == InodeKind::Dir.as_u8() => stack.push(child.ino),
                        k if k == InodeKind::File.as_u8() => {
                            if let Some(bytes) = reader
                                .inode_row(child.ino, resolver)?
                                .and_then(|row| row.manifest)
                            {
                                manifests.push(bytes);
                            }
                        }
                        _ => {}
                    }
                }
            }
            Ok(manifests)
        })
        .await?;
    let mut refs = HashSet::new();
    for bytes in manifests {
        add_manifest_refs(chunks, &Manifest::decode(&bytes)?, &mut refs).await?;
    }
    Ok(refs)
}

async fn add_manifest_refs(
    chunks: &constellation_store_s3::ChunkStore,
    manifest: &Manifest,
    refs: &mut HashSet<ChunkHash>,
) -> Result<()> {
    match &manifest.chunks {
        ChunkInfo::Inline(hashes) => refs.extend(hashes.values().copied()),
        ChunkInfo::Spilled(spill) => {
            refs.insert(*spill);
            refs.extend(decode_chunk_list(&chunks.get_chunk(spill).await?)?.into_values());
        }
    }
    Ok(())
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

/// A manager that can take and read tree snapshots over `chunks`'
/// bucket, for tests: its publisher drains the journal itself, and its
/// node cache runs on a leaked runtime of its own so it works under any
/// caller's runtime flavour and outlives the caller's.
#[cfg(test)]
pub(crate) fn test_manager(
    meta: Arc<Meta>,
    chunks: Arc<constellation_store_s3::ChunkStore>,
    chunk_size: u32,
) -> (SnapshotManager, tempfile::TempDir) {
    use constellation_fs_core::cache::DiskCache;
    use constellation_mtree::{record, Hasher};
    use constellation_store_s3::{CommitChain, PackStore};
    let runtime: &'static tokio::runtime::Runtime = Box::leak(Box::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
    ));
    let handle = runtime.handle().clone();
    let dir = tempfile::TempDir::new().unwrap();
    let backend = chunks.inner().clone();
    let nodes = Arc::new(NodeCache::new(
        PackStore::new(backend.clone()),
        Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap()),
        Hasher::Plain,
        handle.clone(),
    ));
    let access = TreeAccess {
        nodes: nodes.clone(),
        config: record::config(),
        blobs: BlobStore::new(backend.clone(), Hasher::Plain),
    };
    let publisher = Arc::new(tokio::sync::Mutex::new(
        crate::mtree_publish::TreePublisher::new(
            meta.clone(),
            nodes,
            access.blobs.clone(),
            CommitChain::new(backend),
            record::config(),
            1,
            handle.clone(),
        ),
    ));
    let hook: PublishHook = Arc::new(move || {
        let publisher = publisher.clone();
        let handle = handle.clone();
        Box::pin(async move {
            handle
                .spawn(async move { publisher.lock().await.publish_now(1).await })
                .await?
        })
    });
    let manager = SnapshotManager::new(meta, chunks, chunk_size, 1)
        .with_tree(access)
        .with_publisher(hook);
    (manager, dir)
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

    #[test]
    fn snapshot_roots_round_trip_and_refuse_anything_else() {
        let tree = SnapshotRoot {
            seq: 42,
            root: NodeHash([7; 32]),
            ino: 1 << 40 | 9,
        };
        assert_eq!(SnapshotRoot::parse(&tree.encode()).unwrap(), tree);
        assert!(SnapshotRoot::parse(&ChunkHash::of(b"tree blob").to_hex()).is_err());
        assert!(SnapshotRoot::parse("mtree:1:zz:3").is_err());
        assert!(SnapshotRoot::parse("mtree:1:2").is_err());
    }

    /// A snapshot is a retained tree root: later writes to the source and
    /// to a clone of it leave what the snapshot reads unchanged, down to
    /// xattrs and manifests, and GC's view of its chunks is the frozen one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_tree_snapshot_is_frozen_against_source_and_clone_writes() {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let source = meta.mkdir(1, "source", 0o755, 1, 1).unwrap();
        meta.set_xattr(
            source.ino,
            "user.directory",
            b"root",
            constellation_meta::SetXattrMode::Set,
        )
        .unwrap();
        let file = meta.create(source.ino, "file", 0o644, 1, 1).unwrap();
        meta.set_xattr(
            file.ino,
            "user.snapshot",
            b"preserved",
            constellation_meta::SetXattrMode::Set,
        )
        .unwrap();
        let sub = meta.mkdir(source.ino, "sub", 0o700, 1, 1).unwrap();
        meta.symlink(sub.ino, "link", "../file", 1, 1).unwrap();
        let manifest_of = |tag: &[u8]| {
            Manifest::from_chunks(
                DEFAULT_CHUNK_SIZE,
                3,
                vec![ChunkHash::of(tag)],
                constellation_fs_core::INLINE_CHUNKS_MAX,
                ChunkHash::of,
            )
            .0
            .encode()
        };
        let original = manifest_of(b"old");
        meta.set_manifest(file.ino, &original, 3).unwrap();
        let chunks = Arc::new(constellation_store_s3::ChunkStore::new(Arc::new(
            InMemory::new(),
        )));
        let (manager, _nodes) = test_manager(meta.clone(), chunks.clone(), DEFAULT_CHUNK_SIZE);

        let created = manager.create("/source", "before").await.unwrap();
        assert!(created.contains("metadata commit"), "{created}");
        let row = manager.list(Some("/source")).unwrap().remove(0);
        let root = SnapshotRoot::parse(&row.root_hash).unwrap();
        assert_eq!(root.ino, source.ino);

        // Writes after the snapshot, then a clone that is itself written.
        meta.set_manifest(file.ino, &manifest_of(b"newer"), 3)
            .unwrap();
        meta.create(source.ino, "late", 0o644, 1, 1).unwrap();
        manager
            .clone_to("/source", "before", "/copy")
            .await
            .unwrap();
        let clone_root = meta.resolve_path("/copy").unwrap().unwrap();
        let clone_attr = meta.getattr(clone_root).unwrap().unwrap();
        assert_eq!(clone_attr.uid, unsafe { libc::geteuid() });
        assert_eq!(
            meta.get_xattr(clone_root, "user.directory").unwrap(),
            Some(b"root".to_vec())
        );
        assert!(meta.resolve_path("/copy/late").unwrap().is_none());
        assert_eq!(
            meta.readlink(meta.resolve_path("/copy/sub/link").unwrap().unwrap())
                .unwrap()
                .as_deref(),
            Some("../file")
        );
        let clone_file = meta.resolve_path("/copy/file").unwrap().unwrap();
        assert_eq!(meta.manifest(clone_file).unwrap().unwrap(), original);
        meta.set_manifest(clone_file, &manifest_of(b"clone"), 3)
            .unwrap();

        let frozen = manager.list_frozen(&root.object()).await.unwrap();
        assert_eq!(
            frozen.xattrs,
            vec![("user.directory".into(), b"root".to_vec())]
        );
        let names: Vec<&str> = frozen.entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["file", "sub"]);
        assert_eq!(
            frozen.entries[0].xattrs,
            vec![("user.snapshot".into(), b"preserved".to_vec())]
        );
        let object = frozen.entries[0].object.unwrap();
        assert_eq!(
            manager.load_manifest(&object).await.unwrap(),
            Manifest::decode(&original).unwrap()
        );

        // What the FUSE view resolves for a subdirectory of the source.
        let covering = manager.covering("/source/sub").await.unwrap();
        assert_eq!(covering.len(), 1);
        let sub_frozen = manager.list_frozen(&covering[0].1).await.unwrap();
        assert_eq!(sub_frozen.entries[0].target.as_deref(), Some("../file"));

        // GC protects the frozen chunk, not the current ones.
        let refs = snapshot_chunk_refs(&chunks, manager.tree().unwrap(), &root)
            .await
            .unwrap();
        assert!(refs.contains(&ChunkHash::of(b"old")));
        assert!(!refs.contains(&ChunkHash::of(b"newer")));
        assert_eq!(manager.refs(&row.id).await.unwrap().len(), refs.len());
    }
}
