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
use std::collections::{BTreeSet, HashMap, HashSet};
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
    pub(crate) fn from_reader(reader: crate::mtree_read::ChainReader) -> TreeAccess {
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

/// As `(root hex, ino)`: a session handover carries the synthetic nodes
/// that name frozen objects (`view::handoff`).
impl serde::Serialize for FrozenObject {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&(self.root.to_hex(), self.ino), s)
    }
}

impl<'de> serde::Deserialize<'de> for FrozenObject {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let (root, ino): (String, Ino) = serde::Deserialize::deserialize(d)?;
        let root = NodeHash::from_hex(&root)
            .ok_or_else(|| serde::de::Error::custom("a frozen object's root is not a node hash"))?;
        Ok(FrozenObject { root, ino })
    }
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
    /// Whose clone a writable clone is (the engine's process identity):
    /// the engine's injected host; the process-wide native one for a
    /// manager built without an engine (one-shot commands, tests).
    process: Option<Arc<dyn constellation_platform::Process>>,
}

/// Plan 32 §0.4: what a creator sets beyond `path@name`. The default is
/// today's snapshot: manual, owned by no policy, not held.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotOptions {
    /// 0 manual, 1 policy-created (plan 32's scheduler writes 1).
    pub origin: u8,
    /// The directory inode carrying the owning policy; 0 for none.
    pub policy_ino: u64,
    /// Take it already held, so it is never briefly unheld.
    pub held: bool,
    /// The hold's owner namespace (`user:<name>`, `csi:<uid>`).
    pub held_by: Option<String>,
}

impl SnapshotOptions {
    /// The owner, normalized: an empty string is no owner.
    pub fn owner(&self) -> Option<&str> {
        self.held_by.as_deref().filter(|by| !by.is_empty())
    }
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
            process: None,
        }
    }

    /// Take the process identity from the engine's `host`.
    pub fn with_host(mut self, host: &constellation_platform::HostServices) -> Self {
        self.process = Some(host.process.clone());
        self
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

    pub(crate) fn tree(&self) -> Result<&TreeAccess> {
        self.tree
            .as_ref()
            .context("this mount has no metadata tree reader")
    }

    pub async fn create(&self, path: &str, name: &str) -> Result<String> {
        self.create_with(path, name, &SnapshotOptions::default())
            .await
            .map(|(detail, _)| detail)
    }

    /// Take a snapshot, with plan 32 §0.4's extensions: where it came
    /// from, which policy owns it, and whether it is born held (plan 37's
    /// `snapshot.create{hold}`). Returns the summary line and the row as
    /// recorded, so a caller need not look it up again.
    ///
    /// This is the whole operation on whichever node calls it, with no
    /// regard for who holds the write lease: the daemon goes through
    /// [`crate::snapshot_batch`], which runs the same pieces
    /// ([`Self::prepare_create`], [`Self::publish_commit`],
    /// [`Self::put_record`], then the row) at the root-lease holder.
    pub async fn create_with(
        &self,
        path: &str,
        name: &str,
        options: &SnapshotOptions,
    ) -> Result<(String, SnapshotRow)> {
        let (path, ino) = self.prepare_create(path, name, options)?;
        let commit = self.publish_commit().await?;
        let row = match self
            .put_record(&path, name, options, self.creator, ino, commit)
            .await?
        {
            PutRecord::Put(row) => row,
            PutRecord::AlreadyExists(_) => bail!("snapshot {path}@{name} already exists"),
        };
        self.meta.record_snapshot(&row)?;
        Ok((created_detail(&row, commit.0), row))
    }

    /// Validate a creation and resolve its directory: the normalized
    /// path and the directory's inode, as this replica has them.
    pub fn prepare_create(
        &self,
        path: &str,
        name: &str,
        options: &SnapshotOptions,
    ) -> Result<(String, Ino)> {
        validate_name(name)?;
        if let Some(by) = options.owner() {
            validate_owner(by)?;
        }
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
        Ok((path, ino))
    }

    /// Force a metadata publish: the commit `(seq, root)` that now
    /// reflects this node's replica, which a snapshot taken now retains.
    pub async fn publish_commit(&self) -> Result<(u64, NodeHash)> {
        let publish = self
            .publish
            .as_ref()
            .context("this mount cannot publish a metadata commit, so it cannot take snapshots")?;
        publish().await
    }

    /// Write `path@name`'s bucket object for directory `ino` of `commit`
    /// (a create-if-absent: the name is the CAS), and return the row to
    /// record — which the caller journals, under whatever write
    /// admission it holds. [`PutRecord::AlreadyExists`] when the object
    /// is already there; nothing is written then.
    pub async fn put_record(
        &self,
        path: &str,
        name: &str,
        options: &SnapshotOptions,
        creator: u64,
        ino: Ino,
        commit: (u64, NodeHash),
    ) -> Result<PutRecord> {
        let (seq, root) = commit;
        tracing::debug!(%path, name, seq, root = %root.to_hex(), ino, "snapshot: publish returned");
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
        // REFER (plan 32 §6.1, plan 37 §16's `size_bytes`): the local
        // replica's DFS over the live subtree, taken once, here. It is a
        // replica-local read (never an S3 walk); if it fails — the
        // directory went away under us — the snapshot is still worth
        // taking, with no size.
        let refer_bytes = match self.meta.recursive_size(ino) {
            Ok((bytes, _files)) => Some(bytes),
            Err(error) => {
                tracing::debug!(%path, name, %error, "snapshot: no REFER for this snapshot");
                None
            }
        };
        let record = SnapshotRecord::new(
            path,
            name,
            creator,
            SnapshotTreeRoot {
                seq,
                root: root.to_hex(),
                ino,
            },
        )
        // The hold is not part of the bucket object (plan 32 §0.4: it
        // lives only in the row, where releasing it can be recorded).
        .with_extensions(options.origin, options.policy_ino, refer_bytes);
        match self.records.create(&record).await {
            Ok(()) => {}
            Err(StoreError::AlreadyExists) => return Ok(PutRecord::AlreadyExists(record.id())),
            Err(error) => return Err(error.into()),
        }
        Ok(PutRecord::Put(SnapshotRow {
            id: record.id(),
            path: path.to_string(),
            name: name.to_string(),
            root_hash: snapshot.encode(),
            created_unix_ms: record.created_unix_ms,
            origin: options.origin,
            policy_ino: options.policy_ino,
            held: options.held,
            creator,
            held_by: options.owner().map(str::to_string),
            refer_bytes,
        }))
    }

    /// Remove `path@name`'s bucket object (after its row is gone: a
    /// failure in between leaves an orphan object, never a dangling row).
    pub async fn delete_record(&self, path: &str, name: &str) -> Result<()> {
        self.records.delete(path, name).await?;
        Ok(())
    }

    /// Plan 32 §0.4: set or release `target`'s retention hold. `target` is
    /// a snapshot id or a `path@name` selector.
    ///
    /// Ownership is by metadata (plan 32 L6): a hold recorded under an
    /// owner may only be released by that same owner, and may not be
    /// silently taken over by another. `force` overrides both — the
    /// control layer restricts who may ask for it. The comparison itself
    /// is [`Meta::set_snapshot_hold`]'s, made inside the transaction that
    /// writes the result; checking it here against a separate read would
    /// let two concurrent holders both pass.
    pub async fn hold(
        &self,
        target: &str,
        held: bool,
        by: Option<&str>,
        force: bool,
    ) -> Result<(String, SnapshotRow)> {
        let by = by.filter(|by| !by.is_empty());
        if let Some(by) = by {
            validate_owner(by)?;
        }
        let id = snapshot_id_of(target)?;
        let row = self
            .meta
            .set_snapshot_hold(&id, held, by, force)?
            .with_context(|| format!("no such snapshot: {target}"))?;
        Ok((hold_detail(&row), row))
    }

    /// The row for a snapshot id or a `path@name` selector.
    pub fn row(&self, target: &str) -> Result<SnapshotRow> {
        let id = snapshot_id_of(target)?;
        self.meta
            .snapshot_by_id(&id)?
            .with_context(|| format!("no such snapshot: {target}"))
    }

    pub fn list(&self, path: Option<&str>) -> Result<Vec<SnapshotRow>> {
        let normalized = path.map(normalize_path);
        Ok(self.meta.snapshots(normalized.as_deref())?)
    }

    /// Snapshots whose frozen subtree contains the directory `directory`
    /// (an inode), paired with the frozen object corresponding to that
    /// directory: what `<directory>/.constellation/snapshot/` lists.
    ///
    /// The directory is named by inode, and its current path and
    /// ancestors are read from the replica on every call, so a listing
    /// follows the directory through renames; a directory no longer
    /// linked into the tree (removed while a listing of it was open)
    /// lists nothing.
    ///
    /// A snapshot covers `directory` in one of two ways:
    ///
    /// - **by identity** (plan 32 §0.5): its `SnapshotRoot.ino` is the
    ///   current inode of `directory` or of one of its ancestors. A renamed
    ///   directory keeps its whole history this way, whatever path the
    ///   snapshots were taken under.
    /// - **by path** (the pre-plan-32 rule, kept): its recorded path is
    ///   `directory` or a component-aware ancestor of it (`/project-old`
    ///   is not under `/project`). This is the only way a snapshot of a
    ///   directory that was since *replaced* — removed, and a new one
    ///   made at the same path — still shows: under the new directory, as
    ///   it always has. A snapshot matching both ways is an identity match.
    ///
    /// Either way the relative path from the matched ancestor down to
    /// `directory` must still name a directory inside the frozen tree.
    ///
    /// One name shows once. Precedence, first wins:
    /// 1. the nearest match: a snapshot of `directory` itself beats an
    ///    ancestor's snapshot of the same name (as before);
    /// 2. at the same distance, an identity match whose recorded path is
    ///    still the directory's current path, then an identity match taken
    ///    under an old name (renamed), then a path-only match (a replaced
    ///    directory's) — the directory's own history beats a predecessor's
    ///    that merely shared the path;
    /// 3. the newer snapshot (`created_unix_ms`), then the id, so the
    ///    choice never depends on row order.
    ///
    /// A row whose root does not parse is skipped with a warning: it
    /// cannot be listed, and it must not take every other listing down.
    pub async fn covering(&self, directory: Ino) -> Result<Vec<(SnapshotRow, FrozenObject)>> {
        let Some(chain) = self.meta.ancestry(directory)? else {
            return Ok(Vec::new());
        };
        let components: Vec<String> = chain.iter().skip(1).map(|(_, name)| name.clone()).collect();
        let directory = format!("/{}", components.join("/"));
        // The directory and its ancestors, by inode: where each sits now,
        // and how many components of `directory` it spans.
        let live: HashMap<Ino, (String, usize)> = chain
            .iter()
            .enumerate()
            .map(|(depth, (ino, _))| (*ino, (format!("/{}", components[..depth].join("/")), depth)))
            .collect();
        let mut covered = Vec::new();
        for row in self.meta.snapshots(None)? {
            let snapshot = match SnapshotRoot::parse(&row.root_hash) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    tracing::warn!(id = %row.id, path = %row.path, name = %row.name, %error,
                        "snapshot listing: a snapshot row with an unreadable root; skipped");
                    continue;
                }
            };
            let matched = match live.get(&snapshot.ino) {
                Some((current, depth)) => {
                    let rank = if *current == row.path { 0u8 } else { 1 };
                    Some((*depth, rank))
                }
                None => path_depth(&row.path, &directory).map(|depth| (depth, 2)),
            };
            let Some((depth, rank)) = matched else {
                continue;
            };
            let relative = components[depth..].to_vec();
            let SnapshotRoot { root, ino, .. } = snapshot;
            let found = self
                .tree()?
                .read(root, move |reader, _| {
                    let mut ino = ino;
                    for component in &relative {
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
                let distance = components.len() - depth;
                covered.push(((distance, rank), row, object));
            }
        }
        covered.sort_by(|(a, ra, _), (b, rb, _)| {
            a.cmp(b)
                .then(rb.created_unix_ms.cmp(&ra.created_unix_ms))
                .then(ra.id.cmp(&rb.id))
        });
        let mut names = BTreeSet::new();
        let mut covered: Vec<(SnapshotRow, FrozenObject)> = covered
            .into_iter()
            .filter(|(_, row, _)| names.insert(row.name.clone()))
            .map(|(_, row, object)| (row, object))
            .collect();
        covered.sort_by(|(a, _), (b, _)| a.name.cmp(&b.name));
        Ok(covered)
    }

    /// A frozen directory's children, in name order.
    pub async fn list_frozen(&self, dir: &FrozenObject) -> Result<FrozenDir> {
        let FrozenObject { root, ino } = *dir;
        tracing::debug!(root = %root.to_hex(), ino, "snapshot: listing a frozen directory");
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
                tracing::debug!(
                    ino,
                    entries = ?entries.iter().map(|e| (e.name.as_str(), e.kind)).collect::<Vec<_>>(),
                    "snapshot: frozen directory listed"
                );
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

    pub async fn delete(&self, path: &str, name: &str, force: bool) -> Result<String> {
        let path = normalize_path(path);
        let id = constellation_store_s3::snapshot_id(&path, name);
        // A held snapshot is never deleted out from under its owner
        // (plan 32 Step 5, plan 37's CSI driver): release it first.
        if !force {
            if let Some(row) = self.meta.snapshot_by_id(&id)? {
                if row.held {
                    bail!("{}", held_refusal(&row));
                }
            }
        }
        // Row first, then the object: an interruption between the two
        // leaves an orphan object (never a dangling row), which GC's
        // reconciliation deletes once it is older than `gc.horizon`.
        if !self.meta.delete_snapshot_by_id(&id)? {
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
        let (uid, gid) = match &self.process {
            Some(process) => process.effective_ids(),
            None => constellation_platform::native().process.effective_ids(),
        };
        let mut specs = vec![CloneSpec {
            parent_index: None,
            name: String::new(),
            kind: InodeKind::Dir,
            mode: 0o755,
            // The synthetic snapshot root has no persisted owner of its own.
            // Make the ordinary writable clone belong to the daemon's mount
            // user; hard-coding root makes root-level clone entries
            // undeletable on an unprivileged mount.
            uid,
            gid,
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

/// What [`SnapshotManager::put_record`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutRecord {
    /// The bucket object is written; journal this row.
    Put(SnapshotRow),
    /// The name is taken: this id's object already exists.
    AlreadyExists(String),
}

/// `snapshot create`'s summary line for a recorded row.
pub fn created_detail(row: &SnapshotRow, seq: u64) -> String {
    format!(
        "created snapshot {}@{} ({}, metadata commit {seq}){}",
        row.path,
        row.name,
        row.id,
        match (row.held, row.owner()) {
            (true, Some(by)) => format!(", held by {by}"),
            (true, None) => ", held".to_string(),
            (false, _) => String::new(),
        }
    )
}

/// `snapshot hold`/`release`'s summary line for the row as now recorded.
pub fn hold_detail(row: &SnapshotRow) -> String {
    match (row.held, row.owner()) {
        (true, Some(by)) => format!("held snapshot {}@{} for {by}", row.path, row.name),
        (true, None) => format!("held snapshot {}@{}", row.path, row.name),
        (false, _) => format!("released snapshot {}@{}", row.path, row.name),
    }
}

/// Why a delete without `force` refuses a held snapshot.
pub fn held_refusal(row: &SnapshotRow) -> String {
    format!(
        "snapshot {}@{} is held{}; release it first (`snapshot release`) or pass --force",
        row.path,
        row.name,
        match row.owner() {
            Some(by) => format!(" by {by}"),
            None => String::new(),
        }
    )
}

pub fn split_selector(selector: &str) -> Result<(String, String)> {
    let (path, name) = selector
        .rsplit_once('@')
        .with_context(|| format!("snapshot selector {selector:?} must be <path>@<name>"))?;
    validate_name(name)?;
    Ok((normalize_path(path), name.to_string()))
}

/// The snapshot id a control caller named: a `path@name` selector hashed
/// the way creation hashed it, or an id passed through verbatim (plan 37's
/// `DeleteSnapshot` has only the id).
pub fn snapshot_id_of(target: &str) -> Result<String> {
    match target.contains('@') {
        true => {
            let (path, name) = split_selector(target)?;
            Ok(constellation_store_s3::snapshot_id(&path, &name))
        }
        false => Ok(target.to_string()),
    }
}

/// How many leading components of `directory` (both normalized) the
/// snapshot path `path` spans, when `path` is `directory` or a
/// component-aware ancestor of it; `None` otherwise.
fn path_depth(path: &str, directory: &str) -> Option<usize> {
    let depth = path.split('/').filter(|part| !part.is_empty()).count();
    let covers = path == directory
        || path == "/"
        || directory
            .strip_prefix(path)
            .is_some_and(|rest| rest.starts_with('/'));
    covers.then_some(depth)
}

pub fn normalize_path(path: &str) -> String {
    let parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    format!("/{}", parts.join("/"))
}

/// Plan 32 §0.4: the owner namespaces a hold may be recorded under.
/// `policy:` is reserved for plan 32's own scheduler and refused here;
/// an unprefixed value is refused so a typo never becomes a namespace.
/// An empty owner is not an owner at all — callers filter it out before
/// asking.
pub fn validate_owner(by: &str) -> Result<()> {
    let (namespace, rest) = by.split_once(':').with_context(|| {
        format!("hold owner {by:?} needs a namespace: `user:<name>` or `csi:<id>`")
    })?;
    match namespace {
        "user" | "csi" if !rest.is_empty() => Ok(()),
        "user" | "csi" => bail!("hold owner {by:?} has an empty {namespace} name"),
        "policy" => bail!("the `policy:` hold namespace is reserved for snapshot policies"),
        other => bail!("unknown hold owner namespace {other:?}: use `user:` or `csi:`"),
    }
}

pub fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.contains('/') || name.contains('@') {
        bail!("snapshot name must be non-empty and contain neither '/' nor '@'");
    }
    Ok(())
}

/// A manager that can take and read tree snapshots over `chunks`'
/// bucket, for tests: its publisher drains the journal itself, and its
/// node cache runs on a leaked runtime of its own so it works under any
/// caller's runtime flavour and outlives the caller's.
#[cfg(any(test, feature = "test-util"))]
pub fn test_manager(
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

    /// Plan 30 §M3b: `publish_now` refuses while the journal is non-empty
    /// (`SPECULATION_OUTSTANDING`) so it never publishes speculation.
    /// This test's bare `Meta` has no shipper acking it, so simulate one
    /// ship of everything journaled so far under `segment`, exactly as
    /// production does when a segment lands.
    fn ship_all(meta: &Meta, segment: u64) {
        let rows = meta.take_journal(usize::MAX).unwrap();
        let seqs: Vec<u64> = rows.iter().map(|(s, _)| *s).collect();
        meta.ack_journal_rows_at(&seqs, segment).unwrap();
    }

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

    #[test]
    fn hold_owners_need_a_known_namespace() {
        validate_owner("user:attila").unwrap();
        validate_owner("csi:0f3a-content-uid").unwrap();
        // Reserved for plan 32's own scheduler, so nobody squats it.
        assert!(validate_owner("policy:7").is_err());
        // A typo must not become a namespace of its own.
        assert!(validate_owner("attila").is_err());
        assert!(validate_owner("users:attila").is_err());
        assert!(validate_owner("user:").is_err());
    }

    /// Plan 32 §0.4 end to end over a real manager: a snapshot taken held
    /// records its owner, only that owner may release it, and until it is
    /// released nothing deletes it — while GC keeps protecting its chunks,
    /// which is the property a hold exists to guarantee.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_held_snapshot_keeps_its_owner_its_chunks_and_its_life() {
        let meta = Arc::new(crate::mtree_publish::test_meta());
        let dir = meta.mkdir(1, "vol", 0o755, 1, 1).unwrap();
        let file = meta.create(dir.ino, "data", 0o644, 1, 1).unwrap();
        let manifest = Manifest::from_chunks(
            DEFAULT_CHUNK_SIZE,
            9,
            vec![ChunkHash::of(b"held-chunk")],
            constellation_fs_core::INLINE_CHUNKS_MAX,
            ChunkHash::of,
        )
        .0
        .encode();
        meta.set_manifest(file.ino, &manifest, 9).unwrap();
        let chunks = Arc::new(constellation_store_s3::ChunkStore::new(Arc::new(
            InMemory::new(),
        )));
        let (manager, _nodes) = test_manager(meta.clone(), chunks.clone(), DEFAULT_CHUNK_SIZE);
        ship_all(&meta, 1);

        // Plan 37's `CreateSnapshot`: created held, owned by the
        // VolumeSnapshotContent uid, with REFER filled in.
        let options = SnapshotOptions {
            held: true,
            held_by: Some("csi:content-uid".into()),
            ..Default::default()
        };
        let (detail, row) = manager
            .create_with("/vol", "pvc-1", &options)
            .await
            .unwrap();
        assert!(detail.contains("held by csi:content-uid"), "{detail}");
        assert!(row.held);
        assert_eq!(row.owner(), Some("csi:content-uid"));
        assert_eq!(row.creator, 1);
        assert_eq!(row.origin, 0, "a CSI snapshot is manual, never a policy's");
        // REFER is the subtree's logical size at creation, from the
        // replica's own DFS — the one file's 9 bytes.
        assert_eq!(row.refer_bytes, Some(9));
        // …and the same numbers came back through the listing.
        let listed = manager.list(Some("/vol")).unwrap().remove(0);
        assert_eq!(listed, row);

        // An unrelated `snapshot delete` cannot take it out from under the
        // driver, and the refusal names the owner.
        let refused = manager
            .delete("/vol", "pvc-1", false)
            .await
            .expect_err("a held snapshot was deleted");
        let refused = format!("{refused:#}");
        assert!(refused.contains("csi:content-uid"), "{refused}");
        assert!(manager.list(Some("/vol")).unwrap().len() == 1);

        // Nor can another owner release or steal the hold.
        for by in [None, Some("user:attila")] {
            let refused = manager
                .hold("/vol@pvc-1", false, by, false)
                .await
                .expect_err("a foreign owner released the hold");
            assert!(
                format!("{refused:#}").contains("csi:content-uid"),
                "{refused:#}"
            );
        }
        assert!(manager.row("/vol@pvc-1").unwrap().held);

        // The hold is what protects it, and GC still sees its chunks: the
        // only pruning path a snapshot has today is an explicit delete, and
        // that is exactly what the hold refuses.
        let root = SnapshotRoot::parse(&row.root_hash).unwrap();
        let refs = snapshot_chunk_refs(&chunks, manager.tree().unwrap(), &root)
            .await
            .unwrap();
        assert!(refs.contains(&ChunkHash::of(b"held-chunk")));

        // Its own owner releases it — by snapshot id, as plan 37's
        // `DeleteSnapshot` does — and then it deletes.
        let (detail, released) = manager
            .hold(&row.id, false, Some("csi:content-uid"), false)
            .await
            .unwrap();
        assert!(
            detail.starts_with("released snapshot /vol@pvc-1"),
            "{detail}"
        );
        assert!(!released.held);
        assert_eq!(released.owner(), None);
        manager.delete("/vol", "pvc-1", false).await.unwrap();
        assert!(manager.list(Some("/vol")).unwrap().is_empty());
    }

    /// `--force` is the admin's override of both rules (the control layer
    /// is what restricts it to admins), and a plain hold — no `--by`, the
    /// pre-plan-32 behavior — is released by a plain release.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn force_overrides_the_owner_and_a_plain_hold_needs_no_owner() {
        let meta = Arc::new(crate::mtree_publish::test_meta());
        meta.mkdir(1, "vol", 0o755, 1, 1).unwrap();
        let chunks = Arc::new(constellation_store_s3::ChunkStore::new(Arc::new(
            InMemory::new(),
        )));
        let (manager, _nodes) = test_manager(meta.clone(), chunks.clone(), DEFAULT_CHUNK_SIZE);
        ship_all(&meta, 1);
        manager.create("/vol", "plain").await.unwrap();
        // The first snapshot's own row is journaled: a publish refuses
        // while anything is unshipped, so ship it before the next one.
        ship_all(&meta, 2);
        manager.create("/vol", "owned").await.unwrap();

        // A hold with no owner: held, releasable by anyone allowed to call.
        let (_, row) = manager.hold("/vol@plain", true, None, false).await.unwrap();
        assert!(row.held);
        assert_eq!(row.owner(), None);
        let (_, row) = manager
            .hold("/vol@plain", false, None, false)
            .await
            .unwrap();
        assert!(!row.held);

        // A hold with an owner cannot be taken over…
        manager
            .hold("/vol@owned", true, Some("user:attila"), false)
            .await
            .unwrap();
        assert!(manager
            .hold("/vol@owned", true, Some("csi:x"), false)
            .await
            .is_err());
        // …unless forced, which both re-owns it and, forced again,
        // releases it.
        let (_, row) = manager
            .hold("/vol@owned", true, Some("csi:x"), true)
            .await
            .unwrap();
        assert_eq!(row.owner(), Some("csi:x"));
        let (_, row) = manager.hold("/vol@owned", false, None, true).await.unwrap();
        assert!(!row.held);
        // A forced delete does not even ask about the hold.
        manager
            .hold("/vol@owned", true, Some("user:attila"), false)
            .await
            .unwrap();
        manager.delete("/vol", "owned", true).await.unwrap();
        assert!(manager
            .list(Some("/vol"))
            .unwrap()
            .iter()
            .all(|r| r.name != "owned"));

        // A namespace-less or reserved owner is refused before anything is
        // written.
        assert!(manager
            .hold("/vol@plain", true, Some("attila"), false)
            .await
            .is_err());
        assert!(manager
            .hold("/vol@plain", true, Some("policy:1"), false)
            .await
            .is_err());
        assert!(!manager.row("/vol@plain").unwrap().held);
        // A snapshot that does not exist is a refusal, not a silent no-op.
        assert!(manager.hold("/vol@nope", true, None, false).await.is_err());
    }

    /// A snapshot is a retained tree root: later writes to the source and
    /// to a clone of it leave what the snapshot reads unchanged, down to
    /// xattrs and manifests, and GC's view of its chunks is the frozen one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_tree_snapshot_is_frozen_against_source_and_clone_writes() {
        let meta = Arc::new(crate::mtree_publish::test_meta());
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

        ship_all(&meta, 1);
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
        assert_eq!(
            clone_attr.uid,
            constellation_platform::native().process.effective_ids().0
        );
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
        let covering = covering_at(&manager, "/source/sub").await;
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

    /// `covering` of the directory at `dir` now.
    async fn covering_at(manager: &SnapshotManager, dir: &str) -> Vec<(SnapshotRow, FrozenObject)> {
        let ino = manager
            .meta
            .resolve_path(dir)
            .unwrap()
            .expect("no such directory");
        manager.covering(ino).await.unwrap()
    }

    /// What `<dir>/.constellation/snapshot` lists, as `(name, path@name)`.
    async fn listed(manager: &SnapshotManager, dir: &str) -> Vec<(String, String)> {
        covering_at(manager, dir)
            .await
            .into_iter()
            .map(|(row, _)| (row.name.clone(), format!("{}@{}", row.path, row.name)))
            .collect()
    }

    /// The names of a frozen directory's entries.
    async fn frozen_names(manager: &SnapshotManager, object: &FrozenObject) -> Vec<String> {
        manager
            .list_frozen(object)
            .await
            .unwrap()
            .entries
            .into_iter()
            .map(|entry| entry.name)
            .collect()
    }

    /// Plan 32 §0.5: a renamed directory still lists its whole history —
    /// itself and its subdirectories — because a snapshot also covers the
    /// directory whose *inode* it froze, not only the path it was taken at.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_renamed_directory_keeps_its_snapshots() {
        let meta = Arc::new(crate::mtree_publish::test_meta());
        let a = meta.mkdir(1, "a", 0o755, 1, 1).unwrap();
        let sub = meta.mkdir(a.ino, "sub", 0o755, 1, 1).unwrap();
        meta.create(sub.ino, "f", 0o644, 1, 1).unwrap();
        let chunks = Arc::new(constellation_store_s3::ChunkStore::new(Arc::new(
            InMemory::new(),
        )));
        let (manager, _nodes) = test_manager(meta.clone(), chunks.clone(), DEFAULT_CHUNK_SIZE);
        // A publish refuses while anything is unshipped, a snapshot's own
        // row included.
        let mut segment = 0;
        let mut ship = || {
            segment += 1;
            ship_all(&meta, segment)
        };
        ship();
        manager.create("/a", "before").await.unwrap();
        ship();
        assert_eq!(
            listed(&manager, "/a").await,
            [("before".into(), "/a@before".into())]
        );

        meta.rename(1, "a", 1, "b").unwrap();
        ship();
        assert_eq!(
            listed(&manager, "/b").await,
            [("before".into(), "/a@before".into())],
            "the renamed directory lost its history"
        );
        let below = covering_at(&manager, "/b/sub").await;
        assert_eq!(below.len(), 1);
        assert_eq!(frozen_names(&manager, &below[0].1).await, ["f"]);

        // A snapshot taken under the new name joins it, and a name taken
        // both before and after the rename shows once: the one recorded
        // under the directory's current path.
        manager.create("/b", "after").await.unwrap();
        ship();
        manager.create("/b", "before").await.unwrap();
        ship();
        assert_eq!(
            listed(&manager, "/b").await,
            [
                ("after".into(), "/b@after".into()),
                ("before".into(), "/b@before".into()),
            ]
        );
        manager.delete("/b", "before", false).await.unwrap();
        ship();
        assert_eq!(
            listed(&manager, "/b").await,
            [
                ("after".into(), "/b@after".into()),
                ("before".into(), "/a@before".into()),
            ]
        );
    }

    /// A row whose root does not parse is skipped, with a warning: it
    /// takes no listing down with it, neither its own directory's nor any
    /// other's.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unreadable_snapshot_row_does_not_break_listings() {
        let meta = Arc::new(crate::mtree_publish::test_meta());
        meta.mkdir(1, "a", 0o755, 1, 1).unwrap();
        meta.mkdir(1, "other", 0o755, 1, 1).unwrap();
        let chunks = Arc::new(constellation_store_s3::ChunkStore::new(Arc::new(
            InMemory::new(),
        )));
        let (manager, _nodes) = test_manager(meta.clone(), chunks.clone(), DEFAULT_CHUNK_SIZE);
        ship_all(&meta, 1);
        manager.create("/a", "good").await.unwrap();
        ship_all(&meta, 2);
        let good = meta.snapshots(None).unwrap().pop().unwrap();
        for (path, name) in [("/a", "bad"), ("/", "bad-root"), ("/other", "bad")] {
            meta.record_snapshot(&SnapshotRow {
                id: constellation_store_s3::snapshot_id(path, name),
                path: path.into(),
                name: name.into(),
                root_hash: "not a tree root".into(),
                ..good.clone()
            })
            .unwrap();
        }
        ship_all(&meta, 3);
        assert_eq!(
            listed(&manager, "/a").await,
            [("good".into(), "/a@good".into())]
        );
        assert!(listed(&manager, "/other").await.is_empty());
        assert!(listed(&manager, "/").await.is_empty());
    }

    /// Plan 32 §0.5's other half: a directory that was *replaced* — removed,
    /// and a new one made at the same path — keeps today's behavior. The
    /// path matches and the inode does not, so the old directory's
    /// snapshots still show under the new one, frozen as they were. Only
    /// where the replaced directory's snapshot and the new directory's own
    /// (renamed-in) history share a name does identity win.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_replaced_directory_keeps_the_path_rule() {
        let meta = Arc::new(crate::mtree_publish::test_meta());
        let a = meta.mkdir(1, "a", 0o755, 1, 1).unwrap();
        meta.create(a.ino, "old-file", 0o644, 1, 1).unwrap();
        let chunks = Arc::new(constellation_store_s3::ChunkStore::new(Arc::new(
            InMemory::new(),
        )));
        let (manager, _nodes) = test_manager(meta.clone(), chunks.clone(), DEFAULT_CHUNK_SIZE);
        // A publish refuses while anything is unshipped, a snapshot's own
        // row included.
        let mut segment = 0;
        let mut ship = || {
            segment += 1;
            ship_all(&meta, segment)
        };
        ship();
        manager.create("/a", "daily").await.unwrap();
        ship();
        manager.create("/a", "weekly").await.unwrap();
        ship();

        // rm -r /a; mkdir /a
        meta.unlink(a.ino, "old-file").unwrap();
        meta.rmdir(1, "a").unwrap();
        let new_a = meta.mkdir(1, "a", 0o755, 1, 1).unwrap();
        assert_ne!(new_a.ino, a.ino);
        ship();
        let covering = covering_at(&manager, "/a").await;
        let names: Vec<&str> = covering.iter().map(|(row, _)| row.name.as_str()).collect();
        assert_eq!(names, ["daily", "weekly"]);
        assert_eq!(covering[0].1.ino, a.ino, "the old directory, frozen");
        assert_eq!(frozen_names(&manager, &covering[0].1).await, ["old-file"]);

        // Move another directory, with a `daily` of its own, onto the path:
        // its own `daily` (identity) beats the replaced one's (path);
        // the replaced one's `weekly` still shows.
        let c = meta.mkdir(1, "c", 0o755, 1, 1).unwrap();
        meta.create(c.ino, "c-file", 0o644, 1, 1).unwrap();
        ship();
        manager.create("/c", "daily").await.unwrap();
        ship();
        meta.rmdir(1, "a").unwrap();
        meta.rename(1, "c", 1, "a").unwrap();
        ship();
        let covering = covering_at(&manager, "/a").await;
        let shown: Vec<String> = covering
            .iter()
            .map(|(row, _)| format!("{}@{}", row.path, row.name))
            .collect();
        assert_eq!(shown, ["/c@daily", "/a@weekly"]);
        assert_eq!(frozen_names(&manager, &covering[0].1).await, ["c-file"]);
    }
}
