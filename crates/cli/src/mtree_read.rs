//! Plan 28 §11 (S6): read a published commit back — bootstrap, the
//! partial-replica reader, and `fsck`'s tree comparison.
//!
//! S5 made the bucket's metadata a canonical Merkle map. This module is
//! the other direction: given a commit, reconstruct what the replica
//! that published it held.
//!
//! ## Bootstrap
//!
//! [`bootstrap_from_commit`] replaces the checkpoint restore. It finds
//! the chain head, walks the tree with one ordered cursor, and writes
//! the rows straight into a fresh SQLite replica: `0x01` → `inode` (and
//! inline `xattr`), `0x02` → `dentry`, `0x03` → spilled `xattr`, `0x30`
//! → partitions, snapshot rows and the replicated quota. `0x04` is not
//! loaded; it is the `dentry_by_ino` index, which SQLite derives. The
//! caller then resumes tailing each partition from the commit's
//! [`Commit::applied`] vector, exactly where a checkpoint's
//! `VECTOR.json` used to put it.
//!
//! Three things a checkpoint carried and a commit deliberately does not:
//!
//! - **atime.** §P6 keeps it out of the tree, so a bootstrapped inode's
//!   atime starts at its mtime. atime is node-local and best-effort
//!   everywhere else too.
//! - **`deref`.** The deferred-dereference table only makes chunk GC
//!   *faster*: a replica that never saw a dereference misses some
//!   candidates, and the orphan LIST pass collects those later. Nothing
//!   becomes unsafe.
//! - **parked cross-partition renames.** The publisher refuses to commit
//!   while `xpart_pending` is non-empty (see `mtree_publish`), so no
//!   commit can split a rename pair and none needs to carry the table.
//!
//! ## The partial replica
//!
//! [`TreeReader`] answers FUSE's metadata questions — `lookup`,
//! `getattr`, `readdir(plus)`, `listxattr` — straight from a root hash
//! over a node cache, fetching only the leaves a question needs. That
//! is ADR-5's 100M+ path as a property of the format: with the interior
//! resident (§14.1: ~20 MiB at census scale), a cold `ls -la` is the
//! directory's leaves, which S4 packs together. It is exercised at
//! reader level here; *serving* FUSE from it needs §11b's engine swap,
//! because in (B) SQLite answers every FUSE call.
//!
//! ## Why a pack catalog and not the commit's `packs`
//!
//! A commit names only the packs *it* wrote. Its tree mostly lives in
//! packs written by ancestors, some of them retired by retention, and
//! after S7b's compactor some nodes live in packs no commit ever named.
//! The only complete answer to "where is this node" is the set of pack
//! indices on the bucket, so readers load the catalog.

use anyhow::{bail, Context, Result};
use constellation_fs_core::{FileAttr, Ino, InodeKind};
use constellation_meta::{SnapshotRow, SqliteMeta, TreeInode};
use constellation_mtree::keys::{self, Key, Subsystem};
use constellation_mtree::record::{self, DentryRecord, InodeRecord, Payload};
use constellation_mtree::{NodeHash, NodeRef, NodeStore, Tree};
use constellation_store_s3::{BlobStore, Commit, CommitChain, LogStore, NodeCache, SHARD0};
use std::collections::BTreeMap;
use std::sync::Arc;

/// A tree key and its value.
type Pair = (Vec<u8>, Vec<u8>);

// ------------------------------------------------------ 0x30 record codec

/// The replicated quota is stored under `QUOTA_KV_KEY` as a decimal
/// string, empty meaning "explicitly unlimited". An absent key and an
/// empty one differ (the first defers to the creation-time cap), so the
/// tree distinguishes them too: no record, or a record with no field.
pub(crate) fn quota_record(max_bytes: Option<u64>) -> Vec<u8> {
    match max_bytes {
        Some(n) => record::encode_fields(&[&n.to_le_bytes()]),
        None => record::encode_fields(&[]),
    }
}

pub(crate) fn snapshot_record(row: &SnapshotRow) -> Vec<u8> {
    record::encode_fields(&[
        row.path.as_bytes(),
        row.name.as_bytes(),
        row.root_hash.as_bytes(),
        &row.created_unix_ms.to_le_bytes(),
    ])
}

pub(crate) fn partition_record(root_ino: Ino) -> Vec<u8> {
    record::encode_fields(&[&root_ino.to_le_bytes()])
}

fn fixed<const N: usize>(field: &[u8], what: &str) -> Result<[u8; N]> {
    field
        .try_into()
        .with_context(|| format!("{what}: expected {N} bytes, got {}", field.len()))
}

fn text(field: &[u8], what: &str) -> Result<String> {
    String::from_utf8(field.to_vec()).with_context(|| format!("{what} is not UTF-8"))
}

/// The `0x30` keys and values the replica's current state calls for,
/// read through the reader connection so the publisher's snapshot
/// covers them.
pub(crate) fn subsystem_state(meta: &SqliteMeta) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut out = BTreeMap::new();
    for row in meta.snapshots_reader()? {
        out.insert(
            keys::subsystem(Subsystem::Snapshot, row.id.as_bytes()),
            snapshot_record(&row),
        );
    }
    if let Some(raw) = meta.kv_get_reader(constellation_meta::sqlite::QUOTA_KV_KEY)? {
        let max = if raw.is_empty() {
            None
        } else {
            Some(raw.parse::<u64>().context("replicated quota")?)
        };
        out.insert(keys::subsystem(Subsystem::Quota, b""), quota_record(max));
    }
    for (id, root) in meta.partitions()? {
        out.insert(
            keys::subsystem(Subsystem::Partition, id.as_bytes()),
            partition_record(root),
        );
    }
    Ok(out)
}

/// The subsystem ranges the publisher owns. Clones, designations and
/// holds have no replicated SQLite state in (B) and are left alone.
pub(crate) const PUBLISHED_SUBSYSTEMS: [Subsystem; 3] =
    [Subsystem::Snapshot, Subsystem::Quota, Subsystem::Partition];

/// What the `0x30` range of a tree says.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Subsystems {
    pub partitions: Vec<(String, Ino)>,
    pub snapshots: Vec<SnapshotRow>,
    pub quota: Option<Option<u64>>,
}

impl Subsystems {
    fn absorb(&mut self, subsystem: Subsystem, id: &[u8], value: &[u8]) -> Result<()> {
        let fields = record::decode_fields(value)?;
        match subsystem {
            Subsystem::Snapshot => {
                let [path, name, root, created] = fields[..] else {
                    bail!("snapshot record has {} fields", fields.len());
                };
                self.snapshots.push(SnapshotRow {
                    id: text(id, "snapshot id")?,
                    path: text(path, "snapshot path")?,
                    name: text(name, "snapshot name")?,
                    root_hash: text(root, "snapshot root")?,
                    created_unix_ms: i64::from_le_bytes(fixed(created, "snapshot time")?),
                });
            }
            Subsystem::Quota => {
                self.quota = Some(match fields[..] {
                    [] => None,
                    [max] => Some(u64::from_le_bytes(fixed(max, "quota")?)),
                    _ => bail!("quota record has {} fields", fields.len()),
                });
            }
            Subsystem::Partition => {
                let [root] = fields[..] else {
                    bail!("partition record has {} fields", fields.len());
                };
                self.partitions.push((
                    text(id, "partition id")?,
                    u64::from_le_bytes(fixed(root, "partition root")?),
                ));
            }
            // Not written by (B); ignore rather than refuse, so a later
            // writer adding them does not break an older reader.
            Subsystem::Clone | Subsystem::Designation | Subsystem::Hold => {}
        }
        Ok(())
    }
}

// ------------------------------------------------------- record decoding

/// A `0x01` record as the replica's `inode` row. `atime` is set to the
/// mtime: §P6 keeps atime out of the tree.
pub(crate) fn attr_of(ino: Ino, attrs: &record::Attrs) -> Result<FileAttr> {
    Ok(FileAttr {
        ino,
        kind: InodeKind::from_u8(attrs.kind.as_u8())
            .with_context(|| format!("inode {ino}: kind {}", attrs.kind.as_u8()))?,
        size: attrs.size,
        mode: attrs.mode,
        uid: attrs.uid,
        gid: attrs.gid,
        nlink: attrs.nlink,
        atime_ns: attrs.mtime_ns,
        mtime_ns: attrs.mtime_ns,
        ctime_ns: attrs.ctime_ns,
        rdev: attrs.rdev,
    })
}

/// Resolves spilled payloads. A blob is fetched once per call; spills
/// are rare (a value over 1 KiB), so there is no cache.
pub(crate) struct Resolver<'a> {
    pub blobs: &'a BlobStore,
    pub handle: &'a tokio::runtime::Handle,
}

impl Resolver<'_> {
    pub fn payload(&self, payload: &Payload) -> Result<Vec<u8>> {
        match payload {
            Payload::Inline(bytes) => Ok(bytes.clone()),
            Payload::Spilled(hash) => {
                let fut = self.blobs.get(hash);
                let bytes = match tokio::runtime::Handle::try_current() {
                    Ok(_) => tokio::task::block_in_place(|| self.handle.block_on(fut)),
                    Err(_) => self.handle.block_on(fut),
                };
                Ok(bytes?)
            }
        }
    }

    /// One inode's replica row. `spilled` is the inode's `0x03` range
    /// when its record says the xattr set lives there.
    pub fn inode(&self, ino: Ino, value: &[u8], spilled: &[Pair]) -> Result<TreeInode> {
        let rec = InodeRecord::decode(value).with_context(|| format!("inode {ino}"))?;
        let manifest = rec
            .manifest
            .as_ref()
            .map(|payload| self.payload(payload))
            .transpose()?;
        let target = rec
            .symlink_target
            .as_ref()
            .map(|payload| {
                self.payload(payload)
                    .and_then(|t| text(&t, "symlink target"))
            })
            .transpose()?;
        let mut xattrs = Vec::with_capacity(rec.xattrs.len() + spilled.len());
        for (name, value) in &rec.xattrs {
            xattrs.push((text(name, "xattr name")?, value.clone()));
        }
        for (key, value) in spilled {
            let Key::Xattr { name, .. } = Key::parse(key)? else {
                bail!("inode {ino}: a non-xattr key in its xattr range");
            };
            let payload = Payload::decode(value)?;
            xattrs.push((text(name, "xattr name")?, self.payload(&payload)?));
        }
        Ok(TreeInode {
            attr: attr_of(ino, &rec.attrs)?,
            target,
            manifest,
            xattrs,
        })
    }
}

// ---------------------------------------------------------- tree reader

/// One directory entry as the tree stores it: the name, the target ino,
/// and S1's attr copy (so `readdirplus` needs nothing else).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct TreeEntry {
    pub name: Vec<u8>,
    pub ino: Ino,
    pub attrs: record::Attrs,
}

/// What [`TreeReader::verify`] found.
#[derive(Debug, Default)]
pub(crate) struct Verified {
    pub nodes: u64,
    pub leaves: u64,
    /// The root's own aggregate, recomputed from its entries.
    pub root: constellation_mtree::Agg,
    /// Interior entries whose recorded aggregate disagrees with the
    /// child they point at.
    pub bad_aggregates: Vec<NodeHash>,
}

/// FUSE's metadata reads over one published root, fetching only what
/// each call touches. See the module docs.
///
/// In (B) only `fsck` (via [`TreeReader::verify`]) and the tests call
/// it; the FUSE-shaped methods are the partial-replica API that §11b's
/// engine swap will serve from.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct TreeReader<S> {
    tree: Tree<S>,
    root: NodeHash,
}

#[cfg_attr(not(test), allow(dead_code))]
impl<S: NodeStore> TreeReader<S> {
    pub fn new(tree: Tree<S>, root: NodeHash) -> TreeReader<S> {
        TreeReader { tree, root }
    }

    pub fn tree(&self) -> &Tree<S> {
        &self.tree
    }

    /// `lookup(parent, name)`: one point read of `0x02`, attrs included.
    pub fn lookup(&self, parent: Ino, name: &[u8]) -> Result<Option<TreeEntry>> {
        let Some(value) = self.tree.get(&self.root, &keys::dentry(parent, name))? else {
            return Ok(None);
        };
        let rec = DentryRecord::decode(&value)?;
        Ok(Some(TreeEntry {
            name: name.to_vec(),
            ino: rec.ino,
            attrs: rec.attrs,
        }))
    }

    /// `getattr(ino)`: one point read of `0x01`.
    pub fn getattr(&self, ino: Ino) -> Result<Option<InodeRecord>> {
        match self.tree.get(&self.root, &keys::inode(ino))? {
            Some(value) => Ok(Some(InodeRecord::decode(&value)?)),
            None => Ok(None),
        }
    }

    /// One `readdir` page: up to `limit` entries of `dir` strictly after
    /// the name `after` (the previous page's last name). The cursor *is*
    /// a key, which is what makes §12's stable readdir offsets exact.
    pub fn readdir(&self, dir: Ino, after: Option<&[u8]>, limit: usize) -> Result<Vec<TreeEntry>> {
        let range = keys::dentries_of(dir);
        let from = match after {
            // The smallest key after `dir | after`: append a zero byte.
            Some(name) => {
                let mut key = keys::dentry(dir, name);
                key.push(0);
                key
            }
            None => range.start().to_vec(),
        };
        let mut out = Vec::new();
        for (key, value) in self.tree.range(&self.root, &from, range.prefix(), limit)? {
            let Key::Dentry { name, .. } = Key::parse(&key)? else {
                bail!("a non-dentry key inside directory {dir}'s range");
            };
            let rec = DentryRecord::decode(&value)?;
            out.push(TreeEntry {
                name: name.to_vec(),
                ino: rec.ino,
                attrs: rec.attrs,
            });
        }
        Ok(out)
    }

    /// The names of `ino`'s xattrs: inline in the `0x01` record, or the
    /// `0x03` range when the set spilled.
    pub fn listxattr(&self, ino: Ino) -> Result<Vec<Vec<u8>>> {
        let Some(rec) = self.getattr(ino)? else {
            return Ok(Vec::new());
        };
        if !rec.xattrs.is_empty() {
            return Ok(rec.xattrs.into_iter().map(|(name, _)| name).collect());
        }
        let range = keys::xattrs_of(ino);
        let mut names = Vec::new();
        for (key, _) in self
            .tree
            .range(&self.root, range.start(), range.prefix(), usize::MAX)?
        {
            if let Key::Xattr { name, .. } = Key::parse(&key)? {
                names.push(name.to_vec());
            }
        }
        Ok(names)
    }

    /// Read and check every node under the root: each is fetched through
    /// the store (which hash-verifies and structurally validates it),
    /// and every interior entry's aggregate must equal the child's own.
    /// The root's aggregate is returned for comparison with the commit.
    ///
    /// This is `fsck`'s "verify every byte of metadata": the root hash
    /// attests to every node below it, and the aggregate check is what
    /// turns a `du` that is consistently wrong on every replica into a
    /// reported corruption.
    pub fn verify(&self) -> Result<Verified> {
        let leaf_agg = self.tree.config().leaf_agg;
        let mut out = Verified::default();
        let mut stack = vec![(self.root, None::<constellation_mtree::Agg>)];
        while let Some((hash, claimed)) = stack.pop() {
            let bytes = self.tree.store().get(&hash)?;
            let node = NodeRef::parse(&bytes)?;
            let own = node.aggregate(leaf_agg)?;
            match claimed {
                Some(claimed) if claimed != own => out.bad_aggregates.push(hash),
                Some(_) => {}
                None => out.root = own,
            }
            out.nodes += 1;
            if node.is_leaf() {
                out.leaves += 1;
                continue;
            }
            for i in 0..node.count() {
                let (child, agg) = node.child(i)?;
                stack.push((child, Some(agg)));
            }
        }
        Ok(out)
    }

    /// One inode as its replica row, spilled values resolved. `None`
    /// when the tree has no record for it.
    pub fn inode_row(&self, ino: Ino, resolver: &Resolver<'_>) -> Result<Option<TreeInode>> {
        let Some(value) = self.tree.get(&self.root, &keys::inode(ino))? else {
            return Ok(None);
        };
        // An inline set means nothing spilled; otherwise read the range
        // (empty when the inode simply has no xattrs).
        let spilled = if InodeRecord::decode(&value)?.xattrs.is_empty() {
            let range = keys::xattrs_of(ino);
            self.tree
                .range(&self.root, range.start(), range.prefix(), usize::MAX)?
        } else {
            Vec::new()
        };
        Ok(Some(resolver.inode(ino, &value, &spilled)?))
    }

    /// Load every interior node and no leaf: the partial replica's
    /// resident set. Returns (interior nodes, the level-1 nodes' leaf
    /// children), the second being what a full replica would add.
    pub fn warm_interior(&self) -> Result<(usize, usize)> {
        let mut interior = 0usize;
        let mut leaves = 0usize;
        let mut stack = vec![self.root];
        while let Some(hash) = stack.pop() {
            let bytes = self.tree.store().get(&hash)?;
            let node = NodeRef::parse(&bytes)?;
            if node.is_leaf() {
                // A single-leaf tree: the root is the whole interior.
                continue;
            }
            interior += 1;
            for i in 0..node.count() {
                let (child, _) = node.child(i)?;
                if node.level() > 1 {
                    stack.push(child);
                } else {
                    leaves += 1;
                }
            }
        }
        Ok((interior, leaves))
    }
}

// -------------------------------------------------------------- catalog

/// Teach `cache` where every node on the bucket lives. See the module
/// docs for why this is the catalog rather than a commit's `packs`.
pub(crate) async fn attach_catalog(cache: &NodeCache) -> Result<usize> {
    cache
        .refresh_catalog()
        .await
        .context("loading the pack catalog")
}

// ------------------------------------------------------------ bootstrap

/// Disk budget for a bootstrap's scratch node cache. A bootstrap reads
/// each leaf once, in key order, so the cache only has to hold what is
/// in flight; eviction is fine.
const NODE_SCRATCH_BYTES: u64 = 1 << 30;

/// Keys per SQLite transaction during a bootstrap load. Bounds memory at
/// one page of rows; the tree is walked once regardless.
const LOAD_PAGE: usize = 8192;

/// What a tree load produced.
#[derive(Debug)]
pub(crate) struct Loaded {
    pub commit: Commit,
    pub inodes: u64,
    pub dentries: u64,
}

/// Walk `root` in key order and load it into `meta`.
///
/// Synchronous: the cursor resolves nodes through the cache's blocking
/// bridge, so callers run it on a blocking thread.
pub(crate) fn load_tree(
    tree: &Tree<Arc<NodeCache>>,
    root: &NodeHash,
    meta: &SqliteMeta,
    resolver: &Resolver<'_>,
) -> Result<(u64, u64)> {
    // A `0x01` record whose xattr set spilled cannot be finished without
    // its `0x03` keys, and the `0x03` range sorts after all of `0x01`.
    // Spilled sets are rare (a set over 256 B), so read that range
    // first with its own cursor and keep it; everything else streams.
    let mut spilled: BTreeMap<Ino, Vec<Pair>> = BTreeMap::new();
    let xattrs = keys::whole_range(keys::RANGE_XATTR);
    let mut cursor = tree.cursor_at(root, xattrs.start())?;
    while let Some((key, value)) = cursor.entry()? {
        if !xattrs.contains(key) {
            break;
        }
        let Key::Xattr { ino, .. } = Key::parse(key)? else {
            bail!("a non-xattr key inside the xattr range");
        };
        spilled
            .entry(ino)
            .or_default()
            .push((key.to_vec(), value.to_vec()));
        cursor.next()?;
    }

    let mut inodes: Vec<TreeInode> = Vec::new();
    let mut dentries: Vec<(Ino, String, Ino)> = Vec::new();
    let mut subsystems = Subsystems::default();
    let (mut n_inodes, mut n_dentries) = (0u64, 0u64);
    let flush = |inodes: &mut Vec<TreeInode>, dentries: &mut Vec<(Ino, String, Ino)>| {
        meta.load_tree_rows(inodes, dentries)?;
        inodes.clear();
        dentries.clear();
        anyhow::Ok(())
    };

    let mut cursor = tree.cursor(root)?;
    while let Some((key, value)) = cursor.entry()? {
        match Key::parse(key)? {
            Key::Inode { ino } => {
                let own = spilled.remove(&ino).unwrap_or_default();
                inodes.push(resolver.inode(ino, value, &own)?);
                n_inodes += 1;
            }
            Key::Dentry { parent_ino, name } => {
                let target = DentryRecord::decode(value)?;
                dentries.push((parent_ino, text(name, "dentry name")?, target.ino));
                n_dentries += 1;
            }
            // Already read above, and derived by SQLite respectively.
            Key::Xattr { .. } | Key::RDentry { .. } => {}
            Key::Subsystem { subsystem, id } => subsystems.absorb(subsystem, id, value)?,
        }
        if inodes.len() + dentries.len() >= LOAD_PAGE {
            flush(&mut inodes, &mut dentries)?;
        }
        cursor.next()?;
    }
    if let Some((&ino, _)) = spilled.iter().next() {
        bail!("xattr keys for inode {ino}, which has no inode record");
    }
    flush(&mut inodes, &mut dentries)?;
    meta.load_tree_subsystems(
        &subsystems.partitions,
        &subsystems.snapshots,
        subsystems.quota,
    )?;
    meta.finish_tree_load()?;
    Ok((n_inodes, n_dentries))
}

/// Everything a reader of the commit chain needs.
pub(crate) struct ChainReader {
    pub chain: CommitChain,
    pub cache: Arc<NodeCache>,
    pub blobs: BlobStore,
    pub config: constellation_mtree::Config,
}

impl ChainReader {
    /// A reader over the bucket `log` writes to, hashing the way its
    /// writers do (keyed under the addressing key on an E2E
    /// filesystem, §P13), with its node cache in `cache_dir`.
    pub fn for_log(log: &LogStore, cache_dir: &std::path::Path) -> Result<ChainReader> {
        Self::for_store(log.inner(), log.e2e_keys(), cache_dir)
    }

    /// [`Self::for_log`] from a bare backend and the filesystem's keyring.
    pub fn for_store(
        backend: Arc<dyn object_store::ObjectStore>,
        keys: Option<&constellation_store_s3::SharedE2eKeys>,
        cache_dir: &std::path::Path,
    ) -> Result<ChainReader> {
        use constellation_fs_core::cache::DiskCache;
        let (hasher, disk) = match keys {
            Some(keys) => (
                constellation_mtree::Hasher::Keyed(*keys.addressing_key()),
                DiskCache::open_keyed(cache_dir, NODE_SCRATCH_BYTES, *keys.addressing_key())?,
            ),
            None => (
                constellation_mtree::Hasher::Plain,
                DiskCache::open(cache_dir, NODE_SCRATCH_BYTES)?,
            ),
        };
        let cache = Arc::new(NodeCache::new(
            constellation_store_s3::PackStore::new(backend.clone()),
            Arc::new(disk),
            hasher,
            tokio::runtime::Handle::current(),
        ));
        Ok(ChainReader {
            chain: CommitChain::new(backend.clone()),
            cache,
            blobs: BlobStore::new(backend, hasher),
            config: record::config().with_hasher(hasher),
        })
    }

    pub fn tree(&self) -> Result<Tree<Arc<NodeCache>>> {
        Ok(Tree::with_config(self.cache.clone(), self.config)?)
    }

    /// The newest commit, if the chain has one.
    pub async fn head(&self) -> Result<Option<Commit>> {
        let Some(seq) = self.chain.discover_head(0).await? else {
            return Ok(None);
        };
        Ok(self.chain.get(seq).await?)
    }
}

/// Load the chain head into a fresh replica at `meta`. `None` when the
/// chain is empty and the caller should fall back to a checkpoint.
///
/// The caller owns tailing: it resumes each partition's log from the
/// returned commit's `applied` vector.
pub(crate) async fn bootstrap_from_commit(
    reader: &ChainReader,
    meta: Arc<SqliteMeta>,
) -> Result<Option<Loaded>> {
    let Some(commit) = reader.head().await? else {
        return Ok(None);
    };
    let root = commit
        .root(SHARD0)
        .with_context(|| format!("commit {} names no shard 0 root", commit.seq))?;
    let packs = attach_catalog(&reader.cache).await?;
    let tree = reader.tree()?;
    let blobs = reader.blobs.clone();
    let handle = tokio::runtime::Handle::current();
    let started = std::time::Instant::now();
    let (inodes, dentries) = tokio::task::spawn_blocking(move || {
        let resolver = Resolver {
            blobs: &blobs,
            handle: &handle,
        };
        load_tree(&tree, &root, &meta, &resolver)
    })
    .await
    .context("tree bootstrap task")??;
    tracing::info!(
        seq = commit.seq,
        inodes,
        dentries,
        packs,
        ms = started.elapsed().as_millis(),
        "loaded metadata replica from commit"
    );
    Ok(Some(Loaded {
        commit,
        inodes,
        dentries,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mtree_publish::TreePublisher;
    use constellation_fs_core::cache::DiskCache;
    use constellation_fs_core::types::ROOT_INO;
    use constellation_meta::{LogRecord, MetaStore, SetXattrMode};
    use constellation_mtree::{Hasher, MtreeError};
    use constellation_store_s3::PackStore;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use std::collections::BTreeSet;
    use std::sync::Mutex;
    use tempfile::TempDir;

    /// Pack target small enough that a few thousand dentries span many
    /// packs, so "one pack per directory" is a claim with teeth.
    const SMALL_PACKS: usize = 256 << 10;

    fn cold_cache(store: Arc<dyn ObjectStore>, dir: &TempDir) -> Arc<NodeCache> {
        Arc::new(NodeCache::new(
            PackStore::new(store).with_target_bytes(SMALL_PACKS),
            Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap()),
            Hasher::Plain,
            tokio::runtime::Handle::current(),
        ))
    }

    /// `dirs` directories of `files` files each, published as one commit.
    async fn published(
        dirs: u64,
        files: u64,
    ) -> (Arc<dyn ObjectStore>, Arc<SqliteMeta>, Commit, Vec<Ino>) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let meta = Arc::new(SqliteMeta::open_in_memory().unwrap());
        let mut inos = Vec::new();
        for d in 0..dirs {
            let dir = meta
                .mkdir(ROOT_INO, &format!("dir{d:03}"), 0o755, 0, 0)
                .unwrap()
                .ino;
            for f in 0..files {
                meta.create(
                    dir,
                    &format!("file-{f:05}-with-a-longish-name"),
                    0o644,
                    0,
                    0,
                )
                .unwrap();
            }
            inos.push(dir);
        }
        let big = meta
            .child_ino(inos[0], "file-00000-with-a-longish-name")
            .unwrap()
            .unwrap();
        for i in 0..40 {
            meta.set_xattr(big, &format!("user.k{i}"), b"vvvv", SetXattrMode::Set)
                .unwrap();
        }
        let dir = TempDir::new().unwrap();
        let mut publisher = TreePublisher::new(
            Arc::clone(&meta),
            cold_cache(Arc::clone(&store), &dir),
            BlobStore::new(Arc::clone(&store), Hasher::Plain),
            CommitChain::new(Arc::clone(&store)),
            record::config(),
            1,
            tokio::runtime::Handle::current(),
        );
        let records: Vec<LogRecord> = meta
            .take_journal(usize::MAX)
            .unwrap()
            .into_iter()
            .map(|(_, r)| r)
            .collect();
        publisher.note(&records);
        let commit = publisher.publish(1).await.unwrap().unwrap();
        (store, meta, commit, inos)
    }

    /// Every node a reader asked for, with its level.
    struct Recording<S> {
        inner: S,
        seen: Mutex<Vec<(NodeHash, u8)>>,
    }

    impl<S: NodeStore> NodeStore for Recording<S> {
        fn get(&self, hash: &NodeHash) -> Result<Arc<[u8]>, MtreeError> {
            let bytes = self.inner.get(hash)?;
            let level = NodeRef::parse(&bytes)?.level();
            self.seen.lock().unwrap().push((*hash, level));
            Ok(bytes)
        }
        fn put(&self, hash: NodeHash, level: u8, bytes: Vec<u8>) -> Result<(), MtreeError> {
            self.inner.put(hash, level, bytes)
        }
    }

    /// The reader agrees with the replica it was published from on every
    /// FUSE-shaped question, including a spilled xattr set and paging.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_reader_answers_what_the_replica_answers() {
        let (store, meta, commit, dirs) = published(3, 250).await;
        let dir = TempDir::new().unwrap();
        let cache = cold_cache(store, &dir);
        cache.refresh_catalog().await.unwrap();
        let reader = TreeReader::new(
            Tree::with_config(cache, record::config()).unwrap(),
            commit.root(SHARD0).unwrap(),
        );
        tokio::task::spawn_blocking(move || {
            for &d in &dirs {
                let want = meta.readdir(d).unwrap();
                let mut got = Vec::new();
                let mut after: Option<Vec<u8>> = None;
                loop {
                    let page = reader.readdir(d, after.as_deref(), 97).unwrap();
                    let Some(last) = page.last() else { break };
                    after = Some(last.name.clone());
                    got.extend(page);
                }
                assert_eq!(got.len(), want.len());
                for (g, w) in got.iter().zip(&want) {
                    assert_eq!((g.name.as_slice(), g.ino), (w.name.as_bytes(), w.ino));
                    let attr = meta.getattr(w.ino).unwrap().unwrap();
                    assert_eq!((g.attrs.size, g.attrs.mode), (attr.size, attr.mode));
                }
                let one = &want[want.len() / 2];
                assert_eq!(
                    reader.lookup(d, one.name.as_bytes()).unwrap().unwrap().ino,
                    one.ino
                );
                assert!(reader.lookup(d, b"absent").unwrap().is_none());
                assert_eq!(reader.getattr(one.ino).unwrap().unwrap().attrs.nlink, 1);
            }
            let big = meta
                .child_ino(dirs[0], "file-00000-with-a-longish-name")
                .unwrap()
                .unwrap();
            let mut names = reader.listxattr(big).unwrap();
            names.sort();
            let mut want: Vec<Vec<u8>> = meta
                .list_xattrs(big)
                .unwrap()
                .into_iter()
                .map(String::into_bytes)
                .collect();
            want.sort();
            assert_eq!(names, want);
            assert_eq!(want.len(), 40, "the set must have spilled to exercise 0x03");
        })
        .await
        .unwrap();
    }

    /// §12's partial-replica test, at reader level.
    ///
    /// With only the interior resident, a cold `ls -la` of a directory
    /// reads its leaves from one pack (occasionally two, where a pack
    /// seal falls inside the directory), and a `readdir`-driven walk
    /// never faults in a leaf outside the directories it visits — bar
    /// the one leaf on either side a cursor must look at to know a
    /// range has ended. A control scan touches the whole tree, so the
    /// bounds cannot pass by the tree being tiny.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_partial_replica_reads_only_the_directories_it_visits() {
        let (store, _meta, commit, dirs) = published(48, 400).await;
        let dir = TempDir::new().unwrap();
        let cache = cold_cache(Arc::clone(&store), &dir);
        cache.refresh_catalog().await.unwrap();
        let root = commit.root(SHARD0).unwrap();
        let recording = Arc::new(Recording {
            inner: Arc::clone(&cache),
            seen: Mutex::new(Vec::new()),
        });
        let reader = TreeReader::new(
            Tree::with_config(Arc::clone(&recording), record::config()).unwrap(),
            root,
        );
        let cache2 = Arc::clone(&cache);
        tokio::task::spawn_blocking(move || {
            let (interior, total_leaves) = reader.warm_interior().unwrap();
            assert!(
                interior >= 2 && total_leaves > 100,
                "{interior} interior, {total_leaves} leaves"
            );
            assert_eq!(
                cache2.stats().distinct_leaf_packs,
                0,
                "warming the interior must not read a leaf"
            );

            // Cold `ls -la`, one directory at a time.
            let mut one_pack = 0;
            for &d in &dirs {
                cache2.reset_stats();
                let entries = reader.readdir(d, None, usize::MAX).unwrap();
                assert_eq!(entries.len(), 400);
                let packs = cache2.stats().distinct_leaf_packs;
                assert!(packs <= 2, "directory {d} spans {packs} packs");
                one_pack += usize::from(packs == 1);
            }
            assert!(
                one_pack * 4 >= dirs.len() * 3,
                "only {one_pack}/{} dirs in one pack",
                dirs.len()
            );

            // A walk over a few directories, recording every leaf read.
            recording.seen.lock().unwrap().clear();
            let visited = [dirs[3], dirs[17], dirs[40]];
            for &d in &visited {
                reader.readdir(d, None, usize::MAX).unwrap();
            }
            let leaves: BTreeSet<NodeHash> = recording
                .seen
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, level)| *level == 0)
                .map(|(hash, _)| *hash)
                .collect();
            let mut outside = 0;
            for hash in &leaves {
                let bytes = recording.inner.get(hash).unwrap();
                let node = NodeRef::parse(&bytes).unwrap();
                let inside = (0..node.count()).any(|i| {
                    let key = node.key(i).unwrap();
                    visited.iter().any(|&d| keys::dentries_of(d).contains(key))
                });
                outside += usize::from(!inside);
            }
            assert!(
                outside <= 2 * visited.len(),
                "{outside} of {} leaves read hold none of the visited directories",
                leaves.len()
            );
            assert!(
                leaves.len() * 10 < total_leaves,
                "{} of {total_leaves}",
                leaves.len()
            );

            // Control: a full scan does touch (nearly) every leaf.
            recording.seen.lock().unwrap().clear();
            let mut cursor = reader.tree().cursor(&root).unwrap();
            while cursor.entry().unwrap().is_some() {
                cursor.next().unwrap();
            }
            let scanned: BTreeSet<NodeHash> = recording
                .seen
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, level)| *level == 0)
                .map(|(hash, _)| *hash)
                .collect();
            assert_eq!(scanned.len(), total_leaves);
        })
        .await
        .unwrap();
    }
}
