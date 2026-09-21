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
use constellation_mtree::{NodeHash, Tree};
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
    pub fn inode(
        &self,
        ino: Ino,
        value: &[u8],
        spilled: &[Pair],
    ) -> Result<TreeInode> {
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
        use constellation_fs_core::cache::DiskCache;
        let backend = log.inner();
        let (hasher, disk) = match log.e2e_keys() {
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
